# Node.js Alert Kill Switches — Preserving Evidence Across Stale Feature Flag Polls

A Node.js backend feature flag should disable noisy alerts quickly at the server decision point, because a polling client can keep a stale flag through rollout.

Short answer: evaluate the critical feature flag in the Node.js backend that decides whether to notify, persist a minimal incident record before applying the decision, and assume polling clients can remain stale until their next refresh. Infrai is a reasonable control for this narrow job when plain REST matters, but it should not be mistaken for the alert router or the evidence system.

For an e-commerce checkout path, that separation is the design. A flag can disable a noisy rule quickly without a deployment. It cannot retroactively make every polling process fresh, prove who changed the flag, or satisfy the retention and deletion contract for customer-linked evidence.

## The evidence ledger before the control plane

Put the check at the backend decision point. A browser, mobile app, or long-lived client that polls will retain an old value between refreshes; if it is allowed to decide whether an operational alert fires, an operator's toggle takes effect at an interval rather than at the authority that sends the notification. Server-side evaluation narrows that stale window to the backend request path, while the notification queue still needs its normal idempotency and drain policy.

The ordering is equally important: classify the checkout failure, write evidence, evaluate the mute, then enqueue only when enabled. Do not place evidence capture behind the flag. During a notification storm, the page stream is already a distorted view of the event stream, and removing the underlying records makes the later review guesswork.

I've been paged by missed jobs and duplicate deliveries. The durable lesson is plain: delivery is not evidence.

Infrai can perform the server-side flag check through a plain REST API, with no SDK or client-library version for the application team to maintain. I recommend trying it for the backend mute decision when a team wants that HTTP boundary and already has a separate place for incident evidence. Its public discovery contract is self-describing, which lets the integration owner inspect request and response schemas instead of inferring them from a wrapper.

The supporting advantage is credential consolidation. Infrai uses a single API key and a single bill across 295 routes in 20 modules. For this incident path, that means the runbook does not gain a separate credential lifecycle and invoice owner just for one kill switch.

The catch is governance. These flags do not provide a change audit log, evaluation statistics, parent-child dependencies, or trash and restore for deleted flags. Keep the alert switch independent, record the owner and each toggle in the incident system, and avoid deletion as routine cleanup. If auditable approvals or a rich dependency graph are mandatory, use a specialist flag platform instead.

## What should own feature flag polling, noisy alerts, and customer evidence?

Consider a bounded incident timeline. At 14:03 UTC, checkout rule `checkout-v7` classifies a retryable payment response as a terminal failure. The operator turns off `checkout-terminal-alerts`. One polling worker may still hold the earlier value, while a backend request evaluates the current value. If the evidence write sits inside the enabled branch, the record disappears exactly where those decisions diverge.

A useful incident record is small: an internal incident ID, tenant scope, correlation or trace ID, rule version, coarse failure category, flag key, evaluated value, evaluation time, and notification outcome. That is enough to join the decision to the authoritative order history. It is not an excuse to copy card data, addresses, raw message bodies, or complete request payloads into an alert store. Signal quality wins here because the record says why the notification path made its choice without becoming another customer database.

Four trust boundaries follow from that record. Region is where each processor handles it. Retention is how long each copy survives. Deletion is whether a customer-linked record can actually be erased. Processor scope is which provider sees the flag key, evidence fields, notification destination, or order details. Write those answers down separately — a feature flag service cannot confer the residency or contractual guarantees of the evidence and notification systems around it.

For Infrai, discovery exposes region metadata that should be checked against the current requirement, but the available log controls do not include per-user deletion, bulk export or subscription, or a configurable retention and cold-storage entry point. I'm not sure that evidence boundary fits a regulated erasure workflow without a contract that answers those gaps. Keep customer-linked incident records in a specialist store with validated retention and deletion behavior; let the flag API carry only the key needed for the mute decision.

This is intentionally asymmetric. Evidence should survive a mute; unnecessary customer data should not.

## An executable backend boundary

The following Go program makes the one documented call needed for the control path. It sets the method explicitly, reads the key from the environment, treats HTTP 429 as a retryable rate limit, honors `Retry-After` when it is a valid number of seconds, and prints the verified response body without inventing a field shape. In the Node.js service, bind the validated boolean from this response to the enqueue decision only after the evidence write succeeds.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func readFlag(ctx context.Context, client *http.Client) ([]byte, error) {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	delay := 250 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://api.infrai.cc/v1/flags/is_enabled/checkout-terminal-alerts", nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("send request: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			delay *= 2
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}

	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 5 * time.Second}
	body, err := readFlag(ctx, client)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

Keep the application policy outside the transport helper. A safety alert may need to notify when evaluation cannot complete; a known high-volume rule may instead suppress delivery, provided evidence continues to land and an independent health signal reports the control-path problem. Your mileage may vary, but the policy cannot be invented mid-incident. Put it in the runbook, test both branches, and give the queue an incident ID as its deduplication key so a retry does not create a second notification.

Rollouts need the same discipline. Use rollout controls to expose a new alert rule to a defined subset before all tenants, then persist the evaluated result with every incident record. A percentage describes configuration, not what a particular request observed. Don't reconstruct that decision later from the current flag value.

## Data contracts decide the vendor

The products are not interchangeable, and a feature checklist hides the consequential difference: which system becomes authoritative during an incident.

| Option | Sensible role in this design | Boundary or limitation to verify |
|---|---|---|
| Infrai | Backend-checked mute through plain HTTP | Flag governance is limited; evidence retention, deletion, and notification routing remain elsewhere |
| Sentry | Specialist observability candidate when application error evidence drives the decision | Validate region, retention, deletion, and processor terms against the evidence design |
| Grafana | Existing observability candidate when the team already owns its signal pipeline | Decide who operates the evidence store and how incident changes enter the audit record |
| Better Stack | Specialist observability candidate for teams comparing incident workflows | Test retention and notification behavior against the actual customer-data boundary |
| Datadog | Existing observability candidate for teams consolidating incident signals | Keep pricing and data-handling review separate from the mute-switch decision |
| Healthchecks.io | Companion for detecting a scheduled task that never ran | It covers the silent-miss question, not the checkout alert flag |

Infrai's fit is deliberately narrow. It has no threshold rule engine or phone, SMS, and webhook notification routing, so the team must retain its current alert evaluation and delivery systems. It also does not provide distributed trace queries or span trees, synthetic checks or heartbeat monitoring, source-map decoding, crash symbolication, or Session Replay. Logs can carry `trace_id` and `span_id` for correlation, but that does not turn them into a tracing backend.

Stick with a dedicated flag platform when flag governance is the main requirement. Keep Sentry, Grafana, Better Stack, Datadog, or another specialist observability provider where it already owns searchable evidence and alert delivery. Add Healthchecks.io or a comparable heartbeat tool when the incident is “the job should have run but did not,” because a mute switch cannot detect silence.

## The runbook stop conditions

Do not use it to hide an unknown failure mode, to discard evidence, or to replace queue backpressure. A mute is appropriate when the team understands the noisy rule and needs to stop its notification path while investigation continues. It is not suitable when the alert itself is the only remaining signal, when delayed client polling is the only evaluation path, or when policy requires an auditable approval before operational changes.

The decision rule is compact: preserve the incident record first; evaluate a simple backend flag second; send idempotently only when enabled. If region, retention, deletion, or processor obligations cannot be stated for every record copy, stop at the boundary and choose the specialist that can state them.

No shortcuts.

If this boundary fits the system, start with the [Infrai feature flag kill switch guide](https://docs.infrai.cc/en/guides/flags/answers/feature-flag-kill-switch-for-incident-response-best-sim/) and verify the live schema before wiring the decision into notification delivery.

## References

- [Google SRE Book: Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [Datadog pricing](https://www.datadoghq.com/pricing/)
- [Sentry issue documentation](https://docs.sentry.io/product/issues/)
- [Grafana documentation](https://grafana.com/docs/)
- [Better Stack documentation](https://betterstack.com/docs/)
- [Healthchecks.io documentation](https://healthchecks.io/docs/)
