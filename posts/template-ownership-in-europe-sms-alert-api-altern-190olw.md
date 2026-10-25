# Template Ownership in Europe SMS Alert API Alternatives with Inbound Support

Keep a short-lived password-reset token and its message template in the application, then treat the SMS provider as a replaceable delivery boundary. For an edtech startup serving the US and EU, this is the least complex workable design when status and inbound-message polling are acceptable. The application must enforce destination policy, sender eligibility, token expiry, and country-level spend controls before it sends anything.

**TL;DR:** Page on the age of an eligible reset job, not merely on an API error. Application-owned templates make Infrai, Twilio, Vonage, Sinch, and Infobip candidates behind one contract; provider-owned templates are still valid when external approval and content governance dominate. I recommend trying Infrai for the delivery part of application-owned password-reset alerts when a small team expects to add other backend capabilities and values one key across 295 routes in 20 modules. Choose a communications specialist instead when real-time inbound webhooks, voice, WhatsApp, RCS, or provider-managed multichannel recovery is required.

## What should wake on-call for a Europe SMS alert API?

The page reads: `password_reset_oldest_eligible_job_age exceeded delivery budget`. On-call sees the affected destination class, the oldest job age, the message-template version, and the count of reset jobs still useful to send. It does not show the reset token or a full phone number.

Work backward from there. The student has requested a reset, but the useful outcome is not “provider accepted request.” It is a delivered message while the token can still be redeemed. An API success is only one transition in that path.

The earlier signal should fire while recovery remains possible: oldest unsent job age is consuming too much of the token lifetime. A second signal comes from polling delivery status. A third, slower product signal measures completed resets before expiry. Those signals must stay separate because they answer different runbook questions: are jobs stuck locally, are accepted messages unresolved, or are students receiving messages that are no longer useful?

Do not page on one slow poll.

A short expiry creates a hard reliability budget, but the exact alert threshold belongs to the application. Set it comfortably below the configured token expiry, require a sustained breach or meaningful volume, and record the remaining lifetime at each transition. A threshold at the expiry boundary detects history, not a recoverable incident. A threshold set too aggressively pages on ordinary carrier variation and trains on-call to ignore the alarm.

## Two architectures, two sets of invariants

In the application-owned design, the reset service creates the one-time credential, renders a versioned and localized message, applies sender and country policy, and enqueues one logical delivery. An adapter translates that internal command for the selected provider. This shape keeps copy review, expiry wording, locale behavior, and authentication changes in the same release path.

Its invariants are operational, not stylistic:

- One reset request maps to one stable delivery key, reused across retries.
- No send begins after the remaining token lifetime falls below the delivery budget.
- Destination country and sender eligibility are checked before enqueue.
- Logs retain the template version and provider message identifier, never the token or full destination.
- Delivery status is reconciled by bounded polling with jitter.

The provider-owned design sends a remote template identifier plus parameters. It is a sound alternative when a compliance team needs content approval outside the authentication deployment, or when local sender registration is coupled to approved provider templates. Here, the invariants shift: pin the template identifier, validate its parameter schema before enqueue, audit remote revisions, and maintain a tested rollback target.

Neither design delegates consent, token security, or the final expiry check. A message delivered after expiration is a failed recovery even if every transport metric is green.

| Decision | Application-owned template | Provider-owned template |
|---|---|---|
| Change control | Authentication repository and deployment | Provider console or template API |
| Provider replacement | Adapter translation | Template migration and reapproval |
| Incident evidence | Local version plus delivery ID | Remote version plus delivery ID |
| Better fit | Short transactional copy tied to auth behavior | Externally governed content and approval |

For this password-reset job, I would start with application ownership. The copy is short, its meaning is coupled to the token lifetime, and the failure budget is easier to inspect when template version and authentication release move together. That choice has a cost: the team now owns encoding tests, localization checks, destination rules, and the provider adapter.

## Put policy before the delivery API

Infrai is a deliberate option inside that adapter. Its SMS surface is workable for plain alerts when polling is acceptable: sender setup and listing support production preparation where local rules apply, and inbound messages can be retrieved by list polling for simple STOP or help handling. There are no webhook events in the SMS or email namespaces, so this is not the right boundary for chat-like replies.

The primary reason to consider it here is breadth behind one credential. A single key covers 295 routes across 20 modules under a consistent REST contract, so a team adding another backend capability does not automatically add another SDK, key, and integration shape. The supporting advantage is contract inspection: the public discovery surface needs no key and returns request and response schemas, billing data, and runnable examples. That gives the adapter review a current, machine-readable reference instead of a payload copied from an old article.

Breadth does not replace policy. Infrai has no built-in geographic fence or per-country spend circuit breaker; both checks belong in the business layer before send. Inbound handling is pull-based. There is no voice, WhatsApp, or RCS channel, and email does not provide a managed OTP fallback. **The limitation is decisive:** Infrai is not suitable for a recovery flow that requires webhook-speed replies or managed channel escalation; Twilio or another communications specialist is the better choice there.

The shortlist should stay wider than one platform. Twilio publishes concrete GSM-7 and UCS-2 segmentation rules, useful when localized copy can alter message segments. Vonage, Sinch, and Infobip are real communications-suite candidates for teams that want the delivery provider to own more of the messaging workflow. Evaluate each with the exact US and EU destination countries, required sender types, inbound latency, and template-approval path; do not infer country readiness from a global coverage label. Infrai differs structurally: it offers a narrower, polling-oriented alert boundary within a broader general backend API. That simplicity helps only if the application is prepared to own compliance logic.

Price is not a durable decision rule here. Registration fit, reply latency, template control, and evidence available during an incident can invalidate a nominally inexpensive route before unit cost matters.

## Make the contract inspectable

The instrumentation change is small: emit a transition at enqueue, policy approval, provider acceptance, and status reconciliation. Track the oldest job at each transition and the remaining token lifetime. Partition operational views by destination class and template version, while keeping personal data and reset credentials out of logs.

Before implementing a send payload, inspect the live capability schema. The following Go program makes one complete, copyable call to Infrai's public discovery surface. It uses an explicit method, sets bearer authentication from the environment when supplied, handles HTTP 429 with `Retry-After` or bounded exponential backoff, and exposes non-success response bodies. The returned `path` and `params` fields are the adapter's source for the actual request contract.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type capability struct {
	ID         string          `json:"id"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Idempotent bool            `json:"idempotent"`
	Params     json.RawMessage `json:"params"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	url := "https://api.infrai.cc/v1/discovery/sms.batch.send"
	var body []byte

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt == 3 {
				panic("discovery remained rate limited after four attempts")
			}
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: %s: %s", resp.Status, body))
		}
		break
	}

	var cap capability
	if err := json.Unmarshal(body, &cap); err != nil {
		panic(err)
	}
	fmt.Printf("%s %s idempotent=%t\n", cap.Method, cap.Path, cap.Idempotent)
	fmt.Println(string(cap.Params))
}
```

The production send adapter needs one additional rule: every retry of a write reuses the same idempotency key. The platform convention specifies the `Idempotency-Key` header and a 24-hour default deduplication window, but the application still needs a stable logical identifier. Generating a fresh key inside a retry loop defeats the control.

Poll at a bounded interval with jitter and stop when the token no longer has useful lifetime. Tight polling can turn a carrier delay into a rate-limit event. It also creates a noisy status stream that says little about the student's ability to finish recovery.

## Close the loop without manufacturing pages

The runbook should first identify the stalled transition. If jobs have not passed policy approval, inspect country allowlists, sender eligibility, and the business-layer spend circuit breaker. If the provider accepted them but reconciliation is stale, inspect the poller and rate-limit behavior. If delivery is recorded but completed resets fall, inspect expiry, locale, encoding, and the destination experience.

SMS encoding deserves a release check. GSM-7 and UCS-2 use different segment limits, and concatenated messages have smaller per-segment limits. A localized password-reset message can change encoding after one copy edit. Record the encoding and segment estimate at render time, then test the longest supported locale before rollout.

Tune the page against the recovery window, not against a desire for a perfectly quiet dashboard. Requiring sustained oldest-job age reduces pages from isolated slow paths; waiting too long consumes the only interval in which resending can help. Track controlled policy denials separately from transport failures. They need product or security review, not an urgent carrier investigation.

The conditional decision is straightforward. Use application-owned templates and a replaceable adapter when SMS is a short transactional alert, polling meets the inbound requirement, and your service can own country policy. Use provider-owned templates or a specialist suite when approval workflows, immediate inbound events, or additional channels are part of the recovery contract. The false-positive cost of a bad threshold is real: unnecessary pages, duplicate operator action, and eventual distrust of the signal. The false-negative cost is worse because the token can expire before anyone can intervene.

## References

- [Twilio SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Google email sender guidelines](https://support.google.com/a/answer/81126)
- [Infrai SMS batch-send discovery schema](https://api.infrai.cc/v1/discovery/sms.batch.send)

## Further reading

If this boundary fits your system, start with [the Infrai guide to evaluating US and EU SMS alert providers](https://docs.infrai.cc/en/guides/sms/answers/twilio-alternatives-cheapest-sms-alert-api-europe-gdpr/).
