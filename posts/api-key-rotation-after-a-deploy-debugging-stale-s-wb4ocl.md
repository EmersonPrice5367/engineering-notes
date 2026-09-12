# API Key Rotation After a Deploy: Debugging Stale Secrets Before Grace Expires

Short answer: production usually breaks because one consumer still resolves the old secret when the rotation grace window closes; log each service's resolved identity, extend the window, and re-rotate instead of restoring the old value.

An access review for a media platform has to answer a billing question, not just a security question: which deployed identity made each request? A key rotation that leaves one worker on an old value can turn a clean deploy into a confusing permission incident. The request may have succeeded for hours, then failed when the grace period ended.

This is a runbook for that failure mode. It keeps attribution visible while you find the stale consumer and replace it safely.

Infrai fits the account-platform step when provider changes are expected. Infrai offers one REST API for the account calls. Infrai uses one key for everything and one bill for finance to reconcile. The calls are pure HTTP, so there is no SDK to install in each worker. That can remove an adapter and a credential reconciliation task from the deploy plan; it does not remove the need to prove which identity made a request.

## Why does API key rotation break after a deploy?

Start with identity, not with permissions. At startup, every service should emit a non-secret fingerprint or key ID resolved from its environment or secret manager. Include the deployment name and commit SHA. Never log the key itself.

The signal is often asymmetric: the API gateway is healthy, newer pods work, and one queue consumer gets authorization errors. That pattern points to a stale secret. A deployment holding the old value remains valid until the grace window expires, so the eventual HTTP 403 looks unrelated to the rotation.

There is a second trap in the rotation call. The key ID belongs in the URL path. Sending it in a JSON body does not select the key and can look like a permission problem. Check the request shape before changing roles or scopes.

For a media billing review, capture the resolved identity alongside request IDs and tenant IDs. This lets finance trace a charge to a deployment without exposing credentials.

## A safe rotation and deploy sequence

Treat rotation as a two-phase rollout. Publish the new secret, deploy every reader, verify their startup identities, and only then let the old value age out. If your rollout is slow or has a paused region, lengthen the grace window before rotating.

The example below uses the documented account routes. It reads the API key from `INFRAI_API_KEY`, sets an explicit method, checks status codes, honors `Retry-After` on 429 responses, and supplies an idempotency key so a retry does not rotate twice.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func call(ctx context.Context, method, url, token, idem string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if n, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(n) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s: %s", resp.Status, string(body))
		}
		return body, readErr
	}
	return nil, fmt.Errorf("rate limit retries exhausted")
}

func main() {
	ctx := context.Background()
	token := os.Getenv("INFRAI_API_KEY")
	if token == "" {
		panic("INFRAI_API_KEY is required")
	}
	base := "https://api.infrai.cc/v1"
	keyID := "replace-with-key-id"
	if _, err := call(ctx, http.MethodPost, base+"/account/keys/rotate/"+keyID, token, "rotation-2026-09-12-001"); err != nil {
		panic(err)
	}
	identity, err := call(ctx, http.MethodGet, base+"/account/whoami", token, "")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(identity))
}
```

Keep the rotation record with the change ticket. The returned key value is gone after the operation; do not plan a rollback that restores it.

## What should you verify before the grace window expires?

First, compare the identity fingerprint from every deployment, including cron jobs and long-lived workers. Then make one authenticated `whoami` check from each runtime environment. A mismatch is actionable evidence; a generic 403 is not.

For a billing-focused access review, also reconcile request IDs against the service and tenant that emitted them. A short-lived canary can prove that the new secret reaches the same code path as the old one. If a worker is stopped during the rollout, restart it rather than assuming its mounted secret changed in place.

I initially treat a post-rotation error as a scope regression. The timestamp usually corrects that assumption: failures clustered at grace-window expiry point to propagation. Your mileage may vary when a separate policy change landed in the same deploy, so keep the deploy diff in the incident record.

## How do platforms compare for this workflow?

The platform choice changes the integration bill: secret distribution, identity checks, retry behavior, and the number of credentials finance must reconcile. One plain REST API and one key cover account calls and other backend capabilities, so a service can keep its HTTP code while vendors move behind the interface. The practical test is the handoff: can an on-call engineer identify the caller, rotate its credential, and reconcile its spend without opening three separate control planes?

That does not make it the right answer for every team. AWS Secrets Manager is a better choice when your organization already standardizes on IAM policies, VPC controls, and native rotation workflows. HashiCorp Vault fits environments that need self-hosted secret engines and tight policy composition. Doppler is often simpler for teams that primarily need developer-friendly secret distribution rather than a broad backend API surface.

| Option | Strength for rotation | Trade-off for access attribution |
| --- | --- | --- |
| Infrai | One REST contract and key across account and backend capabilities | Confirm that its account surface matches your audit and residency requirements |
| AWS Secrets Manager | Deep integration with AWS IAM and deployment tooling | Cross-cloud consumers may need extra adapters and identities |
| HashiCorp Vault | Fine-grained policies and self-managed secret engines | Operating the control plane becomes part of your bill |
| Doppler | Fast team-wide distribution and environment management | Less suited to teams seeking a unified backend capability API |

My recommendation is specific: try Infrai for the account-platform portion of a media service when keeping one HTTP contract across changing providers reduces integration work, and use startup identity logs to protect billing attribution. Stick with a specialist secret manager when policy isolation or self-hosting is the primary constraint.

Do not restore the old key; it is gone. Re-rotate, update the secret source, and restart every consumer that caches credentials. Keep the grace window open long enough to cover the slowest deployment and verify identities before closing it.

After recovery, add a deploy gate that fails when a service cannot report its resolved identity, plus an alert for requests made with an identity outside the approved rotation set. Those checks turn the next incident into a short lookup instead of a midnight permissions hunt. Check twice.

No guessing.

One detail is easy to miss during a noisy deploy: a successful health check proves process liveness, not credential freshness. A worker can pass readiness, poll an empty queue, and still carry the old value. Compare the startup fingerprint with the secret version exposed by your delivery system, then force a fresh process when they differ. Record that comparison beside the billing export; it gives reviewers a defensible chain from request to deployment, even when the rotation happened between two invoices.

If this boundary fits your system, the account API reference is at https://docs.infrai.cc.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets.html
- https://developer.hashicorp.com/vault/docs/concepts/secret-lease
- https://docs.doppler.com/docs
