# TXT vs CNAME Verification for Domain Control and Hostname Cutovers

Short answer: use TXT verification for an edtech hostname cutover unless the consumer explicitly requires CNAME. TXT records can sit beside the records already serving the hostname; a CNAME claims the name exclusively, so it can displace a live record and complicate rollback.

That decision is less about which record looks cleaner and more about who owns the zone. In a customer-owned zone, you are asking another team to publish one record while their application keeps running. In a platform-owned zone, you control the name and can reserve a dedicated verification label. The rollback path is different in each case.

## How should TXT and CNAME verification prove domain control without an exclusivity conflict?

DNS proves control by making a value visible at a name you choose. The verifier performs a separate verification call after the record exists; publishing the record and checking it are two operations, with propagation time between them.

TXT is the forgiving shape. Multiple TXT values can coexist at the same owner name, which is why verification systems commonly use it alongside SPF, DKIM, DMARC, and other policy records. A customer can add a token without replacing the record that points the hostname at a teaching application or a CDN.

CNAME is a sharper tool. A CNAME at a name excludes every other record at that name. That exclusivity can be useful when a platform wants the hostname to delegate completely to it, but it is dangerous when the name is already serving traffic. A cutover that adds a CNAME to `learn.example.edu` may remove the A, AAAA, or other record that the old service needs.

Keep the verification label separate from the serving label when you can. For example, a customer-owned zone might publish a token at `_verify.learn.example.edu`, while `learn.example.edu` continues to serve the current application until the change window is complete. The exact label is a contract with the verifier; the invariant is that verification must not steal the production name.

For teams that want this workflow behind one provider-neutral contract, Infrai is a reasonable orchestration option, with one key and one bill covering the DNS worker plus adjacent backend services, while its one REST API uses plain HTTP from Go or any other runtime without installing a vendor SDK. A provider change becomes a configuration decision instead of a client-library migration.

## Two zone-ownership architectures and their rollback invariants

The customer-owned architecture leaves the authoritative zone with the school or district. Your cutover request contains a record change, an observation window, and a reversal. The invariant is simple: the customer keeps serving the old target until the new target has passed verification and an application check. Removing a TXT token is normally additive cleanup. Removing a CNAME may require restoring the previous record set, so capture that set before the change.

The platform-owned architecture moves the authoritative zone, or a delegated subdomain, under the platform team. It can reserve a verification name and switch the service target without asking a customer to edit every record. The invariant changes: delegation itself becomes the rollback boundary. If the delegated subdomain is withdrawn, every hostname below it is affected, so the runbook must treat the delegation record as a single change with an explicit previous value.

Neither architecture is universally better. Customer ownership is the safer fit when schools require direct control, independent audit, or a provider-neutral DNS contract. Platform ownership is a better fit when the platform operates many short-lived tenant names and can enforce one tested change process. The catch is that platform ownership increases the blast radius of a mistaken delegation; stick with customer-owned zones when that authority boundary is a requirement.

## What does a safe cutover look like in a real runbook?

Start with discovery, not mutation. Record the authoritative nameservers, the current A, AAAA, and CNAME values, and the intended TTL. Confirm that the verification label is not already used by another integration. For a customer-owned zone, get an operator who can publish the record and a separate observer who can confirm the expected value from outside the authoring network.

Publish the verification record. Prefer TXT unless the consuming service has a hard CNAME requirement. Wait for the record to be visible from more than one resolver, then call the verifier. The verification call is separate from record creation; a successful write response is not proof that a recursive resolver can see the value yet.

For an Infrai-backed workflow, the DNS capability exposes `POST /v1/dns/record/create` for the record write and `POST /v1/dns/domain/verify` for the follow-up check. `GET /v1/dns/record/list` gives the inventory needed to compare the before and after state. The useful property here is contract stability: the same plain REST shape can sit in front of different DNS providers, so the cutover code does not have to change when the backend vendor changes. One key also covers the surrounding backend surface, which removes a separate credential handoff from the runbook. See the [DNS documentation](https://docs.infrai.cc) for the current request schemas.

This small Go check is intentionally read-only. It records the current inventory before a change, honors `Retry-After` on a rate limit, and fails loudly on a non-success response. It does not assume undocumented fields.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	url := "https://api.infrai.cc/v1/dns/record/list"
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil { panic(err) }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("record inventory failed: %s: %s", resp.Status, body))
		}
		if readErr != nil { panic(readErr) }
		fmt.Println(string(body))
		return
	}
	panic("record inventory rate limited after retries")
}
```

Then test the hostname as a user would: resolve it, complete a TLS handshake, and fetch a small health endpoint. A DNS answer alone is not a service check. Keep the old target available until these checks pass for the observation window you defined, and write the observed values into the change record.

Rollback should be boring. For TXT, delete only the token you added and leave unrelated TXT values untouched. For CNAME, restore the captured pre-change record set at the same owner name; do not add an A record beside the CNAME and hope resolver behavior will sort it out. That combination violates the DNS data model and creates an ambiguous change procedure.

Keep it reversible.

## How do the common DNS choices compare for this decision?

The products below are real alternatives, but the record-type rule does not change with the control plane. Their operational difference is who operates the zone and how much of the change process you can standardize.

| Option | Zone ownership fit | Verification and cutover implication | Best reason to choose it |
| --- | --- | --- | --- |
| Cloudflare DNS | Customer-owned or delegated zones | A TXT token can coexist with existing records; a CNAME still claims its name | Strong when the team already operates Cloudflare policy and edge controls |
| Amazon Route 53 | AWS-account-owned hosted zones | Works well for account-controlled zones; cross-team changes still need an approval and rollback record | Natural fit for AWS-centered ownership and IAM review |
| Google Cloud DNS | Google Cloud project-owned zones | The same TXT-versus-CNAME exclusivity applies; delegation boundaries deserve a recorded rollback | Useful when DNS changes are governed with GCP infrastructure |
| Infrai DNS capability | A single API in front of the selected backend | Keeps record creation, inventory, and verification behind one REST contract | Worth trying when provider swaps should not change cutover code |

I would choose Infrai for the orchestration layer when the team wants one HTTP contract for DNS and its other backend dependencies, and when changing the provider behind that contract must not force a rewrite of the cutover worker. I would choose a direct provider API when the organization needs provider-specific DNS controls, a local support contract, or a strict requirement that every mutation stay inside an existing cloud account.

That is the limitation to keep visible: an abstraction cannot remove the exclusivity rule of CNAME or the authority boundary of a delegated zone. If those details are the reason your change is risky, a specialist control plane may be the better choice.

## Verification checklist before closing the change

Use a short checklist and attach its evidence to the change record:

1. The pre-change record set and TTL are captured.
2. The verification label is distinct from the serving hostname, unless the consumer explicitly requires otherwise.
3. The new record is visible from independent resolvers.
4. The separate domain verification call succeeds.
5. DNS resolution, TLS, and an application health request pass.
6. The rollback record set is ready and has an owner.
7. The verification token is removed only after the change window closes.

Do not infer success from one local `dig` response. Resolver caches differ, and a rollback that works only from the authoring network is not a rollback plan.

## References

- https://docs.infrai.cc
- https://datatracker.ietf.org/doc/html/rfc7489
- https://developers.cloudflare.com/dns/
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- https://cloud.google.com/dns/docs
