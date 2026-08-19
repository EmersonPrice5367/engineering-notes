# How to Choose 3 Email Deliverability Services for Custom Domain SPF and DKIM Setup

Short answer: choose an API-only email service by proving three things in order: the custom domain passes SPF and DKIM checks, every terminal bounce reaches an authenticated webhook, and the suppression decision is durable before another student message can be queued.

For an edtech system, that sequence matters more than a long feature list. A password reset and a weekly course digest may share a sender domain, but they should not share retry policy. The reset is time-sensitive; the digest is disposable. Neither should keep targeting an address already reported as permanently invalid.

No SMTP is required for this design. The application submits mail through an HTTPS API, consumes delivery events through a webhook, and owns a small suppression ledger. The catch is that API-only integration moves more delivery-state responsibility into the application. A team that cannot operate a webhook consumer and replay queue should stick with a managed workflow that owns those pieces, even if its sending interface is less tidy.

## Govern recipient eligibility as durable state

The dangerous state is not "the API request failed." That is usually visible. The dangerous state is "the provider accepted the request, later learned that the recipient was invalid, and the application queued another message before recording the bounce." Delivery reliability is therefore a state-reconciliation problem.

Treat the provider's acceptance response as an acknowledgement, not proof of delivery. Give every logical message an application-generated idempotency key. Store the provider message identifier beside it. Then ingest asynchronous events into a durable log before changing recipient state. This leaves evidence for a replay after a deploy, a database timeout, or an operator mistake.

Order can drift. A delayed delivery event may arrive after a permanent bounce, and a webhook may be delivered more than once. The consumer must make terminal recipient states monotonic: a later informational event cannot reactivate an invalid address. It also needs a uniqueness constraint on the event identifier, because checking for a duplicate and then inserting it in two separate operations leaves a race.

Keep the policy narrow. A permanent invalid-recipient result should suppress future traffic to that address. A transient result should enter a bounded retry path instead. A complaint should suppress immediately. The exact event labels vary by service, so normalize them at the adapter boundary and keep the business states stable.

This is the first gate.

## How can an API-only email service authenticate bounce webhooks and manage suppressions?

Use one transaction, or the closest atomic primitive available, to record the event and apply its recipient-state transition. Authenticate the webhook before parsing it into the internal event type. The verification algorithm, signed fields, timestamp tolerance, and key rotation procedure are service-specific; an adapter should implement those details rather than letting them leak into the suppression policy.

The following Go handler shows the boundary. It deliberately accepts a `Verifier` and a transactional `Ledger` instead of assuming a vendor signature format or endpoint. A production adapter can map its documented payload into `DeliveryEvent`, while the policy remains reviewable and testable.

```go
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type EventKind string

const (
	EventDelivered EventKind = "delivered"
	EventTransient EventKind = "transient_failure"
	EventPermanent EventKind = "permanent_failure"
	EventComplaint EventKind = "complaint"
)

type DeliveryEvent struct {
	ID        string    `json:"id"`
	MessageID string    `json:"message_id"`
	Recipient string    `json:"recipient"`
	Kind      EventKind `json:"kind"`
}

type Verifier interface {
	Verify(header http.Header, body []byte) error
}

type Ledger interface {
	// Apply must atomically deduplicate ID and make terminal suppression monotonic.
	Apply(ctx context.Context, event DeliveryEvent) (duplicate bool, err error)
}

type Handler struct {
	Verifier Verifier
	Ledger   Ledger
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if err := h.Verifier.Verify(r.Header, body); err != nil {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var event DeliveryEvent
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&event); err != nil {
		http.Error(w, "invalid event", http.StatusBadRequest)
		return
	}
	if err := validate(event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = h.Ledger.Apply(r.Context(), event)
	if err != nil {
		// A non-success response asks the sender to retry; no event is acknowledged early.
		http.Error(w, "retry later", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validate(event DeliveryEvent) error {
	if event.ID == "" || event.MessageID == "" || event.Recipient == "" {
		return errors.New("missing event identity")
	}
	switch event.Kind {
	case EventDelivered, EventTransient, EventPermanent, EventComplaint:
		return nil
	default:
		return errors.New("unknown event kind")
	}
}
```

There is an important limitation in this compact example: it caps the body at 1 MiB but does not show timestamp validation, key rotation, address canonicalization, or the database transaction. Those belong in the concrete adapter and ledger. Don't fill them in from intuition. Use the selected service's signed-webhook specification and preserve the raw body, since verifying a re-encoded JSON document can change the signed bytes.

The `503` path is intentional application behavior, not an incident report. The ledger has not committed, so acknowledging the event would lose the state transition. On a duplicate event, `Apply` can return success and the handler can answer `204`; duplicate delivery is harmless after the unique insert and suppression update share one atomic operation.

For alternative webhook designs, compare three service capabilities rather than three logos:

| Event path | Best fit | Operational cost |
| --- | --- | --- |
| Signed push with retries | Dependable inbound HTTPS | Signature rotation and retry-aware idempotency |
| Pull with a cursor | Restricted inbound traffic | Cursor durability and polling delay |
| Export to an owned queue | A team already operates the queue | Retention policy and consumer-lag monitoring |

I'm not sure which is operationally smallest for a given team until its ingress rules, queue ownership, and recovery-time target are known.

## Test SPF, DKIM, and recipient transitions before rollout

Custom domain setup should be an acceptance test, not a screenshot of DNS records. SPF publishes which systems may use a domain in the SMTP `MAIL FROM` or `HELO` identity, and RFC 7208 defines evaluation behavior, including a limit on terms that cause DNS queries [1]. Keep the record reviewable. An SPF configuration assembled through layers of includes can cross that processing limit even though each fragment looks reasonable in isolation. DKIM needs the same evidence-driven treatment: send a real message through the exact production path and inspect the receiver's authentication result. A control-panel "verified" badge shows that the service observed a DNS value; it does not prove that the message stream your application will use produces the expected result at a receiver. Use a dedicated subdomain when transactional student mail needs policy and reputation isolation from campus newsletters or staff mail. Before enabling a cohort, run a small matrix through the whole path: a controlled valid mailbox, a documented test recipient that produces a permanent bounce, a transient-failure fixture if the service provides one, and a duplicate delivery of the same webhook payload. Confirm that the valid mailbox stays eligible, the permanent failure becomes suppressed, the transient event does not become a permanent suppression, and the duplicate changes nothing. Test a complaint fixture only through an officially documented sandbox mechanism; generating a real complaint is the wrong test.

Then test the send-side gate. The queue worker must consult suppression state immediately before submission, not only when a campaign is assembled. There can be minutes or hours between those moments. Record a machine-readable reason, source event identifier, and transition time for every suppression, while keeping access to recipient data restricted. API credentials and webhook verification secrets also need controlled storage and rotation; NIST's authenticator guidance is a useful baseline for thinking about verifier compromise and secret handling, though the service's own signing documentation remains authoritative for its webhook scheme [2].

One subtle policy choice deserves an explicit owner: can a user reverse a permanent-bounce suppression after correcting an address? For an unchanged address, automatic removal is unsafe. For a changed address, create a new recipient identity or require a deliberate verified update rather than mutating away the audit trail. Your mileage may vary for institutional addresses that are recycled, so set a retention period with privacy and support teams instead of retaining delivery events forever.

## Plan migration and rollback for the event ledger

Deploy the consumer dark first. It should authenticate, parse, deduplicate, and record events while enforcement remains disabled. Compare its normalized counts with the service's event view, then turn on suppression checks for an internal tenant or a small course cohort. The key health signals are webhook age, unprocessed-event count, signature rejection count, deduplication count, and the time from a permanent event to an enforced suppression.

Page on lost progress, not raw noise. A brief rise in duplicate events is evidence about delivery behavior; growing age of the oldest unprocessed event means recipient state is becoming stale. Alert thresholds need to follow the promised recovery time and normal traffic, so there is no defensible universal number here.

Rollback must disable new enforcement without deleting the ledger. Never replay outbound messages as part of rolling back the event consumer. If a parser release is suspect, route new payloads into the durable raw-event store, restore the previous adapter, and replay only the inbound events by their event identifiers. Existing terminal suppressions remain in force until an authorized policy process changes them.

Never repeat outbound submission.

## Preserve developer experience after delivery is proven

The selection decision can now be made with evidence. Reject a service if it cannot authenticate events, retry them or expose a recoverable event stream, provide stable message and event identifiers, and support SPF and DKIM verification for the intended sending domain. Among the remaining options, choose the one whose failure and replay model fits the team's existing operations. Ask the engineers who will carry the pager to walk through key rotation, event replay, suppression appeals, privacy deletion, and a parser rollback; an apparently simple API can still create a wide runbook and an awkward local development loop. The shortest send call is irrelevant if a permanent bounce can outrun the suppression update.

## References

1. RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
2. NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
