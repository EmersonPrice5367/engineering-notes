# EdTech Reminder Queue Recovery: Idempotency for At-Least-Once Retries

Short answer: treat a repeated user-reminder message as a normal at-least-once replay, and make the send decision durable before acknowledging the queue delivery. The recovery unit is a business key such as `user_id`, `reminder_id`, and the scheduled instant, not the delivery receipt.

I care about this because missed jobs and duplicate deliveries page the same person for opposite reasons. A worker that acknowledges too early loses work. A worker that blindly retries an uncertain send can notify a student twice. The practical target is narrower than exactly-once magic: one durable decision for one reminder, with an explicit path for an uncertain downstream result.

## The incident lesson: a delivery is not a reminder

The failure usually starts with an innocent sequence. The worker receives a message, calls an email or SMS adapter, and then loses its process before the acknowledgment reaches the queue. The queue redelivers. That replay is useful when the process stopped before the external call; it is dangerous when the external system accepted the call first.

I've been paged for both shapes of incident. The queue was doing what an at-least-once contract permits, while our business identity was attached to the wrong thing. A delivery ID can change on replay. A reminder identity should not.

The invariant belongs in storage: for a given user, reminder, and scheduled time, the ledger may have one send record. The record needs more than a boolean. A `pending` decision means a worker began work; `sent` means the downstream operation was confirmed. If a worker dies in between, the retry must follow a lease or reconciliation rule. Silently treating `pending` as `done` prevents duplicates by creating missed reminders.

That distinction is the postmortem.

Start there.

## What should a Node.js reminder worker do when a queue retries the same message?

The Node.js question is really a protocol question. The same ordering applies in any language: validate the business identity, derive one stable key, claim it with a uniqueness constraint, pass that key to a downstream API that supports idempotency when available, record the result, and acknowledge last. A local map is not enough; it disappears on restart and cannot arbitrate between replicas.

The sender and ledger interfaces below are intentionally generic. The queue adapter is outside the critical section and calls `Ack` only after `Handle` returns nil. The example is Go because this repository's article contract requires Go code; a Node.js worker should implement the same states and ordering.

```go
package reminder

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Reminder struct {
	UserID     string
	ReminderID string
	Scheduled  time.Time
	Body       string
}

type Decision string

const (
	Send       Decision = "send"
	AlreadySent Decision = "already_sent"
	RetryLater  Decision = "retry_later"
)

type Ledger interface {
	Begin(context.Context, string) (Decision, error)
	MarkSent(context.Context, string) error
}

type Sender interface {
	Send(context.Context, string, Reminder) error
}

func sendKey(secret []byte, r Reminder) (string, error) {
	if r.UserID == "" || r.ReminderID == "" || r.Scheduled.IsZero() {
		return "", errors.New("missing reminder identity")
	}

	h := hmac.New(sha256.New, secret)
	h.Write([]byte(r.UserID))
	h.Write([]byte{0})
	h.Write([]byte(r.ReminderID))
	h.Write([]byte{0})
	h.Write([]byte(r.Scheduled.UTC().Format(time.RFC3339Nano)))
	return hex.EncodeToString(h.Sum(nil)), nil
}

func Handle(ctx context.Context, secret []byte, ledger Ledger, sender Sender, r Reminder) error {
	key, err := sendKey(secret, r)
	if err != nil {
		return err
	}

	decision, err := ledger.Begin(ctx, key)
	if err != nil {
		return err
	}
	if decision == AlreadySent {
		return nil
	}
	if decision == RetryLater {
		return errors.New("send lease is still active")
	}

	if err := sender.Send(ctx, key, r); err != nil {
		return err
	}
	return ledger.MarkSent(ctx, key)
}
```

`Begin` must be atomic. In a relational store, enforce uniqueness on the send key and update a lease in the same transaction; the exact SQL depends on the chosen store. The sender must treat `key` as its idempotency key. HMAC is useful here for deriving a stable, non-readable identifier from the fields, and RFC 2104 defines the construction; it does not provide delivery semantics by itself.

There are two retry windows to test. If the process stops before `Send`, an expired lease lets another worker take the work. If the provider accepted `Send` but the process stopped before `MarkSent`, a provider-side idempotency contract can safely answer the same operation again. Without that contract, the ledger needs an outbox and a reconciliation job that checks the provider's result before retrying. That reconciliation should have a bounded decision record of its own: which provider request was attempted, when the response became uncertain, what evidence was found later, and who or what released the reminder for another attempt. Otherwise a well-intentioned repair script becomes a second sender with no durable explanation for its choice. I've seen enough recovery work start from incomplete logs to treat that record as part of the feature, not an afterthought. Never label this cross-system boundary exactly once without evidence.

## Choosing the recovery boundary

The queue is responsible for transporting work and making failed acknowledgments eligible for replay. The application is responsible for deciding whether a replay is new business work. The notification provider, when it offers one, is responsible for recognizing a repeated idempotency key. These are separate contracts and should be monitored separately.

Keep the message small: identity, scheduled time, and the data needed to reconstruct the send. Store the full body in durable application storage when it can change or contain sensitive content. Keep the key and outcome in logs, but do not log the reminder body. This gives an operator enough evidence to explain a duplicate without turning the runbook into a data leak.

The recovery table should answer one question per state:

| State | Meaning | Retry action |
|---|---|---|
| `new` | No send decision exists | Create a leased decision |
| `pending` | A worker may be in the external call | Wait for lease expiry or reconcile |
| `sent` | The logical send is confirmed | Acknowledge as a no-op |
| `failed` | The attempt ended before confirmation | Retry under bounded backoff |

The catch is that this design is not suitable when the notification provider cannot identify repeated requests and the business cannot tolerate a possible duplicate. In that case, choose a human reconciliation step or a provider with an idempotency contract instead of pretending the queue can solve it. Stick with a simpler best-effort retry when a reminder is disposable and duplicate delivery is acceptable; the ledger adds operational state that such a product may not need. I'm not sure a generic queue comparison can settle that trade-off: the answer depends on what evidence the on-call team can obtain after a provider timeout.

## Tests and runbook signals

Test the crash points, not just the happy path: after `Begin`, during `Send`, after the provider returns, and before acknowledgment. Run two consumers against one reminder. Replay the same message after a worker restart. Assert that `sent` produces no second call, that an expired `pending` lease can recover, and that an uncertain provider result goes to reconciliation rather than an automatic duplicate.

Watch queue age, delivery attempts, lease expirations, `already_sent` decisions, failed sends, and confirmed sends. A high replay count is not automatically an incident. Two confirmed sends for one business key is.

The operational rule is short: acknowledge only after the durable send decision. That rule will not make an external provider transactional with your database, but it keeps the failure boundary visible and recoverable.

## Further reading

- [RFC 2104: HMAC keyed-hashing for message authentication](https://www.rfc-editor.org/rfc/rfc2104)
- [Google Cloud Pub/Sub overview](https://cloud.google.com/pubsub/docs/overview)
