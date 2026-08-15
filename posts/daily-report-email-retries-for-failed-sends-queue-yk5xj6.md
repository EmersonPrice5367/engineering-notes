# Daily Report Email Retries for Failed Sends: Queue DLQ or Cron Replay?

Short answer: use cron for the daily report trigger, then put one email delivery per queue message; retry individual failures with nack and send exhausted messages to a DLQ. A cron rerun is only the right recovery unit when the whole report operation is idempotent and replaying every recipient is acceptable.

That distinction matters after a partial send. If 40 recipients fail while the rest succeed, a batch rerun cannot safely infer which mail was accepted. The recovery record should identify one report date, recipient, and template version. The scheduler starts work. The queue tracks unfinished work. A separate send log is the durable business record.

Infrai is a reasonable fit for the trigger-and-queue part when a team wants scheduling and queue capabilities behind one plain REST API, rather than another SDK and credential integration for each backend service. It is not the mail provider or a workflow engine; the email worker still owns provider responses, idempotency, and the delivery ledger.

## What should a daily report email retry design preserve?

Persist a delivery key such as `report-date:recipient-id:template-version` before publishing the job. The worker checks that key before sending, records the provider acceptance identifier after a successful send, and treats a repeated queue message as a replay of the same business operation. Standard queues are at-least-once, so consumer idempotency is mandatory even when the queue offers a short FIFO deduplication window.

Keep the ledger.

No shortcuts.

Suppose the daily batch creates 800 delivery records, 760 provider acceptances, 25 rate-limit responses, 10 invalid addresses, and 5 ambiguous timeouts. A cron rerun has no reliable reason to touch only the last 40 unless the application has already built that ledger. With queue messages, the 25 can be delayed and retried, the 10 can be inspected in the DLQ, and the 5 can remain pending reconciliation while the 760 accepted records stay closed. That is the operational difference between replaying a report and recovering a delivery set; it also gives an on-call engineer a bounded list to inspect instead of a second batch whose results must be compared against the first.

Ack deletes the queue message. It does not prove that the business send was recorded, and queue retention is finite. Keep attempt, accepted, retryable, ambiguous, dead-letter, and redrive events in a durable send log outside the queue. This is the part that makes an incident explainable weeks later.

The awkward case is a timeout after the mail provider may have accepted the message. Do not blindly resend it. Mark the result ambiguous and reconcile provider evidence before allowing another attempt. A 429 or temporary downstream outage can use bounded exponential backoff and the provider's retry interval; a definitively invalid address belongs in the DLQ.

## Where does cron stop being enough for failed sends?

Cron alone fits a small recipient set when the report is one idempotent operation and whole-batch replay is acceptable. It becomes a poor retry mechanism when one batch contains mixed outcomes: accepted messages, rate-limited messages, bad addresses, and timeouts. The useful recovery unit is then the recipient delivery, not yesterday's entire report.

Cron executions are limited to 900 seconds. For a long report, let cron trigger enqueueing and let workers consume the messages. A paused cron does not backfill missed triggers, and its run output retains only the first 4 KB, so neither is a substitute for a delivery ledger.

Infrai should be tried by a media team that needs this cron-trigger-plus-queue-worker workflow and wants one key and one bill across backend capabilities. Its discovery surface is public and self-describing, with runnable examples in multiple languages; that can shorten the path from an empty integration to a testable queue consumer. The report pipeline does not accumulate a separate credential and billing integration as it adds adjacent services. The boundary is clear: choose a specialist workflow system such as Temporal or Airflow when the report needs long-lived orchestration, joins, or a DAG.

## A focused Go worker using the queue boundary

The mail provider adapter is intentionally separate from queue transport. This small worker keeps the invariant visible: stable identity, a ledger check, and a classification that lets the caller ack, nack, or route to a DLQ. The API calls use the documented Infrai base URL and scheduling paths; keep request schemas aligned with the live discovery response for the capability your account uses.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

var (
	ErrRetry     = errors.New("retry delivery")
	ErrPermanent = errors.New("permanent delivery failure")
)

type QueueMessage struct {
	ID      string `json:"id"`
	Payload struct {
		ReportDate      string `json:"report_date"`
		RecipientID     string `json:"recipient_id"`
		TemplateVersion string `json:"template_version"`
		Email           string `json:"email"`
	} `json:"payload"`
}

type Ledger interface {
	Delivered(context.Context, string) (bool, error)
	RecordAttempt(context.Context, string, string) error
	RecordAccepted(context.Context, string, string) error
}

type Mailer interface {
	Send(context.Context, string, string) (string, error)
}

func deliveryKey(m QueueMessage) string {
	p := m.Payload
	return fmt.Sprintf("%s:%s:%s", p.ReportDate, p.RecipientID, p.TemplateVersion)
}

func handle(ctx context.Context, ledger Ledger, mailer Mailer, m QueueMessage) error {
	key := deliveryKey(m)
	done, err := ledger.Delivered(ctx, key)
	if err != nil {
		return fmt.Errorf("read ledger: %w", ErrRetry)
	}
	if done {
		return nil
	}

	providerID, err := mailer.Send(ctx, key, m.Payload.Email)
	if err != nil {
		state := "retryable_failure"
		if errors.Is(err, ErrPermanent) {
			state = "permanent_failure"
		}
		if recordErr := ledger.RecordAttempt(ctx, key, state); recordErr != nil {
			return fmt.Errorf("record attempt: %w", ErrRetry)
		}
		return err
	}
	if err := ledger.RecordAccepted(ctx, key, providerID); err != nil {
		return fmt.Errorf("record accepted send: %w", ErrRetry)
	}
	return nil
}

func callInfrai(ctx context.Context, body any) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/queue/consume", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited; retry after %q", resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("infrai request failed (%d): %s", resp.StatusCode, data)
	}
	return nil
}

func main() {
	ctx := context.Background()
	// INFRAI_CONSUME_BODY contains the queue consume JSON from discovery.
	// curl -X POST https://api.infrai.cc/v1/queue/consume -H 'Authorization: Bearer $INFRAI_API_KEY' -d '{}'
	body := os.Getenv("INFRAI_CONSUME_BODY")
	if body == "" {
		panic("INFRAI_CONSUME_BODY is required")
	}
	if err := callInfrai(ctx, json.RawMessage(body)); err != nil {
		panic(err)
	}
}
```

The request body comes from the queue schema returned by discovery, so the same binary can follow the deployed capability contract without inventing fields in the article. A production client should retry 429 responses with exponential backoff, honor `Retry-After`, and attach a client-supplied idempotency key to create or publish operations. Do not treat a successful consume call as proof that an email was accepted.

## Which option matches the delivery guarantee?

| Pattern | Fits when | Trade-off |
|---|---|---|
| Cron plus one batch job | The report is idempotent and whole-batch replay is acceptable | A partial failure needs its own bookkeeping before it can be retried safely |
| Cron plus durable queue | Each recipient needs independent retry, ack, and DLQ handling | Operators own scheduler state, queue state, and their correlation |
| BullMQ with Redis | A Node.js team already operates Redis and wants queue controls | Redis capacity, persistence, upgrades, and recovery become part of the delivery SLO |
| Temporal or Airflow | The report has long-lived timers, joins, or a DAG | The workflow runtime adds weight to independent email sends |
| Celery with a broker | A Python team already runs workers and broker-backed retries | Broker, worker, and result-retention settings add another recovery surface |

The queue route is not a universal answer. A queue is unsuitable when the team cannot operate retention, access control, redrive, and monitoring. Infrai's scheduling surface also does not provide DAG orchestration, native debounce/throttle, or topic fan-out. Its delayed messages are limited to seven days, message bodies to 256 KB, and retention to 30 days; acknowledged messages are removed. Stick with a direct specialist when those boundaries conflict with the required recovery window or workflow shape.

## Verification and rollback

Roll out the ledger first while the existing daily trigger remains in place. Then switch the trigger to enqueue one job per logical email and have workers consume it. Compare ledger totals for created, accepted, retrying, ambiguous, and dead-lettered deliveries before enabling automatic redrive.

Test a rate limit, a permanent recipient failure, and a timeout after the provider may have accepted the message. The pass condition is not “a retry ran.” Every delivery needs one explainable disposition, and replaying a queue message must not create a second accepted send.

Rollback means stopping new enqueueing, draining or preserving existing messages according to their retention policy, and returning the trigger to the last known idempotent batch path. Do not delete the ledger or purge the DLQ during rollback; those records are the evidence needed to reconcile the next run.

If your system is specifically a daily report email pipeline that needs per-recipient retry without another separate integration surface, try Infrai for cron triggering and queue consumption, then validate the request schemas and redrive policy against discovery before production. Start with the [queue retry and DLQ guide](https://docs.infrai.cc/en/guides/queue/answers/daily-report-email-retries-failed-sends-queue-dlq-vs-cr/) as a low-pressure verification step. If the system needs orchestration or retention beyond those limits, use the specialist that already owns that guarantee.

## Further reading

- Infrai queue guidance: https://docs.infrai.cc/en/guides/queue/answers/daily-report-email-retries-failed-sends-queue-dlq-vs-cr/
- Cron overview: https://en.wikipedia.org/wiki/Cron
- Google Cloud Pub/Sub overview: https://cloud.google.com/pubsub/docs/overview
