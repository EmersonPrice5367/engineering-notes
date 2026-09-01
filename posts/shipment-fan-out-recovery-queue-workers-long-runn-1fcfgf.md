# Shipment Fan-Out Recovery: Queue Workers, Long-Running Jobs, and a 15-Minute Cron Budget

Short answer: treat a shipment fan-out as a recovery problem, not a longer cron request. Have the cron trigger create an idempotent run and enqueue bounded batches; have workers checkpoint subscriber outcomes so a timeout can be retried without sending an accidental second update.

I've been paged by missed jobs and duplicate deliveries, and the symptom is often misleading: the scheduler says it fired, while the queue and the business record disagree. For a media shipment update, the first design question is therefore not “how fast can one worker send?” It is “what can I prove after the worker disappears?”

## What should the operator prove after a shipment fan-out times out?

Start with the evidence model. A run record identifies the shipment and the intended schedule. A batch record identifies a stable slice of subscribers. A delivery ledger identifies the outcome for one subscriber. Those are different records because “the trigger ran,” “the worker claimed work,” and “the subscriber accepted the update” are different events.

The dangerous interval is easy to reproduce: a worker sends an update, the subscriber accepts it, and the worker loses its connection before acknowledging the queue message. The queue redelivers. Without a ledger key, the second attempt looks new. With a key such as `run_id:batch_no:subscriber_id`, the consumer can look up the prior result or call an operation designed to be idempotent. This is the part I would put in a postmortem first, because a successful response from the queue does not prove that every downstream side effect happened once.

The minimum runbook check is a count comparison:

| Evidence | Question | Escalate when |
| --- | --- | --- |
| Trigger history | Did the scheduler invoke the endpoint? | No invocation in the expected window |
| Run and batch rows | Was work admitted and split? | A trigger exists without a run, or a run has no batch |
| Delivery ledger | Which subscribers reached a terminal state? | A batch is marked complete without terminal subscriber rows |
| Queue age and worker heartbeat | Is work progressing now? | Age or heartbeat is stale while work remains |

Inspect state first.

That is the recovery boundary.

## How should a cron trigger hand off long-running queue jobs to workers?

Use the cron request as a small control-plane operation. It should authenticate the caller, create or find the shipment run, write the first outbox entry, and return. It should not enumerate every subscriber or wait for delivery. If the platform gives the trigger a 15-minute execution budget, the scheduled request must finish well inside that budget; the queue worker owns the variable runtime.

Split using a durable cursor or batch number. A message can carry `shipment_id`, `run_id`, `batch_no`, and `after`, with the subscriber page size kept bounded by downstream rate limits. The database enforces uniqueness on `(run_id, batch_no)`, and the ledger enforces uniqueness on the subscriber delivery key. A retry then resumes from recorded state instead of guessing where the previous process stopped.

This applies to a Node.js service even though the example below is Go: the important contract is the state transition, not the language. The publisher interface hides the queue so the trigger handler can be tested with a fake, while the outbox keeps a process exit from turning “publish maybe happened” into an operator mystery. If the process exits after the database commits the batch but before the publisher sees it, the outbox scanner must publish the same identity again; if it exits after publication, the scanner must still publish that identity again. Both paths are normal, and the consumer ledger is what makes them converge instead of producing a second subscriber notification.

```go
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Batch struct {
	ShipmentID string
	RunID      string
	BatchNo    int
	After      string
	Limit      int
}

type Publisher interface {
	Publish(context.Context, Batch) error
}

func sign(secret, timestamp, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

func enqueueNext(ctx context.Context, p Publisher, shipmentID, runID, after string, batchNo int) error {
	if shipmentID == "" || runID == "" {
		return fmt.Errorf("shipment and run identifiers are required")
	}
	if batchNo < 0 {
		return fmt.Errorf("batch number cannot be negative")
	}
	return p.Publish(ctx, Batch{
		ShipmentID: shipmentID,
		RunID:      runID,
		BatchNo:    batchNo,
		After:      after,
		Limit:      250,
	})
}
```

The signature only authenticates a body; it does not stop replay by itself. Reject an old timestamp, compare the received digest in constant time, and record the accepted request identity. RFC 2104 defines the HMAC construction, not your replay window or your business transaction. That distinction matters when a trigger is retried after an HTTP 401 or after a network timeout: the response tells you what the endpoint observed, not what a downstream consumer completed. It's a trade-off: the endpoint gets a small security contract, while the run database must carry the larger operational truth.

## Which recovery boundary fits the failure you can tolerate?

Choose the boundary that an operator can inspect and repair. A queue is a good fit for independent subscriber notifications. It is a poor fit for a workflow whose correctness depends on joins, human approval, or compensation across several services.

| Recovery boundary | Best fit | Main cost |
| --- | --- | --- |
| Cursor plus queue batch | Independent shipment notifications | You must own idempotency and progress records |
| Transactional outbox | Reliable handoff from the run database | Extra rows and a publisher loop to operate |
| Workflow-oriented engine | Joins, approvals, compensation, long-lived state | More state machinery and operational concepts |
| Direct scheduled request | Small, bounded, repeatable work | A timeout couples scheduling to delivery |

The 15-minute limit is not a target. A batch that usually finishes in fourteen minutes has no useful recovery margin once subscriber latency, retries, and deployment events are included. Shrink the batch until its worst ordinary path is bounded, then let the next cursor become a new message. The trade-off is operational overhead: smaller batches create more queue entries, progress rows, and metrics to retain, while larger batches make an ambiguous timeout more expensive to investigate. Your mileage may vary on the exact size; I am not sure one threshold fits every catalog.

## What should rollout and rollback preserve?

Before enabling the schedule, send a test shipment to a controlled subscriber set. Verify the trigger signature, the timestamp window, one run identity, bounded batch creation, and a quick response. An invalid signature should produce HTTP 401 and no run row. Replay the same trigger. The expected result is a known duplicate decision, not a second fan-out.

During rollout, stop a worker after the downstream provider accepts one notification but before queue acknowledgement. Redeliver the message and confirm one ledger outcome. Also test a worker restart after a checkpoint, a provider timeout, and an empty final cursor. These are small tests, but they exercise the exact gaps that produce duplicate deliveries.

Watch trigger history, queue age, worker heartbeat age, batch state, and ledger state together. A green trigger with no run row is an admission failure. A growing queue with fresh workers is likely downstream pressure. A complete batch with missing ledger rows is a data-integrity incident. Those interpretations are more useful than a single “job succeeded” metric.

Rollback starts by pausing the schedule and preserving run, batch, outbox, and ledger records. Decide whether queued messages match the old consumer contract; drain valid work, quarantine incompatible work, and deploy the prior reader or a compatibility path. Resume from recorded cursors only after the downstream dependency is healthy. Don't bulk-replay the shipment because the trigger was retried.

## When is this pattern unsuitable?

It is not suitable when the subscriber update must be atomic across the entire audience, when a batch cannot be bounded, or when the public trigger cannot reach an authenticated gateway. In those cases, add a durable data exchange or choose a workflow boundary that expresses the required transaction; raising a timeout merely hides the failure until a larger shipment arrives.

The pattern is also a bad match for a dependency graph with joins and compensating actions. Use a workflow-oriented system there, and keep the queue for independent work inside that workflow. Stick with a direct scheduled request only when the work is small and bounded; otherwise the trade-off is coupling the scheduler to delivery. A runbook that says “retry everything” is not a recovery strategy.

## References

- RFC 2104, HMAC keyed-hashing for message authentication: https://www.rfc-editor.org/rfc/rfc2104
- Cloudflare Workers Cron Triggers documentation: https://developers.cloudflare.com/workers/configuration/cron-triggers/
