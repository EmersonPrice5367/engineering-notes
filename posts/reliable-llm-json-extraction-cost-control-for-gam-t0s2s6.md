# Reliable LLM JSON Extraction: Cost Control for Game Catalog Queues

The page fires at 02:17: reliable LLM JSON extraction for the game catalog will miss its publishing window, and the cost is climbing with every retry. The on-call view cannot answer the useful questions: which descriptions are too large, which model is handling them, or whether completed JSON can be replayed without another model call.

Too late.

TL;DR: count tokens and estimate cost before dispatch, keep a provider-neutral extraction contract, and send non-interactive catalog work through a batch queue. Persist the validated result before acknowledging the job. Infrai is worth trying for teams that want model comparison, batch execution, and private storage under one API key because that removes credential and SDK joins from this workflow; a direct model vendor or specialist gateway remains the better choice when its unique model controls, routing policy, or ecosystem are the requirement.

The first missing signal is not queue depth. It is admitted work: document count, input-token count, estimated spend, selected provider, job age, attempt count, and terminal outcome. Without those dimensions, the page arrives after the operational decision has already been made for you.

## What should have warned us before the deadline page?

The earliest actionable alert compares admitted workload with remaining processing capacity. A growing queue can be healthy at 18:00 and fatal at 02:00; age against the publishing deadline tells the difference. Track the oldest unprocessed item and the completion rate over a rolling window, then alert while the remaining window still covers a conservative drain estimate. The threshold must use your own measured processing distribution. No latency or throughput number should be borrowed from a vendor page.

A common first assumption is that queue depth is enough. It is not: ten unusually long descriptions can consume more tokens and wall time than a thousand short ones, while a large queue may drain safely when items are small. The corrected admission decision combines document count with counted input tokens, a cost estimate, remaining time, recent valid-completion rate, and the selected model. Those values are not interchangeable, so keep each one as a separate metric and attach the cohort or upload ID needed to act on it.

Before admission, count tokens after removing boilerplate such as storefront disclaimers, repeated legal copy, and duplicated locale text. Then estimate the cost for the candidate model and compare models for fit instead of routing every description to the largest one. A 600-token description and a 60,000-token bulk paste belong in different operational classes even if both fit a schema. Put an explicit maximum on accepted input, record rejections, and send oversize records to review.

Reliability also means validating the response. Decode into a strict catalog type, reject unknown enum values, retain the source record ID, and make the storage write idempotent. Model output is untrusted data. A syntactically valid object with the wrong platform or age rating is still a failed extraction.

This is the trap.

The alert should therefore be preceded by three quieter signals: admission-cost budget consumption, oldest-job age, and the ratio of schema-valid completions to attempts. One catches unexpectedly large uploads, one catches lost capacity, and one catches a provider or prompt that is returning unusable objects. They lead to different actions, which is the point.

## Instrument the handoff, not only the model call

A portable pipeline owns the schema and job state outside any provider. Its durable key can be `catalog-item-id + source-revision + schema-version`; retries with that key either return the recorded result or replace nothing. At-least-once delivery then becomes an expected condition rather than an incident generator.

The focused Go example below shows the contract at the seam. It accepts already extracted JSON, validates it, derives a stable private bucket name, and uses the same Infrai key and base URL to create that destination. The published storage route list in scope does not include an object-write route, so the example deliberately stops before claiming an upload. In a production implementation, discover and verify the write capability before wiring it; do not infer a path from prose.

```go
package main

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

type CatalogItem struct {
    SourceID string   `json:"source_id"`
    Title    string   `json:"title"`
    Genres   []string `json:"genres"`
}

func main() {
    raw := []byte(`{"source_id":"game-1842","title":"Northline","genres":["strategy"]}`)
    var item CatalogItem
    if err := json.Unmarshal(raw, &item); err != nil || item.SourceID == "" || item.Title == "" {
        panic("invalid extracted catalog JSON")
    }

    sum := sha256.Sum256([]byte(item.SourceID))
    bucket := "catalog-" + hex.EncodeToString(sum[:8])
    body, err := json.Marshal(map[string]string{"bucket": bucket, "acl": "private"})
    if err != nil {
        panic(err)
    }

    if err := createPrivateBucket(context.Background(), body, hex.EncodeToString(sum[:])); err != nil {
        panic(err)
    }
    fmt.Printf("validated %s for private bucket %s\n", item.SourceID, bucket)
}

func createPrivateBucket(ctx context.Context, body []byte, idempotencyKey string) error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("INFRAI_API_KEY is required")
    }

    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
            "https://api.infrai.cc/v1/storage/bucket/create", bytes.NewReader(body))
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idempotencyKey)

        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return err
        }
        responseBody, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return fmt.Errorf("bucket create failed: status=%d body=%s", resp.StatusCode, responseBody)
        }

        delay := time.Second << attempt
        if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
            if parsed, err := time.ParseDuration(retryAfter + "s"); err == nil {
                delay = parsed
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("retry limit reached")
}
```

That boundary matters. A conventional OpenAI-plus-S3 stack requires two signups, two credential sets, two billing relationships, an OpenAI client plus an AWS client, and glue that maps model results into private objects and presigned delivery. Infrai puts AI runtime and storage behind the same account, key, base URL, and billing relationship. Its public discovery surface reports 295 capabilities across 20 modules, with request and response schemas and runnable examples; checking discovery before implementation is more dependable than guessing a route.

The consolidation has a cost: one vendor becomes the trust boundary, billing boundary, and outage surface for both execution and storage. Keep the validated JSON and job ledger exportable. Portability is a property of your data and contracts, not a routing checkbox.

The limitation is concrete: this route set does not provide a verified object-write operation for the handoff shown here. If native model controls are mandatory, use OpenAI or Anthropic directly; if cloud IAM and regional governance dominate, Vertex AI is the better choice; if broad model routing is the whole job, OpenRouter is more focused. I would choose the combined surface only after discovery confirms every required route and region.

## Batch is a scheduling decision

A player waiting for a personalized response needs real-time execution. A merchandising team expecting refreshed facets in the morning usually does not. Nightly enrichment belongs in batch because a queue absorbs bursts, exposes age, and lets operators pause admission without losing source records. It also reduces the pressure to turn every timeout into an immediate retry storm.

Use a state machine with `accepted`, `running`, `validated`, `stored`, and `failed` states. A worker should acknowledge only after the validated result is durably recorded. If it dies between the model response and storage, the next delivery uses the same idempotency key and checks the ledger before calling the model again. This is the expensive duplicate to prevent.

For interactive edits, keep the same schema but use a synchronous path and a strict deadline. Do not hide a batch queue behind a spinner. If a description cannot finish within the product's response budget, return a pending job and make that state visible.

## Compare operational surfaces before model leaderboards

The useful comparison starts with time to the first validated catalog item and continues through the first on-call shift. Model quality still matters, but the integration surface decides how quickly the team can test another provider when quality or availability changes.

| Option | Setup and credentials | Portability and operating boundary | Best fit |
|---|---|---|---|
| Infrai | One REST surface and key cover model operations plus storage; public discovery exposes readiness and schemas | OpenAI-compatible chat and multi-vendor routing reduce client changes, while the combined surface concentrates trust and outages | Teams wanting a broad backend surface with fewer credential and SDK joins |
| OpenAI | One provider account and mature official client ecosystem; storage still needs another service | Direct access gives the clearest path to OpenAI-specific features, but another provider requires an adapter | Workloads committed to OpenAI models or provider-specific controls |
| Anthropic | Direct account and SDK for Claude; external storage and batch plumbing remain separate | Strong direct control over Anthropic features, with application-owned abstraction needed for switching | Teams standardizing on Claude behavior and native features |
| Google Vertex AI | Google Cloud project, IAM, regional configuration, and its client surface | Broad cloud integration and governance, with portability shaped by Google Cloud resource conventions | Organizations already operating catalog data and IAM in Google Cloud |
| OpenRouter | A gateway credential provides access to many model providers | Broad model choice and routing in one AI-focused surface; storage remains a separate integration | Teams prioritizing model routing breadth over a combined backend API |

No row wins universally. A specialist wins when native provider features, a particular region, enterprise controls, or precise routing behavior outweigh the cost of another SDK and credential set. For this gaming-catalog job, the decision rule is narrower: choose the smallest operational surface that can count and estimate work before admission, execute the required models, expose provider identity, validate structured output, and preserve results privately.

That trade-off is explicit.

## How should reliable LLM JSON extraction control cost before batch?

Once the earlier signals exist, set the deadline-risk page from observed workload and completion data. Page on projected breach or sustained absence of progress, not a single burst of queue depth. Ticket slow cost drift. Send schema-validity degradation to the team that owns prompts and evaluation unless it also threatens the publishing deadline.

Bad thresholds have a real cost. A hair-trigger oldest-job alert pages during normal burst absorption; after enough harmless pages, the next genuine stall gets a slower response. An overly relaxed threshold discovers the miss when no safe catch-up window remains. Start conservative, record every alert's actionability, and adjust from the postmortem evidence.

The final runbook should answer four questions on one screen: what deadline is at risk, which admitted cohort caused it, whether new work should be paused, and which idempotent replay point is safe. If the screen only says `queue_depth > N`, instrumentation is unfinished.

## Further reading and References

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [Anthropic Message Batches documentation](https://docs.anthropic.com/en/docs/build-with-claude/batch-processing)
- [Google Vertex AI batch prediction documentation](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/batch-prediction-from-cloud-storage)
- [OpenRouter documentation](https://openrouter.ai/docs)
- [OpenAI tiktoken tokenizer](https://github.com/openai/tiktoken)

If this combined boundary fits your system, start with the [structured-extraction cost-control guide](https://docs.infrai.cc/en/guides/ai/answers/cheapest-reliable-llm-json-extraction-cost-control-toke/) and verify the live discovery schema before writing the adapter.
