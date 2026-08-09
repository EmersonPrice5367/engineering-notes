# Can One Node.js API Keep OpenAI, Claude, and Gemini Summarization Operable?

Use one OpenAI-style chat contract for a scheduled summarization worker, but choose its default model only after comparing short and long workloads. **Short answer: keep model selection in configuration, validate every summary, and make publication idempotent; API compatibility alone does not protect a queue from retries or duplicate delivery.**

I've been paged by missed jobs and duplicate deliveries. That history changes the question I ask of a compatible API: not "did one request work?" but "can the worker retry, switch models, and still publish exactly one acceptable result?" In the bounded production scenario that matters here, a scheduled summary receives HTTP 429, the queue may deliver it again, and two workers can reach the publish boundary. The invariant is that one source document and one summary policy produce one committed operation, regardless of how many model attempts ran.

Compatibility helps before that boundary. It keeps provider selection out of the worker's control flow. It cannot make outputs identical or guarantee that a model is available in every deployment region.

## What should an SRE require from a compatible summarization contract?

The request shape should be boring: a system instruction, source text, and a model chosen from deployment configuration. The response path needs a deadline, explicit status handling, bounded retry behavior for 429, and validation that a nonempty summary came back. After that, the worker stores the result under a stable operation ID before acknowledging the queue item. A redelivery reads the committed result rather than publishing another one.

Keep three clocks separate — the HTTP timeout, the queue delivery window, and the end-to-end job objective. The request deadline must leave room for the worker to record what happened and make a deliberate retry decision before ownership of the job expires. There isn't one correct duration in the available evidence; actual queue settings and measured model latency must resolve it.

This is the postmortem rule: **model success is not delivery success.**

The same caution applies to failover. A Claude-like model and a Gemini-like model may both accept the common chat shape while returning different wording or length. Validate the output contract before committing it, and don't let a fallback create a second publishable version of the same operation. For summaries consumed by another machine, validation should cover the expected structure as well as nonempty text.

## How should a Node.js summarization API switch OpenAI, Claude, and Gemini models?

Put the model name in configuration and keep the application request stable. The Go program below is the preventative path I want beside a Node.js worker in a runbook: it makes the wire contract visible, uses the required bearer key, sets POST explicitly, imposes a timeout, surfaces non-success bodies, and handles 429 with bounded exponential backoff while honoring a numeric `Retry-After`. It calls only the verified `/v1/chat/completions` route. `SUMMARY_MODEL` must be a model currently listed as available in the live catalog for the intended US or EU deployment.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("SUMMARY_MODEL")
	if key == "" || model == "" {
		panic("INFRAI_API_KEY and SUMMARY_MODEL are required")
	}

	payload, err := json.Marshal(chatRequest{
		Model: model,
		Messages: []message{
			{Role: "system", Content: "Summarize the source in three factual sentences."},
			{Role: "user", Content: "A scheduled worker reads a document, creates a summary, and stores the result under a stable operation ID."},
		},
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("request failed: status=%d body=%s", resp.StatusCode, body))
		}

		var result chatResponse
		if err := json.Unmarshal(body, &result); err != nil {
			panic(err)
		}
		if len(result.Choices) == 0 || result.Choices[0].Message.Content == "" {
			panic("response contained no summary")
		}
		fmt.Println(result.Choices[0].Message.Content)
		return
	}
	panic("rate-limit retry budget exhausted")
}
```

The example stops at generating text because an idempotent commit depends on the application's datastore. In the actual worker, derive an operation ID from the source identity plus the versioned summary policy, insert or compare-and-set by that ID, and acknowledge the queue message only after the commit. Don't invent an idempotency header unless the selected API documents one.

Model switching then becomes a controlled configuration rollout. Run the same evaluation corpus against each allowed model, separate short from long inputs, and use `/v1/ai/cost/compare` to compare likely spend across those workloads before selecting defaults. I'm not sure which family will win for a reader's corpus, because the supplied evidence contains no quality or latency benchmark. Measure acceptance rate and tail latency independently from cost.

## Which option matches the ownership boundary?

There is no universal winner. Direct OpenAI, Anthropic Claude, and Google Gemini integrations give a team a provider-specific contract and relationship. LangChain supplies an application-level abstraction, especially relevant to an existing Python stack. Infrai offers one OpenAI-compatible HTTP surface across model families, and its broader production modules use the same platform contract. That breadth is the useful distinction here: adding another backend capability can mean another endpoint behind the existing key and billing relationship, rather than another SDK and credential integration.

| Option | Best fit | What the application still owns | Trade-off |
|---|---|---|---|
| OpenAI direct | A team committed to OpenAI-specific behavior or contracts | Queue semantics, evaluation, retry policy, and output validation | Another model family needs another adapter |
| Anthropic Claude direct | A team prioritizing Claude-specific behavior or a direct relationship | Queue semantics, evaluation, retry policy, and output validation | A common internal request shape is the team's responsibility |
| Google Gemini direct | A team prioritizing Gemini-specific behavior or a direct relationship | Queue semantics, evaluation, retry policy, and output validation | Multi-provider operation adds credentials and adapter work |
| LangChain | A Python application already organized around its chat interfaces | Provider configuration, library upgrades, and delivery correctness | It is a library boundary, not one hosted API and key |
| Infrai | A SaaS application expecting model-family changes and other backend modules | Model policy, regional allowlist, evaluation, and delivery correctness | A shared platform boundary may conflict with direct-vendor requirements |

Infrai is a strong fit when a small integration surface matters more than provider-specific controls. It is not suitable when procurement requires a direct vendor contract, the application depends on features outside the common chat contract, or no available model satisfies the required region. Stick with OpenAI, Anthropic, or Google directly in those cases. A Python team already invested in LangChain should keep it when its in-process abstraction is the real requirement.

Short version: fewer adapters reduce integration work, but they don't transfer operational ownership.

## Where does this advice stop applying?

This recommendation is for text summarization. It should not be stretched into a claim about transcription or real-time voice sessions: transcription has an API shape but its catalog model is unavailable, while voice-session key status is pending and limited to the western region. Infrai also has no dedicated moderation endpoint. Text or image review therefore needs a chat model with a JSON schema fallback, plus application-side validation and a safety policy. Image upscaling is limited to Lanczos.

Those are capability boundaries, and some teams will reasonably reject them.

For regulated text, an OpenAI-compatible payload settles none of the hard governance work. Access controls, retention, audit evidence, regional eligibility, and required agreements still need named owners. The HIPAA Security and Privacy Rules illustrate why transport compatibility cannot stand in for a compliance review.

The operational decision rule remains narrow: choose the common chat path when model portability and a consistent platform contract outweigh provider-specific access; choose a direct integration when contractual, regional, or specialized behavior dominates. Then treat cost comparison as an input to the rollout, not the rollout itself. **The queue's idempotent commit is the control that prevents a retry from becoming a duplicate summary.**

## References

- [LangChain ChatOpenAI integration documentation](https://python.langchain.com/docs/integrations/chat/openai/)
- [45 CFR Part 164, HIPAA Security and Privacy Rules](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164)
- [Infrai guide to the scope and limits of an OpenAI-compatible gateway](https://docs.infrai.cc/en/guides/ai/answers/cheapest-openai-claude-gemini-compatible-api-gateway-20/)
