# Private Marketplace Upload Erasure: Node.js Multipart Images Before Sharp Commits

The page says `marketplace_media_delete_backlog_high`: 18,420 original images are past their deletion deadline, while the thumbnail queue looks healthy. Sellers can still upload, buyers can still browse, and no request latency graph is red. That is exactly why this page matters. The system is retaining private media it promised to remove, and a green resize pipeline does not make that acceptable.

Short answer: let the browser send large images directly to private S3-compatible storage with multipart upload, let Node.js issue short-lived upload intent and record a retention lease, and run Sharp only after a verified completion event; deletion must be a separately observed state machine covering originals, thumbnails, abandoned parts, and late queue deliveries.

The least complex design is not a byte-proxying Node.js endpoint. It is a control plane. Node.js decides who may upload, the permitted object key, content constraints, and the retention deadline. Storage carries the bytes. A worker reads the completed private original, Sharp creates bounded derivatives, and a deletion reconciler proves that every object derived from the listing reaches the same terminal policy. OWASP's file-upload guidance supports the important boundary here: validate extension and type, generate storage names, impose size limits, authorize the uploader, and keep uploaded files outside the webroot rather than trusting the client's filename or `Content-Type` header.

## How can a Node.js multipart image upload feed the Sharp thumbnail pipeline?

Treat the upload as a workflow with durable identities, not one long request. The browser first asks Node.js for an upload intent. The application authenticates the seller, allocates an opaque `asset_id`, chooses a private key under the marketplace tenant, and stores a row whose state is `uploading`. It also records `retain_until`; that timestamp is policy, not a hint. The browser then uploads parts directly to the object store using narrowly scoped, expiring authorization. After the client reports completion, the application verifies the completed object before changing the row to `ready_for_scan` or publishing a thumbnail job.

Direct upload removes application memory and connection duration from the data path, but it doesn't remove application responsibility. The coordinator still needs an allowlist for accepted formats, a decoded-pixel limit, a maximum encoded size, authorization on every transition, and a generated object name. Sharp should read from a private object and write private derivatives; public delivery, when required, belongs behind a separate authorization or signed-delivery decision. A file that merely has a `.jpg` suffix has proved nothing. Completion is the commit point. Before it, parts can exist without a usable object; after it, the original can become input to scanning and resizing. Persist the multipart upload identifier with the asset row so an expiry worker can abort abandoned sessions. Do not enqueue Sharp from a browser callback alone — the callback can be duplicated, delayed, or sent before the application has performed its own completion checks. The storage documentation for DigitalOcean Spaces describes S3 compatibility and multipart transfers, but compatibility does not settle your retention policy. Your database still has to say which original, derivatives, and incomplete upload belong to one marketplace asset. Use an outbox or an equivalent transactional handoff between the asset state change and the thumbnail job. The worker's idempotency key can be `asset_id + source_version + profile_version`; the destination key should be deterministic for that tuple. A retry then converges on the same derivative instead of accumulating `thumb-final-2.jpg`. It also prevents a late retry from silently recreating an image after deletion: the worker checks the lease and a deletion marker before reading the original and again before publishing the result.

Small rule: deletion wins.

## Reconstruct the deletion incident timeline

The first useful signal was not the backlog page. It was the age of the oldest asset whose `retain_until` had passed but whose deletion state had not reached `deleted`. Queue depth is an indirect measure because a quiet queue can mean the producer stopped, the scheduler missed its run, or all work was acknowledged without achieving the intended object state. For retention, observe policy age and reconcile it against storage. Page on an outcome.

Page the promise.

I initially reach for queue metrics because I've been paged by missed jobs and duplicate deliveries. They are valuable, but they answer transport questions: did work wait, run, retry, or dead-letter? They don't answer the marketplace question: can any private byte associated with this listing still be fetched from storage after its deadline? The better alert joins the control-plane row to a periodic inventory or targeted object check, then measures overdue age and count by policy class. One signal tells the on-call the customer obligation at risk; queue, worker, and storage metrics explain why.

Instrument every transition with `asset_id`, tenant, policy class, source version, job attempt, and a low-cardinality result. Record timestamps for intent created, multipart completed, validation passed, derivative committed, deletion requested, and deletion verified. Keep raw object keys out of broad metrics labels; put them in access-controlled logs or traces. A useful trace for the opening page would show that deletion intents continued, acknowledgements arrived, but verification stopped advancing. That points to the broken handoff without requiring the on-call to infer truth from a single queue chart.

The alert should carry a runbook query and three numbers: oldest overdue age, overdue asset count, and the last successful verification time. Those values are operational choices, not universal thresholds. I'm not sure a one-hour page fits every marketplace; the right threshold depends on the stated deletion promise, normal reconciliation interval, and how quickly an operator can safely drain work. Measure that distribution in a staging replay and then in production before promoting the alert from a ticket to a page.

## A lease checker code example

Model the asset family explicitly. An original can have several thumbnails, a moderation copy, and incomplete multipart state. A listing deletion, account erasure, policy expiry, or superseding upload may all request removal, but they should converge on one tombstone. The tombstone blocks new derivative commits and gives retries a durable fact to consult. Without it, a resize job that was leased before deletion can finish afterward and put a thumbnail back.

Trust the tombstone.

This Go example is the policy core I would keep independent of the queue and object-store clients. The production Node.js coordinator can enforce the same transitions; keeping the state decision pure makes it easy to exercise with delayed and duplicate events. The example uses a concrete marketplace asset and a 24-hour sample lease solely to make the boundary visible, not to prescribe a universal retention period.

```go
package retention

import (
	"errors"
	"time"
)

type State string

const (
	Uploading State = "uploading"
	Ready     State = "ready"
	Deleting  State = "deleting"
	Deleted   State = "deleted"
)

type Asset struct {
	ID            string
	State         State
	RetainUntil   time.Time
	SourceVersion string
	DeleteToken   string
}

func (a Asset) CanPublishDerivative(now time.Time, sourceVersion string) error {
	if a.State != Ready {
		return errors.New("asset is not ready")
	}
	if !now.Before(a.RetainUntil) {
		return errors.New("retention lease has expired")
	}
	if sourceVersion != a.SourceVersion {
		return errors.New("source version is stale")
	}
	return nil
}

func (a Asset) RequestDeletion(now time.Time, token string) Asset {
	if a.State == Deleted || a.State == Deleting {
		return a
	}
	if !now.Before(a.RetainUntil) {
		a.State = Deleting
		a.DeleteToken = token
	}
	return a
}
```

The delete worker lists the expected keys from durable metadata, requests deletion for the original and every derivative, aborts any recorded incomplete multipart upload, and then verifies absence before marking the family `deleted`. A missing object is success for an idempotent delete. A denied operation is not success, and acknowledging that job would turn an authorization fault into silent retention. Use bounded retries, then move the asset to a visible exception state that the reconciler continues to count.

Deployment deserves the same caution as image parsing. Add the retention fields first, dual-write them while the old path still runs, backfill existing assets with an explicitly reviewed policy, and only then enable enforcement. Shadow the reconciler before granting deletion capability: compare what it would remove with listing and legal-hold state. After the diff is understood, enable a narrow policy class, verify object outcomes, and widen gradually. Don't launch a broad deletion worker with an empty backfill value interpreted as "expired."

Test adversarial ordering, not just the happy path. Complete the multipart upload twice. Deliver the resize event after the tombstone. Retry deletion after one derivative is already absent. Replace an image while its old thumbnail job is running. Revoke a seller between intent creation and completion. Feed Sharp a file whose extension and decoded content disagree. Then assert two invariants: no derivative becomes publishable after the tombstone, and every expired asset family eventually reaches verified deletion unless an explicit hold prevents it.

## When should direct private storage be replaced?

This architecture is not suitable when the application must inspect every byte synchronously before it may enter organizational storage; keep a controlled ingress service in that case, accepting its bandwidth and scaling burden. Direct multipart upload also adds browser-side retry state, stale-session cleanup, and more reconciliation work. For small images on an internal tool with low concurrency, proxying through the application may be the clearer system. Stick with that simpler path until request duration or memory pressure provides evidence to split the data plane.

S3-compatible does not mean operationally identical. Before choosing a private storage target, test multipart initiation, completion, abort behavior, authorization scope, metadata handling, and lifecycle interaction against the exact service and client version you will deploy. DigitalOcean Spaces is one documented S3-compatible option; Amazon S3, Cloudflare R2, and MinIO are other real products teams may evaluate, but this design does not rank them. The decision should follow verified behavior, data location requirements, deletion evidence, and the team's ability to operate reconciliation — not the familiarity of a logo.

| Path | Operational fit | Main limitation |
| --- | --- | --- |
| Application proxy | Small internal uploads and synchronous inspection | Application owns byte transfer and long requests |
| Direct multipart upload | Large marketplace media with a durable control plane | Requires abandoned-session cleanup and reconciliation |
| Controlled ingress service | Mandatory inspection before organizational storage | Adds a separately scaled data-plane service |

Set the page threshold too low and normal multipart cleanup jitter wakes someone with no customer-impacting deadline at risk. Set it too high and the first alert arrives after the retention promise has already been missed. A two-tier policy is usually easier to operate: create a ticket while there is remediation time, and page only when oldest-overdue age is approaching the documented commitment or deletion verification has stopped entirely. Tune both against observed completion latency and the policy clock; don't use queue depth as a substitute.

The closeout condition is precise: the reconciler is advancing again, overdue age is falling, exception assets have owners, and a targeted storage check agrees with the control plane. Silence the page only after those facts hold. Otherwise the alert has been acknowledged, not resolved.

## References

- [OWASP File Upload Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html)
- [DigitalOcean Spaces documentation](https://docs.digitalocean.com/products/spaces/)

## Further reading

Start with the OWASP upload controls, then validate the multipart and private-object behavior in the S3-compatible storage documentation linked above against a non-production bucket.
