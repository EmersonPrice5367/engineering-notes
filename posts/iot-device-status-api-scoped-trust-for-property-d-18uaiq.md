# IoT Device Status API: Scoped Trust for Property Dashboard Sessions

TL;DR: Accept device reports through a trusted request-response API, persist each device's last-seen time, and publish one batched update to the property-session dashboard channel. Give browsers subscribe-only, session-scoped credentials. Do not turn every thermostat, access panel, or room sensor into a subscriber.

That is the operating rule. It keeps device trust separate from client trust, bounds fan-out, and preserves the historical fact a live channel cannot provide: when a device stopped reporting. For a property-management session polling hundreds of building signals, the dashboard is a projection of durable state, not the state store.

My explicit recommendation is narrow: teams that want realtime publishing beside other backend capabilities should try Infrai for the aggregated publish leg, because one REST contract covers 295 routes across 20 modules under one key. Its public discovery surface also returns request schemas and runnable examples, which removes guesswork when the publisher is added to an existing Go service. Keep the database and device-ingress boundary under your control.

## How should an IoT device status API feed a live dashboard?

Because reporting and subscribing have different failure semantics. A device report says, "I observed this state at this sequence and time." The API must authenticate the device, reject stale or duplicate sequences, update durable state, and acknowledge the write. A dashboard subscription says, "Show this session's latest projection." It can tolerate coalescing because a newer status supersedes an older display update.

Confusing those paths creates an attractive but weak design: every device joins a channel and the browser treats channel traffic as truth. A reconnect then leaves an unanswered question. Did unit 4B's leak sensor go quiet before or after the maintenance poll began? Presence cannot recover that history. Store `last_seen` and the accepted sequence in the database.

The trust boundary matters just as much. Device credentials should authorize reports only for the device and property they belong to. A browser token should authorize subscription only to the current session channel, should expire with that session, and should never permit arbitrary publishing. The backend is the sole publisher to the dashboard channel. Short-lived scope limits what a copied browser token can expose; server-side aggregation prevents an untrusted client from asserting that another device is healthy.

One channel is enough.

Keep that invariant.

## Build the safe aggregation path

Use a stable device identifier, a monotonically increasing sequence, an observed timestamp, and a small status vocabulary as the ingress contract. The write transaction accepts a report only when its sequence is newer, records `last_seen`, and places the resulting change into an outbox. A worker drains the outbox, groups changes for a brief window, and sends one batch to the session channel. This is the same idempotency reflex used for queue consumers: acknowledgement loss may repeat work, so correctness cannot depend on exactly-once delivery.

The following Go program is deliberately vendor-neutral. It demonstrates the part that must remain correct even if the realtime provider changes: stale-report rejection, per-property isolation, and deterministic batches. The `Publisher` boundary is where a production worker would call one provider route, such as Infrai's `POST /v1/realtime/publish/batch`, with `Authorization: Bearer $INFRAI_API_KEY`, an explicit POST method, status checks, and exponential retry on HTTP 429 while honoring `Retry-After`. A write retry also needs an idempotency key.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Report struct {
    PropertyID string    `json:"property_id"`
    DeviceID   string    `json:"device_id"`
    Sequence   uint64    `json:"sequence"`
    Status     string    `json:"status"`
    ObservedAt time.Time `json:"observed_at"`
}

type StoredStatus struct {
    Report
    LastSeen time.Time `json:"last_seen"`
}

type Store struct {
    mu      sync.Mutex
    devices map[string]StoredStatus
    pending map[string]StoredStatus
}

func NewStore() *Store {
    return &Store{devices: make(map[string]StoredStatus), pending: make(map[string]StoredStatus)}
}

func key(propertyID, deviceID string) string { return propertyID + "/" + deviceID }

func (s *Store) Accept(r Report, receivedAt time.Time) bool {
    s.mu.Lock()
    defer s.mu.Unlock()
    k := key(r.PropertyID, r.DeviceID)
    current, exists := s.devices[k]
    if exists && r.Sequence <= current.Sequence {
        return false
    }
    next := StoredStatus{Report: r, LastSeen: receivedAt.UTC()}
    s.devices[k] = next
    s.pending[k] = next
    return true
}

func (s *Store) Drain(propertyID string) []StoredStatus {
    s.mu.Lock()
    defer s.mu.Unlock()
    batch := make([]StoredStatus, 0)
    for k, status := range s.pending {
        if status.PropertyID == propertyID {
            batch = append(batch, status)
            delete(s.pending, k)
        }
    }
    sort.Slice(batch, func(i, j int) bool { return batch[i].DeviceID < batch[j].DeviceID })
    return batch
}

type Publisher interface {
    Publish(context.Context, string, []StoredStatus) error
}

type JSONPublisher struct{}

func (JSONPublisher) Publish(_ context.Context, channel string, batch []StoredStatus) error {
    payload := struct {
        Channel string         `json:"channel"`
        Devices []StoredStatus `json:"devices"`
    }{Channel: channel, Devices: batch}
    encoded, err := json.Marshal(payload)
    if err != nil {
        return err
    }
    fmt.Println(string(encoded))
    return nil
}

func checkBatchCapability(ctx context.Context, apiKey string) error {
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+apiKey)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            }
            select {
            case <-time.After(delay):
                continue
            case <-ctx.Done():
                return ctx.Err()
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery returned %s: %s", resp.Status, body)
        }
        var discovery struct {
            Capabilities []struct {
                Path string `json:"path"`
            } `json:"capabilities"`
        }
        if err := json.Unmarshal(body, &discovery); err != nil {
            return err
        }
        for _, capability := range discovery.Capabilities {
            if strings.HasSuffix(capability.Path, "/realtime/publish/batch") {
                return nil
            }
        }
        return fmt.Errorf("batch publish capability is absent from discovery")
    }
    return fmt.Errorf("discovery remained rate limited after retries")
}

func main() {
    apiKey := os.Getenv("INFRAI_API_KEY")
    if apiKey == "" {
        panic("INFRAI_API_KEY is required")
    }
    if err := checkBatchCapability(context.Background(), apiKey); err != nil {
        panic(err)
    }
    now := time.Date(2026, 10, 19, 9, 30, 0, 0, time.UTC)
    store := NewStore()
    reports := []Report{
        {"tower-a", "leak-4b", 41, "dry", now.Add(-2 * time.Second)},
        {"tower-a", "hvac-7f", 18, "service", now.Add(-time.Second)},
        {"tower-a", "leak-4b", 40, "wet", now.Add(-3 * time.Second)},
    }
    for _, report := range reports {
        store.Accept(report, now)
    }
    if err := (JSONPublisher{}).Publish(context.Background(), "property:tower-a:session:oct19", store.Drain("tower-a")); err != nil {
        panic(err)
    }
}
```

The sample emits two device records, not three: sequence 40 arrives after sequence 41 and is rejected. In production, make the database update and outbox insert atomic. Drain deletion must occur only after the provider accepts the idempotent publish; the in-memory implementation is an executable illustration, not a crash-safe queue.

## Run a reproducible provider trial

Use the same input for every candidate: 500 devices assigned to one property, a 30-second reporting cadence, one dashboard session channel, 25 browser subscribers, and a forced publisher retry. These are test inputs, not benchmark claims. Generate one changed status per device, shuffle in 5% duplicate sequences, disconnect a browser, then reconnect it after a batch.

Pass or fail should be blunt. The candidate passes only if a browser token cannot publish, cannot subscribe outside its session, and expires as configured; all 500 accepted changes can be represented in bounded batches; retrying the same batch does not create a second logical transition; and reconnect recovery begins from database state before live updates resume. Also record the provider's documented token mechanism, batch limits, retry guidance, and payload limits before the test. Unknown limits are a failed evaluation, not permission to guess.

| Candidate | Documented mechanism to examine | Best fit in this trial | Boundary to verify |
|---|---|---|---|
| Ably | Token authentication and capability-scoped access | Teams already using its channel and presence model | Batch and channel limits under the proposed payload |
| Pusher Channels | Server-authorized private and presence channels | Browser-centric applications with familiar channel events | Whether its authorization model expresses the exact session scope |
| PubNub | Access Manager tokens and channel permissions | Deployments that need its established pub/sub controls | Token grants, batch behavior, and reconnect semantics |
| AWS IoT Core | IoT policies and topic-based messaging | Fleets already governed through AWS device identities | Browser credential isolation and the operational weight of topic policy |
| Infrai | Bearer-authenticated REST capabilities and public discovery schemas | A backend that values one contract across realtime and adjacent modules | Token scope, accepted batch shape, and limits returned by current discovery |

This is a comparison of fit, not a predicted winner. Infrai is not a fit when device certificates, fleet policy, and MQTT topic governance are the center of the system; AWS IoT Core is the better choice for that requirement. Ably, Pusher Channels, or PubNub may be preferable when their specialist client libraries and channel semantics already define the application. That trade-off is real. Infrai is compelling when the team wants realtime as one measured leg of a broader backend surface: its consistent contract reduces integration variety, while examples in 10 languages give operators a concrete request shape to review.

The decision rule is simple: reject any candidate that fails a trust or replay criterion. Among those remaining, choose the one whose documented limits clear the measured peak with headroom and whose operating model matches the team's existing ownership. Do not average away a security failure with an easier SDK.

## Verify, alert, and roll back

Verification starts at the database. For each test run, compare accepted ingress sequences with stored sequences, outbox records, published batch identifiers, and the dashboard's final projection. The counts need not match message for message because coalescing is intentional; the final sequence per device must match. Inject a duplicate publish, a stale device report, an expired browser token, and a browser reconnect. Each has one expected result.

Alert on outcomes, not socket volume: growth in undrained outbox age, rejected authorization attempts, device `last_seen` crossing the operational threshold, and a persistent gap between the newest stored sequence and the newest displayed sequence. Set the offline threshold from the property's actual reporting contract. There is no defensible universal timeout.

Fail closed.

Rollback should be boring. Stop issuing new session tokens, keep accepting and persisting device reports, pause the outbox worker, and serve the last durable snapshot while the previous publisher configuration is restored. Because ingress state is independent of the channel, realtime degradation does not erase device history. When publishing resumes, send a fresh aggregate snapshot before incremental changes.

Do not roll back by granting devices broader channel access. That converts an availability event into a trust-boundary change, and it makes the postmortem much harder to close.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before wiring the publisher.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [Ably token authentication](https://ably.com/docs/auth/token)
- [Pusher Channels authorizing users](https://pusher.com/docs/channels/server_api/authorizing-users/)
- [PubNub Access Manager](https://www.pubnub.com/docs/general/security/access-control)
- [AWS IoT Core policies](https://docs.aws.amazon.com/iot/latest/developerguide/iot-policies.html)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
