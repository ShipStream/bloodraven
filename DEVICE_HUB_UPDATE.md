# Device Hub: Watcher Integration

## Background

We're deploying a new service called `mysql-watcher` that monitors MySQL across both DCs and broadcasts state changes over websocket. Device Hub needs to connect to this service so it knows when its DC goes offline (MySQL failover) and can disconnect hardware / queue commands until the DC is back online.

Today Device Hubs have no way to know about DB failover until requests start failing. This fixes that.

## What Device Hub needs to do

1. On startup, open a websocket connection to the watcher.
2. Listen for JSON messages. When you get one for your DC, act on it.
3. If the websocket connection drops, assume offline (fail-safe).
4. Reconnect with backoff when the connection drops.

## Connection details

**Endpoint:** `ws://mysql-watcher.shared-{az}.svc.cluster.local:8080/ws/status`

- `{az}` is the availability zone name (e.g. `lion`), so a typical URL is `ws://mysql-watcher.shared-lion.svc.cluster.local:8080/ws/status`
- Plain `ws://`, not `wss://` — this is cluster-internal traffic
- Standard websocket upgrade (HTTP GET with `Upgrade: websocket` header), no auth required

The service name and namespace may vary per deployment — make the base URL configurable via an env var like `WATCHER_URL` with the above as the default pattern.

## Message format

Messages are JSON text frames. The schema is:

```json
{
  "dc": "dc1",
  "status": "offline"
}
```

| Field    | Type   | Values                |
|----------|--------|-----------------------|
| `dc`     | string | DC identifier, e.g. `dc1`, `dc2` |
| `status` | string | `"online"` or `"offline"` |

That's the entire protocol. The watcher only sends messages on state transitions — you won't get a message every poll cycle, only when something changes.

## How to handle messages

Each Device Hub knows which DC it's in. Filter messages by the `dc` field — ignore messages for other DCs.

**On `"offline"`:**
- Disconnect hardware (barcode scanners, label printers, etc.)
- Queue any pending commands locally
- Show an indicator to the operator that the system is in failover mode

**On `"online"`:**
- Reconnect hardware
- Flush queued commands
- Clear the failover indicator

## Connection lifecycle

**On connect:** No initial state message is sent. If the watcher has already determined your DC is offline, it will have broadcast that before you connected. So:
- Treat a fresh connection as "online" (normal state).
- If the DC is actually offline, the watcher will send an `"offline"` message on the next state transition or you'll see the websocket drop because the watcher itself will be unreachable.

**On disconnect (any reason — network error, watcher restart, etc.):**
- **Assume offline.** This is the fail-safe behavior. Disconnect hardware, queue commands.
- Begin reconnecting with exponential backoff (e.g. 1s, 2s, 4s, 8s, cap at 30s).
- On successful reconnect, resume normal "online" behavior.

The watcher is a single-replica deployment so it will briefly go down during rolling updates. Device Hub should handle this gracefully — the disconnect-means-offline rule covers it.

## Pseudocode

```
loop:
    conn = connect(WATCHER_URL)
    if conn fails:
        go_offline()
        backoff_sleep()
        continue

    go_online()  // connected = assume online
    
    while conn is open:
        msg = read_json(conn)
        if msg.dc != MY_DC:
            continue
        if msg.status == "offline":
            go_offline()
        else if msg.status == "online":
            go_online()
    
    // connection dropped
    go_offline()
    backoff_sleep()
```

## Testing

You can test locally by connecting any websocket client to the watcher and watching for messages. The watcher also exposes:

- `GET /status` — returns current state of both DCs as JSON (useful for debugging)
- `GET /healthz` — liveness check

## Questions?

Ping Colin if anything is unclear.
