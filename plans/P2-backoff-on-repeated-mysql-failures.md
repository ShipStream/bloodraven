# P2: Backoff on Repeated MySQL Connection Failures in Polling

## Source
Percona bug: K8SPS-539

## Problem
Percona's entrypoint script had a file-existence check with no backoff,
causing excessive CPU utilization during recovery. The same pattern applies
to any tight polling loop that retries a failing operation at a fixed interval.

Bloodraven's topology manager polls both DCs every `PollInterval` (default 2s).
When a DC is unreachable, each poll attempt creates a new TCP connection that
times out (5s timeout), logs the error, and retries 2s later. This is fine
short-term but wasteful when a DC is down for minutes or hours.

Similarly, `WaitForRelayLogDrain()` polls every 500ms for up to 30s. If the
SQL thread is stopped or errored, it will poll 60 times with no useful result.

## Current State in Bloodraven
- `internal/controller/topology.go` -- fixed 2s poll interval regardless of DC state
- `internal/mysql/replication.go` -- 500ms poll in `WaitForRelayLogDrain()`
- `internal/sidecar/fencing.go` -- fixed interval liveness checks
- No exponential backoff or adaptive polling anywhere

## Proposed Fix
1. **Adaptive poll interval for unreachable DCs:**
   When a DC transitions to `Unreachable`, increase the poll interval for that
   DC progressively: 2s -> 4s -> 8s -> 16s -> 30s (cap). Reset to 2s when the
   DC responds again. This reduces CPU and network waste without meaningfully
   delaying detection of recovery.

2. **Backoff in WaitForRelayLogDrain:**
   Use increasing intervals: 500ms, 1s, 2s, 4s... with the same 30s total
   timeout. Also check if SQL thread is stopped/errored and return early
   instead of continuing to poll.

3. **Add jitter** to all backoff intervals (10-20% randomization) to prevent
   synchronized retry storms if multiple operators are running.

## Files to Modify
- `internal/controller/topology.go` -- adaptive poll interval per DC
- `internal/mysql/replication.go` -- backoff in `WaitForRelayLogDrain()`
- New: `internal/util/backoff.go` -- shared backoff helper with jitter

## Testing
- Unit test: verify poll interval increases when DC is unreachable
- Unit test: verify poll interval resets on recovery
- Unit test: verify WaitForRelayLogDrain exits early on SQL thread error
