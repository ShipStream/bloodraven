# P0: Nil-Check / IsNotFound Guard in Topology Loop on CR Deletion

## Source
Percona bug: K8SPS-366

## Problem
If the MysqlReplicaPair CR is deleted while the topology polling loop is active,
the next attempt to fetch the CR or update its status will operate on a nil/stale
object and potentially panic. Percona's operator panicked in exactly this
scenario when querying a non-existing Custom Resource during cluster deletion.

## Current State in Bloodraven
- `internal/controller/runner.go` runs the topology polling loop, which
  periodically fetches DC status and calls `updateCRStatus()`
- `internal/controller/topology.go` references the CR for configuration
  (poll intervals, thresholds, DC specs)
- There are no IsNotFound guards or nil-checks on the CR fetch path in the
  polling goroutine
- No finalizer exists to coordinate shutdown

## Proposed Fix
1. In the topology polling loop, wrap the CR fetch with an `apierrors.IsNotFound`
   check. If the CR is gone, log a message and gracefully stop the polling
   goroutine (cancel context).
2. Before every `Status().Update()` call, verify the CR still exists.
3. Add a finalizer (`shipstream.io/topology-cleanup`) that:
   - Stops the topology polling loop
   - Removes any node taints applied by this CR
   - Cleans up DNS records (or at least logs a warning)
   - Then removes itself so the CR can be garbage collected

## Files to Modify
- `internal/controller/runner.go` -- add IsNotFound guard in poll loop
- `internal/controller/topology.go` -- add nil-safety on CR references
- `internal/controller/reconciler.go` -- add finalizer management

## Testing
- Unit test: simulate CR deletion mid-poll, verify graceful shutdown (no panic)
- Unit test: verify finalizer removes taints before allowing deletion
