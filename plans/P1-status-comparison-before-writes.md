# P1: Status Comparison Before CR Status Writes

## Source
Percona bugs: K8SPS-494, K8SPS-498

## Problem
Writing the CR status subresource on every reconcile loop -- even when nothing
has changed -- bumps `resourceVersion`, which triggers another reconcile,
creating an infinite loop. Percona found that a condition was being set twice
per loop, constantly updating `lastTransitionTime`.

## Current State in Bloodraven
- `internal/controller/runner.go` `updateCRStatus()` pushes a `TopologySnapshot`
  to the CR status on every state change callback
- The topology manager fires `StatusCallback` on every transition, but it's
  unclear if no-op transitions (same state -> same state) are filtered
- No deep-equal comparison is done before calling `Status().Update()`

## Proposed Fix
1. Before calling `Status().Update()`, deep-compare the new status with the
   existing status on the CR object. Skip the update if they are equal.
2. Use `reflect.DeepEqual` or `equality.Semantic.DeepEqual` from
   `k8s.io/apimachinery/pkg/api/equality` for the comparison.
3. For conditions specifically, only update `lastTransitionTime` when the
   condition status actually changes (not on every reconcile).
4. After a successful status update, re-fetch the CR from the API server
   before continuing reconciliation to avoid working with stale objects.

## Files to Modify
- `internal/controller/runner.go` -- add status comparison in `updateCRStatus()`
- `internal/controller/topology.go` -- filter no-op state transitions before
  firing StatusCallback

## Testing
- Unit test: verify Status().Update() is NOT called when status hasn't changed
- Unit test: verify lastTransitionTime only changes on actual condition transitions
