# P0: Retry Logic on Kubernetes API Calls

## Source
Percona bugs: K8SPS-501, K8SPS-494, K8SPS-498

## Problem
Bloodraven makes single-attempt Kubernetes API calls for PVC updates, status
subresource writes, and resource patches. Transient API server errors (conflicts,
timeouts, brief unavailability) will cause the operation to fail silently with
only a log entry.

Percona hit this specifically with PVC expansion (K8SPS-501) -- the operator
tried once to resize the PVC, got a conflict, and gave up. They fixed it by
adding retry logic.

## Current State in Bloodraven
- `internal/controller/reconciler.go` -- creates/updates ConfigMaps, PVCs,
  Deployments, Services with no retry wrappers
- `internal/controller/runner.go` (`updateCRStatus`) -- writes status subresource
  with no conflict retry
- `internal/platform/tainter.go` -- patches nodes with no retry
- `internal/platform/dns.go` -- single-attempt Cloudflare API call

## Proposed Fix
1. Use `retry.RetryOnConflict` from `k8s.io/client-go/util/retry` for all
   status subresource updates and resource patches
2. Add a generic retry helper with exponential backoff for non-idempotent
   operations (DNS updates, taint patches)
3. For PVC updates specifically, retry up to 3 times with 1s/2s/4s backoff

## Files to Modify
- `internal/controller/reconciler.go`
- `internal/controller/runner.go`
- `internal/platform/tainter.go`
- `internal/platform/dns.go`
- New: `internal/util/retry.go` (shared retry helper)

## Testing
- Unit test: mock API server returning Conflict errors, verify retry succeeds
- Unit test: mock API server returning persistent errors, verify gives up after max retries
