# P2: Configurable terminationGracePeriodSeconds

## Source
Percona bug: K8SPS-418

## Problem
Percona added the ability to configure how long a pod has to shut down
gracefully after receiving SIGTERM. The default Kubernetes value of 30 seconds
may not be enough for MySQL instances with:
- Large transactions in flight
- Active long-running queries
- InnoDB buffer pool flush on shutdown
- Binary log sync on shutdown

## Current State in Bloodraven
- `internal/controller/reconciler.go` creates Deployments with pod specs
- No configurable `terminationGracePeriodSeconds` -- uses Kubernetes default (30s)
- No pre-stop hook on the MySQL container

## Proposed Fix
1. **Add to CR spec:**
   ```yaml
   spec:
     mysql:
       terminationGracePeriodSeconds: 60  # default 60
   ```

2. **Apply to pod template** in the Deployment spec:
   ```go
   podSpec.TerminationGracePeriodSeconds = ptr.To(int64(spec.MySQL.TerminationGracePeriodSeconds))
   ```

3. **Add a pre-stop hook** on the MySQL container that:
   - Sets `super_read_only=ON` (prevents new writes)
   - Waits for active transactions to complete (with timeout)
   - This gives a clean shutdown window before SIGKILL

4. **Default to 60s** rather than Kubernetes' 30s, since MySQL needs time
   for InnoDB shutdown and binlog flush.

## Files to Modify
- `api/v1alpha1/types.go` -- add TerminationGracePeriodSeconds field with default
- `internal/controller/reconciler.go` -- apply to pod spec, add pre-stop hook

## Testing
- Unit test: verify terminationGracePeriodSeconds is set on generated pod spec
- Unit test: verify default is 60 when not specified
- Unit test: verify pre-stop hook is present on MySQL container
