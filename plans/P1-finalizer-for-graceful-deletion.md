# P1: Finalizer for Graceful Deletion Ordering

## Source
Percona bugs: K8SPS-333, K8SPS-387, K8SPS-418

## Problem
Without a finalizer, deleting the MysqlReplicaPair CR will immediately garbage
collect all owned resources. This means:
- Both DCs torn down simultaneously with no replication drain
- Node taints left behind (no cleanup)
- DNS records left pointing at now-dead IPs
- In-flight transactions may be lost if terminationGracePeriod is too short

Percona added `wait_for_delete` to ensure full cleanup before re-deployment,
and fixed their pod deletion finalizer to account for primary instance changes.

## Current State in Bloodraven
- No finalizer on the MysqlReplicaPair CR
- No ordered shutdown sequence
- `terminationGracePeriodSeconds` is not configurable via CR spec
- Node taints and DNS records are managed but not cleaned up on CR deletion

## Proposed Fix
1. Add finalizer `shipstream.io/graceful-shutdown` in the reconciler when
   the CR is first created
2. When the CR has a deletion timestamp and the finalizer is present, execute:
   a. Fence the primary: `SET GLOBAL super_read_only=ON`
   b. Wait for replica relay log drain (reuse `WaitForRelayLogDrain`)
   c. Stop replication on the replica: `STOP REPLICA`
   d. Remove node taints for both DCs
   e. Remove/update DNS records (or log a warning for manual cleanup)
   f. Stop the topology polling loop
   g. Remove the finalizer (allowing GC to proceed)
3. Make `terminationGracePeriodSeconds` configurable:
   ```yaml
   spec:
     mysql:
       terminationGracePeriodSeconds: 60  # default 30
   ```
4. Set this value on the MySQL container's pod spec

## Files to Modify
- `internal/controller/reconciler.go` -- add finalizer on create, handle on delete
- `internal/platform/tainter.go` -- add `RemoveAllTaints(zones)` method
- `internal/platform/dns.go` -- add cleanup method (or log warning)
- `api/v1alpha1/types.go` -- add TerminationGracePeriodSeconds field

## Testing
- Unit test: verify finalizer is added on CR creation
- Unit test: verify ordered shutdown sequence on deletion
- Unit test: verify taints are removed during shutdown
- Unit test: verify finalizer is removed last (allowing GC)
