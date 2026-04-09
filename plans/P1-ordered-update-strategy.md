# P1: Ordered Update Strategy (Replica First, Failover, Then Old Primary)

## Source
Percona bugs: K8SPS-365, K8SPS-372, K8SPS-308, K8SPS-307

## Problem
Percona hit crash loops during MySQL version upgrades because containers were
restarted after being added to the cluster. Two-node async replication clusters
failed during SmartUpdate. No log messages indicated which pod was being updated
or when the update finished.

Bloodraven uses `Recreate` deployment strategy, meaning both MySQL and sidecar
containers are torn down and recreated simultaneously. For a two-DC pair, if
both DCs update at the same time, there is guaranteed downtime.

## Current State in Bloodraven
- `internal/controller/reconciler.go` creates Deployments with `Recreate` strategy
- No ordered update / rolling update logic exists
- Image or config changes to the Deployment spec trigger simultaneous recreation
  of both DCs

## Proposed Fix
1. Add an update controller that detects when a Deployment spec change is needed
   (image version, my.cnf config hash, resource limits, etc.)
2. Implement the following ordered sequence:
   a. Update the **replica DC** Deployment first
   b. Wait for the replica pod to become Ready and replication to catch up
      (IO/SQL running, lag < threshold)
   c. **Failover** to the replica (promote it to primary)
   d. Update the **old primary DC** Deployment (now a replica)
   e. Wait for it to become Ready
   f. Re-establish replication from the new primary
   g. Optionally failback to the original primary DC
3. Log each step clearly with structured fields:
   ```
   {"level":"info","msg":"starting ordered update","phase":"update-replica","dc":"dc2"}
   {"level":"info","msg":"replica updated and healthy","phase":"promote-replica","dc":"dc2"}
   {"level":"info","msg":"failover complete","phase":"update-old-primary","dc":"dc1"}
   {"level":"info","msg":"ordered update complete","duration":"3m42s"}
   ```
4. Add a CR status condition `Updating` with progress information
5. Block manual failover during an ordered update

## Files to Modify
- New: `internal/controller/updater.go` -- ordered update controller
- `internal/controller/reconciler.go` -- detect spec drift, delegate to updater
- `internal/controller/failover.go` -- add "update-triggered" failover path
- `api/v1alpha1/types.go` -- add UpdateStrategy field and Updating condition

## Testing
- Unit test: verify replica is updated before primary
- Unit test: verify failover occurs between updates
- Unit test: verify update is blocked if replication is broken pre-update
- E2E test: image change triggers ordered update with zero downtime
