# P1: Factor Replication Health into Ready Condition

## Source
Percona bug: K8SPS-43

## Problem
Percona's operator reported the cluster as "ready" even when replication was
broken. They fixed it by checking replication state in the status calculation.

Bloodraven currently reports `Ready` if at least one DC is writable, but does
not check whether the replica's IO/SQL threads are running or whether
replication lag is excessive.

## Current State in Bloodraven
- `internal/controller/runner.go` sets `Ready` condition if any DC is `Writable`
- `internal/controller/topology.go` polls `SELECT @@read_only` but does NOT
  check `SHOW REPLICA STATUS` during topology polling
- `internal/mysql/replication.go` has `ShowReplicaStatus()` that parses
  IO/SQL thread state, `Seconds_Behind_Source`, and `LastError`
- The sidecar exposes replication status via `/status` endpoint but the
  topology manager doesn't consume it

## Proposed Fix
1. Extend topology polling to also check replica health on the read-only DC:
   - Call `SHOW REPLICA STATUS` (or query sidecar `/status` endpoint)
   - Check IO_Running and SQL_Running
   - Check Seconds_Behind_Source against a configurable threshold
   - Check LastError for non-empty values
2. Add new Degraded reasons:
   - `ReplicationBroken` -- IO or SQL thread not running
   - `ReplicationLagging` -- Seconds_Behind_Source > threshold
   - `ReplicationError` -- LastError is non-empty
3. The `Ready` condition should be false if replication is broken (even if
   primary DC is writable), since data is not being replicated.
4. Add a configurable lag threshold in the CR spec:
   ```yaml
   spec:
     replication:
       maxLagSeconds: 300  # default 300
   ```

## Files to Modify
- `internal/controller/topology.go` -- add replication status polling
- `internal/controller/runner.go` -- add replication health to Ready condition
- `internal/state/machine.go` -- add replication-related degraded reasons
- `api/v1alpha1/types.go` -- add MaxLagSeconds field

## Testing
- Unit test: verify Ready=false when SQL thread is stopped
- Unit test: verify Degraded=ReplicationLagging when lag exceeds threshold
- Unit test: verify Ready=true when replication is healthy
