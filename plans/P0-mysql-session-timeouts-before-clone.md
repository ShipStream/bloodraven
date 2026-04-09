# P0: Set MySQL Session Timeouts Before CLONE Operations

## Source
Percona bugs: K8SPS-517, K8SPS-346, K8SPS-392

## Problem
MySQL CLONE operations on large datasets fail because the default MySQL session
timeouts (net_read_timeout=30s, net_write_timeout=60s) are far too short.
Percona hit "query interrupted" errors on clone because of a 10-second default
read timeout. They fixed it by increasing to 3600s.

For datasets >100GB, even the Go context deadline may not be enough -- the
MySQL-level timeouts kill the connection independently of the Go context.

## Current State in Bloodraven
- `internal/mysql/replication.go` `BootstrapReplica()` executes
  `CLONE INSTANCE FROM` with a Go context timeout
- No MySQL session-level timeouts are set before the CLONE
- `clone_ddl_timeout` is not configured in the generated `my.cnf`
- The clone timeout is not configurable via the CR spec

## Proposed Fix
1. Before executing `CLONE INSTANCE FROM`, run:
   ```sql
   SET SESSION net_read_timeout = 3600;
   SET SESSION net_write_timeout = 3600;
   SET GLOBAL clone_ddl_timeout = 3600;
   ```
2. Add `clone_ddl_timeout=3600` to the generated `my.cnf` in the reconciler's
   ConfigMap builder
3. Make the timeout configurable in the CR spec:
   ```yaml
   spec:
     mysql:
       cloneTimeout: 3600  # seconds, default 3600
   ```
4. Use this value for both the Go context deadline and the MySQL session timeouts

## Files to Modify
- `internal/mysql/replication.go` -- add SET SESSION statements before CLONE
- `internal/controller/reconciler.go` -- add clone_ddl_timeout to my.cnf template
- `api/v1alpha1/types.go` -- add CloneTimeout field to MySQLSpec

## Testing
- Unit test: verify SET SESSION statements are executed before CLONE
- Unit test: verify my.cnf includes clone_ddl_timeout
- Integration consideration: test with large dataset (>10GB) to confirm no
  timeout errors
