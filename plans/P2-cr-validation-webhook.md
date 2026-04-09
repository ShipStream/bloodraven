# P2: CR Validation Webhook for Mandatory/Immutable Fields

## Source
Percona bugs: K8SPS-299, K8SPS-394, K8SPS-287, K8SPS-298

## Problem
Percona found multiple cases where invalid or incomplete CR specs caused
silent failures:
- Missing mandatory fields (`.mysql.size`) caused deployment to fail with
  no clear error
- Changing replication type on a running cluster silently broke it
- Scaling below safe minimums produced no warning
- Deploying async replication without a proxy was allowed but broken

## Current State in Bloodraven
- `api/v1alpha1/types.go` defines the MysqlReplicaPair spec
- No validating webhook or admission controller
- No reconciler-time validation that rejects bad specs
- Immutable field changes (e.g., server-id scheme, zone assignments) are
  silently applied and may break replication

## Proposed Fix
1. **Add a validating admission webhook** that runs on CREATE and UPDATE:

   **Mandatory fields (reject if missing):**
   - `spec.dc1` and `spec.dc2` must both be fully specified
   - Each DC must have `zone`, `lbIP`, `storage.size`, `storage.storageClassName`
   - `spec.mysql.image` must be non-empty

   **Immutable fields (reject changes on UPDATE):**
   - `spec.dc1.zone` and `spec.dc2.zone` -- changing zones on a running
     cluster is not supported
   - Server ID derivation inputs -- changing these would break GTID continuity

   **Safety warnings (allow but set condition):**
   - If only one DC is specified, set a condition warning about no HA

2. **Reconciler-time validation as fallback:**
   Even with a webhook, add validation at the top of `Reconcile()` that
   checks the spec and sets a `ConfigInvalid` condition if something is wrong,
   then returns without reconciling.

3. **Log clear messages** when validation fails, including what the user
   should change.

## Files to Modify
- New: `internal/webhook/validate.go` -- validating webhook handler
- `api/v1alpha1/types.go` -- add validation markers / field annotations
- `internal/controller/reconciler.go` -- add reconciler-time validation
- `config/webhook/` -- webhook configuration manifests

## Testing
- Unit test: reject CR with missing mandatory fields
- Unit test: reject CR with changed immutable fields
- Unit test: accept valid CR
- E2E: attempt to create invalid CR, verify rejection message
