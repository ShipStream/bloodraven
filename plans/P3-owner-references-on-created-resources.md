# P3: Owner References on All Created Resources

## Source
Percona bug: K8SPS-360

## Problem
Percona found that outdated Orchestrator Services were not removed after
cluster downscale. Without owner references, resources created by the
operator become orphans when the CR is deleted or modified, requiring
manual cleanup.

## Current State in Bloodraven
- `internal/controller/reconciler.go` creates ConfigMaps, PVCs, Deployments,
  and Services for each MysqlReplicaPair
- It's unclear whether all resources have owner references set (needs
  verification -- the reconciler may use `controllerutil.SetControllerReference`
  on some but not all resources)

## Proposed Fix
1. **Audit all created resources** and ensure every one has an owner reference
   pointing to the MysqlReplicaPair CR:
   - ConfigMap (my.cnf)
   - PVC per DC
   - Deployment per DC
   - Service per DC
   - Primary Service
   - Replicas Service

2. **Use `controllerutil.SetControllerReference()`** consistently:
   ```go
   if err := controllerutil.SetControllerReference(cr, obj, scheme); err != nil {
       return err
   }
   ```

3. **Verify cascade deletion works:**
   When the CR is deleted, all owned resources should be garbage collected
   automatically by Kubernetes. This is a safety net in addition to the
   finalizer (P1-finalizer-for-graceful-deletion.md).

4. **Handle PVC specially:**
   PVCs may need to survive CR deletion (to preserve data for re-creation).
   Consider using an annotation `shipstream.io/retain-pvc: "true"` to
   control whether PVCs get owner references. Default: retain (no owner ref
   on PVCs), matching Percona's `percona.com/delete-mysql-pvc` finalizer
   pattern.

## Files to Modify
- `internal/controller/reconciler.go` -- add/verify owner references on
  all created resources

## Testing
- Unit test: verify all non-PVC resources have owner references
- Unit test: verify PVCs do NOT have owner references by default
- E2E: delete CR, verify all Services/Deployments/ConfigMaps are cleaned up
- E2E: delete CR, verify PVCs are retained
