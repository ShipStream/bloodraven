# Bloodraven Shared-Node Placement Migration Guide

This guide is for projects that already implemented Bloodraven's previous placement model using single-valued node labels:

```bash
shipstream.io/failover-group=<group>
shipstream.io/site=<site>
```

Bloodraven now requires each `MysqlFailoverGroup` site to declare an explicit `spec.sites[].taintNodeSelector`. This is a breaking API change. There is no compatibility fallback because there are no production deployments to preserve.

## What changed

Previously, Bloodraven inferred which nodes to taint from the failover group name and site name:

```text
shipstream.io/failover-group=<group>,shipstream.io/site=<site>
```

Now, each site declares the selector directly:

```yaml
spec:
  sites:
    - name: iad
      taintNodeSelector:
        shipstream.io/failover-group.orders: "true"
        shipstream.io/site.orders: iad
```

The operator uses this selector for both failover tainting and finalizer cleanup.

The readonly taint remains group-scoped, but it is now consistently `NoExecute`:

```text
shipstream.io/db-readonly-<group>=true:NoExecute
```

## Required manifest changes

For each site in every `MysqlFailoverGroup`, add `taintNodeSelector`.

Before:

```yaml
apiVersion: shipstream.io/v1alpha1
kind: MysqlFailoverGroup
metadata:
  name: orders
spec:
  sites:
    - name: iad
      zone: us-east-1a
      lbIP: 10.0.1.1
      storage:
        storageClassName: fast-ssd
        size: 100Gi
    - name: pdx
      zone: us-west-2a
      lbIP: 10.0.2.1
      storage:
        storageClassName: fast-ssd
        size: 100Gi
```

After:

```yaml
apiVersion: shipstream.io/v1alpha1
kind: MysqlFailoverGroup
metadata:
  name: orders
spec:
  sites:
    - name: iad
      zone: us-east-1a
      taintNodeSelector:
        shipstream.io/failover-group.orders: "true"
        shipstream.io/site.orders: iad
      lbIP: 10.0.1.1
      storage:
        storageClassName: fast-ssd
        size: 100Gi
    - name: pdx
      zone: us-west-2a
      taintNodeSelector:
        shipstream.io/failover-group.orders: "true"
        shipstream.io/site.orders: pdx
      lbIP: 10.0.2.1
      storage:
        storageClassName: fast-ssd
        size: 100Gi
```

Use one label namespace per failover group. For example, `orders` uses `shipstream.io/failover-group.orders` and `shipstream.io/site.orders`; `inventory` uses `shipstream.io/failover-group.inventory` and `shipstream.io/site.inventory`.

## Required node label changes

Relabel each node that participates in a failover group.

Before:

```bash
kubectl label node node-iad-1 \
  shipstream.io/failover-group=orders \
  shipstream.io/site=iad

kubectl label node node-pdx-1 \
  shipstream.io/failover-group=orders \
  shipstream.io/site=pdx
```

After:

```bash
kubectl label node node-iad-1 \
  shipstream.io/failover-group.orders=true \
  shipstream.io/site.orders=iad \
  shipstream.io/failover-group- \
  shipstream.io/site-

kubectl label node node-pdx-1 \
  shipstream.io/failover-group.orders=true \
  shipstream.io/site.orders=pdx \
  shipstream.io/failover-group- \
  shipstream.io/site-
```

For shared nodes, add one selector label pair per failover group:

```bash
kubectl label node node-iad-1 \
  shipstream.io/failover-group.orders=true \
  shipstream.io/site.orders=iad \
  shipstream.io/failover-group.inventory=true \
  shipstream.io/site.inventory=iad
```

## Required workload toleration changes

Review application pod tolerations.

Workers, cron, runners, and other write-dependent pods should not tolerate their own group's readonly taint:

```yaml
# orders write-dependent pod: do not add this toleration
- key: shipstream.io/db-readonly-orders
  operator: Exists
  effect: NoExecute
```

On shared nodes, pods should tolerate other groups' taints so unrelated failovers do not evict them:

```yaml
# orders pod on nodes shared with inventory
spec:
  tolerations:
    - key: shipstream.io/db-readonly-inventory
      operator: Exists
      effect: NoExecute
```

Read-only or stateless pods that intentionally stay alive at both sites may tolerate their own group's taint.

## Cleanup old taints

If a test cluster used the old unscoped taint, remove it manually. The new operator no longer performs legacy cleanup.

```bash
kubectl taint node node-iad-1 shipstream.io/db-readonly- 2>/dev/null || true
kubectl taint node node-pdx-1 shipstream.io/db-readonly- 2>/dev/null || true
```

Also clear group-scoped taints if you are resetting a lab cluster before applying the new manifests:

```bash
kubectl taint node node-iad-1 shipstream.io/db-readonly-orders- 2>/dev/null || true
kubectl taint node node-pdx-1 shipstream.io/db-readonly-orders- 2>/dev/null || true
```

## Suggested migration sequence

1. Update node labels to the new per-group label shape.
2. Update every `MysqlFailoverGroup` manifest with `spec.sites[].taintNodeSelector`.
3. Update workload tolerations from any old assumptions to `NoExecute` and group-scoped taint keys.
4. Apply the new CRD before applying updated `MysqlFailoverGroup` objects.
5. Apply the updated operator and manifests.
6. Verify each site selector matches the expected nodes.
7. Trigger a controlled failover in a non-production environment and confirm only the intended group's workloads are evicted.

## Verification commands

Confirm nodes match the selectors:

```bash
kubectl get nodes -l 'shipstream.io/failover-group.orders=true,shipstream.io/site.orders=iad'
kubectl get nodes -l 'shipstream.io/failover-group.orders=true,shipstream.io/site.orders=pdx'
```

Confirm the CR was accepted with selectors:

```bash
kubectl get mysqlfailovergroup orders -o jsonpath='{range .spec.sites[*]}{.name}: {.taintNodeSelector}{"\n"}{end}'
```

Confirm taints after failover:

```bash
kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints[*].key
```

Expected result: only nodes selected for the losing site carry `shipstream.io/db-readonly-orders`.

## Playground-specific notes

The playground now labels nodes with:

```text
shipstream.io/failover-group.playground=true
shipstream.io/site.playground=iad|pdx
```

The playground `MysqlFailoverGroup` manifests include matching `taintNodeSelector` entries. If you have an existing playground cluster, rerun `./playground/setup.sh` or manually relabel nodes and clear stale taints before testing new chaos scenarios.
