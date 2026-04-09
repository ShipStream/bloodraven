# P3: PodDisruptionBudget Support

## Source
Percona bug: K8SPS-417

## Problem
Without a PodDisruptionBudget (PDB), voluntary disruptions like node drains,
cluster upgrades, or spot instance reclamation can evict MySQL pods without
any availability guarantee. Kubernetes will happily drain both DC pods
simultaneously if they happen to land on the same node or if two nodes
are drained concurrently.

## Current State in Bloodraven
- No PDB is created for MySQL Deployments
- Each DC runs as a single-replica Deployment, so a PDB with
  `minAvailable: 1` on the pair would prevent both from being evicted
  simultaneously

## Proposed Fix
1. **Create a PDB per MysqlReplicaPair** that covers both DC pods:
   ```yaml
   apiVersion: policy/v1
   kind: PodDisruptionBudget
   metadata:
     name: <cr-name>-mysql-pdb
   spec:
     minAvailable: 1
     selector:
       matchLabels:
         shipstream.io/replica-pair: <cr-name>
   ```
   This ensures at least one MySQL pod is always available during
   voluntary disruptions.

2. **Add the shared label** `shipstream.io/replica-pair: <cr-name>` to
   both DC pods so the PDB selector matches them.

3. **Make PDB configurable** in the CR spec:
   ```yaml
   spec:
     mysql:
       podDisruptionBudget:
         minAvailable: 1  # default
   ```

4. **Set owner reference** on the PDB so it's cleaned up with the CR.

## Files to Modify
- `internal/controller/reconciler.go` -- create/update PDB
- `api/v1alpha1/types.go` -- optional PDB configuration

## Testing
- Unit test: verify PDB is created with correct selector
- Unit test: verify PDB is updated when CR spec changes
- E2E: drain a node, verify at least one MySQL pod stays running
