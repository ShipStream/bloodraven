# Wishlist Progress

Tracking implementation progress for items in [bloodraven-wishlist.md](bloodraven-wishlist.md).

## P0 — Must have before first production tenant

- [ ] 1. DNS flip before MySQL promotion (reverse the failover sequence)
- [x] 2. MySQL configuration management — already implemented as `spec.mysqlConf`, docs added
- [x] 3. Container resource requests and limits — `spec.sites[].resources` existed, added `spec.sidecarResources`
- [x] 4. Multiple MySQL credentials (`spec.credentials`) — per-role secrets with MySQL user lifecycle management
- [x] 5. Backup and restore (`spec.backup`) — mysqlsh-based, S3 + PVC targets, bootstrap restore via `spec.initFromBackup`, new `MysqlBackup` CRD
- [x] 6. Reduce default failover cooldown (60m → 5m) — `8cdcf46`, `3594235`
- [ ] 7. Data loss detection and reporting on emergency failover
- [x] 8. Old primary recovery procedure — auto-rejoin on no divergence; fence + alert on divergence (requires wipe/re-clone)

## P1 — Required before multi-tenant scale

- [ ] 9. Shared-node support (per-failover-group taint scoping)
- [x] 10. Per-site extra containers and init containers (`spec.extraContainers`, `spec.extraInitContainers`) — `327b6c5`
- [x] 11. Pod annotations and labels passthrough (`spec.podLabels`, `spec.podAnnotations`) — `2ee4d22`
- [x] 12. PodDisruptionBudget management
- [x] 13. Configurable service types and annotations (`spec.serviceTemplate`) — `327b6c5`
- [ ] 14. Webhook-based notifications

## P2 — Nice to have

- [x] 15. Automatic initial clone — GTID-based donor selection, empty-site detection, annotation-based reclone trigger
- [ ] 16. Read replica lag-aware routing
- [ ] 17. Maintenance mode
- [ ] 18. Operator high availability (leader election)
- [ ] 19. kubectl plugin
- [ ] 20. Helm chart with ArgoCD compatibility
