# Changelog

`msg` strings listed in [the log schema Event reference](site/content/docs/8.observability/7.log-schema.md) do not change without a note here.

## Unreleased

### Breaking: site-scoped gauge labels

`bloodraven_replication_lag_seconds`, `bloodraven_replication_running`,
and `bloodraven_site_state` now carry `namespace`, `group`, and `role`
in addition to the previous labels. Exact-match aggregations and
recording rules that assumed the old label set will stop matching
until they are updated. Metric *names* are unchanged.

`role` is `spec.sites[].role`: `primary-candidate`, `dr-only`, or
`read-only`.

| Metric | Before | After |
|---|---|---|
| `bloodraven_replication_lag_seconds` | `site` | `namespace`, `group`, `site`, `role` |
| `bloodraven_replication_running` | `site`, `thread` | `namespace`, `group`, `site`, `role`, `thread` |
| `bloodraven_site_state` | `site`, `state` | `namespace`, `group`, `site`, `role`, `state` |

Correct RPO lag alert (pages candidate and DR, silent on designed reader lag):

```promql
bloodraven_replication_lag_seconds{role=~"primary-candidate|dr-only"} > 30
```

Promotable-only form:

```promql
bloodraven_replication_lag_seconds{role="primary-candidate"} > 30
```

Do not exclude readers by site name (`{site!="reader"}`). That list is
global across failover groups and silently regresses when a reader is
added.

The production course still describes the pre-change label set. It is
not updated in this change; treat `site/content/docs/8.observability/`
as authoritative.

### Breaking: keyring label spelling

Keyring and encryption-coverage metrics now use the same `namespace` /
`group` spellings as the archiver, restore, and source-state families.
Selectors copied from those families no longer silently match nothing.
This rename is a separate commit and can be reverted on its own.

| Metric | Before | After |
|---|---|---|
| `bloodraven_keyring_phase` | `mysql_namespace`, `failover_group`, `site`, `phase` | `namespace`, `group`, `site`, `phase` |
| `bloodraven_keyring_escrow_version` | `mysql_namespace`, `failover_group`, `site` | `namespace`, `group`, `site` |
| `bloodraven_keyring_escrow_pushes_total` | `failover_group`, `site`, `outcome` | `group`, `site`, `outcome` |
| `bloodraven_keyring_rotations_total` | `failover_group`, `site`, `outcome` | `group`, `site`, `outcome` |
| `bloodraven_encryption_unencrypted_tablespaces` | `mysql_namespace`, `failover_group`, `site` | `namespace`, `group`, `site` |
| `bloodraven_encryption_coverage` | `mysql_namespace`, `failover_group`, `site`, `aspect` | `namespace`, `group`, `site`, `aspect` |

### Added

- Runtime `COMMAND INFO REPLTAKEOVER` probe. `status.dragonfly.replTakeoverSupported` reports whether the running image advertises the command. Warning Event `DragonflyReplTakeoverUnsupported` fires when it does not. This is a capability report, not tag admission; Dragonfly `v1.38.0+` remains the supported pin.
- Emergency `REPLICAOF NO ONE` fallback now increments `bloodraven_dragonfly_promotions_total{result="sessions_lost"}` and emits Warning Event `DragonflySessionsLost` in addition to `DragonflyPromotionCompleted`. The emergency path still does not return an error or block MySQL.

### Changed

- `bloodraven_dragonfly_promotions_total` result values are now `success`, `failed`, `skipped`, and `sessions_lost`. The empty-master fallback no longer increments `success`.

