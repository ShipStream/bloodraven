# Changelog

`msg` strings listed in [the log schema Event reference](site/content/docs/8.observability/7.log-schema.md) do not change without a note here.

## Unreleased

### Added

- Runtime `COMMAND INFO REPLTAKEOVER` probe. `status.dragonfly.replTakeoverSupported` reports whether the running image advertises the command. Warning Event `DragonflyReplTakeoverUnsupported` fires when it does not. This is a capability report, not tag admission; Dragonfly `v1.38.0+` remains the supported pin.
- Emergency `REPLICAOF NO ONE` fallback now increments `bloodraven_dragonfly_promotions_total{result="sessions_lost"}` and emits Warning Event `DragonflySessionsLost` in addition to `DragonflyPromotionCompleted`. The emergency path still does not return an error or block MySQL.

### Changed

- `bloodraven_dragonfly_promotions_total` result values are now `success`, `failed`, `skipped`, and `sessions_lost`. The empty-master fallback no longer increments `success`.
