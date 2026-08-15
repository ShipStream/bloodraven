# Changelog

`msg` strings listed in [the log schema Event reference](site/content/docs/8.observability/7.log-schema.md) do not change without a note here.

## Unreleased

### Changed

- **Failover will not promote a site mid-keyring-rotation.** A replica with `status.encryptionAtRest.sites[].unsealReason=Rotation` is skipped; if it is the only remaining candidate the group stays without a writable primary until the site is `Sealed`. Planned failover to that target is rejected with `reason: KeyringRotation`. This is a deliberate availability cost: promoting would make an unescrowed keyring the sole authoritative copy. Warning Events `KeyringPromotionSkipped` / `KeyringPromotionRefused` and counter `bloodraven_keyring_promotions_blocked_total{outcome="skipped|refused"}` make the refusal visible. There is no cancel-rotation command — finish the rotation, then retry.
