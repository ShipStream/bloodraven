# Changelog

## [Unreleased]

### Added

- Offline Ed25519 license verification. Optional `spec.license` on
  `MysqlFailoverGroup` and operator-level `--license` /
  `BLOODRAVEN_LICENSE` / Helm `license`. The operator verifies the JWT
  with no network calls and exposes organization, edition, and
  update-period end as logs and Prometheus series
  (`bloodraven_license_info`,
  `bloodraven_license_updates_expiry_timestamp_seconds`). Invalid tokens
  and ended update periods never gate functionality or change failover
  behavior. The production signing public key is a placeholder the
  repo owner must fill in; see [License token contract](site/content/docs/2.license-token.md).
