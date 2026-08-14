# Changelog

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
