## Summary

- 

## Verification

- 

## Contributor checklist

- [ ] I have signed the [CLA](../blob/main/CLA.md) (first-time contributors; the bot will prompt you).
- [ ] This change contains no code copied from GPL, AGPL, or other copyleft-licensed sources.

<details>
<summary>Observability change checklist</summary>

Complete this section only if the PR adds, removes, or changes metrics, recording rules, alerts, dashboard panels, Kubernetes Events, structured-log Events, or runbook links. Otherwise, write `No observability signal changes.`

Canonical checklist: `docs/docs/observability-change-checklist.mdx`

- Artifact classes changed: (metrics, recording rules, alerts, dashboard panels, Kubernetes Events, structured-log Events, runbook links)
- Documentation pages updated:
- Metric or recording-rule cardinality evidence: (label, domain, bounded?, per-scope upper bound, consumer/migration notes if applicable)
- Alert annotations and runbook mapping: (`summary`, `description`, `runbook_url`, `dashboard_url`, or specific no-runbook/no-dashboard rationale)
- Dashboard verification evidence: (screenshot, local preview, or validator command and result)
- Kubernetes Event changes: (reason/type, attached object, trigger, operator action/runbook if actionable)
- Structured-log Event compatibility notes: (`msg`, fields, compatibility impact, `log-schema.mdx` update)

</details>
