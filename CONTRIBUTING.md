# Contributing to Bloodraven

Bug reports, reproductions, and pull requests are welcome from everyone —
license holders and free-tier users alike.

## Before your first pull request: sign the CLA

Bloodraven requires a signed [Contributor License Agreement](./CLA.md) before we
can merge your code. Open your pull request as normal; a bot will comment with a
one-line instruction to sign. It takes about ten seconds and covers all your
future contributions.

**Why:** Bloodraven is distributed under the [Business Source
License 1.1](./LICENSE) and sold under [commercial
terms](./LICENSE-COMMERCIAL.md). Without a CLA, contributed code could not be
included in the commercial distribution, and every future license change would
require tracking down every past contributor. You keep the copyright in your
contribution — the CLA is a license grant, not an assignment.

Documentation-only and typo fixes do not require a CLA.

## Licensing your contribution

Do not paste code from GPL, AGPL, or other copyleft-licensed projects. If your
change is adapted from another source, say so in the pull request and name the
license. See [CLA.md](./CLA.md) Section 4.

## Reporting bugs

For failover correctness, split-brain, or data-loss bugs, please include:

- The `MySQLFailoverGroup` resource, with secrets redacted.
- Operator and sidecar logs covering the incident window.
- `kubectl bloodraven status` output.
- Bloodraven version, Kubernetes version, and MySQL version.
- What you expected to happen, and what happened instead.

If you can reproduce it in the [playground](./playground), a chaos scenario or a
script that triggers it is the single most useful thing you can attach.

Suspected security vulnerabilities should go to **security@shipstream.io**, not
to a public issue.

## Development workflow

Read [CLAUDE.md](./CLAUDE.md) for the repository layout, build commands, coding
conventions, and testing guidance. The short version:

```bash
make build          # operator + sidecar
make generate       # deep-copy code
make manifests      # CRDs and RBAC
make vet
make lint           # golangci-lint
make test           # unit + component
```

### Pre-PR gate

Run all of these from the repository root and fix anything they report before
pushing. CI enforces each one, and catching them locally saves a round trip.

1. `make generate && make manifests` — commit any resulting diff. The
   `Generate Check` CI job fails on stale generated files.
2. `make vet`
3. `make lint`
4. `make test`, plus `make test-envtest` if you touched anything that interacts
   with the API server.
5. If you changed kubebuilder RBAC markers, mirror them in
   `charts/bloodraven/templates/clusterrole.yaml` — the chart RBAC is
   hand-maintained.
6. If you added or changed a CRD, copy the regenerated files from
   `config/crd/bases/` to `charts/bloodraven/crds/`.

### Things that will get a PR sent back

- Changing a structured-log `msg` string or field name listed in
  `docs/docs/log-schema.mdx` without updating that document in the same PR.
  Those strings are a stability contract for downstream log pipelines.
- Filenames ending in `_<goos>.go` (`_linux`, `_dragonfly`, `_js`, and friends).
  Go silently drops them from the build with no error.
- Behavior changes to reconciliation, failover, CRD types, or the sidecar
  without a corresponding documentation update under `docs/`.
- New failover-path logic without either a component test in `test/component` or
  a playground scenario in `internal/playground/scenarios`.

## Pull requests

Keep commit subjects short and imperative, matching existing history
(`Upgrade mysql-watcher to Bloodraven MySQL operator`). In the PR description,
explain the operational impact, call out any CRD, failover, or sidecar behavior
change, link the relevant issue, and attach logs, manifests, or screenshots when
cluster-visible behavior changes.

## What we are unlikely to merge

- Synchronous replication or zero-RPO features. Bloodraven is deliberately an
  async-replication failover operator; see
  [why-not-group-replication](./docs/docs/why-not-group-replication.mdx).
- Support for database engines other than MySQL.
- Vendored forks of upstream dependencies.
- Features that add a hard dependency on a specific cloud provider.

Open an issue to discuss anything large before you build it. We would rather
talk about the design than decline a finished pull request.
