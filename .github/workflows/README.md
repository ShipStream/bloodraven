# GitHub Actions Workflows

This directory contains the CI/CD automation for the Bloodraven MySQL operator.

---

## Workflows

### `ci.yml` — Continuous Integration

**Triggers:** Push to `main`, pull requests targeting `main`

Superseded PR runs are cancelled. Pushes to `main` and release calls use unique run groups and do not cancel each other.

Pull requests run jobs for the areas they touch: Go jobs for Go-related source and configuration, docs-build for `site/`, course-check for `course/` or `site/public/courses/`, and license-roundtrip for either Go or site changes. Changes to `ci.yml`, shared CI actions or scripts, or `Makefile` select all jobs. Unknown paths, empty diffs, and diff failures also select all jobs. Pushes to `main` and release calls run everything.

| Job | Description |
|---|---|
| `changes` | Classifies changed paths and selects the affected jobs |
| `lint` | Runs `golangci-lint` (installed via `go install`) |
| `build` | Compiles `cmd/bloodraven` and `cmd/sidecar` |
| `test-unit` | `make test-unit` — unit tests under `./internal/...` |
| `test-component` | `make test-component` — component tests under `./test/component/` |
| `test-envtest` | `make test-envtest` — controller tests using envtest (real API server) |
| `generate-check` | Runs `make generate && make manifests` and fails if files are stale |
| `license-roundtrip` | Go verifier round-trips a token signed by `site/scripts/sign-license-token.mjs` (no `node_modules` needed) |
| `docs-build` | Runs the site Node tests (`npm test`), builds the bloodraven.dev site under `site/`, and verifies `llms-full.txt` includes every docs page |
| `course-check` | Verifies course content, rebuilds the course site, and checks committed output for drift |
| `all-checks` | Summary job — accepts skipped jobs and fails on any failure or cancellation, including `changes`; use this as the single branch-protection required status check |

**Permissions:** `contents: read` (default)

---

### `deploy-site.yml` — bloodraven.dev Deploy

**Triggers:** Push to `main` touching `site/**`, and manual dispatch.

**Permissions:** `contents: read`

Uploads `site/` to the Railway service `bloodraven-site`, which builds it with
Nixpacks (see `site/railway.json`) and serves <https://bloodraven.dev>.

**Required secret:** `RAILWAY_TOKEN` — a Railway *project* token, which scopes
the CLI to a single project and environment with no interactive login. The
workflow fails fast when the secret is missing rather than letting the CLI
offer to create a new project.

**Optional variable:** `RAILWAY_SERVICE` — overrides the default service name
`bloodraven-site`.

> If the Railway project also has the GitHub repo connected for automatic
> deploys, disconnect it in the Railway dashboard. Otherwise every push to
> `main` deploys twice: once from Railway's own integration and once here.

---

### `docs-link-check.yml` — Public Documentation Link Check

**Triggers:** Manual dispatch and nightly schedule.

The published site is <https://bloodraven.dev>. This workflow crawls it
and fails on same-site broken links or missing assets. It requires no
repository secrets.

---

### `release.yml` — Release Automation

**Triggers:** Push of a semver tag matching `v*.*.*`

**Permissions:** `contents: read` by default. Write scopes are granted per job: `contents: write` for the draft/publish release jobs and the Helm job (`gh-pages` push), `packages: write` for the image and Helm OCI pushes, and `id-token: write` only for the image job (cosign OIDC signing).

Steps:

1. **CI gate** — Runs lint, builds, tests, generate drift checks, chart CRD drift checks, the bloodraven.dev site build, and `llms-full.txt` coverage before any release artifact is published.
2. **Draft release** — Creates a draft GitHub Release with image locations, Helm chart locations, install examples, and auto-generated notes.
3. **Docker images** — Builds multi-arch (`linux/amd64` + `linux/arm64`) images for both targets:
   - `ghcr.io/shipstream/bloodraven:{version}` and `:latest`
   - `ghcr.io/shipstream/bloodraven-sidecar:{version}` and `:latest`
4. **Cosign image signing** — Keyless OIDC signing via Sigstore (no secrets required).
5. **Helm chart** — Updates `Chart.yaml` `version`/`appVersion` from the tag, packages the chart, then publishes it to both GitHub Pages and GHCR OCI. The chart is not signed; only the images are.
6. **Publish release** — After Docker and Helm jobs both succeed, the draft release is published. If either fails, the release remains in draft state.

Release output locations:

- Operator image: `ghcr.io/shipstream/bloodraven:{version}`
- Sidecar image: `ghcr.io/shipstream/bloodraven-sidecar:{version}`
- Helm repo: `https://raw.githubusercontent.com/ShipStream/bloodraven/gh-pages`
- Helm OCI repo: `oci://ghcr.io/shipstream/bloodraven/helm`

Helm repo install example:

```bash
helm repo add bloodraven https://raw.githubusercontent.com/ShipStream/bloodraven/gh-pages
helm repo update
helm install bloodraven bloodraven/bloodraven --version 1.2.3
```

Helm OCI install example:

```bash
helm install bloodraven oci://ghcr.io/shipstream/bloodraven/helm/bloodraven --version 1.2.3
```

#### How to trigger a release

```bash
git tag v1.2.3
git push origin v1.2.3
```

This publishes Docker images, creates a GitHub Release, commits the packaged chart + `index.yaml` to the `gh-pages` branch, and pushes the chart to GHCR as an OCI artifact automatically.

#### Required setup for Helm chart publishing

The `gh-pages` branch must exist (can be empty). The release workflow bootstraps it automatically, or create it once with:

```bash
git checkout --orphan gh-pages
git rm -rf .
git commit --allow-empty -m "init gh-pages"
git push origin gh-pages
git checkout main
```

The chart repo is served directly off the `gh-pages` branch via `raw.githubusercontent.com` — no GitHub Pages required. (GitHub Pages is unusable here: the org's Pages custom domain `docs.shipstream.io` is owned by the Mintlify dev center, so `https://shipstream.github.io/bloodraven` redirects there and 404s.) The Helm chart repository URL is:
`https://raw.githubusercontent.com/ShipStream/bloodraven/gh-pages`

---

### `e2e.yml` / `_e2e.yml` — Real-Cluster E2E

**Triggers:**
- Nightly schedule (release profile plus dedicated encryption scenario)
- Manual dispatch with profile selection (smoke / release / full) plus encryption
- Pull requests with the `e2e` label (smoke profile plus encryption)

The reusable workflow (`_e2e.yml`) creates a kind cluster with Calico CNI and deploys the playground. Normal jobs run `playground-chaos run-all` with the selected profile. The dedicated encryption job creates the group with MySQL and escrow TLS from its first generation, adopts encryption with `playground/enable-encryption.sh`, and runs `48-keyring-seal-and-rotation` explicitly. This covers adoption, sealing, key placement, replica rotation, pod replacement, and post-rotation reads against real MySQL without overlapping a separate live TLS rollout. Jobs upload JUnit results where available, chaos forensics, setup logs, and kind logs as artifacts.

Profiles:
| Profile | Scenarios | Use case |
|---|---|---|
| `smoke` | 3 (~3-5 min) | PR label gate, fast feedback |
| `release` | 10 (~20-30 min) | Release and nightly gate |
| `full` | All registered | Full regression (manual only) |

The release workflow (`.github/workflows/release.yml`) blocks publishing on both the smoke failover gate and the dedicated encryption adoption/rotation gate. The broader release profile still runs after publishing for extended validation.

**Permissions:** `contents: read` (default)

---

### `scan.yml` — Trivy Security Scan

**Triggers:** Pull requests targeting `main` unless all changes are under `site/`, `course/`, `proposals/`, `skills/`, or `.agents/` (trees excluded from the image context); daily at 06:17 UTC on `main`; manual dispatch

Superseded PR runs are cancelled.

**Required permissions:** `security-events: write` (for SARIF upload to the Security tab)

Builds all four image variants (2 targets × 2 architectures) and runs Trivy on each:

- Fails the job on any `CRITICAL` or `HIGH` fixable CVEs
- Uploads SARIF results to the GitHub Security tab for same-repository PRs and non-PR runs, including scheduled scans on `main`

---

## Dependabot (`.github/dependabot.yml`)

Weekly automated dependency updates for:
- **Go modules** (`gomod`) — patch and minor updates are grouped into a single PR
- **GitHub Actions** (`github-actions`)
- **Docker** base images in `Dockerfile`

---

## Secrets / Variables

| Name | Required by | Description |
|---|---|---|
| `GITHUB_TOKEN` | All workflows | Automatically provided by GitHub Actions |
Cosign signing uses keyless OIDC and relies only on `id-token: write` permission.
