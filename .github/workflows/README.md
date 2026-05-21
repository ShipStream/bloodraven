# GitHub Actions Workflows

This directory contains the CI/CD automation for the Bloodraven MySQL operator.

---

## Workflows

### `ci.yml` — Continuous Integration

**Triggers:** Push to `main`, pull requests targeting `main`

Runs the full PR gate suite in parallel:

| Job | Description |
|---|---|
| `lint` | Runs `golangci-lint` (installed via `go install`) |
| `build` | Compiles `cmd/bloodraven` and `cmd/sidecar` |
| `test-unit` | `make test-unit` — unit tests under `./internal/...` |
| `test-component` | `make test-component` — component tests under `./test/component/` |
| `test-envtest` | `make test-envtest` — controller tests using envtest (real API server) |
| `generate-check` | Runs `make generate && make manifests` and fails if files are stale |
| `docs-build` | Builds Docusaurus and verifies `llms-full.txt` includes every docs page |
| `all-checks` | Summary job — use this as the single branch-protection required status check |

**Permissions:** `contents: read` (default)

---

### `docs-link-check.yml` — Public Documentation Link Check

**Triggers:** Manual dispatch and nightly schedule.

ReadTheDocs already watches this repository and publishes `main` to
`https://bloodraven.readthedocs.io/en/latest/`. This workflow keeps the
GitHub side deliberately small: it crawls the published site and fails on
same-site broken links or missing assets.

The workflow does not trigger ReadTheDocs builds and requires no
repository secrets.

---

### `release.yml` — Release Automation

**Triggers:** Push of a semver tag matching `v*.*.*`

**Required permissions:** `contents: write`, `packages: write`, `id-token: write` (for cosign OIDC signing)

Steps:

1. **CI gate** — Runs lint, builds, tests, generate drift checks, chart CRD drift checks, docs build, and `llms-full.txt` coverage before any release artifact is published.
2. **Draft release** — Creates a draft GitHub Release with image locations, Helm chart locations, install examples, and auto-generated notes.
3. **Docker images** — Builds multi-arch (`linux/amd64` + `linux/arm64`) images for both targets:
   - `ghcr.io/shipstream/bloodraven:{version}` and `:latest`
   - `ghcr.io/shipstream/bloodraven-sidecar:{version}` and `:latest`
4. **Cosign image signing** — Keyless OIDC signing via Sigstore (no secrets required).
5. **Helm chart** — Updates `Chart.yaml` `version`/`appVersion` from the tag, packages the chart, then publishes it to both GitHub Pages and GHCR OCI.
6. **Publish release** — After Docker and Helm jobs both succeed, the draft release is published. If either fails, the release remains in draft state.

Release output locations:

- Operator image: `ghcr.io/shipstream/bloodraven:{version}`
- Sidecar image: `ghcr.io/shipstream/bloodraven-sidecar:{version}`
- Helm repo: `https://shipstream.github.io/bloodraven`
- Helm OCI repo: `oci://ghcr.io/shipstream/bloodraven/helm`

Helm repo install example:

```bash
helm repo add bloodraven https://shipstream.github.io/bloodraven
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

This publishes Docker images, creates a GitHub Release, updates the GitHub Pages Helm chart repository, and pushes the chart to GHCR as an OCI artifact automatically.

#### Required setup for Helm chart publishing

The `gh-pages` branch must exist (can be empty). Create it once with:

```bash
git checkout --orphan gh-pages
git rm -rf .
git commit --allow-empty -m "init gh-pages"
git push origin gh-pages
git checkout main
```

Then enable GitHub Pages for the repository pointing at the `gh-pages` branch. The Helm chart repository URL will be:
`https://shipstream.github.io/bloodraven`

---

### `e2e.yml` / `_e2e.yml` — Real-Cluster E2E

**Triggers:**
- Nightly schedule (release profile)
- Manual dispatch with profile selection (smoke / release / full)
- Pull requests with the `e2e` label (smoke profile)

The reusable workflow (`_e2e.yml`) creates a kind cluster with Calico CNI, deploys the playground, and runs `playground-chaos run-all` with the selected profile. It uploads JUnit results, chaos forensics, setup logs, and kind logs as artifacts.

Profiles:
| Profile | Scenarios | Use case |
|---|---|---|
| `smoke` | 3 (~3-5 min) | PR label gate, fast feedback |
| `release` | 10 (~20-30 min) | Release and nightly gate |
| `full` | All registered | Full regression (manual only) |

The release workflow (`.github/workflows/release.yml`) blocks Docker image builds and Helm chart publishing on the E2E release-profile gate. This ensures every tagged release is validated against real MySQL failover scenarios (WISHLIST #32).

**Permissions:** `contents: read` (default)

---

### `scan.yml` — Trivy Security Scan

**Triggers:** Pull requests targeting `main`

**Required permissions:** `security-events: write` (for SARIF upload to the Security tab)

Builds all four image variants (2 targets × 2 architectures) and runs Trivy on each:
- Fails the job on any `CRITICAL` or `HIGH` unfixed CVEs
- Uploads SARIF results to the GitHub Security tab for tracking

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
