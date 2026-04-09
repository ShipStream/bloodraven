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
| `all-checks` | Summary job — use this as the single branch-protection required status check |

**Permissions:** `contents: read` (default)

---

### `release.yml` — Release Automation

**Triggers:** Push of a semver tag matching `v*.*.*`

**Required permissions:** `contents: write`, `packages: write`, `id-token: write` (for cosign OIDC signing)

Steps:

1. **Draft release** — Creates a draft GitHub Release with auto-generated notes.
2. **Docker images** — Builds multi-arch (`linux/amd64` + `linux/arm64`) images for both targets:
   - `ghcr.io/shipstream/bloodraven:{version}` and `:latest`
   - `ghcr.io/shipstream/bloodraven-sidecar:{version}` and `:latest`
3. **Cosign image signing** — Keyless OIDC signing via Sigstore (no secrets required).
4. **Helm chart** — Updates `Chart.yaml` `version`/`appVersion` from the tag, then publishes to the `gh-pages` branch as a Helm chart repository via `helm/chart-releaser-action`.
5. **Publish release** — After Docker and Helm jobs both succeed, the draft release is published. If either fails, the release remains in draft state.

#### How to trigger a release

```bash
git tag v1.2.3
git push origin v1.2.3
```

This publishes Docker images, creates a GitHub Release, and updates the Helm chart repository automatically.

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

No additional secrets are required. Cosign signing uses keyless OIDC and relies only on `id-token: write` permission.
