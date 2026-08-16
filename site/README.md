# bloodraven.dev

This directory is the [bloodraven.dev](https://bloodraven.dev) site: a Docus
(Nuxt Content) app deployed to Railway.

## Local development

```bash
npm install
npm run dev
```

The development server listens on `http://localhost:3000/` by default.

## Edit docs

- Put user-facing pages in `content/docs/`.
- Numbered prefixes set sidebar order and are stripped from the published URL
  (`4.configuration/8.encryption-at-rest.md` publishes at
  `/docs/configuration/encryption-at-rest`).
- Use one page type per page: tutorial, how-to guide, reference, or explanation.
- Keep code examples copy-pasteable, and show expected output when verification matters.
- Prefer links to canonical pages over repeating the same operational facts in multiple files.
- Use `content/docs/8.observability/6.observability-change-checklist.md` for
  changes to metrics, recording rules, alerts, dashboard panels, Kubernetes
  Events, structured-log Events, or runbook links.

When you change structured log messages or documented log fields, update
`content/docs/8.observability/7.log-schema.md` in the same pull request.

## Build

```bash
npm run build
npm run verify:llms
```

`verify:llms` checks that `.output/public/llms-full.txt` includes every page
under `content/docs/`.

## License signing

`POST /api/license` and the `/license` page mint an offline-verifiable JWT
from a Polar order. The route is stateless: no database and no email. The
site still builds and serves docs when these variables are unset; minting
returns HTTP 503 until they are.

`POST /api/polar/webhook` is deliberately inert. It verifies Polar's
Standard Webhooks signature with `@polar-sh/sdk`, logs event type plus
order/subscription/product IDs, and returns 202. It does not mint tokens,
write state, or call Polar back. The license flow does not depend on it.

| Variable | Required | Purpose |
|---|---|---|
| `LICENSE_SIGNING_SEED_B64` | to mint | Standard base64 of the raw 32-byte Ed25519 seed. Already set on Railway `bloodraven-site` / `production`. Must derive public key `br-1` (`1b3bea77…6f02`). |
| `POLAR_API_TOKEN` | to mint | Polar Organization Access Token with `orders:read` only. |
| `POLAR_WEBHOOK_SECRET` | for `/api/polar/webhook` | Polar webhook signing secret, as Polar shows it. Invalid signatures return 401 and are not logged. Missing secret returns 503. |
| `POLAR_API_BASE` | no | `https://api.polar.sh` (default) or `https://sandbox-api.polar.sh`. |
| `LICENSE_SIGNING_KID` | no | Defaults to `br-1`. |

Tag each Bloodraven license product in the Polar dashboard with metadata
`edition` = `production` or `organization` (including renewal products).
Products without that key cannot mint a token.

Rate limit: 10 requests / 15 minutes / client IP, in memory, per process.
It resets on deploy and is not shared across instances.

`npm test` runs the Node unit tests. From the repo root,
`make test-license-roundtrip` signs a throwaway token in Node and verifies
it with the Go operator verifier.

`.github/workflows/deploy-site.yml` deploys this directory to Railway on every
push to `main` that touches `site/`, and on manual dispatch.
`.github/workflows/docs-link-check.yml` crawls the published site nightly and
on manual dispatch to catch same-site broken links.
