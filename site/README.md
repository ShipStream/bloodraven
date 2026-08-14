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

`.github/workflows/deploy-site.yml` deploys this directory to Railway on every
push to `main` that touches `site/`, and on manual dispatch.
`.github/workflows/docs-link-check.yml` crawls the published site nightly and
on manual dispatch to catch same-site broken links.
