# Bloodraven documentation site

This directory contains the Docusaurus source for the Bloodraven documentation site.

## Install dependencies

```bash
npm install
```

## Run locally

```bash
npm start
```

The development server serves the docs at `http://localhost:3000/` by default.

## Build

```bash
npm run build
```

The build writes static files to `docs/build/`.

## Edit docs

- Put user-facing pages in `docs/docs/`.
- Update `docs/sidebars.js` when you add, remove, or rename a page.
- Use one page type per page: tutorial, how-to guide, reference, or explanation.
- Keep code examples copy-pasteable, and show expected output when verification matters.
- Prefer links to canonical pages over repeating the same operational facts in multiple files.

## Important pages

| Page | Use it for |
|---|---|
| `docs/docs/intro.mdx` | Documentation landing page and reader journeys. |
| `docs/docs/getting-started.mdx` | First `MysqlFailoverGroup` setup. |
| `docs/docs/install-production.mdx` | Production install path. |
| `docs/docs/runbooks.mdx` | On-call remediation procedures. |
| `docs/docs/crd-reference.mdx` | Custom resource fields and status reference. |
| `docs/docs/log-schema.mdx` | Stable structured log messages and fields. |

When you change structured log messages or documented log fields, update `docs/docs/log-schema.mdx` in the same pull request.
