import { defineRailway, project, service, volume } from 'railway/iac'

/**
 * The resources of the Railway project "Bloodraven Commercial Site" that this
 * repository manages as code. Railway stops honouring railway.json
 * (Config as Code) on 2026-12-01; this file replaces it.
 *
 * It is a partial: it owns only what it declares, so the bloodraven-site
 * service (still on site/railway.json) is left alone. Preview with
 * `railway config plan`, apply with `railway config apply` (from the repo
 * root, linked to the project). Code is deployed separately by
 * .github/workflows/deploy-telemetry.yml (`railway up`).
 *
 * bloodraven-telemetry is the usage report server (telemetry-server/). Its
 * SQLite database lives on the volume at /data: one replica only, since two
 * would each write their own copy. Keep telemetry.bloodraven.dev a plain
 * CNAME (no CDN proxy): the server trusts forwarding headers only from
 * Railway's edge. Variables are unmanaged (set with `railway variables`):
 * PORT=8080, which the domain targets, and optionally IPINFO_TOKEN.
 */
export const partial = 'bloodraven'

export default defineRailway(() => {
  const data = volume('bloodraven-telemetry-volume', { region: 'us-east4-eqdc4a', sizeMB: 50000 })

  const telemetry = service('bloodraven-telemetry', {
    build: { builder: 'DOCKERFILE', dockerfilePath: 'Dockerfile' },
    healthcheck: '/healthz',
    healthcheckTimeout: 60,
    replicas: { 'us-east4-eqdc4a': 1 },
    domains: [{ domain: 'telemetry.bloodraven.dev', port: 8080 }],
    volumeMounts: { '/data': data },
  })

  return project('Bloodraven Commercial Site', {
    variables: { managed: false },
    resources: [data, telemetry],
  })
})
