export default defineNuxtConfig({
  extends: ['docus'],

  site: {
    url: 'https://bloodraven.dev',
    name: 'Bloodraven',
  },

  // Docus' app.vue points `rel=icon` at /favicon.ico, which this site does not
  // ship. These PNG icons are generated from the master mark by
  // `scripts/build-brand-assets.mjs`; the raven sits on its ink field rather
  // than on alpha so it stays visible against a dark browser tab strip.
  app: {
    head: {
      link: [
        { rel: 'icon', type: 'image/png', sizes: '32x32', href: '/img/brand/favicon-32.png' },
        { rel: 'icon', type: 'image/png', sizes: '512x512', href: '/img/brand/favicon-512.png' },
        { rel: 'apple-touch-icon', sizes: '180x180', href: '/img/brand/favicon-180.png' },
      ],
    },
  },

  // Docus enables the Ask AI assistant when AI_GATEWAY_API_KEY is set
  // (Vercel AI Gateway). Without it the site builds and serves normally,
  // just without the assistant.

  llms: {
    domain: 'https://bloodraven.dev',
    title: 'Bloodraven',
    description:
      'Kubernetes operator for MySQL async replication failover groups across sites. '
      + 'Automates failover detection, promotion, DNS steering, fencing, clone bootstrap, '
      + 'backup and restore.',
    full: {
      title: 'Bloodraven - complete documentation',
      description: 'Every Bloodraven documentation page in a single file.',
    },
  },
})
