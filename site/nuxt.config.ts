export default defineNuxtConfig({
  extends: ['docus'],

  site: {
    url: 'https://bloodraven.dev',
    name: 'Bloodraven',
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
