export default defineAppConfig({
  docus: {
    title: 'Bloodraven',
    description: 'Kubernetes operator for MySQL async replication failover groups across sites.',
    url: 'https://bloodraven.dev',
    socials: {
      github: 'https://github.com/ShipStream/bloodraven',
    },
    github: {
      url: 'https://github.com/ShipStream/bloodraven',
      branch: 'main',
      rootDir: 'site',
      edit: true,
    },
    header: {
      title: 'Bloodraven',
      showTitle: true,
    },
    seo: {
      titleTemplate: '%s · Bloodraven',
    },
    toc: {
      title: 'On this page',
    },
  },
})
