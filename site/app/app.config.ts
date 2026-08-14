export default defineAppConfig({
  // Docus reads `header`, `seo`, `github`, `socials` and `toc` from the root of
  // the app config, not from a `docus` key -- nesting them silently disables
  // the header GitHub link, the footer socials and the SEO title template.
  ui: {
    colors: {
      primary: 'blood',
      neutral: 'slate',
    },
  },

  seo: {
    title: 'Bloodraven',
    titleTemplate: '%s · Bloodraven',
    description: 'Kubernetes operator for MySQL async replication failover groups across sites.',
  },

  header: {
    title: 'Bloodraven',
    // The product header draws its own mark (see AppHeader.vue); no logo image
    // is configured so Docus' AppHeaderLogo fallback is never reached.
  },

  socials: {
    github: 'https://github.com/ShipStream/bloodraven',
  },

  github: {
    url: 'https://github.com/ShipStream/bloodraven',
    branch: 'main',
    rootDir: 'site',
    edit: true,
  },

  toc: {
    title: 'On this page',
    bottom: {
      title: 'Community',
      links: [
        {
          icon: 'i-simple-icons-github',
          label: 'Star on GitHub',
          to: 'https://github.com/ShipStream/bloodraven',
          target: '_blank',
        },
        {
          icon: 'i-lucide-graduation-cap',
          label: 'Bloodraven in Production',
          to: '/courses/',
          target: '_blank',
        },
      ],
    },
  },

  // Seeds the "Ask AI" panel. The homepage chips are set in content/index.md so
  // the operator-facing question set stays editable next to the copy.
  assistant: {
    faqQuestions: [
      {
        category: 'Failover',
        items: [
          'How does Bloodraven decide which site to promote?',
          'What happens to my data if the primary site dies mid-transaction?',
          'How do I run a planned switchover with zero data loss?',
        ],
      },
      {
        category: 'Setup',
        items: [
          'Show me a minimal two-site MysqlFailoverGroup manifest.',
          'What do I need before installing the operator in production?',
          'How do I turn on data-at-rest encryption?',
        ],
      },
      {
        category: 'Operations',
        items: [
          'How do I schedule backups to S3 with retention?',
          'A site is stuck in RecoveryBlocked -- what do I do?',
          'Which alerts should I page on?',
        ],
      },
    ],
  },

  docus: {
    locale: 'en',
  },
})
