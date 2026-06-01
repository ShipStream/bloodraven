// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Get Started',
      items: [
        'known-limitations',
        'getting-started',
        'playground',
        'install-production',
        'production-install-examples',
      ],
    },
    {
      type: 'category',
      label: 'Configuration',
      items: [
        'configuration',
        'crd-reference',
        'crd-versioning',
        'placement-contract',
        'app-integration',
        'credentials-and-tls',
        'security-model',
        'production-hardening',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      items: [
        'architecture',
        'multi-site',
        'multi-cluster-dr',
        'why-not-group-replication',
        'operator-availability',
        'durability-and-rpo',
        'architecture-diagrams',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      items: [
        'operations-overview',
        'operations',
        'kubectl-plugin',
        'runbooks',
        'troubleshooting',
        'failover',
        'planned-failover',
        'network-partitions',
        'failure-mode-matrix',
        'upgrade-policy',
        'gitops',
      ],
    },
    {
      type: 'category',
      label: 'Backup and restore',
      items: [
        'backup-overview',
        'backup-restore',
        'backup-s3',
        'backup-pvc',
        'backup-encryption',
        'backup-verification',
      ],
    },
    {
      type: 'category',
      label: 'Observability',
      items: [
        'observability-overview',
        'monitoring',
        'monitoring-prometheus',
        'monitoring-grafana',
        'alert-runbook-map',
        'observability-change-checklist',
        'log-schema',
      ],
    },
    {
      type: 'category',
      label: 'Maintenance',
      items: [
        'examples',
        'docs-maintenance',
      ],
    },
  ],
};

export default sidebars;
