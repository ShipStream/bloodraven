<script setup lang="ts">
/**
 * Bloodraven against the other Kubernetes MySQL operators.
 *
 * Everything about another project here is a dated snapshot, checked against
 * that project's repository, release notes, docs, CI configuration and issue
 * tracker on `asOf`. Re-verify before changing a cell, and move `asOf` when
 * you do. Prefer the project's own documentation over blog posts; where a
 * cell rests on an open issue report rather than maintainer documentation,
 * link the issue so the reader can judge it.
 *
 * Bloodraven's own figures are counted from this repository: chaos scenarios
 * from `runner.Register(` in internal/playground/scenarios, profiles from
 * internal/playground/runner/profile.go. Keep them in step with that code.
 *
 * Cell text is rendered with v-html so cells can link to sources. It is all
 * static text in this file; never feed it anything user-supplied.
 */
useSeoMeta({
  title: 'Compare Kubernetes MySQL operators',
  description:
    'Bloodraven compared with the Oracle MySQL Operator, Percona\'s two MySQL operators, MOCO, '
    + 'Vitess, mariadb-operator, KubeBlocks, KubeDB, Charmed MySQL and Tungsten: licensing, '
    + 'failover, fencing, multi-site, backups, security, observability and failure testing.',
})

const asOf = '4 November 2026'

type Tone = 'g' | 'p' | 'b' | 'n'
interface Operator { id: string, name: string, meta: string, href: string }
interface Row { label: string, cells: string[] }
interface Section { title: string, rows: Row[] }

const operators: Operator[] = [
  { id: 'bloodraven', name: 'Bloodraven', meta: 'ShipStream · v1.6.0', href: 'https://github.com/ShipStream/bloodraven' },
  { id: 'oracle', name: 'Oracle MySQL Operator', meta: 'InnoDBCluster · 26.10.0-2.3.1', href: 'https://github.com/mysql/mysql-operator' },
  { id: 'pxc', name: 'Percona PXC', meta: 'XtraDB Cluster · v1.20.0', href: 'https://github.com/percona/percona-xtradb-cluster-operator' },
  { id: 'ps', name: 'Percona PS', meta: 'Percona Server · v1.3.0', href: 'https://github.com/percona/percona-server-mysql-operator' },
  { id: 'moco', name: 'MOCO', meta: 'Cybozu · v0.38.0', href: 'https://github.com/cybozu-go/moco' },
  { id: 'vitess', name: 'Vitess Operator', meta: 'PlanetScale · v2.17.1', href: 'https://github.com/planetscale/vitess-operator' },
  { id: 'mariadb', name: 'mariadb-operator', meta: 'Community · 26.10.2', href: 'https://github.com/mariadb-operator/mariadb-operator' },
  { id: 'kubeblocks', name: 'KubeBlocks MySQL', meta: 'ApeCloud · v1.0.2', href: 'https://github.com/apecloud/kubeblocks-addons' },
  { id: 'kubedb', name: 'KubeDB MySQL', meta: 'AppsCode · v2026.10.12', href: 'https://kubedb.com/kubernetes/databases/run-and-manage-mysql-on-kubernetes/' },
  { id: 'charmed', name: 'Charmed MySQL K8s', meta: 'Canonical · rev457', href: 'https://github.com/canonical/mysql-operators' },
  { id: 'tungsten', name: 'Tungsten Operator', meta: 'Continuent · commercial', href: 'https://docs.continuent.com/operator/' },
]

const gh = (repo: string, n: number) =>
  `<a href="https://github.com/${repo}/issues/${n}" rel="noopener">#${n}</a>`
const ORA = 'mysql/mysql-operator'
const MDB = 'mariadb-operator/mariadb-operator'

// Each cell is "<tone>|<html>": g strong, p partial/preview/paid/caveated,
// b absent/weak/known-broken, n neutral. Order matches `operators`.
const sections: Section[] = [
  {
    title: 'Licensing and cost',
    rows: [
      { label: 'Operator licence', cells: [
        'p|BSL 1.1, source-available. Each version becomes Apache-2.0 two years after release',
        'g|UPL-1.0',
        'g|Apache-2.0',
        'g|Apache-2.0',
        'g|Apache-2.0',
        'g|Apache-2.0',
        'g|MIT',
        'p|AGPL-3.0 since 2023; addons Apache-2.0',
        'b|AppsCode Community License (source-available, non-compete); source private',
        'g|Apache-2.0',
        'b|Proprietary',
      ] },
      { label: 'Free in production?', cells: [
        'p|For individuals, non-profits, non-commercial use and companies under $1M revenue. All non-production use is free',
        'p|Community edition yes; key features need Enterprise',
        'g|Yes, every feature',
        'g|Yes, every feature',
        'g|Yes',
        'g|Yes; turnkey multi-cluster is commercial',
        'p|Yes; MaxScale 25.x and the Enterprise operator are commercial',
        'p|Yes, under AGPL; an Enterprise edition exists',
        'b|No. The free community licence ended in 2024; trials are 30 days',
        'g|Yes',
        'b|No',
      ] },
      { label: 'Features behind a paid tier', cells: [
        'g|None. No licence server, no phone-home',
        'b|Physical and incremental backup (MEB), PITR, ClusterSet, Vault/KMIP/OCI keyrings, audit log',
        'g|None',
        'g|None',
        'g|None',
        'p|Turnkey cross-cluster deployment (PlanetScale)',
        'p|FIPS, drain-triggered switchover, MaxScale 25.x',
        'p|GUI',
        'b|Production use; GUI',
        'g|None',
        'n|Everything',
      ] },
      { label: 'Published price', cells: [
        'n|$990 one-time per failover group or $4,900 per organization; optional update renewals. <a href="/pricing">Pricing</a>',
        'n|MySQL Enterprise Edition, about $5,350 per server per year',
        'n|Support subscription, not published',
        'n|Support subscription, not published',
        'b|No commercial support offered',
        'n|PlanetScale, not published',
        'n|MariaDB Enterprise, not published',
        'n|Enterprise, not published',
        'n|Per GB of database memory, 100 GB minimum',
        'n|Ubuntu Pro support from $500 per node per year',
        'n|Up to $8,400 per node per year, plus support',
      ] },
    ],
  },
  {
    title: 'Project health',
    rows: [
      { label: 'First release', cells: [
        'b|April 2026',
        'g|2022',
        'g|2019',
        'p|GA November 2025',
        'g|2020',
        'g|2020 (Vitess itself 2011, CNCF graduated)',
        'g|2022',
        'p|2022; the project still labels itself alpha',
        'g|2018',
        'g|2021',
        'p|GA May 2025',
      ] },
      { label: 'Releases in the last 12 months', cells: [
        'g|27',
        'g|About 8, quarterly',
        'p|3',
        'g|4',
        'g|8',
        'p|3',
        'g|7',
        'p|1 GA, 70 alpha and beta',
        'g|Frequent',
        'p|About 4',
        'n|Not published',
      ] },
      { label: 'GitHub stars / contributors', cells: [
        'b|New project; one primary maintainer',
        'p|947 / 16',
        'p|625 / about 88',
        'p|234 / about 34',
        'p|385 / about 47',
        'p|374 / about 71 (Vitess: 21k stars)',
        'g|1,033 / about 96',
        'g|3,162 / about 108',
        'n|Source is private',
        'p|New monorepo / 31',
        'n|Closed source',
      ] },
      { label: 'API version', cells: [
        'b|v1alpha1, no conversion webhook yet',
        'g|v2',
        'g|v1',
        'p|v1; async mode in tech preview',
        'p|v1beta2',
        'p|v2',
        'g|Stable',
        'p|v1 with addon churn',
        'g|v1',
        'n|Juju charm',
        'n|Not published',
      ] },
    ],
  },
  {
    title: 'Database and platform',
    rows: [
      { label: 'Database versions', cells: [
        'b|MySQL Community 9.7 LTS only',
        'g|MySQL 8.4, 9.7, 26.x (Community or Enterprise)',
        'g|PXC 8.4, 8.0, 5.7',
        'g|Percona Server 9.7, 8.4, 8.0',
        'p|MySQL 8.0.28 to 8.4',
        'p|MySQL or Percona 8.0, 8.4',
        'n|MariaDB 10.6 and later only',
        'p|MySQL 5.7, 8.0, 8.4',
        'g|MySQL 5.7 to 9.7',
        'p|8.0 stable; 8.4 in candidate',
        'n|MySQL, Percona, MariaDB',
      ] },
      { label: 'Platforms in CI', cells: [
        'p|kind (Kubernetes 1.35) in public CI; managed clouds not tested',
        'p|minikube, k3d, kind, OKE, OpenShift in internal CI',
        'g|GKE, EKS, AKS, OpenShift, DOKS, Minikube',
        'g|GKE, EKS, DOKS, OpenShift, Rancher, Minikube',
        'p|kind, Kubernetes 1.34 to 1.36',
        'p|kind',
        'p|kind',
        'p|EKS, AKS',
        'n|Not published',
        'p|Canonical Kubernetes',
        'n|Not published',
      ] },
      { label: 'arm64', cells: [
        'p|Multi-arch images; not tested in end-to-end CI',
        'p|Multi-arch images',
        'g|Yes',
        'b|Not documented',
        'g|Yes',
        'p|Vitess images',
        'g|Yes',
        'g|Yes',
        'n|Not documented',
        'g|Yes, tested',
        'g|Yes',
      ] },
    ],
  },
  {
    title: 'Topology',
    rows: [
      { label: 'Replication model', cells: [
        'n|Asynchronous GTID replication, every replica directly from the primary',
        'n|Group Replication (InnoDB Cluster), single primary',
        'n|Galera, virtually synchronous',
        'n|Group Replication (GA); async with Orchestrator (tech preview)',
        'n|Loss-less semi-sync that never falls back to async',
        'n|Async or semi-sync per shard, with sharding',
        'n|Async, semi-sync or Galera',
        'n|Semi-sync (default), Group Replication or Orchestrator',
        'n|Group Replication, InnoDB Cluster, semi-sync',
        'n|Group Replication with ClusterSet',
        'n|Async, through the Tungsten replicator',
      ] },
      { label: 'Members needed for HA', cells: [
        'g|2 sites',
        'p|3',
        'p|3',
        'p|3 (Group Replication), 2 (async)',
        'p|3',
        'p|2 or more per shard',
        'p|2 (async), 3 (Galera)',
        'g|2 (semi-sync)',
        'p|3',
        'p|3',
        'p|3',
      ] },
      { label: 'Read replicas', cells: [
        'g|Read-only sites that are never promoted and are fenced if they turn writable; DR-only sites',
        'g|Async read replicas',
        'p|Every node is readable',
        'g|Yes',
        'g|Yes',
        'g|Yes',
        'g|Yes',
        'g|Yes',
        'g|Yes',
        'g|Yes',
        'g|Yes',
      ] },
      { label: 'Sharding', cells: [
        'b|No', 'b|No', 'b|No', 'b|No', 'b|No', 'g|Yes, the core feature', 'b|No', 'b|No', 'b|No', 'b|No', 'b|No',
      ] },
      { label: 'Replicate from an outside MySQL', cells: [
        'b|No; import with a MySQL Shell dump',
        'b|No; seed by clone or dump',
        'g|Yes',
        'p|Through ClusterSet',
        'g|Yes',
        'p|Through import workflows',
        'g|Yes',
        'n|Not documented',
        'g|Yes',
        'p|Through ClusterSet',
        'g|Yes, including other databases',
      ] },
    ],
  },
  {
    title: 'High availability and failover',
    rows: [
      { label: 'What decides a failover', cells: [
        'n|The operator, polling every 2 s',
        'n|Group Replication consensus; the operator repairs membership',
        'n|Galera quorum and proxy health checks',
        'n|Group Replication; async: Orchestrator',
        'n|The operator',
        'n|VTOrc',
        'n|The operator, or MaxScale',
        'n|A sidecar holding a ConfigMap lock',
        'n|A coordinator sidecar',
        'n|Group Replication and Juju hooks',
        'n|Tungsten Manager, by quorum',
      ] },
      { label: 'Documented failover time', cells: [
        'g|About 6 s to detect plus up to 30 s relay-log drain. <a href="/docs/operations/failure-mode-matrix">Failure-mode matrix</a>',
        `b|Not documented. Open reports describe a 2-minute failover and a 22-minute write outage (${gh(ORA, 69)})`,
        'g|About 5 s for Galera, then 10 to 30 s for HAProxy',
        'p|Group Replication: seconds. Async: waits to recover the old primary\'s binlogs, up to a 1 h default timeout',
        'b|Not documented',
        'p|Seconds; no figure published',
        `b|Not documented. An open report describes no failover at all on node power loss (${gh(MDB, 1960)})`,
        'p|"Generally under 30 s" (vendor)',
        'g|About 10 s, rarely 45 s',
        'b|Not documented',
        'b|Not published',
      ] },
      { label: 'Data loss on unplanned failover', cells: [
        'p|Possible, bounded by replica lag. The lost transactions are recorded in status. <a href="/docs/architecture/durability-and-rpo">Durability and RPO</a>',
        'g|None inside one cluster; possible on a forced cross-cluster failover',
        'g|None inside one cluster; possible across sites',
        'g|None with Group Replication. Async: none if the old pod is still reachable, otherwise wait or force with loss',
        'g|None by design; writes block instead',
        'g|None for acknowledged writes with semi-sync durability',
        'p|Possible with async',
        'p|Possible once semi-sync times out',
        'g|None with Group Replication',
        'g|None inside one cluster',
        'p|Possible (async)',
      ] },
      { label: 'Planned switchover without data loss', cells: [
        'g|One annotation or <code>kubectl bloodraven promote</code>; waits for the target to catch up and rolls back on timeout',
        'p|MySQL Shell command; ClusterSet by custom resource',
        'b|No command',
        'g|Yes',
        'g|<code>kubectl moco switchover</code>',
        'g|PlannedReparentShard',
        `g|Yes; an open report describes the primary stuck read-only (${gh(MDB, 1852)})`,
        'g|Yes',
        'g|Yes',
        'g|Juju action',
        'g|Yes',
      ] },
      { label: 'Split-brain protection', cells: [
        'g|<code>super_read_only</code>, read-only boot, a sidecar that fences an isolated primary on its own, and node taints that evict application pods from the old site',
        'p|Quorum and self-abort on expulsion; across clusters the old primary must be fenced by hand',
        'g|Quorum: a minority refuses queries',
        'p|Quorum with Group Replication; async relies on <code>read_only</code> and refuses a forced promotion while the old primary is writable',
        'g|Stops replication on every replica so the old primary cannot get acknowledgements',
        'p|Semi-sync acknowledgements; no STONITH',
        'b|<code>read_only</code> on demotion only',
        `b|Open report: pods start writable (${gh('apecloud/kubeblocks-addons', 3103)})`,
        'p|Quorum with Group Replication',
        'p|Quorum; docs acknowledge split-brain after a partition',
        'p|Quorum',
      ] },
      { label: 'Divergent (errant) transactions', cells: [
        'g|Detected and reported with an event and metric; the site stays fenced, ranks last as a candidate, and needs the divergent GTID confirmed before a reclone',
        'p|Group Replication rejects the member; ClusterSet needs manual steps',
        'n|Not applicable (Galera)',
        'b|Not documented',
        'p|Detected and excluded; recloning is manual',
        'p|Excluded from promotion; no automatic reclone',
        `b|Optional reseed on one error code; open report of forked GTID history (${gh(MDB, 2013)})`,
        'b|Not documented',
        'g|Repaired by pseudo-transaction or clone',
        'n|Handled by Group Replication',
        'b|Not documented',
      ] },
      { label: 'Anti-flap cooldown', cells: [
        'g|Yes, 5 min by default, survives an operator restart',
        'b|Not documented',
        'n|Not applicable',
        'p|Through Orchestrator',
        'b|Not documented',
        'p|Through VTOrc',
        'b|Not documented',
        'b|Not documented',
        'b|Not documented',
        'b|Not documented',
        'b|Not documented',
      ] },
      { label: 'While the operator is down', cells: [
        'b|No promotion; sidecars keep it safe, but writes stop if the primary also dies. <a href="/docs/architecture/operator-availability">Operator availability</a>',
        'p|Failover continues; restarted members stay offline',
        'g|Failover continues',
        'g|Failover continues',
        'b|No failover',
        'g|Failover continues',
        'b|No failover unless MaxScale is used',
        'g|Sidecar-driven',
        'n|Not documented',
        'p|Failover continues; recovery waits',
        'g|Failover continues',
      ] },
    ],
  },
  {
    title: 'Multi-site, disaster recovery and routing',
    rows: [
      { label: 'Automatic failover across zones or regions of one Kubernetes cluster', cells: [
        'g|Yes, the design goal: two or more sites, async so it tolerates WAN latency, with site roles and priorities. <a href="/docs/architecture/multi-site">Multi-site</a>',
        'p|If the group spans 3 zones; latency-sensitive',
        'p|If the cluster spans 3 zones; latency-sensitive',
        'p|If the group spans 3 zones',
        'p|Across zones; writes block on a lagging link',
        'g|Yes, aware of cells',
        'p|Yes',
        'n|Not documented',
        'p|Across zones',
        'p|Across zones',
        'b|Within one region only',
      ] },
      { label: 'Across separate Kubernetes clusters', cells: [
        'b|Manual: restore from S3 with PITR in the other cluster. <a href="/docs/architecture/multi-cluster-dr">Multi-cluster DR</a>',
        'p|ClusterSet (Enterprise); manual failover; needs cross-cluster pod networking',
        'p|Async replication; manual promotion',
        `p|ClusterSet; manual, or a forced failover flagged unsafe (${gh('percona/percona-server-mysql-operator', 1658)})`,
        'p|Replicate from an outside source; manual promotion',
        'p|Do-it-yourself federation, or commercial',
        `p|Manual switchover; an open report describes both clusters left writable (${gh(MDB, 1965)})`,
        'b|Not documented',
        'p|Remote replica; manual promotion',
        'p|ClusterSet; manual; a forced promotion can split-brain',
        'b|No',
      ] },
      { label: 'DNS or global traffic steering', cells: [
        'g|Writes DNSEndpoint records for external-dns, only after the promotion is confirmed',
        'b|No', 'b|No', 'b|No', 'b|No',
        'n|Not needed; VTGate routes',
        'b|No; the load balancer is repointed by hand',
        'b|No', 'b|No', 'b|No', 'b|No',
      ] },
      { label: 'Client routing', cells: [
        'n|No proxy: primary, replica and per-site Services, plus DNS',
        'g|MySQL Router',
        'g|HAProxy or ProxySQL',
        'g|HAProxy or MySQL Router',
        'n|Services only',
        'g|VTGate',
        'p|Optional MaxScale (licence caveats)',
        'p|Services; optional ProxySQL',
        'p|Services; Router for InnoDB Cluster',
        'g|MySQL Router',
        'g|Tungsten Connector',
      ] },
      { label: 'Cache or session failover', cells: [
        'g|Optional Dragonfly that follows the active site',
        'n|—', 'n|—', 'n|—', 'n|—', 'n|—', 'n|—',
        'n|Separate Redis addon', 'n|Separate Redis operator', 'n|—', 'n|—',
      ] },
    ],
  },
  {
    title: 'Backup and restore',
    rows: [
      { label: 'Backup method', cells: [
        'p|Logical only, with MySQL Shell',
        'p|MySQL Shell dump; physical only with Enterprise',
        'g|XtraBackup, physical',
        'g|XtraBackup, physical',
        'p|MySQL Shell dump, logical',
        'g|Built-in, XtraBackup or MySQL Shell',
        'g|mariadb-backup, VolumeSnapshot or dump',
        'g|XtraBackup, VolumeSnapshot or mydumper',
        'g|XtraBackup, logical or VolumeSnapshot',
        'g|XtraBackup',
        'p|Vendor tool',
      ] },
      { label: 'Storage targets', cells: [
        'p|S3 and S3-compatible, PVC',
        'g|PVC, OCI, S3, Azure',
        'g|S3, GCS, Azure, PVC',
        'g|S3, GCS, Azure',
        'g|S3, GCS, Azure',
        'g|S3, GCS, Azure, Ceph, volume',
        'g|S3, Azure, PVC, NFS',
        'g|S3, GCS, OSS, MinIO, PVC',
        'g|S3, GCS, Azure and more',
        'p|S3 only',
        'p|S3, folder',
      ] },
      { label: 'Point-in-time recovery', cells: [
        `p|Yes, into a new group; refuses to replay across sites after a failover (${gh('ShipStream/bloodraven', 206)})`,
        'b|Enterprise only',
        'g|Yes',
        'p|Tech preview',
        'g|Yes, into a new cluster',
        'g|Yes',
        'p|Replication topology only, with open bug reports',
        'g|Yes',
        'g|Yes',
        'b|Disabled on 8.4',
        'n|Not documented',
      ] },
      { label: 'Automated restore testing', cells: [
        'g|Yes: scheduled restores into a throwaway MySQL with PITR replay and a sanity query. <a href="/docs/backup-and-restore/backup-verification">Backup verification</a>',
        'b|No', 'b|No', 'b|No', 'b|No',
        'p|Each scheduled backup restores the previous one first',
        'b|No', 'b|No', 'b|No', 'b|No', 'b|No',
      ] },
      { label: 'Backup encryption', cells: [
        'g|Client-side AES-256-GCM for dumps, binlogs and manifests',
        'p|Depends on storage',
        'p|Storage-side',
        'g|Yes',
        `b|No (${gh('cybozu-go/moco', 845)})`,
        'p|Storage-side',
        'p|SSE-C',
        'g|Yes',
        'g|Yes',
        'b|Storage-side only',
        'b|Not documented',
      ] },
      { label: 'Incremental backup', cells: [
        'b|No; archived binlogs cover the gap',
        'p|Enterprise only',
        'b|No',
        'p|Tech preview',
        'b|No',
        'g|Yes',
        'b|No',
        'g|Yes',
        'p|Through binlog archiving',
        'b|No',
        'b|Not documented',
      ] },
      { label: 'Restore options', cells: [
        'g|New group, in place for the whole instance, or one schema in place',
        'p|New cluster only',
        'p|New or same cluster',
        'p|New or same cluster',
        'p|New cluster; single schema',
        'p|New tablets',
        'g|New or in place; single database',
        'p|New cluster only',
        'g|Same or different database',
        'p|In place',
        'p|In place, with manual steps',
      ] },
    ],
  },
  {
    title: 'Day-2 operations',
    rows: [
      { label: 'Rolling MySQL upgrade', cells: [
        'g|Standby first, then a no-loss handoff; writes pause for up to about 30 s',
        'g|Yes', 'g|Yes, with automatic minor upgrades', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes',
      ] },
      { label: 'Configuration changes on a live cluster', cells: [
        'g|Reconciled',
        `b|Open reports: ignored (${gh(ORA, 63)}, ${gh(ORA, 56)})`,
        'g|Yes', 'g|Yes, without restart where possible', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes', 'g|Yes',
      ] },
      { label: 'Command-line tool', cells: [
        'g|<code>kubectl bloodraven</code> for status, promote, reclone, backup and verify. <a href="/docs/operations/kubectl-plugin">kubectl plugin</a>',
        'b|MySQL Shell only',
        'p|None; GUI through OpenEverest',
        'b|None',
        'g|<code>kubectl moco</code>',
        'g|vtctldclient, VTAdmin',
        'b|None',
        'g|kbcli',
        'g|<code>kubectl dba</code>',
        'g|Juju',
        'g|<code>kubectl tungsten</code>',
      ] },
      { label: 'Databases and users as resources', cells: [
        'g|MysqlDatabase: database, owner, users, grants, limits',
        'b|No',
        'p|Users in the cluster resource',
        'p|Users in the cluster resource',
        'b|No',
        'p|Keyspaces',
        'g|Database, User and Grant resources',
        'g|Yes',
        'g|Yes',
        'p|Through relations',
        'b|Not documented',
      ] },
      { label: 'Coordination with app deploys', cells: [
        'g|Deployment API: migration lock and failover holds, revoked on emergency. <a href="/docs/configuration/deployment-api">Deployment API</a>',
        'b|No', 'b|No', 'b|No', 'b|No', 'p|Online DDL', 'b|No', 'b|No', 'b|No', 'b|No', 'p|Schema-change tooling',
      ] },
    ],
  },
  {
    title: 'Security',
    rows: [
      { label: 'TLS', cells: [
        'g|cert-manager; encrypted transport required',
        'p|From Secrets you supply',
        'g|cert-manager, automatic rotation',
        'g|cert-manager, rotation without restart',
        'p|cert-manager for the agent; operator-to-MySQL traffic is not encrypted',
        `b|No mTLS (${gh('planetscale/vitess-operator', 718)})`,
        'g|Built-in CA or cert-manager',
        'g|Yes',
        'g|cert-manager',
        'g|Through TLS charms',
        'g|cert-manager',
      ] },
      { label: 'Encryption at rest', cells: [
        'p|Keyring file with escrow and safe rotation; not compliance-grade TDE. <a href="/docs/configuration/encryption-at-rest">Encryption at rest</a>',
        'p|Keyring file; Vault, OCI and KMIP with Enterprise',
        'g|Vault keyring',
        'g|Vault keyring',
        'b|No',
        'b|Not managed',
        'b|Not documented',
        'b|Not documented',
        'b|Not documented',
        'b|Not supported',
        'b|Not documented',
      ] },
      { label: 'Signed images', cells: [
        'g|cosign; daily vulnerability scans',
        'n|Not documented', 'n|Not documented', 'n|Not documented', 'n|Not documented', 'n|Not documented',
        'n|Not documented', 'n|Not documented', 'n|Not documented', 'n|Not documented', 'n|Not documented',
      ] },
    ],
  },
  {
    title: 'Observability and documentation',
    rows: [
      { label: 'Metrics', cells: [
        'g|About 57 operator metrics with a ServiceMonitor',
        `b|Bring your own exporter, with an open report it breaks on recent versions (${gh(ORA, 61)}); no operator metrics`,
        'g|PMM',
        'g|PMM',
        'g|Operator and backup metrics',
        'g|Every component',
        'g|Exporters and operator metrics',
        'g|Exporter',
        'g|Exporter',
        'g|Prometheus, Loki, Tempo',
        'n|Not documented',
      ] },
      { label: 'Dashboards and alerts shipped', cells: [
        'p|5 Grafana dashboards; alert rules as an example file. <a href="/docs/observability/alert-runbook-map">Alert runbook map</a>',
        'b|None',
        'p|Through PMM',
        'p|Through PMM',
        'b|None',
        'b|None',
        'b|None',
        'g|Dashboard and alert rules',
        'p|Dashboards, no alerts',
        'g|Dashboards and tested alert rules',
        'p|Vendor dashboard',
      ] },
      { label: 'Failure-mode documentation', cells: [
        'g|Failure-mode matrix with timings, runbooks, RPO matrix, partition guide, published known failures',
        'b|No failover chapter',
        'g|Detailed, with timings',
        'g|Good',
        'g|Precise state documentation',
        'p|Upstream Vitess docs',
        'p|Moderate',
        'p|Moderate',
        'p|Moderate',
        'p|Moderate',
        'p|Vendor manual',
      ] },
    ],
  },
  {
    title: 'Failure testing',
    rows: [
      { label: 'Real-cluster test scenarios', cells: [
        'g|62 chaos scenarios. <a href="/docs/get-started/playground#automated-chaos-suite">Chaos suite</a>',
        'p|51 suites',
        'g|70 suites',
        'g|65 suites',
        'p|16 suites',
        'b|8 for the operator; Vitess itself has about 80 failover tests outside Kubernetes',
        'g|About 117 specs',
        'n|Test code is private',
        'n|Test code is private',
        'g|126 tests',
        'n|Test code is private',
      ] },
      { label: '…that inject a failure', cells: [
        'g|About 51: kills, sustained outages, network partitions, frozen processes, wiped volumes, RBAC, DNS and storage outages, rogue writes, lag',
        'b|About 13: process kills and pod deletes',
        'p|About 9',
        'p|About 9',
        'p|4',
        'b|1',
        'b|About 8: pod and volume deletes',
        'p|15 fault types, from public CI logs',
        'n|Not verifiable',
        'p|About 10',
        'n|Not verifiable',
      ] },
      { label: 'Network-partition tests', cells: [
        'g|Several partition shapes, including partitioning only the operator',
        'b|No', 'p|One pod', 'p|Yes', 'b|No', 'p|Upstream only', 'b|No', 'g|Yes', 'n|Not verifiable', 'g|Yes', 'n|Not verifiable',
      ] },
      { label: 'Deterministic simulation', cells: [
        'g|Seeded fault schedules run against the real control loop on every CI run',
        'b|No', 'b|No', 'b|No', 'b|No', 'b|No', 'b|No', 'b|No', 'b|No', 'b|No', 'b|No',
      ] },
      { label: 'Where the tests run', cells: [
        'p|Public GitHub Actions on kind: a smoke set gates every release, 28 scenarios nightly, the full set on demand',
        'b|Internal CI, results not public',
        'g|GKE on every PR; failure tests at release on six platforms',
        'g|GKE on every PR, failure tests included; six platforms at release',
        'g|Public CI across Kubernetes and MySQL versions',
        'p|kind',
        'g|Public CI on kind',
        'b|Manually triggered on EKS and AKS',
        'b|Private',
        'p|Nightly',
        'b|Private',
      ] },
    ],
  },
]

const visible = ref<Set<string>>(new Set(operators.map(o => o.id)))

function toggle(id: string) {
  if (id === 'bloodraven') return
  const next = new Set(visible.value)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  visible.value = next
}

const shown = computed(() =>
  operators.map((op, i) => ({ op, i })).filter(({ op }) => visible.value.has(op.id)),
)

function cell(raw: string | undefined): { tone: Tone, html: string } {
  const v = raw ?? 'n|'
  const k = v.indexOf('|')
  return { tone: v.slice(0, k) as Tone, html: v.slice(k + 1) }
}
</script>

<template>
  <main class="page">
    <section class="br-shell head">
      <p class="br-kicker">
        Compare
      </p>
      <h1 class="title">
        Bloodraven and the other Kubernetes MySQL operators
      </h1>
      <p class="lede">
        Ten other operators, compared on what you would check before trusting one
        with a production database: licence and cost, how failover decides and
        fences, what happens across sites, backups you can prove restore, and how
        much failure testing stands behind the claims.
      </p>
      <p class="note">
        Checked on <strong>{{ asOf }}</strong> against each project's repository,
        release notes, documentation, CI configuration and issue tracker. Where a
        cell rests on an open issue report rather than the maintainers' own
        documentation, it links the report. Projects change; if a cell is wrong or
        out of date,
        <a class="link br-focus" href="https://github.com/ShipStream/bloodraven/issues/new" rel="noopener">open an issue</a>
        and we will fix it.
      </p>
    </section>

    <section id="summary" class="br-shell block">
      <h2 class="h2">
        The short version
      </h2>
      <div class="prose">
        <p>
          <strong>Pick the replication model first.</strong> Group Replication,
          Galera and loss-less semi-sync lose no committed data when a node fails,
          but they need three members and slow down over long links. Asynchronous
          replication works over a WAN with two sites, at the cost of possible data
          loss on an unplanned failover.
        </p>
        <p>
          <strong>No operator, Bloodraven included, fails over automatically between
          separate Kubernetes clusters.</strong> Every cross-cluster path in this
          table is a manual promotion. Bloodraven automates failover between sites
          of one Kubernetes cluster that spans zones or regions; recovering in a
          different cluster is a restore from S3.
        </p>
        <p>
          <strong>Where Bloodraven is ahead:</strong> automatic failover with only
          two sites, layered fencing that includes evicting application pods from
          the old site, DNS steering, an interlock before discarding divergent
          transactions, automated restore testing, and the most failure testing
          of any project here.
        </p>
        <p>
          <strong>Where Bloodraven is behind:</strong> it is new, with one primary
          maintainer and a v1alpha1 API. It runs MySQL 9.7 only, takes logical
          backups only, has no proxy, and cannot promote while the operator itself
          is down. Companies over $1M revenue pay for production use. Two failure
          scenarios are open known failures; see
          <NuxtLink class="link br-focus" to="/docs/get-started/known-limitations">Known limitations</NuxtLink>.
        </p>
      </div>
    </section>

    <section id="choose" class="br-shell block">
      <h2 class="h2">
        Which one fits
      </h2>
      <ul class="fits">
        <li><span class="fit-need">No data loss inside one cluster, mature and fully open source</span><span class="fit-pick">Percona PXC</span></li>
        <li><span class="fit-need">Upstream Group Replication with support from Oracle</span><span class="fit-pick">Oracle MySQL Operator, with an Enterprise subscription</span></li>
        <li><span class="fit-need">Plain semi-sync replication, no proxy, small codebase</span><span class="fit-pick">MOCO</span></li>
        <li><span class="fit-need">Sharding, or more write volume than one primary can take</span><span class="fit-pick">Vitess</span></li>
        <li><span class="fit-need">MariaDB</span><span class="fit-pick">mariadb-operator</span></li>
        <li><span class="fit-need">One platform for several database engines</span><span class="fit-pick">KubeBlocks or KubeDB</span></li>
        <li><span class="fit-need">Two sites, async, automatic site failover with DNS, and you can accept bounded data loss on a sudden failure</span><span class="fit-pick">Bloodraven</span></li>
      </ul>
    </section>

    <section id="chart" class="br-shell block">
      <h2 class="h2">
        Feature by feature
      </h2>
      <p class="legend">
        <span class="chip" data-tone="g">Strong or present</span>
        <span class="chip" data-tone="p">Partial, preview, paid or caveated</span>
        <span class="chip" data-tone="b">Missing, weak or reported broken</span>
        <span class="chip" data-tone="n">Neutral</span>
      </p>
      <fieldset class="filters">
        <legend class="filters-legend">
          Columns
        </legend>
        <label
          v-for="op in operators"
          :key="op.id"
          class="col-toggle"
          :data-on="visible.has(op.id) ? 'true' : 'false'"
        >
          <input
            type="checkbox"
            :checked="visible.has(op.id)"
            :disabled="op.id === 'bloodraven'"
            @change="toggle(op.id)"
          >
          {{ op.name }}
        </label>
      </fieldset>
    </section>

    <div class="scroller br-focus" tabindex="0" role="region" aria-label="Operator comparison table">
      <table class="cmp-table">
        <thead>
          <tr>
            <th class="rowh" scope="col">
              Capability
            </th>
            <th
              v-for="{ op } in shown"
              :key="op.id"
              scope="col"
              :data-self="op.id === 'bloodraven' ? 'true' : 'false'"
            >
              <a :href="op.href" rel="noopener" class="col-name">{{ op.name }}</a>
              <small>{{ op.meta }}</small>
            </th>
          </tr>
        </thead>
        <tbody v-for="section in sections" :key="section.title">
          <tr class="sec">
            <th :colspan="shown.length + 1" scope="colgroup">
              <span>{{ section.title }}</span>
            </th>
          </tr>
          <tr v-for="row in section.rows" :key="row.label">
            <th class="rowh" scope="row">
              {{ row.label }}
            </th>
            <td
              v-for="{ op, i } in shown"
              :key="op.id"
              :data-tone="cell(row.cells[i]).tone"
              :data-self="op.id === 'bloodraven' ? 'true' : 'false'"
              v-html="cell(row.cells[i]).html"
            />
          </tr>
        </tbody>
      </table>
    </div>

    <section id="method" class="br-shell block">
      <h2 class="h2">
        How the failure tests were counted
      </h2>
      <div class="prose">
        <p>
          Counts come from each repository's test tree and CI configuration, not
          from marketing pages. "Injects a failure" means the test kills, freezes,
          partitions, deletes or denies something and then checks the outcome; a
          test that only exercises a planned switchover or an upgrade does not
          count.
        </p>
        <ul class="method">
          <li><strong>Bloodraven:</strong> 62 registered scenarios. Four need the encryption setup and run in their own CI jobs; two are open known failures, kept quarantined. Every PR runs 190 component tests, 57 envtest tests and a 400-seed deterministic simulation sweep.</li>
          <li><strong>Oracle:</strong> 51 end-to-end files, of which about 13 tests crash a process or delete a pod. No partition or node-loss tests. CI is internal.</li>
          <li><strong>Percona PXC:</strong> 70 end-to-end tests; 3 chaos-mesh suites plus 6 with kill or delete steps. Chaos suites run at release time.</li>
          <li><strong>Percona PS:</strong> 65 kuttl suites, including Group Replication self-healing and async failover with a killed primary; these run on every PR.</li>
          <li><strong>MOCO:</strong> 16 suites with 74 specs; 4 kill or delete during writes.</li>
          <li><strong>Vitess:</strong> the operator has 8 end-to-end scripts. Vitess itself has 49 reparent and 33 VTOrc end-to-end tests that run as local processes.</li>
          <li><strong>mariadb-operator:</strong> about 117 specs on kind; about 8 delete a pod or volume. Multi-cluster specs cover the success path.</li>
          <li><strong>Charmed MySQL:</strong> 126 integration tests; about 10 inject faults with chaos-mesh, drains, freezes and kills.</li>
          <li><strong>KubeBlocks, KubeDB and Tungsten</strong> keep their test code private. KubeBlocks publishes CI logs showing 15 fault types.</li>
        </ul>
      </div>
    </section>

    <section id="not-included" class="br-shell block">
      <h2 class="h2">
        Not in the table
      </h2>
      <div class="prose">
        <p>
          <strong>Bitpoke (Presslabs) mysql-operator</strong> has had no release
          since May 2023 and depends on Orchestrator, whose upstream is archived.
          <strong>RadonDB</strong> has been inactive since early 2025.
          <strong>Oracle's NDB Operator</strong> runs NDB Cluster rather than
          InnoDB replication. <strong>OpenEverest</strong> is a management platform
          over the Percona operators, so its MySQL behaviour is Percona PXC's.
          TiDB, OceanBase and PolarDB-X are MySQL-compatible distributed databases
          rather than MySQL.
        </p>
      </div>
    </section>

    <section class="br-shell block block-last">
      <div class="outbound">
        <NuxtLink class="out br-focus" to="/docs/architecture/why-not-group-replication">
          <span class="out-label">Design</span>
          <span class="out-text">Why Bloodraven uses asynchronous replication rather than Group Replication</span>
        </NuxtLink>
        <NuxtLink class="out br-focus" to="/docs/operations/failure-mode-matrix">
          <span class="out-label">Failure modes</span>
          <span class="out-text">What fails, how it is detected, what the operator does and how long it takes</span>
        </NuxtLink>
        <NuxtLink class="out br-focus" to="/docs/get-started/known-limitations">
          <span class="out-label">Known limitations</span>
          <span class="out-text">What Bloodraven does not do yet, stated plainly</span>
        </NuxtLink>
        <NuxtLink class="out br-focus" to="/pricing">
          <span class="out-label">Pricing</span>
          <span class="out-text">Free for most users; a one-time licence for production at larger companies</span>
        </NuxtLink>
      </div>
    </section>
  </main>
</template>

<style scoped>
.page {
  background: var(--br-paper);
  color: var(--br-text);
  padding-bottom: 96px;
}

.head {
  padding: 72px 0 8px;
}

.head .title,
.head .lede,
.head .note {
  max-width: 760px;
}

.title {
  margin: 12px 0 18px;
  font-size: clamp(2rem, 4vw, 2.8rem);
  line-height: 1.05;
  letter-spacing: -0.04em;
  font-weight: 750;
}

.lede,
.note {
  margin: 0 0 14px;
  color: var(--br-text-dim);
  font-size: 16px;
  line-height: 1.55;
}

.note strong {
  color: var(--br-text);
}

.block {
  padding-top: 48px;
}

.block-last {
  padding-top: 56px;
}

.h2 {
  margin: 0 0 18px;
  font-size: 22px;
  font-weight: 700;
  letter-spacing: -0.02em;
}

.link {
  color: var(--br-red);
  text-decoration: none;
  font-weight: 600;
}

.link:hover {
  text-decoration: underline;
}

/* Prose ------------------------------------------------------------------ */

.prose {
  max-width: 760px;
}

.prose p {
  margin: 0 0 14px;
  color: var(--br-text-dim);
  font-size: 15px;
  line-height: 1.65;
}

.prose strong {
  color: var(--br-text);
}

.method {
  margin: 0;
  padding-left: 18px;
  display: grid;
  gap: 8px;
  color: var(--br-text-dim);
  font-size: 14.5px;
  line-height: 1.55;
}

/* Which one fits ---------------------------------------------------------- */

.fits {
  margin: 0;
  padding: 0;
  list-style: none;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  overflow: hidden;
}

.fits li {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  justify-content: space-between;
  gap: 4px 18px;
  padding: 13px 16px;
  font-size: 15px;
  line-height: 1.45;
}

.fits li + li {
  border-top: 1px solid var(--br-line-light);
}

.fits li:last-child {
  background: color-mix(in srgb, var(--br-red) 7%, transparent);
}

.fit-need {
  color: var(--br-text-dim);
}

.fit-pick {
  flex: none;
  font-weight: 650;
}

/* Legend and filters ----------------------------------------------------- */

.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 0 0 14px;
}

.chip {
  padding: 3px 10px;
  border: 1px solid var(--br-line-light);
  border-radius: 999px;
  font-size: 12.5px;
}

.filters {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 0;
  padding: 0;
  border: 0;
}

.filters-legend {
  float: left;
  margin-right: 6px;
  padding: 4px 0;
  color: var(--br-text-faint);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.col-toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px;
  border: 1px solid var(--br-line-light);
  border-radius: 999px;
  font-size: 13px;
  cursor: pointer;
  user-select: none;
}

.col-toggle[data-on='false'] {
  color: var(--br-text-faint);
}

.col-toggle input {
  accent-color: var(--br-red);
}

/* Tones: tinted from the brand accents so they read in both colour modes. */
[data-tone='g'] { background: color-mix(in srgb, var(--br-green) 18%, transparent); }
[data-tone='p'] { background: color-mix(in srgb, var(--br-amber) 20%, transparent); }
[data-tone='b'] { background: color-mix(in srgb, var(--br-red) 13%, transparent); }
[data-tone='n'] { background: color-mix(in srgb, var(--br-text) 4%, transparent); }

/* Table ------------------------------------------------------------------ */

/* The table is wider than the page shell, so it gets its own scroll box with
   a sticky header row and a sticky capability column. */
.scroller {
  width: var(--br-shell);
  margin: 20px auto 0;
  max-height: 80vh;
  overflow: auto;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  background: var(--br-paper);
}

.cmp-table {
  border-collapse: separate;
  border-spacing: 0;
  font-size: 12.5px;
  line-height: 1.45;
}

.cmp-table th,
.cmp-table td {
  min-width: 180px;
  max-width: 240px;
  padding: 8px 10px;
  border-right: 1px solid var(--br-line-light);
  border-bottom: 1px solid var(--br-line-light);
  vertical-align: top;
  text-align: left;
}

.cmp-table thead th {
  position: sticky;
  top: 0;
  z-index: 3;
  background: var(--br-paper-2);
  font-weight: 700;
}

.cmp-table thead th[data-self='true'] {
  background: var(--br-red);
}

.cmp-table thead th[data-self='true'] .col-name,
.cmp-table thead th[data-self='true'] small {
  color: #fff;
}

.col-name {
  color: var(--br-text);
  font-size: 13px;
  text-decoration: none;
}

.col-name:hover {
  text-decoration: underline;
}

.cmp-table thead small {
  display: block;
  margin-top: 2px;
  color: var(--br-text-faint);
  font-family: var(--br-mono);
  font-size: 10.5px;
  font-weight: 500;
}

.cmp-table .rowh {
  position: sticky;
  left: 0;
  z-index: 2;
  min-width: 190px;
  max-width: 190px;
  background: var(--br-paper-2);
  font-weight: 650;
}

.cmp-table thead .rowh {
  z-index: 4;
}

.sec th {
  padding: 10px 10px 8px;
  background: var(--br-paper);
  color: var(--br-red);
  font-family: var(--br-mono);
  font-size: 10.5px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

/* Keep the section label in view while the body scrolls sideways. */
.sec th span {
  position: sticky;
  left: 10px;
}

.cmp-table td[data-self='true'] {
  box-shadow: inset 3px 0 0 var(--br-red);
}

.cmp-table td :deep(a) {
  color: var(--br-red);
  font-weight: 600;
  text-decoration: none;
}

.cmp-table td :deep(a:hover) {
  text-decoration: underline;
}

.cmp-table td :deep(code) {
  font-family: var(--br-mono);
  font-size: 11.5px;
}

/* Outbound --------------------------------------------------------------- */

.outbound {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
  padding-top: 26px;
  border-top: 1px solid var(--br-line-light);
}

.out {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 18px;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  text-decoration: none;
  transition: border-color 150ms ease, background 150ms ease;
}

.out:hover {
  border-color: color-mix(in srgb, var(--br-red) 45%, transparent);
  background: var(--br-paper-2);
}

.out-label {
  color: var(--br-red);
  font-family: var(--br-mono);
  font-size: 10.5px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.out-text {
  color: var(--br-text-dim);
  font-size: 14px;
  line-height: 1.5;
}

@media (max-width: 600px) {
  .fits li {
    flex-direction: column;
    gap: 4px;
  }

  /* On a phone the sticky capability column would take half the width. */
  .cmp-table .rowh {
    min-width: 112px;
    max-width: 112px;
  }

  .cmp-table th,
  .cmp-table td {
    min-width: 160px;
  }
}

@media (min-width: 760px) {
  .outbound {
    grid-template-columns: repeat(2, 1fr);
  }
}
</style>
