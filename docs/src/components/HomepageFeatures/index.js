import clsx from 'clsx';
import Heading from '@theme/Heading';
import useBaseUrl from '@docusaurus/useBaseUrl';

/* The course is a self-contained static site under static/course/, so it has
   no Docusaurus route. Link to it with a plain anchor and useBaseUrl \u2014 a
   routed <Link> would be rejected by onBrokenLinks: 'throw', and a hardcoded
   path would break if baseUrl ever changes. */
function CourseLink() {
  return <a href={useBaseUrl('/course/')}>Bloodraven in Production</a>;
}

const FeatureList = [
  {
    icon: '\uD83C\uDF93',
    title: 'Get Your Team Up to Speed',
    description: (
      <>
        <CourseLink /> is a hands-on course for devops and sysadmins \u2014 seven
        units, seven graded projects, and a local three-site cluster you break on
        purpose. Your team learns how failover actually behaves \u2014 and how to
        build, secure and upgrade a group \u2014 without reading the docs end to
        end.
      </>
    ),
  },
  {
    icon: '\u26A1',
    title: 'Automated Failover',
    description: (
      <>
        Polls MySQL every 2 seconds. When the primary becomes unreachable,
        Bloodraven promotes the standby, flips DNS via external-dns, and migrates
        workloads via node taints — no human intervention required.
      </>
    ),
  },
  {
    icon: '\uD83D\uDEE1\uFE0F',
    title: 'Split-Brain Safe',
    description: (
      <>
        Five layers of protection including sidecar self-fencing: an isolated
        primary sets <code>super_read_only=ON</code> when it loses contact with
        the operator and its peer, preventing divergent writes.
      </>
    ),
  },
  {
    icon: '\uD83D\uDD04',
    title: 'Zero-Downtime Updates',
    description: (
      <>
        Ordered update strategy rolls out MySQL and sidecar changes one site at
        a time, failing over to the updated standby before updating the old
        primary.
      </>
    ),
  },
  {
    icon: '\uD83D\uDCE1',
    title: 'Real-Time Status',
    description: (
      <>
        WebSocket broadcasts push site state changes to connected clients
        instantly. REST status API and Prometheus metrics provide full
        observability.
      </>
    ),
  },
  {
    icon: '\uD83E\uDEAC',
    title: 'Clone-Based Bootstrap',
    description: (
      <>
        New or replacement replicas are seeded using MySQL's clone plugin with
        GTID auto-positioning — no manual data transfer or snapshot management.
      </>
    ),
  },
  {
    icon: '\uD83D\uDCBE',
    title: 'Backup & Restore',
    description: (
      <>
        Scheduled and on-demand dumps via <code>util.dumpInstance()</code> to
        S3 or PVC, with structured retention, exponential-backoff retries,
        Prometheus metrics, automatic artifact cleanup on delete, and
        bootstrap-only <code>util.loadDump()</code> restores into a brand-new
        failover group.
      </>
    ),
  },
  {
    icon: '\uD83C\uDFAF',
    title: 'Single Source of Truth',
    description: (
      <>
        One controller, one CRD, one reconciliation loop. No distributed
        consensus, no split-brain coordinator, no coordination problems.
      </>
    ),
  },
  {
    icon: '\uD83C\uDFAE',
    title: 'Interactive Playground',
    description: (
      <>
        Spin up a full Bloodraven cluster locally with a single script. Includes a
        real-time dashboard, counter app, chaos monkey, and DNS visualization — all
        on k3d, kind, or minikube.
      </>
    ),
  },
  {
    icon: '💥',
    title: 'Chaos-Tested in CI',
    description: (
      <>
        49 automated chaos scenarios — primary kills, network partitions,
        split-brain, self-fencing, data wipes, backup and PITR verification —
        run nightly against real Kubernetes clusters, and a smoke subset gates
        every release before artifacts ship.
      </>
    ),
  },
];

function Feature({icon, title, description}) {
  return (
    <div className={clsx('col col--4')} style={{marginBottom: '1.5rem'}}>
      <div className="feature-card">
        <span className="feature-icon">{icon}</span>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className="features-section">
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
