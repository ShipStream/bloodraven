import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Automated Failover',
    description: (
      <>
        Continuously monitors MySQL replication across two sites. When the
        primary becomes unreachable, Bloodraven promotes the standby, flips DNS,
        and migrates workloads — no human intervention required.
      </>
    ),
  },
  {
    title: 'Split-Brain Safe',
    description: (
      <>
        Sidecar self-fencing ensures an isolated MySQL instance sets{' '}
        <code>super_read_only</code> when it cannot reach the operator or its
        peer, preventing divergent writes during network partitions.
      </>
    ),
  },
  {
    title: 'Zero-Downtime Updates',
    description: (
      <>
        Ordered update strategy rolls out MySQL and sidecar changes one site at
        a time, failing over to the updated standby before updating the old
        primary.
      </>
    ),
  },
];

function Feature({title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
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
