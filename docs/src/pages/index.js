import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import Heading from '@theme/Heading';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className="hero-bloodraven">
      <div className="hero-content">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className="hero-actions">
          <Link className="button button--hero button--lg" to="/docs/">
            Get Started
          </Link>
          {/* Plain anchor, not <Link>: the course is a self-contained static
              site under static/course/, so it has no Docusaurus route and the
              broken-link checker would reject a routed link to it. */}
          <a
            className="button button--hero-secondary button--lg"
            href={useBaseUrl('/course/')}>
            Take the Course
          </a>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Home"
      description="Kubernetes operator for MySQL async replication failover across two sites">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
