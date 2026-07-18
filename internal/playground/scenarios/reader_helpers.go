package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// Shared helpers for the reader-site scenarios (40-44). All of them
// operate on the playground's 3-site topology: two primary-candidates
// plus exactly one dedicated read-only reader.

// statusSiteByName returns the status entry for the named site, or nil.
func statusSiteByName(mfg *v1alpha1.MysqlFailoverGroup, name string) *v1alpha1.SiteStatus {
	for i := range mfg.Status.Sites {
		if mfg.Status.Sites[i].Name == name {
			return &mfg.Status.Sites[i]
		}
	}
	return nil
}

// playgroundInternalSiteHost is the per-site internal Service FQDN the
// operator uses as the replication source host.
func playgroundInternalSiteHost(group, site, namespace string) string {
	return fmt.Sprintf("mysql-%s-%s-internal.%s.svc.cluster.local", group, site, namespace)
}

// canonicalMySQLHost normalizes a MySQL host string for comparison:
// lower-cased, trimmed, without a :3306 suffix or trailing dot.
func canonicalMySQLHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimSuffix(host, ":3306")
	return strings.TrimSuffix(host, ".")
}

// assertReaderServingStatus asserts the CR status shape of a healthy,
// serving reader: read-only state, replicating, converged directly onto
// expectedHost, with known lag at or below readOnlyMaxLagSeconds.
func assertReaderServingStatus(mfg *v1alpha1.MysqlFailoverGroup, status *v1alpha1.SiteStatus, expectedHost string) error {
	if status.State != "read-only" {
		return fmt.Errorf("state=%q, want read-only", status.State)
	}
	if !status.Replicating {
		return fmt.Errorf("replicating=false")
	}
	if status.SourceConvergenceState != v1alpha1.SourceConvergenceConverged {
		return fmt.Errorf("sourceConvergenceState=%q, want Converged", status.SourceConvergenceState)
	}
	if canonicalMySQLHost(status.SourceHost) != canonicalMySQLHost(expectedHost) {
		return fmt.Errorf("sourceHost=%q, want %q", status.SourceHost, expectedHost)
	}
	if status.SecondsBehindSource == nil {
		return fmt.Errorf("secondsBehindSource is unknown")
	}
	maxLag := mfg.Spec.EffectiveReadOnlyMaxLagSeconds()
	if *status.SecondsBehindSource > maxLag {
		return fmt.Errorf("secondsBehindSource=%d exceeds reader threshold %d", *status.SecondsBehindSource, maxLag)
	}
	return nil
}

// findReadOnlyReaderSite returns the name of the single read-only role
// site, erroring when zero or multiple readers are present.
func findReadOnlyReaderSite(mfg *v1alpha1.MysqlFailoverGroup) (string, error) {
	reader := ""
	for i := range mfg.Spec.Sites {
		if mfg.Spec.Sites[i].IsReadOnlyReader() {
			if reader != "" {
				return "", fmt.Errorf("requires exactly one read-only role site, found at least %q and %q", reader, mfg.Spec.Sites[i].Name)
			}
			reader = mfg.Spec.Sites[i].Name
		}
	}
	if reader == "" {
		return "", fmt.Errorf("requires exactly one read-only role site")
	}
	return reader, nil
}

// readerTopology names the three roles of the playground topology at
// scenario start plus the active site's internal replication host.
type readerTopology struct {
	reader     string
	active     string
	standby    string
	activeHost string
}

// resolveReaderTopology runs the shared healthy-baseline precheck and
// then resolves and validates the reader topology: exactly one reader,
// a promotable active site, a promotable standby, a serving reader
// status converged on the active site, and the reader's client Service
// publishing exactly the reader pod.
func resolveReaderTopology(ctx context.Context, env *runner.Env) (readerTopology, error) {
	var topo readerTopology
	if err := AssertHealthyBaseline(ctx, env); err != nil {
		return topo, err
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return topo, err
	}
	topo.reader, err = findReadOnlyReaderSite(mfg)
	if err != nil {
		return topo, fmt.Errorf("precheck: %w", err)
	}
	topo.active = mfg.Status.ActiveSite
	if site := mfg.Spec.SiteByName(topo.active); site == nil || !site.IsPromotable() {
		return topo, fmt.Errorf("active site %q is missing or non-promotable", topo.active)
	}
	topo.standby, err = PeerOf(mfg, topo.active)
	if err != nil {
		return topo, err
	}
	topo.activeHost = playgroundInternalSiteHost(env.FG, topo.active, env.Namespace)

	readerStatus := statusSiteByName(mfg, topo.reader)
	if readerStatus == nil {
		return topo, fmt.Errorf("reader %q missing from status", topo.reader)
	}
	if err := assertReaderServingStatus(mfg, readerStatus, topo.activeHost); err != nil {
		return topo, fmt.Errorf("reader status precheck: %w", err)
	}

	pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, topo.reader)
	if err != nil {
		return topo, err
	}
	endpoints, err := env.Kube.ServiceEndpointState(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, topo.reader))
	if err != nil {
		return topo, fmt.Errorf("read reader client EndpointSlices: %w", err)
	}
	ready := endpoints.ReadyPodNames("mysql")
	if len(ready) != 1 || ready[0] != pod.Name {
		return topo, fmt.Errorf("reader client Service ready pods=%v, want exactly %s", ready, pod.Name)
	}
	return topo, nil
}

// waitReaderClientEndpoint polls the reader's client Service until its
// EndpointSlices publish exactly the reader's current MySQL pod as
// ready on the mysql port.
func waitReaderClientEndpoint(ctx context.Context, env *runner.Env, reader string, timeout time.Duration) error {
	pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, reader)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		endpoints, err := env.Kube.ServiceEndpointState(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, reader))
		if err != nil {
			last = err.Error()
		} else {
			ready := endpoints.ReadyPodNames("mysql")
			last = fmt.Sprintf("ready=%v", ready)
			if len(ready) == 1 && ready[0] == pod.Name {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("reader client endpoint did not publish pod %s within %s (last: %s)", pod.Name, timeout, last)
}

// seedMarkerRow creates the marker schema on the given site (normally
// the active primary) and inserts one marker row. qualifiedTable must be
// a `db.table` name; the database and table are created on demand.
func seedMarkerRow(ctx context.Context, env *runner.Env, site, qualifiedTable, marker string) error {
	dbName, _, ok := strings.Cut(qualifiedTable, ".")
	if !ok {
		return fmt.Errorf("qualified table %q must be db.table", qualifiedTable)
	}
	client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open MySQL %s: %w", site, err)
	}
	defer client.Close()
	// Statements run one at a time: go-sql-driver rejects placeholder
	// args inside a multi-statement batch.
	for _, stmt := range []struct {
		query string
		args  []any
	}{
		{query: "CREATE DATABASE IF NOT EXISTS " + dbName},
		{query: "CREATE TABLE IF NOT EXISTS " + qualifiedTable + " (marker VARCHAR(128) PRIMARY KEY)"},
		{query: "INSERT INTO " + qualifiedTable + " (marker) VALUES (?)", args: []any{marker}},
	} {
		if _, err := client.Exec(ctx, stmt.query, stmt.args...); err != nil {
			return fmt.Errorf("write marker %q on %s: %w", marker, site, err)
		}
	}
	return nil
}

// waitForMarkerOnSite polls the named site until the marker row is
// visible, proving live end-to-end replication into that site.
func waitForMarkerOnSite(ctx context.Context, env *runner.Env, site, qualifiedTable, marker string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
		if err == nil {
			count, queryErr := client.ScalarInt(ctx, "SELECT COUNT(*) FROM "+qualifiedTable+" WHERE marker=?", marker)
			_ = client.Close()
			if queryErr == nil && count == 1 {
				return nil
			}
			last = queryErr
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("marker %q did not replicate to %s within %s: %v", marker, site, timeout, last)
}
