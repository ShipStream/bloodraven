package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// s49SupportHosts scopes the users[] principal to the hosts the scenario's
// own connection arrives from — a port-forward lands on the pod's loopback,
// IPv4 or IPv6 — plus one address nothing here ever uses. No '%': if host
// matching were not real, the support connection below would still work
// from anywhere and the assertion would prove nothing.
var s49SupportHosts = []string{"127.0.0.1", "::1", "203.0.113.7"}

func init() {
	runner.Register(scenario49TenantDatabaseFailover())
}

const (
	s49CRName       = "chaos-tenant-database"
	s49SecretName   = "chaos-tenant-owner"
	s49DatabaseName = "chaos_tenant_wms"
	s49OwnerUser    = "chaos_tenant_app"
	s49OwnerPass    = "chaos-tenant-pw"
	// s49GrantUser is a principal the playground already creates (the
	// replication user). spec.grants[] is grant-only, so the scenario needs
	// a user that exists independently of this CRD — using an existing one
	// is the point, not a shortcut.
	s49GrantUser = "replicator"
	// The spec.users[] entry, shaped like the per-tenant support reader it
	// exists for: Secret-backed, SELECT-only, resource-limited.
	s49UserSecretName = "chaos-tenant-support"

	s49SupportUser = "chaos_tenant_support"
	s49SupportPass = "chaos-support-pw"
)

// scenario49TenantDatabaseFailover creates a MysqlDatabase, waits for Ready,
// then performs a planned switchover and asserts the CR re-reconciles against
// the new primary with its grants intact.
//
// The assertion that matters is the last one: grants replicate, so the
// database and the GRANT rows will be present on the new primary either way.
// What this scenario proves is that the operator noticed the flip and
// re-applied — status.activeSite follows the group — because a CR reporting
// Ready against a primary it has not spoken to since a failover is reporting
// something it does not know.
func scenario49TenantDatabaseFailover() runner.Scenario {
	return runner.Scenario{
		ID:    "49-tenant-database-failover",
		Title: "MysqlDatabase survives a planned switchover",
		Hypothesis: "A MysqlDatabase reaches Ready on the active primary, and after a planned switchover " +
			"the operator re-applies it against the new primary (status.activeSite follows the group) with " +
			"the database, the owner grant, the grants[] entry and the SELECT-only spec.users[] principal " +
			"(schema-scoped, resource-limited, no global privilege) present there.",
		Risk:     "low",
		DocLink:  "site/content/docs/4.configuration/7.tenant-databases.md",
		Timeout:  8 * time.Minute,
		Precheck: s49Precheck,
		Steps: []runner.Step{
			s49CreateTenantDatabase(),
			s49ObserveReady(),
			s49VerifyOnPrimary("initial apply"),
			s49VerifySupportDenied("initial apply"),
			injectPlannedFailoverAnnotation(),
			observePlannedFailoverSucceeded(),
			s49ObserveReappliedOnNewPrimary(),
			s49VerifyOnPrimary("after switchover"),
			s49VerifySupportDenied("after switchover"),
		},
		Cleanup: s49DeleteLeftovers,
	}
}

func s49Precheck(ctx context.Context, env *runner.Env) error {
	if err := AssertHealthyBaseline(ctx, env); err != nil {
		return err
	}
	// spec.grants[] fails the CR when the named user does not exist. That is
	// correct behaviour, but it would make this scenario fail for a reason
	// that has nothing to do with failover, so surface it as a prerequisite.
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	client, err := env.MySQL(mfg.Status.ActiveSite)
	if err != nil {
		return fmt.Errorf("open primary %s: %w", mfg.Status.ActiveSite, err)
	}
	n, err := client.ScalarInt(ctx,
		"SELECT COUNT(*) FROM mysql.user WHERE user = ? AND host = '%'", s49GrantUser)
	if err != nil {
		return fmt.Errorf("check for grants[] user %q: %w", s49GrantUser, err)
	}
	if n == 0 {
		return fmt.Errorf("MySQL user %q does not exist on %s; recreate it (see AGENTS.md, 'After any MySQL data wipe')",
			s49GrantUser, mfg.Status.ActiveSite)
	}
	return s49DeleteLeftovers(ctx, env)
}

func s49CreateTenantDatabase() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create the owner Secret and the MysqlDatabase",
		Do: func(ctx context.Context, env *runner.Env) error {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: s49SecretName, Namespace: env.Namespace},
				Data: map[string][]byte{
					"username": []byte(s49OwnerUser),
					"password": []byte(s49OwnerPass),
				},
			}
			if err := env.Kube.Controller.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create owner secret: %w", err)
			}

			userSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: s49UserSecretName, Namespace: env.Namespace},
				Data: map[string][]byte{
					"username": []byte(s49SupportUser),
					"password": []byte(s49SupportPass),
				},
			}
			if err := env.Kube.Controller.Create(ctx, userSecret); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create users[] secret: %w", err)
			}

			mdb := &v1alpha1.MysqlDatabase{
				ObjectMeta: metav1.ObjectMeta{Name: s49CRName, Namespace: env.Namespace},
				Spec: v1alpha1.MysqlDatabaseSpec{
					GroupRef:     v1alpha1.LocalGroupRef{Name: env.FG},
					DatabaseName: s49DatabaseName,
					Owner:        v1alpha1.MysqlDatabaseOwner{SecretName: s49SecretName},
					Users: []v1alpha1.MysqlDatabaseUser{{
						SecretName: s49UserSecretName,
						Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
						Hosts:      s49SupportHosts,
						ResourceLimits: &v1alpha1.MysqlUserResourceLimits{
							MaxUserConnections: 5,
							MaxQueriesPerHour:  3600,
						},
					}},
					Grants: []v1alpha1.MysqlDatabaseGrant{{
						Username:   s49GrantUser,
						Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeDelete},
					}},
					// Explicit, even though it is the default: this scenario
					// creates a real database and must clean it up.
					DeletionPolicy: v1alpha1.MysqlDatabaseDelete,
				},
			}
			if err := env.Kube.Controller.Create(ctx, mdb); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create mysqldatabase: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("created MysqlDatabase %s/%s for database %s", env.Namespace, s49CRName, s49DatabaseName))
			return nil
		},
	}
}

func s49ObserveReady() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "MysqlDatabase reaches Ready",
		Do: func(ctx context.Context, env *runner.Env) error {
			mdb, err := s49WaitForReady(ctx, env, 3*time.Minute, "")
			if err != nil {
				return err
			}
			return ctxStash(ctx, env, "s49SiteAtFirstApply", mdb.Status.ActiveSite)
		},
	}
}

func s49ObserveReappliedOnNewPrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "MysqlDatabase re-applies against the promoted primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			if target == "" {
				return fmt.Errorf("no switchover target stashed")
			}
			before := ctxFetch(env, "s49SiteAtFirstApply")
			if before == target {
				return fmt.Errorf("switchover target %q equals the site of the first apply; nothing moved", target)
			}
			if _, err := s49WaitForReady(ctx, env, 3*time.Minute, target); err != nil {
				return err
			}
			return nil
		},
	}
}

func s49VerifyOnPrimary(stage string) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "database, owner grant and grants[] entry present on the active primary (" + stage + ")",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			site := mfg.Status.ActiveSite
			if site == "" {
				return fmt.Errorf("group has no active site")
			}
			client, err := env.MySQL(site)
			if err != nil {
				return fmt.Errorf("open primary %s: %w", site, err)
			}
			return s49AssertMySQLState(ctx, client, site)
		},
	}
}

func s49AssertMySQLState(ctx context.Context, client *pgmysql.SiteClient, site string) error {
	n, err := client.ScalarInt(ctx,
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", s49DatabaseName)
	if err != nil {
		return fmt.Errorf("query schemata on %s: %w", site, err)
	}
	if n != 1 {
		return fmt.Errorf("database %q missing on %s", s49DatabaseName, site)
	}

	n, err = client.ScalarInt(ctx,
		"SELECT COUNT(*) FROM mysql.user WHERE user = ? AND host = '%'", s49OwnerUser)
	if err != nil {
		return fmt.Errorf("query mysql.user on %s: %w", site, err)
	}
	if n != 1 {
		return fmt.Errorf("owner user %q missing on %s", s49OwnerUser, site)
	}

	for _, user := range []string{s49OwnerUser, s49GrantUser} {
		n, err = client.ScalarInt(ctx,
			"SELECT COUNT(*) FROM mysql.db WHERE db = ? AND user = ? AND host = '%'", s49DatabaseName, user)
		if err != nil {
			return fmt.Errorf("query mysql.db on %s: %w", site, err)
		}
		if n != 1 {
			return fmt.Errorf("no schema-level grant for %q on %q at %s", user, s49DatabaseName, site)
		}
	}

	// The grant row existing is not enough: assert the requested privileges
	// actually landed. The grants[] entry asks for SELECT and DELETE, so
	// both columns must be Y — a row with either at N would mean the
	// reconciler silently under-granted.
	for _, col := range []string{"Select_priv", "Delete_priv"} {
		v, err := client.ScalarString(ctx,
			"SELECT "+col+" FROM mysql.db WHERE db = ? AND user = ? AND host = '%'", s49DatabaseName, s49GrantUser)
		if err != nil {
			return fmt.Errorf("query %s on %s: %w", col, site, err)
		}
		if v != "Y" {
			return fmt.Errorf("user %q has %s=%q on %q at %s, want Y", s49GrantUser, col, v, s49DatabaseName, site)
		}
	}

	if err := s49AssertSupportUser(ctx, client, site); err != nil {
		return err
	}

	// The privilege this CRD must never confer. Grant_priv on a schema-level
	// row is how WITH GRANT OPTION would show up.
	for _, user := range []string{s49OwnerUser, s49GrantUser, s49SupportUser} {
		granted, err := client.ScalarString(ctx,
			"SELECT Grant_priv FROM mysql.db WHERE db = ? AND user = ? AND host = '%'", s49DatabaseName, user)
		if err != nil {
			return fmt.Errorf("query Grant_priv on %s: %w", site, err)
		}
		if granted != "N" {
			return fmt.Errorf("user %q has Grant_priv=%q on %q at %s; a MysqlDatabase must never confer GRANT OPTION",
				user, granted, s49DatabaseName, site)
		}
	}
	return nil
}

// s49AssertSupportUser proves the spec.users[] principal is what the support
// -access story requires: it exists, it is SELECT-only on this tenant's
// schema, it carries the configured resource limits, and — the cross-tenant
// property the whole initiative exists for — it holds no global privilege and
// no grant on any other schema on this shared group.
func s49AssertSupportUser(ctx context.Context, client *pgmysql.SiteClient, site string) error {
	// One account per declared host, and no '%' account: the host list is
	// the whole principal.
	n, err := client.ScalarInt(ctx,
		"SELECT COUNT(*) FROM mysql.user WHERE user = ?", s49SupportUser)
	if err != nil {
		return fmt.Errorf("query mysql.user for the users[] principal on %s: %w", site, err)
	}
	if n != int64(len(s49SupportHosts)) {
		return fmt.Errorf("spec.users[] principal %q has %d account(s) on %s, want one per host (%d)", s49SupportUser, n, site, len(s49SupportHosts))
	}
	for _, host := range s49SupportHosts {
		n, err := client.ScalarInt(ctx,
			"SELECT COUNT(*) FROM mysql.user WHERE user = ? AND host = ?", s49SupportUser, host)
		if err != nil {
			return fmt.Errorf("query mysql.user for %s@%s on %s: %w", s49SupportUser, host, site, err)
		}
		if n != 1 {
			return fmt.Errorf("spec.users[] principal %q missing on host %s at %s", s49SupportUser, host, site)
		}

		// SELECT-only on this schema, per account: the granted privilege
		// is present and a write privilege is not. Read-only comes from
		// the grant, never from a proxy, so this is the assertion that
		// actually enforces it.
		for col, want := range map[string]string{"Select_priv": "Y", "Insert_priv": "N", "Update_priv": "N", "Delete_priv": "N"} {
			v, err := client.ScalarString(ctx,
				"SELECT "+col+" FROM mysql.db WHERE db = ? AND user = ? AND host = ?", s49DatabaseName, s49SupportUser, host)
			if err != nil {
				return fmt.Errorf("query %s for %s@%s on %s: %w", col, s49SupportUser, host, site, err)
			}
			if v != want {
				return fmt.Errorf("users[] principal %q@%s has %s=%q on %q at %s, want %q",
					s49SupportUser, host, col, v, s49DatabaseName, site, want)
			}
		}

		// Cross-tenant isolation, asserted rather than assumed: no global
		// SELECT (which would read every tenant on this shared group) and
		// no schema grant anywhere but this tenant's own database.
		global, err := client.ScalarString(ctx,
			"SELECT Select_priv FROM mysql.user WHERE user = ? AND host = ?", s49SupportUser, host)
		if err != nil {
			return fmt.Errorf("query global Select_priv on %s: %w", site, err)
		}
		if global != "N" {
			return fmt.Errorf("users[] principal %q@%s has global Select_priv=%q at %s; it would read every tenant on this group",
				s49SupportUser, host, global, site)
		}
		n, err = client.ScalarInt(ctx,
			"SELECT COUNT(*) FROM mysql.db WHERE user = ? AND host = ? AND db <> ?", s49SupportUser, host, s49DatabaseName)
		if err != nil {
			return fmt.Errorf("query foreign schema grants on %s: %w", site, err)
		}
		if n != 0 {
			return fmt.Errorf("users[] principal %q@%s holds %d grant row(s) on schemas other than %q at %s",
				s49SupportUser, host, n, s49DatabaseName, site)
		}

		// The resource limits the CR declared — per account, which is how
		// MySQL enforces them.
		for col, want := range map[string]int64{"max_user_connections": 5, "max_questions": 3600} {
			got, err := client.ScalarInt(ctx,
				"SELECT "+col+" FROM mysql.user WHERE user = ? AND host = ?", s49SupportUser, host)
			if err != nil {
				return fmt.Errorf("query %s on %s: %w", col, site, err)
			}
			if got != want {
				return fmt.Errorf("users[] principal %q@%s has %s=%d at %s, want %d", s49SupportUser, host, col, got, site, want)
			}
		}
	}
	return nil
}

// s49OtherDatabase stands in for a sibling tenant's schema on the same
// shared group: the one thing the support principal must not be able to read.
const s49OtherDatabase = "chaos_tenant_other"

// s49VerifySupportDenied is the behavioral half of the cross-tenant
// assertion. s49AssertSupportUser proves the grant tables are right; this
// step connects AS the users[] principal and proves MySQL actually enforces
// them: a SELECT against a sibling schema on the same group is denied, and a
// write (CREATE TABLE) on its own schema is denied. The sibling schema is
// created by root for the duration of the step and dropped afterwards.
func s49VerifySupportDenied(stage string) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "users[] principal is denied on a sibling schema and cannot write its own (" + stage + ")",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			site := mfg.Status.ActiveSite
			if site == "" {
				return fmt.Errorf("group has no active site")
			}
			root, err := env.MySQL(site)
			if err != nil {
				return fmt.Errorf("open primary %s as root: %w", site, err)
			}
			if _, err := root.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+s49OtherDatabase); err != nil {
				return fmt.Errorf("create sibling schema: %w", err)
			}
			defer func() {
				_, _ = root.Exec(context.WithoutCancel(ctx), "DROP DATABASE IF EXISTS "+s49OtherDatabase)
			}()
			if _, err := root.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+s49OtherDatabase+".secrets (id INT PRIMARY KEY)"); err != nil {
				return fmt.Errorf("create sibling table: %w", err)
			}

			// The principal has no '%' account, so this connection succeeding
			// is itself the proof that host scoping matched the real source
			// address (the port-forward's loopback) rather than a wildcard.
			support, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site,
				pgmysql.Credentials{RootUser: s49SupportUser, RootPassword: s49SupportPass})
			if err != nil {
				return fmt.Errorf("connect to %s as users[] principal %q (hosts %v): %w", site, s49SupportUser, s49SupportHosts, err)
			}
			defer support.Close()

			if err := s49ExpectDenied(ctx, support, "SELECT COUNT(*) FROM "+s49OtherDatabase+".secrets"); err != nil {
				return fmt.Errorf("cross-tenant read: %w", err)
			}
			if err := s49ExpectDenied(ctx, support, "CREATE TABLE "+s49DatabaseName+".should_not_exist (id INT)"); err != nil {
				return fmt.Errorf("write on own schema: %w", err)
			}
			// And the positive control: its own schema is readable, so the
			// denials above are privilege verdicts, not a broken connection.
			if _, err := support.ScalarInt(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", s49DatabaseName); err != nil {
				return fmt.Errorf("users[] principal cannot read its own schema metadata: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("users[] principal %q denied on %s and denied CREATE TABLE on %s at %s", s49SupportUser, s49OtherDatabase, s49DatabaseName, site))
			return nil
		},
	}
}

// s49ExpectDenied runs stmt as the given client and requires MySQL to refuse
// it with an access-denied verdict (ER_TABLEACCESS_DENIED_ERROR 1142 or
// ER_DBACCESS_DENIED_ERROR 1044), not to succeed and not to fail some other
// way.
func s49ExpectDenied(ctx context.Context, c *pgmysql.SiteClient, stmt string) error {
	_, err := c.DB.ExecContext(ctx, stmt)
	if err == nil {
		return fmt.Errorf("%q succeeded for %q; it must be denied", stmt, s49SupportUser)
	}
	var me *mysqldriver.MySQLError
	if !errors.As(err, &me) || (me.Number != 1142 && me.Number != 1044) {
		return fmt.Errorf("%q failed for %q, but not with an access-denied verdict: %w", stmt, s49SupportUser, err)
	}
	return nil
}

// s49WaitForReady polls the CR until it is Ready with observedGeneration
// caught up. When wantSite is non-empty it additionally requires
// status.activeSite to have moved there.
func s49WaitForReady(ctx context.Context, env *runner.Env, timeout time.Duration, wantSite string) (*v1alpha1.MysqlDatabase, error) {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		var mdb v1alpha1.MysqlDatabase
		key := client.ObjectKey{Namespace: env.Namespace, Name: s49CRName}
		if err := env.Kube.Controller.Get(ctx, key, &mdb); err != nil {
			last = err.Error()
		} else {
			switch {
			case mdb.Status.Phase == v1alpha1.MysqlDatabasePhaseFailed:
				// Failed is terminal-shaped; waiting out the timeout would
				// only delay the same verdict with a worse message.
				return nil, fmt.Errorf("MysqlDatabase failed: %s", mdb.Status.Message)
			case mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseReady:
				last = fmt.Sprintf("phase=%s message=%q", mdb.Status.Phase, mdb.Status.Message)
			case mdb.Status.ObservedGeneration != mdb.Generation:
				last = fmt.Sprintf("observedGeneration=%d generation=%d", mdb.Status.ObservedGeneration, mdb.Generation)
			case wantSite != "" && mdb.Status.ActiveSite != wantSite:
				last = fmt.Sprintf("status.activeSite=%q want %q", mdb.Status.ActiveSite, wantSite)
			default:
				return &mdb, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("timed out waiting for MysqlDatabase %s to be Ready: %s", s49CRName, last)
}

// s49DeleteLeftovers removes the CR (deletionPolicy Delete drops the database
// and the owner user, but never the shared grants[] principal) and the owner
// Secret. Also used as a precheck so a previous aborted run cannot leave a
// Ready CR that makes the observe step pass without the operator doing
// anything.
func s49DeleteLeftovers(ctx context.Context, env *runner.Env) error {
	mdb := &v1alpha1.MysqlDatabase{ObjectMeta: metav1.ObjectMeta{Name: s49CRName, Namespace: env.Namespace}}
	if err := env.Kube.Controller.Delete(ctx, mdb); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete mysqldatabase: %w", err)
	}

	// Wait for the finalizer to release so the next run starts clean. A
	// deadline expiry is a hard failure, not a shrug: returning nil here
	// would report clean cleanup over a wedged deletion, and the next run's
	// Create would no-op against the terminating CR and fail somewhere far
	// from the real cause — the exact class of chronic flake PR #112 was
	// about.
	deadline := time.Now().Add(2 * time.Minute)
	released := false
	for time.Now().Before(deadline) {
		var live v1alpha1.MysqlDatabase
		key := client.ObjectKey{Namespace: env.Namespace, Name: s49CRName}
		err := env.Kube.Controller.Get(ctx, key, &live)
		if apierrors.IsNotFound(err) {
			released = true
			break
		}
		if err != nil {
			return fmt.Errorf("wait for mysqldatabase deletion: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if !released {
		var live v1alpha1.MysqlDatabase
		key := client.ObjectKey{Namespace: env.Namespace, Name: s49CRName}
		state := "unknown"
		if err := env.Kube.Controller.Get(ctx, key, &live); err == nil {
			state = fmt.Sprintf("phase=%s finalizers=%v message=%q", live.Status.Phase, live.Finalizers, live.Status.Message)
		}
		return fmt.Errorf("MysqlDatabase %s still terminating after 2m (%s); finalizer never released", s49CRName, state)
	}

	for name, kind := range map[string]string{s49SecretName: "owner", s49UserSecretName: "users[]"} {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace}}
		if err := env.Kube.Controller.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete %s secret: %w", kind, err)
		}
	}
	return nil
}
