package scenarios

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

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
			"the database, the owner grant and the grants[] entry present there.",
		Risk:     "low",
		DocLink:  "site/content/docs/4.configuration/7.tenant-databases.md",
		Timeout:  8 * time.Minute,
		Precheck: s49Precheck,
		Steps: []runner.Step{
			s49CreateTenantDatabase(),
			s49ObserveReady(),
			s49VerifyOnPrimary("initial apply"),
			injectPlannedFailoverAnnotation(),
			observePlannedFailoverSucceeded(),
			s49ObserveReappliedOnNewPrimary(),
			s49VerifyOnPrimary("after switchover"),
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

			mdb := &v1alpha1.MysqlDatabase{
				ObjectMeta: metav1.ObjectMeta{Name: s49CRName, Namespace: env.Namespace},
				Spec: v1alpha1.MysqlDatabaseSpec{
					GroupRef:     v1alpha1.LocalGroupRef{Name: env.FG},
					DatabaseName: s49DatabaseName,
					Owner:        v1alpha1.MysqlDatabaseOwner{SecretName: s49SecretName},
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

	// The privilege this CRD must never confer. Grant_priv on a schema-level
	// row is how WITH GRANT OPTION would show up.
	for _, user := range []string{s49OwnerUser, s49GrantUser} {
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

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: s49SecretName, Namespace: env.Namespace}}
	if err := env.Kube.Controller.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete owner secret: %w", err)
	}
	return nil
}
