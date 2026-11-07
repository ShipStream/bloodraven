package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// credentialGrantsVersion is mixed into the credential hash so a change to
// the role grant sets re-runs credential reconciliation once on upgrade,
// even when no Secret changed. Bump it whenever a role's grants change.
const credentialGrantsVersion = "3"

// appGlobalPrivileges is the app role's grant: data and schema
// privileges on every database, without the administrative privileges
// (SUPER, CONNECTION_ADMIN, CREATE USER, SHUTDOWN, FILE, replication,
// ...) that GRANT ALL would include. Holding SUPER or CONNECTION_ADMIN
// would let app sessions write through read_only.
const appGlobalPrivileges = "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, " +
	"CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, " +
	"CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER, SHOW DATABASES"

// appSystemSchemaRevoke is the partial revoke that keeps the app role from
// writing the mysql schema. appGlobalPrivileges is granted ON *.*, which
// includes mysql.user and mysql.global_grants: an app session could write
// itself SUPER or CONNECTION_ADMIN there and hold it from the next restart
// or FLUSH PRIVILEGES. It lists every schema-level write and DDL privilege
// in appGlobalPrivileges. It needs partial_revokes=ON, and a GRANT on *.*
// drops it again, so it runs after every app GRANT.
const appSystemSchemaRevoke = "REVOKE INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, " +
	"CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, " +
	"CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER ON mysql.* FROM '%s'@'%%'"

// appAdminPrivilegeQuery counts the administrative global privileges an
// account holds: any dynamic privilege, or a static one outside
// appGlobalPrivileges. App accounts created before the grant was narrowed
// hold GRANT ALL and need their global privileges reset.
const appAdminPrivilegeQuery = `SELECT
	(SELECT COUNT(*) FROM mysql.global_grants WHERE USER = ? AND HOST = '%') +
	(SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = '%' AND (
		Super_priv = 'Y' OR Create_user_priv = 'Y' OR Shutdown_priv = 'Y' OR
		File_priv = 'Y' OR Process_priv = 'Y' OR Reload_priv = 'Y' OR
		Repl_slave_priv = 'Y' OR Repl_client_priv = 'Y' OR
		Create_tablespace_priv = 'Y' OR Create_role_priv = 'Y' OR Drop_role_priv = 'Y'))`

// Optional operator Secret keys naming a dedicated replication account. They
// are set together or not at all; with neither, the operator's own username
// and password replicate.
const (
	replicationUserKey     = "MYSQL_REPLICATION_USER"
	replicationPasswordKey = "MYSQL_REPLICATION_PASSWORD"
)

// replicationCredentials resolves the account replicas, clones and
// old-primary recovery use from the operator Secret's data, as the first-boot
// init script resolves it. An empty value counts as absent. With neither
// replication key it returns the operator's username and password. It
// returns a non-empty problem, and no credentials, when only one key is set
// (one half would be borrowed from the operator account, so a password-only
// Secret would present the replication password for the operator account,
// which MySQL does not hold), or when MYSQL_REPLICATION_USER names the
// operator account with a different password (two passwords for one
// account).
func replicationCredentials(data map[string][]byte) (user, pass, problem string) {
	opUser, opPass := string(data["username"]), string(data["password"])
	user, pass = string(data[replicationUserKey]), string(data[replicationPasswordKey])
	switch {
	case user == "" && pass == "":
		return opUser, opPass, ""
	case user == "":
		return "", "", fmt.Sprintf("%s is set without %s; set both to name a dedicated replication account, or omit both to replicate as the operator",
			replicationPasswordKey, replicationUserKey)
	case pass == "":
		return "", "", fmt.Sprintf("%s is set without %s; set both to name a dedicated replication account, or omit both to replicate as the operator",
			replicationUserKey, replicationPasswordKey)
	case user == opUser && pass != opPass:
		return "", "", fmt.Sprintf("%s names the operator account %q, but %s differs from its password; omit both keys to replicate as the operator, or name a dedicated account",
			replicationUserKey, user, replicationPasswordKey)
	}
	return user, pass, ""
}

// replicationGrants is what a dedicated replication account needs: the
// replica IO thread (REPLICATION SLAVE), the replica-status reads the
// bootstrap and recovery paths make (REPLICATION CLIENT), and a clone donor
// session (BACKUP_ADMIN, CLONE_ADMIN).
const replicationGrants = "REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN"

// credentialRole defines a MySQL user role with its expected privileges.
type credentialRole struct {
	name       string
	secretName string
	grants     []string
	// resetAdminPrivileges revokes every global privilege before the
	// grants run when the account holds administrative ones, so an
	// account provisioned with a broader grant converges to grants.
	resetAdminPrivileges bool
	// restrictSystemSchema applies appSystemSchemaRevoke after the grants;
	// see restrictSystemSchema.
	restrictSystemSchema bool
}

// reconcileCredentials ensures MySQL users exist on the primary with
// role-appropriate grants. It is a best-effort operation: failures are
// logged but do not block the main reconcile loop.
func (r *MysqlFailoverGroupReconciler) reconcileCredentials(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	logger := log.FromContext(ctx)

	if fg.Status.ActiveSite == "" {
		logger.V(1).Info("no active site yet, skipping credential reconciliation")
		return nil
	}

	// No writes on the target while spec.initFromBackup has not succeeded
	// (review F004): a point-in-time restore proves the loaded baseline
	// (R2) and the replay (R6) against the target's exact gtid_executed,
	// so a credential write during the load or the replay (a Secret
	// rotation) would fail it. The users the restore Job needs come from
	// the first-boot init script, and the hash annotation is still stale
	// after the restore, so the roles converge on the first reconcile
	// after it succeeds.
	if restoreInFlight(fg) {
		logger.V(1).Info("initFromBackup restore in flight, deferring credential reconciliation")
		return nil
	}

	// Compute hash of all credential secret data.
	currentHash, err := r.computeCredentialHash(ctx, fg)
	if err != nil {
		return fmt.Errorf("compute credential hash: %w", err)
	}

	// Check if credentials have changed since last reconciliation, and
	// whether the app role's mysql schema restriction still has to land.
	rolesCurrent := fg.Annotations[v1alpha1.CredentialHashAnnotation] == currentHash
	restrictionCurrent := fg.Spec.Credentials.AppSecretName == "" ||
		fg.Annotations[v1alpha1.SystemSchemaRestrictionAnnotation] == currentHash
	if rolesCurrent && restrictionCurrent {
		return nil
	}

	// Build the list of roles to reconcile, then fail closed on any
	// collision with a tenant-owned principal BEFORE any SQL: reconcileRole
	// runs ALTER USER ... IDENTIFIED BY plus global grants, which pointed at
	// a tenant owner would reset its password and hand it instance-wide
	// privileges. The MysqlDatabase path refuses group principals
	// (reservedGroupUsernames); this is the reciprocal direction, and the
	// two checks are what makes "disjoint principals" true rather than
	// intended.
	roles := r.buildRoles(ctx, fg)
	claimed, err := r.tenantClaimedUsernames(ctx, fg)
	if err != nil {
		return fmt.Errorf("list tenant database claims: %w", err)
	}
	for _, role := range roles {
		username, _, ok := strings.Cut(role.secretName, "\x00")
		if !ok {
			continue
		}
		if claimer, taken := claimed[username]; taken {
			return fmt.Errorf("role %q resolves to username %q, which MysqlDatabase %q owns; group credentials must not manage tenant principals",
				role.name, username, claimer)
		}
	}

	db, err := openAdminConnection(ctx, r.Client, fg, openMySQL)
	if err != nil {
		return err
	}
	defer db.Close()

	if !rolesCurrent {
		for _, role := range roles {
			if err := reconcileRole(ctx, db, role); err != nil {
				return fmt.Errorf("reconcile %s user: %w", role.name, err)
			}
			logger.Info("reconciled MySQL user", "role", role.name)
		}
		if err := r.setCredentialAnnotation(ctx, fg, v1alpha1.CredentialHashAnnotation, currentHash); err != nil {
			return err
		}
	}

	// A GRANT on *.* drops the restriction, so it follows every role pass.
	// On failure only this step is retried.
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	ensure := func(ctx context.Context) error {
		if r.Runner == nil {
			return errors.New("no topology manager to confirm partial_revokes on every site")
		}
		ctx, cancel := context.WithTimeout(ctx, partialRevokesConfirmTimeout)
		defer cancel()
		return r.Runner.EnsurePartialRevokes(ctx, nn)
	}
	if err := restrictSystemSchema(ctx, db, roles, ensure); err != nil {
		return err
	}
	return r.setCredentialAnnotation(ctx, fg, v1alpha1.SystemSchemaRestrictionAnnotation, currentHash)
}

// tenantClaimedUsernames returns the MySQL usernames owned by live
// MysqlDatabases on fg, mapped to the claiming CR's name. status.ownerUser
// is echoed before the tenant path's first statement runs, and an owner
// rotation target is written ahead as status.pendingOwnerUser the same way,
// so any account the tenant path has touched — or will retry — is claimed.
// CRs being deleted keep their claim: only the finalizer decides whether the
// account survives, and failing closed here costs one defer, not a password
// reset.
func (r *MysqlFailoverGroupReconciler) tenantClaimedUsernames(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (map[string]string, error) {
	var list v1alpha1.MysqlDatabaseList
	if err := r.List(ctx, &list, ctrlclient.InNamespace(fg.Namespace)); err != nil {
		return nil, err
	}
	claimed := make(map[string]string)
	for i := range list.Items {
		mdb := &list.Items[i]
		if mdb.Spec.FailoverGroupRef.Name != fg.Name {
			continue
		}
		if mdb.Status.OwnerUser != "" {
			claimed[mdb.Status.OwnerUser] = mdb.Name
		}
		if mdb.Status.PendingOwnerUser != "" {
			claimed[mdb.Status.PendingOwnerUser] = mdb.Name
		}
		// spec.users[] principals are tenant claims too: their write-ahead
		// ledger (status.appliedUsers) is stamped before their first
		// statement runs, so — like the owner — any account the users[]
		// path has touched or will retry is claimed, including an
		// in-flight rotation target.
		for _, state := range mdb.Status.AppliedUsers {
			if state.Username != "" {
				claimed[state.Username] = mdb.Name
			}
			if state.PendingUsername != "" {
				claimed[state.PendingUsername] = mdb.Name
			}
		}
	}
	return claimed, nil
}

func (r *MysqlFailoverGroupReconciler) buildRoles(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) []credentialRole {
	var roles []credentialRole

	readSecret := func(name string) (string, string) {
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &s); err != nil {
			return "", ""
		}
		return string(s.Data["username"]), string(s.Data["password"])
	}

	// Operator: full admin.
	roles = append(roles, credentialRole{
		name:       "operator",
		secretName: fg.Spec.Credentials.OperatorSecretName,
		grants:     []string{"GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION"},
	})

	if fg.Spec.Credentials.AppSecretName != "" {
		roles = append(roles, credentialRole{
			name:       "app",
			secretName: fg.Spec.Credentials.AppSecretName,
			grants:     []string{"GRANT " + appGlobalPrivileges + " ON *.* TO '%s'@'%%'"},

			resetAdminPrivileges: true,
			restrictSystemSchema: true,
		})
	}

	if fg.Spec.Credentials.ReadOnlySecretName != "" {
		roles = append(roles, credentialRole{
			name:       "readonly",
			secretName: fg.Spec.Credentials.ReadOnlySecretName,
			grants: []string{
				"GRANT SELECT, SHOW VIEW, SHOW DATABASES, PROCESS ON *.* TO '%s'@'%%'",
			},
		})
	}

	if fg.Spec.Credentials.MonitorSecretName != "" {
		roles = append(roles, credentialRole{
			name:       "monitor",
			secretName: fg.Spec.Credentials.MonitorSecretName,
			grants: []string{
				"GRANT PROCESS, REPLICATION CLIENT ON *.* TO '%s'@'%%'",
				"GRANT SELECT ON performance_schema.* TO '%s'@'%%'",
			},
		})
	}

	if fg.Spec.Credentials.BackupSecretName != "" {
		roles = append(roles, credentialRole{
			name:       "backup",
			secretName: fg.Spec.Credentials.BackupSecretName,
			grants: []string{
				"GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER, RELOAD, BACKUP_ADMIN, REPLICATION CLIENT ON *.* TO '%s'@'%%'",
			},
		})
	}

	// Resolve usernames/passwords from secrets.
	resolved := make([]credentialRole, 0, len(roles))
	for _, role := range roles {
		user, pass := readSecret(role.secretName)
		if user == "" || pass == "" {
			continue
		}
		role.secretName = user + "\x00" + pass // pack user+pass for reconcileRole
		resolved = append(resolved, role)
	}
	return resolved
}

func reconcileRole(ctx context.Context, db *sql.DB, role credentialRole) error {
	parts := strings.SplitN(role.secretName, "\x00", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid packed credentials for role %s", role.name)
	}
	username, password := parts[0], parts[1]

	// CREATE USER IF NOT EXISTS + ALTER USER to set password idempotently.
	stmts := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'",
			escapeSingleQuotes(username), escapeSingleQuotes(password)),
		fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'",
			escapeSingleQuotes(username), escapeSingleQuotes(password)),
	}

	if role.resetAdminPrivileges {
		var adminPrivs int
		if err := db.QueryRowContext(ctx, appAdminPrivilegeQuery, username, username).Scan(&adminPrivs); err != nil {
			return fmt.Errorf("check %s admin privileges: %w", role.name, err)
		}
		if adminPrivs > 0 {
			// REVOKE ALL ON *.* also drops dynamic privileges; the grants
			// below restore the role's set straight after.
			stmts = append(stmts, fmt.Sprintf("REVOKE ALL PRIVILEGES ON *.* FROM '%s'@'%%'", escapeSingleQuotes(username)))
		}
	}

	for _, grant := range role.grants {
		stmts = append(stmts, fmt.Sprintf(grant, escapeSingleQuotes(username)))
	}

	stmts = append(stmts, "FLUSH PRIVILEGES")

	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %s: %w", credentialStatementErrorLabel(role.name, stmt), err)
		}
	}
	return nil
}

// credentialRetryInterval is how soon a failed credential reconcile is
// retried.
const credentialRetryInterval = time.Minute

// partialRevokesConfirmTimeout bounds the per-site partial_revokes check.
const partialRevokesConfirmTimeout = 15 * time.Second

// restrictSystemSchema applies appSystemSchemaRevoke to every role that asks
// for it. ensure must first confirm partial_revokes=ON on every site: the
// revoke replicates, and a replica with partial_revokes OFF stops its SQL
// thread on it.
func restrictSystemSchema(ctx context.Context, db *sql.DB, roles []credentialRole, ensure func(context.Context) error) error {
	var restricted []credentialRole
	for _, role := range roles {
		if role.restrictSystemSchema {
			restricted = append(restricted, role)
		}
	}
	if len(restricted) == 0 {
		return nil
	}
	if err := ensure(ctx); err != nil {
		return fmt.Errorf("restrict %s user on the mysql schema: partial_revokes not confirmed on every site: %w",
			restricted[0].name, err)
	}
	for _, role := range restricted {
		username, _, _ := strings.Cut(role.secretName, "\x00")
		stmt := fmt.Sprintf(appSystemSchemaRevoke, escapeSingleQuotes(username))
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %s: %w", credentialStatementErrorLabel(role.name, stmt), err)
		}
	}
	return nil
}

func credentialStatementErrorLabel(roleName, stmt string) string {
	upper := strings.ToUpper(stmt)
	if strings.Contains(upper, "IDENTIFIED BY") {
		return fmt.Sprintf("%s credential statement", roleName)
	}
	return fmt.Sprintf("%s statement %q", roleName, truncateSQL(stmt))
}

// computeCredentialHash fingerprints the consumed keys of the credential Secrets with
// the group's digest key (see secret_digest.go): the result is published in
// an annotation on the group, readable by principals denied Secret access,
// so an unkeyed digest would let them verify password guesses offline.
func (r *MysqlFailoverGroupReconciler) computeCredentialHash(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	key, err := ensureDigestKey(ctx, r.Client, r.APIReader, r.Scheme, fg)
	if err != nil {
		return "", err
	}
	// Only what reconcileCredentials consumes (Δ15): the role accounts
	// and the operator's replication account.
	// A passphrase or decryption Secret is never read here, so a missing
	// one does not fail the reconcile and a passphrase edit does not
	// re-run credential reconciliation.
	secrets := make(map[string]map[string][]byte)
	for name, keys := range credentialHashSecretKeys(fg) {
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &s); err != nil {
			return "", fmt.Errorf("get secret %s: %w", name, err)
		}
		secrets[name] = pickSecretKeys(s.Data, keys)
	}
	h := sha256.New()
	fmt.Fprintf(h, "grants=%s\n", credentialGrantsVersion)
	fmt.Fprintf(h, "secrets=%s\n", secretContentDigest(key, secrets))
	return hex.EncodeToString(h.Sum(nil))[:16], nil
}

func (r *MysqlFailoverGroupReconciler) setCredentialAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, key, hash string) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &fresh); err != nil {
			return err
		}
		base := fresh.DeepCopy()
		if fresh.Annotations == nil {
			fresh.Annotations = make(map[string]string)
		}
		fresh.Annotations[key] = hash
		return r.Patch(ctx, &fresh, metadataPatch(base))
	})
}

// adminCredentials resolves the username/password Bloodraven uses to connect
// to a group's primary as an administrator, plus the root password used for
// the initial-setup fallback, from the operator Secret's username, password
// and MYSQL_ROOT_PASSWORD keys.
func adminCredentials(ctx context.Context, c ctrlclient.Client, fg *v1alpha1.MysqlFailoverGroup) (user, password, rootPassword string, err error) {
	secretName := fg.Spec.Credentials.OperatorSecretName
	if secretName == "" {
		return "", "", "", fmt.Errorf("group %q references no operator credential secret", fg.Name)
	}

	var secret corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: secretName}, &secret); err != nil {
		return "", "", "", fmt.Errorf("get operator secret: %w", err)
	}
	rootPassword = string(secret.Data["MYSQL_ROOT_PASSWORD"])
	return string(secret.Data["username"]), string(secret.Data["password"]), rootPassword, nil
}

// reservedGroupUsernames returns the MySQL account names that belong to the
// MysqlFailoverGroup itself: root, the replication user, and every principal
// named by spec.credentials.
//
// The MysqlDatabase reconciler uses this to refuse to adopt a group-level
// principal as a tenant database owner. The reason is specific and worth
// stating, because nothing else enforces it:
//
// Bloodraven applies ALTER USER … IDENTIFIED BY from the owner Secret's
// bytes. That is desired-state semantics, and it is correct for a user this
// CRD owns. Pointed at a Secret whose `username` is `root` or the operator
// account, the same statement becomes a password reset on a group-level
// credential — which turns "provision a tenant database" into privilege
// escalation for anyone who can write that Secret, and into an outage for
// anyone who mistypes secretName.
//
// The two MySQL-admin call sites are documented as managing disjoint
// principals. This is what makes that true rather than merely intended.
//
// Any Secret named by the group that cannot be read — including one that
// does not exist — fails the check closed, because this is a security gate:
// a Secret deleted mid-rotation (delete + recreate) or a transient API error
// would otherwise silently shrink the reserved set for exactly the window in
// which a tenant Secret naming that group principal would slip through. The
// caller maps NotFound to Pending (a group Secret mid-rotation is an
// ordering problem) and everything else to a reconcile error.
//
// The well-known principals that never come from a Secret are hardcoded:
// root, the conventional replication user, and MySQL's built-in system
// accounts (which pass ValidateMysqlUsername, since account names may
// contain dots).
func reservedGroupUsernames(ctx context.Context, c ctrlclient.Client, fg *v1alpha1.MysqlFailoverGroup) (map[string]bool, error) {
	reserved := map[string]bool{
		"root":             true,
		"replicator":       true,
		"mysql.sys":        true,
		"mysql.session":    true,
		"mysql.infoschema": true,
	}

	add := func(name string) {
		if name != "" {
			reserved[name] = true
		}
	}

	names := []string{
		fg.Spec.Credentials.OperatorSecretName,
		fg.Spec.Credentials.AppSecretName,
		fg.Spec.Credentials.ReadOnlySecretName,
		fg.Spec.Credentials.MonitorSecretName,
		fg.Spec.Credentials.BackupSecretName,
	}

	for _, name := range names {
		if name == "" {
			continue
		}
		var secret corev1.Secret
		if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &secret); err != nil {
			return nil, fmt.Errorf("read group credential secret %s/%s: %w", fg.Namespace, name, err)
		}
		add(string(secret.Data["username"]))
		add(string(secret.Data[replicationUserKey]))
	}
	return reserved, nil
}

// openMySQLFunc is the signature of openMySQL. It exists so component tests
// can substitute an in-memory MySQL model without forking the production
// connection path.
type openMySQLFunc func(user, password, addr, tlsConfigName string) (*sql.DB, error)

// openAdminConnection resolves the operator credential for fg and opens an
// admin connection to the group's current active primary, falling back to
// root for initial setup as the credential reconciler always has — with one
// deliberate exception: an empty or absent MYSQL_ROOT_PASSWORD no longer
// triggers an empty-password root attempt (see the rationale where the
// fallback is applied below).
//
// THIS IS THE ONE PLACE IN BLOODRAVEN THAT HOLDS MYSQL ADMIN. It has exactly
// two callers — reconcileCredentials (group-level roles) and the
// MysqlDatabase reconciler (per-tenant databases). They manage disjoint
// principals, but they share this function on purpose: a second connection
// path would be a second place where root-equivalent credentials are
// assembled, which is precisely the property the MysqlDatabase CRD exists to
// avoid handing out. If you are adding a third caller, that is a design
// decision, not a refactor.
//
// The caller owns the returned *sql.DB and must Close it.
func openAdminConnection(
	ctx context.Context,
	c ctrlclient.Client,
	fg *v1alpha1.MysqlFailoverGroup,
	open openMySQLFunc,
) (*sql.DB, error) {
	logger := log.FromContext(ctx)

	operatorUser, operatorPass, rootPass, err := adminCredentials(ctx, c, fg)
	if err != nil {
		return nil, err
	}

	activeSite := fg.Spec.SiteByName(fg.Status.ActiveSite)
	if activeSite == nil || !activeSite.IsPromotable() {
		return nil, fmt.Errorf("active site %q is not a PrimaryCandidate site", fg.Status.ActiveSite)
	}
	primaryHost := fmt.Sprintf("%s:%d", internalSiteServiceHost(fg.Name, activeSite.Name, fg.Namespace), mysqlPort)

	tlsConfigName := ""
	if fg.Spec.TLS != nil {
		tlsConfigName, err = mysqlTLSConfig(ctx, c, fg, siteServiceHost(fg.Name, activeSite.Name, fg.Namespace))
		if err != nil {
			return nil, fmt.Errorf("configure TLS for admin connection: %w", err)
		}
	}

	// Try operator credentials first, fall back to root for initial setup.
	// An operator Secret without MYSQL_ROOT_PASSWORD must not turn every
	// operator-connect failure into an empty-password root login attempt.
	db, err := open(operatorUser, operatorPass, primaryHost, tlsConfigName)
	if err != nil {
		if rootPass == "" {
			return nil, fmt.Errorf("connect to primary as %s: %w", operatorUser, err)
		}
		logger.Info("operator credentials failed, trying root for initial setup", "error", err)
		db, err = open("root", rootPass, primaryHost, tlsConfigName)
		if err != nil {
			return nil, fmt.Errorf("connect to primary as root: %w", err)
		}
	}
	return db, nil
}

func openMySQL(user, password, addr, tlsConfigName string) (*sql.DB, error) {
	cfg := mysqldriver.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	if tlsConfigName != "" {
		cfg.TLSConfig = tlsConfigName
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Second)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func escapeSingleQuotes(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "'", "''")
}

func truncateSQL(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}
