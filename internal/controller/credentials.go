package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
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

const credentialHashAnnotation = "shipstream.io/credential-hash"

// credentialRole defines a MySQL user role with its expected privileges.
type credentialRole struct {
	name       string
	secretName string
	grants     []string
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

	// Compute hash of all credential secret data.
	currentHash, err := r.computeCredentialHash(ctx, fg)
	if err != nil {
		return fmt.Errorf("compute credential hash: %w", err)
	}

	// Check if credentials have changed since last reconciliation.
	if fg.Annotations != nil && fg.Annotations[credentialHashAnnotation] == currentHash {
		return nil
	}

	db, err := openAdminConnection(ctx, r.Client, fg, openMySQL)
	if err != nil {
		return err
	}
	defer db.Close()

	// Build the list of roles to reconcile.
	roles := r.buildRoles(ctx, fg)

	for _, role := range roles {
		if err := reconcileRole(ctx, db, role); err != nil {
			return fmt.Errorf("reconcile %s user: %w", role.name, err)
		}
		logger.Info("reconciled MySQL user", "role", role.name)
	}

	// Update the credential hash annotation.
	return r.setCredentialHash(ctx, fg, currentHash)
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
		secretName: fg.Spec.Credentials.OperatorSecret,
		grants:     []string{"GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION"},
	})

	if fg.Spec.Credentials.AppSecret != "" {
		roles = append(roles, credentialRole{
			name:       "app",
			secretName: fg.Spec.Credentials.AppSecret,
			grants:     []string{"GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%'"},
		})
	}

	if fg.Spec.Credentials.ReadOnlySecret != "" {
		roles = append(roles, credentialRole{
			name:       "readonly",
			secretName: fg.Spec.Credentials.ReadOnlySecret,
			grants: []string{
				"GRANT SELECT, SHOW VIEW, SHOW DATABASES, PROCESS ON *.* TO '%s'@'%%'",
			},
		})
	}

	if fg.Spec.Credentials.MonitorSecret != "" {
		roles = append(roles, credentialRole{
			name:       "monitor",
			secretName: fg.Spec.Credentials.MonitorSecret,
			grants: []string{
				"GRANT PROCESS, REPLICATION CLIENT ON *.* TO '%s'@'%%'",
				"GRANT SELECT ON performance_schema.* TO '%s'@'%%'",
			},
		})
	}

	if fg.Spec.Credentials.BackupSecret != "" {
		roles = append(roles, credentialRole{
			name:       "backup",
			secretName: fg.Spec.Credentials.BackupSecret,
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

func credentialStatementErrorLabel(roleName, stmt string) string {
	upper := strings.ToUpper(stmt)
	if strings.Contains(upper, "IDENTIFIED BY") {
		return fmt.Sprintf("%s credential statement", roleName)
	}
	return fmt.Sprintf("%s statement %q", roleName, truncateSQL(stmt))
}

func (r *MysqlFailoverGroupReconciler) computeCredentialHash(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	h := sha256.New()
	names := fg.Spec.AllReferencedSecretNames()
	sort.Strings(names)
	for _, name := range names {
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &s); err != nil {
			return "", fmt.Errorf("get secret %s: %w", name, err)
		}
		keys := make([]string, 0, len(s.Data))
		for k := range s.Data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(h, "%s/%s=%x\n", name, k, sha256.Sum256(s.Data[k]))
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:16], nil
}

func (r *MysqlFailoverGroupReconciler) setCredentialHash(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, hash string) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = make(map[string]string)
		}
		fresh.Annotations[credentialHashAnnotation] = hash
		return r.Update(ctx, &fresh)
	})
}

// openMySQLFunc is the signature of openMySQL. It exists so component tests
// can substitute an in-memory MySQL model without forking the production
// connection path.
type openMySQLFunc func(user, password, addr, tlsConfigName string) (*sql.DB, error)

// openAdminConnection resolves the operator credential for fg and opens an
// admin connection to the group's current active primary, falling back to
// root for initial setup exactly as the credential reconciler always has.
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

	var operatorSecret corev1.Secret
	operatorSecretName := fg.Spec.Credentials.OperatorSecret
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: operatorSecretName}, &operatorSecret); err != nil {
		return nil, fmt.Errorf("get operator secret: %w", err)
	}
	operatorUser := string(operatorSecret.Data["username"])
	operatorPass := string(operatorSecret.Data["password"])
	rootPass := string(operatorSecret.Data["MYSQL_ROOT_PASSWORD"])

	activeSite := fg.Spec.SiteByName(fg.Status.ActiveSite)
	if activeSite == nil || !activeSite.IsPromotable() {
		return nil, fmt.Errorf("active site %q is not a primary-candidate", fg.Status.ActiveSite)
	}
	primaryHost := fmt.Sprintf("%s:%d", internalSiteServiceHost(fg.Name, activeSite.Name, fg.Namespace), mysqlPort)

	tlsConfigName := ""
	if fg.Spec.TLS != nil {
		var err error
		tlsConfigName, err = mysqlTLSConfig(ctx, c, fg, siteServiceHost(fg.Name, activeSite.Name, fg.Namespace))
		if err != nil {
			return nil, fmt.Errorf("configure TLS for admin connection: %w", err)
		}
	}

	// Try operator credentials first, fall back to root for initial setup.
	db, err := open(operatorUser, operatorPass, primaryHost, tlsConfigName)
	if err != nil {
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
