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

	// Read operator credentials for connecting to MySQL.
	operatorSecretName := fg.Spec.Credentials.OperatorSecret
	var operatorSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: operatorSecretName}, &operatorSecret); err != nil {
		return fmt.Errorf("get operator secret: %w", err)
	}
	operatorUser := string(operatorSecret.Data["username"])
	operatorPass := string(operatorSecret.Data["password"])
	rootPass := string(operatorSecret.Data["MYSQL_ROOT_PASSWORD"])

	primaryHost := fmt.Sprintf("mysql-%s-primary.%s.svc.cluster.local:%d", fg.Name, fg.Namespace, mysqlPort)

	// Try operator credentials first, fall back to root for initial setup.
	db, err := openMySQL(operatorUser, operatorPass, primaryHost)
	if err != nil {
		logger.Info("operator credentials failed, trying root for initial setup", "error", err)
		db, err = openMySQL("root", rootPass, primaryHost)
		if err != nil {
			return fmt.Errorf("connect to primary as root: %w", err)
		}
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

func openMySQL(user, password, addr string) (*sql.DB, error) {
	cfg := mysqldriver.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
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
