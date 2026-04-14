package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// reconcileInitUsersConfigMap creates or updates the ConfigMap containing the
// shell script that runs during MySQL first-boot (via /docker-entrypoint-initdb.d/).
// The script reads credentials from mounted Secret volumes and creates MySQL
// users with role-appropriate GRANTs.
func (r *MysqlFailoverGroupReconciler) reconcileInitUsersConfigMap(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s-init-users", fg.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if err := controllerutil.SetControllerReference(fg, cm, r.Scheme); err != nil {
			return err
		}
		cm.Labels = map[string]string{
			labelAppName:       "mysql",
			labelInstance:      fg.Name,
			labelFailoverGroup: fg.Name,
			labelManagedBy:     managerName,
		}
		cm.Data = map[string]string{
			"01-bloodraven-users.sh": generateInitUsersScript(fg),
		}
		return nil
	})
	return err
}

func generateInitUsersScript(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.UsesCredentials() {
		return generateCredentialsModeInitScript(fg)
	}
	return generateSecretNameModeInitScript()
}

// generateSecretNameModeInitScript creates the replication user from env vars
// injected by the legacy secretName secret (MYSQL_REPLICATION_USER/PASSWORD).
func generateSecretNameModeInitScript() string {
	return `#!/bin/bash
set -euo pipefail

escape_sql() {
    local val="$1"
    val="${val//\\/\\\\}"
    val="${val//\'/\'\'}"
    printf '%s' "$val"
}

if [ -z "${MYSQL_REPLICATION_USER:-}" ] || [ -z "${MYSQL_REPLICATION_PASSWORD:-}" ]; then
    echo "bloodraven-init: MYSQL_REPLICATION_USER/PASSWORD not set, skipping replication user"
    exit 0
fi

REPL_USER=$(escape_sql "$MYSQL_REPLICATION_USER")
REPL_PASS=$(escape_sql "$MYSQL_REPLICATION_PASSWORD")

echo "bloodraven-init: creating replication user '${REPL_USER}'"
mysql -u root -p"${MYSQL_ROOT_PASSWORD}" <<EOSQL
CREATE USER IF NOT EXISTS '${REPL_USER}'@'%' IDENTIFIED BY '${REPL_PASS}';
ALTER USER '${REPL_USER}'@'%' IDENTIFIED BY '${REPL_PASS}';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN ON *.* TO '${REPL_USER}'@'%';
FLUSH PRIVILEGES;
EOSQL
echo "bloodraven-init: replication user setup complete"
`
}

func generateCredentialsModeInitScript(fg *v1alpha1.MysqlFailoverGroup) string {
	script := `#!/bin/bash
set -euo pipefail

read_cred() { cat "/etc/mysql/creds/$1/$2" 2>/dev/null || echo ""; }

escape_sql() {
    local val="$1"
    val="${val//\\/\\\\}"
    val="${val//\'/\'\'}"
    printf '%s' "$val"
}

create_user_with_grants() {
    local user pass grants
    user=$(escape_sql "$(read_cred "$1" username)")
    pass=$(escape_sql "$(read_cred "$1" password)")
    if [ -z "$user" ] || [ -z "$pass" ]; then
        echo "bloodraven-init: skipping $1 (credentials not mounted)"
        return
    fi
    grants="${2//__USER__/$user}"
    echo "bloodraven-init: creating $1 user '${user}'"
    mysql -u root -p"${MYSQL_ROOT_PASSWORD}" <<EOSQL
CREATE USER IF NOT EXISTS '${user}'@'%' IDENTIFIED BY '${pass}';
ALTER USER '${user}'@'%' IDENTIFIED BY '${pass}';
${grants}
FLUSH PRIVILEGES;
EOSQL
}

`
	// Operator user — full admin for topology management, replication, cloning.
	script += `create_user_with_grants operator "GRANT ALL PRIVILEGES ON *.* TO '__USER__'@'%' WITH GRANT OPTION;"
`

	if fg.Spec.Credentials.AppSecret != "" {
		script += `create_user_with_grants app "GRANT ALL PRIVILEGES ON *.* TO '__USER__'@'%';"
`
	}

	if fg.Spec.Credentials.ReadOnlySecret != "" {
		script += `create_user_with_grants readonly "GRANT SELECT, SHOW VIEW, SHOW DATABASES, PROCESS ON *.* TO '__USER__'@'%';"
`
	}

	if fg.Spec.Credentials.MonitorSecret != "" {
		script += `create_user_with_grants monitor "GRANT PROCESS, REPLICATION CLIENT ON *.* TO '__USER__'@'%'; GRANT SELECT ON performance_schema.* TO '__USER__'@'%';"
`
	}

	if fg.Spec.Credentials.BackupSecret != "" {
		script += `create_user_with_grants backup "GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER, RELOAD, BACKUP_ADMIN, REPLICATION CLIENT ON *.* TO '__USER__'@'%';"
`
	}

	script += `echo "bloodraven-init: user setup complete"
`
	return script
}
