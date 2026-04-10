package controller

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Bloodraven-specific labels added to backup / restore / schedule
// resources. The labelFailoverGroup constant is declared in reconciler.go.
const (
	labelMysqlBackup    = "shipstream.io/mysqlbackup"
	labelBackupProfile  = "shipstream.io/backup-profile"
	labelBackupSchedule = "shipstream.io/backup-schedule"
	labelResourceKind   = "shipstream.io/resource"
)

// Finalizers used by backup-related CRs.
const (
	mysqlBackupFinalizer = "shipstream.io/mysqlbackup"
)

//go:embed backup_script.py
var dumpScript string

//go:embed restore_script.py
var restoreScript string

// BackupDumpScript returns the embedded Python dump script. Exposed so the
// failover-group reconciler can populate a shared ConfigMap.
func BackupDumpScript() string { return dumpScript }

// BackupRestoreScript returns the embedded Python load script.
func BackupRestoreScript() string { return restoreScript }

// backupScriptsConfigMapName is the name of the shared ConfigMap that holds
// the dump.py / restore.py scripts for a failover group. One per group.
func backupScriptsConfigMapName(fgName string) string {
	return fmt.Sprintf("mysql-%s-backup-scripts", fgName)
}

// backupCredsSecretName is the name of the derived Secret that carries the
// per-backup MYSQL_USER/MYSQL_PASSWORD parsed from the group's DSN secret.
func backupCredsSecretName(backupName string) string {
	return fmt.Sprintf("mysqlbackup-%s-creds", backupName)
}

// restoreCredsSecretName is the name of the derived Secret used by the
// one-shot restore Job.
func restoreCredsSecretName(fgName string) string {
	return fmt.Sprintf("mysql-%s-restore-creds", fgName)
}

// backupJobName returns the Job name for a given MysqlBackup.
func backupJobName(backupName string) string {
	return truncateDNS1123(fmt.Sprintf("mysqlbackup-%s", backupName))
}

// restoreJobName returns the restore Job name for a given failover group
// and target site.
func restoreJobName(fgName, siteName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-%s-restore", fgName, siteName))
}

// scheduleCronJobName returns the CronJob name for a given schedule entry.
func scheduleCronJobName(fgName, scheduleName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-backup-%s", fgName, scheduleName))
}

// ownedBackupPVCName returns the operator-managed PVC name for a PVC-backed
// profile when the user does not set claimName.
func ownedBackupPVCName(fgName, profileName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-backup-%s", fgName, profileName))
}

// truncateDNS1123 truncates a name to 63 characters to fit the DNS-1123
// label limit and strips trailing non-alphanumeric characters so the
// result is always a valid Kubernetes resource name. When the raw name
// exceeds the limit we append a short hash of the original so truncated
// names that would otherwise collide stay distinct.
func truncateDNS1123(name string) string {
	if len(name) <= 63 {
		return trimDNS1123Tail(name)
	}
	// Reserve 9 chars for "-" + 8-char hex hash so the truncated body
	// plus the suffix stay within the 63-char label limit.
	const reserve = 9
	sum := sha256.Sum256([]byte(name))
	suffix := "-" + hex.EncodeToString(sum[:])[:8]
	body := name[:63-reserve]
	return trimDNS1123Tail(body) + suffix
}

// trimDNS1123Tail drops any trailing character that isn't a DNS-1123
// alphanumeric. This keeps names like "mysql-lion-backup-daily-" (with
// a trailing dash from truncation) valid.
func trimDNS1123Tail(s string) string {
	for len(s) > 0 {
		c := s[len(s)-1]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			return s
		}
		s = s[:len(s)-1]
	}
	return s
}

// findProfile locates a named BackupProfile on a MysqlFailoverGroup.
// Returns nil if the group has no backup spec or no matching profile.
func findProfile(fg *v1alpha1.MysqlFailoverGroup, name string) *v1alpha1.BackupProfile {
	if fg.Spec.Backup == nil {
		return nil
	}
	for i := range fg.Spec.Backup.Profiles {
		if fg.Spec.Backup.Profiles[i].Name == name {
			return &fg.Spec.Backup.Profiles[i]
		}
	}
	return nil
}

// backupImage returns the resolved backup image for a failover group.
func backupImage(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.Backup != nil && fg.Spec.Backup.Image != "" {
		return fg.Spec.Backup.Image
	}
	return v1alpha1.DefaultBackupImage
}
