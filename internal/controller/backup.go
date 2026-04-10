package controller

import (
	_ "embed"
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
// label limit that applies to most Kubernetes resource names.
func truncateDNS1123(name string) string {
	if len(name) <= 63 {
		return name
	}
	return name[:63]
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
