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

//go:embed cleanup_script.py
var cleanupScript string

// BackupDumpScript returns the embedded Python dump script. Exposed so the
// failover-group reconciler can populate a shared ConfigMap.
func BackupDumpScript() string { return dumpScript }

// BackupRestoreScript returns the embedded Python load script.
func BackupRestoreScript() string { return restoreScript }

// BackupCleanupScript returns the embedded Python cleanup script used by
// the MysqlBackup finalizer to delete the underlying artifact (S3 prefix
// or PVC subdirectory) before the CR is removed.
func BackupCleanupScript() string { return cleanupScript }

// backupScriptsConfigMapName is the name of the shared ConfigMap that holds
// the dump.py / restore.py / cleanup.py scripts for a failover group. One
// per group.
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

// cleanupJobName returns the Job name for the artifact-cleanup Job
// attached to the MysqlBackup finalizer.
func cleanupJobName(backupName string) string {
	return truncateDNS1123(fmt.Sprintf("mysqlbackup-%s-cleanup", backupName))
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

// mysqlImageFor returns the resolved MySQL server image for a failover
// group. Mirrors the resolution used by the Deployment syncer in
// reconciler.go. Exposed here so backup CRs can stamp the tag in use at
// dump time onto MysqlBackup.status.mysqlImage.
func mysqlImageFor(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.Image != "" {
		return fg.Spec.Image
	}
	return defaultMySQLImage
}

// ensureBackupLabels stamps the three canonical labels on a MysqlBackup
// based on its spec. Returns true when it actually changed anything so
// the caller can decide whether to issue an Update. Idempotent.
//
// The three labels are:
//
//	shipstream.io/failover-group   -> spec.failoverGroupRef.name
//	shipstream.io/backup-profile   -> spec.profileName
//	app.kubernetes.io/managed-by   -> bloodraven
//
// Stamping them for every MysqlBackup (including ad-hoc CRs created by
// `kubectl create`) means retention, finalizers, and the failover-group
// rollup can all use label-selector lookups without having to list and
// filter the whole namespace.
func ensureBackupLabels(backup *v1alpha1.MysqlBackup) bool {
	desired := map[string]string{
		labelFailoverGroup: backup.Spec.FailoverGroupRef.Name,
		labelBackupProfile: backup.Spec.ProfileName,
		labelManagedBy:     managerName,
	}
	changed := false
	if backup.Labels == nil {
		backup.Labels = map[string]string{}
	}
	for k, v := range desired {
		if v == "" {
			continue
		}
		if backup.Labels[k] != v {
			backup.Labels[k] = v
			changed = true
		}
	}
	return changed
}

// humanBytes formats a byte count using binary (1024-based) units so the
// Go side can produce the exact same string that backup_script.py writes
// into the BLOODRAVEN_DUMP_COMPLETE sentinel. Values below 1 KiB are
// returned as "<N> B"; everything else gets one decimal and a unit suffix
// from {KiB, MiB, GiB, TiB, PiB, EiB}.
func humanBytes(n int64) string {
	const unit = int64(1024)
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := unit, 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	suffix := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), suffix[exp])
}

// ptr32 returns a pointer to the given int32. Used for Secret.DefaultMode
// in the backup / restore / cleanup Jobs.
func ptr32(v int32) *int32 { return &v }
