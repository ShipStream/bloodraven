package controller

import (
	_ "embed"
	"fmt"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Labels / finalizers specific to MysqlBackupVerification. The shared
// failover-group / backup-profile / managed-by labels declared on backup
// resources are reused.
const (
	labelMysqlBackupVerification = "shipstream.io/mysqlbackup-verification"
	labelVerificationSchedule    = "shipstream.io/verification-schedule"

	mysqlBackupVerificationFinalizer = "shipstream.io/mysqlbackup-verification"

	verificationResourceKindCR  = "backup-verification"
	verificationResourceKindCJ  = "backup-verification-schedule"
	verificationResourceKindPod = "backup-verification-pod"
)

//go:embed verify_script.sh
var verifyScript string

// BackupVerifyScript returns the embedded verify.sh used by the Job
// container to bootstrap an ephemeral mysqld and delegate the load to
// the shared restore.py.
func BackupVerifyScript() string { return verifyScript }

// verificationJobName returns the Job name for a given
// MysqlBackupVerification. Same DNS-1123 shape as backup/restore jobs.
func verificationJobName(crName string) string {
	return truncateDNS1123(fmt.Sprintf("mysqlverify-%s", crName))
}

// verificationPVCName returns the ephemeral datadir PVC name for a
// given verification run. One PVC per run; never reused.
func verificationPVCName(crName string) string {
	return truncateDNS1123(fmt.Sprintf("mysqlverify-%s-data", crName))
}

// verificationCredsSecretName returns the derived Secret holding the
// S3 / DSN credentials needed by the load script inside the verification
// Job. Mirrors backupCredsSecretName's naming pattern.
func verificationCredsSecretName(crName string) string {
	return truncateDNS1123(fmt.Sprintf("mysqlverify-%s-creds", crName))
}

// verificationScheduleCronJobName returns the CronJob name for the
// operator-managed CronJob that fires trigger-verification for a given
// (failover group, profile) pair.
func verificationScheduleCronJobName(fgName, profileName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-verify-%s", fgName, profileName))
}

// ensureVerificationLabels stamps the canonical labels on a
// MysqlBackupVerification based on its spec so retention, finalizers,
// and schedule rollups can use label-selector lookups. Returns true if
// anything changed so the caller can decide whether to issue an Update.
func ensureVerificationLabels(v *v1alpha1.MysqlBackupVerification) bool {
	desired := map[string]string{
		labelFailoverGroup: v.Spec.FailoverGroupRef.Name,
		labelBackupProfile: v.Spec.ProfileName,
		labelManagedBy:     managerName,
	}
	changed := false
	if v.Labels == nil {
		v.Labels = map[string]string{}
	}
	for k, val := range desired {
		if val == "" {
			continue
		}
		if v.Labels[k] != val {
			v.Labels[k] = val
			changed = true
		}
	}
	return changed
}
