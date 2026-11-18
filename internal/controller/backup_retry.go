package controller

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
)

// effectiveBackupAttempt survives a lost status patch and remains compatible
// with pre-lineage backups whose attempt was recorded only in status.
func effectiveBackupAttempt(backup *v1beta1.MysqlBackup) int32 {
	attempt := max(backup.Status.Attempt, 1)
	if n, err := strconv.ParseInt(backup.Annotations[v1beta1.BackupAttemptAnnotation], 10, 32); err == nil && n > int64(attempt) {
		attempt = int32(n)
	}
	return attempt
}

func retryBackupName(failed *v1beta1.MysqlBackup, attempt int32) string {
	digest := sha256.Sum256([]byte(failed.UID))
	return fmt.Sprintf("backup-retry-%x-r%d", digest[:16], attempt)
}

func retryBackupMatches(existing, expected *v1beta1.MysqlBackup) bool {
	for _, key := range []string{v1beta1.BackupRetryOfLabel, v1beta1.FailoverGroupLabel, v1beta1.BackupProfileLabel, v1beta1.BackupScheduleLabel} {
		if existing.Labels[key] != expected.Labels[key] {
			return false
		}
	}
	for _, key := range []string{v1beta1.BackupAttemptAnnotation, v1beta1.BackupRetryRootAnnotation, v1beta1.BackupRetryRootUIDAnnotation} {
		if existing.Annotations[key] != expected.Annotations[key] {
			return false
		}
	}
	return existing.Spec.FailoverGroupRef == expected.Spec.FailoverGroupRef && existing.Spec.ProfileName == expected.Spec.ProfileName
}
