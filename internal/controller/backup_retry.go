package controller

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	labelBackupRetryOf           = "shipstream.io/backup-retry-of"
	annotationBackupRetryRoot    = "shipstream.io/backup-retry-root"
	annotationBackupRetryRootUID = "shipstream.io/backup-retry-root-uid"
	annotationBackupAttempt      = "shipstream.io/backup-attempt"
)

// effectiveBackupAttempt survives a lost status patch and remains compatible
// with pre-lineage backups whose attempt was recorded only in status.
func effectiveBackupAttempt(backup *v1alpha1.MysqlBackup) int32 {
	attempt := max(backup.Status.Attempt, 1)
	if n, err := strconv.ParseInt(backup.Annotations[annotationBackupAttempt], 10, 32); err == nil && n > int64(attempt) {
		attempt = int32(n)
	}
	return attempt
}

func retryBackupName(failed *v1alpha1.MysqlBackup, attempt int32) string {
	digest := sha256.Sum256([]byte(failed.UID))
	return fmt.Sprintf("backup-retry-%x-r%d", digest[:16], attempt)
}

func retryBackupMatches(existing, expected *v1alpha1.MysqlBackup) bool {
	for _, key := range []string{labelBackupRetryOf, labelFailoverGroup, labelBackupProfile, labelBackupSchedule} {
		if existing.Labels[key] != expected.Labels[key] {
			return false
		}
	}
	for _, key := range []string{annotationBackupAttempt, annotationBackupRetryRoot, annotationBackupRetryRootUID} {
		if existing.Annotations[key] != expected.Annotations[key] {
			return false
		}
	}
	return existing.Spec.FailoverGroupRef == expected.Spec.FailoverGroupRef && existing.Spec.ProfileName == expected.Spec.ProfileName
}
