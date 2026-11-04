package backupmeta

import (
	"fmt"
	"path"
	"strconv"
	"strings"
)

// Artifact-cleanup exit codes shared by `bloodraven artifact-cleanup` and the
// MysqlBackup finalizer.
//
// The values avoid 1 and 2, which the Go runtime uses for os.Exit(1) paths
// and unrecovered panics, so a crash is never read as a refusal.
const (
	// CleanupExitRejected is a permanent refusal or configuration error
	// (EX_USAGE).
	CleanupExitRejected = 64
	// CleanupExitTransient is a storage error worth retrying (EX_TEMPFAIL).
	CleanupExitTransient = 75
)

const (
	cleanupCompletePrefix = "BLOODRAVEN_CLEANUP_COMPLETE"
	// CleanupFailedPrefix starts the termination message of a failed run.
	CleanupFailedPrefix = "BLOODRAVEN_CLEANUP_FAILED:"
)

// FormatCleanupComplete is the success verdict: the number of objects (S3) or
// files (PVC) that were deleted.
func FormatCleanupComplete(objects int) string {
	return fmt.Sprintf("%s objects=%d", cleanupCompletePrefix, objects)
}

// ParseCleanupComplete returns the deleted-object count from a success
// verdict. ok is false for any other text, including the verdict-less output
// of the legacy cleanup.py.
func ParseCleanupComplete(message string) (objects int, ok bool) {
	for _, line := range strings.Split(message, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != cleanupCompletePrefix || !strings.HasPrefix(fields[1], "objects=") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(fields[1], "objects="))
		if err != nil || n < 0 {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// CleanupArtifactName returns the last path segment of an artifact location,
// which for every producer is the MysqlBackup name.
func CleanupArtifactName(location string) string {
	trimmed := strings.TrimRight(location, "/")
	if trimmed == "" {
		return ""
	}
	return path.Base(trimmed)
}
