package controller

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	batchv1 "k8s.io/api/batch/v1"
)

type backupEnvironmentValue struct {
	value string
	known bool
}

// literalArtifactEnvValue preserves an already-resolved path through kubelet's
// EnvVar.Value expansion. Status and source snapshots retain the actual path.
func literalArtifactEnvValue(value string) string {
	return strings.ReplaceAll(value, "$", "$$")
}

// expandBackupEnvironment follows EnvVar.Value's single-pass expansion:
// earlier declarations are visible, $$ escapes $, and replacements are not
// expanded again. Unknown service/envFrom/valueFrom inputs remain unknown.
func expandBackupEnvironment(value string, previous map[string]backupEnvironmentValue) backupEnvironmentValue {
	var out strings.Builder
	known := true
	for i := 0; i < len(value); {
		if value[i] == '$' && i+1 < len(value) {
			switch value[i+1] {
			case '$':
				out.WriteByte('$')
				i += 2
				continue
			case '(':
				if end := strings.IndexByte(value[i+2:], ')'); end >= 0 {
					end += i + 2
					entry := previous[value[i+2:end]]
					if entry.known {
						out.WriteString(entry.value)
					} else {
						known = false
						out.WriteString(value[i : end+1])
					}
					i = end + 1
					continue
				}
			}
		}
		out.WriteByte(value[i])
		i++
	}
	return backupEnvironmentValue{value: out.String(), known: known}
}

func backupJobDestination(job *batchv1.Job) (v1alpha1.BackupStorageType, string, error) {
	return backupJobEnvironmentDestination(job, true)
}

// backupJobRawDestination is only used to recognize previously persisted
// templates that need repair; its result is never treated as a known path.
func backupJobRawDestination(job *batchv1.Job) (v1alpha1.BackupStorageType, string, error) {
	return backupJobEnvironmentDestination(job, false)
}

func backupJobEnvironmentDestination(job *batchv1.Job, expand bool) (v1alpha1.BackupStorageType, string, error) {
	c := backupJobMainContainer(job)
	if c == nil {
		return "", "", fmt.Errorf("backup Job has no dump or uploader container")
	}
	env := make(map[string]backupEnvironmentValue)
	for _, v := range c.Env {
		entry := backupEnvironmentValue{value: v.Value, known: v.ValueFrom == nil}
		if expand && entry.known {
			entry = expandBackupEnvironment(v.Value, env)
		}
		env[v.Name] = entry
	}
	storage := v1alpha1.BackupStorageType(env["BLOODRAVEN_STORAGE_TYPE"].value)
	encrypted, _ := backupJobEncryption(job)
	required := []string{"BLOODRAVEN_STORAGE_TYPE", "BLOODRAVEN_OUTPUT_URL"}
	if encrypted && storage == v1alpha1.BackupStorageS3 {
		required = append(required, "BLOODRAVEN_S3_BUCKET")
	}
	for _, name := range required {
		if !env[name].known {
			return storage, "", fmt.Errorf("backup Job %s depends on unavailable runtime environment", name)
		}
	}
	location, err := backupmeta.DisplayLocation(string(storage), env["BLOODRAVEN_S3_BUCKET"].value, env["BLOODRAVEN_OUTPUT_URL"].value, encrypted)
	return storage, location, err
}
