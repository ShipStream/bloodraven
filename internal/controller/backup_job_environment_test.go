package controller

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestBackupJobDestination_OrderedEnvironmentExpansion(t *testing.T) {
	unknown := &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}
	for _, tc := range []struct {
		name string
		env  []corev1.EnvVar
		want string
		ok   bool
	}{
		{name: "earlier-variable", env: []corev1.EnvVar{{Name: "BLOODRAVEN_OUTPUT_URL", Value: "$(BLOODRAVEN_STORAGE_TYPE)/dump"}}, want: "S3/dump", ok: true},
		{name: "duplicate-and-self-reference", env: []corev1.EnvVar{{Name: "A", Value: "one"}, {Name: "A", Value: "$(A)-two"}, {Name: "BLOODRAVEN_OUTPUT_URL", Value: "$(A)/dump"}}, want: "one-two/dump", ok: true},
		{name: "no-recursive-expansion", env: []corev1.EnvVar{{Name: "A", Value: "$$(UNKNOWN)"}, {Name: "BLOODRAVEN_OUTPUT_URL", Value: "$(A)/$(BLOODRAVEN_STORAGE_TYPE)"}}, want: "$(UNKNOWN)/S3", ok: true},
		{name: "escaped-dollar", env: []corev1.EnvVar{{Name: "BLOODRAVEN_OUTPUT_URL", Value: "$$cash/$$(UNKNOWN)/$$$(BLOODRAVEN_STORAGE_TYPE)"}}, want: "$cash/$(UNKNOWN)/$S3", ok: true},
		{name: "empty-known-value", env: []corev1.EnvVar{{Name: "A", Value: ""}, {Name: "BLOODRAVEN_OUTPUT_URL", Value: "prefix$(A)"}}, want: "prefix", ok: true},
		{name: "literal-dollar-and-unclosed-reference", env: []corev1.EnvVar{{Name: "BLOODRAVEN_OUTPUT_URL", Value: "$cash/$(INCOMPLETE"}}, want: "$cash/$(INCOMPLETE", ok: true},
		{name: "unknown-unused", env: []corev1.EnvVar{{Name: "A", ValueFrom: unknown}, {Name: "BLOODRAVEN_OUTPUT_URL", Value: "literal/path"}}, want: "literal/path", ok: true},
		{name: "forward-reference", env: []corev1.EnvVar{{Name: "BLOODRAVEN_OUTPUT_URL", Value: "$(LATER)"}, {Name: "LATER", Value: "later"}}},
		{name: "unknown-referenced", env: []corev1.EnvVar{{Name: "A", ValueFrom: unknown}, {Name: "BLOODRAVEN_OUTPUT_URL", Value: "$(A)/dump"}}},
		{name: "unknown-transitive", env: []corev1.EnvVar{{Name: "A", ValueFrom: unknown}, {Name: "B", Value: "$(A)"}, {Name: "BLOODRAVEN_OUTPUT_URL", Value: "$(B)/dump"}}},
		{name: "unknown-service-env", env: []corev1.EnvVar{{Name: "BLOODRAVEN_OUTPUT_URL", Value: "$(SERVICE_HOST)/dump"}}},
		{name: "output-from-runtime", env: []corev1.EnvVar{{Name: "BLOODRAVEN_OUTPUT_URL", ValueFrom: unknown}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{}
			env := append([]corev1.EnvVar{{Name: "BLOODRAVEN_STORAGE_TYPE", Value: "S3"}}, tc.env...)
			job.Spec.Template.Spec.Containers = []corev1.Container{{Name: backupJobContainerName, Env: env}}
			_, got, err := backupJobDestination(job)
			if (err == nil) != tc.ok || got != tc.want {
				t.Fatalf("got %q/%v, want %q recoverable=%v", got, err, tc.want, tc.ok)
			}
		})
	}
}
