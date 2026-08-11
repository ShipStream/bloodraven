package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestBuildBackupJob_TLSLayout(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })

	for _, tc := range []struct {
		name      string
		encrypted bool
		tls       bool
	}{
		{name: "plain without TLS"},
		{name: "plain with TLS", tls: true},
		{name: "encrypted without TLS", encrypted: true},
		{name: "encrypted with TLS", encrypted: true, tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithBackup()
			if tc.encrypted {
				fg = fgWithEncryptedBackup()
			}
			if tc.tls {
				fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: "mysql-tls"}
			}
			backup := &v1alpha1.MysqlBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "lion-nightly-abc", Namespace: "ns"},
				Spec: v1alpha1.MysqlBackupSpec{
					FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
					ProfileName:      "nightly-s3",
				},
			}
			job, err := BuildBackupJob(BackupJobInputs{
				FailoverGroup:        fg,
				Profile:              fg.Spec.Backup.Profiles[0],
				Backup:               backup,
				SourceSite:           "pdx",
				CredsSecretName:      "mysql-creds",
				ScriptsConfigMapName: "backup-scripts",
			})
			if err != nil {
				t.Fatalf("BuildBackupJob: %v", err)
			}

			mysqlsh := &job.Spec.Template.Spec.Containers[0]
			if tc.encrypted {
				mysqlsh = &job.Spec.Template.Spec.InitContainers[0]
			}
			assertTLSJobContainer(t, mysqlsh, tc.tls)
			assertTLSJobVolume(t, job.Spec.Template.Spec.Volumes, tc.tls)
		})
	}
}

func TestBuildRestoreJobSpec_TLSLayout(t *testing.T) {
	for _, tc := range []struct {
		name string
		tls  bool
	}{
		{name: "without TLS"},
		{name: "with TLS", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithBackup()
			if tc.tls {
				fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: "mysql-tls"}
			}
			r, _ := newReconciler(fg)
			job, err := r.buildRestoreJobSpec(context.Background(), fg, restoreJobInputs{
				TargetSite: "iad",
				CredsName:  "mysql-creds",
				Source: v1alpha1.InitFromBackupSource{
					PVC: &v1alpha1.InitFromBackupPVCSource{
						ClaimName: "restore-source",
						SubPath:   "dump",
					},
				},
			})
			if err != nil {
				t.Fatalf("buildRestoreJobSpec: %v", err)
			}

			assertTLSJobContainer(t, &job.Spec.Template.Spec.Containers[0], tc.tls)
			assertTLSJobVolume(t, job.Spec.Template.Spec.Volumes, tc.tls)
		})
	}
}

func assertTLSJobContainer(t *testing.T, container *corev1.Container, wantTLS bool) {
	t.Helper()
	env := make(map[string]string, len(container.Env))
	for _, item := range container.Env {
		env[item.Name] = item.Value
	}
	caFile, hasCAFile := env["BLOODRAVEN_TLS_CA_FILE"]
	if wantTLS {
		if env["BLOODRAVEN_TLS"] != "1" {
			t.Errorf("%s BLOODRAVEN_TLS = %q, want 1", container.Name, env["BLOODRAVEN_TLS"])
		}
		if !hasCAFile || caFile != mysqlTLSMountPath+"/ca.crt" {
			t.Errorf("%s BLOODRAVEN_TLS_CA_FILE = %q, want %q", container.Name, caFile, mysqlTLSMountPath+"/ca.crt")
		}
		for _, mount := range container.VolumeMounts {
			if mount.Name == "tls" && mount.MountPath == mysqlTLSMountPath && mount.ReadOnly {
				return
			}
		}
		t.Errorf("%s has no read-only tls mount at %s", container.Name, mysqlTLSMountPath)
		return
	}
	if _, ok := env["BLOODRAVEN_TLS"]; ok || hasCAFile {
		t.Errorf("%s non-TLS layout contains TLS env: %+v", container.Name, env)
	}
	for _, mount := range container.VolumeMounts {
		if mount.Name == "tls" || mount.MountPath == mysqlTLSMountPath {
			t.Errorf("%s non-TLS layout contains TLS mount: %+v", container.Name, mount)
		}
	}
}

func assertTLSJobVolume(t *testing.T, volumes []corev1.Volume, wantTLS bool) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != "tls" {
			continue
		}
		if !wantTLS {
			t.Errorf("non-TLS layout contains TLS volume: %+v", volume)
			return
		}
		if volume.Secret == nil || volume.Secret.SecretName != "mysql-tls" {
			t.Errorf("TLS volume = %+v, want Secret mysql-tls", volume)
		}
		return
	}
	if wantTLS {
		t.Error("TLS layout has no tls volume")
	}
}
