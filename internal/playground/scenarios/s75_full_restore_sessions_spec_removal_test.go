package scenarios

import (
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestParseS75ClientLog(t *testing.T) {
	log := strings.Join([]string{
		"1763076000 S75_HOLD_START via=primary rc=0 out=",
		"1763076000 S75_PROBE via=site rc=0 out=probe-ok",
		"mysql: [Warning] Using a password on the command line interface can be insecure.",
		"1763076010 S75_HOLD_END via=site rc=1 out=ERROR 2013 (HY000) at line 1: Lost connection to MySQL server during query ",
		"1763076012 S75_PROBE via=site rc=1 out=ERROR 3032 (HY000): The server is currently in the offline mode",
		"1763076012 S75_PROBE via=primary rc=x out=garbage",
		"",
	}, "\n")
	got := parseS75ClientLog([]byte(log))
	if len(got) != 4 {
		t.Fatalf("parsed %d records, want 4: %+v", len(got), got)
	}
	end, ok := s75HoldEnd(got, "site")
	if !ok || end.RC != 1 || !strings.Contains(end.Out, "2013") || !end.At.Equal(time.Unix(1763076010, 0)) {
		t.Fatalf("hold end = %+v, %v", end, ok)
	}
	if _, ok := s75HoldEnd(got, "primary"); ok {
		t.Fatal("primary session reported ended")
	}
	since := s75ProbesSince(got, "site", time.Unix(1763076001, 0))
	if len(since) != 1 || since[0].RC != 1 || !strings.Contains(since[0].Out, "3032") {
		t.Fatalf("site probes since = %+v", since)
	}
	if !s75LastProbeOK(got[:2], "site") || s75LastProbeOK(got, "site") || s75LastProbeOK(got, "primary") {
		t.Fatal("s75LastProbeOK disagrees with the last probe")
	}
}

func s75TestJob() *batchv1.Job {
	return &batchv1.Job{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Volumes: []corev1.Volume{
			{Name: "decrypt-src", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "claim", ReadOnly: true}}},
			{Name: "restore-decrypted", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
		InitContainers: []corev1.Container{{
			Name: "decrypt-download",
			VolumeMounts: []corev1.VolumeMount{
				{Name: "decrypt-src", MountPath: "/restore-src-encrypted", ReadOnly: true},
				{Name: "restore-decrypted", MountPath: "/restore-decrypted"},
			},
		}},
		Containers: []corev1.Container{{
			Name:         "restore",
			Env:          []corev1.EnvVar{{Name: "BLOODRAVEN_LOAD_PROGRESS_FILE", Value: ""}},
			VolumeMounts: []corev1.VolumeMount{{Name: "restore-decrypted", MountPath: "/restore-decrypted", ReadOnly: true}},
		}},
	}}}}
}

func TestS75RestoreJobReadsReadOnlyPVC(t *testing.T) {
	if err := s75RestoreJobReadsReadOnlyPVC(s75TestJob(), "claim"); err != nil {
		t.Fatalf("well-formed Job refused: %v", err)
	}
	cases := map[string]func(*batchv1.Job){
		"read-write volume": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly = false
		},
		"read-write mount": func(j *batchv1.Job) {
			j.Spec.Template.Spec.InitContainers[0].VolumeMounts[0].ReadOnly = false
		},
		"other claim": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other"
		},
		"no decrypt step": func(j *batchv1.Job) {
			j.Spec.Template.Spec.InitContainers[0].Name = "something-else"
		},
		"staging read-write": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].VolumeMounts[0].ReadOnly = false
		},
		"progress file unset": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Env = nil
		},
		"progress file beside the dump": func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Env[0].Value = "/restore-decrypted/progress.json"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			job := s75TestJob()
			mutate(job)
			if err := s75RestoreJobReadsReadOnlyPVC(job, "claim"); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestPVCBackupProfileSpec(t *testing.T) {
	spec := pvcBackupProfileSpec("p", "playground-fast", "1Gi", "secret")
	p := spec.Profiles[0]
	if p.Storage.Type != v1alpha1.BackupStoragePVC || p.Storage.PVC == nil || p.Storage.PVC.Provisioned == nil ||
		*p.Storage.PVC.Provisioned.StorageClassName != "playground-fast" || p.Storage.PVC.Provisioned.Size != "1Gi" {
		t.Fatalf("storage = %+v", p.Storage)
	}
	if p.Encryption == nil || p.Encryption.PassphraseSecretKeyRef.Name != "secret" {
		t.Fatalf("encryption = %+v", p.Encryption)
	}
	if pvcBackupProfileSpec("p", "c", "1Gi", "").Profiles[0].Encryption != nil {
		t.Fatal("unencrypted profile has encryption")
	}
}
