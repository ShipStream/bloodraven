package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
)

// The check line round-trips through the formatter and the parser, with
// every value escaped (a destination with spaces, '%' and '='), and the
// parser refuses anything else.
func TestPITRArchiveCheckLine(t *testing.T) {
	odd, err := pitrarchive.NewS3Destination("archive-bucket", "a b/%20=x", "https://s3.example.test")
	if err != nil {
		t.Fatal(err)
	}
	src := v1alpha1.PITRArchiveSource{Namespace: "ns", Name: "orders", UID: pbtUID}
	id := pitrarchive.CreateArchiveID(pbtUID, odd)
	for _, r := range []PITRArchiveCheckResult{
		{State: PITRArchiveCheckStateAbsent, Destination: odd.String()},
		{State: PITRArchiveCheckStateVerified, ArchiveID: id, Source: src, Destination: odd.String()},
		{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveForeign, ArchiveID: id, Source: src, Destination: odd.String()},
		{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveListFailed, Destination: odd.String()},
	} {
		line := FormatPITRArchiveCheckLine(r)
		if strings.Count(line, " ") != len(pitrCheckLineKeys)+1 {
			t.Fatalf("line %q has unescaped spaces", line)
		}
		got, err := parsePITRArchiveCheckLine("some log\n" + line + "\n")
		if err != nil || got != r {
			t.Fatalf("round trip of %+v = %+v, %v", r, got, err)
		}
	}
	good := FormatPITRArchiveCheckLine(PITRArchiveCheckResult{State: PITRArchiveCheckStateAbsent, Destination: odd.String()})
	for name, msg := range map[string]string{
		"no line":            "panic: boom",
		"garbage":            PITRArchiveCheckLinePrefix + " state=Absent",
		"reordered keys":     strings.Replace(good, "state=Absent reason=", "reason= state=Absent", 1),
		"unknown state":      strings.Replace(good, "state=Absent", "state=Maybe", 1),
		"reason on Absent":   strings.Replace(good, "reason=", "reason=PITRArchiveLegacy", 1),
		"unknown token":      strings.Replace(strings.Replace(good, "state=Absent", "state=Refused", 1), "reason=", "reason=Nope", 1),
		"refused, no reason": strings.Replace(good, "state=Absent", "state=Refused", 1),
		"verified, no id":    strings.Replace(good, "state=Absent", "state=Verified", 1),
		"bad archive id":     strings.Replace(good, "archiveID=", "archiveID=ar1-x", 1),
		"bad destination":    good[:strings.Index(good, "destination=")] + "destination=s3:%2F%2Fnope",
		"bad escape":         strings.Replace(good, "sourceName=", "sourceName=%zz", 1),
	} {
		if _, err := parsePITRArchiveCheckLine(msg); err == nil {
			t.Errorf("%s: parsed %q", name, msg)
		}
	}
}

// Δ26 and §4.4: the check Job has the cleanup Job's pod shape, a
// deterministic name, the binding and snapshot labels, the candidate
// mounted read-only 0400 and required, no service account token, the
// deadline, no retries, and the role's env.
func TestBuildPITRArchiveCheckJob(t *testing.T) {
	prev := operatorImageFromEnv
	operatorImageFromEnv = "bloodraven:test"
	defer func() { operatorImageFromEnv = prev }()

	s3 := pbtProfile("a", "p1", "pass-a")
	pvc := v1alpha1.BackupProfile{Name: "pv", Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{ClaimName: "archive", SubPath: "orders"}}}
	fg := pbtGroup(s3, pvc)
	fg.Spec.Backup.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
	for _, tc := range []struct {
		profile v1alpha1.BackupProfile
		write   bool
		enc     v1alpha1.PITRArchiveBindingEncryption
	}{
		{profile: s3, write: true, enc: passEnc("mysql-orders-pitr-key-0123456789")},
		{profile: pvc, write: false, enc: noneEnc},
	} {
		d, err := pitrprofile.DestinationFor(fg, &tc.profile)
		if err != nil {
			t.Fatal(err)
		}
		target := &pitrBindingTarget{dest: d, write: tc.write, read: !tc.write, profile: &tc.profile}
		entry := &v1alpha1.PITRArchiveBinding{ID: pitrBindingID(d), Destination: d.String(), Encryption: tc.enc}
		job, err := buildPITRArchiveCheckJob(fg, entry, target, tc.enc.KeySecretName)
		if err != nil {
			t.Fatal(err)
		}
		if job.Name != pitrCheckJobName(pbtName, entry.ID, tc.enc.KeySecretName) || len(job.Name) > 63 ||
			job.Name == pitrCheckJobName(pbtName, entry.ID, "other") {
			t.Fatalf("job name %q", job.Name)
		}
		if !metav1.IsControlledBy(job, fg) {
			t.Fatal("check Job is not controlled by the group")
		}
		l := job.Labels
		if l[labelAppName] != appNameBackupJob || l[labelResourceKind] != resourcePITRArchiveCheck || l[labelPITRBindingID] != entry.ID ||
			l[labelFailoverGroup] != pbtName || l[labelPITRKeySecret] != tc.enc.KeySecretName {
			t.Fatalf("labels %v", l)
		}
		spec := job.Spec
		pod := spec.Template.Spec
		if *spec.ActiveDeadlineSeconds != pitrCheckJobActiveDeadlineSeconds || *spec.BackoffLimit != 0 ||
			pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken || pod.RestartPolicy != corev1.RestartPolicyNever ||
			len(pod.Containers) != 1 || pod.ImagePullSecrets[0].Name != "pull" || len(pod.Tolerations) == 0 {
			t.Fatalf("pod shape %+v", spec)
		}
		c := pod.Containers[0]
		env := envMap(c.Env)
		wantRole := PITRArchiveCheckRoleReader
		if tc.write {
			wantRole = PITRArchiveCheckRoleWriter
		}
		if c.Image != "bloodraven:test" || strings.Join(c.Command, " ") != strings.Join(operatorCommand("pitr-archive-check"), " ") ||
			env[PITRArchiveCheckRoleEnv] != wantRole || env["BLOODRAVEN_FAILOVER_GROUP_UID"] != string(pbtUID) ||
			env["BLOODRAVEN_PITR_DESTINATION"] != literalArtifactEnvValue(d.String()) || env["BLOODRAVEN_PITR_MANIFEST_PREFIX"] != d.KeyPrefix ||
			env["BLOODRAVEN_PITR_ENCRYPTION_MODE"] != string(tc.enc.Mode) || env["BLOODRAVEN_PITR_ALGORITHM"] != string(tc.enc.Algorithm) {
			t.Fatalf("container %+v env %v", c, env)
		}
		vols := map[string]corev1.Volume{}
		for _, v := range pod.Volumes {
			vols[v.Name] = v
		}
		key, hasKey := vols[pitrCheckKeyVolume]
		if hasKey != (tc.enc.KeySecretName != "") {
			t.Fatalf("key volume present=%t for %+v", hasKey, tc.enc)
		}
		if hasKey {
			sv := key.Secret
			if sv.SecretName != tc.enc.KeySecretName || *sv.DefaultMode != 0o400 || sv.Optional == nil || *sv.Optional ||
				sv.Items[0].Key != pitrKeySnapshotDataKey || env["BLOODRAVEN_PITR_PASSPHRASE_FILE"] == "" {
				t.Fatalf("key volume %+v", sv)
			}
		}
		switch d.Kind {
		case pitrarchive.DestinationS3:
			if vols["pitr-aws-creds"].Secret == nil || env["BLOODRAVEN_PITR_S3_BUCKET"] != d.Bucket || env["BLOODRAVEN_PITR_STORAGE_TYPE"] != "S3" {
				t.Fatalf("S3 wiring %v", env)
			}
		case pitrarchive.DestinationPVC:
			claim := vols["pitr-archive"].PersistentVolumeClaim
			if claim == nil || claim.ClaimName != d.Claim || !claim.ReadOnly || env["BLOODRAVEN_PITR_PVC_MOUNT_PATH"] != pitrCheckPVCMountPath {
				t.Fatalf("PVC wiring %+v %v", claim, env)
			}
		}
		for _, m := range c.VolumeMounts {
			if m.Name != "tmp" && !m.ReadOnly {
				t.Fatalf("mount %s is writable", m.Name)
			}
		}
	}
	// A candidate that disagrees with the identity under test is a bug.
	d, _ := pitrprofile.DestinationFor(fg, &s3)
	target := &pitrBindingTarget{dest: d, write: true, profile: &s3}
	entry := &v1alpha1.PITRArchiveBinding{ID: pitrBindingID(d), Destination: d.String(), Encryption: passEnc("x")}
	if _, err := buildPITRArchiveCheckJob(fg, entry, target, ""); err == nil {
		t.Fatal("built a plaintext check for a Passphrase identity")
	}
	operatorImageFromEnv = ""
	if _, err := buildPITRArchiveCheckJob(fg, entry, target, "x"); err == nil {
		t.Fatal("built a check Job without an operator image")
	}
}

// §4.4 "Reading the result": only the Job whose UID the entry records is
// read; a pod Pending past the timeout, a Job with no pod, a failed Job and
// a finished Job without a line are failures; a running Job is in flight.
func TestObservePITRArchiveCheck(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	e := newPBTEnv(t, pbtGroup(a))
	now := time.Now()
	mkJob := func(name string, created time.Time) *batchv1.Job {
		j := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pbtNS, CreationTimestamp: metav1.NewTime(created)}}
		if err := e.c.Create(context.Background(), j); err != nil {
			t.Fatal(err)
		}
		return j
	}
	observe := func(j *batchv1.Job, uid string) pitrCheckOutcome {
		entry := &v1alpha1.PITRArchiveBinding{Check: &v1alpha1.PITRArchiveBindingCheck{JobName: j.Name, JobUID: j.UID}}
		if uid != "" {
			entry.Check.JobUID = "other"
		}
		out, err := e.r.observePITRArchiveCheck(context.Background(), e.fg(), entry, now)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	running := mkJob("running", now)
	if out := observe(running, ""); out.done {
		t.Fatalf("a fresh Job is done: %+v", out)
	}
	if out := observe(running, "x"); !out.done || !strings.Contains(out.failure, "replaced") {
		t.Fatalf("another UID = %+v", out)
	}
	nopod := mkJob("nopod", now.Add(-2*pitrCheckPendingTimeout))
	if out := observe(nopod, ""); !out.done || !strings.Contains(out.failure, "no pod created") {
		t.Fatalf("no pod = %+v", out)
	}
	ok := mkJob("ok", now)
	e.finishCheck(ok, 0, FormatPITRArchiveCheckLine(PITRArchiveCheckResult{State: PITRArchiveCheckStateAbsent, Destination: e.dest(a).String()}))
	if out := observe(ok, ""); !out.done || out.result == nil || out.result.State != PITRArchiveCheckStateAbsent {
		t.Fatalf("verdict = %+v", out)
	}
	noLine := mkJob("noline", now)
	e.finishCheck(noLine, 0, "done")
	if out := observe(noLine, ""); !out.done || out.result != nil || !strings.Contains(out.failure, "without a verdict") {
		t.Fatalf("no line = %+v", out)
	}
	failed := mkJob("failed", now)
	e.finishCheck(failed, 3, "store unreachable")
	if out := observe(failed, ""); !out.done || !strings.Contains(out.failure, "store unreachable") {
		t.Fatalf("failed = %+v", out)
	}
	gone := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "gone"}}
	if out := observe(gone, ""); !out.done || !strings.Contains(out.failure, "disappeared") {
		t.Fatalf("gone = %+v", out)
	}
}
