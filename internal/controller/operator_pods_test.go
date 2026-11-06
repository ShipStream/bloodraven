package controller

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
	"github.com/shipstream/bloodraven/internal/platform"
)

// operatorPodsTestImage is a sentinel operator image so the walk below
// can tell operator-image containers apart from mysqlsh/mysql ones.
const operatorPodsTestImage = "registry.test/bloodraven:operator-pods"

// renderedPod is one operator-built pod template, named for failures.
type renderedPod struct {
	name string
	spec corev1.PodSpec
}

// renderOperatorPods renders every Job/CronJob pod template the
// operator builds for a failover group, in the variants that pull in
// operator-image containers (encryption on, PITR on), plus the plain
// variants that only carry tolerations.
func renderOperatorPods(t *testing.T) (*v1alpha1.MysqlFailoverGroup, []renderedPod) {
	t.Helper()
	SetOperatorImageDefaults(operatorPodsTestImage, "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })

	ctx := context.Background()
	fg := fgWithEncryptedBackup()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 3 * * 0"}
	encProfile := fg.Spec.Backup.Profiles[0]
	plainProfile := fg.Spec.Backup.Profiles[1]

	seed := successfulBackup("seed", fg.Name, encProfile.Name)
	seed.Status.Encrypted = true
	recordTestDumpKey(seed)
	seed.Status.EncryptionAlgorithm = "AES-256-GCM"

	var pods []renderedPod
	addJob := func(name string, job *batchv1.Job, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		pods = append(pods, renderedPod{name: name, spec: job.Spec.Template.Spec})
	}

	for _, p := range []v1alpha1.BackupProfile{encProfile, plainProfile} {
		mb := &v1alpha1.MysqlBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "b-" + p.Name, Namespace: fg.Namespace},
			Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name}, ProfileName: p.Name},
		}
		job, err := BuildBackupJob(BackupJobInputs{
			DumpKeySecretName: testDumpKeySecret,
			FailoverGroup:     fg, Profile: p, Backup: mb, SourceSite: "pdx",
			CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
		})
		addJob("backup/"+p.Name, job, err)
	}

	verify := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify", Namespace: fg.Namespace},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name},
			ProfileName:      encProfile.Name,
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup: fg, Profile: encProfile, Verification: verify, Backup: seed,
		CredsSecretName: "creds", ScriptsConfigMapName: "scripts",
	})
	addJob("verification/encrypted", job, err)

	job, err = buildCleanupJob(cleanupJobInputs{
		FailoverGroup: fg, Profile: &encProfile, Backup: seed,
		Storage: &v1alpha1.BackupArtifactStorage{Bucket: "bloodraven-backups"},
	})
	addJob("cleanup", job, err)

	// The PITR archive check Job (#203), as a writer with a candidate key
	// snapshot and as a reader of a plaintext archive (U40).
	checkFG := fg.DeepCopy()
	checkFG.UID = "00000000-0000-4000-8000-0000000000a1"
	for _, tc := range []struct {
		name    string
		profile v1alpha1.BackupProfile
		write   bool
		key     string
	}{
		{"archive-check/writer", encProfile, true, "mysql-" + fg.Name + "-pitr-key-0123456789"},
		{"archive-check/reader", plainProfile, false, ""},
	} {
		dest, err := pitrprofile.DestinationFor(checkFG, &tc.profile)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		enc := v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.PITRArchiveEncryptionNone}
		if tc.key != "" {
			enc = v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.PITRArchiveEncryptionPassphrase, Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM, KeySecretName: tc.key}
		}
		entry := &v1alpha1.PITRArchiveBinding{ID: pitrBindingID(dest), Destination: dest.String(), Encryption: enc}
		job, err := buildPITRArchiveCheckJob(checkFG, entry, &pitrBindingTarget{dest: dest, write: tc.write, profile: &tc.profile}, tc.key)
		addJob(tc.name, job, err)
	}

	src := v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: seed.Name}}
	pit := &v1alpha1.PointInTimeSpec{StopDatetime: "2026-04-15T09:30:00Z", ArchiveProfileName: addSourceArchiveProfile(fg)}
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: src, PointInTime: pit}
	// In-place restores refuse pointInTime, so that variant has no PITR init container.
	fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "c1", Source: src}
	r, c := newReconciler(fg, seed)

	job, err = r.buildRestoreJob(ctx, fg, fg.Spec.Sites[0].Name, "creds")
	addJob("restore/bootstrap", job, err)
	job, err = r.buildInPlaceRestoreJob(ctx, fg, fg.Spec.Sites[0].Name, "creds", true, "")
	addJob("restore/in-place", job, err)

	if err := r.reconcileBackupSchedules(ctx, fg); err != nil {
		t.Fatalf("reconcileBackupSchedules: %v", err)
	}
	if err := r.reconcileVerificationSchedules(ctx, fg); err != nil {
		t.Fatalf("reconcileVerificationSchedules: %v", err)
	}
	for _, name := range []string{
		scheduleCronJobName(fg.Name, fg.Spec.Backup.Schedules[0].Name),
		verificationScheduleCronJobName(fg.Name, encProfile.Name),
	} {
		var cj batchv1.CronJob
		if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &cj); err != nil {
			t.Fatalf("get cronjob %s: %v", name, err)
		}
		pods = append(pods, renderedPod{name: "cronjob/" + name, spec: cj.Spec.JobTemplate.Spec.Template.Spec})
	}
	return fg, pods
}

// TestOperatorImageContainersUseAbsoluteBinaryPath is the regression
// test for the encrypted-backup StartError: the operator image is
// distroless and only has /bloodraven, so a bare "bloodraven" argv[0]
// fails with `exec: "bloodraven": executable file not found in $PATH`.
func TestOperatorImageContainersUseAbsoluteBinaryPath(t *testing.T) {
	_, pods := renderOperatorPods(t)

	want := map[string][]string{
		"backup/nightly-s3":      {backupStampInitContainerName, backupEncryptUploadContainerName},
		"verification/encrypted": {"decrypt-download"},
		"restore/bootstrap":      {restorePITRDumpMetaContainerName, restorePITRInitContainerName, "decrypt-download"},
		"restore/in-place":       {"decrypt-download"},
		"cleanup":                {cleanupContainerName},
		"archive-check/writer":   {pitrCheckContainerName},
		"archive-check/reader":   {pitrCheckContainerName},
	}
	for _, p := range pods {
		var got []string
		for _, ctr := range append(append([]corev1.Container{}, p.spec.InitContainers...), p.spec.Containers...) {
			if ctr.Image != operatorPodsTestImage {
				continue
			}
			got = append(got, ctr.Name)
			if len(ctr.Command) == 0 || ctr.Command[0] != operatorBinaryPath {
				t.Errorf("%s: container %q runs the operator image with command %q; argv[0] must be %q",
					p.name, ctr.Name, ctr.Command, operatorBinaryPath)
			}
		}
		if strings.HasPrefix(p.name, "cronjob/") {
			want[p.name] = []string{"trigger"}
		}
		if exp, ok := want[p.name]; ok && strings.Join(got, ",") != strings.Join(exp, ",") {
			t.Errorf("%s: operator-image containers = %v, want %v (the walk must not pass vacuously)", p.name, got, exp)
		}
	}
}

// TestOperatorBinaryPathMatchesDockerfile ties operatorBinaryPath to
// where the operator image actually installs the binary.
func TestOperatorBinaryPathMatchesDockerfile(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile"))
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	var stage, copyDest, entrypoint string
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "FROM":
			stage = ""
			if n := len(fields); n >= 3 && strings.EqualFold(fields[n-2], "AS") {
				stage = fields[n-1]
			}
		case "COPY":
			if stage == "bloodraven" && len(fields) >= 3 && strings.HasPrefix(fields[1], "--from=") {
				copyDest = fields[len(fields)-1]
			}
		case "ENTRYPOINT":
			if stage == "bloodraven" {
				m := regexp.MustCompile(`\[\s*"([^"]+)"`).FindStringSubmatch(line)
				if m == nil {
					t.Fatalf("operator ENTRYPOINT is not exec form: %q", line)
				}
				entrypoint = m[1]
			}
		}
	}
	if copyDest != operatorBinaryPath {
		t.Errorf("Dockerfile bloodraven stage copies the binary to %q, operatorBinaryPath is %q", copyDest, operatorBinaryPath)
	}
	if entrypoint != operatorBinaryPath {
		t.Errorf("Dockerfile bloodraven stage ENTRYPOINT is %q, operatorBinaryPath is %q", entrypoint, operatorBinaryPath)
	}
}

// TestNoBareOperatorBinaryCommand catches a hand-written
// `[]string{"bloodraven", ...}` argv anywhere in the module, including
// builders the render walk above does not know about yet.
func TestNoBareOperatorBinaryCommand(t *testing.T) {
	root := filepath.Join("..", "..")
	bare := regexp.MustCompile(`\[\]string\{\s*"bloodraven"\s*,`)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "bitpoke", "orchestrator", "node_modules", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if loc := bare.FindIndex(body); loc != nil {
			line := strings.Count(string(body[:loc[0]]), "\n") + 1
			t.Errorf("%s:%d: bare \"bloodraven\" argv; use operatorCommand(...) so the image path %q is used", path, line, operatorBinaryPath)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

// TestOperatorPodsTolerateOwnGroupReadOnlyTaint pins the placement rule:
// every pod the operator builds for a group survives, and can schedule
// onto, nodes carrying that group's read-only NoExecute taint, exactly
// like the group's MySQL pods. Other groups' taints (shared nodes) and
// the legacy unscoped key stay untolerated.
func TestOperatorPodsTolerateOwnGroupReadOnlyTaint(t *testing.T) {
	fg, pods := renderOperatorPods(t)

	own := corev1.Taint{Key: v1alpha1.ReadOnlyTaintKey(fg.Name), Value: v1alpha1.ReadOnlyTaintValue, Effect: corev1.TaintEffectNoExecute}
	other := corev1.Taint{Key: v1alpha1.ReadOnlyTaintKey(fg.Name + "-other"), Value: v1alpha1.ReadOnlyTaintValue, Effect: corev1.TaintEffectNoExecute}
	legacy := corev1.Taint{Key: platform.LegacyTaintKey, Value: v1alpha1.ReadOnlyTaintValue, Effect: corev1.TaintEffectNoExecute}

	if len(pods) != 10 {
		t.Fatalf("rendered %d pod templates, want 10", len(pods))
	}
	for _, p := range pods {
		if !tolerates(p.spec.Tolerations, own) {
			t.Errorf("%s: does not tolerate own group taint %s; a failover would evict it (tolerations=%+v)", p.name, own.Key, p.spec.Tolerations)
		}
		if tolerates(p.spec.Tolerations, other) {
			t.Errorf("%s: tolerates another group's taint %s", p.name, other.Key)
		}
		if tolerates(p.spec.Tolerations, legacy) {
			t.Errorf("%s: tolerates legacy taint %s", p.name, legacy.Key)
		}
		if p.spec.NodeSelector != nil || p.spec.Affinity != nil {
			t.Errorf("%s: unexpected nodeSelector/affinity %v/%v; Jobs reach MySQL via Services and may be pinned by PVC topology", p.name, p.spec.NodeSelector, p.spec.Affinity)
		}
	}
}

// tolerates applies the scheduler's Equal/Exists toleration rules
// (corev1.Toleration.ToleratesTaint without the Lt/Gt extension, which
// would drag klog into the test's direct imports).
func tolerates(tols []corev1.Toleration, taint corev1.Taint) bool {
	for _, tol := range tols {
		if tol.Effect != "" && tol.Effect != taint.Effect {
			continue
		}
		if tol.Key != "" && tol.Key != taint.Key {
			continue
		}
		switch tol.Operator {
		case corev1.TolerationOpExists:
			return true
		case "", corev1.TolerationOpEqual:
			if tol.Value == taint.Value {
				return true
			}
		}
	}
	return false
}
