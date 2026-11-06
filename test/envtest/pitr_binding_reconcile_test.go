//go:build envtest

package envtest

import (
	"bytes"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// The #203 binding lifecycle against a real API server, the reconciler
// running and the sidecars' reports injected into status.pitr.archive
// (IMPL-203 §7.3, WP-C).

const (
	bindingCondition = "PITRArchiveBindingReady"
	pitrRenderKey    = "bloodraven.shipstream.io/pitr-render"
	acceptedKey      = "bloodraven.shipstream.io/pitr-accepted"
	specHashKey      = "shipstream.io/spec-hash"
)

func bindingFG(ns string) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG(ns)
	// Recreate: the reconciler itself patches existing Deployments, so a
	// render change is visible without the ordered updater.
	fg.Spec.UpdateStrategy = "Recreate"
	s3 := func(prefix string) v1alpha1.BackupStorage {
		return v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{Bucket: "archive", Prefix: prefix, EndpointURL: "http://rustfs:9000", CredentialsSecret: "s3-creds"}}
	}
	enc := func(secret string) *v1alpha1.BackupEncryptionSpec {
		return &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: secret}}
	}
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"},
		Profiles: []v1alpha1.BackupProfile{
			{Name: "a", Storage: s3("p1"), Encryption: enc("key-a")},
			{Name: "c", Storage: s3("p2"), Encryption: enc("key-c")},
		},
	}
	return fg
}

type bindingHarness struct {
	t   *testing.T
	ns  string
	r   *controller.MysqlFailoverGroupReconciler
	rec *record.FakeRecorder
}

func newBindingHarness(t *testing.T, ns string) *bindingHarness {
	// Check Jobs run the operator image.
	controller.SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	t.Cleanup(func() { controller.SetOperatorImageDefaults("", "") })
	h := &bindingHarness{t: t, ns: ns}
	h.restart()
	h.secret("s3-creds", map[string][]byte{"AWS_ACCESS_KEY_ID": []byte("k"), "AWS_SECRET_ACCESS_KEY": []byte("s")})
	return h
}

// restart is a new reconciler instance on the persisted state.
func (h *bindingHarness) restart() {
	h.rec = record.NewFakeRecorder(500)
	h.r = &controller.MysqlFailoverGroupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: h.rec}
}

func (h *bindingHarness) reconcile(times int) {
	h.t.Helper()
	for i := 0; i < times; i++ {
		if _, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: h.ns, Name: "lion"}}); err != nil {
			h.t.Fatalf("reconcile: %v", err)
		}
	}
}

func (h *bindingHarness) fg() *v1alpha1.MysqlFailoverGroup {
	h.t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: h.ns, Name: "lion"}, &fg); err != nil {
		h.t.Fatal(err)
	}
	return &fg
}

func (h *bindingHarness) deploy(site string) *appsv1.Deployment {
	h.t.Helper()
	var d appsv1.Deployment
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: h.ns, Name: "mysql-lion-" + site}, &d); err != nil {
		h.t.Fatal(err)
	}
	return &d
}

func (h *bindingHarness) secret(name string, data map[string][]byte) {
	h.t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: h.ns, Name: name}, Data: data}
	if err := k8sClient.Create(ctx, s); err != nil {
		h.t.Fatal(err)
	}
}

func (h *bindingHarness) setSecret(name, key, value string) {
	h.t.Helper()
	var s corev1.Secret
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: h.ns, Name: name}, &s); err != nil {
		h.t.Fatal(err)
	}
	s.Data[key] = []byte(value)
	if err := k8sClient.Update(ctx, &s); err != nil {
		h.t.Fatal(err)
	}
}

func (h *bindingHarness) binding(profile string) *v1alpha1.PITRArchiveBinding {
	h.t.Helper()
	fg := h.fg()
	dest := h.dest(fg, profile)
	for i := range fg.Status.PITRArchiveBindings {
		if fg.Status.PITRArchiveBindings[i].Destination == dest.String() {
			return &fg.Status.PITRArchiveBindings[i]
		}
	}
	return nil
}

func (h *bindingHarness) dest(fg *v1alpha1.MysqlFailoverGroup, profile string) pitrarchive.Destination {
	h.t.Helper()
	for i := range fg.Spec.Backup.Profiles {
		p := &fg.Spec.Backup.Profiles[i]
		if p.Name == profile {
			d, err := pitrarchive.NewS3Destination(p.Storage.S3.Bucket, p.Storage.S3.Prefix, p.Storage.S3.EndpointURL)
			if err != nil {
				h.t.Fatal(err)
			}
			return d
		}
	}
	h.t.Fatalf("profile %s not found", profile)
	return pitrarchive.Destination{}
}

// prove injects the sidecars' report that every site verified the archive
// at profile's destination with keyRevision (consumer proof, K4).
func (h *bindingHarness) prove(profile, keyRevision string) {
	h.t.Helper()
	fg := h.fg()
	dest := h.dest(fg, profile)
	now := metav1.NowMicro()
	fg.Status.PITR = &v1alpha1.PITRStatus{Enabled: true, ProfileName: fg.Spec.Backup.PITR.ProfileName, Archive: &v1alpha1.PITRArchiveObservation{
		Destination: dest.String(), ArchiveID: pitrarchive.CreateArchiveID(fg.UID, dest), State: v1alpha1.PITRArchiveVerified,
		EncryptionMode: v1alpha1.PITRArchiveEncryptionPassphrase, KeyRevision: keyRevision, ObservedTime: &now,
	}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		h.t.Fatal(err)
	}
}

func (h *bindingHarness) condition() *metav1.Condition {
	return apimeta.FindStatusCondition(h.fg().Status.Conditions, bindingCondition)
}

func (h *bindingHarness) checkJobs() []batchv1.Job {
	h.t.Helper()
	var jobs batchv1.JobList
	if err := k8sClient.List(ctx, &jobs, client.InNamespace(h.ns), client.MatchingLabels{"shipstream.io/resource": "pitr-archive-check"}); err != nil {
		h.t.Fatal(err)
	}
	return jobs.Items
}

func (h *bindingHarness) keySnapshots() []corev1.Secret {
	h.t.Helper()
	var list corev1.SecretList
	if err := k8sClient.List(ctx, &list, client.InNamespace(h.ns), client.MatchingLabels{"app.kubernetes.io/name": "mysql-pitr-key"}); err != nil {
		h.t.Fatal(err)
	}
	return list.Items
}

func (h *bindingHarness) templateOf(site string) (string, string) {
	h.t.Helper()
	d := h.deploy(site)
	b, err := d.Spec.Template.Marshal()
	if err != nil {
		h.t.Fatal(err)
	}
	return d.Annotations[specHashKey], string(b)
}

func passphraseVolume(d *appsv1.Deployment) *corev1.SecretVolumeSource {
	for _, v := range d.Spec.Template.Spec.Volumes {
		if v.Name == "pitr-passphrase" {
			return v.Secret
		}
	}
	return nil
}

func TestEnvtest_PITRBindingLifecycle(t *testing.T) {
	ns := "envtest-pitr-binding"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)
	h := newBindingHarness(t, ns)
	h.secret("key-a", map[string][]byte{"passphrase": bytes.Repeat([]byte("a"), 32)})
	h.secret("key-c", map[string][]byte{"passphrase": bytes.Repeat([]byte("c"), 32)})
	if err := k8sClient.Create(ctx, bindingFG(ns)); err != nil {
		t.Fatal(err)
	}

	// Row 37: a fresh group renders the candidate snapshot at creation
	// (optional mount, key revision), and the binding waits for proof.
	h.reconcile(3)
	entry := h.binding("a")
	if entry == nil || entry.State != v1alpha1.PITRArchiveBindingPending || entry.Encryption.KeySecretName == "" {
		t.Fatalf("fresh binding %+v, want Pending with a candidate snapshot", entry)
	}
	snapshot := entry.Encryption.KeySecretName
	for _, site := range []string{"dc1", "dc2"} {
		d := h.deploy(site)
		vol := passphraseVolume(d)
		if vol == nil || vol.SecretName != snapshot || vol.Optional == nil || !*vol.Optional {
			t.Fatalf("%s mounts %+v, want the optional snapshot %s", site, vol, snapshot)
		}
		if !strings.Contains(d.Annotations[pitrRenderKey], `"keySecretName":"`+snapshot+`"`) {
			t.Fatalf("%s render annotation %s", site, d.Annotations[pitrRenderKey])
		}
		if strings.Contains(d.Spec.Template.String(), "BLOODRAVEN_PITR_PROFILE_NAME") {
			t.Fatal("the profile name is rendered (K7)")
		}
	}
	var snap corev1.Secret
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapshot}, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Immutable == nil || !*snap.Immutable || len(snap.OwnerReferences) != 0 {
		t.Fatalf("snapshot immutable=%v owners=%v", snap.Immutable, snap.OwnerReferences)
	}

	// Injected proof → Bound, with the acceptance annotation on the
	// snapshot.
	h.prove("a", snapshot)
	h.reconcile(2)
	if entry = h.binding("a"); entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound {
		t.Fatalf("after proof: %+v, want Bound", entry)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapshot}, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Annotations[acceptedKey] == "" {
		t.Fatal("accepted snapshot has no acceptance annotation")
	}
	if c := h.condition(); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("condition %+v, want True", c)
	}
	hash1, tmpl1 := h.templateOf("dc1")

	// Row 6: rotating the user's Secret changes neither the spec hash nor
	// the template; the condition reports KeyChangeRefused.
	h.setSecret("key-a", "passphrase", strings.Repeat("z", 32))
	h.reconcile(2)
	if hash, tmpl := h.templateOf("dc1"); hash != hash1 || tmpl != tmpl1 {
		t.Fatal("a refused rotation changed the Deployment")
	}
	if c := h.condition(); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "KeyChangeRefused" {
		t.Fatalf("condition %+v, want False/KeyChangeRefused", c)
	}
	h.setSecret("key-a", "passphrase", strings.Repeat("a", 32)+"\n") // row 7: whitespace only
	h.reconcile(2)
	if c := h.condition(); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("condition after a whitespace-only edit %+v, want True", c)
	}

	// Row 14: a reconciler restart changes nothing.
	h.restart()
	h.reconcile(2)
	if hash, tmpl := h.templateOf("dc1"); hash != hash1 || tmpl != tmpl1 {
		t.Fatal("a reconciler restart changed the Deployment")
	}

	// Row 15: a status wipe is rebuilt from the acceptance annotation, with
	// no check Job and no template change.
	fg := h.fg()
	fg.Status.PITRArchiveBindings = nil
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	h.reconcile(2)
	if entry = h.binding("a"); entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound || entry.Encryption.KeySecretName != snapshot {
		t.Fatalf("after a status wipe: %+v, want Bound with %s", entry, snapshot)
	}
	if jobs := h.checkJobs(); len(jobs) != 0 {
		t.Fatalf("recovery ran check Jobs: %d", len(jobs))
	}
	if hash, tmpl := h.templateOf("dc1"); hash != hash1 || tmpl != tmpl1 {
		t.Fatal("recovery changed the Deployment")
	}

	// Row 22: disabling PITR keeps the Bound entry; re-enabling renders the
	// same binding again.
	fg = h.fg()
	fg.Spec.Backup.PITR.Enabled = false
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	h.reconcile(2)
	if entry = h.binding("a"); entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound {
		t.Fatalf("disabled PITR dropped the binding: %+v", entry)
	}
	if vol := passphraseVolume(h.deploy("dc1")); vol != nil {
		t.Fatal("disabled PITR still mounts the snapshot")
	}
	fg = h.fg()
	fg.Spec.Backup.PITR.Enabled = true
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	h.reconcile(2)
	if hash, _ := h.templateOf("dc1"); hash != hash1 {
		t.Fatal("re-enabling PITR did not render the same binding")
	}

	// Row 23: a prefix switch while a writer runs starts a check Job (C2);
	// the writers keep the held render meanwhile.
	fg = h.fg()
	fg.Spec.Backup.PITR.ProfileName = "c"
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	h.reconcile(2)
	if jobs := h.checkJobs(); len(jobs) != 1 {
		t.Fatalf("prefix switch: %d check Jobs, want 1 (binding %+v)", len(jobs), h.binding("c"))
	}
	if hash, _ := h.templateOf("dc1"); hash != hash1 {
		t.Fatal("the held render changed before the new prefix was authenticated")
	}

	// Row 28: releasing P1's binding is refused while the Deployments
	// still render its snapshot.
	fg = h.fg()
	if fg.Annotations == nil {
		fg.Annotations = map[string]string{}
	}
	id := h.binding("a").ID
	fg.Annotations[controller.ReleasePITRArchiveBindingAnnotation] = id + ":confirm=lion"
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	h.reconcile(1)
	if h.binding("a") == nil || h.fg().Annotations[controller.ReleasePITRArchiveBindingAnnotation] == "" {
		t.Fatal("a release of an in-use binding was not refused")
	}

	// Back to P1, PITR disabled: the snapshot is no longer used anywhere
	// (Recreate re-renders the Deployments), so the release completes and
	// the snapshot is deleted (row 27).
	fg = h.fg()
	fg.Spec.Backup.PITR.ProfileName = "a"
	fg.Spec.Backup.PITR.Enabled = false
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	h.reconcile(4)
	if e := h.binding("a"); e != nil {
		t.Fatalf("released binding still present: %+v", e)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapshot}, &snap); err == nil {
		t.Fatal("released snapshot not deleted")
	}
	if h.fg().Annotations[controller.ReleasePITRArchiveBindingAnnotation] != "" {
		t.Fatal("release annotation kept after the release")
	}
}

// K3, row 38: a Deployment rendered before #203 (no pitr-render
// annotation) is not updated until a check Job authenticates the candidate;
// a Verified result is followed by the render.
func TestEnvtest_PITRBindingDefersPre203Deployment(t *testing.T) {
	ns := "envtest-pitr-binding-pre203"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)
	h := newBindingHarness(t, ns)
	h.secret("key-a", map[string][]byte{"passphrase": bytes.Repeat([]byte("a"), 32)})
	if err := k8sClient.Create(ctx, bindingFG(ns)); err != nil {
		t.Fatal(err)
	}
	fg := h.fg()
	for _, site := range []string{"dc1", "dc2"} {
		sel := map[string]string{"app.kubernetes.io/name": "mysql", "app.kubernetes.io/instance": "lion", "shipstream.io/site": site}
		labels := map[string]string{"shipstream.io/failover-group": "lion"}
		for k, v := range sel {
			labels[k] = v
		}
		pre := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mysql-lion-" + site, Annotations: map[string]string{specHashKey: "pre-203"}, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: sel},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: sel},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:old"}}},
				},
			},
		}
		if err := k8sClient.Create(ctx, pre); err != nil {
			t.Fatal(err)
		}
	}
	h.reconcile(2)
	if d := h.deploy("dc1"); d.Annotations[specHashKey] != "pre-203" || d.Annotations[pitrRenderKey] != "" {
		t.Fatalf("the pre-#203 Deployment was updated before the check: %v", d.Annotations)
	}
	jobs := h.checkJobs()
	if len(jobs) != 1 {
		t.Fatalf("%d check Jobs, want 1 (C2)", len(jobs))
	}
	job := jobs[0]
	entry := h.binding("a")
	if entry == nil || entry.Check == nil || entry.Check.JobName != job.Name {
		t.Fatalf("entry %+v does not record check Job %s", entry, job.Name)
	}

	// Inject the check result: Verified, this group's archive.
	dest := h.dest(fg, "a")
	line := controller.FormatPITRArchiveCheckLine(controller.PITRArchiveCheckResult{
		State: controller.PITRArchiveCheckStateVerified, ArchiveID: pitrarchive.CreateArchiveID(fg.UID, dest),
		Source: v1alpha1.PITRArchiveSource{Namespace: ns, Name: "lion", UID: fg.UID}, Destination: dest.String(),
	})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: job.Name + "-pod", Labels: map[string]string{"job-name": job.Name},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "check", Image: "bloodraven:test"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status = corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "check",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: line}}}}}
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	h.reconcile(3)
	if entry = h.binding("a"); entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound {
		t.Fatalf("after a Verified check: %+v, want Bound", entry)
	}
	if d := h.deploy("dc1"); d.Annotations[pitrRenderKey] == "" || passphraseVolume(d) == nil {
		t.Fatalf("the Deployment was not rendered after the check: %v", d.Annotations)
	}
}

// I-17: a MysqlBackup of an encrypted profile gets its own dump-key
// snapshot before its Job, and the Job mounts it.
func TestEnvtest_MysqlBackupDumpKeySnapshot(t *testing.T) {
	ns := "envtest-pitr-binding-dumpkey"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)
	h := newBindingHarness(t, ns)
	h.secret("key-a", map[string][]byte{"passphrase": []byte("  " + strings.Repeat("a", 32) + "\n")})
	fg := bindingFG(ns)
	fg.Spec.Backup.PITR = nil
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fg = h.fg()
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: "writable"}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "nightly-1"},
		Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "a"},
	}
	if err := k8sClient.Create(ctx, mb); err != nil {
		t.Fatal(err)
	}
	br := &controller.MysqlBackupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
	for i := 0; i < 5; i++ {
		if _, err := br.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	var list corev1.SecretList
	if err := k8sClient.List(ctx, &list, client.InNamespace(ns), client.MatchingLabels{"app.kubernetes.io/name": "mysql-dump-key"}); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("%d dump-key snapshots, want 1", len(list.Items))
	}
	snap := list.Items[0]
	if snap.Immutable == nil || !*snap.Immutable || len(snap.OwnerReferences) != 0 || string(snap.Data["passphrase"]) != strings.Repeat("a", 32) {
		t.Fatalf("dump-key snapshot %+v", snap.ObjectMeta)
	}
	var job batchv1.Job
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "mysqlbackup-nightly-1"}, &job); err != nil {
		t.Fatal(err)
	}
	mounted := ""
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "backup-passphrase" && v.Secret != nil {
			mounted = v.Secret.SecretName
		}
	}
	if mounted != snap.Name || snap.CreationTimestamp.After(job.CreationTimestamp.Time) {
		t.Fatalf("Job mounts %q, want %q created first", mounted, snap.Name)
	}
	if ps := mb.Status.ArtifactStorage; ps == nil || ps.PassphraseSecret == nil || ps.PassphraseSecret.Name != snap.Name {
		t.Fatalf("recorded dump key %+v", mb.Status.ArtifactStorage)
	}
}
