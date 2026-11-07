package component

// PITR archive encryption bindings across real archivers (#203, IMPL-203
// §7.2): real BinlogArchivers on a shared PVC archive, configured with a
// passphrase file standing in for the mounted key snapshot, the binding
// pass of the production reconciler on a fake API server, and check Jobs
// executed in process (sidecar.ProbeArchive, the probe pitr-archive-check
// runs, formatted with the controller's line format).

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

const (
	pbcNS                = "ns"
	pbcGroupName         = "orders"
	pbcCondition         = "PITRArchiveBindingReady"
	pbcRenderAnnotation  = "bloodraven.shipstream.io/pitr-render"
	pbcAcceptedAnnotaion = "bloodraven.shipstream.io/pitr-accepted"
)

// pbcProfile is a PVC profile on the shared archive claim at subPath,
// encrypted with Secret secret ("" = plaintext).
func pbcProfile(name, subPath, secret string) v1alpha1.BackupProfile {
	p := v1alpha1.BackupProfile{Name: name, Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStoragePVC,
		PVC: &v1alpha1.PVCStorage{ClaimName: "archive", SubPath: subPath}}}
	if secret != "" {
		p.Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: secret}}
	}
	return p
}

// bindingEnv is one group, its archive and its operator.
type bindingEnv struct {
	t   *testing.T
	w   *writerArchive
	c   client.Client
	r   *controller.MysqlFailoverGroupReconciler
	rec *record.FakeRecorder
}

func newBindingEnv(t *testing.T, profiles []v1alpha1.BackupProfile, secrets map[string]string) *bindingEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pbcGroupName, Namespace: pbcNS, UID: types.UID(writerGroup)},
		Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "a", ServerID: 101}, {Name: "b", ServerID: 102}},
			Backup: &v1alpha1.BackupSpec{Profiles: profiles, PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: profiles[0].Name}}},
	}
	objs := []client.Object{fg}
	for name, value := range secrets {
		objs = append(objs, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pbcNS}, Data: map[string][]byte{"passphrase": []byte(value)}})
	}
	n := 0
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithIndex(&v1alpha1.MysqlBackupVerification{}, "spec.failoverGroupRef.name", func(o client.Object) []string {
			return []string{o.(*v1alpha1.MysqlBackupVerification).Spec.FailoverGroupRef.Name}
		}).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				n++
				obj.SetUID(types.UID(fmt.Sprintf("00000000-0000-4000-9000-%012d", n)))
			}
			return c.Create(ctx, obj, opts...)
		}}).
		WithObjects(objs...).Build()
	controller.SetOperatorImageDefaults("bloodraven:test", "")
	rec := record.NewFakeRecorder(500)
	return &bindingEnv{t: t, w: newWriterArchive(t), c: c, rec: rec,
		r: &controller.MysqlFailoverGroupReconciler{Client: c, Scheme: scheme, Recorder: rec, APIReader: c}}
}

func (e *bindingEnv) fg() *v1alpha1.MysqlFailoverGroup {
	e.t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbcNS, Name: pbcGroupName}, &fg); err != nil {
		e.t.Fatal(err)
	}
	return &fg
}

func (e *bindingEnv) pass() {
	e.t.Helper()
	if _, err := e.r.ReconcilePITRArchiveBindingsForTest(context.Background(), e.fg()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *bindingEnv) update(edit func(fg *v1alpha1.MysqlFailoverGroup)) {
	e.t.Helper()
	fg := e.fg()
	edit(fg)
	if err := e.c.Update(context.Background(), fg); err != nil {
		e.t.Fatal(err)
	}
}

func (e *bindingEnv) updateStatus(edit func(st *v1alpha1.MysqlFailoverGroupStatus)) {
	e.t.Helper()
	fg := e.fg()
	edit(&fg.Status)
	if err := e.c.Status().Update(context.Background(), fg); err != nil {
		e.t.Fatal(err)
	}
}

func (e *bindingEnv) setSecret(name, value string) {
	e.t.Helper()
	var s corev1.Secret
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbcNS, Name: name}, &s); err != nil {
		if err := e.c.Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pbcNS},
			Data: map[string][]byte{"passphrase": []byte(value)}}); err != nil {
			e.t.Fatal(err)
		}
		return
	}
	s.Data = map[string][]byte{"passphrase": []byte(value)}
	if err := e.c.Update(context.Background(), &s); err != nil {
		e.t.Fatal(err)
	}
}

func (e *bindingEnv) secret(name string) *corev1.Secret {
	var s corev1.Secret
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbcNS, Name: name}, &s); err != nil {
		return nil
	}
	return &s
}

func (e *bindingEnv) dest(p v1alpha1.BackupProfile) pitrarchive.Destination {
	e.t.Helper()
	d, err := pitrarchive.NewPVCDestination(pbcNS, p.Storage.PVC.ClaimName, p.Storage.PVC.SubPath)
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

func (e *bindingEnv) entry(p v1alpha1.BackupProfile) *v1alpha1.PITRArchiveBinding {
	want := e.dest(p).String()
	for _, b := range e.fg().Status.PITRArchiveBindings {
		if b.Destination == want {
			return &b
		}
	}
	return nil
}

func (e *bindingEnv) expectEntry(p v1alpha1.BackupProfile, state v1alpha1.PITRArchiveBindingState, reason string) *v1alpha1.PITRArchiveBinding {
	e.t.Helper()
	got := e.entry(p)
	if got == nil || got.State != state || string(got.Reason) != reason {
		e.t.Fatalf("binding for %s = %+v, want %s/%s", p.Name, got, state, reason)
	}
	return got
}

func (e *bindingEnv) expectCond(status metav1.ConditionStatus, reason string) *metav1.Condition {
	e.t.Helper()
	c := meta.FindStatusCondition(e.fg().Status.Conditions, pbcCondition)
	if c == nil || c.Status != status || c.Reason != reason {
		e.t.Fatalf("condition = %+v, want %s/%s", c, status, reason)
	}
	return c
}

// keyFile writes the named snapshot's bytes to a file, as the kubelet
// projects the mounted snapshot.
func (e *bindingEnv) keyFile(snapshot string) string {
	e.t.Helper()
	s := e.secret(snapshot)
	if s == nil {
		e.t.Fatalf("snapshot %s missing", snapshot)
	}
	return e.writeKey(string(s.Data["passphrase"]))
}

func (e *bindingEnv) writeKey(value string) string {
	e.t.Helper()
	f := filepath.Join(e.t.TempDir(), "passphrase")
	if err := os.WriteFile(f, []byte(value), 0o400); err != nil {
		e.t.Fatal(err)
	}
	return f
}

// store opens the archive at dest's prefix with the key file ("" =
// plaintext).
func (e *bindingEnv) store(dest pitrarchive.Destination, keyFile string) sidecar.ArchiveStore {
	e.t.Helper()
	s, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC", ManifestPrefix: dest.KeyPrefix,
		PassphraseFile: keyFile, PVC: &sidecar.PITRPVCConfig{MountPath: e.w.root, MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

// archiver runs site's sidecar archiver on dest with the key file, the
// way the pod renders it (revision = the snapshot name).
type archiver struct {
	a    *sidecar.BinlogArchiver
	stop func()
	srv  *simServer
	dest pitrarchive.Destination
}

func (e *bindingEnv) startArchiver(site string, srv *simServer, dest pitrarchive.Destination, enc v1alpha1.PITRArchiveBindingEncryption, keyFile string) *archiver {
	e.t.Helper()
	ident := sidecar.ArchiveIdentity{
		Source:      pitrarchive.Source{Namespace: pbcNS, Name: pbcGroupName, UID: types.UID(writerGroup)},
		Destination: dest,
		Encryption:  pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionMode(enc.Mode), Algorithm: string(enc.Algorithm)},
		KeyRevision: enc.KeySecretName,
	}
	if enc.Mode == v1alpha1.EncryptionModeNone {
		ident.KeyRevision = "plaintext"
	}
	ctx, cancel := context.WithCancel(e.w.ctx)
	cfg := &sidecar.PITRConfig{BinlogDir: srv.dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: dest.KeyPrefix,
		PollInterval: 10 * time.Millisecond, RoleProbeInterval: 10 * time.Millisecond, Archive: ident}
	a := sidecar.NewBinlogArchiver(cfg, site, srv, e.store(dest, keyFile), slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	e.w.wg.Add(1)
	go func() { defer e.w.wg.Done(); defer close(done); a.Run(ctx) }()
	return &archiver{a: a, stop: func() { cancel(); <-done }, srv: srv, dest: dest}
}

func (x *archiver) status() *pitrarchive.ArchiveStatus {
	return x.a.Snapshot(context.Background()).Archive
}

func (e *bindingEnv) waitState(x *archiver, state pitrarchive.ObservedState) *pitrarchive.ArchiveStatus {
	e.t.Helper()
	e.w.eventually("archiver "+string(state), func() bool { st := x.status(); return st != nil && st.State == state })
	return x.status()
}

// report publishes an archiver's status as status.pitr.archive (one site
// reporting, as the runner aggregates it).
func (e *bindingEnv) report(st *pitrarchive.ArchiveStatus) {
	e.updateStatus(func(s *v1alpha1.MysqlFailoverGroupStatus) {
		if s.PITR == nil {
			s.PITR = &v1alpha1.PITRStatus{}
		}
		obs := &v1alpha1.PITRArchiveObservation{Destination: st.Destination, ArchiveID: st.ArchiveID,
			EncryptionMode: v1alpha1.EncryptionMode(st.EncryptionMode), KeyRevision: st.KeyRevision,
			State: v1alpha1.PITRArchiveObservedState(st.State), Reason: v1alpha1.PITRArchiveRefusalReason(st.Reason)}
		s.PITR.Archive = obs
	})
}

// uploads counts the files in site's manifest, read with the key file.
func (e *bindingEnv) uploads(dest pitrarchive.Destination, keyFile, site string) int {
	e.t.Helper()
	m, err := sidecar.LoadManifest(context.Background(), e.store(dest, keyFile), dest.KeyPrefix, site)
	if err != nil {
		return -1
	}
	return len(m.Files)
}

// runCheck executes the check Job the binding records, in process: the
// probe pitr-archive-check runs, with the Job's env and the mounted
// snapshot, and the verdict on the pod's termination message.
func (e *bindingEnv) runCheck(p v1alpha1.BackupProfile) controller.PITRArchiveCheckResult {
	e.t.Helper()
	entry := e.entry(p)
	if entry == nil || entry.Check == nil {
		e.t.Fatalf("no check in flight for %s: %+v", p.Name, entry)
	}
	var job batchv1.Job
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbcNS, Name: entry.Check.JobName}, &job); err != nil {
		e.t.Fatal(err)
	}
	env := map[string]string{}
	for _, v := range job.Spec.Template.Spec.Containers[0].Env {
		env[v.Name] = strings.ReplaceAll(v.Value, "$$", "$")
	}
	dest, err := pitrarchive.ParseDestination(env["BLOODRAVEN_PITR_DESTINATION"])
	if err != nil {
		e.t.Fatal(err)
	}
	keyFile := ""
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Secret != nil && v.Name == "pitr-passphrase" {
			keyFile = e.keyFile(v.Secret.SecretName)
		}
	}
	expect := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: dest,
		Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionMode(env["BLOODRAVEN_PITR_ENCRYPTION_MODE"]), Algorithm: env["BLOODRAVEN_PITR_ALGORITHM"]}}
	if env[controller.PITRArchiveCheckRoleEnv] == controller.PITRArchiveCheckRoleWriter {
		expect.Role, expect.SourceUID = pitrarchive.RoleWriter, types.UID(env["BLOODRAVEN_FAILOVER_GROUP_UID"])
	}
	probe, err := sidecar.ProbeArchive(context.Background(), e.store(dest, keyFile), env["BLOODRAVEN_PITR_MANIFEST_PREFIX"], expect)
	if err != nil {
		e.t.Fatal(err)
	}
	res := controller.PITRArchiveCheckResult{DestinationDigest: controller.PITRArchiveCheckDestinationDigest(dest.String())}
	switch probe.State {
	case sidecar.ArchiveProbeAbsent:
		res.State = controller.PITRArchiveCheckStateAbsent
	case sidecar.ArchiveProbeVerified:
		res.State = controller.PITRArchiveCheckStateVerified
	default:
		res.State, res.Reason = controller.PITRArchiveCheckStateRefused, probe.Token
	}
	if d := probe.Descriptor; d != nil {
		res.ArchiveID = d.ArchiveID
		res.Source = v1alpha1.PITRArchiveSource{Namespace: d.Source.Namespace, Name: d.Source.Name, UID: d.Source.UID}
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-" + string(job.UID)[len(job.UID)-6:], Namespace: pbcNS,
		Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name},
		// The Job controller's pod: controlled by the Job, the only pod a
		// verdict is read from.
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controlledByJob}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "pitr-archive-check",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: controller.FormatPITRArchiveCheckLine(res)}}}}}}
	if err := e.c.Create(context.Background(), pod); err != nil {
		e.t.Fatal(err)
	}
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
	if err := e.c.Status().Update(context.Background(), &job); err != nil {
		e.t.Fatal(err)
	}
	return res
}

// establish runs the fresh-group path (row 37): the pass makes the
// candidate snapshot, site a's archiver mounts it, creates the descriptor,
// and its report binds it.
func (e *bindingEnv) establish(p v1alpha1.BackupProfile, srv *simServer) (*archiver, *v1alpha1.PITRArchiveBinding, string) {
	e.t.Helper()
	e.pass()
	cand := e.expectEntry(p, v1alpha1.PITRArchiveBindingPending, "Authenticating")
	keyFile := ""
	if cand.Encryption.KeySecretName != "" {
		keyFile = e.keyFile(cand.Encryption.KeySecretName)
	}
	x := e.startArchiver("a", srv, e.dest(p), cand.Encryption, keyFile)
	e.report(e.waitState(x, pitrarchive.StateVerified))
	e.pass()
	return x, e.expectEntry(p, v1alpha1.PITRArchiveBindingBound, "Bound"), keyFile
}

func (e *bindingEnv) waitUploads(dest pitrarchive.Destination, keyFile, site string, n int) {
	e.t.Helper()
	e.w.eventually(fmt.Sprintf("%d uploads at %s", n, dest), func() bool { return e.uploads(dest, keyFile, site) >= n })
}

// Projection + restart: once Bound, a rotation of the user's Secret is
// refused and changes nothing a pod mounts; a writer restarted from the
// snapshot keeps verifying and uploading; restarted on the rotated live
// bytes (what a pod mounting the user's Secret would get) it refuses
// KeyMismatch (#200).
func TestPITRBindingProjectionAndRestart(t *testing.T) {
	a := pbcProfile("a", "", "pass-a")
	e := newBindingEnv(t, []v1alpha1.BackupProfile{a}, map[string]string{"pass-a": "k1\n"})
	srv := newSimServer(t, writerUUIDA, 101, false)
	x, bound, keyFile := e.establish(a, srv)
	snap := e.secret(bound.Encryption.KeySecretName)
	if snap.Annotations[pbcAcceptedAnnotaion] == "" || snap.Immutable == nil || !*snap.Immutable || len(snap.OwnerReferences) != 0 {
		t.Fatalf("snapshot = %+v", snap.ObjectMeta)
	}
	srv.apply(liveTxns(writerUUIDA, 101, 1, 3)...)
	srv.rotate()
	e.waitUploads(x.dest, keyFile, "a", 1)

	e.setSecret("pass-a", "k2")
	e.pass()
	e.expectCond(metav1.ConditionFalse, "KeyChangeRefused")
	if got := e.entry(a); got.Encryption != bound.Encryption || got.State != v1alpha1.PITRArchiveBindingBound {
		t.Fatalf("binding moved: %+v", got)
	}

	// Restart from the snapshot (what the pod mounts): verified, uploads go on.
	x.stop()
	x = e.startArchiver("a", srv, x.dest, bound.Encryption, e.keyFile(bound.Encryption.KeySecretName))
	if st := e.waitState(x, pitrarchive.StateVerified); st.KeyRevision != bound.Encryption.KeySecretName || st.ArchiveID != bound.ArchiveID {
		t.Fatalf("restarted archiver = %+v", st)
	}
	srv.apply(liveTxns(writerUUIDA, 101, 4, 6)...)
	srv.rotate()
	e.waitUploads(x.dest, keyFile, "a", 2)
	x.stop()

	// Control: the rotated live bytes do not open the archive.
	ctl := e.startArchiver("a", srv, x.dest, bound.Encryption, e.writeKey("k2"))
	if st := e.waitState(ctl, pitrarchive.StateRefused); st.Reason != pitrarchive.ReasonArchiveKeyMismatch {
		t.Fatalf("control archiver = %+v", st)
	}
	ctl.stop()
	// The refusal of a Bound binding never demotes it.
	e.report(ctl.status())
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, "Bound")
}

// K12: a missing snapshot file fails the archiver closed (store init
// error, nothing uploaded) while the server it watches keeps serving.
func TestPITRBindingMissingSnapshotFile(t *testing.T) {
	a := pbcProfile("a", "", "pass-a")
	e := newBindingEnv(t, []v1alpha1.BackupProfile{a}, map[string]string{"pass-a": "k1"})
	srv := newSimServer(t, writerUUIDA, 101, false)
	dest := e.dest(a)
	cfg := &sidecar.PITRConfig{StorageType: "PVC", ManifestPrefix: dest.KeyPrefix, PassphraseFile: filepath.Join(t.TempDir(), "absent"),
		BinlogDir: srv.dir, BinlogIndex: "mysql-bin.index", PollInterval: 10 * time.Millisecond, RoleProbeInterval: 10 * time.Millisecond,
		PVC: &sidecar.PITRPVCConfig{MountPath: e.w.root, MountVerifier: sidecar.TempDirMountVerifierForTest},
		Archive: sidecar.ArchiveIdentity{Source: pitrarchive.Source{Namespace: pbcNS, Name: pbcGroupName, UID: types.UID(writerGroup)},
			Destination: dest, Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM},
			KeyRevision: "mysql-orders-pitr-key-0000000000"}}
	sup := sidecar.NewArchiverSupervisor(cfg, "a", srv, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sup.Run(ctx) }()
	defer func() { cancel(); <-done }()
	e.w.eventually("store init failure", func() bool {
		st := sup.Snapshot(context.Background())
		return strings.Contains(st.LastError, "archive store init") && st.StoreReady != nil && !*st.StoreReady
	})
	srv.apply(liveTxns(writerUUIDA, 101, 1, 3)...)
	srv.rotate()
	if ro, err := srv.IsReadOnly(context.Background()); err != nil || ro {
		t.Fatalf("the server stopped serving: ro=%t err=%v", ro, err)
	}
	keys, _ := e.w.store.List(context.Background(), dest.KeyPrefix+"/")
	if len(keys) != 0 {
		t.Fatalf("archive objects written without a key: %v", keys)
	}
	st := sup.Snapshot(context.Background())
	e.report(st.Archive)
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, "Authenticating")
}

// K3 (row 38/38a): a Deployment from before #203 (no render annotation)
// runs a writer with a key the operator never saw. The live key no longer
// matches the archive: the Writer check refuses KeyMismatch, the condition
// names the deferred Deployment, which is not touched. Putting the
// original key back verifies on the next round and binds it. Also row 14:
// an operator restart mid-check reads the same Job's result.
func TestPITRBindingPre203Deployment(t *testing.T) {
	a := pbcProfile("a", "", "pass-a")
	e := newBindingEnv(t, []v1alpha1.BackupProfile{a}, map[string]string{"pass-a": "k-new"})
	srv := newSimServer(t, writerUUIDA, 101, false)
	// The pre-#203 writer created the archive with the original key.
	old := e.startArchiver("a", srv, e.dest(a), v1alpha1.PITRArchiveBindingEncryption{Mode: "Passphrase", Algorithm: "AES-256-GCM"}, e.writeKey("k-orig"))
	e.waitState(old, pitrarchive.StateVerified)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "mysql-orders-a", Namespace: pbcNS,
		Labels: map[string]string{"app.kubernetes.io/name": "mysql", "shipstream.io/failover-group": pbcGroupName, "shipstream.io/site": "a"}}}
	if err := e.c.Create(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	e.report(old.status()) // a pre-#203 pod: empty revision, never proof
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, "Authenticating")
	// Operator restart mid-check: a new reconciler reads the same Job.
	e.r = &controller.MysqlFailoverGroupReconciler{Client: e.c, Scheme: e.r.Scheme, Recorder: e.rec, APIReader: e.c}
	inFlight := e.entry(a).Check
	e.pass()
	if got := e.entry(a).Check; got == nil || *got != *inFlight {
		t.Fatalf("restart replaced the check: %+v vs %+v", got, inFlight)
	}
	if res := e.runCheck(a); res.Reason != pitrarchive.TokenArchiveKeyMismatch {
		t.Fatalf("check = %+v", res)
	}
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingRefused, "KeyMismatch")
	c := e.expectCond(metav1.ConditionFalse, "KeyMismatch")
	if !strings.Contains(c.Message, "Deployments mysql-orders-a are not updated") || !strings.Contains(c.Message, "Secret pass-a key passphrase") {
		t.Fatalf("message %q", c.Message)
	}
	var after appsv1.Deployment
	if err := e.c.Get(context.Background(), client.ObjectKeyFromObject(dep), &after); err != nil ||
		after.ResourceVersion != dep.ResourceVersion || after.Annotations[pbcRenderAnnotation] != "" {
		t.Fatalf("the deferred Deployment changed: %+v %v", after.ObjectMeta, err)
	}
	// The user restores the original key: a new round binds it.
	e.setSecret("pass-a", "k-orig")
	for i := 0; i < 3 && e.entry(a).State != v1alpha1.PITRArchiveBindingBound; i++ {
		e.pass()
		if e.entry(a).Check != nil {
			e.runCheck(a)
			e.pass()
		}
	}
	bound := e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, "Bound")
	if string(e.secret(bound.Encryption.KeySecretName).Data["passphrase"]) != "k-orig" {
		t.Fatalf("bound the wrong key: %+v", bound)
	}
	old.stop()
}

// K6 (row 42): a read binding of another group's archive, made this
// group's write target, is never written: the condition refuses and the
// binding stays Bound for reads.
func TestPITRBindingForeignReadMadeWriteTarget(t *testing.T) {
	a := pbcProfile("a", "", "")
	src := pbcProfile("src", "foreign", "")
	e := newBindingEnv(t, []v1alpha1.BackupProfile{a, src}, nil)
	// Another group's archive at src's destination.
	other := e.startArchiver("x", newSimServer(t, writerUUIDB, 102, false), e.dest(src),
		v1alpha1.PITRArchiveBindingEncryption{Mode: "None"}, "")
	e.waitState(other, pitrarchive.StateVerified)
	other.stop()
	// That archiver used this group's UID; rewrite it as another group's.
	desc, err := pitrarchive.NewDescriptor(pitrarchive.Source{Namespace: "x", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ee"},
		e.dest(src), pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}, nil, "x", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := desc.Canonical()
	if err := e.w.store.Put(context.Background(), pitrarchive.DescriptorKey(e.dest(src).KeyPrefix), strings.NewReader(string(body)), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup.PITR.Enabled = false
		fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	})
	e.pass()
	if res := e.runCheck(src); res.State != controller.PITRArchiveCheckStateVerified {
		t.Fatalf("read check = %+v", res)
	}
	e.pass()
	bound := e.expectEntry(src, v1alpha1.PITRArchiveBindingBound, "Bound")
	if bound.Source == nil || bound.Source.Name != "billing" {
		t.Fatalf("read binding = %+v", bound)
	}
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "src"}
	})
	e.pass()
	e.expectEntry(src, v1alpha1.PITRArchiveBindingBound, "Bound")
	e.expectCond(metav1.ConditionFalse, "ArchiveForeign")
}

// Rows 9 and 10 plus conflicting edits: an alias of the active profile at
// the same destination with the same bytes is accepted; one with other
// bytes, made active together with a rotation of the original, is refused
// in one message naming both profiles. The binding never changes. (The
// render and spec hash, identical in both cases, are the renderer's: WP-C
// pitr_render_test.go.)
func TestPITRBindingSamePrefixAlias(t *testing.T) {
	a := pbcProfile("a", "", "pass-a")
	e := newBindingEnv(t, []v1alpha1.BackupProfile{a}, map[string]string{"pass-a": "k1", "pass-b": "k1"})
	x, bound, _ := e.establish(a, newSimServer(t, writerUUIDA, 101, false))
	defer x.stop()
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, pbcProfile("b", "", "pass-b"))
		fg.Spec.Backup.PITR.ProfileName = "b"
	})
	e.pass()
	e.expectCond(metav1.ConditionTrue, "Bound")
	// One update: b gets other bytes and a is rotated; both resolve here.
	e.setSecret("pass-b", "kb")
	e.setSecret("pass-a", "ka")
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "a"}}
	})
	e.pass()
	c := e.expectCond(metav1.ConditionFalse, "KeyChangeRefused")
	if !strings.Contains(c.Message, "profile b's passphrase") || !strings.Contains(c.Message, "profile a's passphrase") || strings.Count(c.Message, "Rotation") != 1 {
		t.Fatalf("message %q", c.Message)
	}
	if got := e.entry(a); got.Encryption != bound.Encryption {
		t.Fatalf("binding moved: %+v", got)
	}
}

// Old-history read: after the documented prefix switch, the old archive's
// binding is retained; with its user Secret deleted, a reader still opens
// the old archive through the retained snapshot.
func TestPITRBindingRetainedArchiveRead(t *testing.T) {
	a := pbcProfile("a", "", "pass-a")
	c := pbcProfile("c", "p2", "pass-c")
	e := newBindingEnv(t, []v1alpha1.BackupProfile{a, c}, map[string]string{"pass-a": "k1", "pass-c": "k2"})
	srv := newSimServer(t, writerUUIDA, 101, false)
	x, oldBound, _ := e.establish(a, srv)
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "c" })
	// The running writer is held on a (its Deployment's render).
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "mysql-orders-a", Namespace: pbcNS,
		Labels: map[string]string{"app.kubernetes.io/name": "mysql", "shipstream.io/failover-group": pbcGroupName, "shipstream.io/site": "a"}}}
	if err := e.c.Create(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	e.pass()
	if res := e.runCheck(c); res.State != controller.PITRArchiveCheckStateAbsent {
		t.Fatalf("check of the new prefix = %+v", res)
	}
	e.pass()
	est := e.expectEntry(c, v1alpha1.PITRArchiveBindingPending, "Establishing")
	x.stop()
	y := e.startArchiver("a", srv, e.dest(c), est.Encryption, e.keyFile(est.Encryption.KeySecretName))
	defer y.stop()
	e.report(e.waitState(y, pitrarchive.StateVerified))
	e.pass()
	e.expectEntry(c, v1alpha1.PITRArchiveBindingBound, "Bound")
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, "Bound")

	s := e.secret("pass-a")
	if err := e.c.Delete(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	e.pass()
	retained := e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, "Bound")
	if retained.Encryption != oldBound.Encryption {
		t.Fatalf("retained binding = %+v", retained)
	}
	d, err := sidecar.OpenArchiveForRead(context.Background(), e.store(e.dest(a), e.keyFile(retained.Encryption.KeySecretName)), e.dest(a).KeyPrefix,
		pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: e.dest(a), ArchiveID: retained.ArchiveID,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}})
	if err != nil || d.ArchiveID != retained.ArchiveID {
		t.Fatalf("reading the retained archive: %v", err)
	}
}

// Rows 15, 16, 34 and 19 on a real archive: a status wipe rebuilds the
// encrypted bindings from their snapshots without a Job, and each rebuilt
// snapshot opens its real descriptor; a plaintext binding is re-proven by
// the running writer; a snapshot replaced by hand is KeyMaterialLost and
// kept.
func TestPITRBindingStatusWipeAndIntegrity(t *testing.T) {
	a := pbcProfile("a", "", "pass-a")
	c := pbcProfile("c", "p2", "pass-c")
	pl := pbcProfile("pl", "p3", "")
	e := newBindingEnv(t, []v1alpha1.BackupProfile{a, c, pl}, map[string]string{"pass-a": "k1", "pass-c": "k2"})
	var bounds []*v1alpha1.PITRArchiveBinding
	for i, p := range []v1alpha1.BackupProfile{a, c, pl} {
		e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = p.Name })
		x, b, _ := e.establish(p, newSimServer(t, writerUUIDA, uint32(101+i), false))
		x.stop()
		bounds = append(bounds, b)
	}
	// Row 34: K1's live Secret is gone; the status is wiped.
	if err := e.c.Delete(context.Background(), e.secret("pass-a")); err != nil {
		t.Fatal(err)
	}
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = nil; st.PITR = nil })
	e.pass()
	for i, p := range []v1alpha1.BackupProfile{a, c} {
		got := e.expectEntry(p, v1alpha1.PITRArchiveBindingBound, "Bound")
		if got.Encryption != bounds[i].Encryption || got.ArchiveID != bounds[i].ArchiveID {
			t.Fatalf("rebuilt %+v, want %+v", got, bounds[i])
		}
		probe, err := sidecar.ProbeArchive(context.Background(), e.store(e.dest(p), e.keyFile(got.Encryption.KeySecretName)), e.dest(p).KeyPrefix,
			pitrarchive.Expectation{Role: pitrarchive.RoleWriter, SourceUID: types.UID(writerGroup), Destination: e.dest(p),
				Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}})
		if err != nil || probe.State != sidecar.ArchiveProbeVerified {
			t.Fatalf("rebuilt snapshot of %s does not open its archive: %+v %v", p.Name, probe, err)
		}
	}
	// Row 16: the plaintext binding is re-proven by the running writer.
	e.expectEntry(pl, v1alpha1.PITRArchiveBindingPending, "Authenticating")
	y := e.startArchiver("a", newSimServer(t, writerUUIDA, 110, false), e.dest(pl), v1alpha1.PITRArchiveBindingEncryption{Mode: "None"}, "")
	e.report(e.waitState(y, pitrarchive.StateVerified))
	y.stop()
	e.pass()
	e.expectEntry(pl, v1alpha1.PITRArchiveBindingBound, "Bound")
	var jobs batchv1.JobList
	if err := e.c.List(context.Background(), &jobs, client.InNamespace(pbcNS)); err != nil || len(jobs.Items) != 0 {
		t.Fatalf("recovery ran Jobs: %+v %v", jobs.Items, err)
	}

	// Row 19: c's snapshot replaced by hand with other bytes.
	snap := e.secret(bounds[1].Encryption.KeySecretName)
	if err := e.c.Delete(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	snap.ResourceVersion, snap.UID = "", ""
	snap.Data = map[string][]byte{"passphrase": []byte("forged")}
	if err := e.c.Create(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	e.pass()
	e.expectEntry(c, v1alpha1.PITRArchiveBindingBound, "KeyMaterialLost")
	if s := e.secret(snap.Name); s == nil || string(s.Data["passphrase"]) != "forged" {
		t.Fatal("the forged snapshot was deleted or rewritten")
	}
}

// controlledByJob marks a check pod's owner reference as its controller.
var controlledByJob = true
