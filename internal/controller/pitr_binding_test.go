package controller

// Test harness of the PITR archive binding engine (#203): a fake API
// server, a reconciler, and helpers that play the parts the engine only
// observes (sidecar reports, site Deployments, check Job pods).

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
)

const (
	pbtNS   = "db"
	pbtName = "orders"
	pbtUID  = types.UID("6f1c2d3e-0000-4000-8000-0000000002b3")
)

// pbtProfile is an S3 profile on prefix, encrypted with Secret secret
// (plaintext when secret is "").
func pbtProfile(name, prefix, secret string) v1alpha1.BackupProfile {
	p := v1alpha1.BackupProfile{Name: name, Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{
		Bucket: "archive-bucket", Prefix: prefix, EndpointURL: "https://s3.example.test", Region: "us-east-1", CredentialsSecret: "s3-creds",
	}}}
	if secret != "" {
		p.Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: secret}}
	}
	return p
}

// pbtGroup is a two-site group archiving with its first profile.
func pbtGroup(profiles ...v1alpha1.BackupProfile) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pbtName, Namespace: pbtNS, UID: pbtUID, Generation: 3},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}},
			Backup: &v1alpha1.BackupSpec{Profiles: profiles,
				PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: profiles[0].Name}},
		},
	}
}

func pbtSecret(name, value string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pbtNS},
		Data: map[string][]byte{"passphrase": []byte(value)}}
}

// pbtUIDs numbers the UIDs the harness assigns on Create.
var pbtUIDs int

// pbtFind returns the binding for destination in st.
func pbtFind(st *v1alpha1.MysqlFailoverGroupStatus, destination string) *v1alpha1.PITRArchiveBinding {
	for i := range st.PITRArchiveBindings {
		if st.PITRArchiveBindings[i].Destination == destination {
			return &st.PITRArchiveBindings[i]
		}
	}
	return nil
}

type pbtEnv struct {
	t   *testing.T
	c   client.Client
	r   *MysqlFailoverGroupReconciler
	rec *record.FakeRecorder
	now time.Time
}

func newPBTEnv(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, objs ...client.Object) *pbtEnv {
	return newPBTEnvWith(t, interceptor.Funcs{}, fg, objs...)
}

func newPBTEnvWith(t *testing.T, funcs interceptor.Funcs, fg *v1alpha1.MysqlFailoverGroup, objs ...client.Object) *pbtEnv {
	t.Helper()
	prevImage := operatorImageFromEnv
	operatorImageFromEnv = "bloodraven:test"
	t.Cleanup(func() { operatorImageFromEnv = prevImage })
	pitrCheckRetryGate.Range(func(k, _ any) bool { pitrCheckRetryGate.Delete(k); return true })
	create := funcs.Create
	funcs.Create = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		// The API server assigns UIDs; check Jobs are correlated by them.
		if obj.GetUID() == "" {
			pbtUIDs++
			obj.SetUID(types.UID(fmt.Sprintf("00000000-0000-4000-9000-%012d", pbtUIDs)))
		}
		if create != nil {
			return create(ctx, c, obj, opts...)
		}
		return c.Create(ctx, obj, opts...)
	}
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithIndex(&v1alpha1.MysqlBackupVerification{}, verificationGroupIndex, indexVerificationGroup).
		WithInterceptorFuncs(funcs).
		WithObjects(append([]client.Object{fg}, objs...)...).Build()
	rec := record.NewFakeRecorder(200)
	return &pbtEnv{t: t, c: c, rec: rec, now: time.Now(),
		r: &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: rec, APIReader: c}}
}

// fg reads the group.
func (e *pbtEnv) fg() *v1alpha1.MysqlFailoverGroup {
	e.t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbtNS, Name: pbtName}, &fg); err != nil {
		e.t.Fatal(err)
	}
	return &fg
}

// update edits the group's spec or metadata.
func (e *pbtEnv) update(edit func(fg *v1alpha1.MysqlFailoverGroup)) {
	e.t.Helper()
	fg := e.fg()
	edit(fg)
	if err := e.c.Update(context.Background(), fg); err != nil {
		e.t.Fatal(err)
	}
}

// updateStatus edits the group's status.
func (e *pbtEnv) updateStatus(edit func(st *v1alpha1.MysqlFailoverGroupStatus)) {
	e.t.Helper()
	fg := e.fg()
	edit(&fg.Status)
	if err := e.c.Status().Update(context.Background(), fg); err != nil {
		e.t.Fatal(err)
	}
}

// pass runs one binding pass on the stored group and checks that the
// in-memory status the renderer sees equals what was written.
func (e *pbtEnv) pass() *v1alpha1.MysqlFailoverGroup {
	e.t.Helper()
	fg := e.fg()
	if _, err := e.r.reconcilePITRArchiveBindings(context.Background(), fg); err != nil {
		e.t.Fatalf("pass: %v", err)
	}
	stored := e.fg()
	if len(stored.Status.PITRArchiveBindings) != len(fg.Status.PITRArchiveBindings) {
		e.t.Fatalf("in-memory bindings %+v differ from stored %+v", fg.Status.PITRArchiveBindings, stored.Status.PITRArchiveBindings)
	}
	return stored
}

// entry returns the binding for profile p of the stored group.
func (e *pbtEnv) entry(p v1alpha1.BackupProfile) *v1alpha1.PITRArchiveBinding {
	e.t.Helper()
	fg := e.fg()
	return findPITRBinding(fg, e.dest(p))
}

func (e *pbtEnv) dest(p v1alpha1.BackupProfile) pitrarchive.Destination {
	e.t.Helper()
	d, err := pitrprofile.DestinationFor(e.fg(), &p)
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

func (e *pbtEnv) cond() *metav1.Condition {
	return meta.FindStatusCondition(e.fg().Status.Conditions, conditionPITRArchiveBindingReady)
}

// snapName is the snapshot name of value at p's destination.
func (e *pbtEnv) snapName(p v1alpha1.BackupProfile, value string) string {
	e.t.Helper()
	key, err := readDigestKey(context.Background(), e.c, e.fg())
	if err != nil {
		e.t.Fatal(err)
	}
	return pitrKeySnapshotName(pbtName, key, e.dest(p).String(), []byte(strings.TrimSpace(value)))
}

func (e *pbtEnv) secret(name string) (*corev1.Secret, bool) {
	var s corev1.Secret
	err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbtNS, Name: name}, &s)
	if err != nil {
		return nil, false
	}
	return &s, true
}

func (e *pbtEnv) setSecret(name, value string) {
	e.t.Helper()
	s, ok := e.secret(name)
	if !ok {
		if err := e.c.Create(context.Background(), pbtSecret(name, value)); err != nil {
			e.t.Fatal(err)
		}
		return
	}
	s.Data = map[string][]byte{"passphrase": []byte(value)}
	if err := e.c.Update(context.Background(), s); err != nil {
		e.t.Fatal(err)
	}
}

func (e *pbtEnv) deleteObj(obj client.Object) {
	e.t.Helper()
	if err := e.c.Delete(context.Background(), obj); err != nil {
		e.t.Fatal(err)
	}
}

// observe sets status.pitr.archive as the runner would from the sidecars.
func (e *pbtEnv) observe(obs *v1alpha1.PITRArchiveObservation) {
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		if st.PITR == nil {
			st.PITR = &v1alpha1.PITRStatus{}
		}
		st.PITR.Archive = obs
	})
}

// verifiedObs is the sidecars' Verified report of enc at p's destination.
func (e *pbtEnv) verifiedObs(p v1alpha1.BackupProfile, enc v1alpha1.PITRArchiveBindingEncryption) *v1alpha1.PITRArchiveObservation {
	d := e.dest(p)
	return &v1alpha1.PITRArchiveObservation{Destination: d.String(), ArchiveID: pitrarchive.CreateArchiveID(pbtUID, d),
		EncryptionMode: enc.Mode, State: v1alpha1.PITRArchiveVerified, KeyRevision: pitrKeyRevision(enc)}
}

// refusedObs is the sidecars' refusal of enc at p's destination.
func (e *pbtEnv) refusedObs(p v1alpha1.BackupProfile, enc v1alpha1.PITRArchiveBindingEncryption, reason string) *v1alpha1.PITRArchiveObservation {
	return &v1alpha1.PITRArchiveObservation{Destination: e.dest(p).String(), EncryptionMode: enc.Mode,
		State: v1alpha1.PITRArchiveRefused, Reason: v1alpha1.PITRArchiveRefusalReason(reason), KeyRevision: pitrKeyRevision(enc)}
}

// render is the pitr-render annotation of enc at p's destination.
func (e *pbtEnv) render(p v1alpha1.BackupProfile, enc v1alpha1.PITRArchiveBindingEncryption) string {
	d := e.dest(p)
	return pitrRenderInputs{
		SchemaVersion: pitrRenderSchemaVersion, Enabled: true, Destination: d.String(),
		ArchiveID: pitrarchive.CreateArchiveID(pbtUID, d), StorageType: "S3", Bucket: d.Bucket, KeyPrefix: d.KeyPrefix,
		EncryptionMode: string(enc.Mode), Algorithm: string(enc.Algorithm), KeySecretName: enc.KeySecretName,
		Region: "us-east-1", EndpointURL: "https://s3.example.test", CredentialsSecret: "s3-creds",
	}.Canonical()
}

// deploy creates (or replaces) site's Deployment with a pitr-render
// annotation ("" for a pre-#203 Deployment).
func (e *pbtEnv) deploy(site, render string) {
	e.t.Helper()
	d := pbcDeploy("mysql-"+pbtName+"-"+site, pbtName, site, render)
	d.Namespace = pbtNS
	var existing appsv1.Deployment
	if err := e.c.Get(context.Background(), client.ObjectKeyFromObject(&d), &existing); err == nil {
		existing.Annotations = d.Annotations
		if err := e.c.Update(context.Background(), &existing); err != nil {
			e.t.Fatal(err)
		}
		return
	}
	if err := e.c.Create(context.Background(), &d); err != nil {
		e.t.Fatal(err)
	}
}

func encOf(mode v1alpha1.PITRArchiveEncryptionMode, snap string) v1alpha1.PITRArchiveBindingEncryption {
	if mode == v1alpha1.PITRArchiveEncryptionNone {
		return v1alpha1.PITRArchiveBindingEncryption{Mode: mode}
	}
	return v1alpha1.PITRArchiveBindingEncryption{Mode: mode, Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM, KeySecretName: snap}
}

func passEnc(snap string) v1alpha1.PITRArchiveBindingEncryption {
	return encOf(v1alpha1.PITRArchiveEncryptionPassphrase, snap)
}

var noneEnc = encOf(v1alpha1.PITRArchiveEncryptionNone, "")

// checkJob returns the check Job the entry records.
func (e *pbtEnv) checkJob(entry *v1alpha1.PITRArchiveBinding) *batchv1.Job {
	e.t.Helper()
	if entry == nil || entry.Check == nil {
		e.t.Fatalf("no check in flight: %+v", entry)
	}
	var job batchv1.Job
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbtNS, Name: entry.Check.JobName}, &job); err != nil {
		e.t.Fatalf("check job %s: %v", entry.Check.JobName, err)
	}
	return &job
}

func (e *pbtEnv) jobs() []batchv1.Job {
	var list batchv1.JobList
	if err := e.c.List(context.Background(), &list, client.InNamespace(pbtNS), client.MatchingLabels{labelResourceKind: resourcePITRArchiveCheck}); err != nil {
		e.t.Fatal(err)
	}
	return list.Items
}

// finishCheck plays the check Job: its pod terminates with message and
// exit code, and the Job records the outcome.
func (e *pbtEnv) finishCheck(job *batchv1.Job, code int32, message string) {
	e.t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-" + string(job.UID)[len(job.UID)-6:], Namespace: pbtNS,
			Labels:          map[string]string{"batch.kubernetes.io/job-name": job.Name},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{
			Name: pitrCheckContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: message}},
		}}},
	}
	cond := batchv1.JobComplete
	if code != 0 {
		pod.Status.Phase = corev1.PodFailed
		cond = batchv1.JobFailed
	}
	if err := e.c.Create(context.Background(), pod); err != nil {
		e.t.Fatal(err)
	}
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: cond, Status: corev1.ConditionTrue})
	if err := e.c.Status().Update(context.Background(), job); err != nil {
		e.t.Fatal(err)
	}
}

// verdict plays a check Job that reports res.
func (e *pbtEnv) verdict(entry *v1alpha1.PITRArchiveBinding, res PITRArchiveCheckResult) {
	e.t.Helper()
	if res.Destination == "" {
		res.Destination = entry.Destination
	}
	e.finishCheck(e.checkJob(entry), 0, FormatPITRArchiveCheckLine(res))
}

// events drains the recorder.
func (e *pbtEnv) events() []string {
	var out []string
	for {
		select {
		case ev := <-e.rec.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

// expectEvents checks that exactly the given reasons were emitted (in any
// order) since the last drain.
func (e *pbtEnv) expectEvents(reasons ...string) []string {
	e.t.Helper()
	got := e.events()
	if len(got) != len(reasons) {
		e.t.Fatalf("events = %q, want reasons %v", got, reasons)
	}
	for _, r := range reasons {
		found := false
		for _, ev := range got {
			if strings.Contains(ev, " "+r+" ") {
				found = true
			}
		}
		if !found {
			e.t.Fatalf("events = %q, want reason %s", got, r)
		}
	}
	return got
}

// expectCond checks the condition's status and reason.
func (e *pbtEnv) expectCond(status metav1.ConditionStatus, reason string) *metav1.Condition {
	e.t.Helper()
	c := e.cond()
	if c == nil || c.Status != status || c.Reason != reason {
		e.t.Fatalf("condition = %+v, want %s/%s", c, status, reason)
	}
	return c
}

// expectEntry checks the entry's state and reason.
func (e *pbtEnv) expectEntry(p v1alpha1.BackupProfile, state v1alpha1.PITRArchiveBindingState, reason v1alpha1.PITRArchiveBindingReason) *v1alpha1.PITRArchiveBinding {
	e.t.Helper()
	got := e.entry(p)
	if got == nil || got.State != state || got.Reason != reason {
		e.t.Fatalf("entry for %s = %+v, want %s/%s", p.Name, got, state, reason)
	}
	return got
}

// bound drives p's write binding to Bound by consumer proof and returns it.
func (e *pbtEnv) bound(p v1alpha1.BackupProfile) *v1alpha1.PITRArchiveBinding {
	e.t.Helper()
	e.pass()
	entry := e.entry(p)
	if entry == nil {
		e.t.Fatalf("no entry for %s", p.Name)
	}
	e.observe(e.verifiedObs(p, entry.Encryption))
	e.pass()
	e.events()
	return e.expectEntry(p, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
}

// §4.1: targets merge by destination; the identity profile is the write
// profile when written, else the first read profile; missing profiles and
// terminal readers contribute nothing.
func TestPITRBindingTargets(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	alias := pbtProfile("alias", "p1", "pass-b")
	src := pbtProfile("src", "p9", "pass-src")
	fg := pbtGroup(a, alias, src)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	mbv := func(name, mode, profile string, phase v1alpha1.VerificationPhase) v1alpha1.MysqlBackupVerification {
		return v1alpha1.MysqlBackupVerification{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pbtNS},
			Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: pbtName},
				PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: mode, ArchiveProfileName: profile}},
			Status: v1alpha1.MysqlBackupVerificationStatus{Phase: phase},
		}
	}
	mbvs := []v1alpha1.MysqlBackupVerification{
		mbv("z-alias", "latest", "alias", ""),
		mbv("b-default", "timestamp", "", v1alpha1.VerificationPhasePending),
		mbv("c-none", "none", "src", ""),
		mbv("d-done", "latest", "src", v1alpha1.VerificationPhaseSucceeded),
		mbv("e-missing", "latest", "nope", ""),
	}
	targets := pitrBindingTargets(fg, mbvs)
	if len(targets) != 2 {
		t.Fatalf("targets = %+v", targets)
	}
	w, r := targets[0], targets[1]
	if !w.write || !w.read || w.profile.Name != "a" || pitrProfileNames(w.uses) != "a, alias" ||
		strings.Join(w.readers, ",") != "MysqlBackupVerification b-default,MysqlBackupVerification z-alias" {
		t.Fatalf("write target = %+v uses %s readers %v", w, pitrProfileNames(w.uses), w.readers)
	}
	if r.write || !r.read || r.profile.Name != "src" || strings.Join(r.readers, ",") != "initFromBackup" {
		t.Fatalf("read target = %+v", r)
	}
	// A terminal restore and disabled PITR leave only the verification that
	// names its profile.
	fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseSucceeded}
	fg.Spec.Backup.PITR.Enabled = false
	targets = pitrBindingTargets(fg, mbvs)
	if len(targets) != 1 || targets[0].write || targets[0].profile.Name != "alias" {
		t.Fatalf("targets with PITR off = %+v", targets)
	}
}

// Row 37 (fresh group, empty prefix), K4 and row 52: the candidate snapshot
// is made before any writer exists, no check Job runs, and only a Verified
// report with the exact mode and revision binds it.
func TestPITRBindingFreshGroupConsumerProof(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1\n"))
	e.pass()
	want := passEnc(e.snapName(a, "k1"))
	entry := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	if entry.Encryption != want || entry.ID != pitrBindingID(e.dest(a)) || entry.Check != nil {
		t.Fatalf("entry = %+v, want candidate %+v", entry, want)
	}
	snap, ok := e.secret(want.KeySecretName)
	if !ok {
		t.Fatal("candidate snapshot not created")
	}
	if snap.Immutable == nil || !*snap.Immutable || len(snap.OwnerReferences) != 0 || string(snap.Data["passphrase"]) != "k1" ||
		snap.Labels[labelAppName] != appNamePITRKeySnapshot || snap.Labels[labelFailoverGroupUID] != string(pbtUID) ||
		snap.Labels[labelPITRBindingID] != entry.ID || snap.Labels[labelFailoverGroup] != pbtName ||
		snap.Annotations[annotationPITRDestination] != entry.Destination || snap.Annotations[annotationPITRAccepted] != "" {
		t.Fatalf("snapshot = %+v", snap)
	}
	if len(e.jobs()) != 0 {
		t.Fatal("a check Job ran for a group with no writer")
	}
	e.expectCond(metav1.ConditionUnknown, string(pitrBindingAuthenticating))

	// No proof: a pre-#203 pod's empty revision, another mode, another
	// revision, a refusal, an ID not derived for this group.
	for name, obs := range map[string]*v1alpha1.PITRArchiveObservation{
		"empty revision": func() *v1alpha1.PITRArchiveObservation { o := e.verifiedObs(a, want); o.KeyRevision = ""; return o }(),
		"other mode": func() *v1alpha1.PITRArchiveObservation {
			o := e.verifiedObs(a, want)
			o.EncryptionMode = "None"
			return o
		}(),
		"other revision": func() *v1alpha1.PITRArchiveObservation { o := e.verifiedObs(a, want); o.KeyRevision = "x"; return o }(),
		"no archive ID":  func() *v1alpha1.PITRArchiveObservation { o := e.verifiedObs(a, want); o.ArchiveID = ""; return o }(),
		"foreign ID": func() *v1alpha1.PITRArchiveObservation {
			o := e.verifiedObs(a, want)
			o.ArchiveID = pitrarchive.CreateArchiveID("someone-else", e.dest(a))
			return o
		}(),
	} {
		e.observe(obs)
		e.pass()
		if got := e.entry(a); got.State != v1alpha1.PITRArchiveBindingPending {
			t.Fatalf("%s proved the binding: %+v", name, got)
		}
	}
	e.events()

	// One verified site suffices (State AwaitingDescriptor with an ID).
	obs := e.verifiedObs(a, want)
	obs.State = v1alpha1.PITRArchiveAwaitingDescriptor
	e.observe(obs)
	e.pass()
	entry = e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
	if entry.ArchiveID != obs.ArchiveID || entry.Source == nil || entry.Source.UID != pbtUID || entry.Source.Name != pbtName || entry.Encryption != want {
		t.Fatalf("bound entry = %+v", entry)
	}
	e.expectEvents(eventReasonPITRArchiveBound)
	snap, _ = e.secret(want.KeySecretName)
	if a, err := verifyPITRAcceptance(snap); err != nil || a.ArchiveID != entry.ArchiveID || a.Source != *entry.Source {
		t.Fatalf("acceptance = %+v, %v", a, err)
	}
	c := e.expectCond(metav1.ConditionTrue, string(pitrBindingBound))
	if !strings.Contains(c.Message, "no full backup is stamped with archive "+entry.ArchiveID) || c.ObservedGeneration != 3 {
		t.Fatalf("condition = %+v", c)
	}

	// A succeeded backup stamped with the archive drops the baseline note.
	b := &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: pbtNS},
		Spec:   v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: pbtName}, ProfileName: "a"},
		Status: v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded, PITRArchiveID: entry.ArchiveID}}
	if err := e.c.Create(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	e.pass()
	if c := e.cond(); strings.Contains(c.Message, "no full backup") {
		t.Fatalf("baseline note kept: %s", c.Message)
	}
	// Steady state: nothing changes, nothing is emitted.
	before := e.fg().ResourceVersion
	e.pass()
	if e.fg().ResourceVersion != before {
		t.Fatal("an unchanged pass wrote status")
	}
	e.expectEvents()
}

// Row 47 (O13): an encrypted profile whose Secret does not exist yet has no
// candidate; the entry says why, and the Secret's arrival makes one.
func TestPITRBindingNoCandidate(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a))
	e.pass()
	entry := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	if entry.Encryption.KeySecretName != "" || entry.Encryption.Mode != v1alpha1.PITRArchiveEncryptionPassphrase || !strings.Contains(entry.Message, "pass-a") {
		t.Fatalf("entry = %+v", entry)
	}
	e.expectCond(metav1.ConditionUnknown, string(pitrBindingAuthenticating))
	e.setSecret("pass-a", "k1")
	e.pass()
	if got := e.entry(a); got.Encryption.KeySecretName != e.snapName(a, "k1") || got.Message != "" {
		t.Fatalf("entry after the Secret appeared = %+v", got)
	}
}

// Row 16: a plaintext binding is not durable; after a status wipe the
// running writer (held render = the candidate) re-proves it with the
// "plaintext" revision, without a check Job.
func TestPITRBindingPlaintextStatusWipe(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	e := newPBTEnv(t, pbtGroup(a))
	e.bound(a)
	e.deploy("iad", e.render(a, noneEnc))
	e.deploy("pdx", e.render(a, noneEnc))
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = nil })
	e.observe(nil)
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	if len(e.jobs()) != 0 {
		t.Fatal("a check Job ran although the running writer renders the candidate")
	}
	e.observe(e.verifiedObs(a, noneEnc))
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
	e.expectCond(metav1.ConditionTrue, string(pitrBindingBound))
}

// Drift on a Bound write binding (§4.5; rows 2, 4-10, 22, 25, 31, 33):
// the entry never changes, only the condition, and each refusal Warns once.
func TestPITRBindingDriftOnBound(t *testing.T) {
	type step struct {
		name   string
		edit   func(e *pbtEnv)
		reason string
		status metav1.ConditionStatus
		event  []string
		inMsg  []string
	}
	a := pbtProfile("a", "p1", "pass-a")
	steps := []step{
		{name: "row 7 whitespace-only edit", edit: func(e *pbtEnv) { e.setSecret("pass-a", "  k1 \n") },
			status: metav1.ConditionTrue, reason: string(pitrBindingBound)},
		{name: "row 33 another key of the same Secret", edit: func(e *pbtEnv) {
			s, _ := e.secret("pass-a")
			s.Data["username"] = []byte("x")
			_ = e.c.Update(context.Background(), s)
		}, status: metav1.ConditionTrue, reason: string(pitrBindingBound)},
		{name: "row 6 in-place rotation", edit: func(e *pbtEnv) { e.setSecret("pass-a", "k2") },
			status: metav1.ConditionFalse, reason: reasonPITRKeyChangeRefused, event: []string{eventReasonPITRArchiveKeyChangeRefused},
			inMsg: []string{"profile a's passphrase (Secret pass-a key passphrase)", pitrBindingProcedure}},
		{name: "rotation back", edit: func(e *pbtEnv) { e.setSecret("pass-a", "k1") },
			status: metav1.ConditionTrue, reason: string(pitrBindingBound)},
		{name: "row 8 live Secret deleted", edit: func(e *pbtEnv) { s, _ := e.secret("pass-a"); e.deleteObj(s) },
			status: metav1.ConditionTrue, reason: string(pitrBindingBound), event: []string{eventReasonPITRArchiveKeySourceRemoved},
			inMsg: []string{"key source removed"}},
		{name: "row 4 renamed Secret, same bytes", edit: func(e *pbtEnv) {
			e.setSecret("pass-a2", "k1")
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name = "pass-a2"
			})
		}, status: metav1.ConditionTrue, reason: string(pitrBindingBound)},
		{name: "row 5 renamed Secret, other bytes", edit: func(e *pbtEnv) {
			e.setSecret("pass-a3", "k3")
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name = "pass-a3"
			})
		}, status: metav1.ConditionFalse, reason: reasonPITRKeyChangeRefused, event: []string{eventReasonPITRArchiveKeyChangeRefused}},
		{name: "row 2 encryption removed", edit: func(e *pbtEnv) {
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Profiles[0].Encryption = nil })
		}, status: metav1.ConditionFalse, reason: reasonPITREncryptionChangeRefused, event: []string{eventReasonPITRArchiveEncryptionChangeRefused},
			inMsg: []string{"profile a configures None", "accepted as Passphrase/AES-256-GCM"}},
		{name: "encryption restored", edit: func(e *pbtEnv) {
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "pass-a2"}}
			})
		}, status: metav1.ConditionTrue, reason: string(pitrBindingBound)},
		{name: "row 9 alias with the same bytes made active", edit: func(e *pbtEnv) {
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
				b := pbtProfile("b", "p1", "pass-a2")
				fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, b)
				fg.Spec.Backup.PITR.ProfileName = "b"
			})
		}, status: metav1.ConditionTrue, reason: string(pitrBindingBound)},
		{name: "row 10 alias with other bytes made active", edit: func(e *pbtEnv) {
			e.setSecret("pass-b", "kb")
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[1].Encryption.PassphraseSecret.Name = "pass-b"
			})
		}, status: metav1.ConditionFalse, reason: reasonPITRKeyChangeRefused, event: []string{eventReasonPITRArchiveKeyChangeRefused},
			inMsg: []string{"profile b's passphrase", "profiles resolving there: b, a"}},
		{name: "row 10 alias with other encryption made active", edit: func(e *pbtEnv) {
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Profiles[1].Encryption = nil })
		}, status: metav1.ConditionFalse, reason: reasonPITREncryptionChangeRefused, event: []string{eventReasonPITRArchiveEncryptionChangeRefused}},
		{name: "row 22 PITR disabled", edit: func(e *pbtEnv) {
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.Enabled = false })
		}, status: metav1.ConditionTrue, reason: string(pitrBindingBound)},
		{name: "row 25 profile removed and re-added with rotated bytes", edit: func(e *pbtEnv) {
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles = []v1alpha1.BackupProfile{pbtProfile("a", "p1", "pass-a3")}
				fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"}
			})
		}, status: metav1.ConditionFalse, reason: reasonPITRKeyChangeRefused, event: []string{eventReasonPITRArchiveKeyChangeRefused}},
	}
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	bound := e.bound(a)
	for _, s := range steps {
		s.edit(e)
		e.pass()
		if got := e.entry(a); got == nil || got.State != bound.State || got.Reason != bound.Reason || got.Encryption != bound.Encryption || got.ArchiveID != bound.ArchiveID {
			t.Fatalf("%s: entry changed: %+v", s.name, got)
		}
		c := e.expectCond(s.status, s.reason)
		for _, m := range s.inMsg {
			if !strings.Contains(c.Message, m) {
				t.Fatalf("%s: message %q lacks %q", s.name, c.Message, m)
			}
		}
		e.expectEvents(s.event...)
		// Edge-triggered: the same state emits nothing more.
		e.pass()
		e.expectEvents()
	}
}

// Row 20 (Δ8): a sidecar refusal of a Bound entry (a replaced descriptor)
// never demotes it.
func TestPITRBindingBoundNeverDemoted(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	bound := e.bound(a)
	e.observe(e.refusedObs(a, bound.Encryption, "ArchiveKeyMismatch"))
	e.pass()
	if got := e.entry(a); got.State != v1alpha1.PITRArchiveBindingBound || got.Reason != pitrBindingBound {
		t.Fatalf("entry = %+v", got)
	}
	e.expectCond(metav1.ConditionTrue, string(pitrBindingBound))
	e.expectEvents()
}

// §4.4 correlated sidecar refusal (non-Bound only): every refusal reason
// maps to a binding reason; UnsafeHistory and StoreUnsupported leave the
// entry Pending pointing at PITRArchiveReady; a report for another revision
// is not correlated.
func TestPITRBindingCorrelatedSidecarRefusal(t *testing.T) {
	cases := map[string]v1alpha1.PITRArchiveBindingReason{
		"ArchiveKeyMismatch": pitrBindingKeyMismatch, "ArchiveForeign": pitrBindingArchiveForeign,
		"ArchiveCopy": pitrBindingArchiveCopy, "ArchiveLegacy": pitrBindingArchiveLegacy,
		"ArchiveDescriptorMissing": pitrBindingArchiveDescriptorMissing, "ArchiveEncryptionMismatch": pitrBindingArchiveEncryptionMismatch,
		"ArchiveVersionUnsupported": pitrBindingArchiveVersionUnsupported, "ArchiveUnsafeHistory": "", "ArchiveStoreUnsupported": "",
	}
	for raw, want := range cases {
		t.Run(raw, func(t *testing.T) {
			a := pbtProfile("a", "p1", "pass-a")
			e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
			e.pass()
			cand := e.entry(a).Encryption
			other := cand
			other.KeySecretName = "mysql-orders-pitr-key-0000000000"
			e.observe(e.refusedObs(a, other, raw))
			e.pass()
			e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
			e.observe(e.refusedObs(a, cand, raw))
			e.pass()
			if want == "" {
				got := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
				if !strings.Contains(got.Message, "PITRArchiveReady") {
					t.Fatalf("message %q", got.Message)
				}
				return
			}
			got := e.expectEntry(a, v1alpha1.PITRArchiveBindingRefused, want)
			if want == pitrBindingKeyMismatch && !strings.Contains(got.Message, "recover the archive's original passphrase into Secret pass-a key passphrase") {
				t.Fatalf("first-upgrade text missing: %q", got.Message)
			}
			e.expectCond(metav1.ConditionFalse, string(want))
			e.expectEvents(eventReasonPITRArchiveBindingRefused)
		})
	}
}

// Δ21/O14: a Refused entry is retried only on new input: the live
// candidate changing, the encryption identity changing, or the sidecars'
// correlated report no longer refusing. Without new input it stays put.
func TestPITRBindingRetryOnlyOnNewInput(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	e.pass()
	cand := e.entry(a).Encryption
	e.observe(e.refusedObs(a, cand, "ArchiveKeyMismatch"))
	e.pass()
	refused := e.expectEntry(a, v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch)
	e.events()
	for i := 0; i < 3; i++ {
		e.pass()
		if got := e.entry(a); got.State != refused.State || !got.LastTransitionTime.Equal(refused.LastTransitionTime) {
			t.Fatalf("pass %d retried without new input: %+v", i, got)
		}
	}
	e.expectEvents()
	// The sidecars' report for the same revision no longer refuses.
	obs := e.refusedObs(a, cand, "")
	obs.State = v1alpha1.PITRArchiveAwaitingDescriptor
	e.observe(obs)
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	// Refused again, then a new live key retries with the new candidate.
	e.observe(e.refusedObs(a, cand, "ArchiveKeyMismatch"))
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch)
	e.setSecret("pass-a", "k0")
	e.pass()
	got := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	if got.Encryption.KeySecretName != e.snapName(a, "k0") {
		t.Fatalf("retry candidate = %+v", got.Encryption)
	}
}

// C1 (row 26, 26a): a read binding is authenticated by a check Job; a wrong
// key refuses KeyMismatch and waits for new input; the fixed Secret runs a
// new check, and Verified binds the archive with its foreign source.
func TestPITRBindingReadCheck(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	src := pbtProfile("src", "p9", "pass-src")
	fg := pbtGroup(a, src)
	fg.Spec.Backup.PITR.Enabled = false
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	e := newPBTEnv(t, fg, pbtSecret("pass-src", "wrong"))
	e.pass()
	entry := e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	job := e.checkJob(entry)
	if env := envMap(job.Spec.Template.Spec.Containers[0].Env); env[PITRArchiveCheckRoleEnv] != PITRArchiveCheckRoleReader ||
		job.Labels[labelPITRKeySecret] != entry.Encryption.KeySecretName {
		t.Fatalf("check job env %v labels %v", env, job.Labels)
	}
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveKeyMismatch})
	e.pass()
	refused := e.expectEntry(src, v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch)
	if !strings.Contains(refused.Message, "Secret pass-src key passphrase") {
		t.Fatalf("message %q", refused.Message)
	}
	e.expectCond(metav1.ConditionFalse, string(pitrBindingKeyMismatch))
	e.expectEvents(eventReasonPITRArchiveBindingRefused)
	if len(e.jobs()) != 0 {
		t.Fatal("the consumed check Job was not deleted")
	}
	e.pass()
	if len(e.jobs()) != 0 {
		t.Fatal("a refused read binding re-checked without new input")
	}
	// The user fixes the Secret: a new round (the listed wrong snapshot
	// first, then the live bytes), Verified, Bound (foreign).
	e.setSecret("pass-src", "right")
	e.pass()
	entry = e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	if entry.Encryption.KeySecretName != refused.Encryption.KeySecretName {
		t.Fatalf("round started with %s, want the listed snapshot %s", entry.Encryption.KeySecretName, refused.Encryption.KeySecretName)
	}
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveKeyMismatch})
	e.pass()
	entry = e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	foreign := v1alpha1.PITRArchiveSource{Namespace: "other", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ff"}
	archive := pitrarchive.CreateArchiveID(foreign.UID, e.dest(src))
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified, ArchiveID: archive, Source: foreign})
	e.pass()
	bound := e.expectEntry(src, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
	if bound.ArchiveID != archive || *bound.Source != foreign || bound.Encryption.KeySecretName != e.snapName(src, "right") {
		t.Fatalf("bound read entry = %+v", bound)
	}
	snap, _ := e.secret(bound.Encryption.KeySecretName)
	if a, err := verifyPITRAcceptance(snap); err != nil || a.Source != foreign {
		t.Fatalf("acceptance of a read binding = %+v, %v", a, err)
	}

	// Row 26a: an unfixable verdict refuses (restores and MBVs fail on it).
	e2 := newPBTEnv(t, fg.DeepCopy(), pbtSecret("pass-src", "k"))
	e2.pass()
	e2.verdict(e2.entry(src), PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveNotFound})
	e2.pass()
	got := e2.expectEntry(src, v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveNotFound)
	if r := pitrBindingReadiness(got); r == nil || !r.terminal {
		t.Fatalf("readiness = %+v", r)
	}
}

// §4.4 candidate order: listed snapshots (accepted first, then newest)
// before the live bytes; KeyMismatch moves to the next candidate in the
// same pass; exhausting them refuses KeyMismatch and records the live
// candidate, so no retry happens without new input.
func TestPITRBindingCandidateOrder(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	src := pbtProfile("src", "p9", "pass-src")
	fg := pbtGroup(a, src)
	fg.Spec.Backup.PITR.Enabled = false
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	e := newPBTEnv(t, fg, pbtSecret("pass-src", "live"))
	e.pass() // creates the digest key and the live snapshot
	dest := e.dest(src).String()
	key, _ := readDigestKey(context.Background(), e.c, e.fg())
	older := newPITRKeySnapshot(e.fg(), pitrKeySnapshotName(pbtName, key, dest, []byte("old")), dest, []byte("old"), "")
	older.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	newer := newPITRKeySnapshot(e.fg(), pitrKeySnapshotName(pbtName, key, dest, []byte("new")), dest, []byte("new"), "")
	newer.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	for _, s := range []*corev1.Secret{older, newer} {
		if err := e.c.Create(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	// Restart the round on the new listing.
	for _, j := range e.jobs() {
		e.deleteObj(&j)
	}
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = nil })
	e.pass()
	var tried []string
	for i := 0; i < 3; i++ {
		entry := e.entry(src)
		tried = append(tried, entry.Encryption.KeySecretName)
		e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveKeyMismatch})
		e.pass()
	}
	want := []string{newer.Name, older.Name, e.snapName(src, "live")}
	if strings.Join(tried, ",") != strings.Join(want, ",") {
		t.Fatalf("tried %v, want %v", tried, want)
	}
	got := e.expectEntry(src, v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch)
	if got.Encryption.KeySecretName != want[2] {
		t.Fatalf("refused entry records %+v, want the live candidate", got.Encryption)
	}
	e.pass()
	if len(e.jobs()) != 0 {
		t.Fatal("an exhausted round started again without new input")
	}
}

// C2 + K3 (rows 38, 38a, 38b): pre-#203 Deployments (no decodable render)
// mean a writer runs with an unknown key: a Writer check authenticates the
// live key; a refusal names the deferred Deployments.
func TestPITRBindingPre203Deployments(t *testing.T) {
	for _, tc := range []struct {
		name   string
		res    PITRArchiveCheckResult
		state  v1alpha1.PITRArchiveBindingState
		reason v1alpha1.PITRArchiveBindingReason
	}{
		{name: "row 38 verified", res: PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified}, state: v1alpha1.PITRArchiveBindingBound, reason: pitrBindingBound},
		{name: "row 38a key mismatch", res: PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveKeyMismatch}, state: v1alpha1.PITRArchiveBindingRefused, reason: pitrBindingKeyMismatch},
		{name: "row 38b legacy", res: PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveLegacy}, state: v1alpha1.PITRArchiveBindingRefused, reason: pitrBindingArchiveLegacy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := pbtProfile("a", "p1", "pass-a")
			e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
			e.deploy("iad", "")
			e.deploy("pdx", `{"schemaVersion":9}`)
			e.pass()
			entry := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
			job := e.checkJob(entry)
			if envMap(job.Spec.Template.Spec.Containers[0].Env)[PITRArchiveCheckRoleEnv] != PITRArchiveCheckRoleWriter {
				t.Fatal("a write target's check is not a Writer check")
			}
			res := tc.res
			if res.State == PITRArchiveCheckStateVerified {
				res.ArchiveID = pitrarchive.CreateArchiveID(pbtUID, e.dest(a))
				res.Source = v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID}
			}
			e.verdict(entry, res)
			e.pass()
			got := e.expectEntry(a, tc.state, tc.reason)
			if tc.state == v1alpha1.PITRArchiveBindingRefused {
				c := e.expectCond(metav1.ConditionFalse, string(tc.reason))
				if !strings.Contains(c.Message, "Deployments mysql-orders-iad, mysql-orders-pdx are not updated") {
					t.Fatalf("deferral not named: %q", c.Message)
				}
				return
			}
			if got.Source.UID != pbtUID {
				t.Fatalf("entry = %+v", got)
			}
		})
	}
}

// Row 23 (procedure: a new empty prefix while the old one is held), row 24
// (a legacy prefix): a C2 check; Absent → Establishing (no further check),
// and the writer's proof binds it; the old binding is retained.
func TestPITRBindingPrefixSwitch(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	c := pbtProfile("c", "p2", "pass-c")
	l := pbtProfile("l", "p3", "pass-c")
	e := newPBTEnv(t, pbtGroup(a, c, l), pbtSecret("pass-a", "k1"), pbtSecret("pass-c", "k2"))
	old := e.bound(a)
	e.deploy("iad", e.render(a, old.Encryption))
	e.deploy("pdx", e.render(a, old.Encryption))
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "c" })
	e.pass()
	entry := e.expectEntry(c, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateAbsent})
	e.pass()
	est := e.expectEntry(c, v1alpha1.PITRArchiveBindingPending, pitrBindingEstablishing)
	e.pass()
	if len(e.jobs()) != 0 {
		t.Fatal("an Establishing entry ran another check")
	}
	e.expectCond(metav1.ConditionUnknown, string(pitrBindingEstablishing))
	e.observe(e.verifiedObs(c, est.Encryption))
	e.pass()
	e.expectEntry(c, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
	e.expectCond(metav1.ConditionTrue, string(pitrBindingBound))

	// Row 24: switching to a legacy prefix refuses; the held writer keeps
	// archiving to c.
	e.deploy("iad", e.render(c, est.Encryption))
	e.deploy("pdx", e.render(c, est.Encryption))
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "l" })
	e.pass()
	e.verdict(e.entry(l), PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveLegacy})
	e.pass()
	e.expectEntry(l, v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveLegacy)
	e.expectCond(metav1.ConditionFalse, string(pitrBindingArchiveLegacy))
}

// Rows 12/13 via a check: a foreign write prefix refuses with the owner
// recorded; a copy refuses ArchiveCopy.
func TestPITRBindingForeignWriteCheck(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	c := pbtProfile("c", "p2", "pass-a")
	e := newPBTEnv(t, pbtGroup(a, c), pbtSecret("pass-a", "k1"))
	old := e.bound(a)
	e.deploy("iad", e.render(a, old.Encryption))
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "c" })
	e.pass()
	owner := v1alpha1.PITRArchiveSource{Namespace: "x", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ee"}
	e.verdict(e.entry(c), PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveForeign,
		ArchiveID: pitrarchive.CreateArchiveID(owner.UID, e.dest(c)), Source: owner})
	e.pass()
	got := e.expectEntry(c, v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveForeign)
	if got.Source == nil || *got.Source != owner {
		t.Fatalf("foreign owner not recorded: %+v", got)
	}
}

// Row 21: a check Job that cannot start is deleted, reported, and retried
// after pitrCheckRetryAfter, not before; a Job that fails without a verdict
// line is a failure too.
func TestPITRBindingCheckCannotStart(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	src := pbtProfile("src", "p9", "")
	fg := pbtGroup(a, src)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	e := newPBTEnv(t, fg)
	e.pass()
	entry := e.entry(src)
	job := e.checkJob(entry)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-p", Namespace: pbtNS, CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
			Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
		Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: pitrCheckContainerName,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "pull denied"}}}}},
	}
	if err := e.c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	e.pass()
	got := e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	if !strings.Contains(got.Message, "could not start") || !strings.Contains(got.Message, "ImagePullBackOff") || got.Check != nil {
		t.Fatalf("entry = %+v", got)
	}
	if len(e.jobs()) != 0 {
		t.Fatal("the stuck check Job was not deleted")
	}
	e.pass()
	if len(e.jobs()) != 0 {
		t.Fatal("retried before pitrCheckRetryAfter")
	}
	pitrCheckRetryGate.Store(pitrCheckRetryKey(e.fg(), got.ID), time.Now().Add(-time.Second))
	e.pass()
	entry = e.entry(src)
	e.finishCheck(e.checkJob(entry), 2, "")
	e.pass()
	if got := e.entry(src); !strings.Contains(got.Message, "failed") || got.State != v1alpha1.PITRArchiveBindingPending {
		t.Fatalf("failure without a line = %+v", got)
	}
}

// Row 14: an operator restart mid-check reads the result of the recorded
// Job by UID; a Job of that name with another UID is not trusted: it is
// deleted and a new one is created.
func TestPITRBindingRestartMidCheck(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	src := pbtProfile("src", "p9", "")
	fg := pbtGroup(a, src)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	e := newPBTEnv(t, fg)
	e.pass()
	job := e.checkJob(e.entry(src))
	job.UID = "job-uid-1"
	if err := e.c.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		pbtFind(st, e.dest(src).String()).Check.JobUID = "job-uid-1"
	})
	// A fresh reconciler (operator restart) on the persisted status.
	e.r = &MysqlFailoverGroupReconciler{Client: e.c, Scheme: testScheme(), Recorder: e.rec, APIReader: e.c}
	e.pass()
	if got := e.entry(src); got.Check == nil || got.Check.JobUID != "job-uid-1" || len(e.jobs()) != 1 {
		t.Fatalf("restart replaced the in-flight check: %+v", got)
	}
	// Status names another UID: the Job is a leftover.
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { pbtFind(st, e.dest(src).String()).Check = nil })
	e.pass()
	if len(e.jobs()) != 0 {
		t.Fatal("a Job whose UID status does not record was kept")
	}
	e.pass()
	if got := e.entry(src); got.Check == nil || len(e.jobs()) != 1 {
		t.Fatalf("no new check after the leftover went: %+v", got)
	}
}

// K6 (rows 42, 43): a Bound read binding made the write target is never
// written unless this group created it at exactly this destination; the
// condition refuses, the entry stays Bound.
func TestPITRBindingWriterEligibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source v1alpha1.PITRArchiveSource
		id     func(d pitrarchive.Destination) string
		reason v1alpha1.PITRArchiveBindingReason
	}{
		{name: "row 42 foreign", source: v1alpha1.PITRArchiveSource{Namespace: "x", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ee"},
			id: func(d pitrarchive.Destination) string {
				return pitrarchive.CreateArchiveID("00000000-0000-4000-8000-0000000000ee", d)
			}, reason: pitrBindingArchiveForeign},
		{name: "row 43 copy", source: v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID},
			id: func(pitrarchive.Destination) string { return "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa" }, reason: pitrBindingArchiveCopy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := pbtProfile("a", "p1", "")
			e := newPBTEnv(t, pbtGroup(a))
			d := e.dest(a)
			src := tc.source
			e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
				st.PITRArchiveBindings = []v1alpha1.PITRArchiveBinding{{ID: pitrBindingID(d), Destination: d.String(), ArchiveID: tc.id(d),
					Source: &src, Encryption: noneEnc, State: v1alpha1.PITRArchiveBindingBound, Reason: pitrBindingBound}}
			})
			e.pass()
			e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
			e.expectCond(metav1.ConditionFalse, string(tc.reason))
			e.expectEvents(eventReasonPITRArchiveBindingRefused)
			e.pass()
			e.expectEvents()
		})
	}
}

// Row 30: a 17th destination gets no entry; the condition names it.
func TestPITRBindingLimit(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	e := newPBTEnv(t, pbtGroup(a))
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		for i := 0; i < pitrArchiveBindingsMax; i++ {
			d, _ := pitrarchive.NewS3Destination("other", "x"+string(rune('a'+i)), "")
			st.PITRArchiveBindings = append(st.PITRArchiveBindings, v1alpha1.PITRArchiveBinding{ID: pitrBindingID(d), Destination: d.String(),
				ArchiveID: pitrarchive.CreateArchiveID(pbtUID, d), Source: &v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID},
				Encryption: noneEnc, State: v1alpha1.PITRArchiveBindingBound, Reason: pitrBindingBound})
		}
	})
	e.pass()
	if e.entry(a) != nil || len(e.fg().Status.PITRArchiveBindings) != pitrArchiveBindingsMax {
		t.Fatal("a 17th binding was created")
	}
	c := e.expectCond(metav1.ConditionFalse, reasonPITRBindingLimitReached)
	if !strings.Contains(c.Message, e.dest(a).String()) {
		t.Fatalf("message %q does not name the destination", c.Message)
	}
	e.expectEvents(eventReasonPITRArchiveBindingLimitReached)
}

// Δ9/K1 (row 35): an unused entry that is not Bound is dropped, but its
// candidate snapshot is kept.
func TestPITRBindingUnusedPendingDropped(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	e.pass()
	snap := e.entry(a).Encryption.KeySecretName
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.Enabled = false })
	e.pass()
	if len(e.fg().Status.PITRArchiveBindings) != 0 || e.cond() != nil {
		t.Fatalf("status = %+v", e.fg().Status)
	}
	if _, ok := e.secret(snap); !ok {
		t.Fatal("the candidate snapshot was deleted")
	}
}

// §4.10 condition precedence: limit > refused > eligibility >
// EncryptionChangeRefused > KeyChangeRefused > KeyMaterialLost > Pending >
// Bound.
func TestPITRBindingConditionPrecedence(t *testing.T) {
	fg := pbtGroup(pbtProfile("a", "p1", ""))
	p := &pitrBindingPass{fg: fg, r: &MysqlFailoverGroupReconciler{}}
	entry := func(state v1alpha1.PITRArchiveBindingState, reason v1alpha1.PITRArchiveBindingReason) v1alpha1.PITRArchiveBinding {
		return v1alpha1.PITRArchiveBinding{ID: "pab-0000000000000000", State: state, Reason: reason, Encryption: noneEnc}
	}
	cases := []struct {
		entries []v1alpha1.PITRArchiveBinding
		limit   bool
		want    string
	}{
		{entries: []v1alpha1.PITRArchiveBinding{entry(v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch)}, limit: true, want: reasonPITRBindingLimitReached},
		{entries: []v1alpha1.PITRArchiveBinding{entry(v1alpha1.PITRArchiveBindingBound, pitrBindingKeyMaterialLost), entry(v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveLegacy)}, want: string(pitrBindingArchiveLegacy)},
		{entries: []v1alpha1.PITRArchiveBinding{entry(v1alpha1.PITRArchiveBindingPending, pitrBindingEstablishing), entry(v1alpha1.PITRArchiveBindingBound, pitrBindingKeyMaterialLost)}, want: string(pitrBindingKeyMaterialLost)},
		{entries: []v1alpha1.PITRArchiveBinding{entry(v1alpha1.PITRArchiveBindingBound, pitrBindingBound), entry(v1alpha1.PITRArchiveBindingPending, pitrBindingEstablishing)}, want: string(pitrBindingEstablishing)},
		{entries: []v1alpha1.PITRArchiveBinding{entry(v1alpha1.PITRArchiveBindingPending, pitrBindingEstablishing), entry(v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)}, want: string(pitrBindingAuthenticating)},
		{entries: []v1alpha1.PITRArchiveBinding{entry(v1alpha1.PITRArchiveBindingBound, pitrBindingBound)}, want: string(pitrBindingBound)},
	}
	for i, c := range cases {
		p.entries, p.limit = c.entries, nil
		if c.limit {
			p.limit = []string{"s3://x"}
		}
		if got := p.condition(context.Background()); got.Reason != c.want {
			t.Fatalf("case %d: reason %s, want %s", i, got.Reason, c.want)
		}
	}
	// No targets and no entries: the condition is removed.
	p.entries, p.limit = nil, nil
	if got := p.condition(context.Background()); got != nil {
		t.Fatalf("condition without bindings = %+v", got)
	}
	// Refusals are bounded and carry the procedure once.
	var texts []string
	for i := 0; i < 12; i++ {
		texts = append(texts, "r")
	}
	if msg := pitrJoinRefusals(texts); strings.Count(msg, "r; ") < 8 || !strings.Contains(msg, "and 4 more") || strings.Count(msg, pitrBindingProcedure) != 1 {
		t.Fatalf("joined = %q", msg)
	}
}

// Row 46 (and 4a with a disabled hold): PITR enabled on a running group
// whose Deployments render PITR disabled: no writer runs, so the candidate
// is rendered optimistically and no check Job runs; the writer proves it.
func TestPITRBindingEnableOnRunningGroup(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	e.deploy("iad", pitrRenderDisabled().Canonical())
	e.deploy("pdx", pitrRenderDisabled().Canonical())
	e.pass()
	entry := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating)
	if len(e.jobs()) != 0 || entry.Encryption.KeySecretName != e.snapName(a, "k1") {
		t.Fatalf("entry = %+v, jobs = %d", entry, len(e.jobs()))
	}
	e.observe(e.verifiedObs(a, entry.Encryption))
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, pitrBindingBound)
	e.expectEvents(eventReasonPITRArchiveBound)
}
