package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Δ4: the snapshot name is keyed (digest key, destination, bytes) and a
// DNS-1123 label even for long group names.
func TestPITRKeySnapshotName(t *testing.T) {
	k1, k2 := []byte(strings.Repeat("a", 32)), []byte(strings.Repeat("b", 32))
	base := pitrKeySnapshotName("orders", k1, "s3://b/p1?endpoint=", []byte("k"))
	for name, other := range map[string]string{
		"digest key":  pitrKeySnapshotName("orders", k2, "s3://b/p1?endpoint=", []byte("k")),
		"destination": pitrKeySnapshotName("orders", k1, "s3://b/p2?endpoint=", []byte("k")),
		"bytes":       pitrKeySnapshotName("orders", k1, "s3://b/p1?endpoint=", []byte("k2")),
	} {
		if other == base {
			t.Fatalf("the name does not change with the %s", name)
		}
	}
	if !strings.HasPrefix(base, "mysql-orders-pitr-key-") || len(base) != len("mysql-orders-pitr-key-")+10 {
		t.Fatalf("name %q", base)
	}
	long := pitrKeySnapshotName(strings.Repeat("x", 60), k1, "d", []byte("k"))
	if len(long) > 63 {
		t.Fatalf("long name %q", long)
	}
}

// §4.3 Create: AlreadyExists adopts only this group's snapshot of the same
// destination and bytes; anything else is refused, never overwritten.
func TestEnsurePITRKeySnapshot(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a))
	fg := e.fg()
	key, err := ensureDigestKey(context.Background(), e.c, e.c, testScheme(), fg)
	if err != nil {
		t.Fatal(err)
	}
	dest := e.dest(a).String()
	s1, err := e.r.ensurePITRKeySnapshot(context.Background(), fg, key, dest, []byte("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if again, err := e.r.ensurePITRKeySnapshot(context.Background(), fg, key, dest, []byte("k1")); err != nil || again.Name != s1.Name {
		t.Fatalf("re-ensure = %v, %v", again, err)
	}
	// Another object holds the name of the k2 snapshot.
	name := pitrKeySnapshotName(pbtName, key, dest, []byte("k2"))
	squatter := newPITRKeySnapshot(fg, name, dest, []byte("k2"), "")
	squatter.Labels[v1alpha1.FailoverGroupUIDLabel] = "another-group"
	if err := e.c.Create(context.Background(), squatter); err != nil {
		t.Fatal(err)
	}
	if _, err := e.r.ensurePITRKeySnapshot(context.Background(), fg, key, dest, []byte("k2")); !errors.Is(err, errPITRSnapshotNameHeld) {
		t.Fatalf("err = %v", err)
	}
	// Listing is scoped by the group UID: the squatter is never seen.
	snaps, err := e.r.listPITRKeySnapshots(context.Background(), fg)
	if err != nil || len(snaps) != 1 || snaps[0].Name != s1.Name {
		t.Fatalf("listed %+v, %v", snaps, err)
	}
}

// K8: the acceptance is written before the status write, with a keyCheck
// computed locally that equals the descriptor's (a real NewDescriptor).
func TestAcceptPITRKeySnapshotMatchesDescriptor(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	entry := e.bound(a)
	snap, _ := e.secret(entry.Encryption.KeySecretName)
	acc, err := verifyPITRAcceptance(snap)
	if err != nil {
		t.Fatal(err)
	}
	d, err := pitrarchive.NewDescriptor(pitrarchive.Source{Namespace: pbtNS, Name: pbtName, UID: pbtUID}, e.dest(a),
		pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}, []byte("k1"), "iad", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if d.ArchiveID != acc.ArchiveID || d.Encryption.KeyCheck != acc.KeyCheck {
		t.Fatalf("acceptance %s/%s, descriptor %s/%s", acc.ArchiveID, acc.KeyCheck, d.ArchiveID, d.Encryption.KeyCheck)
	}
	if err := pitrarchive.VerifyDescriptor(d, pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: e.dest(a),
		Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}}, snap.Data["passphrase"]); err != nil {
		t.Fatalf("the snapshot bytes do not open the descriptor: %v", err)
	}
	// A second acceptance of another archive is refused, not overwritten.
	other := *entry
	other.ArchiveID = "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := e.r.acceptPITRKeySnapshot(context.Background(), e.fg(), &other, "", time.Now()); err == nil {
		t.Fatal("re-accepted a snapshot for another archive")
	}
}

// Rows 15, 34, 36 (K8 recovery): a status wipe rebuilds every accepted
// binding from its snapshot (no Job, no descriptor read), also one whose
// live Secret is gone; a malformed acceptance is never used, Warned once,
// never deleted.
func TestPITRBindingRecovery(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	c := pbtProfile("c", "p2", "pass-c")
	e := newPBTEnv(t, pbtGroup(a, c), pbtSecret("pass-a", "k1"), pbtSecret("pass-c", "k2"))
	k1 := e.bound(a)
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "c" })
	k2 := e.bound(c)
	// Row 34: K1's live Secret deleted, status wiped.
	s, _ := e.secret("pass-a")
	e.deleteObj(s)
	// Row 36: a snapshot with a malformed acceptance.
	bad := newPITRKeySnapshot(e.fg(), "mysql-orders-pitr-key-badbadbad0", e.dest(a).String(), []byte("x"), "")
	bad.Annotations[v1alpha1.PITRAcceptedAnnotation] = `{"schemaVersion":1,`
	if err := e.c.Create(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = nil })
	e.pass()
	for _, want := range []*v1alpha1.PITRArchiveBinding{k1, k2} {
		got := findBindingIn(e.fg().Status.PITRArchiveBindings, want.ID)
		if got == nil || got.State != v1alpha1.PITRArchiveBindingBound || got.Encryption != want.Encryption ||
			got.ArchiveID != want.ArchiveID || *got.Source != *want.Source {
			t.Fatalf("rebuilt %+v, want %+v", got, want)
		}
	}
	if len(e.jobs()) != 0 {
		t.Fatal("recovery ran a check Job")
	}
	cond := e.expectCond(metav1.ConditionTrue, string(v1alpha1.PITRArchiveBindingReasonBound))
	if !strings.Contains(cond.Message, "key snapshot "+bad.Name+" is unreadable") {
		t.Fatalf("message %q", cond.Message)
	}
	e.expectEvents(v1alpha1.EventPITRArchiveKeySnapshotUnreadable)
	e.pass()
	e.expectEvents()
	if _, ok := e.secret(bad.Name); !ok {
		t.Fatal("the unreadable snapshot was deleted")
	}
}

// Row 30a (O9): recovery rebuilds at most 16 bindings, in-use destinations
// first, and names the rest.
func TestPITRBindingRecoveryOverflow(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	inUse := e.bound(a)
	fg := e.fg()
	src := v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID}
	for i := 0; i < pitrArchiveBindingsMax; i++ {
		d, _ := pitrarchive.NewS3Destination("aaa-retained", fmt.Sprintf("r%02d", i), "")
		entry := &v1alpha1.PITRArchiveBinding{Destination: d.String(), ArchiveID: pitrarchive.CreateArchiveID(pbtUID, d), Source: &src,
			Encryption: passEnc(fmt.Sprintf("mysql-orders-pitr-key-r%09d", i)), State: v1alpha1.PITRArchiveBindingBound}
		acc, err := newPITRAcceptance(entry, []byte("k"), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := acc.encode()
		if err := e.c.Create(context.Background(), newPITRKeySnapshot(fg, entry.Encryption.KeySecretName, d.String(), []byte("k"), raw)); err != nil {
			t.Fatal(err)
		}
	}
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = nil })
	e.pass()
	got := e.fg().Status.PITRArchiveBindings
	if len(got) != pitrArchiveBindingsMax || findBindingIn(e.fg().Status.PITRArchiveBindings, inUse.ID) == nil {
		t.Fatalf("rebuilt %d bindings, in-use one present=%t", len(got), findBindingIn(e.fg().Status.PITRArchiveBindings, inUse.ID) != nil)
	}
	c := e.expectCond(metav1.ConditionFalse, v1alpha1.ReasonBindingLimitReached)
	if !strings.Contains(c.Message, "could not be rebuilt") {
		t.Fatalf("message %q", c.Message)
	}
}

// Rows 17-19 and 39 (Δ34, C3): a missing snapshot is re-created when the
// live bytes reproduce its name; otherwise the entry is KeyMaterialLost
// and recovers when the snapshot is back; a snapshot replaced with other
// bytes is KeyMaterialLost and never deleted; a re-created digest key keeps
// every name.
func TestPITRBindingSnapshotIntegrity(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	entry := e.bound(a)
	name := entry.Encryption.KeySecretName
	snap := func() *corev1.Secret { s, _ := e.secret(name); return s }

	// Row 17.
	e.deleteObj(snap())
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
	if _, err := verifyPITRAcceptance(snap()); err != nil {
		t.Fatalf("re-created snapshot: %v", err)
	}
	e.expectEvents(v1alpha1.EventPITRArchiveKeySnapshotRecreated)

	// Row 18: the live bytes were rotated before the snapshot vanished.
	saved := snap().DeepCopy()
	e.setSecret("pass-a", "k2")
	e.deleteObj(snap())
	e.pass()
	lost := e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost)
	if lost.Encryption.KeySecretName != name {
		t.Fatalf("the lost entry changed its snapshot: %+v", lost)
	}
	// The accepted bytes are gone, so no key change can be judged.
	e.expectCond(metav1.ConditionFalse, string(v1alpha1.PITRArchiveBindingReasonKeyMaterialLost))
	e.expectEvents(v1alpha1.EventPITRArchiveKeyMaterialLost)
	e.setSecret("pass-a", "k1")
	e.pass()
	e.expectCond(metav1.ConditionTrue, string(v1alpha1.PITRArchiveBindingReasonBound))
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
	e.events()

	// Row 19: replaced by hand with other bytes under the same name.
	e.deleteObj(snap())
	forged := saved.DeepCopy()
	forged.ResourceVersion, forged.UID = "", ""
	forged.Data = map[string][]byte{"passphrase": []byte("forged")}
	if err := e.c.Create(context.Background(), forged); err != nil {
		t.Fatal(err)
	}
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost)
	e.expectCond(metav1.ConditionFalse, string(v1alpha1.PITRArchiveBindingReasonKeyMaterialLost))
	e.expectEvents(v1alpha1.EventPITRArchiveKeyMaterialLost)
	e.pass()
	e.expectEvents()
	if s := snap(); s == nil || string(s.Data["passphrase"]) != "forged" {
		t.Fatal("the operator deleted or rewrote the forged snapshot")
	}
	// Deleting it leads to row 17.
	e.deleteObj(snap())
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
	e.events()

	// Row 39: a re-created digest key keeps every name and the condition.
	var dk corev1.Secret
	if err := e.c.Get(context.Background(), client.ObjectKey{Namespace: pbtNS, Name: digestKeySecretName(pbtName)}, &dk); err != nil {
		t.Fatal(err)
	}
	e.deleteObj(&dk)
	e.pass()
	if got := e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound); got.Encryption.KeySecretName != name {
		t.Fatalf("entry = %+v", got)
	}
	e.expectCond(metav1.ConditionTrue, string(v1alpha1.PITRArchiveBindingReasonBound))
}

// pbtReleaseEnv is a group with an unused Bound binding a (after a switch
// to c) ready to be released.
func pbtReleaseEnv(t *testing.T, funcs interceptor.Funcs) (*pbtEnv, v1alpha1.BackupProfile, *v1alpha1.PITRArchiveBinding) {
	a := pbtProfile("a", "p1", "pass-a")
	c := pbtProfile("c", "p2", "pass-c")
	e := newPBTEnvWith(t, funcs, pbtGroup(a, c), pbtSecret("pass-a", "k1"), pbtSecret("pass-c", "k2"))
	entry := e.bound(a)
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "c" })
	e.bound(c)
	return e, a, entry
}

func (e *pbtEnv) release(value string) {
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		if fg.Annotations == nil {
			fg.Annotations = map[string]string{}
		}
		fg.Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation] = value
	})
}

func (e *pbtEnv) releaseAnnotation() (string, bool) {
	v, ok := e.fg().Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation]
	return v, ok
}

// Rows 27, 29, 35: release of an unused binding deletes its snapshots,
// removes the entry and the annotation; a malformed or unconfirmed value is
// rejected and removed; an id with no entry releases its snapshots.
func TestPITRBindingRelease(t *testing.T) {
	e, a, entry := pbtReleaseEnv(t, interceptor.Funcs{})
	for _, bad := range []string{entry.ID + ":confirm=other", entry.ID, "pab-xyz:confirm=" + pbtName} {
		e.release(bad)
		e.pass()
		if _, ok := e.releaseAnnotation(); ok {
			t.Fatalf("%q: rejected annotation kept", bad)
		}
		e.expectEvents(v1alpha1.EventPITRArchiveBindingReleaseRejected)
		e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
	}
	e.release(entry.ID + ":confirm=" + pbtName)
	e.pass()
	if e.entry(a) != nil {
		t.Fatal("released entry kept")
	}
	if _, ok := e.secret(entry.Encryption.KeySecretName); ok {
		t.Fatal("released snapshot kept")
	}
	if _, ok := e.releaseAnnotation(); ok {
		t.Fatal("release annotation kept")
	}
	e.expectEvents(v1alpha1.EventPITRArchiveBindingReleased)

	// Row 35: a never-accepted candidate of a dropped entry is released by
	// its id although no entry names it.
	x := pbtProfile("x", "p7", "pass-x")
	e.setSecret("pass-x", "kx")
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, x)
		fg.Spec.Backup.PITR.ProfileName = "x"
	})
	e.pass()
	cand := e.entry(x)
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "c" })
	e.pass()
	if e.entry(x) != nil {
		t.Fatal("unused Pending entry kept")
	}
	if _, ok := e.secret(cand.Encryption.KeySecretName); !ok {
		t.Fatal("dropped entry's snapshot deleted implicitly")
	}
	e.events()
	e.release(cand.ID + ":confirm=" + pbtName)
	e.pass()
	if _, ok := e.secret(cand.Encryption.KeySecretName); ok {
		t.Fatal("release by id kept the snapshot")
	}
	e.expectEvents(v1alpha1.EventPITRArchiveBindingReleased)
}

// Row 28 (K9): release is refused while anything uses the binding: it is a
// target, a site Deployment renders its snapshot, a non-terminal pod (an
// old ReplicaSet mid-roll) mounts it, a labelled Job has not finished, or a
// live verification holds it. The annotation is kept and retried.
func TestPITRBindingReleaseRefusedInUse(t *testing.T) {
	cases := map[string]func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding){
		"a current target": func(e *pbtEnv, _ *v1alpha1.PITRArchiveBinding) {
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "a"}}
			})
		},
		"a Deployment render": func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding) {
			e.deploy("iad", e.render(pbtProfile("a", "p1", "pass-a"), entry.Encryption))
		},
		"a pod of an old ReplicaSet": func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mysql-orders-iad-old", Namespace: pbtNS},
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "pitr-passphrase", VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: entry.Encryption.KeySecretName}}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			if err := e.c.Create(context.Background(), pod); err != nil {
				e.t.Fatal(err)
			}
		},
		// U03: a scaled-to-zero Deployment with no render annotation still
		// mounts the snapshot through its pod template.
		"a scaled-to-zero Deployment's pod template": func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding) {
			d := pbcDeploy("mysql-"+pbtName+"-iad", pbtName, "iad", "")
			d.Namespace = pbtNS
			zero := int32(0)
			d.Spec.Replicas = &zero
			d.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "pitr-passphrase", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: entry.Encryption.KeySecretName}}}}
			if err := e.c.Create(context.Background(), &d); err != nil {
				e.t.Fatal(err)
			}
		},
		// U03: what a Deployment with an undecodable annotation renders
		// cannot be told.
		"a malformed render annotation": func(e *pbtEnv, _ *v1alpha1.PITRArchiveBinding) {
			e.deploy("iad", `{"schemaVersion":99}`)
		},
		// U57: a maintenance pod mounting the snapshot through a projected
		// volume, and one reading it from env.
		"a pod's projected volume": func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repair", Namespace: pbtNS},
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "keys", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
					Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: entry.Encryption.KeySecretName}}}}}}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			if err := e.c.Create(context.Background(), pod); err != nil {
				e.t.Fatal(err)
			}
		},
		"a pod's env": func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repair-env", Namespace: pbtNS},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Env: []corev1.EnvVar{{Name: "K", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: entry.Encryption.KeySecretName}, Key: "passphrase"}}}}}}},
				Status: corev1.PodStatus{Phase: corev1.PodPending}}
			if err := e.c.Create(context.Background(), pod); err != nil {
				e.t.Fatal(err)
			}
		},
		"a running labelled Job": func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding) {
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: pbtNS,
				Labels: map[string]string{v1alpha1.PITRKeySecretLabel: entry.Encryption.KeySecretName}}}
			if err := e.c.Create(context.Background(), job); err != nil {
				e.t.Fatal(err)
			}
		},
		"a live verification hold": func(e *pbtEnv, entry *v1alpha1.PITRArchiveBinding) {
			mbv := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "verify-1", Namespace: pbtNS, UID: "mbv-uid-1"},
				Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "someone-else"}, ProfileName: "a"}}
			if err := e.c.Create(context.Background(), mbv); err != nil {
				e.t.Fatal(err)
			}
			s, _ := e.secret(entry.Encryption.KeySecretName)
			s.Annotations[v1alpha1.VerificationHoldAnnotationPrefix+"mbv-uid-1"] = "verify-1"
			if err := e.c.Update(context.Background(), s); err != nil {
				e.t.Fatal(err)
			}
		},
	}
	for name, use := range cases {
		t.Run(name, func(t *testing.T) {
			e, a, entry := pbtReleaseEnv(t, interceptor.Funcs{})
			use(e, entry)
			e.events()
			e.release(entry.ID + ":confirm=" + pbtName)
			e.pass()
			if e.entry(a) == nil {
				t.Fatal("entry removed while in use")
			}
			if _, ok := e.secret(entry.Encryption.KeySecretName); !ok {
				t.Fatal("snapshot deleted while in use")
			}
			if _, ok := e.releaseAnnotation(); !ok {
				t.Fatal("annotation removed while in use")
			}
			got := e.events()
			found := false
			for _, ev := range got {
				found = found || strings.Contains(ev, v1alpha1.EventPITRArchiveBindingReleaseRefused)
			}
			if !found {
				t.Fatalf("events = %q", got)
			}
		})
	}
	// A hold whose verification is gone and stopped is not live.
	e, a, entry := pbtReleaseEnv(t, interceptor.Funcs{})
	s, _ := e.secret(entry.Encryption.KeySecretName)
	s.Annotations[v1alpha1.VerificationHoldAnnotationPrefix+"gone-uid"] = "gone"
	if err := e.c.Update(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	e.release(entry.ID + ":confirm=" + pbtName)
	e.pass()
	if e.entry(a) != nil {
		t.Fatal("a dead hold blocked the release")
	}
}

// Row 28a: a verification stamps its hold between the release's in-use
// read and the delete: the resourceVersion precondition fails and the
// snapshot is kept, and the next pass refuses. The entry already left the
// stored status (G1: it goes before any snapshot); the kept snapshot's
// acceptance rebuilds it if the release is withdrawn.
func TestPITRBindingReleaseRacesHold(t *testing.T) {
	raced := false
	funcs := interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if s, ok := obj.(*corev1.Secret); ok && s.Labels[labelAppName] == appNamePITRKeySnapshot && !raced {
			raced = true
			var cur corev1.Secret
			if err := c.Get(ctx, client.ObjectKeyFromObject(s), &cur); err != nil {
				return err
			}
			mbv := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "verify-2", Namespace: pbtNS, UID: "mbv-uid-2"},
				Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "x"}, ProfileName: "a"}}
			if err := c.Create(ctx, mbv); err != nil {
				return err
			}
			cur.Annotations[v1alpha1.VerificationHoldAnnotationPrefix+"mbv-uid-2"] = "verify-2"
			if err := c.Update(ctx, &cur); err != nil {
				return err
			}
		}
		return c.Delete(ctx, obj, opts...)
	}}
	e, _, entry := pbtReleaseEnv(t, funcs)
	e.release(entry.ID + ":confirm=" + pbtName)
	e.pass()
	if !raced {
		t.Fatal("the delete never ran")
	}
	if _, ok := e.secret(entry.Encryption.KeySecretName); !ok {
		t.Fatal("the release deleted a snapshot whose hold landed after the in-use read")
	}
	if _, ok := e.fg().Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation]; !ok {
		t.Fatal("the release annotation was removed although the release did not finish")
	}
	e.events()
	e.pass()
	got := e.events()
	if len(got) != 1 || !strings.Contains(got[0], v1alpha1.EventPITRArchiveBindingReleaseRefused) {
		t.Fatalf("events after the race = %q", got)
	}
}

// Row 28b: the snapshot of a binding under release is gone while its entry
// and the annotation remain (deleted by hand, or by a release of an older
// operator that deleted before writing status); the next pass finishes the
// release and never reports the missing snapshot as lost key material.
func TestPITRBindingReleaseCrashFinishes(t *testing.T) {
	e, a, entry := pbtReleaseEnv(t, interceptor.Funcs{})
	e.release(entry.ID + ":confirm=" + pbtName)
	s, _ := e.secret(entry.Encryption.KeySecretName)
	e.deleteObj(s)
	e.pass()
	if e.entry(a) != nil {
		t.Fatal("entry kept")
	}
	got := e.expectEvents(v1alpha1.EventPITRArchiveBindingReleased)
	for _, ev := range got {
		if strings.Contains(ev, v1alpha1.EventPITRArchiveKeyMaterialLost) {
			t.Fatalf("lost key material reported during a release: %q", got)
		}
	}
	if _, ok := e.secret(entry.Encryption.KeySecretName); ok {
		t.Fatal("C3 re-created a snapshot under release")
	}
}

// Row 48 (K1, Δ31): snapshots have no owner, so deleting the group (the
// API server's garbage collector only follows owner references) leaves
// them; a later group with the same name never sees them (UID label).
func TestPITRKeySnapshotsOutliveGroup(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	entry := e.bound(a)
	s, _ := e.secret(entry.Encryption.KeySecretName)
	if len(s.OwnerReferences) != 0 {
		t.Fatalf("snapshot has owners %+v", s.OwnerReferences)
	}
	successor := pbtGroup(a)
	successor.UID = "6f1c2d3e-0000-4000-8000-0000000002b4"
	snaps, err := e.r.listPITRKeySnapshots(context.Background(), successor)
	if err != nil || len(snaps) != 0 {
		t.Fatalf("a successor sees %d snapshots (%v)", len(snaps), err)
	}
}
