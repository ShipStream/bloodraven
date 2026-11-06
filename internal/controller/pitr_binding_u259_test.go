package controller

// Regression tests for the PR #259 review findings (U##) the binding
// engine, the renderer and their consumers own: each reproduces the
// reported interleaving or input.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// ownVerified is a Verified verdict for p's destination created by the
// pbt group.
func (e *pbtEnv) ownVerified(p v1alpha1.BackupProfile) PITRArchiveCheckResult {
	return PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified, ArchiveID: pitrarchive.CreateArchiveID(pbtUID, e.dest(p)),
		Source: v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID}}
}

// readTargetEnv is a group whose PITR is off and whose initFromBackup reads
// profile src (prefix p9, encrypted with Secret pass-src = value, or
// plaintext when value is "").
func readTargetEnv(t *testing.T, funcs interceptor.Funcs, value string) (*pbtEnv, v1alpha1.BackupProfile) {
	t.Helper()
	secret := ""
	var objs []client.Object
	if value != "" {
		secret = "pass-src"
		objs = append(objs, pbtSecret("pass-src", value))
	}
	src := pbtProfile("src", "p9", secret)
	fg := pbtGroup(pbtProfile("a", "p1", ""), src)
	fg.Spec.Backup.PITR.Enabled = false
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	return newPBTEnvWith(t, funcs, fg, objs...), src
}

func eventHasReason(events []string, reason string) bool {
	for _, ev := range events {
		if strings.Contains(ev, " "+reason+" ") {
			return true
		}
	}
	return false
}

// U01: a site Deployment re-created (or added) while the others are
// deferred carries a decodable annotation; the others still carry none.
// The render stays deferred for every existing Deployment, so the
// unauthenticated candidate never reaches the pre-#203 writers.
func TestPITRRenderDefersWhileAnySiteIsUnannotated(t *testing.T) {
	fg := renderFG()
	setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonKeyMismatch, "mysql-lion-pitr-key-aaaaaaaaaa")
	candidate, action := resolvePITRRender(fg, []appsv1.Deployment{siteDeploy(fg, "dc1", nil), siteDeploy(fg, "dc2", nil)})
	if action != pitrRenderDefer {
		t.Fatalf("fixture: all unannotated resolves %s", action)
	}
	// dc2 was re-created with the candidate render (row 4a for a new site).
	deploys := []appsv1.Deployment{siteDeploy(fg, "dc1", nil), siteDeploy(fg, "dc2", candidate)}
	in, action := resolvePITRRender(fg, deploys)
	if action != pitrRenderDefer {
		t.Fatalf("a decodable annotation on one site lifted the deferral of the unannotated one (%s)", action)
	}
	if in.Canonical() != candidate.Canonical() {
		t.Fatalf("a site created now renders %s, want the held candidate %s", in.Canonical(), candidate.Canonical())
	}
	render := groupPITRRender{inputs: in, action: action, existing: map[string]bool{deploys[0].Name: true, deploys[1].Name: true}}
	if !render.defers(deploys[0].Name) || render.defers(resourceName(fg.Name, "dc3")) {
		t.Fatal("defers must hold existing Deployments and let a new site render")
	}
	// The backup of the active profile does not wait for a roll that the
	// deferral never starts.
	if pitrBackupWaitsForRollout(fg, "a", deploys) {
		t.Fatal("a backup waits on a deferred render")
	}
	// Once the binding is authenticated (Bound), the roll proceeds.
	setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound, "mysql-lion-pitr-key-aaaaaaaaaa")
	if _, action := resolvePITRRender(fg, deploys); action != pitrRenderApply {
		t.Fatalf("Bound still defers (%s)", action)
	}
}

// U01 (binding side): a pre-#203 writer next to an annotated site whose
// render is disabled is still a running writer of an unknown key: the
// write target is authenticated by a check (C2), not optimistically, and
// a refusal names only the unannotated Deployment.
func TestPITRBindingMixedUnannotatedRunsCheck(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	e.deploy("iad", "")
	e.deploy("pdx", pitrRenderDisabled().Canonical())
	e.pass()
	entry := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	if entry.Check == nil {
		t.Fatal("no check for a write target while a pre-#203 writer runs")
	}
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveKeyMismatch})
	e.pass()
	got := e.expectEntry(a, v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonKeyMismatch)
	if !strings.Contains(got.Message, "Deployments mysql-orders-iad are not updated") || strings.Contains(got.Message, "mysql-orders-pdx are") {
		t.Fatalf("deferral note = %q", got.Message)
	}
}

// U02: a listing refused for permission is not a sticky refusal: the entry
// stays Pending (readers wait), and the check is retried after the retry
// gate without any input change, so granting ListBucket recovers.
func TestPITRBindingListFailedIsRetried(t *testing.T) {
	e, src := readTargetEnv(t, interceptor.Funcs{}, "k")
	e.pass()
	e.verdict(e.entry(src), PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveListFailed})
	e.pass()
	got := e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	if !strings.Contains(got.Message, "s3:ListBucket") || !strings.Contains(got.Message, "retrying") {
		t.Fatalf("message %q", got.Message)
	}
	if r := pitrBindingReadiness(got); r == nil || r.terminal {
		t.Fatalf("readiness = %+v, want a wait", r)
	}
	pitrCheckRetryGate.Store(pitrCheckRetryKey(e.fg(), got.ID), time.Now().Add(-time.Second))
	e.pass()
	entry := e.entry(src)
	if entry.Check == nil {
		t.Fatal("ListFailed was not re-checked after the retry gate")
	}
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified, ArchiveID: pitrarchive.CreateArchiveID("00000000-0000-4000-8000-0000000000ff", e.dest(src)),
		Source: v1alpha1.PITRArchiveSource{Namespace: "other", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ff"}})
	e.pass()
	e.expectEntry(src, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)

	// A ListFailed refusal stored before the fix is retried too.
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.PITRArchiveBindings[0].State, st.PITRArchiveBindings[0].Reason = v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveListFailed
		st.PITRArchiveBindings[0].ArchiveID, st.PITRArchiveBindings[0].Source = "", nil
	})
	if s, ok := e.secret(e.entry(src).Encryption.KeySecretName); ok {
		delete(s.Annotations, v1alpha1.PITRAcceptedAnnotation)
		if err := e.c.Update(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	e.pass()
	if e.entry(src).State != v1alpha1.PITRArchiveBindingPending {
		t.Fatalf("stored ListFailed not retried: %+v", e.entry(src))
	}
}

// U04: the destination of an Establishing entry stops being written while
// a verification still reads it: it is authenticated as a read (a Reader
// check), not left waiting for a writer that no longer exists.
func TestPITRBindingEstablishingBecomesRead(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	b := pbtProfile("b", "p2", "")
	e := newPBTEnv(t, pbtGroup(a, b))
	old := e.bound(a)
	e.deploy("iad", e.render(a, old.Encryption))
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "b" })
	mbv := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "v", Namespace: pbtNS},
		Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: pbtName}, ProfileName: "b",
			PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: "latest", ArchiveProfileName: "b"}}}
	if err := e.c.Create(context.Background(), mbv); err != nil {
		t.Fatal(err)
	}
	e.pass()
	e.verdict(e.entry(b), PITRArchiveCheckResult{State: PITRArchiveCheckStateAbsent})
	e.pass()
	e.expectEntry(b, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonEstablishing)
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "a" })
	e.pass()
	got := e.expectEntry(b, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	job := e.checkJob(got)
	if envMap(job.Spec.Template.Spec.Containers[0].Env)[PITRArchiveCheckRoleEnv] != PITRArchiveCheckRoleReader {
		t.Fatal("the read is not authenticated by a Reader check")
	}
	e.verdict(got, PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveNotFound})
	e.pass()
	e.expectEntry(b, v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveNotFound)
}

// U04: a check started for one role is dropped when the target's role
// flips, and a refusal by the other role's rules is checked again under
// the current role (once).
func TestPITRBindingRoleFlip(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	c := pbtProfile("c", "p2", "")
	e := newPBTEnv(t, pbtGroup(a, c))
	old := e.bound(a)
	e.deploy("iad", e.render(a, old.Encryption))
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup.PITR.ProfileName = "c"
		fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "c"}}
	})
	e.pass()
	writer := e.checkJob(e.entry(c))
	if envMap(writer.Spec.Template.Spec.Containers[0].Env)[PITRArchiveCheckRoleEnv] != PITRArchiveCheckRoleWriter {
		t.Fatal("fixture: not a Writer check")
	}
	// The write moves back to a; c is only read now. The Writer check in
	// flight answers the wrong question and is dropped.
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "a" })
	e.pass()
	got := e.expectEntry(c, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	if got.Check != nil || !strings.Contains(got.Message, "checked as Reader") {
		t.Fatalf("entry = %+v", got)
	}
	// A refusal a writer check recorded (ArchiveForeign) is re-checked as a
	// read, once.
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		for i := range st.PITRArchiveBindings {
			if st.PITRArchiveBindings[i].ID == got.ID {
				st.PITRArchiveBindings[i].State, st.PITRArchiveBindings[i].Reason = v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveForeign
				st.PITRArchiveBindings[i].Message = "ArchiveForeign: PITRArchiveForeign reported by the writer check of the plaintext identity"
				st.PITRArchiveBindings[i].Check = nil
			}
		}
	})
	for _, j := range e.jobs() {
		e.deleteObj(&j)
	}
	e.pass()
	got = e.entry(c)
	if got.State != v1alpha1.PITRArchiveBindingPending || got.Check == nil {
		t.Fatalf("a writer-rules refusal of a read target was not re-checked: %+v", got)
	}
	e.verdict(got, PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveDescriptorMissing})
	e.pass()
	refused := e.expectEntry(c, v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveDescriptorMissing)
	if !strings.Contains(refused.Message, pitrCheckRoleMarkerReader) {
		t.Fatalf("message %q", refused.Message)
	}
	e.pass()
	if len(e.jobs()) != 0 {
		t.Fatal("a reader refusal of a read target was re-checked without new input")
	}
}

// U07: lost key material keeps a remedy that never destroys the only copy
// of an accepted key: a snapshot that exists but fails the check is never
// to be deleted before the original key is safe.
func TestPITRKeyMaterialLostMessages(t *testing.T) {
	e := &v1alpha1.PITRArchiveBinding{ArchiveID: "a", Destination: "s3://x", Encryption: passEnc("snap")}
	invalid := pitrKeyMaterialLostMessage(e, pitrIntegrityInvalid, "Secret snap does not hold the accepted key")
	if !strings.Contains(invalid, "do not delete it") || !strings.Contains(invalid, "only copy") {
		t.Fatalf("invalid: %q", invalid)
	}
	missing := pitrKeyMaterialLostMessage(e, pitrIntegrityNotFound, "key snapshot Secret snap is missing")
	if strings.Contains(missing, "delete it so") || !strings.Contains(missing, "restore it from a backup") {
		t.Fatalf("missing: %q", missing)
	}
}

// U08: a removed site's Deployment is never re-rendered; scaled to zero
// with no pod it no longer blocks the stamp, and while it may still run
// the reason names the way out.
func TestRenderedWriterArchiveIDRemovedSite(t *testing.T) {
	fg := renderFG()
	fg.Spec.Sites = []v1alpha1.SiteSpec{{Name: "dc1"}, {Name: "dc2"}}
	bindAllTestPITRArchives(fg)
	cur := testPITRRender(fg)
	old := cur
	old.ArchiveID = pitrarchive.CreateArchiveID(fg.UID, destOf(t, fg, "c"))
	removed := siteDeploy(fg, "dc3", old)
	deploys := []appsv1.Deployment{siteDeploy(fg, "dc1", cur), siteDeploy(fg, "dc2", cur), removed}
	if _, ok, why := renderedWriterArchiveID(fg, deploys); ok || !strings.Contains(why, "no longer in spec.sites") {
		t.Fatalf("running removed site: ok=%t why=%q", ok, why)
	}
	zero := int32(0)
	deploys[2].Spec.Replicas = &zero
	if id, ok, why := renderedWriterArchiveID(fg, deploys); !ok || id != cur.ArchiveID {
		t.Fatalf("scaled-down removed site still disagrees: %q %t %q", id, ok, why)
	}
	deploys[2].Status.Replicas = 1
	if _, ok, _ := renderedWriterArchiveID(fg, deploys); ok {
		t.Fatal("a removed site whose pod still runs was ignored")
	}
}

// U08: a managed site whose render annotation was lost or edited while its
// spec hash still matches is drift, so the updater repairs it.
func TestSpecDriftRepairsRenderAnnotation(t *testing.T) {
	fg := renderFG()
	bindAllTestPITRArchives(fg)
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}); err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	runner := &TopologyManagerRunner{client: c, apiReader: c, logger: slog.New(slog.NewTextHandler(&buf, nil))}
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(buf.String())
		}
	})
	fresh := &v1alpha1.MysqlFailoverGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(fg), fresh); err != nil {
		t.Fatal(err)
	}
	if sites, ok := runner.specDriftSites(context.Background(), c, fresh); !ok || len(sites) != 0 {
		t.Fatalf("fixture: drift %v %t", sites, ok)
	}
	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, "dc1")}, &d); err != nil {
		t.Fatal(err)
	}
	in, err := parsePITRRender(d.Annotations[v1alpha1.PITRRenderAnnotation])
	if err != nil {
		t.Fatal(err)
	}
	in.PollInterval = "7m"
	d.Annotations[v1alpha1.PITRRenderAnnotation] = in.Canonical()
	if err := c.Update(context.Background(), &d); err != nil {
		t.Fatal(err)
	}
	if sites, ok := runner.specDriftSites(context.Background(), c, fresh); !ok || len(sites) != 1 {
		t.Fatalf("an edited render annotation with a matching hash is not drift: %v", sites)
	}
}

// U09: while the render defers an existing Deployment, its site ConfigMap
// is not rewritten from the candidate; a site being created gets one.
func TestReconcileSiteConfigMapsDeferKeepsExisting(t *testing.T) {
	fg := renderFG()
	fg.Spec.Sites = []v1alpha1.SiteSpec{{Name: "dc1"}, {Name: "dc2"}}
	existing := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: siteConfigMapName(fg.Name, "dc1"), Namespace: fg.Namespace},
		Data: map[string]string{"bloodraven.cnf": "pre-upgrade"}}
	r, c := newReconciler(fg, existing)
	in, _ := resolvePITRRender(fg, nil)
	render := groupPITRRender{inputs: in, action: pitrRenderDefer, existing: map[string]bool{resourceName(fg.Name, "dc1"): true}}
	if err := r.reconcileSiteConfigMaps(context.Background(), fg, render); err != nil {
		t.Fatal(err)
	}
	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(existing), &cm); err != nil || cm.Data["bloodraven.cnf"] != "pre-upgrade" {
		t.Fatalf("deferred site's ConfigMap rewritten: %v %v", cm.Data, err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: siteConfigMapName(fg.Name, "dc2")}, &cm); err != nil {
		t.Fatalf("a site being created got no ConfigMap: %v", err)
	}
}

// U10: an unused non-Bound entry, dropped by this pass, never holds the
// slot a new target needs.
func TestPITRBindingLimitIgnoresDiscardableEntries(t *testing.T) {
	a := pbtProfile("a", "p1", "")
	e := newPBTEnv(t, pbtGroup(a))
	var fill []v1alpha1.PITRArchiveBinding
	src := v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID}
	for i := 0; i < pitrArchiveBindingsMax-1; i++ {
		d, err := pitrarchive.NewS3Destination("archive-bucket", fmt.Sprintf("old-%02d", i), "https://s3.example.test")
		if err != nil {
			t.Fatal(err)
		}
		s := src
		fill = append(fill, v1alpha1.PITRArchiveBinding{ID: pitrBindingID(d), Destination: d.String(), ArchiveID: pitrarchive.CreateArchiveID(pbtUID, d),
			Source: &s, Encryption: noneEnc, State: v1alpha1.PITRArchiveBindingBound, Reason: v1alpha1.PITRArchiveBindingReasonBound, LastTransitionTime: &metav1.Time{Time: time.Now().Truncate(time.Second)}})
	}
	d, _ := pitrarchive.NewS3Destination("archive-bucket", "unused", "https://s3.example.test")
	fill = append(fill, v1alpha1.PITRArchiveBinding{ID: pitrBindingID(d), Destination: d.String(), Encryption: noneEnc,
		State: v1alpha1.PITRArchiveBindingPending, Reason: v1alpha1.PITRArchiveBindingReasonAuthenticating, LastTransitionTime: &metav1.Time{Time: time.Now().Truncate(time.Second)}})
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = fill })
	e.pass()
	if e.entry(a) == nil {
		t.Fatalf("the active destination got no entry: %+v", e.cond())
	}
	if c := e.cond(); c != nil && c.Reason == v1alpha1.ReasonBindingLimitReached {
		t.Fatalf("condition = %+v", c)
	}
	if n := len(e.fg().Status.PITRArchiveBindings); n != pitrArchiveBindingsMax {
		t.Fatalf("%d entries", n)
	}
}

// U12: a pass computed from a stale cache neither records nor deletes
// anything: the check Job the stored status records survives.
func TestPITRBindingStalePassKeepsRecordedCheckJob(t *testing.T) {
	e, src := readTargetEnv(t, interceptor.Funcs{}, "")
	stale := e.fg()
	e.pass()
	job := e.checkJob(e.entry(src))
	if _, err := e.r.reconcilePITRArchiveBindings(context.Background(), stale); !errors.Is(err, errPITRBindingStatusMoved) {
		t.Fatalf("stale pass = %v", err)
	}
	var got batchv1.Job
	if err := e.c.Get(context.Background(), client.ObjectKeyFromObject(job), &got); err != nil || got.UID != job.UID {
		t.Fatalf("the recorded check Job was deleted by a stale pass: %v", err)
	}
	e.verdict(e.entry(src), PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveNotFound})
	e.pass()
	e.expectEntry(src, v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveNotFound)
}

// U14: at the binding limit a new active destination gets no entry and is
// never rendered: the group keeps its held render, or archives nothing.
func TestPITRRenderHoldsAtBindingLimit(t *testing.T) {
	fg := renderFG()
	for i := 0; i < pitrArchiveBindingsMax; i++ {
		d, err := pitrarchive.NewS3Destination("archive", fmt.Sprintf("old-%02d", i), "https://s3.example.com")
		if err != nil {
			t.Fatal(err)
		}
		fg.Status.PITRArchiveBindings = append(fg.Status.PITRArchiveBindings, v1alpha1.PITRArchiveBinding{ID: pitrBindingID(d), Destination: d.String(),
			State: v1alpha1.PITRArchiveBindingBound, Reason: v1alpha1.PITRArchiveBindingReasonBound})
	}
	fg.Spec.Backup.Profiles[0].Encryption = nil
	if in, action := resolvePITRRender(fg, nil); in.Enabled || action != pitrRenderApply {
		t.Fatalf("no hold: rendered %s/%s, want disabled", in.Canonical(), action)
	}
	d, _ := pitrarchive.ParseDestination(fg.Status.PITRArchiveBindings[0].Destination)
	held := pitrProfileRender(fg, findProfile(fg, "c"), d).withEncryption(noneEnc)
	held.ArchiveID = pitrarchive.CreateArchiveID(fg.UID, d)
	in, _ := resolvePITRRender(fg, []appsv1.Deployment{siteDeploy(fg, "dc1", held)})
	if in.Destination != held.Destination {
		t.Fatalf("held render %s, want %s", in.Destination, held.Destination)
	}
}

// U20: a release removed the entry, but the cached listing still shows its
// snapshot: recovery confirms uncached and does not resurrect it.
func TestPITRBindingRecoveryConfirmsUncached(t *testing.T) {
	e, a, entry := pbtReleaseEnv(t, interceptor.Funcs{})
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		kept := st.PITRArchiveBindings[:0]
		for _, x := range st.PITRArchiveBindings {
			if x.ID != entry.ID {
				kept = append(kept, x)
			}
		}
		st.PITRArchiveBindings = kept
	})
	e.r.APIReader = goneReader{Reader: e.c, name: entry.Encryption.KeySecretName}
	e.pass()
	if got := e.entry(a); got != nil {
		t.Fatalf("a binding whose snapshot is gone was rebuilt from the cache: %+v", got)
	}
	e.r.APIReader = e.c
	e.pass()
	if e.entry(a) == nil {
		t.Fatal("a confirmed accepted snapshot was not rebuilt")
	}
}

// goneReader reports the Secret name as NotFound (an uncached reader that
// saw a deletion the cache has not).
type goneReader struct {
	client.Reader
	name string
}

func (g goneReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok && key.Name == g.name {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, key.Name)
	}
	return g.Reader.Get(ctx, key, obj, opts...)
}

// U21: a restore that failed before its Job was created keeps its read
// target (it is rebuilt on every reconcile once the cause is fixed); one
// that failed with a Job, or succeeded, drops it.
func TestPITRBindingTargetsPreLaunchRestoreFailure(t *testing.T) {
	src := pbtProfile("src", "p9", "")
	fg := pbtGroup(pbtProfile("a", "p1", ""), src)
	fg.Spec.Backup.PITR.Enabled = false
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "src"}}
	for _, tc := range []struct {
		status *v1alpha1.RestoreStatus
		want   int
	}{
		{nil, 1},
		{&v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed}, 1},
		{&v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: "restore"}, 0},
		{&v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseSucceeded}, 0},
	} {
		fg.Status.Restore = tc.status
		if got := pitrBindingTargets(fg, nil); len(got) != tc.want {
			t.Fatalf("restore %+v: %d targets, want %d", tc.status, len(got), tc.want)
		}
	}
	// Multi-pass: the refused entry stays Refused (no new check Job, no
	// flapping) while the restore records its pre-launch failure.
	fg.Status.Restore = nil
	e := newPBTEnv(t, fg)
	e.pass()
	e.verdict(e.entry(src), PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveNotFound})
	e.pass()
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, Reason: string(v1alpha1.PITRTokenArchiveBindingNotReady)}
	})
	for i := 0; i < 3; i++ {
		e.pass()
		e.expectEntry(src, v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveNotFound)
		if len(e.jobs()) != 0 {
			t.Fatal("a pre-launch refusal launched another check Job")
		}
	}
}

// U22: the snapshot a check verified is replaced (deleted and re-created
// under its name with other bytes) before the verdict is applied: the
// verdict is dropped and the new object checked, never accepted.
func TestPITRBindingAcceptanceRefusesReplacedSnapshot(t *testing.T) {
	e, src := readTargetEnv(t, interceptor.Funcs{}, "k1")
	e.pass()
	entry := e.entry(src)
	job := e.checkJob(entry)
	if job.Annotations[v1alpha1.PITRKeySecretUIDAnnotation] == "" {
		t.Fatal("the check Job does not record the snapshot UID it mounts")
	}
	snap, _ := e.secret(entry.Encryption.KeySecretName)
	e.deleteObj(snap)
	forged := newPITRKeySnapshot(e.fg(), snap.Name, entry.Destination, []byte("k2"), "")
	if err := e.c.Create(context.Background(), forged); err != nil {
		t.Fatal(err)
	}
	foreign := v1alpha1.PITRArchiveSource{Namespace: "other", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ff"}
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified, ArchiveID: pitrarchive.CreateArchiveID(foreign.UID, e.dest(src)), Source: foreign})
	e.events()
	e.pass()
	got := e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	if !strings.Contains(got.Message, "replaced") {
		t.Fatalf("message %q", got.Message)
	}
	if s, _ := e.secret(snap.Name); s.Annotations[v1alpha1.PITRAcceptedAnnotation] != "" {
		t.Fatal("the replaced snapshot was accepted")
	}
	if eventHasReason(e.events(), v1alpha1.EventPITRArchiveBound) {
		t.Fatal("PITRArchiveBound emitted for a replaced snapshot")
	}
}

// U23: an evaluation input that cannot be read never certifies the
// bindings: the pass fails and the stored condition stays.
func TestPITRBindingEvaluationErrorKeepsCondition(t *testing.T) {
	armed := false
	funcs := interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*corev1.Secret); ok && armed && key.Name == "pass-a" {
			return errors.New("etcd timeout")
		}
		return c.Get(ctx, key, obj, opts...)
	}}
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnvWith(t, funcs, pbtGroup(a), pbtSecret("pass-a", "k1"))
	e.bound(a)
	e.setSecret("pass-a", "k2")
	e.pass()
	before := e.expectCond(metav1.ConditionFalse, v1alpha1.ReasonKeyChangeRefused)
	armed = true
	if _, err := e.r.reconcilePITRArchiveBindings(context.Background(), e.fg()); err == nil {
		t.Fatal("a pass whose evaluation could not read its inputs succeeded")
	}
	after := e.cond()
	if after == nil || after.Reason != before.Reason || after.Message != before.Message || after.Status != before.Status {
		t.Fatalf("condition replaced: %+v", after)
	}
}

// U27: the per-group memory of the render log and the schedule refusals
// does not outlive the group or a removed schedule, and empty render-log
// states are never stored.
func TestPITRGroupMemosPruned(t *testing.T) {
	r := &MysqlFailoverGroupReconciler{}
	fg := renderFG()
	fg.Namespace = "db"
	r.logPITRRender(fg, nil, pitrRenderApply)
	if len(r.pitrRenderLog) != 0 {
		t.Fatalf("empty render-log state stored: %v", r.pitrRenderLog)
	}
	r.logPITRRender(fg, []appsv1.Deployment{siteDeploy(fg, "dc1", nil)}, pitrRenderDefer)
	if len(r.pitrRenderLog) != 1 {
		t.Fatal("fixture: the deferral was not remembered")
	}
	r.noteScheduleRefused(fg, "nightly", "read-only")
	r.noteScheduleRefused(fg, "hourly", "read-only")
	r.forgetScheduleRefusals(client.ObjectKeyFromObject(fg), fg.UID, map[string]bool{"nightly": true})
	if len(r.scheduleRefused) != 1 {
		t.Fatalf("a removed schedule's refusal kept: %v", r.scheduleRefused)
	}
	r.forgetGroupMemos(client.ObjectKeyFromObject(fg))
	if len(r.pitrRenderLog) != 0 || len(r.scheduleRefused) != 0 {
		t.Fatalf("memos of a deleted group kept: %v %v", r.pitrRenderLog, r.scheduleRefused)
	}
}

// U28, U44: the verdict comes only from a pod the recorded Job controls,
// read uncached; a leftover pod carrying the Job's name label (another Job
// of the same name) is never read.
func TestPITRCheckVerdictPodSelection(t *testing.T) {
	e, src := readTargetEnv(t, interceptor.Funcs{}, "")
	e.pass()
	entry := e.entry(src)
	job := e.checkJob(entry)
	stale := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-stale", Namespace: pbtNS,
		Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{
			Name: pitrCheckContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0,
				Message: FormatPITRArchiveCheckLine(PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveKeyMismatch,
					DestinationDigest: PITRArchiveCheckDestinationDigest(entry.Destination)})}}}}}}
	if err := e.c.Create(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	out, err := e.r.observePITRArchiveCheck(context.Background(), e.fg(), entry, time.Now())
	if err != nil || out.done {
		t.Fatalf("a label-only pod's verdict was read: %+v %v", out, err)
	}
	// The cached client lags (no pod at all); the uncached reader has the
	// finished pod: the verdict is read.
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateAbsent})
	lagging := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(e.fg()).Build()
	r := &MysqlFailoverGroupReconciler{Client: lagging, Scheme: testScheme(), APIReader: e.c}
	out, err = r.observePITRArchiveCheck(context.Background(), e.fg(), entry, time.Now())
	if err != nil || !out.done || out.result == nil {
		t.Fatalf("verdict not read through the uncached reader: %+v %v", out, err)
	}
}

// U29: finishing one release never erases a newer request written after
// the pass read the group.
func TestPITRReleaseAnnotationRemovalIsConditional(t *testing.T) {
	e, _, entry := pbtReleaseEnv(t, interceptor.Funcs{})
	processed := entry.ID + ":confirm=" + pbtName
	e.release(processed)
	stale := e.fg()
	e.release("pab-0123456789abcdef:confirm=" + pbtName)
	if err := e.r.removePITRReleaseAnnotation(context.Background(), stale, processed); err != nil {
		t.Fatal(err)
	}
	if v, ok := e.releaseAnnotation(); !ok || v != "pab-0123456789abcdef:confirm="+pbtName {
		t.Fatalf("a newer request was erased: %q %t", v, ok)
	}
	if err := e.r.removePITRReleaseAnnotation(context.Background(), e.fg(), "pab-0123456789abcdef:confirm="+pbtName); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.releaseAnnotation(); ok {
		t.Fatal("the processed request was not removed")
	}
}

// U31, U39: the acceptance write fails once. Nothing is recorded Bound and
// no PITRArchiveBound is emitted; a restored entry keeps its completed
// check Job, so the next pass applies the same verdict (no new Job, no
// retry gate); a new entry bound by proof stays Pending.
func TestPITRBindingAcceptanceFailure(t *testing.T) {
	fail := 0
	funcs := interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		if s, ok := obj.(*corev1.Secret); ok && s.Labels[labelAppName] == appNamePITRKeySnapshot && fail > 0 {
			fail--
			return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, s.Name, errors.New("modified"))
		}
		return c.Patch(ctx, obj, patch, opts...)
	}}
	t.Run("restored entry keeps its verdict", func(t *testing.T) {
		e, src := readTargetEnv(t, funcs, "k1")
		e.pass()
		entry := e.entry(src)
		job := e.checkJob(entry)
		foreign := v1alpha1.PITRArchiveSource{Namespace: "other", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ff"}
		e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified, ArchiveID: pitrarchive.CreateArchiveID(foreign.UID, e.dest(src)), Source: foreign})
		e.events()
		fail = 1
		e.pass()
		got := e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
		if got.Check == nil || got.Check.JobUID != job.UID {
			t.Fatalf("the restored entry lost its check: %+v", got)
		}
		var kept batchv1.Job
		if err := e.c.Get(context.Background(), client.ObjectKeyFromObject(job), &kept); err != nil {
			t.Fatalf("the completed check Job was deleted under a restored Check: %v", err)
		}
		if eventHasReason(e.events(), v1alpha1.EventPITRArchiveBound) {
			t.Fatal("PITRArchiveBound before the acceptance")
		}
		if w := pitrCheckRetryWait(e.fg(), got.ID, time.Now()); w != 0 {
			t.Fatalf("retry gate armed (%s)", w)
		}
		e.pass()
		e.expectEntry(src, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
		if !eventHasReason(e.events(), v1alpha1.EventPITRArchiveBound) {
			t.Fatal("no PITRArchiveBound once accepted")
		}
	})
	t.Run("new entry bound by proof stays Pending", func(t *testing.T) {
		a := pbtProfile("a", "p1", "pass-a")
		e := newPBTEnvWith(t, funcs, pbtGroup(a), pbtSecret("pass-a", "k1"))
		e.pass()
		entry := e.entry(a)
		e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = nil })
		e.observe(e.verifiedObs(a, entry.Encryption))
		e.events()
		fail = 1
		e.pass()
		got := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
		if got.ArchiveID != "" || got.Source != nil || !strings.Contains(got.Message, "accepting the key snapshot failed") {
			t.Fatalf("entry = %+v", got)
		}
		if eventHasReason(e.events(), v1alpha1.EventPITRArchiveBound) {
			t.Fatal("PITRArchiveBound before the acceptance")
		}
		e.pass()
		e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
	})
}

// U33: an object of the snapshot's name appears between the integrity read
// and C3's create: nothing is reported re-created and KeyMaterialLost is
// not cleared until the object is checked.
func TestPITRBindingC3RaceIsNotARecreation(t *testing.T) {
	race := false
	funcs := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if s, ok := obj.(*corev1.Secret); ok && race && s.Labels[labelAppName] == appNamePITRKeySnapshot {
			return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, s.Name)
		}
		return c.Create(ctx, obj, opts...)
	}}
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnvWith(t, funcs, pbtGroup(a), pbtSecret("pass-a", "k1"))
	entry := e.bound(a)
	snap, _ := e.secret(entry.Encryption.KeySecretName)
	e.setSecret("pass-a", "other")
	e.deleteObj(snap)
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost)
	e.events()
	e.setSecret("pass-a", "k1")
	race = true
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost)
	if got := e.events(); eventHasReason(got, v1alpha1.EventPITRArchiveKeySnapshotRecreated) {
		t.Fatalf("a recreation reported for an object the operator did not create: %q", got)
	}
}

// U43: a present but unreadable verdict line is reported with the parser's
// reason, not as a missing line.
func TestPITRCheckUnparseableVerdict(t *testing.T) {
	e, src := readTargetEnv(t, interceptor.Funcs{}, "")
	e.pass()
	e.finishCheck(e.checkJob(e.entry(src)), 0, PITRArchiveCheckLinePrefix+" state=Verified")
	e.pass()
	got := e.expectEntry(src, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	if !strings.Contains(got.Message, "does not parse") || strings.Contains(got.Message, "without a verdict line") {
		t.Fatalf("message %q", got.Message)
	}
}

// U49: a round that advanced past the first candidate meets the next
// candidate's leftover Job: it waits for it to go and resumes with that
// candidate, never restarting at the first.
func TestPITRBindingRoundResumesAfterReplacement(t *testing.T) {
	e, src := readTargetEnv(t, interceptor.Funcs{}, "live")
	e.pass()
	dest := e.dest(src).String()
	key, _ := readDigestKey(context.Background(), e.c, e.fg())
	listed := newPITRKeySnapshot(e.fg(), pitrKeySnapshotName(pbtName, key, dest, []byte("old")), dest, []byte("old"), "")
	if err := e.c.Create(context.Background(), listed); err != nil {
		t.Fatal(err)
	}
	for _, j := range e.jobs() {
		e.deleteObj(&j)
	}
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.PITRArchiveBindings = nil })
	e.pass()
	first := e.entry(src)
	if first.Encryption.KeySecretName != listed.Name {
		t.Fatalf("round starts with %s, want the listed %s", first.Encryption.KeySecretName, listed.Name)
	}
	// A leftover Job of the live candidate's deterministic name.
	liveName := e.snapName(src, "live")
	leftover := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: pitrCheckJobName(pbtName, first.ID, liveName), Namespace: pbtNS}}
	if err := e.c.Create(context.Background(), leftover); err != nil {
		t.Fatal(err)
	}
	e.verdict(first, PITRArchiveCheckResult{State: PITRArchiveCheckStateRefused, Reason: pitrarchive.TokenArchiveKeyMismatch})
	e.pass()
	waiting := e.entry(src)
	if waiting.Message != pitrCheckReplacingMessage || waiting.Encryption.KeySecretName != liveName {
		t.Fatalf("entry = %+v", waiting)
	}
	e.pass()
	resumed := e.entry(src)
	if resumed.Check == nil || resumed.Encryption.KeySecretName != liveName {
		t.Fatalf("the round restarted instead of resuming with %s: %+v", liveName, resumed)
	}
}

// U54: a completed check Job no entry records (its pass crashed after the
// acceptance, recovery rebuilt the entry) is reclaimed after a successful
// status write.
func TestPITRBindingReclaimsOrphanCheckJobs(t *testing.T) {
	e, src := readTargetEnv(t, interceptor.Funcs{}, "")
	e.pass()
	job := e.checkJob(e.entry(src))
	e.finishCheck(job, 0, "done")
	e.updateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.PITRArchiveBindings[0].Check = nil
		st.PITRArchiveBindings[0].Message = ""
	})
	pitrCheckRetryGate.Store(pitrCheckRetryKey(e.fg(), e.entry(src).ID), time.Now().Add(time.Hour))
	e.pass()
	for _, j := range e.jobs() {
		if j.UID == job.UID {
			t.Fatal("an orphaned completed check Job was not reclaimed")
		}
	}
}

// U58: an acceptance that cannot be built for C3 surfaces as an error,
// never as "the bytes do not match".
func TestPITRRecreateSurfacesAcceptanceErrors(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	entry := e.bound(a)
	broken := *entry
	broken.Source = nil
	key, _ := readDigestKey(context.Background(), e.c, e.fg())
	_, err := e.r.recreatePITRKeySnapshot(context.Background(), e.fg(), key, &broken, []byte("k1"), time.Now())
	if err == nil || !strings.Contains(err.Error(), broken.Encryption.KeySecretName) {
		t.Fatalf("err = %v", err)
	}
}

// U63: a group with nothing to bind does no uncached read at all, and a
// group with PITR disabled lists no Deployment to render.
func TestPITRBindingIdleGroupSkipsUncachedReads(t *testing.T) {
	fg := pbtGroup(pbtProfile("a", "p1", ""))
	fg.Spec.Backup.PITR.Enabled = false
	e := newPBTEnv(t, fg)
	e.r.APIReader = panicReader{}
	if _, err := e.r.reconcilePITRArchiveBindings(context.Background(), e.fg()); err != nil {
		t.Fatal(err)
	}
	if got, err := e.r.resolveGroupPITRRender(context.Background(), e.fg()); err != nil || got.inputs.Enabled {
		t.Fatalf("render = %+v %v", got, err)
	}
}

// panicReader fails a test that reads uncached.
type panicReader struct{}

func (panicReader) Get(_ context.Context, key client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	panic("uncached Get of " + key.String())
}

func (panicReader) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	panic(fmt.Sprintf("uncached List of %T", list))
}

// U68: the pass lists a group's verifications without any field index (a
// manager-less client has none).
func TestPITRVerificationsListedWithoutIndex(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(
		&v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: pbtNS},
			Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: pbtName}}},
		&v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "theirs", Namespace: pbtNS},
			Spec: v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "other"}}},
	).Build()
	r := &MysqlFailoverGroupReconciler{Client: c}
	got, err := r.listPITRVerifications(context.Background(), pbtGroup(pbtProfile("a", "p1", "")))
	if err != nil || len(got) != 1 || got[0].Name != "mine" {
		t.Fatalf("verifications = %v, %v", got, err)
	}
}

// U69: a verification replaying point in time with PITR disabled is told
// it can name a retained archive.
func TestVerificationPITRDisabledRemedy(t *testing.T) {
	fg := renderFG()
	fg.Spec.Backup.PITR.Enabled = false
	_, err := buildRestorePITRFragmentsFor(fg, &pitrReplayRequest{Mode: "latest", Verification: true})
	if err == nil || !strings.Contains(err.Error(), "pointInTime.archiveProfileName") {
		t.Fatalf("err = %v", err)
	}
}
