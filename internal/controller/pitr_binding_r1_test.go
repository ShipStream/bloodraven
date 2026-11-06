package controller

// Regression tests for the #203 review round 1 findings G1, G3, G5 and F-A:
// each reproduces the reported interleaving.

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// siteRender is the decoded pitr-render annotation of site's Deployment.
func (e *pbtEnv) siteRender(site string) pitrRenderInputs {
	e.t.Helper()
	var d appsv1.Deployment
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: pbtNS, Name: "mysql-" + pbtName + "-" + site}, &d); err != nil {
		e.t.Fatal(err)
	}
	in, err := parsePITRRender(d.Annotations[v1alpha1.PITRRenderAnnotation])
	if err != nil {
		e.t.Fatalf("site %s render: %v", site, err)
	}
	return in
}

func (e *pbtEnv) applySite(site string) error {
	return e.r.applySiteDeployment(context.Background(), types.NamespacedName{Namespace: pbtNS, Name: pbtName}, site)
}

// G1: the user switches the PITR profile back to archive A while A's
// release is between its in-use check and its snapshot deletion, and the
// ordered updater renders a site from the fresh group right then. Before
// the fix the updater found A's entry still Bound, installed a Deployment
// mounting A's snapshot, and the release deleted that snapshot. Now the
// render waits for the release's critical section (the in-use reads, the
// status write that removes the entry, the deletions), so it either is
// seen by the in-use check or no longer finds the binding. The race is
// injected at two points of the critical section: the pod listing of the
// in-use check (after the Deployments were read) and the deletion.
// Invariant: no Deployment renders a key snapshot that does not exist.
func TestPITRBindingReleaseSerializedWithOrderedRender(t *testing.T) {
	for _, at := range []string{"in-use pod listing", "snapshot deletion"} {
		t.Run(at, func(t *testing.T) {
			var (
				e        *pbtEnv
				raced    bool
				renderMu sync.Mutex
				renderEr error
				wg       sync.WaitGroup
			)
			// race switches the profile back to A and starts an ordered
			// render, giving it 300 ms to finish (the unfixed code renders
			// at once; the fixed one blocks until the release is done).
			race := func(ctx context.Context, c client.WithWatch) error {
				raced = true
				var fg v1alpha1.MysqlFailoverGroup
				if err := c.Get(ctx, types.NamespacedName{Namespace: pbtNS, Name: pbtName}, &fg); err != nil {
					return err
				}
				fg.Spec.Backup.PITR.ProfileName = "a"
				if err := c.Update(ctx, &fg); err != nil {
					return err
				}
				done := make(chan struct{})
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer close(done)
					err := e.applySite("iad")
					renderMu.Lock()
					renderEr = err
					renderMu.Unlock()
				}()
				select {
				case <-done:
				case <-time.After(300 * time.Millisecond):
				}
				return nil
			}
			armed := false
			funcs := interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.PodList); ok && armed && at == "in-use pod listing" && !raced {
						if err := race(ctx, c); err != nil {
							return err
						}
					}
					return c.List(ctx, list, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if s, ok := obj.(*corev1.Secret); ok && s.Labels[labelAppName] == appNamePITRKeySnapshot && armed && at == "snapshot deletion" && !raced {
						if err := race(ctx, c); err != nil {
							return err
						}
					}
					return c.Delete(ctx, obj, opts...)
				},
			}
			var a v1alpha1.BackupProfile
			var entryA *v1alpha1.PITRArchiveBinding
			e, a, entryA = pbtReleaseEnv(t, funcs)
			for _, site := range []string{"iad", "pdx"} {
				if err := e.applySite(site); err != nil {
					t.Fatal(err)
				}
			}
			if got := e.siteRender("iad").KeySecretName; got == entryA.Encryption.KeySecretName {
				t.Fatalf("fixture: iad already renders A's snapshot %s", got)
			}
			e.release(entryA.ID + ":confirm=" + pbtName)
			armed = true
			e.pass()
			wg.Wait()
			if !raced {
				t.Fatal("the race was never injected")
			}
			renderMu.Lock()
			if renderEr != nil {
				t.Fatalf("ordered render: %v", renderEr)
			}
			renderMu.Unlock()
			for _, site := range []string{"iad", "pdx"} {
				in := e.siteRender(site)
				if in.KeySecretName == "" {
					continue
				}
				if _, ok := e.secret(in.KeySecretName); !ok {
					t.Fatalf("site %s renders key snapshot %s, which the release deleted", site, in.KeySecretName)
				}
			}
			// Either the release went through (the render then held the
			// previous writer), or it was refused because the render came
			// first and A is in use again.
			if _, ok := e.secret(entryA.Encryption.KeySecretName); !ok && e.entry(a) != nil {
				t.Fatal("the released entry is still in status")
			}
		})
	}
}

// G1: the ordered updater has read the group (archive A selected again,
// its entry Bound) and is about to write a site Deployment mounting A's
// snapshot when the user switches away from A once more and A's release
// runs. Nothing the release reads has changed yet (the group's last write
// is the switch away, the Deployments still render the other archive), so
// only the shared render lock keeps it from deleting the snapshot the
// in-flight render is installing: the release waits, sees the new render,
// and is refused.
func TestPITRBindingReleaseWaitsForInFlightRender(t *testing.T) {
	var (
		e         *pbtEnv
		armed     bool
		aboutTo   = make(chan struct{})
		proceed   = make(chan struct{})
		closeOnce sync.Once
	)
	funcs := interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if d, ok := obj.(*appsv1.Deployment); ok && armed && d.Name == "mysql-"+pbtName+"-iad" {
			armed = false
			closeOnce.Do(func() { close(aboutTo) })
			select {
			case <-proceed:
			case <-time.After(500 * time.Millisecond):
			}
		}
		return c.Update(ctx, obj, opts...)
	}}
	var entryA *v1alpha1.PITRArchiveBinding
	e, _, entryA = pbtReleaseEnv(t, funcs)
	for _, site := range []string{"iad", "pdx"} {
		if err := e.applySite(site); err != nil {
			t.Fatal(err)
		}
	}
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "a" })
	armed = true
	var wg sync.WaitGroup
	var renderEr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		renderEr = e.applySite("iad")
	}()
	<-aboutTo
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup.PITR.ProfileName = "c"
		if fg.Annotations == nil {
			fg.Annotations = map[string]string{}
		}
		fg.Annotations[v1alpha1.ReleasePITRArchiveBindingAnnotation] = entryA.ID + ":confirm=" + pbtName
	})
	e.pass()
	close(proceed)
	wg.Wait()
	if renderEr != nil {
		t.Fatalf("ordered render: %v", renderEr)
	}
	in := e.siteRender("iad")
	if in.KeySecretName != entryA.Encryption.KeySecretName {
		t.Fatalf("fixture: the in-flight render installed %s, not A's %s", in.KeySecretName, entryA.Encryption.KeySecretName)
	}
	if _, ok := e.secret(in.KeySecretName); !ok {
		t.Fatalf("site iad renders key snapshot %s, which the release deleted", in.KeySecretName)
	}
}

// G1: the release removes the entry from status before it deletes the
// snapshots. A crash in between leaves the snapshots and the annotation:
// the next pass neither rebuilds the entry from the snapshots' acceptance
// nor re-creates anything, and finishes the release.
func TestPITRBindingReleaseCrashAfterStatusWrite(t *testing.T) {
	crashed := false
	funcs := interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if s, ok := obj.(*corev1.Secret); ok && s.Labels[labelAppName] == appNamePITRKeySnapshot && !crashed {
			crashed = true
			return errors.New("operator crashed")
		}
		return c.Delete(ctx, obj, opts...)
	}}
	e, a, entry := pbtReleaseEnv(t, funcs)
	e.release(entry.ID + ":confirm=" + pbtName)
	fg := e.fg()
	if _, err := e.r.reconcilePITRArchiveBindings(context.Background(), fg); err == nil {
		t.Fatal("the crashed release reported success")
	}
	if e.entry(a) != nil {
		t.Fatal("the entry was not removed before the snapshot deletion")
	}
	if _, ok := e.secret(entry.Encryption.KeySecretName); !ok {
		t.Fatal("fixture: the snapshot is gone")
	}
	e.events()
	e.pass()
	if e.entry(a) != nil {
		t.Fatal("recovery rebuilt a binding under release")
	}
	if _, ok := e.secret(entry.Encryption.KeySecretName); ok {
		t.Fatal("the release did not finish")
	}
	if _, ok := e.releaseAnnotation(); ok {
		t.Fatal("the annotation outlived the finished release")
	}
	e.expectEvents(v1alpha1.EventPITRArchiveBindingReleased)
}

// F-A: pass N binds an entry by its check Job's verdict, writes Bound and
// deletes the Job; a reconcile queued during pass N reads a cache that
// still shows the entry Pending with that check. It finds the Job gone and
// would demote the entry to Pending/Authenticating over the stored Bound.
// The write now finds the stored bindings moved, writes nothing and
// returns errPITRBindingStatusMoved (Reconcile requeues without rendering).
func TestPITRBindingStaleCachedPassDoesNotDemoteBound(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	// A pre-#203 writer runs: C2 authenticates with a check Job.
	e.deploy("iad", "")
	e.deploy("pdx", "")
	e.pass()
	entry := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	if entry.Check == nil {
		t.Fatalf("no check Job started: %+v", entry)
	}
	stale := e.fg() // the cache, frozen before pass N's write
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified,
		ArchiveID: pitrarchive.CreateArchiveID(pbtUID, e.dest(a)), Source: v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID}})
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
	if len(e.jobs()) != 0 {
		t.Fatal("fixture: the consumed check Job was not deleted")
	}
	e.events()

	in := stale.DeepCopy()
	_, err := e.r.reconcilePITRArchiveBindings(context.Background(), in)
	if !errors.Is(err, errPITRBindingStatusMoved) {
		t.Fatalf("stale pass = %v, want errPITRBindingStatusMoved", err)
	}
	got := e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
	if got.Check != nil {
		t.Fatalf("the stale pass wrote a check: %+v", got)
	}
	if len(e.jobs()) != 0 {
		t.Fatal("the stale pass started a duplicate check Job")
	}
	for _, ev := range e.events() {
		if strings.Contains(ev, v1alpha1.EventPITRArchiveBound) {
			t.Fatalf("the stale pass emitted a second %s: %q", v1alpha1.EventPITRArchiveBound, ev)
		}
	}
	// The next pass, from the stored state, is a no-op.
	e.pass()
	e.expectEntry(a, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
}

// G3: a plaintext check runs against empty prefix p2 while the writer runs
// archive A; the user then switches the PITR profile to an encrypted alias
// of p2 before the Absent verdict is consumed (or right after, while the
// entry is Establishing). The entry used to keep mode None in Establishing
// forever, and the render disabled PITR because the candidate no longer
// matched the profile. Now the superseded candidate is dropped, the encrypted
// candidate is authenticated, and the previous writer render is held.
func TestPITRBindingSupersededCandidateDoesNotWedge(t *testing.T) {
	for _, consumeFirst := range []bool{false, true} {
		name := "verdict pending at the switch"
		if consumeFirst {
			name = "Establishing at the switch"
		}
		t.Run(name, func(t *testing.T) {
			a := pbtProfile("a", "p1", "pass-a")
			b := pbtProfile("b", "p2", "")
			b2 := pbtProfile("b2", "p2", "pass-b")
			e := newPBTEnv(t, pbtGroup(a, b, b2), pbtSecret("pass-a", "k1"), pbtSecret("pass-b", "k2"))
			entryA := e.bound(a)
			heldA := e.render(a, entryA.Encryption)
			e.deploy("iad", heldA)
			e.deploy("pdx", heldA)

			e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "b" })
			e.pass()
			entryB := e.expectEntry(b, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
			if entryB.Check == nil || entryB.Encryption.Mode != v1alpha1.PITRArchiveEncryptionNone {
				t.Fatalf("no plaintext check: %+v", entryB)
			}
			e.verdict(entryB, PITRArchiveCheckResult{State: PITRArchiveCheckStateAbsent})
			if consumeFirst {
				e.pass()
				e.expectEntry(b, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonEstablishing)
			}
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "b2" })

			// Before the binding pass runs, the ordered updater may render
			// from the stored status: it must hold A, not disable PITR.
			if in, _ := resolvePITRRender(e.fg(), e.deploysList()); !in.Enabled || in.KeySecretName != entryA.Encryption.KeySecretName {
				t.Fatalf("render before the pass = %+v, want the held writer A", in)
			}

			for i := 0; i < 3; i++ {
				e.pass()
			}
			got := e.expectEntry(b2, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
			if got.Encryption.Mode != v1alpha1.PITRArchiveEncryptionPassphrase || got.Encryption.KeySecretName != e.snapName(b2, "k2") {
				t.Fatalf("entry candidate %+v, want the encrypted alias's key", got.Encryption)
			}
			if got.Check == nil {
				t.Fatal("no check authenticates the encrypted candidate")
			}
			if in, _ := resolvePITRRender(e.fg(), e.deploysList()); !in.Enabled || in.KeySecretName != entryA.Encryption.KeySecretName {
				t.Fatalf("render after the pass = %+v, want the held writer A", in)
			}
			// The encrypted candidate's own Absent now establishes it.
			e.verdict(got, PITRArchiveCheckResult{State: PITRArchiveCheckStateAbsent})
			e.pass()
			est := e.expectEntry(b2, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonEstablishing)
			if in, _ := resolvePITRRender(e.fg(), e.deploysList()); !in.Enabled || in.KeySecretName != est.Encryption.KeySecretName {
				t.Fatalf("render once established = %+v, want the encrypted candidate", in)
			}
		})
	}
}

// deploysList lists the site Deployments.
func (e *pbtEnv) deploysList() []appsv1.Deployment {
	e.t.Helper()
	d, err := e.r.listPITRSiteDeployments(context.Background(), e.fg())
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

// G5: the controller accepts a verdict only when its destination digest
// matches the destination in the check Job's own environment, which must
// be the entry's. A verdict for another destination is retried, never
// applied.
func TestPITRBindingCheckVerdictDestinationDigest(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"))
	e.deploy("iad", "")
	e.deploy("pdx", "")
	e.pass()
	entry := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	e.verdict(entry, PITRArchiveCheckResult{State: PITRArchiveCheckStateVerified,
		ArchiveID: pitrarchive.CreateArchiveID(pbtUID, e.dest(a)), Source: v1alpha1.PITRArchiveSource{Namespace: pbtNS, Name: pbtName, UID: pbtUID},
		DestinationDigest: PITRArchiveCheckDestinationDigest("s3://archive-bucket/elsewhere/binlogs")})
	e.pass()
	got := e.expectEntry(a, v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating)
	if !strings.Contains(got.Message, "names another destination") {
		t.Fatalf("message %q", got.Message)
	}
}

// F-E: CEL compares raw storage strings, so a spelling-only storage edit
// (same normalized destination) bundled with an encryption edit is
// admitted (test/envtest TestPITRBindingCELAllowedEdits). Reconcile is the
// authority: the destination, and so the Bound entry, is unchanged, and
// the encryption change is refused with the previous identity kept.
func TestPITRBindingSpellingOnlyStorageEditRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(p *v1alpha1.BackupProfile)
		reason string
	}{
		{name: "prefix slash, other Secret", edit: func(p *v1alpha1.BackupProfile) {
			p.Storage.S3.Prefix = "/" + p.Storage.S3.Prefix + "/"
			p.Encryption.PassphraseSecret.Name = "pass-x"
		}, reason: v1alpha1.ReasonKeyChangeRefused},
		{name: "endpoint case and port, encryption removed", edit: func(p *v1alpha1.BackupProfile) {
			p.Storage.S3.EndpointURL = "https://S3.Example.Test:443/"
			p.Encryption = nil
		}, reason: v1alpha1.ReasonEncryptionChangeRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := pbtProfile("a", "p1", "pass-a")
			e := newPBTEnv(t, pbtGroup(a), pbtSecret("pass-a", "k1"), pbtSecret("pass-x", "kx"))
			bound := e.bound(a)
			before := e.dest(a)
			e.update(func(fg *v1alpha1.MysqlFailoverGroup) { tc.edit(&fg.Spec.Backup.Profiles[0]) })
			if after := e.dest(e.fg().Spec.Backup.Profiles[0]); after != before {
				t.Fatalf("fixture: the edit changed the destination %s -> %s", before, after)
			}
			e.pass()
			got := e.entry(a)
			if got == nil || got.State != v1alpha1.PITRArchiveBindingBound || got.Encryption != bound.Encryption {
				t.Fatalf("entry changed: %+v", got)
			}
			e.expectCond(metav1.ConditionFalse, tc.reason)
			fg := e.fg()
			if in, _ := resolvePITRRender(fg, nil); in.KeySecretName != bound.Encryption.KeySecretName {
				t.Fatalf("render %+v, want the accepted key %s", in, bound.Encryption.KeySecretName)
			}
		})
	}
}

// F-H: a binding log line's cause says what happened: a new entry or a
// missing key is an input change, never "check" when no check ran; the
// render log lines carry the group's namespace.
func TestPITRBindingLogCauseAndRenderNamespace(t *testing.T) {
	a := pbtProfile("a", "p1", "pass-a")
	missing := pbtProfile("m", "p3", "absent-secret")
	e := newPBTEnv(t, pbtGroup(a, missing), pbtSecret("pass-a", "k1"))
	var lines []string
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{}))
	run := func() {
		t.Helper()
		if _, err := e.r.reconcilePITRArchiveBindings(ctx, e.fg()); err != nil {
			t.Fatal(err)
		}
	}
	causeOf := func() []string {
		var out []string
		for _, l := range lines {
			if strings.Contains(l, logMsgPITRBindingChanged) {
				i := strings.Index(l, `"cause"=`)
				out = append(out, strings.Trim(strings.SplitN(l[i+len(`"cause"=`):], " ", 2)[0], `"`))
			}
		}
		lines = nil
		return out
	}
	run()
	if got := causeOf(); len(got) != 1 || got[0] != pitrBindingCauseInput {
		t.Fatalf("new entry causes = %v, want [input]", got)
	}
	// The identity profile's Secret is missing: an input, not a check.
	e.update(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.ProfileName = "m" })
	run()
	if got := causeOf(); len(got) != 1 || got[0] != pitrBindingCauseInput {
		t.Fatalf("missing key causes = %v, want [input]", got)
	}

	var buf strings.Builder
	e.r.Logger = slog.New(slog.NewJSONHandler(&buf, nil))
	e.deploy("iad", "not json")
	e.deploy("pdx", "")
	if _, err := e.r.resolveGroupPITRRender(context.Background(), e.fg()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, msg := range []string{logMsgPITRRenderIgnored, logMsgPITRRenderDeferred} {
		found := false
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, `"msg":"`+msg+`"`) {
				found = true
				if !strings.Contains(l, `"namespace":"`+pbtNS+`"`) {
					t.Fatalf("%q lacks the namespace: %s", msg, l)
				}
			}
		}
		if !found {
			t.Fatalf("no %q line in %s", msg, out)
		}
	}
}
