package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Tests of the restore hold (#275): a restore that reads a MysqlBackup
// through source.backupRef is a reader of it, as a verification is.

const restoreHoldTestID = "hold-1"

// restoringGroup is a second group in the namespace whose initFromBackup
// reads the backup named "held" (heldBackup, of group lion).
func restoringGroup(phase v1alpha1.BackupPhase, refUID string) *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	fg.Name, fg.UID = "restorer", "uid-restorer"
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "held"}},
	}
	if phase != "" || refUID != "" {
		fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: phase}
	}
	if refUID != "" {
		fg.Status.Restore.BackupRef = &v1alpha1.RestoreBackupRef{Name: "held", UID: refUID, HoldID: restoreHoldTestID}
	}
	return fg
}

// inPlaceRestoringGroup is restoringGroup for a restoreInPlace run.
func inPlaceRestoringGroup(phase v1alpha1.RestoreInPlacePhase, refUID string) *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	fg.Name, fg.UID = "restorer", "uid-restorer"
	fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{
		Confirm: "2026-04-17T14:00:00Z",
		Source:  v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "held"}},
	}
	if phase != "" {
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: phase}
		if refUID != "" {
			fg.Status.RestoreInPlace.BackupRef = &v1alpha1.RestoreBackupRef{Name: "held", UID: refUID, HoldID: restoreHoldTestID}
		}
	}
	return fg
}

func restoreHeldBackup(group string) *v1alpha1.MysqlBackup {
	b := heldBackup()
	b.Annotations = map[string]string{restoreHoldKey(restoreHoldTestID): group}
	return b
}

func terminatingBackup(b *v1alpha1.MysqlBackup) *v1alpha1.MysqlBackup {
	now := metav1.Now()
	b.DeletionTimestamp = &now
	b.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
	return b
}

func restoreHoldJob(group string, condition batchv1.JobConditionType) *batchv1.Job {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: restoreJobName(group, "iad"), Namespace: "ns",
		Labels: map[string]string{labelAppName: restoreJobAppName, v1alpha1.FailoverGroupLabel: group}}}
	if condition != "" {
		job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue}}
	}
	return job
}

func restoreHoldPod(group string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: group + "-restore-pod-" + strings.ToLower(string(phase)), Namespace: "ns",
			Labels: map[string]string{labelAppName: restoreJobAppName, v1alpha1.FailoverGroupLabel: group}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func restoreHoldAnnotations(b *v1alpha1.MysqlBackup) []string {
	var keys []string
	for key := range b.Annotations {
		if strings.HasPrefix(key, v1alpha1.RestoreHoldAnnotationPrefix) {
			keys = append(keys, key)
		}
	}
	return keys
}

func TestArtifactReaders_Restores(t *testing.T) {
	withSpec := func(fg *v1alpha1.MysqlFailoverGroup, mutate func(*v1alpha1.MysqlFailoverGroup)) *v1alpha1.MysqlFailoverGroup {
		mutate(fg)
		return fg
	}
	tests := []struct {
		name string
		objs []client.Object
		want string
	}{
		// Decided from the group's spec and status alone.
		{name: "initFromBackup not started yet", objs: []client.Object{heldBackup(), restoringGroup("", "")}, want: "restorer"},
		{name: "initFromBackup Pending, not pinned", objs: []client.Object{heldBackup(), restoringGroup(v1alpha1.BackupPhasePending, "")}, want: "restorer"},
		{name: "initFromBackup pinned, Pending (no Job yet)", objs: []client.Object{heldBackup(), restoringGroup(v1alpha1.BackupPhasePending, holdBackupUID)}, want: "restorer"},
		{name: "initFromBackup pinned, Running", objs: []client.Object{heldBackup(), restoringGroup(v1alpha1.BackupPhaseRunning, holdBackupUID)}, want: "restorer"},
		{name: "initFromBackup Succeeded", objs: []client.Object{heldBackup(), restoringGroup(v1alpha1.BackupPhaseSucceeded, holdBackupUID)}, want: ""},
		{name: "initFromBackup Failed", objs: []client.Object{heldBackup(), restoringGroup(v1alpha1.BackupPhaseFailed, holdBackupUID)}, want: ""},
		{name: "initFromBackup pinned to an earlier backup of this name", objs: []client.Object{heldBackup(), restoringGroup(v1alpha1.BackupPhaseRunning, "uid-previous")}, want: ""},
		{name: "initFromBackup of another backup", objs: []client.Object{heldBackup(), withSpec(restoringGroup("", ""), func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Source.BackupRef.Name = "another"
		})}, want: ""},
		{name: "initFromBackup from S3", objs: []client.Object{heldBackup(), withSpec(restoringGroup("", ""), func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Source = v1alpha1.RestoreSource{S3: &v1alpha1.S3Storage{Bucket: "b", Prefix: "held"}}
		})}, want: ""},
		{name: "restoreInPlace not observed yet", objs: []client.Object{heldBackup(), inPlaceRestoringGroup("", "")}, want: "restorer"},
		{name: "restoreInPlace pinned, Preflight", objs: []client.Object{heldBackup(), inPlaceRestoringGroup(v1alpha1.RestoreInPlacePreflight, holdBackupUID)}, want: "restorer"},
		{name: "restoreInPlace pinned, Restoring, spec removed", objs: []client.Object{heldBackup(), withSpec(inPlaceRestoringGroup(v1alpha1.RestoreInPlaceRestoring, holdBackupUID), func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.RestoreInPlace = nil
		})}, want: "restorer"},
		{name: "restoreInPlace pinned, Resuming", objs: []client.Object{heldBackup(), inPlaceRestoringGroup(v1alpha1.RestoreInPlaceResuming, holdBackupUID)}, want: "restorer"},
		{name: "restoreInPlace Succeeded", objs: []client.Object{heldBackup(), inPlaceRestoringGroup(v1alpha1.RestoreInPlaceSucceeded, holdBackupUID)}, want: ""},
		{name: "restoreInPlace Failed", objs: []client.Object{heldBackup(), inPlaceRestoringGroup(v1alpha1.RestoreInPlaceFailed, holdBackupUID)}, want: ""},
		{name: "restoreInPlace pinned to an earlier backup of this name", objs: []client.Object{heldBackup(), inPlaceRestoringGroup(v1alpha1.RestoreInPlaceRestoring, "uid-previous")}, want: ""},
		// A backup being deleted: only a restore that already reads it.
		{name: "terminating, restore without a Job", objs: []client.Object{terminatingBackup(heldBackup()), restoringGroup(v1alpha1.BackupPhasePending, "")}, want: ""},
		{name: "terminating, unpinned restore with a Job (older operator)", objs: []client.Object{terminatingBackup(heldBackup()), withSpec(restoringGroup(v1alpha1.BackupPhaseRunning, ""), func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Restore.JobName = "mysql-restorer-iad-restore"
		})}, want: "restorer"},
		{name: "terminating, pinned restore", objs: []client.Object{terminatingBackup(heldBackup()), restoringGroup(v1alpha1.BackupPhaseRunning, holdBackupUID)}, want: "restorer"},
		// Stamped holds.
		{name: "stamped, restore running", objs: []client.Object{restoreHeldBackup("restorer"), restoringGroup(v1alpha1.BackupPhaseRunning, holdBackupUID)}, want: "restorer"},
		{name: "stamped, Failed while its Job has not finished", objs: []client.Object{restoreHeldBackup("restorer"), restoringGroup(v1alpha1.BackupPhaseFailed, holdBackupUID), restoreHoldJob("restorer", "")}, want: "restorer"},
		{name: "stamped, Failed Job with a pod still running", objs: []client.Object{restoreHeldBackup("restorer"), restoringGroup(v1alpha1.BackupPhaseFailed, holdBackupUID), restoreHoldJob("restorer", batchv1.JobFailed), restoreHoldPod("restorer", corev1.PodRunning)}, want: "restorer"},
		{name: "stamped, finished and quiescent", objs: []client.Object{restoreHeldBackup("restorer"), restoringGroup(v1alpha1.BackupPhaseSucceeded, holdBackupUID), restoreHoldJob("restorer", batchv1.JobComplete), restoreHoldPod("restorer", corev1.PodSucceeded)}, want: ""},
		{name: "stamped, group deleted, orphan pod running", objs: []client.Object{restoreHeldBackup("restorer"), restoreHoldPod("restorer", corev1.PodRunning)}, want: "restorer"},
		{name: "stamped, group deleted, nothing running", objs: []client.Object{restoreHeldBackup("restorer")}, want: ""},
		{name: "stamped by an earlier attempt, the current one reads another backup", objs: []client.Object{restoreHeldBackup("restorer"), withSpec(restoringGroup(v1alpha1.BackupPhaseRunning, "uid-other"), func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Restore.BackupRef = &v1alpha1.RestoreBackupRef{Name: "other", UID: "uid-other", HoldID: "hold-2"}
		})}, want: ""},
		{name: "stamped, another group's pods do not keep it", objs: []client.Object{restoreHeldBackup("restorer"), restoreHoldPod("lion", corev1.PodRunning)}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(tc.objs...).Build()
			var backup v1alpha1.MysqlBackup
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "held"}, &backup); err != nil {
				t.Fatal(err)
			}
			refs, err := artifactReaderRefs(context.Background(), c, &backup)
			if err != nil {
				t.Fatal(err)
			}
			got := ""
			for _, ref := range refs {
				if ref.Kind != readerRestore {
					t.Fatalf("reader %+v is not a restore", ref)
				}
				got += ref.Name
			}
			if got != tc.want {
				t.Fatalf("restore readers = %q, want %q", got, tc.want)
			}
			if held, err := NewBackupReadChecker(c).BeingRead(context.Background(), &backup); err != nil || held != (tc.want != "") {
				t.Fatalf("BeingRead = %v, %v", held, err)
			}
		})
	}
}

// Any reader lookup that fails is an error, never "not read".
func TestArtifactReaders_RestoreLookupsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		objs []client.Object
		fail func(obj any) bool
	}{
		{name: "group List", objs: []client.Object{heldBackup()}, fail: func(obj any) bool {
			_, ok := obj.(*v1alpha1.MysqlFailoverGroupList)
			return ok
		}},
		{name: "group Get of a stamped hold", objs: []client.Object{restoreHeldBackup("restorer")}, fail: func(obj any) bool {
			_, ok := obj.(*v1alpha1.MysqlFailoverGroup)
			return ok
		}},
		{name: "restore pod List of a stamped hold", objs: []client.Object{restoreHeldBackup("restorer")}, fail: func(obj any) bool {
			_, ok := obj.(*corev1.PodList)
			return ok
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(tc.objs...).Build()
			c := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if tc.fail(obj) {
						return errors.New("injected")
					}
					return c.Get(ctx, key, obj, opts...)
				},
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if tc.fail(list) {
						return errors.New("injected")
					}
					return c.List(ctx, list, opts...)
				},
			})
			backup := tc.objs[0].(*v1alpha1.MysqlBackup)
			if refs, err := artifactReaderRefs(context.Background(), c, backup); err == nil {
				t.Fatalf("readers = %v with a failing lookup, want an error", refs)
			}
		})
	}
}

func TestDescribeReaders(t *testing.T) {
	v := func(name string) artifactReader { return artifactReader{Kind: readerVerification, Name: name} }
	g := func(name string) artifactReader { return artifactReader{Kind: readerRestore, Name: name} }
	for _, tc := range []struct {
		refs []artifactReader
		want string
	}{
		{[]artifactReader{v("a"), v("b")}, "verification a, b"},
		{[]artifactReader{g("lion")}, "the restore of group lion"},
		{[]artifactReader{g("lion"), g("tiger")}, "the restores of groups lion, tiger"},
		{[]artifactReader{v("a"), g("lion")}, "verification a and the restore of group lion"},
	} {
		if got := describeReaders(tc.refs); got != tc.want {
			t.Errorf("describeReaders = %q, want %q", got, tc.want)
		}
	}
}

func TestBackupHasReadHold(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"", false},
		{"bloodraven.shipstream.io/other", false},
		{v1alpha1.VerificationHoldAnnotationPrefix + "uid-v", true},
		{restoreHoldKey("hold-1"), true},
	} {
		b := &v1alpha1.MysqlBackup{}
		if tc.key != "" {
			b.Annotations = map[string]string{tc.key: "x"}
		}
		if got := BackupHasReadHold(b); got != tc.want {
			t.Errorf("BackupHasReadHold(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

// retentionRestoreFixture is two successful backups of one profile, b-a
// (newest) and b-b, and a group restoring from b-b.
type retentionRestoreFixture struct {
	r       *MysqlBackupReconciler
	c       client.Client
	rec     *record.FakeRecorder
	backups []v1alpha1.MysqlBackup
	group   *v1alpha1.MysqlFailoverGroup
}

func newRetentionRestoreFixture(t *testing.T, bAge time.Duration, group *v1alpha1.MysqlFailoverGroup, stamped bool) *retentionRestoreFixture {
	t.Helper()
	backups := makeBackups(t, time.Now(), 0, bAge)
	for i := range backups {
		backups[i].UID = types.UID("uid-" + backups[i].Name)
	}
	if stamped {
		backups[1].Annotations = map[string]string{restoreHoldKey(restoreHoldTestID): group.Name}
	}
	r, c := newBackupReconciler(t, append(asClientObjects(backups), group)...)
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec
	return &retentionRestoreFixture{r: r, c: c, rec: rec, backups: backups, group: group}
}

// restoringFrom is a group whose restore reads b-b (uid-b-b).
func restoringFrom(inPlace bool) *v1alpha1.MysqlFailoverGroup {
	var fg *v1alpha1.MysqlFailoverGroup
	if inPlace {
		fg = inPlaceRestoringGroup(v1alpha1.RestoreInPlaceRestoring, "uid-b-b")
		fg.Spec.RestoreInPlace.Source.BackupRef.Name = "b-b"
		fg.Status.RestoreInPlace.BackupRef.Name = "b-b"
	} else {
		fg = restoringGroup(v1alpha1.BackupPhaseRunning, "uid-b-b")
		fg.Spec.InitFromBackup.Source.BackupRef.Name = "b-b"
		fg.Status.Restore.BackupRef.Name = "b-b"
	}
	return fg
}

// endRestore writes the restore's terminal status, as its reconciler does.
func (f *retentionRestoreFixture) endRestore(t *testing.T, succeeded bool) {
	t.Helper()
	if st := f.group.Status.RestoreInPlace; st != nil {
		st.Phase = v1alpha1.RestoreInPlaceFailed
		if succeeded {
			st.Phase = v1alpha1.RestoreInPlaceSucceeded
		}
	} else {
		f.group.Status.Restore.Phase = v1alpha1.BackupPhaseFailed
		if succeeded {
			f.group.Status.Restore.Phase = v1alpha1.BackupPhaseSucceeded
		}
	}
	if err := f.c.Status().Update(context.Background(), f.group); err != nil {
		t.Fatal(err)
	}
}

// TestPruneSuccessful_KeepsBackupARestoreReads pins #275 for retention: a
// backup that no rule keeps any more (pushed out of keepLast by a newer
// one, or aged past keepWithinDays) stays while a restore reads it, with a
// RetentionDeferred Event naming the restoring group, and is pruned on the
// first pass after the restore ends, whether it succeeded or failed.
func TestPruneSuccessful_KeepsBackupARestoreReads(t *testing.T) {
	for _, rule := range []struct {
		name       string
		bAge       time.Duration
		keepWithin time.Duration
	}{
		{name: "pushed out of keepLast", bAge: time.Hour},
		{name: "aged past keepWithinDays", bAge: 73 * time.Hour, keepWithin: 48 * time.Hour},
	} {
		for _, restore := range []struct {
			name      string
			inPlace   bool
			stamped   bool
			succeeded bool
		}{
			{name: "initFromBackup, stamped, succeeds", stamped: true, succeeded: true},
			{name: "initFromBackup, stamped, fails", stamped: true},
			{name: "initFromBackup, pinned only, succeeds", succeeded: true},
			{name: "restoreInPlace, stamped, succeeds", inPlace: true, stamped: true, succeeded: true},
			{name: "restoreInPlace, stamped, fails", inPlace: true, stamped: true},
		} {
			t.Run(rule.name+"/"+restore.name, func(t *testing.T) {
				ctx := context.Background()
				f := newRetentionRestoreFixture(t, rule.bAge, restoringFrom(restore.inPlace), restore.stamped)
				if err := f.r.pruneSuccessful(ctx, f.backups, 1, rule.keepWithin); err != nil {
					t.Fatal(err)
				}
				assertEqualSet(t, listBackupNames(t, f.r), []string{"b-a", "b-b"})
				events := drainEvents(f.rec)
				if len(events) != 1 || !strings.Contains(events[0], v1alpha1.EventRetentionDeferred) ||
					!strings.Contains(events[0], "the restore of group restorer") {
					t.Fatalf("events = %q, want one RetentionDeferred naming the restoring group", events)
				}

				f.endRestore(t, restore.succeeded)
				if err := f.r.pruneSuccessful(ctx, f.backups, 1, rule.keepWithin); err != nil {
					t.Fatal(err)
				}
				assertEqualSet(t, listBackupNames(t, f.r), []string{"b-a"})
			})
		}
	}
}

// A restore pinned to an earlier backup of the same name does not protect
// the backup that replaced it: readers are matched by UID.
func TestPruneSuccessful_SameNameReplacementIsNotProtectedByARestore(t *testing.T) {
	for _, inPlace := range []bool{false, true} {
		group := restoringFrom(inPlace)
		if inPlace {
			group.Status.RestoreInPlace.BackupRef.UID = "uid-b-b-previous"
		} else {
			group.Status.Restore.BackupRef.UID = "uid-b-b-previous"
		}
		f := newRetentionRestoreFixture(t, time.Hour, group, false)
		if err := f.r.pruneSuccessful(context.Background(), f.backups, 1, 0); err != nil {
			t.Fatal(err)
		}
		assertEqualSet(t, listBackupNames(t, f.r), []string{"b-a"})
	}
}

func TestPruneSuccessful_LogsBackupHeldByRestore(t *testing.T) {
	f := newRetentionRestoreFixture(t, time.Hour, restoringFrom(false), true)
	var lines []string
	logger := funcr.New(func(prefix, args string) { lines = append(lines, args) }, funcr.Options{})
	if err := f.r.pruneSuccessful(log.IntoContext(context.Background(), logger), f.backups, 1, 0); err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if strings.Contains(line, `"msg"="backup prune held by verification"`) {
			t.Fatalf("a restore reader was logged as a verification: %s", line)
		}
		if strings.Contains(line, `"msg"="backup prune held by restore"`) &&
			strings.Contains(line, `"namespace"="ns"`) && strings.Contains(line, `"backup"="b-b"`) &&
			strings.Contains(line, `"backupUID"="uid-b-b"`) && strings.Contains(line, `"fg"="lion"`) &&
			strings.Contains(line, `"profile"="nightly"`) && strings.Contains(line, `"restoreGroups"=["restorer"]`) {
			return
		}
	}
	t.Fatalf("missing structured log line; got %q", lines)
}

// The delete carries the resourceVersion retention read before it looked
// for readers: a hold stamped in between makes the delete conflict.
func TestDeleteUnreadBackup_HoldStampedAfterTheReadWins(t *testing.T) {
	ctx := context.Background()
	group := restoringGroup("", "")
	group.Spec.InitFromBackup = nil // not visible to the reader scan
	f := newRetentionRestoreFixture(t, time.Hour, group, false)
	victim := f.backups[1]
	stamped := false
	f.r.Client = interceptor.NewClient(f.c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if !stamped {
				// A restore pins and stamps between the reader List and
				// the delete.
				stamped = true
				var b v1alpha1.MysqlBackup
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &b); err != nil {
					return err
				}
				b.Annotations = map[string]string{restoreHoldKey(restoreHoldTestID): "restorer"}
				if err := c.Update(ctx, &b); err != nil {
					return err
				}
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	f.r.APIReader = f.c
	if err := f.r.deleteUnreadBackup(ctx, &victim, NewBackupReadChecker(f.c)); err != nil {
		t.Fatal(err)
	}
	if !stamped {
		t.Fatal("the delete was never attempted")
	}
	assertEqualSet(t, listBackupNames(t, f.r), []string{"b-a", "b-b"})
}

func TestFinalize_DefersWhileRestoreReads(t *testing.T) {
	ctx := context.Background()
	f := newCleanupFixture(t)
	group := restoringGroup(v1alpha1.BackupPhaseRunning, string(f.mb.UID))
	group.Spec.InitFromBackup.Source.BackupRef.Name = f.mb.Name
	group.Status.Restore.BackupRef.Name = f.mb.Name
	status := group.Status.DeepCopy()
	if err := f.c.Create(ctx, group); err != nil {
		t.Fatal(err)
	}
	group.Status = *status
	if err := f.c.Status().Update(ctx, group); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if done, requeue := f.finalize(t); done || requeue != 30*time.Second {
			t.Fatalf("done=%v requeue=%s, want a 30s deferral", done, requeue)
		}
	}
	if _, ok := f.cleanupJob(t); ok {
		t.Fatal("cleanup started while a restore reads the artifact")
	}
	cond := cleanupCondition(t, f)
	if cond == nil || cond.Reason != v1alpha1.ReasonHeldByRestore || !strings.Contains(cond.Message, "the restore of group restorer") {
		t.Fatalf("condition %+v", cond)
	}
	if n := countEvents(f.rec, "ArtifactCleanupDeferred"); n != 1 {
		t.Fatalf("ArtifactCleanupDeferred events = %d, want 1", n)
	}

	group.Status.Restore.Phase = v1alpha1.BackupPhaseFailed
	if err := f.c.Status().Update(ctx, group); err != nil {
		t.Fatal(err)
	}
	f.finalize(t)
	if _, ok := f.cleanupJob(t); !ok {
		t.Fatal("cleanup did not start after the restore ended")
	}
}

// A hold whose restore can no longer read is stripped from a backup being
// deleted before anything else, so it does not keep the PITR cutoff pinned.
func TestFinalize_StripsDeadRestoreHold(t *testing.T) {
	f := newCleanupFixture(t)
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	f.mb.Annotations = map[string]string{restoreHoldKey(restoreHoldTestID): "deleted-group"}
	if err := f.c.Update(context.Background(), f.mb); err != nil {
		t.Fatal(err)
	}
	f.finalize(t)
	if err := f.c.Get(context.Background(), client.ObjectKeyFromObject(f.mb), f.mb); err != nil {
		t.Fatal(err)
	}
	if BackupHasReadHold(f.mb) {
		t.Fatalf("dead restore hold kept: %v", f.mb.Annotations)
	}
	if _, ok := f.cleanupJob(t); !ok {
		t.Fatal("cleanup did not start once the dead hold was stripped")
	}
}

// reconcileBackup runs the MysqlBackup reconciler until the backup settles
// and returns the last result.
func reconcileBackup(t *testing.T, r *MysqlBackupReconciler, name string) ctrl.Result {
	t.Helper()
	var res ctrl.Result
	for range 4 {
		var err error
		res, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: name}})
		if err != nil {
			t.Fatal(err)
		}
		if !res.Requeue {
			break
		}
	}
	return res
}

// The hold of a restore that ended without releasing it, or whose group
// was deleted, is stripped from a live backup by the backup's own
// reconciler; a live hold is kept and the backup is looked at again.
func TestBackupReconcile_StripsDeadRestoreHolds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		objs     []client.Object
		wantHeld bool
	}{
		{name: "group deleted", wantHeld: false},
		{name: "group deleted, its restore pod still running", objs: []client.Object{restoreHoldPod("restorer", corev1.PodRunning)}, wantHeld: true},
		{name: "restore running", objs: []client.Object{restoringGroup(v1alpha1.BackupPhaseRunning, holdBackupUID)}, wantHeld: true},
		{name: "restore ended, release lost", objs: []client.Object{restoringGroup(v1alpha1.BackupPhaseSucceeded, holdBackupUID)}, wantHeld: false},
		{name: "spec no longer names the restore", objs: []client.Object{func() client.Object {
			fg := restoringGroup(v1alpha1.BackupPhaseRunning, holdBackupUID)
			fg.Spec.InitFromBackup = nil
			return fg
		}()}, wantHeld: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backup := restoreHeldBackup("restorer")
			backup.Finalizers = []string{v1alpha1.MysqlBackupFinalizer}
			r, c := newBackupReconciler(t, append([]client.Object{fgWithBackup(), backup}, tc.objs...)...)
			res := reconcileBackup(t, r, backup.Name)
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(backup), backup); err != nil {
				t.Fatal(err)
			}
			if held := backupHasRestoreHold(backup); held != tc.wantHeld {
				t.Fatalf("held = %v, want %v (annotations %v)", held, tc.wantHeld, backup.Annotations)
			}
			if tc.wantHeld && (res.RequeueAfter <= 0 || res.RequeueAfter > restoreHoldRecheck) {
				t.Fatalf("requeue = %s, want a recheck within %s while the hold is live", res.RequeueAfter, restoreHoldRecheck)
			}
		})
	}
}

// --- The restoring side: pin, stamp, create, release ----------------------

func restoreHoldSeed() *v1alpha1.MysqlBackup {
	seed := succeededSeedBackup()
	seed.UID = "uid-seed"
	return seed
}

// createRestoreSeedBackup creates the Succeeded backup "seed" that
// fgInPlaceRestore's spec reads, for fixtures that drive an in-place
// restore through Preflight: a run whose source backup cannot be held ends
// there.
func createRestoreSeedBackup(t *testing.T, c client.Client, namespace string) {
	t.Helper()
	seed := restoreHoldSeed()
	seed.Namespace = namespace
	if err := c.Create(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
}

func fetchBackup(t *testing.T, c client.Client, name string) *v1alpha1.MysqlBackup {
	t.Helper()
	var b v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, &b); err != nil {
		t.Fatal(err)
	}
	return &b
}

func fetchGroup(t *testing.T, c client.Client, name string) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, &fg); err != nil {
		t.Fatal(err)
	}
	return &fg
}

func restoreJobExists(t *testing.T, c client.Client, name string) (*batchv1.Job, bool) {
	t.Helper()
	var job batchv1.Job
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, &job)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return &job, true
}

func endRestoreJob(t *testing.T, c client.Client, job *batchv1.Job, condition batchv1.JobConditionType) {
	t.Helper()
	now := metav1.Now()
	job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, LastTransitionTime: now}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
}

// restartedGroupReconciler is a reconciler with none of the previous one's
// memory, on the same API state: an operator restart.
func restartedGroupReconciler(c client.Client) *MysqlFailoverGroupReconciler {
	return &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: record.NewFakeRecorder(20)}
}

func initRestoreFixture(t *testing.T, seed *v1alpha1.MysqlBackup) (*MysqlFailoverGroupReconciler, client.Client, string) {
	t.Helper()
	fg := fgInitFromMysqlBackup()
	seedPublished(fg)
	site := fg.Spec.Sites[0].Name
	r, c := newReconciler(fg, seed, primaryReadyDeployment("lion", site), operatorCredsSecret())
	return r, c, restoreJobName("lion", site)
}

// TestRestoreHold_InitFromBackup pins the order for a bootstrap restore:
// the backup is pinned by UID on status.restore.backupRef and held before
// the Job exists, the Job records the UID it was built from, the terminal
// status keeps the pin, and the hold is released on the pass after the
// terminal status was persisted (also by a restarted operator).
func TestRestoreHold_InitFromBackup(t *testing.T) {
	for _, outcome := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run(string(outcome), func(t *testing.T) {
			ctx := context.Background()
			r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
			if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			fg := fetchGroup(t, c, "lion")
			ref := fg.Status.Restore.BackupRef
			if ref == nil || ref.Name != "seed" || ref.UID != "uid-seed" || ref.HoldID == "" {
				t.Fatalf("status.restore.backupRef = %+v, want the pinned seed", ref)
			}
			job, ok := restoreJobExists(t, c, jobName)
			if !ok || job.Annotations[v1alpha1.RestoreBackupUIDAnnotation] != "uid-seed" {
				t.Fatalf("restore Job missing or not built from the pinned backup: %+v", job)
			}
			seed := fetchBackup(t, c, "seed")
			if seed.Annotations[restoreHoldKey(ref.HoldID)] != "lion" {
				t.Fatalf("hold not stamped: %v", seed.Annotations)
			}
			if names, err := artifactReaders(ctx, c, seed); err != nil || len(names) != 1 || names[0] != "lion" {
				t.Fatalf("readers = %v, %v", names, err)
			}

			endRestoreJob(t, c, job, outcome)
			if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			fg = fetchGroup(t, c, "lion")
			if !initRestoreTerminal(fg.Status.Restore) || fg.Status.Restore.BackupRef == nil || *fg.Status.Restore.BackupRef != *ref {
				t.Fatalf("terminal status = %+v, want it to keep the pin", fg.Status.Restore)
			}
			// The pass that wrote the terminal status does not release.
			if seed = fetchBackup(t, c, "seed"); seed.Annotations[restoreHoldKey(ref.HoldID)] != "lion" {
				t.Fatal("hold released before the terminal status was persisted")
			}
			// Dead already, though: retention no longer waits for it.
			if names, err := artifactReaders(ctx, c, seed); err != nil || len(names) != 0 {
				t.Fatalf("readers after the restore ended = %v, %v", names, err)
			}

			if _, err := restartedGroupReconciler(c).reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			if seed = fetchBackup(t, c, "seed"); len(restoreHoldAnnotations(seed)) != 0 {
				t.Fatalf("hold not released after the restore ended: %v", seed.Annotations)
			}
		})
	}
}

// A restore whose pods have not stopped keeps its hold after the terminal
// status; the release happens once they have.
func TestRestoreHold_ReleaseWaitsForRestorePods(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	job, _ := restoreJobExists(t, c, jobName)
	endRestoreJob(t, c, job, batchv1.JobFailed)
	pod := restoreHoldPod("lion", corev1.PodRunning)
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
			t.Fatal(err)
		}
	}
	seed := fetchBackup(t, c, "seed")
	if len(restoreHoldAnnotations(seed)) != 1 {
		t.Fatalf("hold released under a running restore pod: %v", seed.Annotations)
	}
	if names, err := artifactReaders(ctx, c, seed); err != nil || len(names) != 1 {
		t.Fatalf("readers = %v, %v, want the group while its pod runs", names, err)
	}
	pod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	if seed = fetchBackup(t, c, "seed"); len(restoreHoldAnnotations(seed)) != 0 {
		t.Fatalf("hold not released once the pod stopped: %v", seed.Annotations)
	}
}

// An operator restart between the pin and the stamp, and between the stamp
// and the Job's creation: the next pass finishes the same attempt under
// the same hold ID, and the backup is a held reader's all the way through.
func TestRestoreHold_InitFromBackup_RestartBeforeJobCreation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		funcs       func(failed *bool) interceptor.Funcs
		wantStamped bool
	}{
		{name: "after the pin, before the stamp", funcs: func(failed *bool) interceptor.Funcs {
			return interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*v1alpha1.MysqlBackup); ok && !*failed {
					*failed = true
					return errors.New("injected: operator stopped")
				}
				return c.Patch(ctx, obj, patch, opts...)
			}}
		}},
		{name: "after the stamp, before the Job", wantStamped: true, funcs: func(failed *bool) interceptor.Funcs {
			return interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*batchv1.Job); ok && !*failed {
					*failed = true
					return errors.New("injected: operator stopped")
				}
				return c.Create(ctx, obj, opts...)
			}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
			failed := false
			r.Client = interceptor.NewClient(c.(client.WithWatch), tc.funcs(&failed))
			if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err == nil || !failed {
				t.Fatalf("err = %v, failed = %v, want the injected stop", err, failed)
			}
			if _, ok := restoreJobExists(t, c, jobName); ok {
				t.Fatal("restore Job created without a confirmed hold")
			}
			fg := fetchGroup(t, c, "lion")
			ref := fg.Status.Restore.BackupRef
			if ref == nil || ref.UID != "uid-seed" || fg.Status.Restore.Phase != v1alpha1.BackupPhasePending {
				t.Fatalf("status.restore = %+v, want a Pending restore with its pin persisted", fg.Status.Restore)
			}
			seed := fetchBackup(t, c, "seed")
			if stamped := seed.Annotations[restoreHoldKey(ref.HoldID)] == "lion"; stamped != tc.wantStamped {
				t.Fatalf("stamped = %v, want %v", stamped, tc.wantStamped)
			}
			// Pinned, so already a reader: retention would defer.
			if names, err := artifactReaders(ctx, c, seed); err != nil || len(names) != 1 {
				t.Fatalf("readers = %v, %v", names, err)
			}

			if _, err := restartedGroupReconciler(c).reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			if _, ok := restoreJobExists(t, c, jobName); !ok {
				t.Fatal("restore Job not created after the restart")
			}
			fg = fetchGroup(t, c, "lion")
			if got := fg.Status.Restore.BackupRef; got == nil || *got != *ref {
				t.Fatalf("pin changed across the restart: %+v, was %+v", got, ref)
			}
			seed = fetchBackup(t, c, "seed")
			if keys := restoreHoldAnnotations(seed); len(keys) != 1 || keys[0] != restoreHoldKey(ref.HoldID) {
				t.Fatalf("holds after the restart = %v, want the one of the pinned attempt", keys)
			}
		})
	}
}

// A stamp that loses a resourceVersion race (retention, or anything else,
// wrote the backup since it was read) creates nothing and is retried.
func TestRestoreHold_StampConflictIsRetried(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
	conflicts := 1
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*v1alpha1.MysqlBackup); ok && conflicts > 0 {
				conflicts--
				return apierrors.NewConflict(schema.GroupResource{Resource: "mysqlbackups"}, obj.GetName(), errors.New("injected"))
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	requeue, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion"))
	if err != nil || requeue != restoreHoldRetry {
		t.Fatalf("requeue = %s, err = %v, want a retry", requeue, err)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created without its hold")
	}
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	if _, ok := restoreJobExists(t, c, jobName); !ok {
		t.Fatal("restore Job not created after the retry")
	}
}

// A backup that cannot be held ends the attempt with nothing created: one
// being deleted (the delete won), and one whose pinned object was replaced
// under the same name.
func TestRestoreHold_InitFromBackup_RefusesUnholdableBackup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		seed    func() *v1alpha1.MysqlBackup
		pinned  string
		message string
	}{
		{name: "being deleted", seed: func() *v1alpha1.MysqlBackup { return terminatingBackup(restoreHoldSeed()) }, message: "is being deleted"},
		{name: "replaced after the pin", seed: restoreHoldSeed, pinned: "uid-seed-previous", message: "was replaced by another backup of the same name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, c, jobName := initRestoreFixture(t, tc.seed())
			fg := fetchGroup(t, c, "lion")
			if tc.pinned != "" {
				fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending,
					BackupRef: &v1alpha1.RestoreBackupRef{Name: "seed", UID: tc.pinned, HoldID: restoreHoldTestID}}
				if err := c.Status().Update(ctx, fg); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.reconcileRestoreJob(ctx, fg); err != nil {
				t.Fatal(err)
			}
			if _, ok := restoreJobExists(t, c, jobName); ok {
				t.Fatal("restore Job created from a backup that cannot be held")
			}
			fg = fetchGroup(t, c, "lion")
			if fg.Status.Restore.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(fg.Status.Restore.Message, tc.message) {
				t.Fatalf("status.restore = %+v, want Failed (%s)", fg.Status.Restore, tc.message)
			}
			if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 0 {
				t.Fatalf("hold stamped on a refused backup: %v", keys)
			}
			if !drainForEvent(r, v1alpha1.EventRestoreBuildFailed) {
				t.Fatal("no RestoreBuildFailed Event")
			}
		})
	}
}

// The Job is built through the cache. When the cache still shows another
// object under the backup's name than the API server has, nothing is
// pinned, held or created until they agree.
func TestRestoreHold_JobBuiltFromStaleCacheIsNotCreated(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
	current := restoreHoldSeed()
	current.UID = "uid-seed-recreated"
	group := fetchGroup(t, c, "lion")
	group.ResourceVersion = ""
	r.APIReader = fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(current, group).Build()
	requeue, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion"))
	if err != nil || requeue != restoreHoldRetry {
		t.Fatalf("requeue = %s, err = %v, want a retry", requeue, err)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created from a cached copy of another backup")
	}
	if fg := fetchGroup(t, c, "lion"); fg.Status.Restore != nil && fg.Status.Restore.BackupRef != nil {
		t.Fatalf("pinned %+v from a stale read", fg.Status.Restore.BackupRef)
	}
}

// The pin is decided from the group as the API server has it. A pass whose
// cached group still shows the attempt unfinished, after its terminal
// status was written, must not go on under that attempt's hold ID (readers
// may have seen it dead): nothing is created until the cache has caught up.
func TestRestoreHold_StaleViewOfTheRestoreIsRetried(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
	stale := fetchGroup(t, c, "lion")
	stale.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending,
		BackupRef: &v1alpha1.RestoreBackupRef{Name: "seed", UID: "uid-seed", HoldID: restoreHoldTestID}}
	// The API server has the attempt Failed already.
	live := fetchGroup(t, c, "lion")
	live.Status.Restore = stale.Status.Restore.DeepCopy()
	live.Status.Restore.Phase = v1alpha1.BackupPhaseFailed
	if err := c.Status().Update(ctx, live); err != nil {
		t.Fatal(err)
	}
	requeue, err := r.reconcileRestoreJob(ctx, stale)
	if err != nil || requeue != restoreHoldRetry {
		t.Fatalf("requeue = %s, err = %v, want a retry", requeue, err)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created under an ended attempt's hold")
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 0 {
		t.Fatalf("ended attempt's hold stamped again: %v", keys)
	}
	// With the current group the retry is a new attempt.
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	ref := fetchGroup(t, c, "lion").Status.Restore.BackupRef
	if _, ok := restoreJobExists(t, c, jobName); !ok || ref == nil || ref.HoldID == restoreHoldTestID {
		t.Fatalf("pin = %+v, want a new attempt with its own hold and a Job", ref)
	}
}

// A retry of a failed restore (its Job deleted) is a new attempt: it pins
// again and takes a new hold ID, never the ended attempt's.
func TestRestoreHold_InitFromBackup_RetryTakesANewHold(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := initRestoreFixture(t, restoreHoldSeed())
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	job, _ := restoreJobExists(t, c, jobName)
	endRestoreJob(t, c, job, batchv1.JobFailed)
	for range 2 {
		if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
			t.Fatal(err)
		}
	}
	first := fetchGroup(t, c, "lion").Status.Restore.BackupRef
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); first == nil || len(keys) != 0 {
		t.Fatalf("first attempt: pin %+v, holds %v", first, keys)
	}
	if err := c.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}
	// While the retry waits for its target, its Pending status must not
	// inherit the ended attempt's pin: that hold was seen dead.
	var deploy appsv1.Deployment
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: resourceName("lion", "iad")}, &deploy); err != nil {
		t.Fatal(err)
	}
	deploy.Status.ReadyReplicas = 0
	if err := c.Status().Update(ctx, &deploy); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	if st := fetchGroup(t, c, "lion").Status.Restore; st.Phase != v1alpha1.BackupPhasePending || st.BackupRef != nil {
		t.Fatalf("waiting retry = %+v, want Pending without the ended attempt's pin", st)
	}
	deploy.Status.ReadyReplicas = 1
	if err := c.Status().Update(ctx, &deploy); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreJob(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	second := fetchGroup(t, c, "lion").Status.Restore.BackupRef
	if second == nil || second.HoldID == first.HoldID || second.UID != "uid-seed" {
		t.Fatalf("retry pin = %+v, want a new hold ID (first %+v)", second, first)
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 1 || keys[0] != restoreHoldKey(second.HoldID) {
		t.Fatalf("holds = %v, want the retry's", keys)
	}
	if _, ok := restoreJobExists(t, c, jobName); !ok {
		t.Fatal("retry did not create a Job")
	}
}

// A direct S3 source has no MysqlBackup to hold.
func TestRestoreHold_DirectSourceTakesNoHold(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	fg.Spec.InitFromBackup.Source = v1alpha1.RestoreSource{S3: &v1alpha1.S3Storage{Bucket: "b", Prefix: "dump", CredentialsSecretName: "s3-creds"}}
	seedPublished(fg)
	site := fg.Spec.Sites[0].Name
	r, c := newReconciler(fg, primaryReadyDeployment("lion", site), operatorCredsSecret())
	if _, err := r.reconcileRestoreJob(context.Background(), fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	job, ok := restoreJobExists(t, c, restoreJobName("lion", site))
	if !ok {
		t.Fatal("restore Job not created")
	}
	if _, ok := job.Annotations[v1alpha1.RestoreBackupUIDAnnotation]; ok {
		t.Fatalf("direct-source Job names a backup: %v", job.Annotations)
	}
	if ref := fetchGroup(t, c, "lion").Status.Restore.BackupRef; ref != nil {
		t.Fatalf("direct-source restore pinned %+v", ref)
	}
}

// inPlaceHoldFixture is a per-schema in-place restore in Preflight.
func inPlaceHoldFixture(t *testing.T, seed *v1alpha1.MysqlBackup) (*MysqlFailoverGroupReconciler, client.Client, string) {
	t.Helper()
	fg := fgInPlaceRestore("orders")
	scope, _ := inPlaceRestoreScope(fg.Spec.RestoreInPlace)
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlacePreflight,
		Scope:      scope,
		RunConfirm: fg.Spec.RestoreInPlace.Confirm,
		RunID:      "run-1",
		StartTime:  ptrTime(time.Now()),
	}
	r, c := newReconciler(fg, seed, operatorCredsSecret())
	return r, c, inPlaceRestoreJobName(fg.Name, fg.Spec.Sites[0].Name)
}

// driveInPlace reconciles the in-place restore until phase is reached.
func driveInPlace(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, until func(*v1alpha1.RestoreInPlaceStatus) bool) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	for range 8 {
		fg := fetchGroup(t, c, "lion")
		if until(fg.Status.RestoreInPlace) {
			return fg
		}
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	fg := fetchGroup(t, c, "lion")
	if !until(fg.Status.RestoreInPlace) {
		t.Fatalf("in-place restore did not get there: %+v", fg.Status.RestoreInPlace)
	}
	return fg
}

// TestRestoreHold_InPlace pins the order for an in-place restore: the
// backup is pinned and held in Preflight (before anything is fenced), the
// pin is carried through every phase onto the terminal status, the Job is
// built from the pinned backup, and the hold is released on the pass after
// the terminal status.
func TestRestoreHold_InPlace(t *testing.T) {
	for _, outcome := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run(string(outcome), func(t *testing.T) {
			ctx := context.Background()
			r, c, jobName := inPlaceHoldFixture(t, restoreHoldSeed())
			// One Preflight pass.
			if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			fg := fetchGroup(t, c, "lion")
			ref := fg.Status.RestoreInPlace.BackupRef
			if ref == nil || ref.Name != "seed" || ref.UID != "uid-seed" || ref.HoldID == "" {
				t.Fatalf("status.restoreInPlace = %+v, want the seed pinned in Preflight", fg.Status.RestoreInPlace)
			}
			if seed := fetchBackup(t, c, "seed"); seed.Annotations[restoreHoldKey(ref.HoldID)] != "lion" {
				t.Fatalf("hold not stamped in Preflight: %v", seed.Annotations)
			}
			if _, ok := restoreJobExists(t, c, jobName); ok {
				t.Fatal("restore Job created in Preflight")
			}

			fg = driveInPlace(t, r, c, func(s *v1alpha1.RestoreInPlaceStatus) bool { return s.JobName != "" })
			if got := fg.Status.RestoreInPlace.BackupRef; got == nil || *got != *ref {
				t.Fatalf("pin not carried to Restoring: %+v, was %+v", got, ref)
			}
			job, ok := restoreJobExists(t, c, jobName)
			if !ok || job.Annotations[v1alpha1.RestoreBackupUIDAnnotation] != "uid-seed" {
				t.Fatalf("restore Job missing or not built from the pinned backup: %+v", job)
			}

			endRestoreJob(t, c, job, outcome)
			fg = driveInPlace(t, r, c, inPlaceRestoreTerminal)
			if got := fg.Status.RestoreInPlace.BackupRef; got == nil || *got != *ref {
				t.Fatalf("terminal status lost the pin: %+v", fg.Status.RestoreInPlace)
			}
			seed := fetchBackup(t, c, "seed")
			if seed.Annotations[restoreHoldKey(ref.HoldID)] != "lion" {
				t.Fatal("hold released before the terminal status was persisted")
			}
			if names, err := artifactReaders(ctx, c, seed); err != nil || len(names) != 0 {
				t.Fatalf("readers after the restore ended = %v, %v", names, err)
			}
			if _, err := restartedGroupReconciler(c).reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 0 {
				t.Fatalf("hold not released after the restore ended: %v", keys)
			}
		})
	}
}

// A backup that cannot be held ends the run in Preflight: nothing is
// fenced, no Job is created, and the confirm counts as used, so a backup
// that turns up later under that name does not start the restore.
func TestRestoreHold_InPlace_RefusedInPreflight(t *testing.T) {
	for _, tc := range []struct {
		name    string
		seed    *v1alpha1.MysqlBackup
		message string
	}{
		{name: "gone", message: "the backup is gone"},
		{name: "being deleted", seed: terminatingBackup(restoreHoldSeed()), message: "the backup is being deleted"},
		{name: "not Succeeded", seed: func() *v1alpha1.MysqlBackup {
			b := restoreHoldSeed()
			b.Status.Phase = v1alpha1.BackupPhaseRunning
			return b
		}(), message: "the backup is not Succeeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			seed := tc.seed
			if seed == nil {
				seed = restoreHoldSeed()
				seed.Name = "unrelated"
			}
			r, c, jobName := inPlaceHoldFixture(t, seed)
			if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			fg := fetchGroup(t, c, "lion")
			st := fg.Status.RestoreInPlace
			if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, tc.message) || !strings.Contains(st.Message, "nothing was restored") {
				t.Fatalf("status.restoreInPlace = %+v, want Failed (%s)", st, tc.message)
			}
			if st.ConfirmTokenUsed != fg.Spec.RestoreInPlace.Confirm {
				t.Fatalf("confirmTokenUsed = %q, want the run's confirm consumed", st.ConfirmTokenUsed)
			}
			if !drainForEvent(r, v1alpha1.EventRestoreInPlaceBuildFailed) {
				t.Fatal("no RestoreInPlaceBuildFailed Event")
			}
			// Terminal on the same confirm: nothing starts again.
			if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
				t.Fatal(err)
			}
			if _, ok := restoreJobExists(t, c, jobName); ok {
				t.Fatal("restore Job created")
			}
			if got := fetchGroup(t, c, "lion").Status.RestoreInPlace.Phase; got != v1alpha1.RestoreInPlaceFailed {
				t.Fatalf("phase = %s after another pass, want Failed", got)
			}
		})
	}
}

// The backup pinned in Preflight is deleted (by hand, the hold removed) and
// created again under the same name before the Job exists: the run ends
// instead of loading the replacement.
func TestRestoreHold_InPlace_ReplacedBeforeJobCreation(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := inPlaceHoldFixture(t, restoreHoldSeed())
	if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, fetchBackup(t, c, "seed")); err != nil {
		t.Fatal(err)
	}
	replacement := restoreHoldSeed()
	replacement.UID = "uid-seed-recreated"
	if err := c.Create(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	fg := driveInPlace(t, r, c, inPlaceRestoreTerminal)
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "was replaced by another backup of the same name") {
		t.Fatalf("status.restoreInPlace = %+v, want Failed (replaced)", st)
	}
	if _, ok := restoreJobExists(t, c, jobName); ok {
		t.Fatal("restore Job created from the replacement")
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 0 {
		t.Fatalf("hold stamped on the replacement: %v", keys)
	}
}

// A new run after a terminal one pins again under a new hold ID, and a
// status that starts a new run never inherits the previous run's pin.
func TestRestoreHold_InPlace_NewRunTakesANewHold(t *testing.T) {
	ctx := context.Background()
	r, c, jobName := inPlaceHoldFixture(t, restoreHoldSeed())
	fg := driveInPlace(t, r, c, func(s *v1alpha1.RestoreInPlaceStatus) bool { return s.JobName != "" })
	first := fg.Status.RestoreInPlace.BackupRef.DeepCopy()
	job, _ := restoreJobExists(t, c, jobName)
	endRestoreJob(t, c, job, batchv1.JobComplete)
	driveInPlace(t, r, c, inPlaceRestoreTerminal)

	fg = fetchGroup(t, c, "lion")
	fg.Spec.RestoreInPlace.Confirm = "2026-04-18T14:00:00Z"
	if err := c.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	// The pass that accepts the new run releases the ended run's hold
	// first and starts a status without its pin.
	if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	fg = fetchGroup(t, c, "lion")
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlacePreflight || st.BackupRef != nil {
		t.Fatalf("new run's first status = %+v, want Preflight without the previous pin", st)
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 0 {
		t.Fatalf("ended run's hold kept: %v", keys)
	}
	if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	second := fetchGroup(t, c, "lion").Status.RestoreInPlace.BackupRef
	if second == nil || second.HoldID == first.HoldID || second.UID != "uid-seed" {
		t.Fatalf("second run pin = %+v, want a new hold ID (first %+v)", second, first)
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 1 || keys[0] != restoreHoldKey(second.HoldID) {
		t.Fatalf("holds = %v, want the second run's", keys)
	}
}

// The source is edited to another backup before the Job exists: the run
// reads the new one, so it pins and holds that and drops the first hold.
func TestRestoreHold_InPlace_SourceEditedBeforeJob(t *testing.T) {
	ctx := context.Background()
	other := restoreHoldSeed()
	other.Name, other.UID = "other", "uid-other"
	r, c, _ := inPlaceHoldFixture(t, restoreHoldSeed())
	if err := c.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileInPlaceRestore(ctx, fetchGroup(t, c, "lion")); err != nil {
		t.Fatal(err)
	}
	fg := fetchGroup(t, c, "lion")
	fg.Spec.RestoreInPlace.Source.BackupRef.Name = "other"
	if err := c.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fg = driveInPlace(t, r, c, func(s *v1alpha1.RestoreInPlaceStatus) bool { return s.JobName != "" })
	ref := fg.Status.RestoreInPlace.BackupRef
	if ref == nil || ref.Name != "other" || ref.UID != "uid-other" {
		t.Fatalf("pin = %+v, want the edited source", ref)
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "seed")); len(keys) != 0 {
		t.Fatalf("first source still held: %v", keys)
	}
	if keys := restoreHoldAnnotations(fetchBackup(t, c, "other")); len(keys) != 1 || keys[0] != restoreHoldKey(ref.HoldID) {
		t.Fatalf("edited source holds = %v", keys)
	}
}
