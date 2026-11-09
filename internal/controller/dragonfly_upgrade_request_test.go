package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

func dragonflyStatefulSetForTest(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) *appsv1.StatefulSet {
	t.Helper()
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site)}, &sts); err != nil {
		t.Fatal(err)
	}
	return &sts
}

func dragonflyContainerHasEnv(sts *appsv1.StatefulSet, name string) bool {
	c, _ := dragonflyContainerFromTemplate(sts.Spec.Template.Spec)
	return slices.ContainsFunc(c.Env, func(e corev1.EnvVar) bool { return e.Name == name })
}

// K001: one edit that sets the image, SnapshotRestore and the snapshot
// block together, on StatefulSets rendered without a snapshot. The run
// must not SAVE or restart anything until the render has rolled the
// snapshot flags, credentials and service account out on the sites (on
// their running image) and the active pod runs that template; the active
// is then restarted on the target with those settings still in place.
func TestDragonflySnapshotUpgrade_CombinedEditWaitsForSnapshotConfig(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	fg.Spec.Dragonfly.Snapshot.CredentialsSecretName = "dragonfly-s3"
	before := fg.DeepCopy()
	before.Spec.Dragonfly.Image = dfSourceImage
	before.Spec.Dragonfly.UpgradeStrategy = v1alpha1.DragonflyUpgradeStrategyRolling
	before.Spec.Dragonfly.Snapshot = nil

	r, c := newReconciler(fg)
	seedDragonflyStatefulSets(t, r, c, before, bothSites(dfSourceImage))
	for _, site := range []string{"dc1", "dc2"} {
		if err := c.Create(ctx, dragonflyPodForTest(fg, site, dfSourceImage, true)); err != nil {
			t.Fatal(err)
		}
	}
	conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }
	pass := func() dragonflyUpgradeStep {
		t.Helper()
		step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
		if err != nil {
			t.Fatal(err)
		}
		return step
	}

	if step := pass(); step.hold != 0 || step.wait == 0 {
		t.Fatalf("before the render = %+v, want a wait that lets the render run", step)
	}
	if up := upgradeRecord(t, c, fg); up != nil {
		t.Fatalf("a run started on StatefulSets without the snapshot settings: %+v", up)
	}

	// The render applies the snapshot settings, keeping each live image;
	// the active's pod has not been replaced yet.
	renderDragonflyRolledOut(t, r, c, getFGByKey(t, c, fgNN(fg)))
	setDragonflyStatefulSetStatus(t, c, fg, "dc1", func(sts *appsv1.StatefulSet) {
		sts.Status.UpdatedReplicas, sts.Status.UpdateRevision = 0, "rev2"
	})
	active := dragonflyStatefulSetForTest(t, c, fg, "dc1")
	if img := dragonflyTemplateImage(active); img != dfSourceImage {
		t.Fatalf("the render moved the active to %q under SnapshotRestore", img)
	}
	if c, _ := dragonflyContainerFromTemplate(active.Spec.Template.Spec); !slices.Contains(c.Args, "--dir="+fg.Spec.Dragonfly.Snapshot.Dir) {
		t.Fatalf("the render did not install --dir: %v", c.Args)
	}
	step := pass()
	if step.hold != 0 || step.wait == 0 || upgradeRecord(t, c, fg) != nil {
		t.Fatalf("with the active pod on the old template = %+v, record %+v; want a wait", step, upgradeRecord(t, c, fg))
	}

	setDragonflyStatefulSetStatus(t, c, fg, "dc1", func(sts *appsv1.StatefulSet) { sts.Status = rolledOutDragonflyStatus(sts) })
	if step := pass(); step.hold == 0 {
		t.Fatalf("once rolled out = %+v, want a started run", step)
	}
	for i := 0; i < 3; i++ {
		pass()
	}
	up := upgradeRecord(t, c, fg)
	if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore || conn.saveCalls != 1 {
		t.Fatalf("record %+v after %d SAVEs, want one SAVE and the active restarted", up, conn.saveCalls)
	}
	active = dragonflyStatefulSetForTest(t, c, fg, "dc1")
	ac, _ := dragonflyContainerFromTemplate(active.Spec.Template.Spec)
	if ac.Image != dfTargetImage || !slices.Contains(ac.Args, "--dir="+fg.Spec.Dragonfly.Snapshot.Dir) ||
		!dragonflyContainerHasEnv(active, "AWS_ACCESS_KEY_ID") || active.Spec.Template.Spec.ServiceAccountName != "dragonfly-backup" {
		t.Fatalf("restarted active template image=%q args=%v sa=%q, want the target with the snapshot settings",
			ac.Image, ac.Args, active.Spec.Template.Spec.ServiceAccountName)
	}
}

// K002: a run whose record was lost after the active's template moved, but
// whose active is still loading its snapshot, resumes waiting for the
// active: no replica is moved, restarted or re-attached until the active
// answers as a loaded master.
func TestDragonflySnapshotUpgrade_LostRecordWaitsForLoadingActive(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	r, c := newReconciler(fg)
	seedDragonflyStatefulSets(t, r, c, fg, map[string]string{"dc1": dfTargetImage, "dc2": dfSourceImage})
	replicaPod := dragonflyPodForTest(fg, "dc2", dfSourceImage, true)
	for _, pod := range []*corev1.Pod{dragonflyPodForTest(fg, "dc1", dfTargetImage, true), replicaPod} {
		if err := c.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, persist: dragonfly.PersistenceInfo{Loading: true}}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }

	for i := 0; i < 3; i++ {
		if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
			t.Fatal(err)
		}
	}
	up := upgradeRecord(t, c, fg)
	if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore || !strings.Contains(up.Message, "loading") {
		t.Fatalf("record = %+v, want a resumed run waiting for the loading active", up)
	}
	if img := liveDragonflyImage(t, c, fg, "dc2"); img != dfSourceImage {
		t.Fatalf("replica StatefulSet moved to %q while the active was loading", img)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(replicaPod), &corev1.Pod{}); err != nil {
		t.Fatalf("replica pod restarted while the active was loading: %v", err)
	}
	if conn.replicaOfArgs != nil || conn.saveCalls != 0 {
		t.Fatalf("REPLICAOF %v / %d SAVEs on a resumed run with a loading active", conn.replicaOfArgs, conn.saveCalls)
	}

	conn.persist = dragonfly.PersistenceInfo{}
	for i := 0; i < 2; i++ {
		if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
			t.Fatal(err)
		}
	}
	if up := upgradeRecord(t, c, fg); up.Phase != v1alpha1.DragonflyUpgradePhaseReattachingReplicas {
		t.Fatalf("record = %+v, want ReattachingReplicas once the active loaded", up)
	}
	if img := liveDragonflyImage(t, c, fg, "dc2"); img != dfTargetImage {
		t.Fatalf("replica StatefulSet on %q after the active loaded, want the target", img)
	}
}

// K013/K057: a run in flight ends when spec.dragonfly changes under it,
// Dragonfly is disabled or removed, or the run outlives its deadline; it
// never re-creates a site from settings the user replaced. Once the active
// has moved, a spec change holds (traffic off) until the active has
// loaded.
func TestDragonflySnapshotUpgrade_InFlightRunFollowsSpec(t *testing.T) {
	type want struct {
		reason v1alpha1.DragonflyUpgradeReason
		hold   bool // held (active loading), not ended
	}
	for _, tc := range []struct {
		name          string
		phase         v1alpha1.DragonflyUpgradePhase
		mutate        func(*v1alpha1.MysqlFailoverGroup)
		loading       bool
		deleteActive  bool
		want          want
		trafficOnEnds bool
	}{
		{name: "image reverted before SAVE", phase: v1alpha1.DragonflyUpgradePhasePending,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Dragonfly.Image = dfSourceImage },
			want:   want{reason: v1alpha1.DragonflyUpgradeReasonSpecChanged}},
		{name: "Rolling while replicas reattach", phase: v1alpha1.DragonflyUpgradePhaseReattachingReplicas,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Dragonfly.UpgradeStrategy = v1alpha1.DragonflyUpgradeStrategyRolling
			},
			want: want{reason: v1alpha1.DragonflyUpgradeReasonSpecChanged}},
		{name: "Rolling while the active loads", phase: v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore, loading: true,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Dragonfly.UpgradeStrategy = v1alpha1.DragonflyUpgradeStrategyRolling
			},
			want: want{hold: true}},
		{name: "image moved on after the active loaded", phase: v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Dragonfly.Image = "dragonfly:v1.40.0" },
			want:   want{reason: v1alpha1.DragonflyUpgradeReasonSpecChanged}},
		{name: "snapshot dir changed with the active deleted", phase: v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore, deleteActive: true,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Dragonfly.Snapshot.Dir = "s3://elsewhere/prefix" },
			want:   want{reason: v1alpha1.DragonflyUpgradeReasonSpecChanged}},
		{name: "disabled", phase: v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore, loading: true,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Dragonfly.Enabled = false },
			want:   want{reason: v1alpha1.DragonflyUpgradeReasonDragonflyDisabled}},
		{name: "removed with a StatefulSet missing", phase: v1alpha1.DragonflyUpgradePhaseUpdatingActive, deleteActive: true,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Dragonfly = nil },
			want:   want{reason: v1alpha1.DragonflyUpgradeReasonDragonflyDisabled}},
		{name: "timed out while the active loads", phase: v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore, loading: true,
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
				old := metav1.NewTime(time.Now().Add(-dragonflyUpgradeRunTimeout - time.Minute))
				fg.Status.Dragonfly.Upgrade.StartTime = &old
			},
			want: want{reason: v1alpha1.DragonflyUpgradeReasonTimedOut}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, c, fg, conn := inFlightUpgradeFixture(t, tc.phase)
			if tc.loading {
				conn.persist = dragonfly.PersistenceInfo{Loading: true}
			}
			if tc.deleteActive {
				deleteDragonflySite(t, c, fg, "dc1")
			}
			// Write the mutation's status part, then its spec part (each
			// write reads the other half back from the server).
			live := getFGByKey(t, c, fgNN(fg))
			tc.mutate(live)
			if err := c.Status().Update(ctx, live); err != nil {
				t.Fatal(err)
			}
			live = getFGByKey(t, c, fgNN(fg))
			tc.mutate(live)
			if err := c.Update(ctx, live); err != nil {
				t.Fatal(err)
			}

			step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
			if err != nil {
				t.Fatalf("pass: %v", err)
			}
			up := upgradeRecord(t, c, fg)
			if tc.want.hold {
				if step.hold == 0 || up == nil || up.Phase != tc.phase || !strings.Contains(up.Message, "loaded its snapshot") {
					t.Fatalf("= (%+v) record %+v, want a hold at %s until the active loaded", step, up, tc.phase)
				}
				for _, p := range podsForSite(t, c, fg, "dc1") {
					if p.Labels[v1alpha1.DragonflyTrafficLabel] == dragonflyTrafficEnabled {
						t.Fatal("the loading active got its traffic back")
					}
				}
				conn.persist = dragonfly.PersistenceInfo{}
				if step, err = r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
					t.Fatal(err)
				}
				up = upgradeRecord(t, c, fg)
				tc.want.reason = v1alpha1.DragonflyUpgradeReasonSpecChanged
			}
			if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != tc.want.reason {
				t.Fatalf("record = %+v, want Failed/%s", up, tc.want.reason)
			}
			if step.hold != 0 {
				t.Fatalf("an ended run still holds the reconcile: %+v", step)
			}
			if tc.deleteActive {
				var sts appsv1.StatefulSet
				err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, "dc1")}, &sts)
				if err == nil {
					t.Fatal("the run re-created the active StatefulSet after spec.dragonfly changed")
				}
			}
		})
	}
}

// V002: a spec change that ends a run while the active may be loading is
// decided by probing the active with the connection settings its pod runs,
// not the replaced ones. The run holds the render, so the pod (and its
// site Service) still has its original auth Secret and port; dialing it
// with the new spec's would fail against a fully restored pod and keep the
// run, and its traffic hold, until the run's deadline.
func TestDragonflySnapshotUpgrade_SpecChangeProbesTheInstalledSettings(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.MysqlFailoverGroup)
	}{
		{name: "auth Secret replaced", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Dragonfly.Auth = &v1alpha1.DragonflyAuthSpec{PasswordSecretKeyRef: v1alpha1.DragonflyPasswordSecretKeyRef{Name: "df-auth-new", Key: "password"}}
		}},
		{name: "client port changed", mutate: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Dragonfly.Port = 6390 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r, c, fg, conn := inFlightUpgradeFixture(t, v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore)
			for name, pw := range map[string]string{"df-auth-old": "old-pw", "df-auth-new": "new-pw"} {
				if err := c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: fg.Namespace, Name: name},
					Data: map[string][]byte{"password": []byte(pw)}}); err != nil {
					t.Fatal(err)
				}
			}
			// The active pod as the render started it: the old Secret and
			// the default client port.
			var pod corev1.Pod
			if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, "dc1") + "-0"}, &pod); err != nil {
				t.Fatal(err)
			}
			pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "client", ContainerPort: 6379}}
			pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: DragonflyAuthEnvVar, ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "df-auth-old"}, Key: "password"}}}}
			if err := c.Update(ctx, &pod); err != nil {
				t.Fatal(err)
			}
			wantAddr := dragonflySiteServiceName(fg.Name, "dc1") + "." + fg.Namespace + ".svc.cluster.local:6379"
			r.dragonflyConnector = func(_ context.Context, addr, password string) (DragonflyConnection, error) {
				if addr == wantAddr && password == "old-pw" {
					return conn, nil
				}
				return &fakeDragonflyConn{infoErr: errors.New("WRONGPASS"), persistErr: errors.New("WRONGPASS")}, nil
			}

			live := getFGByKey(t, c, fgNN(fg))
			tc.mutate(live)
			if err := c.Update(ctx, live); err != nil {
				t.Fatal(err)
			}
			step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
			if err != nil {
				t.Fatal(err)
			}
			up := upgradeRecord(t, c, fg)
			if step.hold != 0 || up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != v1alpha1.DragonflyUpgradeReasonSpecChanged {
				t.Fatalf("= (%+v) record %+v, want the loaded active probed on its own settings and the run ended SpecChanged", step, up)
			}
			for _, p := range podsForSite(t, c, fg, "dc1") {
				if p.Labels[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled {
					t.Fatal("the loaded active did not get its traffic back")
				}
			}
		})
	}
}

// V003: a run that reaches its deadline while the active it restarted is
// still loading ends without putting that Dragonfly behind the Services:
// its traffic stays off, the label sweep after the run does not give it
// back (or make it master) until the site answers as loaded, and the
// Failed record that gates the sweep is not removed while it holds.
func TestDragonflySnapshotUpgrade_TimedOutLoadingActiveStaysOutOfTraffic(t *testing.T) {
	ctx := context.Background()
	r, c, fg, conn := inFlightUpgradeFixture(t, v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore)
	conn.persist = dragonfly.PersistenceInfo{Loading: true}
	// The restarted active comes from the template: role replica, traffic on.
	var pod corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, "dc1") + "-0"}, &pod); err != nil {
		t.Fatal(err)
	}
	pod.Labels[v1alpha1.DragonflyRoleLabel] = "replica"
	pod.Labels[v1alpha1.DragonflyTrafficLabel] = dragonflyTrafficEnabled
	if err := c.Update(ctx, &pod); err != nil {
		t.Fatal(err)
	}
	live := getFGByKey(t, c, fgNN(fg))
	old := metav1.NewTime(time.Now().Add(-dragonflyUpgradeRunTimeout - time.Minute))
	live.Status.Dragonfly.Upgrade.StartTime = &old
	if err := c.Status().Update(ctx, live); err != nil {
		t.Fatal(err)
	}

	step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
	if err != nil {
		t.Fatal(err)
	}
	up := upgradeRecord(t, c, fg)
	if step.hold != 0 || up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != v1alpha1.DragonflyUpgradeReasonTimedOut ||
		!strings.Contains(up.Message, "traffic stays off") {
		t.Fatalf("= (%+v) record %+v, want Failed/TimedOut saying the traffic stays off", step, up)
	}
	serving := func() bool {
		for _, p := range podsForSite(t, c, fg, "dc1") {
			if p.Labels[v1alpha1.DragonflyTrafficLabel] == dragonflyTrafficEnabled {
				return true
			}
		}
		return false
	}
	if serving() {
		t.Fatal("the timed-out run put the loading active back in traffic")
	}
	for range 2 {
		if err := r.syncDragonflyPodLabels(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
			t.Fatal(err)
		}
	}
	if serving() {
		t.Fatal("the label sweep gave the loading active its traffic back")
	}

	// Every site on one image would remove the Failed record; it stays
	// while the traffic it gates is held.
	dc2 := dragonflyStatefulSetForTest(t, c, fg, "dc2")
	dc2.Spec.Template.Spec.Containers[0].Image = dfTargetImage
	if err := c.Update(ctx, dc2); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
		t.Fatal(err)
	}
	if up := upgradeRecord(t, c, fg); up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed {
		t.Fatalf("record = %+v, want the Failed record kept while the active's traffic is held", up)
	}

	conn.persist = dragonfly.PersistenceInfo{}
	if err := r.syncDragonflyPodLabels(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
		t.Fatal(err)
	}
	for _, p := range podsForSite(t, c, fg, "dc1") {
		if p.Labels[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled || p.Labels[v1alpha1.DragonflyRoleLabel] != "master" {
			t.Fatalf("labels %v once the active loaded, want master with traffic", p.Labels)
		}
	}
	if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
		t.Fatal(err)
	}
	if up := upgradeRecord(t, c, fg); up != nil {
		t.Fatalf("record = %+v, want it removed once the traffic is back and every site runs one image", up)
	}
}

func podsForSite(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) []corev1.Pod {
	t.Helper()
	pods, err := listDragonflyPodsForSite(context.Background(), c, fg, site)
	if err != nil {
		t.Fatal(err)
	}
	return pods
}

// K056: a StatefulSet refusal recorded earlier is cleared by an in-flight
// run's uncached look once the StatefulSet is gone or matches, as the
// render (held while the run is in flight) would clear it.
func TestDragonflySnapshotUpgrade_InFlightRunClearsStaleSelectorRefusal(t *testing.T) {
	r, c, fg, _ := inFlightUpgradeFixture(t, v1alpha1.DragonflyUpgradePhaseReattachingReplicas)
	name := dragonflyStatefulSetName(fg.Name, "dc2")
	r.Runner.SetGroupRefusal(fg, workloadRefusalKey("StatefulSet", name), v1alpha1.ReasonWorkloadSelectorMismatch, "refused earlier")
	if _, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), getFGByKey(t, c, fgNN(fg))); err != nil {
		t.Fatal(err)
	}
	if up := upgradeRecord(t, c, fg); up == nil || !dragonflyUpgradeInFlight(up) {
		t.Fatalf("record = %+v, want the run still in flight", up)
	}
	if keys := r.Runner.GroupRefusalKeys(fgNN(fg), workloadRefusalKey("StatefulSet", "")); len(keys) != 0 {
		t.Fatalf("refusals %v outlived the matching StatefulSet", keys)
	}
}

// K058: the deferral memo is per incarnation and dropped with the group, so
// a group re-created under the same name gets its first Deferred Event.
func TestDragonflyUpgradeDeferralMemoPerIncarnation(t *testing.T) {
	fg := fgWithDragonflySnapshotUpgrade()
	fg.UID = "uid-1"
	r, _ := newReconciler(fg)
	rec := record.NewFakeRecorder(8)
	r.Recorder = rec
	ctx := context.Background()
	r.noteDragonflyUpgradeDeferral(ctx, fg, "cause")
	r.noteDragonflyUpgradeDeferral(ctx, fg, "cause")
	again := fg.DeepCopy()
	again.UID = "uid-2"
	r.noteDragonflyUpgradeDeferral(ctx, again, "cause")
	if n := countMatching(drainEvents(rec), "DragonflyUpgradeDeferred"); n != 2 {
		t.Fatalf("%d Deferred Events, want one per incarnation", n)
	}
	// A reconcile that finds the group gone drops the memo.
	r.dragonflyUpgradeDeferNote[types.NamespacedName{Namespace: fg.Namespace, Name: "gone"}] = "x"
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: fg.Namespace, Name: "gone"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.dragonflyUpgradeDeferNote[types.NamespacedName{Namespace: fg.Namespace, Name: "gone"}]; ok {
		t.Fatal("the deferral memo outlived the deleted group")
	}
}
