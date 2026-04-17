package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// --- pure helpers -------------------------------------------------------

func TestParseConfirmTimestamp_Valid(t *testing.T) {
	cases := []string{
		"2026-04-17T14:32:00Z",
		"2026-04-17T14:32:00+00:00",
		"2026-04-17T14:32:00-07:00",
	}
	for _, s := range cases {
		if _, err := parseConfirmTimestamp(s); err != nil {
			t.Errorf("%q: unexpected err: %v", s, err)
		}
	}
}

func TestParseConfirmTimestamp_Rejects(t *testing.T) {
	cases := []string{
		"",
		"not a date",
		"2026-04-17", // missing time component; RFC 3339 requires T.
	}
	for _, s := range cases {
		if _, err := parseConfirmTimestamp(s); err == nil {
			t.Errorf("%q: expected error, got nil", s)
		}
	}
}

func TestConfirmAdvances(t *testing.T) {
	type tc struct {
		name    string
		spec    string
		last    string
		want    bool
		wantErr bool
	}
	cases := []tc{
		{"empty-last", "2026-04-17T14:32:00Z", "", true, false},
		{"advances", "2026-04-17T14:32:00Z", "2026-04-17T14:00:00Z", true, false},
		{"equal", "2026-04-17T14:32:00Z", "2026-04-17T14:32:00Z", false, false},
		{"goes-backward", "2026-04-17T14:00:00Z", "2026-04-17T14:32:00Z", false, false},
		{"bad-spec", "garbage", "2026-04-17T14:00:00Z", false, true},
		{"bad-last-is-replaceable", "2026-04-17T14:00:00Z", "garbage", true, false},
	}
	for _, c := range cases {
		got, err := confirmAdvances(c.spec, c.last)
		if c.wantErr && err == nil {
			t.Errorf("%s: want error, got nil", c.name)
			continue
		}
		if !c.wantErr && err != nil {
			t.Errorf("%s: unexpected err: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestInPlaceRestoreScope(t *testing.T) {
	// No LoadOptions => full.
	scope, name := inPlaceRestoreScope(&v1alpha1.RestoreInPlaceSpec{})
	if scope != "full" || name != "" {
		t.Errorf("empty options: got (%q,%q)", scope, name)
	}

	// Empty includeSchemas => full.
	scope, name = inPlaceRestoreScope(&v1alpha1.RestoreInPlaceSpec{
		LoadOptions: &v1alpha1.LoadOptions{},
	})
	if scope != "full" || name != "" {
		t.Errorf("empty includeSchemas: got (%q,%q)", scope, name)
	}

	// Single-schema => schema:<name>.
	scope, name = inPlaceRestoreScope(&v1alpha1.RestoreInPlaceSpec{
		LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{"orders"}},
	})
	if scope != "schema:orders" || name != "orders" {
		t.Errorf("per-schema: got (%q,%q)", scope, name)
	}
}

func TestValidateInPlaceRestoreSpec(t *testing.T) {
	// Valid: full-instance from mysqlBackupRef.
	good := &v1alpha1.RestoreInPlaceSpec{
		Confirm: "2026-04-17T14:00:00Z",
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	if err := validateInPlaceRestoreSpec(good); err != nil {
		t.Errorf("valid spec rejected: %v", err)
	}

	// Invalid: confirm missing.
	bad1 := *good
	bad1.Confirm = ""
	if err := validateInPlaceRestoreSpec(&bad1); err == nil {
		t.Error("empty confirm should be rejected")
	}

	// Invalid: multiple includeSchemas.
	bad2 := *good
	bad2.LoadOptions = &v1alpha1.LoadOptions{IncludeSchemas: []string{"a", "b"}}
	if err := validateInPlaceRestoreSpec(&bad2); err == nil {
		t.Error("multi-schema includeSchemas should be rejected")
	} else if !strings.Contains(err.Error(), "at most one schema") {
		t.Errorf("unexpected error: %v", err)
	}

	// Invalid: no source set.
	bad3 := *good
	bad3.Source = v1alpha1.InitFromBackupSource{}
	if err := validateInPlaceRestoreSpec(&bad3); err == nil {
		t.Error("missing source should be rejected")
	}

	// Invalid: two sources set.
	bad4 := *good
	bad4.Source = v1alpha1.InitFromBackupSource{
		MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		S3: &v1alpha1.S3Storage{
			Bucket:            "x",
			CredentialsSecret: "y",
		},
	}
	if err := validateInPlaceRestoreSpec(&bad4); err == nil {
		t.Error("two sources should be rejected")
	}
}

func TestInPlaceRestoreInFlight(t *testing.T) {
	// No spec => not in flight.
	fg := &v1alpha1.MysqlFailoverGroup{}
	if inPlaceRestoreInFlight(fg) {
		t.Error("no spec should not be in flight")
	}

	// Spec set, no status => in flight (conservative pre-observation).
	fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "x"}
	if !inPlaceRestoreInFlight(fg) {
		t.Error("spec set but no status should be in flight")
	}

	// Succeeded / Failed / empty phase => not in flight.
	for _, terminal := range []v1alpha1.RestoreInPlacePhase{
		v1alpha1.RestoreInPlaceSucceeded,
		v1alpha1.RestoreInPlaceFailed,
		v1alpha1.RestoreInPlaceNone,
	} {
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: terminal}
		if inPlaceRestoreInFlight(fg) {
			t.Errorf("phase %q should not be in flight", terminal)
		}
	}

	// Active phases => in flight.
	for _, active := range []v1alpha1.RestoreInPlacePhase{
		v1alpha1.RestoreInPlacePreflight,
		v1alpha1.RestoreInPlaceFencing,
		v1alpha1.RestoreInPlaceRestoring,
		v1alpha1.RestoreInPlaceResuming,
	} {
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: active}
		if !inPlaceRestoreInFlight(fg) {
			t.Errorf("phase %q should be in flight", active)
		}
	}
}

func TestInPlaceRestoreFencesPrimaryService(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	// No spec: never fences.
	if inPlaceRestoreFencesPrimaryService(fg) {
		t.Error("no spec should not fence")
	}

	// Full-instance, no status yet: fences (conservative).
	fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
	if !inPlaceRestoreFencesPrimaryService(fg) {
		t.Error("full-instance with no status should fence")
	}

	// Full-instance, Fencing / Restoring / Resuming: fences.
	for _, p := range []v1alpha1.RestoreInPlacePhase{
		v1alpha1.RestoreInPlaceFencing,
		v1alpha1.RestoreInPlaceRestoring,
		v1alpha1.RestoreInPlaceResuming,
	} {
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: p}
		if !inPlaceRestoreFencesPrimaryService(fg) {
			t.Errorf("full-instance phase %q should fence", p)
		}
	}

	// Full-instance, terminal: does not fence (restore is done).
	for _, p := range []v1alpha1.RestoreInPlacePhase{
		v1alpha1.RestoreInPlaceSucceeded,
		v1alpha1.RestoreInPlaceFailed,
	} {
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: p}
		if inPlaceRestoreFencesPrimaryService(fg) {
			t.Errorf("full-instance terminal phase %q should not fence", p)
		}
	}

	// Per-schema: never fences.
	fg.Spec.RestoreInPlace.LoadOptions = &v1alpha1.LoadOptions{
		IncludeSchemas: []string{"orders"},
	}
	for _, p := range []v1alpha1.RestoreInPlacePhase{
		v1alpha1.RestoreInPlacePreflight,
		v1alpha1.RestoreInPlaceFencing,
		v1alpha1.RestoreInPlaceRestoring,
		v1alpha1.RestoreInPlaceResuming,
		v1alpha1.RestoreInPlaceSucceeded,
	} {
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: p}
		if inPlaceRestoreFencesPrimaryService(fg) {
			t.Errorf("per-schema phase %q should not fence", p)
		}
	}
}

// --- state machine integration tests ----------------------------------

// fgInPlaceRestore returns a MysqlFailoverGroup with status.activeSite
// set and a RestoreInPlace spec pointing at the same seed MysqlBackup
// the bootstrap tests use.
func fgInPlaceRestore(schema string) *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	fg.Status.ActiveSite = fg.Spec.Sites[0].Name
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: fg.Spec.Sites[0].Name, State: "writable"},
		{Name: fg.Spec.Sites[1].Name, State: "read-only"},
	}
	ripSpec := &v1alpha1.RestoreInPlaceSpec{
		Confirm: "2026-04-17T14:00:00Z",
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	if schema != "" {
		ripSpec.LoadOptions = &v1alpha1.LoadOptions{
			IncludeSchemas: []string{schema},
		}
	}
	fg.Spec.RestoreInPlace = ripSpec
	return fg
}

// primaryFencedPod returns a pod for the active site with role=fenced,
// mirroring what syncPodLabels produces once it observes the Fencing
// phase. Used to let inPlaceFencing advance past its wait-for-label
// gate in tests.
func primaryFencedPod(fgName, site string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(fgName, site) + "-abc",
			Namespace: "ns",
			Labels: map[string]string{
				labelAppName:  "mysql",
				labelInstance: fgName,
				labelSite:     site,
				labelRole:     "fenced",
			},
		},
	}
}

func TestReconcileInPlaceRestore_NoSpec_IsNoOp(t *testing.T) {
	fg := fgWithBackup()
	r, _ := newReconciler(fg)
	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d != 0 {
		t.Errorf("no spec: want 0 requeue, got %s", d)
	}
}

func TestReconcileInPlaceRestore_InvalidConfirmTerminates(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Spec.RestoreInPlace.Confirm = "not-a-timestamp"
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcileInPlaceRestore: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace == nil {
		t.Fatalf("status.restoreInPlace not stamped")
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Errorf("want Failed, got %s", fresh.Status.RestoreInPlace.Phase)
	}
	if !strings.Contains(fresh.Status.RestoreInPlace.Message, "RFC 3339") {
		t.Errorf("want RFC 3339 message, got %q", fresh.Status.RestoreInPlace.Message)
	}
}

func TestReconcileInPlaceRestore_FirstCallMovesToPreflight(t *testing.T) {
	fg := fgInPlaceRestore("")
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("first pass: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace == nil || fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Fatalf("want Preflight, got %+v", fresh.Status.RestoreInPlace)
	}
	if fresh.Status.RestoreInPlace.Scope != "full" {
		t.Errorf("want scope=full, got %q", fresh.Status.RestoreInPlace.Scope)
	}
}

func TestReconcileInPlaceRestore_Preflight_WaitsForWritableActive(t *testing.T) {
	fg := fgInPlaceRestore("")
	// Flip active to not-writable.
	fg.Status.Sites[0].State = "unreachable"
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:     v1alpha1.RestoreInPlacePreflight,
		Scope:     "full",
		StartTime: ptrTime(time.Now()),
	}

	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if d == 0 {
		t.Error("expected requeue when active site is not writable")
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Errorf("want stays Preflight, got %s", fresh.Status.RestoreInPlace.Phase)
	}
	if !strings.Contains(fresh.Status.RestoreInPlace.Message, "writable") {
		t.Errorf("want writable in message, got %q", fresh.Status.RestoreInPlace.Message)
	}
}

func TestReconcileInPlaceRestore_Preflight_TransitionsToFencing(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:     v1alpha1.RestoreInPlacePreflight,
		Scope:     "full",
		StartTime: ptrTime(time.Now()),
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Errorf("want Fencing, got %s", fresh.Status.RestoreInPlace.Phase)
	}
	if fresh.Status.RestoreInPlace.TargetSite == "" {
		t.Error("want TargetSite populated after Preflight")
	}
}

func TestReconcileInPlaceRestore_FullInstance_CreatesJobWithDropAndReset(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: fg.Spec.Sites[0].Name,
		Scope:      "full",
		StartTime:  ptrTime(time.Now()),
	}
	seed := succeededSeedBackup()
	// Fenced pod lets the builder read labels if it needs to; not
	// strictly required for Restoring phase but harmless.
	fencedPod := primaryFencedPod(fg.Name, fg.Spec.Sites[0].Name)
	r, c := newReconciler(fg, seed, dsnSecret(), fencedPod)

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name:      inPlaceRestoreJobName(fg.Name, fg.Spec.Sites[0].Name),
		Namespace: fg.Namespace,
	}, &job); err != nil {
		t.Fatalf("in-place restore Job not created: %v", err)
	}

	got := envMap(job.Spec.Template.Spec.Containers[0].Env)
	if got["BLOODRAVEN_DROP_ALL_USER_SCHEMAS"] != "1" {
		t.Errorf("full-instance: want BLOODRAVEN_DROP_ALL_USER_SCHEMAS=1, got %q",
			got["BLOODRAVEN_DROP_ALL_USER_SCHEMAS"])
	}
	if got["BLOODRAVEN_RESET_REPLICATION"] != "1" {
		t.Errorf("full-instance: want BLOODRAVEN_RESET_REPLICATION=1, got %q",
			got["BLOODRAVEN_RESET_REPLICATION"])
	}
	if got["BLOODRAVEN_DROP_SCHEMAS"] != "" {
		t.Errorf("full-instance should not set BLOODRAVEN_DROP_SCHEMAS, got %q",
			got["BLOODRAVEN_DROP_SCHEMAS"])
	}
}

func TestReconcileInPlaceRestore_PerSchema_CreatesJobWithDropAndSkipBinlogFalse(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: fg.Spec.Sites[0].Name,
		Scope:      "schema:orders",
		StartTime:  ptrTime(time.Now()),
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name:      inPlaceRestoreJobName(fg.Name, fg.Spec.Sites[0].Name),
		Namespace: fg.Namespace,
	}, &job); err != nil {
		t.Fatalf("in-place restore Job not created: %v", err)
	}

	got := envMap(job.Spec.Template.Spec.Containers[0].Env)
	if got["BLOODRAVEN_DROP_SCHEMAS"] != "orders" {
		t.Errorf("want BLOODRAVEN_DROP_SCHEMAS=orders, got %q", got["BLOODRAVEN_DROP_SCHEMAS"])
	}
	if got["BLOODRAVEN_DROP_ALL_USER_SCHEMAS"] != "" {
		t.Errorf("per-schema must not set BLOODRAVEN_DROP_ALL_USER_SCHEMAS, got %q",
			got["BLOODRAVEN_DROP_ALL_USER_SCHEMAS"])
	}
	// Per-schema must binlog the load so it replicates to the peer.
	loadOpts := got["BLOODRAVEN_LOAD_OPTIONS"]
	if !strings.Contains(loadOpts, `"skipBinlog":false`) {
		t.Errorf("per-schema should force skipBinlog=false, got load opts %q", loadOpts)
	}
}

func TestReconcileInPlaceRestore_TerminalSucceededStaysIdle(t *testing.T) {
	fg := fgInPlaceRestore("")
	// Prior run already consumed this confirm token.
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:            v1alpha1.RestoreInPlaceSucceeded,
		ConfirmTokenUsed: "2026-04-17T14:00:00Z",
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded {
		t.Errorf("terminal with equal confirm: want Succeeded, got %s", fresh.Status.RestoreInPlace.Phase)
	}
}

func TestReconcileInPlaceRestore_TerminalSucceededRearmsOnNewerConfirm(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Spec.RestoreInPlace.Confirm = "2026-04-17T15:00:00Z"
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:            v1alpha1.RestoreInPlaceSucceeded,
		ConfirmTokenUsed: "2026-04-17T14:00:00Z",
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Errorf("bumped confirm should re-arm to Preflight, got %s", fresh.Status.RestoreInPlace.Phase)
	}
}

func TestReconcileInPlaceRestore_PerSchema_ResumingSkipsRecloneAnnotation(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceResuming,
		TargetSite: fg.Spec.Sites[0].Name,
		Scope:      "schema:orders",
		StartTime:  ptrTime(time.Now()),
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded {
		t.Errorf("want Succeeded, got %s", fresh.Status.RestoreInPlace.Phase)
	}
	if got := fresh.GetAnnotations()[RecloneAnnotation]; got != "" {
		t.Errorf("per-schema should not set reclone annotation, got %q", got)
	}
}

func TestReconcileInPlaceRestore_FullInstance_ResumingSetsRecloneAnnotation(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceResuming,
		TargetSite: fg.Spec.Sites[0].Name,
		Scope:      "full",
		StartTime:  ptrTime(time.Now()),
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded {
		t.Errorf("want Succeeded, got %s", fresh.Status.RestoreInPlace.Phase)
	}
	peerSite := fg.Spec.Sites[1].Name
	if got := fresh.GetAnnotations()[RecloneAnnotation]; got != peerSite {
		t.Errorf("full-instance Resuming should set reclone=%s, got %q", peerSite, got)
	}
	if fresh.Status.RestoreInPlace.ConfirmTokenUsed != fg.Spec.RestoreInPlace.Confirm {
		t.Errorf("want ConfirmTokenUsed=%q, got %q",
			fg.Spec.RestoreInPlace.Confirm, fresh.Status.RestoreInPlace.ConfirmTokenUsed)
	}
}

// --- helpers ------------------------------------------------------------

func envMap(env []corev1.EnvVar) map[string]string {
	out := map[string]string{}
	for _, e := range env {
		out[e.Name] = e.Value
	}
	return out
}

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

// Reference imported types to avoid unused-import complaints if tests
// are pruned; silences go vet in minimal builds.
var _ = appsv1.Deployment{}
