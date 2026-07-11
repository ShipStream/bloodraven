package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

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
	// A pure validation rejection never executed, so it must NOT consume the
	// confirm: confirmTokenUsed stays empty. This preserves the invalid-
	// timestamp behavior — the still-invalid confirm keeps failing
	// confirmAdvances so the status holds, and once the user supplies a valid
	// confirm the request proceeds.
	if got := fresh.Status.RestoreInPlace.ConfirmTokenUsed; got != "" {
		t.Errorf("a validation rejection must leave confirmTokenUsed empty, got %q", got)
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
	// Newly created Jobs must be stamped with the request's confirm token
	// so a later reconcile can tell them apart from a leftover Job.
	if got := job.Annotations[restoreInPlaceConfirmAnnotation]; got != fg.Spec.RestoreInPlace.Confirm {
		t.Errorf("created Job confirm annotation = %q, want %q", got, fg.Spec.RestoreInPlace.Confirm)
	}
}

func TestInPlaceRestoreJobIsForConfirm(t *testing.T) {
	confirm := "2026-04-17T14:00:00Z"
	if inPlaceRestoreJobIsForConfirm(nil, confirm) {
		t.Error("nil job must not match")
	}
	j := &batchv1.Job{}
	if inPlaceRestoreJobIsForConfirm(j, "") {
		t.Error("empty confirm must not match")
	}
	// No annotation (leftover predating the annotation, or from another
	// request) is always treated as stale.
	if inPlaceRestoreJobIsForConfirm(j, confirm) {
		t.Error("unannotated job must be treated as stale")
	}
	// An older confirm is stale.
	j.Annotations = map[string]string{restoreInPlaceConfirmAnnotation: "2026-04-17T13:00:00Z"}
	if inPlaceRestoreJobIsForConfirm(j, confirm) {
		t.Error("older-confirm job must be treated as stale")
	}
	// Exact match is this request's Job.
	j.Annotations[restoreInPlaceConfirmAnnotation] = confirm
	if !inPlaceRestoreJobIsForConfirm(j, confirm) {
		t.Error("matching-confirm job must be honored")
	}
}

// inPlaceRestoreJobObj builds a fixed-name in-place restore Job for the
// active site, optionally stamped with a confirm token and/or a terminal
// JobFailed condition (mirroring a Job that hit backoffLimit).
func inPlaceRestoreJobObj(fg *v1alpha1.MysqlFailoverGroup, confirm string, failed bool) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      inPlaceRestoreJobName(fg.Name, fg.Spec.Sites[0].Name),
			Namespace: fg.Namespace,
		},
	}
	if confirm != "" {
		job.Annotations = map[string]string{restoreInPlaceConfirmAnnotation: confirm}
	}
	if failed {
		job.Status.Conditions = []batchv1.JobCondition{{
			Type:    batchv1.JobFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "BackoffLimitExceeded",
			Message: "Job has reached the specified backoff limit",
		}}
	}
	return job
}

// A leftover terminal Job from a PRIOR restore request (older/absent
// confirm) that happens to share the fixed Job name must be deleted, not
// inherited as the fresh request's outcome. This is the exact live
// scenario 36 failure: the fresh confirmed request landed on a retained
// Failed Job and was marked Failed without ever running.
func TestReconcileInPlaceRestore_StaleJobNotInherited(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: site,
		Scope:      "schema:orders",
		StartTime:  ptrTime(time.Now()),
	}
	// Stale Job: older confirm than the current spec, terminally Failed.
	stale := inPlaceRestoreJobObj(fg, "2026-04-17T13:00:00Z", true)
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret(), stale)

	// Reconcile #1: the stale Job is cleared, not attributed to this request.
	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile #1: %v", err)
	}
	if d <= 0 {
		t.Errorf("want a positive requeue after clearing the stale Job, got %s", d)
	}
	jobKey := types.NamespacedName{Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}
	var gone batchv1.Job
	if gerr := c.Get(context.Background(), jobKey, &gone); !apierrors.IsNotFound(gerr) {
		t.Fatalf("stale Job should have been deleted, got err=%v", gerr)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring {
		t.Fatalf("fresh request must not inherit the stale terminal phase; want Restoring, got %s",
			fg.Status.RestoreInPlace.Phase)
	}

	// Reconcile #2: with the stale Job gone, a clean Job is created for THIS
	// request and stamped with the current confirm.
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile #2: %v", err)
	}
	var created batchv1.Job
	if err := c.Get(context.Background(), jobKey, &created); err != nil {
		t.Fatalf("clean Job not created on reconcile #2: %v", err)
	}
	if got := created.Annotations[restoreInPlaceConfirmAnnotation]; got != fg.Spec.RestoreInPlace.Confirm {
		t.Errorf("recreated Job confirm annotation = %q, want %q", got, fg.Spec.RestoreInPlace.Confirm)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || fg.Status.RestoreInPlace.JobName != created.Name {
		t.Errorf("want Restoring with jobName=%s, got phase=%s jobName=%s",
			created.Name, fg.Status.RestoreInPlace.Phase, fg.Status.RestoreInPlace.JobName)
	}
}

// A terminal Job created for THIS request (matching confirm) is honored:
// its Failed phase is the request's real outcome and it is not deleted.
func TestReconcileInPlaceRestore_MatchingFailedJobIsHonored(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: site,
		Scope:      "schema:orders",
		JobName:    inPlaceRestoreJobName(fg.Name, site),
		StartTime:  ptrTime(time.Now()),
	}
	own := inPlaceRestoreJobObj(fg, fg.Spec.RestoreInPlace.Confirm, true)
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret(), own)

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Errorf("this request's own failed Job must be honored as Failed, got %s",
			fg.Status.RestoreInPlace.Phase)
	}
	// A failed-but-executed restore must record the confirm it consumed, so
	// the terminal Failed holds and the operator does not silently re-arm the
	// destructive restore on the same confirm (the live scenario-36 defect).
	if got := fg.Status.RestoreInPlace.ConfirmTokenUsed; got != fg.Spec.RestoreInPlace.Confirm {
		t.Errorf("failed restore must record the executed confirm; confirmTokenUsed=%q want %q",
			got, fg.Spec.RestoreInPlace.Confirm)
	}
	var still batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}, &still); err != nil {
		t.Errorf("matching Job must not be deleted: %v", err)
	}
}

// Changing spec.confirm while a destructive restore Job is RUNNING must never
// delete that Job. It may be mid-DROP or mid-load on the live primary; killing
// the pod would leave the schema half-gone and start a second loader over the
// wreckage. The in-flight run is honored to completion; the new confirm waits.
func TestReconcileInPlaceRestore_RunningJobSurvivesConfirmChange(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	site := fg.Spec.Sites[0].Name
	accepted := fg.Spec.RestoreInPlace.Confirm // the confirm the Job runs under
	// The user patches confirm while the restore Job is running.
	fg.Spec.RestoreInPlace.Confirm = "2026-04-17T15:00:00Z"
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: site,
		Scope:      "schema:orders",
		JobName:    inPlaceRestoreJobName(fg.Name, site),
		StartTime:  ptrTime(time.Now()),
	}
	// No terminal condition: the Job is still dropping/loading right now.
	running := inPlaceRestoreJobObj(fg, accepted, false)
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret(), running)

	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if d <= 0 {
		t.Errorf("want a positive requeue while the accepted restore Job runs, got %s", d)
	}

	var live batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}, &live); err != nil {
		t.Fatalf("a RUNNING restore Job must not be deleted when confirm changes: %v", err)
	}
	if got := live.Annotations[restoreInPlaceConfirmAnnotation]; got != accepted {
		t.Errorf("the in-flight Job must keep running under its own confirm; annotation=%q want %q", got, accepted)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring {
		t.Errorf("phase=%s, want Restoring while the accepted Job finishes", fg.Status.RestoreInPlace.Phase)
	}
	if !strings.Contains(fg.Status.RestoreInPlace.Message, "in-flight") {
		t.Errorf("status should explain the deferral, got %q", fg.Status.RestoreInPlace.Message)
	}
}

// A transient failure refreshing the restore credentials Secret must not fail
// a run whose Job is already in flight: the pod already holds the Secret, and
// stamping terminal Failed there would ALSO unfreeze the topology underneath a
// live DROP/load. It stays terminal only when no Job exists (nothing executed).
func TestReconcileInPlaceRestore_CredsErrorDoesNotFailAnInFlightJob(t *testing.T) {
	// Shared fixture: status is Restoring and the restore credentials cannot be
	// resolved (no dsnSecret seeded), so ensureRestoreCredsSecret fails.
	newFG := func() *v1alpha1.MysqlFailoverGroup {
		fg := fgInPlaceRestore("orders")
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
			Phase:      v1alpha1.RestoreInPlaceRestoring,
			TargetSite: fg.Spec.Sites[0].Name,
			Scope:      "schema:orders",
			JobName:    inPlaceRestoreJobName(fg.Name, fg.Spec.Sites[0].Name),
			StartTime:  ptrTime(time.Now()),
		}
		return fg
	}

	// Control: with no Job, the same creds failure IS terminal — nothing has
	// executed, so failing the request is correct (and proves the fixture
	// really does make ensureRestoreCredsSecret fail).
	noJob := newFG()
	rNoJob, _ := newReconciler(noJob, succeededSeedBackup())
	if _, err := rNoJob.reconcileInPlaceRestore(context.Background(), noJob); err != nil {
		t.Fatalf("reconcile (no job): %v", err)
	}
	if noJob.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Fatalf("with no Job, a creds failure must be terminal; phase=%s want Failed",
			noJob.Status.RestoreInPlace.Phase)
	}
	if !strings.Contains(noJob.Status.RestoreInPlace.Message, "restore creds") {
		t.Fatalf("want a creds failure message, got %q", noJob.Status.RestoreInPlace.Message)
	}

	// The real case: the same failure with a destructive Job in flight must NOT
	// fail the run — the pod already holds the Secret, and stamping terminal
	// would unfreeze the topology underneath a live DROP/load.
	fg := newFG()
	site := fg.Spec.Sites[0].Name
	running := inPlaceRestoreJobObj(fg, fg.Spec.RestoreInPlace.Confirm, false)
	r, c := newReconciler(fg, succeededSeedBackup(), running)

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile (job in flight): %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring {
		t.Errorf("a creds refresh error must not fail an in-flight restore; phase=%s want Restoring",
			fg.Status.RestoreInPlace.Phase)
	}
	var still batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: inPlaceRestoreJobName(fg.Name, site), Namespace: fg.Namespace}, &still); err != nil {
		t.Errorf("the in-flight Job must survive a creds refresh error: %v", err)
	}
	if !drainForEvent(r, "RestoreInPlaceCredsRefreshFailed") {
		t.Error("expected a RestoreInPlaceCredsRefreshFailed event surfacing the non-fatal refresh failure")
	}
}

// drainForEvent reports whether the reconciler's fake recorder saw an event
// with the given reason.
func drainForEvent(r *MysqlFailoverGroupReconciler, reason string) bool {
	rec, ok := r.Recorder.(*record.FakeRecorder)
	if !ok {
		return false
	}
	for {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, reason) {
				return true
			}
		default:
			return false
		}
	}
}

// The terminal token is the one the JOB ran with, not whatever spec.confirm
// happens to say when it finishes. Otherwise a confirm changed mid-run is
// marked consumed by a run that never requested it, and the second (real)
// restore silently never happens. The newer confirm must re-arm its own run.
func TestReconcileInPlaceRestore_TerminalTokenComesFromTheJob(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	site := fg.Spec.Sites[0].Name
	ran := fg.Spec.RestoreInPlace.Confirm // the confirm the Job actually ran with
	newer := "2026-04-17T15:00:00Z"
	// The user changed confirm while that Job was running.
	fg.Spec.RestoreInPlace.Confirm = newer
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: site,
		Scope:      "schema:orders",
		JobName:    inPlaceRestoreJobName(fg.Name, site),
		StartTime:  ptrTime(time.Now()),
	}
	done := inPlaceRestoreJobObj(fg, ran, false)
	done.Status.Conditions = []batchv1.JobCondition{{
		Type:   batchv1.JobComplete,
		Status: corev1.ConditionTrue,
	}}
	seed := succeededSeedBackup()
	r, _ := newReconciler(fg, seed, dsnSecret(), done)

	// #1 Restoring → Resuming, carrying the Job's confirm.
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile #1: %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceResuming {
		t.Fatalf("want Resuming after the Job completed, got %s", fg.Status.RestoreInPlace.Phase)
	}

	// #2 Resuming → Succeeded, stamped against the executed confirm.
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile #2: %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded {
		t.Fatalf("want Succeeded, got %s", fg.Status.RestoreInPlace.Phase)
	}
	if got := fg.Status.RestoreInPlace.ConfirmTokenUsed; got != ran {
		t.Fatalf("confirmTokenUsed=%q want %q (the confirm the Job ran with, not the mid-run change)", got, ran)
	}

	// #3 The newer confirm was never consumed, so it re-arms its own run.
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile #3: %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Errorf("the mid-run confirm change must re-arm a fresh run; phase=%s want Preflight",
			fg.Status.RestoreInPlace.Phase)
	}
}

// Same binding on the failure path: a Job that ran under an older confirm and
// failed records THAT confirm, so the newer one can still re-arm a retry.
func TestReconcileInPlaceRestore_FailedJobRecordsTheJobsConfirm(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	site := fg.Spec.Sites[0].Name
	ran := fg.Spec.RestoreInPlace.Confirm
	fg.Spec.RestoreInPlace.Confirm = "2026-04-17T15:00:00Z" // changed mid-run
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: site,
		Scope:      "schema:orders",
		JobName:    inPlaceRestoreJobName(fg.Name, site),
		StartTime:  ptrTime(time.Now()),
	}
	failed := inPlaceRestoreJobObj(fg, ran, true)
	seed := succeededSeedBackup()
	r, _ := newReconciler(fg, seed, dsnSecret(), failed)

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile #1: %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Fatalf("want Failed, got %s", fg.Status.RestoreInPlace.Phase)
	}
	if got := fg.Status.RestoreInPlace.ConfirmTokenUsed; got != ran {
		t.Fatalf("confirmTokenUsed=%q want %q (the confirm the failed Job ran with)", got, ran)
	}
	// The newer confirm is untouched by that failure and re-arms a retry.
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile #2: %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Errorf("newer confirm must re-arm after the older run failed; phase=%s want Preflight",
			fg.Status.RestoreInPlace.Phase)
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

// The end-to-end of the live scenario-36 re-arm defect: this request's own
// restore Job runs and fails (backoffLimit). The reconciler must attribute
// Failed to this request AND record the executed confirm, then HOLD terminal
// on the same unchanged confirm — it must not silently re-arm and re-run the
// destructive restore (Failed→Preflight→…). Only a newer confirm may retry.
func TestReconcileInPlaceRestore_FailedJobRecordsConfirmAndHolds(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	site := fg.Spec.Sites[0].Name
	confirm := fg.Spec.RestoreInPlace.Confirm
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: site,
		Scope:      "schema:orders",
		JobName:    inPlaceRestoreJobName(fg.Name, site),
		StartTime:  ptrTime(time.Now()),
	}
	own := inPlaceRestoreJobObj(fg, confirm, true)
	seed := succeededSeedBackup()
	r, _ := newReconciler(fg, seed, dsnSecret(), own)

	// Reconcile #1: the Job failed → Failed, executed confirm recorded.
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile #1: %v", err)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Fatalf("want Failed after the Job failed, got %s", fg.Status.RestoreInPlace.Phase)
	}
	if got := fg.Status.RestoreInPlace.ConfirmTokenUsed; got != confirm {
		t.Fatalf("failed restore must record the executed confirm; confirmTokenUsed=%q want %q", got, confirm)
	}

	// Reconcile #2: same unchanged confirm must hold — no destructive re-arm.
	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile #2: %v", err)
	}
	if d != 0 {
		t.Errorf("held terminal Failed should be idle (0 requeue), got %s", d)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Errorf("terminal Failed must hold on the same confirm (the live re-arm bug), got %s",
			fg.Status.RestoreInPlace.Phase)
	}
}

// A terminal Failed that recorded its confirm must stay idle when the spec
// confirm is unchanged — the mirror of the Succeeded idle case, and the direct
// regression for the auto-re-arm-on-failure defect.
func TestReconcileInPlaceRestore_TerminalFailedStaysIdleOnSameConfirm(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:            v1alpha1.RestoreInPlaceFailed,
		Scope:            "schema:orders",
		TargetSite:       fg.Spec.Sites[0].Name,
		ConfirmTokenUsed: fg.Spec.RestoreInPlace.Confirm,
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if d != 0 {
		t.Errorf("terminal Failed on the same confirm should be idle (0 requeue), got %s", d)
	}

	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Errorf("terminal Failed with equal confirm must NOT re-arm; want Failed, got %s",
			fresh.Status.RestoreInPlace.Phase)
	}
}

// A terminal Failed re-arms only when the user advances confirm to a strictly
// newer RFC 3339 timestamp.
func TestReconcileInPlaceRestore_TerminalFailedRearmsOnNewerConfirm(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	fg.Spec.RestoreInPlace.Confirm = "2026-04-17T15:00:00Z"
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:            v1alpha1.RestoreInPlaceFailed,
		Scope:            "schema:orders",
		TargetSite:       fg.Spec.Sites[0].Name,
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
		t.Errorf("a newer confirm must re-arm a terminal Failed to Preflight, got %s",
			fresh.Status.RestoreInPlace.Phase)
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
