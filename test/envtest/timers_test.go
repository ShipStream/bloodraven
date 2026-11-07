//go:build envtest

package envtest

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// E8 (A-34..A-39, A-41): timers are bounded Durations, a valid zero is kept,
// numeric fields carry their unit and sign, and user-written instants are
// pattern-checked metav1.Time values.

// e8VerificationPointInTime is a valid pointInTime block for mode: Timestamp
// carries a stopTime, Latest none.
func e8VerificationPointInTime(mode v1alpha1.PointInTimeMode) *v1alpha1.PointInTimeVerificationSpec {
	pit := &v1alpha1.PointInTimeVerificationSpec{Mode: mode}
	if mode == v1alpha1.PointInTimeModeTimestamp {
		pit.StopTime = &metav1.Time{Time: time.Date(2026, 4, 15, 9, 30, 0, 0, time.UTC)}
	}
	return pit
}

func e8Duration(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

// e8Verification is a minimal MysqlBackupVerification of group lion.
func e8Verification(ns, name string) *v1alpha1.MysqlBackupVerification {
	return &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       v1alpha1.MysqlBackupVerificationSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
	}
}

// e8Bound is one bounded Duration field: set writes d into a fresh object.
type e8Bound struct {
	field    string
	min, max time.Duration
	build    func(ns, name string, d time.Duration) client.Object
}

func e8FG(set func(*v1alpha1.MysqlFailoverGroup, time.Duration)) func(ns, name string, d time.Duration) client.Object {
	return func(ns, name string, d time.Duration) client.Object {
		fg := newVerificationFG(ns)
		fg.Name = name
		set(fg, d)
		return fg
	}
}

// TestEnvtest_DurationBounds: every spec Duration refuses zero, a negative
// value, and values just outside its A-38 bounds, and admits both bounds
// (WE-Q3 for the planned-failover timers). TestDurationFieldsCarryCELBounds
// checks the same set is complete against the Go types.
func TestEnvtest_DurationBounds(t *testing.T) {
	ns := createNamespace(t, "e8-bounds")
	bounds := []e8Bound{
		{field: "pollInterval", min: time.Second, max: 5 * time.Minute, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.PollInterval = e8Duration(d)
		})},
		{field: "connectionDrainTimeout", min: time.Second, max: 10 * time.Minute, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.ConnectionDrainTimeout = e8Duration(d)
		})},
		{field: "failoverCooldown", min: 30 * time.Second, max: 24 * time.Hour, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.FailoverCooldown = e8Duration(d)
		})},
		{field: "cloneTimeout", min: time.Minute, max: 24 * time.Hour, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.CloneTimeout = e8Duration(d)
		})},
		{field: "sidecar.peerCheckInterval", min: time.Second, max: time.Minute, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			// Keep the 3x lease rule satisfied at every probed value.
			fg.Spec.Sidecar.LeaseTimeout = e8Duration(10 * time.Minute)
			fg.Spec.Sidecar.PeerCheckInterval = e8Duration(d)
		})},
		{field: "sidecar.leaseTimeout", min: 3 * time.Second, max: 10 * time.Minute, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.Sidecar.PeerCheckInterval = e8Duration(time.Second)
			fg.Spec.Sidecar.LeaseTimeout = e8Duration(d)
		})},
		{field: "archivePollInterval", min: 5 * time.Second, max: time.Hour, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3", ArchivePollInterval: e8Duration(d)}
		})},
		{field: "retry.initialBackoff", min: time.Second, max: time.Hour, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.Backup.Retry = &v1alpha1.BackupRetrySpec{MaxAttempts: 2, InitialBackoff: e8Duration(d), MaxBackoff: e8Duration(24 * time.Hour)}
		})},
		{field: "retry.maxBackoff", min: time.Second, max: 24 * time.Hour, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.Backup.Retry = &v1alpha1.BackupRetrySpec{MaxAttempts: 2, InitialBackoff: e8Duration(time.Second), MaxBackoff: e8Duration(d)}
		})},
		{field: "keyring.escrowTimeout", min: 30 * time.Second, max: time.Hour, build: func(ns, name string, d time.Duration) client.Object {
			fg := newEncryptionFG(ns, name)
			fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true, Keyring: &v1alpha1.KeyringSpec{EscrowTimeout: e8Duration(d)}}
			return fg
		}},
		{field: "plannedFailover.maxLagWait", min: v1alpha1.PlannedFailoverMaxLagWaitMin, max: v1alpha1.PlannedFailoverMaxLagWaitMax, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{MaxLagWait: e8Duration(d)}
		})},
		{field: "plannedFailover.drainTimeout", min: time.Second, max: 10 * time.Minute, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{DrainTimeout: e8Duration(d)}
		})},
		{field: "dragonfly.plannedFailover.maxSyncWait", min: time.Second, max: 10 * time.Minute, build: e8FG(func(fg *v1alpha1.MysqlFailoverGroup, d time.Duration) {
			fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true, Image: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0",
				PlannedFailover: &v1alpha1.DragonflyPlannedFailoverSpec{MaxSyncWait: e8Duration(d)}}
		})},
		{field: "sanityCheck.timeout", min: time.Second, max: time.Hour, build: func(ns, name string, d time.Duration) client.Object {
			v := e8Verification(ns, name)
			v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1", Timeout: e8Duration(d)}
			return v
		}},
		{field: "discoveryInterval", min: 30 * time.Second, max: 24 * time.Hour, build: func(ns, name string, d time.Duration) client.Object {
			sc := minimalEnvtestStandby(name, ns)
			sc.Spec.Freshness = &v1alpha1.StandbyFreshnessSpec{DiscoveryInterval: e8Duration(d)}
			return sc
		}},
	}
	n := 0
	for _, b := range bounds {
		for _, tc := range []struct {
			d  time.Duration
			ok bool
		}{
			{d: 0}, {d: -b.min}, {d: b.min - time.Millisecond}, {d: b.min, ok: true}, {d: b.max, ok: true}, {d: b.max + time.Second},
		} {
			n++
			obj := b.build(ns, fmt.Sprintf("bound-%d", n), tc.d)
			err := k8sClient.Create(ctx, obj)
			if tc.ok && err != nil {
				t.Errorf("%s=%s refused: %v", b.field, tc.d, err)
			}
			if !tc.ok {
				wantRefused(t, fmt.Sprintf("%s=%s", b.field, tc.d), err, b.field)
			}
		}
	}

	// maxBackoff must not be shorter than initialBackoff.
	fg := newVerificationFG(ns)
	fg.Name = "retry-inverted"
	fg.Spec.Backup.Retry = &v1alpha1.BackupRetrySpec{MaxAttempts: 2, InitialBackoff: e8Duration(10 * time.Minute), MaxBackoff: e8Duration(time.Minute)}
	wantRefused(t, "maxBackoff < initialBackoff", k8sClient.Create(ctx, fg), "retry.maxBackoff must be at least retry.initialBackoff")

	// An unset field takes the documented default.
	fg = newVerificationFG(ns)
	fg.Name = "defaults"
	fg.Spec.Backup.Retry = &v1alpha1.BackupRetrySpec{MaxAttempts: 2}
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	got := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), got); err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string][2]any{
		"cloneTimeout":                {got.Spec.CloneTimeout, time.Hour},
		"failoverCooldown":            {got.Spec.FailoverCooldown, 5 * time.Minute},
		"retry.initialBackoff":        {got.Spec.Backup.Retry.InitialBackoff, time.Minute},
		"retry.maxBackoff":            {got.Spec.Backup.Retry.MaxBackoff, 30 * time.Minute},
		"plannedFailover.maxLagWait":  {got.Spec.PlannedFailover.MaxLagWait, 5 * time.Minute},
		"plannedFailover.drainTimout": {got.Spec.PlannedFailover.DrainTimeout, 30 * time.Second},
	} {
		d, _ := pair[0].(*metav1.Duration)
		if d == nil || d.Duration != pair[1].(time.Duration) {
			t.Errorf("%s defaulted to %v, want %v", name, pair[0], pair[1])
		}
	}
	if got.Spec.DNS.TTLSeconds != 60 {
		t.Errorf("dns.ttlSeconds = %d, want 60", got.Spec.DNS.TTLSeconds)
	}
}

// TestEnvtest_ZeroIsKept (A-37): a defaulted scalar whose zero is valid is a
// pointer, so an explicit 0 survives admission and the typed round trip;
// only an absent field takes the default.
func TestEnvtest_ZeroIsKept(t *testing.T) {
	ns := createNamespace(t, "e8-zero")
	fg := newVerificationFG(ns)
	fg.Name = "zeros"
	fg.Spec.Replication = &v1alpha1.ReplicationSpec{MaxLagSeconds: new(int64(0))}
	fg.Spec.Backup.BackoffLimit = new(int32(0))
	fg.Spec.Backup.MaxLagSecondsForSource = new(int64(0))
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	got := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), got); err != nil {
		t.Fatal(err)
	}
	if p := got.Spec.Replication.MaxLagSeconds; p == nil || *p != 0 || got.Spec.EffectiveMaxLagSeconds() != 0 {
		t.Errorf("replication.maxLagSeconds = %v, effective %d; want 0 kept", p, got.Spec.EffectiveMaxLagSeconds())
	}
	if p := got.Spec.Backup.BackoffLimit; p == nil || *p != 0 {
		t.Errorf("backup.backoffLimit = %v, want 0 kept", p)
	}
	if p := got.Spec.Backup.MaxLagSecondsForSource; p == nil || *p != 0 {
		t.Errorf("backup.maxLagSecondsForSource = %v, want 0 kept", p)
	}

	fg = newVerificationFG(ns)
	fg.Name = "defaults"
	fg.Spec.Replication = &v1alpha1.ReplicationSpec{}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), got); err != nil {
		t.Fatal(err)
	}
	if p := got.Spec.Replication.MaxLagSeconds; p == nil || *p != 300 {
		t.Errorf("replication.maxLagSeconds default = %v, want 300", p)
	}
	if p := got.Spec.Backup.BackoffLimit; p == nil || *p != 2 {
		t.Errorf("backup.backoffLimit default = %v, want 2", p)
	}
	if p := got.Spec.Backup.MaxLagSecondsForSource; p == nil || *p != 300 {
		t.Errorf("backup.maxLagSecondsForSource default = %v, want 300", p)
	}

	// F-54/F-11: ttlSecondsAfterFinished has no default on the MBV or on
	// the profile mirror; 0 stays 0 and absent stays absent.
	for _, tc := range []struct {
		name string
		ttl  *int32
	}{{name: "ttl-unset"}, {name: "ttl-zero", ttl: new(int32(0))}} {
		v := e8Verification(ns, tc.name)
		v.Spec.TTLSecondsAfterFinished = tc.ttl
		if err := k8sClient.Create(ctx, v); err != nil {
			t.Fatal(err)
		}
		stored := inertFieldsGet(t, ns, tc.name, "MysqlBackupVerification")
		val, found, _ := unstructured.NestedFieldNoCopy(stored.Object, "spec", "ttlSecondsAfterFinished")
		if (tc.ttl == nil) == found || (found && fmt.Sprint(val) != "0") {
			t.Errorf("%s: stored ttlSecondsAfterFinished = %v (found %v)", tc.name, val, found)
		}
	}
	fg = newVerificationFG(ns)
	fg.Name = "profile-ttl"
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 5 * * *"}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), got); err != nil {
		t.Fatal(err)
	}
	if ttl := got.Spec.Backup.Profiles[0].Verification.TTLSecondsAfterFinished; ttl != nil {
		t.Errorf("profile verification.ttlSecondsAfterFinished defaulted to %d, want unset", *ttl)
	}
}

// TestEnvtest_NumericSigns: maxMemory must be a positive Quantity (F-12),
// proactorThreads at least 1, startingDeadlineSeconds and minResult not
// negative.
func TestEnvtest_NumericSigns(t *testing.T) {
	ns := createNamespace(t, "e8-signs")
	for i, tc := range []struct {
		mem any
		ok  bool
	}{
		{mem: "-1Gi"}, {mem: "0"}, {mem: int64(0)}, {mem: "-1"}, {mem: "512Mi", ok: true}, {mem: int64(536870912), ok: true}, {mem: "1.5Gi", ok: true},
	} {
		fg := newVerificationFG(ns)
		fg.Name = fmt.Sprintf("mem-%d", i)
		fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true, Image: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0"}
		u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
		inertFieldsSet(t, u, tc.mem, "spec", "dragonfly", "maxMemory")
		err := k8sClient.Create(ctx, u)
		if tc.ok && err != nil {
			t.Errorf("maxMemory=%v refused: %v", tc.mem, err)
		}
		if !tc.ok {
			wantRefused(t, fmt.Sprintf("maxMemory=%v", tc.mem), err, "maxMemory")
		}
	}
	fg := newVerificationFG(ns)
	fg.Name = "threads-zero"
	fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true, Image: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0", ProactorThreads: new(int32(0))}
	wantRefused(t, "proactorThreads=0", k8sClient.Create(ctx, fg), "proactorThreads")
	fg = newVerificationFG(ns)
	fg.Name = "threads-ok"
	q := resource.MustParse("512Mi")
	fg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true, Image: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.0", ProactorThreads: new(int32(1)), MaxMemory: &q}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("proactorThreads=1 maxMemory=512Mi: %v", err)
	}

	fg = newVerificationFG(ns)
	fg.Name = "deadline-negative"
	fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{{Name: "nightly", ProfileName: "nightly-s3", Schedule: "0 2 * * *", StartingDeadlineSeconds: new(int64(-1))}}
	wantRefused(t, "schedules[].startingDeadlineSeconds=-1", k8sClient.Create(ctx, fg), "startingDeadlineSeconds")
	fg = newVerificationFG(ns)
	fg.Name = "verify-deadline-negative"
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{Enabled: true, Schedule: "0 5 * * *", StartingDeadlineSeconds: new(int64(-1))}
	wantRefused(t, "verification.startingDeadlineSeconds=-1", k8sClient.Create(ctx, fg), "startingDeadlineSeconds")

	v := e8Verification(ns, "min-result-negative")
	v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1", MinResult: new(int64(-1))}
	wantRefused(t, "sanityCheck.minResult=-1", k8sClient.Create(ctx, v), "minResult")
	v = e8Verification(ns, "sanity-default")
	v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1"}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatal(err)
	}
	gotV := &v1alpha1.MysqlBackupVerification{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(v), gotV); err != nil {
		t.Fatal(err)
	}
	if to := gotV.Spec.SanityCheck.Timeout; to == nil || to.Duration != time.Minute || gotV.Spec.SanityCheck.MinResult != nil {
		t.Errorf("sanityCheck defaults: timeout %v minResult %v, want 60s and unset", to, gotV.Spec.SanityCheck.MinResult)
	}

	// status.sanityCheck.outcome is a closed enum.
	gotV.Status.SanityCheck = &v1alpha1.SanityCheckResult{Outcome: "Ran"}
	wantRefused(t, "status.sanityCheck.outcome=Ran", k8sClient.Status().Update(ctx, gotV), "outcome")
}

// TestEnvtest_StopTimePattern (A-39, OPUS-R3-03, F-02): both stopTime fields
// admit exactly the strings metav1.Time decodes at second precision: RFC
// 3339, an uppercase T and Z, an explicit zone with a bounded offset, and
// no fraction. Every admitted value decodes with metav1.Time.UnmarshalJSON,
// so a gap between the apiserver's date-time check and Go's parser fails
// here instead of in an informer.
func TestEnvtest_StopTimePattern(t *testing.T) {
	ns := createNamespace(t, "e8-stop-time")
	stop := metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	for i, tc := range []struct {
		stop string
		ok   bool
	}{
		{stop: "2030-01-01T00:00:00Z", ok: true},
		{stop: "2030-01-01T02:00:00+02:00", ok: true},
		{stop: "2030-01-01T02:00:00-23:59", ok: true},
		{stop: "2030-01-01T00:00:00"},
		{stop: "2030-01-01 00:00:00"},
		{stop: "2030-01-01 00:00:00Z"},
		{stop: "2030-01-01t00:00:00Z"},
		{stop: "2030-01-01T00:00:00z"},
		{stop: "2030-01-01T00:00:00.5Z"},
		{stop: "2030-01-01T00:00:00.123456Z"},
		{stop: "2030-01-01T02:00:00.000001+02:00"},
		{stop: "2030-01-01T00:00:00,5Z"},
		{stop: "2030-01-01T00:00:00+25:00"},
		{stop: "2030-01-01T00:00:00+99:00"},
		{stop: "2030-01-01T00:00:00+05:99"},
		{stop: "2030-01-01T00:00:00+0200"},
		{stop: "2030-01-01T00:00:00Z "},
		{stop: "yesterday"},
	} {
		name := fmt.Sprintf("stop-%d", i)
		fg := pitrRestoreFG(ns, &v1alpha1.PointInTimeSpec{StopTime: stop, ArchiveProfileName: "nightly-s3"})
		fg.Name = name
		u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
		inertFieldsSet(t, u, tc.stop, "spec", "initFromBackup", "pointInTime", "stopTime")
		err := k8sClient.Create(ctx, u)
		if tc.ok != (err == nil) {
			t.Errorf("restore stopTime %q: ok=%v, err=%v", tc.stop, tc.ok, err)
		}
		if err == nil {
			e8DecodesStopTime(t, "MysqlFailoverGroup", ns, name, "spec", "initFromBackup", "pointInTime", "stopTime")
		}

		v := e8Verification(ns, name)
		v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: &stop}
		uv := inertFieldsObject(t, v, "MysqlBackupVerification")
		inertFieldsSet(t, uv, tc.stop, "spec", "pointInTime", "stopTime")
		err = k8sClient.Create(ctx, uv)
		if tc.ok != (err == nil) {
			t.Errorf("verification stopTime %q: ok=%v, err=%v", tc.stop, tc.ok, err)
		}
		if err == nil {
			e8DecodesStopTime(t, "MysqlBackupVerification", ns, name, "spec", "pointInTime", "stopTime")
		}
	}

	// mode: Timestamp needs stopTime and mode: Latest forbids it.
	for name, pit := range map[string]*v1alpha1.PointInTimeVerificationSpec{
		"timestamp-without-stop": {Mode: v1alpha1.PointInTimeModeTimestamp},
		"latest-with-stop":       {Mode: v1alpha1.PointInTimeModeLatest, StopTime: &stop},
	} {
		v := e8Verification(ns, name)
		v.Spec.PointInTime = pit
		wantRefused(t, name, k8sClient.Create(ctx, v), "pointInTime.stopTime is required when mode is Timestamp and forbidden when mode is Latest")
	}
	for _, mode := range []v1alpha1.PointInTimeMode{v1alpha1.PointInTimeModeLatest, v1alpha1.PointInTimeModeTimestamp} {
		v := e8Verification(ns, "mode-"+strings.ToLower(string(mode)))
		v.Spec.PointInTime = e8VerificationPointInTime(mode)
		if err := k8sClient.Create(ctx, v); err != nil {
			t.Errorf("mode %s: %v", mode, err)
		}
	}

	// The restore's stopTime is required.
	fg := pitrRestoreFG(ns, &v1alpha1.PointInTimeSpec{StopTime: stop, ArchiveProfileName: "nightly-s3"})
	fg.Name = "restore-without-stop"
	u := inertFieldsObject(t, fg, "MysqlFailoverGroup")
	unstructured.RemoveNestedField(u.Object, "spec", "initFromBackup", "pointInTime", "stopTime")
	wantRefused(t, "restore without stopTime", k8sClient.Create(ctx, u), "stopTime")

	// The installed schema carries the pattern on both properties.
	for _, path := range [][]string{
		{"mysqlfailovergroups.shipstream.io", "spec", "initFromBackup", "pointInTime", "stopTime"},
		{"mysqlbackupverifications.shipstream.io", "spec", "pointInTime", "stopTime"},
	} {
		prop := installedSchemaProperty(t, path[0], path[1:]...)
		if prop.Pattern != v1alpha1StopTimePattern || prop.Type != "string" {
			t.Errorf("%s %v: type %q pattern %q", path[0], path[1:], prop.Type, prop.Pattern)
		}
	}
}

// v1alpha1StopTimePattern is A-39's RFC 3339 pattern, as both stopTime
// markers spell it.
const v1alpha1StopTimePattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`

// e8DecodesStopTime reads the stored stopTime string and decodes it the way
// the typed client does.
func e8DecodesStopTime(t *testing.T, kind, ns, name string, path ...string) {
	t.Helper()
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind(kind))
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, stored); err != nil {
		t.Fatal(err)
	}
	raw, _, _ := unstructured.NestedString(stored.Object, path...)
	b, _ := json.Marshal(raw)
	var mt metav1.Time
	if err := mt.UnmarshalJSON(b); err != nil {
		t.Errorf("%s %s: admitted stopTime %q does not decode: %v", kind, name, raw, err)
	}
	if mt.Nanosecond() != 0 {
		t.Errorf("%s %s: admitted stopTime %q has a fraction", kind, name, raw)
	}
}
