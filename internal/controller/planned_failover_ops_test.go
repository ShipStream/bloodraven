package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

const (
	opsUUIDA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" // dc1, the source
	opsUUIDB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" // dc2, the target
	opsUUIDC = "cccccccc-cccc-cccc-cccc-cccccccccccc" // a foreign writer
	opsF     = opsUUIDA + ":1-10"                     // the fenced set
)

// opsMySQL is a MySQL server with MySQL's read_only / super_read_only
// coupling, a replication channel that RESET REPLICA ALL removes, a
// server_uuid, and a log of every mutating statement.
type opsMySQL struct {
	mysql.Checker // unused methods panic

	mu       sync.Mutex
	name     string
	uuid     string
	ro, sro  bool
	repl     *mysql.ReplicaStatus
	gtid     string
	down     bool
	failStmt map[string]error
	stmts    []string
	// beforeSRO runs (without mu) just before SET super_read_only=OFF.
	beforeSRO func()
	// sroReadErr, when set, fails SELECT @@super_read_only only (a loaded
	// server answering the single read_only probe but timing out on the
	// identity read).
	sroReadErr error
}

func (m *opsMySQL) log(stmt string) error {
	m.stmts = append(m.stmts, stmt)
	if m.down {
		return errors.New("connection refused")
	}
	return m.failStmt[stmt]
}

func (m *opsMySQL) statements() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.stmts)
}

func (m *opsMySQL) read() error {
	if m.down {
		return errors.New("connection refused")
	}
	return nil
}

func (m *opsMySQL) CheckReadOnly(context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ro, m.read()
}
func (m *opsMySQL) CheckSuperReadOnly(context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sroReadErr != nil {
		return false, m.sroReadErr
	}
	return m.sro, m.read()
}
func (m *opsMySQL) ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return mysql.GTIDSnapshot{ServerUUID: m.uuid, GtidExecuted: m.gtid}, m.read()
}
func (m *opsMySQL) GetGtidExecuted(context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gtid, m.read()
}
func (m *opsMySQL) ShowReplicaStatus(context.Context) (*mysql.ReplicaStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.repl == nil {
		return nil, m.read()
	}
	rs := *m.repl
	return &rs, m.read()
}
func (m *opsMySQL) SetSuperReadOnly(_ context.Context, on bool) error {
	if !on && m.beforeSRO != nil {
		m.beforeSRO()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.log("sro=" + onOffForOps(on)); err != nil {
		return err
	}
	m.sro = on
	if on {
		m.ro = true
	}
	return nil
}
func (m *opsMySQL) SetReadOnly(_ context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.log("ro=" + onOffForOps(on)); err != nil {
		return err
	}
	m.ro = on
	if !on {
		m.sro = false
	}
	return nil
}
func (m *opsMySQL) StopReplica(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.log("stop"); err != nil {
		return err
	}
	if m.repl != nil {
		m.repl.IORunning, m.repl.SQLRunning = false, false
	}
	return nil
}
func (m *opsMySQL) ResetReplicaAll(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.log("reset"); err != nil {
		return err
	}
	m.repl = nil
	return nil
}
func (m *opsMySQL) StartReplica(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.log("start"); err != nil {
		return err
	}
	if m.repl != nil {
		m.repl.IORunning, m.repl.SQLRunning = true, true
	}
	return nil
}
func (m *opsMySQL) ChangeReplicationSource(_ context.Context, opts mysql.ReplicationSourceOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.log("change:" + opts.Host); err != nil {
		return err
	}
	m.repl = &mysql.ReplicaStatus{SourceHost: opts.Host}
	return nil
}
func (m *opsMySQL) KillAppConnections(context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, m.log("kill")
}
func (m *opsMySQL) PinSession(context.Context) (mysql.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.read(); err != nil {
		return nil, err
	}
	return mysql.CheckerSession(m), nil
}
func (m *opsMySQL) Close() error { return nil }

func onOffForOps(on bool) string {
	if on {
		return "ON"
	}
	return "OFF"
}

// opsRegistry is a ManagerRegistry over one manager.
type opsRegistry struct {
	mu       sync.Mutex
	current  *TopologyManager
	retiring []*TopologyManager
	slot     *PlannedClaimSlot
	leader   context.Context
}

func (r *opsRegistry) Snapshot(types.NamespacedName) RegistrySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RegistrySnapshot{Current: r.current, Retiring: slices.Clone(r.retiring), Slot: r.slot}
}
func (r *opsRegistry) Slot(types.NamespacedName) *PlannedClaimSlot { return r.slot }
func (r *opsRegistry) LeaderContext() context.Context {
	if r.leader == nil {
		return context.Background()
	}
	return r.leader
}

// opsFixture: dc1 the writable source, dc2 a healthy replica of it, and
// optionally dc3 (role given), all wired to one claim slot.
type opsFixture struct {
	tm       *TopologyManager
	reg      *opsRegistry
	topo     *runnerPlannedTopology
	nn       types.NamespacedName
	clk      *clock.FakeClock
	src, tgt *opsMySQL
	third    *opsMySQL
	ref      PlannedRunRef
}

func newOpsFixture(t *testing.T, thirdRole state.SiteRole) *opsFixture {
	t.Helper()
	f := &opsFixture{
		nn:  types.NamespacedName{Namespace: "ns", Name: "lion"},
		clk: clock.NewFakeClock(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)),
		src: &opsMySQL{name: "dc1", uuid: opsUUIDA, gtid: opsF},
		tgt: &opsMySQL{name: "dc2", uuid: opsUUIDB, gtid: opsF, ro: true, sro: true,
			repl: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}},
	}
	sites := []SiteTopologyConfig{
		{Name: "dc1", Role: state.SiteRolePrimaryCandidate, Host: "mysql-dc1", LoadBalancerIP: "1.1.1.1"},
		{Name: "dc2", Role: state.SiteRolePrimaryCandidate, Host: "mysql-dc2", LoadBalancerIP: "2.2.2.2"},
	}
	checkers := []mysql.Checker{f.src, f.tgt}
	if thirdRole != "" {
		f.third = &opsMySQL{name: "dc3", uuid: opsUUIDC, gtid: opsF, ro: true, sro: true,
			repl: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
		sites = append(sites, SiteTopologyConfig{Name: "dc3", Role: thirdRole, Host: "mysql-dc3", LoadBalancerIP: "3.3.3.3"})
		checkers = append(checkers, f.third)
	}
	cfg := TopologyConfig{Name: "lion", Namespace: "ns", Sites: sites, PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2}
	f.tm = NewTopologyManagerWithClock(cfg, checkers, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger(), f.clk)
	f.tm.failoverCooldown = 0
	f.tm.mu.Lock()
	f.tm.sites[0].state = state.StateWritable
	for i := 1; i < len(f.tm.sites); i++ {
		f.tm.sites[i].state = state.StateReadOnly
	}
	f.tm.mu.Unlock()
	f.tm.SetReadyForSim()
	f.reg = &opsRegistry{current: f.tm, slot: &PlannedClaimSlot{}}
	WireRegisteredManager(f.tm, ManagerWiring{Slot: f.reg.slot})
	f.topo = newRunnerPlannedTopology(f.reg)
	f.ref = PlannedRunRef{Group: f.nn, RunID: "run-1", Source: "dc1", Target: "dc2"}
	return f
}

// hold installs the run's claim (as a committed Fencing write leaves it).
func (f *opsFixture) hold(needsReprove bool) {
	f.reg.slot.hydrate(plannedClaim{mode: claimModeRun, runID: f.ref.RunID, source: f.ref.Source, target: f.ref.Target,
		needsReprove: needsReprove})
}

// fenced puts dc1 behind the run's fence.
func (f *opsFixture) fenced() {
	f.src.mu.Lock()
	f.src.ro, f.src.sro = true, true
	f.src.mu.Unlock()
}

// opsRun is the run's block in phase.
func opsRun(phase v1alpha1.PlannedFailoverPhase) *v1alpha1.PlannedFailoverStatus {
	gtid := opsF
	t0 := metav1.NewTime(time.Date(2026, 11, 4, 11, 59, 0, 0, time.UTC))
	pf := &v1alpha1.PlannedFailoverStatus{
		RunID: "run-1", Revision: 3, Phase: phase, TargetSite: "dc2", SourceSite: "dc1", StartTime: t0, ClaimTime: &t0,
		ClaimBaseline: &v1alpha1.FailoverRecordRef{},
		Fence:         &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceApplied, IntentTime: t0, SourceServerUUID: opsUUIDA, GtidSet: &gtid},
	}
	if phase == v1alpha1.PlannedFailoverPhasePromoting {
		pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionIntended, IntentTime: t0, TargetServerUUID: opsUUIDB}
	}
	return pf
}

func opsGroup(pf *v1alpha1.PlannedFailoverStatus) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Status.PlannedFailover = pf
	return fg
}

func opsSet(t *testing.T, s string) mysql.GTIDSet {
	t.Helper()
	g, err := mysql.ParseGTIDSet(s)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// --- §7.1 classification -------------------------------------------------

func TestClassifyPromotionStage_EveryRow(t *testing.T) {
	F := opsSet(t, opsF)
	running := &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}
	stopped := &mysql.ReplicaStatus{}
	own := opsF + "," + opsUUIDB + ":1-2"
	foreign := opsF + "," + opsUUIDC + ":1"
	missing := opsUUIDA + ":1-5"
	cases := []struct {
		name     string
		live     promotionLive
		memo     bool // memo proves no SRO clear was sent
		stage    PromotionStage
		kind     PromoteOutcomeKind
		reason   v1alpha1.PlannedFailoverReason
		terminal v1alpha1.PlannedFailoverOutcome
	}{
		{"uuid changed, memo", promotionLive{uuid: opsUUIDC, readOnly: true, gtidRaw: opsF}, true, PromotionStageTargetReplaced, PromoteAborted, v1alpha1.PlannedFailoverReasonTargetChanged, ""},
		{"uuid changed, no memo", promotionLive{uuid: opsUUIDC, readOnly: true, gtidRaw: opsF}, false, PromotionStageInconsistent, PromoteInconsistent, v1alpha1.PlannedFailoverReasonExecuteFailed, ""},
		{"writable with channel, memo", promotionLive{uuid: opsUUIDB, repl: running, gtidRaw: opsF}, true, PromotionStageTargetReplaced, PromoteAborted, v1alpha1.PlannedFailoverReasonForeignWritable, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{"writable with channel, no memo", promotionLive{uuid: opsUUIDB, repl: running, gtidRaw: opsF}, false, PromotionStageInconsistent, PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, ""},
		{"writable, own extras", promotionLive{uuid: opsUUIDB, gtidRaw: own}, false, PromotionStageWritable, 0, "", ""},
		{"writable, foreign extras", promotionLive{uuid: opsUUIDB, gtidRaw: foreign}, false, PromotionStageInconsistent, PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, ""},
		{"writable, missing F", promotionLive{uuid: opsUUIDB, gtidRaw: missing}, false, PromotionStageInconsistent, PromoteInconsistent, v1alpha1.PlannedFailoverReasonExecuteFailed, ""},
		{"writable, memo proves no write", promotionLive{uuid: opsUUIDB, gtidRaw: own}, true, PromotionStageTargetReplaced, PromoteAborted, v1alpha1.PlannedFailoverReasonForeignWritable, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{"read-only, channel running", promotionLive{uuid: opsUUIDB, readOnly: true, repl: running, gtidRaw: opsF}, false, PromotionStageNotStarted, 0, "", ""},
		{"read-only, channel stopped", promotionLive{uuid: opsUUIDB, readOnly: true, repl: stopped, gtidRaw: opsF}, false, PromotionStageReplicaStopped, 0, "", ""},
		{"read-only, no channel, = F", promotionLive{uuid: opsUUIDB, readOnly: true, gtidRaw: opsF}, false, PromotionStageReplicaReset, 0, "", ""},
		{"read-only, own extras, memo", promotionLive{uuid: opsUUIDB, readOnly: true, gtidRaw: own}, true, PromotionStageTargetReplaced, PromoteAborted, v1alpha1.PlannedFailoverReasonAuthorityChanged, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{"read-only, own extras, no memo", promotionLive{uuid: opsUUIDB, readOnly: true, gtidRaw: own}, false, PromotionStageWasWritable, 0, "", ""},
		{"read-only, missing F, memo", promotionLive{uuid: opsUUIDB, readOnly: true, gtidRaw: missing}, true, PromotionStageTargetReplaced, PromoteAborted, v1alpha1.PlannedFailoverReasonTargetChanged, ""},
		{"read-only, missing F, no memo", promotionLive{uuid: opsUUIDB, readOnly: true, gtidRaw: missing}, false, PromotionStageInconsistent, PromoteInconsistent, v1alpha1.PlannedFailoverReasonExecuteFailed, ""},
		{"read-only, foreign extras", promotionLive{uuid: opsUUIDB, readOnly: true, gtidRaw: foreign}, false, PromotionStageInconsistent, PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.live.gtid = opsSet(t, tc.live.gtidRaw)
			stage, verdict := classifyPromotionStage(tc.live, F, opsUUIDB, tc.memo)
			if stage != tc.stage {
				t.Fatalf("stage = %q, want %q", stage, tc.stage)
			}
			switch {
			case tc.reason == "" && verdict != nil:
				t.Fatalf("verdict = %+v, want a resumable stage", *verdict)
			case tc.reason != "" && verdict == nil:
				t.Fatal("no verdict")
			case tc.reason != "" && (verdict.Kind != tc.kind || verdict.Reason != tc.reason || verdict.Terminal != tc.terminal):
				t.Fatalf("verdict = %+v, want kind %d reason %q terminal %q", *verdict, tc.kind, tc.reason, tc.terminal)
			}
		})
	}
}

// --- staged statements ---------------------------------------------------

// TestPromoteVerifiedFrom_StatementSequence: every resumable stage sends
// exactly its row's statements, the gate runs immediately before
// super_read_only=OFF, and nothing past a stage is repeated. The
// maintenance handoff's promoteVerified is the NotStarted row.
func TestPromoteVerifiedFrom_StatementSequence(t *testing.T) {
	F := opsSet(t, opsF)
	cases := []struct {
		from PromotionStage
		repl *mysql.ReplicaStatus
		gtid string
		want []string
	}{
		{PromotionStageNotStarted, &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}, opsF, []string{"stop", "reset", "gate", "sro=OFF", "ro=OFF"}},
		{PromotionStageReplicaStopped, &mysql.ReplicaStatus{}, opsF, []string{"reset", "gate", "sro=OFF", "ro=OFF"}},
		{PromotionStageReplicaReset, nil, opsF, []string{"gate", "sro=OFF", "ro=OFF"}},
		{PromotionStageWasWritable, nil, opsF + "," + opsUUIDB + ":1", []string{"gate", "sro=OFF", "ro=OFF"}},
	}
	for _, tc := range cases {
		t.Run(string(tc.from), func(t *testing.T) {
			m := &opsMySQL{uuid: opsUUIDB, ro: true, sro: true, repl: tc.repl, gtid: tc.gtid}
			var memo []promoteMemoStage
			hooks := promoteHooks{
				before: func(s promoteMemoStage) { memo = append(memo, s) },
				gate: func(context.Context) error {
					m.mu.Lock()
					m.stmts = append(m.stmts, "gate")
					m.mu.Unlock()
					return nil
				},
			}
			_, reached, err := NewFailoverController(testLogger()).promoteVerifiedFrom(context.Background(), mysql.CheckerSession(m), "dc2", F, tc.from, hooks)
			if err != nil || reached != promoteStageWriteAttempted {
				t.Fatalf("promoteVerifiedFrom = %v, %v", reached, err)
			}
			if got := m.statements(); !slices.Equal(got, tc.want) {
				t.Fatalf("statements = %v, want %v", got, tc.want)
			}
			if memo[len(memo)-1] != promoteMemoROClearSent || !slices.Contains(memo, promoteMemoSROClearSent) {
				t.Fatalf("memo stages = %v", memo)
			}
		})
	}
	t.Run("gate refusal sends nothing further", func(t *testing.T) {
		m := &opsMySQL{uuid: opsUUIDB, ro: true, sro: true, gtid: opsF}
		refused := errors.New("refused")
		_, _, err := NewFailoverController(testLogger()).promoteVerifiedFrom(context.Background(), mysql.CheckerSession(m), "dc2", F,
			PromotionStageReplicaReset, promoteHooks{gate: func(context.Context) error { return refused }})
		if !errors.Is(err, refused) || len(m.statements()) != 0 {
			t.Fatalf("err=%v statements=%v; want the refusal and nothing sent", err, m.statements())
		}
	})
	t.Run("maintenance handoff unchanged", func(t *testing.T) {
		m := &opsMySQL{uuid: opsUUIDB, ro: true, sro: true, gtid: opsF, repl: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}}
		if _, _, err := NewFailoverController(testLogger()).promoteVerified(context.Background(), m, "dc2", F); err != nil {
			t.Fatal(err)
		}
		if got, want := m.statements(), []string{"stop", "reset", "sro=OFF", "ro=OFF"}; !slices.Equal(got, want) {
			t.Fatalf("handoff statements = %v, want %v", got, want)
		}
	})
}

// --- Promote --------------------------------------------------------------

func TestPromote_FromNotStartedConfirms(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	if _, err := f.topo.ReadTargetServerUUID(context.Background(), f.ref); err != nil {
		t.Fatal(err)
	}
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteConfirmed {
		t.Fatalf("Promote = %+v, %v; want Confirmed", out, err)
	}
	if got, want := f.tgt.statements(), []string{"stop", "reset", "sro=OFF", "ro=OFF"}; !slices.Equal(got, want) {
		t.Fatalf("target statements = %v, want %v", got, want)
	}
	if got := f.src.statements(); len(got) != 0 {
		t.Fatalf("Promote sent %v to the source; it must send nothing to any other site", got)
	}
	// A re-run of the confirmed promotion repeats nothing (row 15).
	out, err = f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteConfirmed || out.Stage != PromotionStageWritable {
		t.Fatalf("re-run = %+v, %v; want Confirmed from Writable", out, err)
	}
	if got := f.tgt.statements(); len(got) != 4 {
		t.Fatalf("re-run sent more statements: %v", got)
	}
}

// TestPF_CrossProcessOwnUUIDExtrasPromote: with no memo (a new process), a
// read-only target holding F plus its own extras was written by the run:
// WasWritable clears its fences and confirms.
func TestPF_CrossProcessOwnUUIDExtrasPromote(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	f.tgt.repl = nil
	f.tgt.sro = false // mysqld boot state after a restart
	f.tgt.gtid = opsF + "," + opsUUIDB + ":1-3"
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteConfirmed || out.Stage != PromotionStageWasWritable {
		t.Fatalf("Promote = %+v, %v; want Confirmed from WasWritable", out, err)
	}
	if got, want := f.tgt.statements(), []string{"sro=OFF", "ro=OFF"}; !slices.Equal(got, want) {
		t.Fatalf("statements = %v, want %v", got, want)
	}
}

// TestPF_SameProcessOwnUUIDExtrasAreForeign: when this process's memo proves
// it never cleared super_read_only, own-uuid extras are someone else's.
func TestPF_SameProcessOwnUUIDExtrasAreForeign(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	if _, err := f.topo.ReadTargetServerUUID(context.Background(), f.ref); err != nil {
		t.Fatal(err)
	}
	f.tgt.repl = nil
	f.tgt.gtid = opsF + "," + opsUUIDB + ":1"
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteAborted || out.Terminal != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
		t.Fatalf("Promote = %+v, %v; want Aborted -> SourceLeftFenced", out, err)
	}
	if got := f.tgt.statements(); len(got) != 0 {
		t.Fatalf("statements = %v, want none", got)
	}
}

// The pre-write gate refuses beside a writable peer and an unreadable
// promotable peer, with nothing written to the target.
func TestPromote_PreWriteGate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *opsFixture)
		reason v1alpha1.PlannedFailoverReason
	}{
		{"writable third site", func(f *opsFixture) { f.third.ro, f.third.sro = false, false }, v1alpha1.PlannedFailoverReasonForeignWritable},
		{"unreadable dr-only site", func(f *opsFixture) { f.third.down = true }, v1alpha1.PlannedFailoverReasonPeerUnreadable},
		{"source unfenced", func(f *opsFixture) { f.src.sro = false }, v1alpha1.PlannedFailoverReasonSourceUnfenced},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, state.SiteRoleDROnly)
			f.hold(false)
			f.fenced()
			f.tgt.repl, f.tgt.gtid = nil, opsF
			tc.mutate(f)
			out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
			if err != nil || out.Kind != PromoteAborted || out.Reason != tc.reason {
				t.Fatalf("Promote = %+v, %v; want Aborted{%s}", out, err, tc.reason)
			}
			if got := f.tgt.statements(); len(got) != 0 {
				t.Fatalf("the gate refused but the target got %v", got)
			}
		})
	}
	t.Run("an unreadable reader is only a warning", func(t *testing.T) {
		f := newOpsFixture(t, state.SiteRoleReadOnly)
		f.hold(false)
		f.fenced()
		f.third.down = true
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteConfirmed || !slices.Contains(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers) {
			t.Fatalf("Promote = %+v, %v; want Confirmed with UnreachablePeers", out, err)
		}
	})
}

// TestPF_ConfirmWithLateUnreadablePeerSucceeds: a peer that answered at
// the gate and is unreadable at the confirm leaves the run Confirmed with
// UnreachablePeers, never a hold.
func TestPF_ConfirmWithLateUnreadablePeerSucceeds(t *testing.T) {
	f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
	f.hold(false)
	f.fenced()
	f.tgt.repl = nil
	f.tgt.beforeSRO = func() {
		f.third.mu.Lock()
		f.third.down = true
		f.third.mu.Unlock()
	}
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteConfirmed || !slices.Contains(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers) {
		t.Fatalf("Promote = %+v, %v; want Confirmed with UnreachablePeers", out, err)
	}
}

// A foreign writer that appears after the gate is caught by the confirm:
// a hold, never Succeeded beside a writable site (row 12).
func TestPromote_ConfirmSeesForeignWritable(t *testing.T) {
	f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
	f.hold(false)
	f.fenced()
	f.tgt.repl = nil
	f.tgt.beforeSRO = func() {
		f.third.mu.Lock()
		f.third.ro, f.third.sro = false, false
		f.third.mu.Unlock()
	}
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteInconsistent || out.Reason != v1alpha1.PlannedFailoverReasonForeignWritable {
		t.Fatalf("Promote = %+v, %v; want Inconsistent{ForeignWritable}", out, err)
	}
}

// Every mutating facade call refuses without the claim, and a hydrated
// claim refuses everything but the re-proof.
func TestPlannedFacade_RequiresClaim(t *testing.T) {
	f := newOpsFixture(t, "")
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	calls := map[string]func() error{
		"Fence":   func() error { _, err := f.topo.Fence(context.Background(), f.ref, opsUUIDA); return err },
		"Kill":    func() error { _, err := f.topo.KillAppConnections(context.Background(), f.ref); return err },
		"Promote": func() error { _, err := f.topo.Promote(context.Background(), f.ref, pf); return err },
		"Rollback": func() error {
			_, err := f.topo.Rollback(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhaseRollingBack))
			return err
		},
		"Record": func() error { return f.topo.RecordPlannedPromotion(context.Background(), f.ref, f.clk.Now(), opsF) },
		"DNS":    func() error { _, err := f.topo.ApplyDNS(context.Background(), f.ref); return err },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, errClaimLost) {
			t.Errorf("%s without the claim: %v, want errClaimLost", name, err)
		}
	}
	f.hold(true)
	for name, call := range calls {
		if err := call(); !errors.Is(err, errReproveRequired) {
			t.Errorf("%s on a hydrated claim: %v, want errReproveRequired", name, err)
		}
	}
	if len(f.src.statements())+len(f.tgt.statements()) != 0 {
		t.Fatalf("refused calls sent statements: %v %v", f.src.statements(), f.tgt.statements())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.reg.leader = ctx
	if err := calls["Kill"](); !errors.Is(err, errLeaderLost) {
		t.Errorf("Kill after leadership loss: %v, want errLeaderLost", err)
	}
}

// --- fence ----------------------------------------------------------------

func TestFence_SendsOnlyToTheIntentServer(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	proof, err := f.topo.Fence(context.Background(), f.ref, "replaced-uuid")
	if err != nil || proof.Kind != FenceNotSent || proof.Reason != v1alpha1.PlannedFailoverReasonAuthorityChanged || !proof.SourceWritable {
		t.Fatalf("Fence with a stale uuid = %+v, %v; want NotSent{AuthorityChanged}, source writable", proof, err)
	}
	if len(f.src.statements()) != 0 {
		t.Fatalf("fence sent to a replaced source: %v", f.src.statements())
	}
	proof, err = f.topo.Fence(context.Background(), f.ref, opsUUIDA)
	if err != nil || proof.Kind != FenceApplied || proof.GtidSet != opsF || proof.Attribution != FenceByMemo {
		t.Fatalf("Fence = %+v, %v; want Applied by memo with F", proof, err)
	}
}

// TestPF_I20d_CrossProcessFenceRequiresIntentAndUUID: a new process
// attributes super_read_only=ON to the run only on the intent's server.
func TestPF_I20d_CrossProcessFenceRequiresIntentAndUUID(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	rec := *opsRun(v1alpha1.PlannedFailoverPhaseFencing).Fence
	proof, err := f.topo.ProveFence(context.Background(), f.ref, rec)
	if err != nil || proof.Kind != FenceApplied || proof.Attribution != FenceByIntentAndUUID {
		t.Fatalf("ProveFence = %+v, %v; want Applied by intent+uuid", proof, err)
	}
	f.src.uuid = "replaced-pod"
	proof, err = f.topo.ProveFence(context.Background(), f.ref, rec)
	if err != nil || proof.Kind != FenceNotAttributable {
		t.Fatalf("ProveFence on a replaced pod = %+v, %v; want NotAttributable", proof, err)
	}
}

// --- rollback (§9) --------------------------------------------------------

func TestRollbackPreconditions(t *testing.T) {
	cases := []struct {
		name     string
		third    state.SiteRole
		phase    v1alpha1.PlannedFailoverPhase
		reason   v1alpha1.PlannedFailoverReason
		mutate   func(f *opsFixture)
		outcome  v1alpha1.PlannedFailoverOutcome
		warning  v1alpha1.PlannedFailoverWarning
		unfenced bool
	}{
		{name: "source already writable", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate: func(f *opsFixture) {}, outcome: v1alpha1.PlannedFailoverOutcomeRolledBack},
		{name: "attributable fence, no writable peer", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate: func(f *opsFixture) { f.fenced() }, outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, unfenced: true},
		{name: "unreadable peer does not block", third: state.SiteRoleDROnly, reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate: func(f *opsFixture) { f.fenced(); f.third.down = true }, outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, unfenced: true},
		{name: "replaced source pod", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate: func(f *opsFixture) { f.fenced(); f.src.uuid = "replaced" }, outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
			warning: v1alpha1.PlannedFailoverWarningFenceNotAttributable},
		{name: "writable reader", third: state.SiteRoleReadOnly, reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate: func(f *opsFixture) { f.fenced(); f.third.ro, f.third.sro = false, false }, outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{name: "source lost", reason: v1alpha1.PlannedFailoverReasonSourceLost,
			mutate: func(f *opsFixture) { f.fenced(); f.src.down = true }, outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{name: "source unreachable retries", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate: func(f *opsFixture) { f.fenced(); f.src.down = true }, outcome: ""},
		// One failed read opens the target-loss window (K053); the source
		// stays fenced. TestPF_RollbackTargetLossWindow pins the expiry.
		{name: "possible write, target unreachable retries", phase: v1alpha1.PlannedFailoverPhasePromoting, reason: v1alpha1.PlannedFailoverReasonPromotionAborted,
			mutate: func(f *opsFixture) { f.fenced(); f.tgt.down = true }, outcome: ""},
		{name: "possible write, target extras", phase: v1alpha1.PlannedFailoverPhasePromoting, reason: v1alpha1.PlannedFailoverReasonPromotionAborted,
			mutate: func(f *opsFixture) { f.fenced(); f.tgt.gtid = opsF + "," + opsUUIDB + ":1" }, outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired},
		{name: "divergent target before Promoting still unfences", reason: v1alpha1.PlannedFailoverReasonDivergentTarget,
			mutate: func(f *opsFixture) { f.fenced(); f.tgt.gtid = opsF + "," + opsUUIDC + ":1" }, outcome: v1alpha1.PlannedFailoverOutcomeRolledBack,
			warning: v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored, unfenced: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, tc.third)
			f.hold(false)
			f.tgt.repl = nil
			tc.mutate(f)
			pf := opsRun(v1alpha1.PlannedFailoverPhaseRollingBack)
			if tc.phase == v1alpha1.PlannedFailoverPhasePromoting {
				pf.Promotion = opsRun(tc.phase).Promotion
			}
			pf.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: tc.reason, IntentTime: metav1.NewTime(f.clk.Now())}
			res, err := f.topo.Rollback(context.Background(), f.ref, pf)
			if err != nil || res.Outcome != tc.outcome {
				t.Fatalf("Rollback = %+v, %v; want outcome %q", res, err, tc.outcome)
			}
			if tc.warning != "" && !slices.Contains(res.Warnings, tc.warning) {
				t.Fatalf("warnings = %v, want %s", res.Warnings, tc.warning)
			}
			gotUnfence := slices.Contains(f.src.statements(), "sro=OFF")
			if gotUnfence != tc.unfenced {
				t.Fatalf("source statements = %v; unfence sent = %v, want %v", f.src.statements(), gotUnfence, tc.unfenced)
			}
			if tc.unfenced && tc.warning == "" {
				if got := f.tgt.statements(); !slices.Equal(got, []string{"change:mysql-dc1", "start"}) {
					t.Fatalf("target channel restore = %v, want CHANGE then START", got)
				}
			}
		})
	}
}

// --- re-proof (§4.4) ------------------------------------------------------

func opsReproveGroup(f *opsFixture, pf *v1alpha1.PlannedFailoverStatus, last time.Time, target string) *v1alpha1.MysqlFailoverGroup {
	fg := opsGroup(pf)
	if !last.IsZero() {
		t := metav1.NewTime(last)
		fg.Status.SetLastFailover(&t, target, "")
	}
	return fg
}

func TestReprove_PreWritePhases(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(true)
	f.fenced()
	pf := opsRun(v1alpha1.PlannedFailoverPhaseWaitingForLag)
	res, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(pf))
	if err != nil || !res.OK {
		t.Fatalf("Reprove = %+v, %v; want OK", res, err)
	}
	if v, _ := f.reg.slot.View(); v.NeedsReprove {
		t.Fatal("a passed re-proof left needsReprove set")
	}

	f.hold(false)
	f.reg.slot.update(f.ref.RunID, func(c *plannedClaim) { c.needsReprove = true })
	moved := opsReproveGroup(f, pf, f.clk.Now().Add(-10*time.Second), "dc2")
	res, err = f.topo.Reprove(context.Background(), f.ref, moved)
	if err != nil || res.RollBack == nil || *res.RollBack != v1alpha1.PlannedFailoverReasonAuthorityChanged {
		t.Fatalf("Reprove after a gap failover = %+v, %v; want RollBack{AuthorityChanged}", res, err)
	}
}

// TestPF_HydratedClaimReprovesAfterGapPromotion: a fence lifted in the gap
// (a rebooted source) is not the run's fence any more.
func TestPF_HydratedClaimReprovesAfterGapPromotion(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(true)
	f.src.ro, f.src.sro = true, false
	res, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(opsRun(v1alpha1.PlannedFailoverPhaseDraining)))
	if err != nil || res.RollBack == nil || *res.RollBack != v1alpha1.PlannedFailoverReasonSourceUnfenced {
		t.Fatalf("Reprove = %+v, %v; want RollBack{SourceUnfenced}", res, err)
	}
}

// TestPF_PromotingReproveTargetSubsetRollsBack: in Promoting, a target that
// reads read-only at GTID ⊆ F committed nothing of the run: put its fence
// back and roll back, even when the failed check was a changed record.
func TestPF_PromotingReproveTargetSubsetRollsBack(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(true)
	f.fenced()
	f.tgt.repl, f.tgt.sro = nil, false
	fg := opsReproveGroup(f, opsRun(v1alpha1.PlannedFailoverPhasePromoting), f.clk.Now().Add(-10*time.Second), "dc1")
	res, err := f.topo.Reprove(context.Background(), f.ref, fg)
	if err != nil || res.RollBack == nil || *res.RollBack != v1alpha1.PlannedFailoverReasonAuthorityChanged || res.Hold != nil {
		t.Fatalf("Reprove = %+v, %v; want RollBack{AuthorityChanged}", res, err)
	}
	if !slices.Contains(f.tgt.statements(), "sro=ON") {
		t.Fatalf("target statements = %v; want its super_read_only restored", f.tgt.statements())
	}
}

// A changed record after a possible write holds; the run never names the
// target over it.
func TestReprove_PromotingChangedRecordHolds(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(true)
	f.fenced()
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	f.tgt.gtid = opsF + "," + opsUUIDB + ":1-4"
	fg := opsReproveGroup(f, opsRun(v1alpha1.PlannedFailoverPhasePromoting), f.clk.Now().Add(-10*time.Second), "dc1")
	res, err := f.topo.Reprove(context.Background(), f.ref, fg)
	if err != nil || res.Hold == nil || *res.Hold != v1alpha1.PlannedFailoverReasonAuthorityChanged {
		t.Fatalf("Reprove = %+v, %v; want Hold{AuthorityChanged}", res, err)
	}
	if err := f.topo.NameUnconfirmedTarget(f.ref); err != nil {
		t.Fatal(err)
	}
}

// TestPF_ReproveLackOfEvidenceNeverHolds: an unreadable peer or a rebooted
// source after a possible write hands off to the gate and confirm.
func TestPF_ReproveLackOfEvidenceNeverHolds(t *testing.T) {
	f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
	f.hold(true)
	f.fenced()
	f.third.down = true
	f.src.sro = false // rebooted
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	f.tgt.gtid = opsF + "," + opsUUIDB + ":1"
	res, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(opsRun(v1alpha1.PlannedFailoverPhasePromoting)))
	if err != nil || res.HandOff == nil || res.Hold != nil || res.RollBack != nil {
		t.Fatalf("Reprove = %+v, %v; want HandOff and never a hold", res, err)
	}
	if v, _ := f.reg.slot.View(); v.NeedsReprove {
		t.Fatal("HandOff left needsReprove set; Promote could not apply the gate/confirm mappings")
	}
}

// The run's own failover record, flushed before the Publishing write
// landed, is recognised by content (row 13).
func TestReprove_RecognisesOwnRecord(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(true)
	f.fenced()
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	fg := opsReproveGroup(f, pf, pf.Promotion.IntentTime.Add(2*time.Second), "dc2")
	fg.Annotations = map[string]string{
		v1alpha1.LastFailoverAnnotation:             failoverStampFormat(pf.Promotion.IntentTime.Add(2 * time.Second)),
		v1alpha1.LastFailoverTargetAnnotation:       "dc2",
		v1alpha1.PromotionGtidExecutedAnnotation:    opsF,
		v1alpha1.PromotionGtidExecutedForAnnotation: failoverWatermarkBinding(pf.Promotion.IntentTime.Add(2*time.Second), "dc2"),
	}
	res, err := f.topo.Reprove(context.Background(), f.ref, fg)
	if err != nil || !res.OK {
		t.Fatalf("Reprove over the run's own record = %+v, %v; want OK", res, err)
	}
}

// --- naming (§6.5) --------------------------------------------------------

// TestPF_SourceLeftFencedIsNamedAndReasserted: a run that leaves its own
// fence names the source; after release, with every site read-only and no
// lastFailoverTarget, the topology manager re-asserts it.
func TestPF_SourceLeftFencedIsNamedAndReasserted(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: opsUUIDA + ":1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: opsUUIDA + ":1-10"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "")
	tm.mu.Lock()
	tm.lastActiveSite, tm.lastActiveSiteNamed = "dc1", false // observed only, as restored
	tm.promotionGtidExecuted = opsUUIDA + ":1-10"
	tm.mu.Unlock()
	reg := &opsRegistry{current: tm, slot: &PlannedClaimSlot{}}
	WireRegisteredManager(tm, ManagerWiring{Slot: reg.slot})
	topo := newRunnerPlannedTopology(reg)
	ref := PlannedRunRef{RunID: "run-1", Source: "dc1", Target: "dc2"}
	reg.slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"})

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("the topology manager re-asserted while the run held its claim")
	}
	if err := topo.NameFencedSource(ref); err != nil {
		t.Fatal(err)
	}
	if !topo.ReleasePlannedFailoverClaim(ref.Group, "run-1", ReleaseTerminal) {
		t.Fatal("release refused")
	}
	if !tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("the named fenced source was not re-asserted after release")
	}
	site0.mu.Lock()
	ro := site0.readOnly
	site0.mu.Unlock()
	if ro {
		t.Fatal("dc1 still read-only after the re-assert")
	}
}

// TestPF_SourceLeftFencedNamingSurvivesRestart: restoreFailoverState's
// planned half names the source of the latest SourceLeftFenced run with an
// attributable fence, by its terminal record or its terminal marker.
func TestPF_SourceLeftFencedNamingSurvivesRestart(t *testing.T) {
	end := func(mut func(pf *v1alpha1.PlannedFailoverStatus, fg *v1alpha1.MysqlFailoverGroup)) *v1alpha1.MysqlFailoverGroup {
		pf := opsRun(v1alpha1.PlannedFailoverPhaseFailed)
		pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
			Reason: v1alpha1.PlannedFailoverReasonStatusStalled, CompletionTime: metav1.Now()}
		fg := opsGroup(pf)
		mut(pf, fg)
		return fg
	}
	cases := []struct {
		name  string
		fg    *v1alpha1.MysqlFailoverGroup
		last  time.Time
		named bool
	}{
		{"terminal record", end(func(*v1alpha1.PlannedFailoverStatus, *v1alpha1.MysqlFailoverGroup) {}), time.Time{}, true},
		{"terminal marker", end(func(pf *v1alpha1.PlannedFailoverStatus, fg *v1alpha1.MysqlFailoverGroup) {
			pf.Result, pf.Phase = nil, v1alpha1.PlannedFailoverPhaseWaitingForLag
			fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: TerminalMarker{RunID: "run-1",
				Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled}.String()}
		}), time.Time{}, true},
		{"fence not attributable", end(func(pf *v1alpha1.PlannedFailoverStatus, _ *v1alpha1.MysqlFailoverGroup) {
			pf.Result.Warnings = []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningFenceNotAttributable}
		}), time.Time{}, false},
		{"failover after the run started", end(func(*v1alpha1.PlannedFailoverStatus, *v1alpha1.MysqlFailoverGroup) {}),
			time.Date(2026, 11, 4, 12, 30, 0, 0, time.UTC), false},
		{"authority moved before the fence", end(func(pf *v1alpha1.PlannedFailoverStatus, _ *v1alpha1.MysqlFailoverGroup) {
			pf.Result.Reason, pf.Fence.State = v1alpha1.PlannedFailoverReasonAuthorityChanged, v1alpha1.PlannedFailoverFenceIntended
		}), time.Time{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: true}, &mockMySQL{readOnly: true})
			tm.lastFailover = tc.last
			r := &TopologyManagerRunner{logger: testLogger()}
			r.restorePlannedFailoverState(tm, tc.fg, types.NamespacedName{Namespace: "ns", Name: "lion"})
			named := tm.lastActiveSite == "dc1" && tm.lastActiveSiteNamed
			if named != tc.named {
				t.Fatalf("named = %v (lastActiveSite=%q named=%v), want %v", named, tm.lastActiveSite, tm.lastActiveSiteNamed, tc.named)
			}
		})
	}
}

// RecordPlannedPromotion records once per run and leaves the promotion on
// the claim, which a replacement manager of the same process applies.
func TestRecordPlannedPromotion_OncePerRunAndCarriedToReplacement(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	at := f.clk.Now()
	for range 2 {
		if err := f.topo.RecordPlannedPromotion(context.Background(), f.ref, at, opsF); err != nil {
			t.Fatal(err)
		}
	}
	f.tm.mu.RLock()
	last, target, gtid, changes := f.tm.lastFailover, f.tm.lastFailoverTarget, f.tm.promotionGtidExecuted, f.tm.authorityChanges
	f.tm.mu.RUnlock()
	if !last.Equal(at) || target != "dc2" || gtid != opsF || changes != 1 {
		t.Fatalf("record = %s/%s/%s after %d recordFailovers; want one record of the run", last, target, gtid, changes)
	}
	repl := newOpsFixture(t, "")
	repl.tm.SetLastFailover(at.Add(-time.Hour))
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	WireRegisteredManager(repl.tm, ManagerWiring{Slot: f.reg.slot, HydrateFrom: pf})
	repl.tm.mu.RLock()
	defer repl.tm.mu.RUnlock()
	if !repl.tm.lastFailover.Equal(at.Truncate(time.Second)) || repl.tm.lastFailoverTarget != "dc2" || repl.tm.promotionGtidExecuted != opsF {
		t.Fatalf("replacement record = %s/%s/%s; want the claim's promotion with watermark F",
			repl.tm.lastFailover, repl.tm.lastFailoverTarget, repl.tm.promotionGtidExecuted)
	}
}

func TestNameUnconfirmedTarget_NamesWithoutStamping(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	before := f.tm.lastFailover
	if err := f.topo.NameUnconfirmedTarget(f.ref); err != nil {
		t.Fatal(err)
	}
	f.tm.mu.RLock()
	defer f.tm.mu.RUnlock()
	if f.tm.lastFailoverTarget != "dc2" || !f.tm.lastFailover.Equal(before) || f.tm.promotedSite != "" {
		t.Fatalf("named = %q lastFailover moved=%v promotedSite=%q; want the name only",
			f.tm.lastFailoverTarget, !f.tm.lastFailover.Equal(before), f.tm.promotedSite)
	}
	if !f.tm.statusWriteFailed {
		t.Fatal("the name is not scheduled for the next snapshot write")
	}
}

func TestProbeLag(t *testing.T) {
	f := newOpsFixture(t, "")
	f.tgt.gtid = opsUUIDA + ":1-4"
	lp, err := f.topo.ProbeLag(context.Background(), f.ref, opsF)
	if err != nil || !lp.SourceReachable || !lp.TargetReachable || lp.CaughtUp || lp.Divergent {
		t.Fatalf("lagging = %+v, %v", lp, err)
	}
	f.tgt.gtid = opsF + "," + opsUUIDC + ":1"
	lp, _ = f.topo.ProbeLag(context.Background(), f.ref, opsF)
	if !lp.CaughtUp || !lp.Divergent {
		t.Fatalf("divergent = %+v", lp)
	}
	f.tgt.down = true
	if lp, _ = f.topo.ProbeLag(context.Background(), f.ref, opsF); lp.TargetReachable {
		t.Fatalf("down target reported reachable: %+v", lp)
	}
}

// TestPF_I20e_SameProcessMemoAttributesFence and the cross-process Fence
// re-run: a Fencing re-run that finds super_read_only=ON calls Fence again;
// the facade attributes it (memo, or intent plus uuid) instead of reporting
// that authority moved.
func TestPF_I20e_SameProcessMemoAttributesFence(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	if proof, err := f.topo.Fence(context.Background(), f.ref, opsUUIDA); err != nil || proof.Kind != FenceApplied {
		t.Fatalf("first Fence = %+v, %v", proof, err)
	}
	proof, err := f.topo.Fence(context.Background(), f.ref, opsUUIDA)
	if err != nil || proof.Kind != FenceApplied || proof.Attribution != FenceByMemo {
		t.Fatalf("Fence re-run = %+v, %v; want Applied by memo", proof, err)
	}
	if n := strings.Count(strings.Join(f.src.statements(), ","), "sro=ON"); n != 1 {
		t.Fatalf("the fence was sent %d times, want once", n)
	}
	other := newOpsFixture(t, "")
	other.hold(false)
	other.fenced()
	proof, err = other.topo.Fence(context.Background(), other.ref, opsUUIDA)
	if err != nil || proof.Kind != FenceApplied || proof.Attribution != FenceByIntentAndUUID {
		t.Fatalf("cross-process Fence re-run = %+v, %v; want Applied by intent+uuid", proof, err)
	}
	other.src.uuid = "replaced"
	if proof, _ = other.topo.Fence(context.Background(), other.ref, opsUUIDA); proof.Kind != FenceNotAttributable {
		t.Fatalf("Fence on a replaced, read-only pod = %+v; want NotAttributable", proof)
	}
}
