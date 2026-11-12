package component

// Known limitations of the restore planner after a GTID was issued twice
// under one server_uuid, built with real BinlogArchivers (the machinery of
// the planner property sweep) from a history the operator still permits:
//
//	a is the primary and crashes. It comes back without its last
//	transactions (a volume that came back older), which b holds. No site
//	that holds them answers when the operator decides (b is down or
//	unreachable), so the operator makes a writable again
//	(site docs, failover, "What this cannot see"). a issues the lost GTID
//	numbers to other transactions; b, back later, skips those.
//
// THE PLANS COUNTED AS WRONG HERE ARE WRONG: they replay, or take from the
// backup, the transactions a lost where a's history has the ones it issued
// under their GTIDs later. The tests pin which planner certifies them, so
// that nobody takes the lag-window anchor of issue #269 (pitrplan
// containmentAnchors) for safe to enable while these histories are
// reachable, and so that closing one of them is a deliberate change.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// reassertInPlace is the history above for site, the primary: it crashes,
// loses its last lose transactions and is made writable again with no
// failover in between. Its sidecar restarts with it. observed says whether
// the sidecar saw the server read-only before it was writable again: only
// then does the episode's promotion record carry a promotion set. lag is
// how many transactions the server commits before its sidecar's next role
// probe; the first of them reuse the lost GTID numbers. Each becomes a
// target.
func (p *propTrial) reassertInPlace(site string, lose, lag int, observed bool) {
	log := p.model.log[site]
	p.model.past[site] = append(p.model.past[site], append([]propCommit(nil), log...))
	for _, c := range log[len(log)-lose:] {
		p.lostTail = p.lostTail.Union(pitrplan.SetOf(c.g))
	}
	p.model.log[site] = append([]propCommit(nil), log[:len(log)-lose]...)
	p.crashed = true
	p.stop[site]()
	p.srv[site].crash(lose)
	start := func() {
		p.arch[site], p.stop[site] = p.w.startRestartableOn(site, p.srv[site], p.store[site], p.ident)
	}
	if observed {
		start()
		at := time.Now()
		for range 2 {
			p.w.eventually("a scan of "+site+" after its crash", func() bool {
				began := p.arch[site].Snapshot(context.Background()).LastScanAt
				if !began.After(at) {
					return false
				}
				at = began
				return true
			})
		}
	}
	p.records[site] = len(p.w.manifest(site).Promotions)
	var own []binlogtest.Txn
	p.srv[site].promote(true, nil, func() []binlogtest.Txn {
		for i := 0; i < lag; i++ {
			own = append(own, p.newCommit(site))
		}
		return own
	})
	if !observed {
		start()
	}
	for _, txn := range own {
		p.targets = append(p.targets, txn.Commit.Add(-time.Microsecond))
	}
	p.records[site]++
	p.w.waitRecords(site, p.records[site])
}

// retire drops from the oracle's view of the archive every row whose
// transactions the backup holds, as retention does once that backup is the
// oldest retained one.
func (o *propOracle) retire() {
	for i := range o.arch.Sites {
		sm := &o.arch.Sites[i]
		kept := sm.Rows[:0]
		for _, row := range sm.Rows {
			if o.B.Contains(row.Hints.GTIDs) {
				delete(o.rows, pitrplan.RowRef{Site: row.Site, Name: row.Name, ID: row.ID, State: row.State})
				continue
			}
			kept = append(kept, row)
		}
		sm.Rows = kept
	}
	o.archived = pitrplan.Set{}
	for _, row := range o.rows {
		o.archived = o.archived.Union(row.Hints.GTIDs)
	}
	copies := o.copies[:0]
	for _, c := range o.copies {
		if _, ok := o.rows[c.row]; ok {
			copies = append(copies, c)
		}
	}
	o.copies = copies
}

// verdict plans T and holds the answer to the model like checkCrashed, but
// reports instead of failing: "refused <token>", "right", or "WRONG: why".
func (o *propOracle) verdict(T time.Time) (string, *pitrplan.Plan) {
	t, p := o.t, o.p
	X := pitrplan.Set{}
	budget := pitrplan.NewBudget(64 << 20)
	o.mu.Lock()
	o.fetched = map[pitrplan.RowRef]*pitrplan.FileIndex{}
	o.mu.Unlock()
	plan, err := pitrplan.Run(context.Background(), pitrplan.Request{Mode: pitrplan.ModeTimestamp, Stop: T, Exclude: X, Delta: o.delta, Dump: p.backup, Budget: budget},
		o.arch, o.fetcher(budget))
	if err != nil {
		ref, ok := pitrplan.RefusalOf(err)
		if !ok {
			t.Fatalf("T=%s: non-refusal error %v", T.Format(time.RFC3339Nano), err)
		}
		if testing.Verbose() {
			t.Logf("T=%s: %.600s", T.Format("15:04:05.000000"), ref.Message)
		}
		return "refused " + string(ref.Token), nil
	}
	var w *propCopy
	for i := range o.copies {
		c := &o.copies[i]
		if c.oct >= T.UnixMicro() && !o.B.Has(c.g) && (w == nil || c.oct < w.oct) {
			w = c
		}
	}
	if w == nil || plan.Witness == nil || plan.Witness.GTID != w.g || plan.Witness.CommitTime != w.oct {
		return fmt.Sprintf("WRONG: witness %+v, the model's is %+v", plan.Witness, w), plan
	}
	origin := ""
	for site, uuid := range p.uuid {
		if uuid == w.g.SID {
			origin = site
		}
	}
	h, ok := p.model.historyBefore(origin, w.g, w.oct)
	if !ok {
		return fmt.Sprintf("WRONG: the witness %s committed at %d is no transaction %s committed", w.g, w.oct, origin), plan
	}
	if why := o.differs(h, X, plan, o.checkSegments("T="+T.Format(time.RFC3339Nano), X, plan)); why != "" {
		return "WRONG: " + why, plan
	}
	return "right", plan
}

// residualTrial is one history of reassertInPlace.
type residualTrial struct {
	// observed: the reissuing episode's record carries a promotion set.
	observed bool
	// holderBackup: the backup is taken on b, which holds the transactions a
	// lost, before a is writable again, and retention has retired the rows
	// that backup holds.
	holderBackup bool
	// lost: none of a's binlogs from the crash on reaches the archive (a is
	// lost each time before it uploads them), so a's later episodes are
	// archived only as b's copies. Otherwise a uploads as usual, and the
	// archive holds the transactions a issued again beside the lost ones.
	lost bool
	// foreign: b was the primary for a while before the crash, so the copies
	// follow transactions of another server_uuid and need an anchor.
	foreign bool
	// lose and lag are reassertInPlace's.
	lose, lag int
}

func (q residualTrial) String() string {
	return fmt.Sprintf("observed=%v holderBackup=%v lost=%v foreign=%v lose=%d lag=%d", q.observed, q.holderBackup, q.lost, q.foreign, q.lose, q.lag)
}

// residualTally counts the verdicts of the targets of one kind.
type residualTally struct{ right, refused, wrong int }

func (c *residualTally) add(v string) {
	switch {
	case v == "right":
		c.right++
	case strings.HasPrefix(v, "WRONG"):
		c.wrong++
	default:
		c.refused++
	}
}

func (c residualTally) String() string {
	return fmt.Sprintf("%d right, %d refused, %d WRONG", c.right, c.refused, c.wrong)
}

// residualKinds are the kinds of target of a residualTrial: in the episode
// that issued the lost GTIDs again or in a's next one, and in the lag window
// of that episode's promotion record or after it.
var residualKinds = []string{"reissuing episode/lag", "reissuing episode/time", "later episode/lag", "later episode/time"}

// run builds the history and plans every target with the planner as it
// ships and with the containment anchor on.
func (q residualTrial) run(t *testing.T) (shipped, on map[string]residualTally) {
	p := newPropTrial(t, propConfirmDelay)
	for range 3 {
		p.commit("a")
	}
	if !q.holderBackup {
		p.takeBackup("a")
	}
	if q.foreign {
		p.drain("b", 0)
		p.failover("a", "b", 0, false, propPromotion{rotate: true})
		for range 2 {
			p.commit("b")
		}
		p.failover("b", "a", 0, false, propPromotion{rotate: true})
	}
	for range 3 {
		p.commit("a")
	}
	p.srv["a"].rotate()
	p.waitUploaded("a")
	for range q.lose {
		p.commit("a") // the tail a loses
	}
	p.drain("b", 0) // b holds it
	if q.lost {
		p.store["a"].muted.Store(true)
	}
	if q.holderBackup {
		p.takeBackup("b")
		// b is down when a is made writable again; its restart opens a new
		// binlog file, so the file with its copies of a's history up to here
		// holds nothing but the backup's transactions.
		p.srv["b"].rotate()
	}
	n := len(p.targets)
	p.reassertInPlace("a", q.lose, q.lag, q.observed)
	if !p.reissued {
		t.Fatal("a did not issue a lost GTID again")
	}
	if rec := p.w.manifest("a").Promotions[p.records["a"]-1]; (rec.PromotionGTIDExecuted != "") != q.observed {
		t.Fatalf("the reissuing episode's record: %+v", rec)
	}
	targets := map[string][]time.Time{"reissuing episode/lag": append([]time.Time(nil), p.targets[n:]...)}
	for range 3 {
		p.commit("a")
		targets["reissuing episode/time"] = append(targets["reissuing episode/time"], p.target("a"))
	}
	p.drain("b", 0)
	// b takes over (a planned handoff), and a is promoted again: an episode
	// whose record carries a promotion set that holds the reissued GTIDs.
	p.failover("a", "b", 0, false, propPromotion{rotate: true})
	for range 2 {
		p.commit("b")
	}
	n = len(p.targets)
	p.failover("b", "a", 0, false, propPromotion{rotate: true, lag: 2})
	targets["later episode/lag"] = append([]time.Time(nil), p.targets[n:]...)
	if rec := p.w.manifest("a").Promotions[p.records["a"]-1]; rec.PromotionGTIDExecuted == "" {
		t.Fatalf("the later episode's record carries no promotion set: %+v", rec)
	}
	for range 3 {
		p.commit("a")
		targets["later episode/time"] = append(targets["later episode/time"], p.target("a"))
	}
	// b archives its copies of a's episodes once it is the primary again.
	p.failover("a", "b", 0, false, propPromotion{rotate: true})
	p.commit("b")
	p.srv["b"].rotate()
	p.waitUploaded("b")

	plan := func(t *testing.T, containment bool) map[string]residualTally {
		o := newPropOracle(t, p, time.Microsecond)
		o.containment = containment
		if q.holderBackup {
			o.retire()
		}
		out := map[string]residualTally{}
		for _, kind := range residualKinds {
			var tally residualTally
			for _, T := range targets[kind] {
				v, pl := o.verdict(T)
				tally.add(v)
				if testing.Verbose() {
					by := ""
					if pl != nil && pl.Witness != nil {
						by = fmt.Sprintf(" (witness %s, %s, anchors %v)", pl.Witness.GTID, pl.Witness.GroundTruth, pl.Witness.AnchorRecords)
					}
					t.Logf("containment=%v %s target %s: %s%s", containment, kind, T.Format("15:04:05.000000"), v, by)
				}
			}
			out[kind] = tally
		}
		return out
	}
	shipped = plan(t, false)
	t.Run("containment anchors", func(t *testing.T) {
		pitrplan.EnableContainmentAnchorsForTest(t)
		on = plan(t, true)
	})
	for _, k := range residualKinds {
		t.Logf("%s: %-22s shipped: %-32s containment anchors: %s", q, k, shipped[k], on[k])
	}
	return shipped, on
}

// TestKnownLimitationPITRPlanAfterInPlaceReissue pins what both planners
// answer after the history above (see the comment at the top of the file).
// a's binlogs of its episodes after the crash never reach the archive (a
// is lost each time before it uploads them), so those episodes are archived
// only as b's copies, which follow the transactions a lost.
//
//   - reissuing episode recorded without a promotion set (the sidecar came
//     up after the operator had made the server writable again), then a
//     later episode of a with one: nothing dates the lost transactions. A
//     target in the lag window of the later record is refused by the planner
//     as it ships and CERTIFIED WRONG with the containment anchor on; a
//     target that a record anchors by time is CERTIFIED WRONG by both (PR
//     #280 "found, not fixed" 5);
//   - every record carries a promotion set, but the only copy of the lost
//     transactions is in the backup (taken on b; retention retired the rows
//     it holds): the planner dates archived copies, not the backup. Both
//     planners CERTIFY WRONG by time; the lag window is refused as shipped
//     and CERTIFIED WRONG with the containment anchor on.
//
// In both, the copies in the reissuing episode's own lag window follow only
// a's server_uuid, need no anchor and are CERTIFIED WRONG by both planners.
func TestKnownLimitationPITRPlanAfterInPlaceReissue(t *testing.T) {
	for _, tc := range []struct {
		name string
		q    residualTrial
	}{
		{"reissuing episode recorded without a promotion set", residualTrial{lost: true, lose: 2, lag: 3}},
		{"the lost transactions are only in the backup", residualTrial{observed: true, holderBackup: true, lost: true, lose: 2, lag: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shipped, on := tc.q.run(t)
			// What is pinned: no planner certifies a right plan through b's
			// copies after the reissue; the later episode's lag window is
			// refused as shipped and certified wrong with the anchor on; a
			// target a record anchors by time is certified wrong by both.
			if got := shipped["later episode/lag"]; got.wrong != 0 || got.right != 0 || got.refused == 0 {
				t.Errorf("as shipped, the later episode's lag window: %s; want only refusals", got)
			}
			if got := on["later episode/lag"]; got.wrong == 0 {
				t.Errorf("with containment anchors, the later episode's lag window: %s; this test pins wrong certifications, and a planner that refuses them all has closed the limitation: update the test and the comment at pitrplan containmentAnchors", got)
			}
			for name, got := range map[string]residualTally{"as shipped": shipped["later episode/time"], "with containment anchors": on["later episode/time"]} {
				if got.wrong == 0 {
					t.Errorf("%s, the later episode's targets anchored by time: %s; this test pins wrong certifications (a closed limitation: update the test)", name, got)
				}
			}
			for _, k := range residualKinds {
				if shipped[k].right != 0 || on[k].right != 0 {
					t.Errorf("%s: a plan was certified right (shipped %s, containment %s); the fixture no longer builds the history it describes", k, shipped[k], on[k])
				}
			}
		})
	}
}

// TestPITRPlanResidualSweep runs random histories of reassertInPlace
// (PITR_PLAN_RESIDUAL_SEEDS of them, default 4) and reports what both
// planners answer. It asserts what the archive side does guarantee: when
// the transactions a issued again are archived beside the lost ones, no
// planner certifies a wrong plan. The wrong plans of the other trials are
// the known limitation the test above pins; the sweep counts them.
func TestPITRPlanResidualSweep(t *testing.T) {
	seeds := int64(4)
	if v := os.Getenv("PITR_PLAN_RESIDUAL_SEEDS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = n
	}
	total := map[string]map[string]residualTally{"shipped": {}, "containment": {}}
	onlyAnchor, onlyShipped := 0, 0
	for seed := int64(1); seed <= seeds; seed++ {
		r := rand.New(rand.NewSource(seed))
		q := residualTrial{observed: r.Intn(2) == 0, holderBackup: r.Intn(3) == 0, lost: r.Intn(4) != 0, foreign: r.Intn(2) == 0, lose: 1 + r.Intn(3), lag: 1 + r.Intn(3)}
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			shipped, on := q.run(t)
			for _, k := range residualKinds {
				for name, got := range map[string]residualTally{"shipped": shipped[k], "containment": on[k]} {
					sum := total[name][k]
					sum.right, sum.refused, sum.wrong = sum.right+got.right, sum.refused+got.refused, sum.wrong+got.wrong
					total[name][k] = sum
					if !q.lost && got.wrong != 0 {
						t.Errorf("%s, %s, %s: %s although the archive holds the transactions a issued again", q, name, k, got)
					}
				}
				onlyAnchor += max(0, on[k].wrong-shipped[k].wrong)
				onlyShipped += max(0, shipped[k].wrong-on[k].wrong)
			}
		})
	}
	for _, k := range residualKinds {
		t.Logf("%d trials, %-22s shipped: %-32s containment anchors: %s", seeds, k, total["shipped"][k], total["containment"][k])
	}
	t.Logf("wrong plans certified only with the containment anchor on: %d; only by the planner as it ships: %d", onlyAnchor, onlyShipped)
}
