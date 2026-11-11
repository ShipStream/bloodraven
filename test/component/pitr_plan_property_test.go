package component

// Planner property sweep (#206, design §9.1 property_test, §9.2): seeded
// random failover and failback histories run through real BinlogArchivers
// (two sites over simulated mysqlds sharing one PVC archive); the planner
// then plans restores from the archive the archivers produced. A reference
// model replays every server's commit log, and the oracle decides what the
// planner may answer before it looks at the answer:
//
//   - when the planner certifies a timestamp restore, its witness is the
//     model's earliest archived commit at or after T outside B ∪ X, Λ is that
//     transaction's origin's executed set just before it, D is the model's
//     {g archived : maxOct(B) < oct(g) < T, g ∉ Λ ∪ B} and D ⊆ X, and each
//     segment, re-derived from the indexed bytes of the file it names,
//     applied to B yields exactly Λ ∖ X;
//   - when it certifies latest mode, Λ is a state some server actually held,
//     it never includes a transaction lost at a failover unless excluded,
//     and the model's loss evidence is excluded and reported;
//   - when it refuses, the model justifies the token: TargetNotCovered only
//     without a model witness, DivergentHistory only when the model's D (or
//     loss evidence) is not excluded, StopAmbiguous only for a witness
//     archived only as a replica copy that no promotion record or origin
//     transaction anchors, ArchiveGap only when the model's history is not
//     fully archived from the backup's position, and so on.
//
// A failover may also be a crash of the old primary that loses its last
// transactions although the replica has applied them (an unsynced binlog
// tail). The old primary then rejoins by replication, which never gives it
// its own transactions back, and once it is promoted again it issues their
// GTIDs to other transactions, which the replica skips. From then on a GTID
// can name two transactions, so in such a trial the model works on
// transactions (GTID and commit time): a certified plan must replay, and
// build on a backup that holds, exactly the transactions the witness's
// origin had, and the transaction that was lost is not one of them at any
// target after the loss. The model does not predict which refusal such a
// trial gets.
//
// Promotions are the operator's (#229, #269): the new primary drains its
// relay log, rotates its binlog (most of the time; the rotation may not be
// acknowledged) and becomes writable, and it may commit before its
// sidecar's next role probe, so that its promotion record already holds its
// first transactions (the lag window). Sometimes its applier is still
// running then and adds copies after its first own commit. The targets
// include those first transactions. Every promotion record's promotion set
// and confirmation are held to the model as well: the set must precede the
// episode's first own transaction in the server's own log.
//
// The planner ships with the containment anchor of #269 off
// (pitrplan.EnableContainmentAnchorsForTest has why), and TestPITRPlanProperty
// holds the shipped planner to the model: a witness in a promotion record's
// lag window that is archived only as a replica copy refuses, and no
// certified plan is anchored by a record that holds its witness or was
// observed within δ of it. TestPITRPlanPropertyContainmentAnchors runs the
// same trials with the anchor on, so that the rule keeps its property
// coverage until it is enabled.
//
// PITR_PLAN_SEED=<n> replays one trial; PITR_PLAN_SEEDS=<n> runs seeds 1..n
// of TestPITRPlanProperty, PITR_PLAN_CONTAINMENT_SEEDS=<n> those of
// TestPITRPlanPropertyContainmentAnchors.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// propProbeSlack bounds how long after a role change the archivers (role
// probe every 10 ms) observe it; the anchor model widens its windows by it.
const propProbeSlack = 100 * time.Millisecond

type propCommit struct {
	g   pitrplan.GTID
	oct int64
}

// propModel is the reference: every server's executed sequence, in order.
type propModel struct {
	log map[string][]propCommit // site → commits it executed, in order
	// past holds, per site, its log as it was before each crash that lost
	// its tail: the history of a server incarnation that is gone.
	past map[string][][]propCommit
	// oct and origin are by GTID: of the transaction issued last under it.
	oct    map[pitrplan.GTID]int64
	origin map[pitrplan.GTID]string // site that committed it
	last   time.Time
}

// logs are site's current log and its past ones.
func (m *propModel) logs(site string) [][]propCommit {
	return append([][]propCommit{m.log[site]}, m.past[site]...)
}

// historyBefore is what site had executed, transaction by transaction, just
// before it executed the transaction g committed at oct, in whichever of
// its incarnations executed it.
func (m *propModel) historyBefore(site string, g pitrplan.GTID, oct int64) ([]propCommit, bool) {
	for _, log := range m.logs(site) {
		for i, c := range log {
			if c.g == g && c.oct == oct {
				return log[:i], true
			}
		}
	}
	return nil, false
}

// heldHistories are the states some server held, in any incarnation, whose
// GTID set is set.
func (m *propModel) heldHistories(set pitrplan.Set) [][]propCommit {
	var out [][]propCommit
	for site := range m.log {
		for _, log := range m.logs(site) {
			s := pitrplan.Set{}
			if s.Equal(set) {
				out = append(out, nil)
			}
			for i, c := range log {
				s = s.Union(pitrplan.SetOf(c.g))
				if s.Equal(set) {
					out = append(out, log[:i+1])
				}
			}
		}
	}
	return out
}

func (m *propModel) tick() time.Time {
	now := time.Now().UTC().Truncate(time.Microsecond)
	if !now.After(m.last) {
		now = m.last.Add(time.Microsecond)
	}
	m.last = now
	return now
}

// executedBefore is site's executed set just before it executed g.
func (m *propModel) executedBefore(site string, g pitrplan.GTID) (pitrplan.Set, bool) {
	s := pitrplan.Set{}
	for _, c := range m.log[site] {
		if c.g == g {
			return s, true
		}
		s = s.Union(pitrplan.SetOf(c.g))
	}
	return nil, false
}

// heldState reports whether some server held exactly set at some point.
func (m *propModel) heldState(set pitrplan.Set) bool {
	return len(m.heldHistories(set)) > 0
}

type propTrial struct {
	w     *writerArchive
	ident sidecar.ArchiveIdentity
	srv   map[string]*simServer
	arch  map[string]*sidecar.BinlogArchiver
	// stop stops a site's archiver (its sidecar container).
	stop    map[string]func()
	store   map[string]*mutedStore
	model   *propModel
	uuid    map[string]string
	id      map[string]uint32
	records map[string]int
	queue   map[string][]binlogtest.Txn // replica apply lag
	dead    pitrplan.Set
	lossy   bool
	backup  backupmeta.DumpMeta
	// backupIssues are the backup's transactions with their commit times,
	// and backupMaxOct the newest of those.
	backupIssues map[pitrplan.GTID]int64
	backupMaxOct int64
	targets      []time.Time
	// crashed: an old primary lost its last transactions (lostTail) in a
	// crash although the replica had applied them, and rejoined as a
	// replica. reissued: it was promoted again and issued one of their
	// GTIDs to another transaction.
	crashed, reissued bool
	lostTail          pitrplan.Set
	// promoted are the promotions made by failover, in order.
	promoted []propPromoted
	// inverted: a promoted server applied copies after its first own
	// commit, so its log and the old primary's order them differently.
	inverted bool
}

// mutedStore is one site's view of the archive store. Once muted, the
// site's binlog uploads fail: its sidecar still saves its manifest, and
// with it its records, but none of its files reaches the archive, as when
// a new primary is lost before it uploads anything.
type mutedStore struct {
	sidecar.ArchiveStore
	site  string
	muted atomic.Bool
}

func (s *mutedStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.muted.Load() && strings.HasPrefix(key, "binlogs/"+s.site+"/") {
		return errors.New("injected: this site's binlog uploads fail")
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

func (s *mutedStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	return sidecar.PutIfAbsent(ctx, s.ArchiveStore, key, r, size)
}

// propPromotion is how a failover promotes the replica.
type propPromotion struct {
	// rotate: the operator's promotion-time FLUSH BINARY LOGS was
	// acknowledged (#229).
	rotate bool
	// lag is how many transactions the new primary commits before its
	// sidecar's next role probe: its promotion record holds them.
	lag int
	// late is how many copies of the old primary's last transactions the
	// new primary applies after its first own commit (a server that is
	// writable and still replicating).
	late int
	// lose: the old primary does not step down but crashes, and loses its
	// last lose transactions, which the new primary holds. restart: its
	// sidecar restarts with it (the pod crashed, not only mysqld). mute:
	// from this promotion on, none of the new primary's binlogs reaches
	// the archive.
	lose    int
	restart bool
	mute    bool
}

// propPromoted is one promotion as the model saw it.
type propPromoted struct {
	site   string
	record int // index of its promotion record in the site's manifest
	rotate bool
	// set is what the server had executed when it became writable.
	set pitrplan.Set
}

// newPropTrial starts a writable a and a replicating b, each with a real
// archiver, and waits for a's first promotion record. confirmDelay is the
// archivers' promotion-record confirmation delay (0: production's).
func newPropTrial(t *testing.T, confirmDelay time.Duration) *propTrial {
	p := &propTrial{w: newWriterArchive(t), srv: map[string]*simServer{}, arch: map[string]*sidecar.BinlogArchiver{}, stop: map[string]func(){}, store: map[string]*mutedStore{},
		model: &propModel{log: map[string][]propCommit{}, past: map[string][][]propCommit{}, oct: map[pitrplan.GTID]int64{}, origin: map[pitrplan.GTID]string{}},
		uuid:  map[string]string{"a": writerUUIDA, "b": writerUUIDB}, id: map[string]uint32{"a": 101, "b": 102},
		records: map[string]int{"a": 1, "b": 0}, queue: map[string][]binlogtest.Txn{"b": {}}, dead: pitrplan.Set{}, lostTail: pitrplan.Set{}}
	p.w.confirmDelay = confirmDelay
	p.srv["a"] = newSimServer(t, writerUUIDA, 101, false)
	p.srv["b"] = newSimServer(t, writerUUIDB, 102, true)
	p.ident = p.w.identity(writerGroup, p.w.dest)
	for _, s := range []string{"a", "b"} {
		p.store[s] = &mutedStore{ArchiveStore: p.w.store, site: s}
		p.arch[s], p.stop[s] = p.w.startRestartableOn(s, p.srv[s], p.store[s], p.ident)
	}
	p.w.waitRecords("a", 1)
	return p
}

func (p *propTrial) commit(primary string) {
	p.srv[primary].apply(p.newCommit(primary))
}

// newCommit is primary's next transaction, committed now in the model; the
// caller executes it on the server.
func (p *propTrial) newCommit(primary string) binlogtest.Txn {
	gno := int64(1)
	for _, c := range p.model.log[primary] {
		if c.g.SID == p.uuid[primary] && c.g.GNO >= gno {
			gno = c.g.GNO + 1
		}
	}
	at := p.model.tick()
	txn := binlogtest.Txn{SID: p.uuid[primary], GNO: gno, Commit: at, ServerID: p.id[primary]}
	g := pitrplan.GTID{SID: txn.SID, GNO: gno}
	if p.lostTail.Has(g) {
		// The lowest free number is one the server lost.
		p.reissued = true
	}
	p.model.log[primary] = append(p.model.log[primary], propCommit{g, at.UnixMicro()})
	p.model.oct[g] = at.UnixMicro()
	p.model.origin[g] = primary
	for site := range p.srv {
		// A replica skips a GTID it has executed: its copy of a transaction
		// the primary lost stands in for the one issued again.
		if site != primary && p.queue[site] != nil && !p.executed(site).Has(g) {
			p.queue[site] = append(p.queue[site], txn)
		}
	}
	return txn
}

// crash is the old primary's part of a failover in which it does not step
// down: it crashes, loses its last lose transactions and restarts
// read-only without them, with its sidecar when restart. Its log before
// the crash becomes history. It returns once the site's archiver has
// finished a scan that began after the restart, so any record the change
// calls for is written, and counts the site's records anew.
func (p *propTrial) crash(site string, lose int, restart bool) {
	log := p.model.log[site]
	p.model.past[site] = append(p.model.past[site], append([]propCommit(nil), log...))
	for _, c := range log[len(log)-lose:] {
		p.lostTail = p.lostTail.Union(pitrplan.SetOf(c.g))
	}
	p.model.log[site] = append([]propCommit(nil), log[:len(log)-lose]...)
	p.crashed = true
	if restart {
		p.stop[site]()
	}
	p.srv[site].crash(lose)
	if restart {
		p.arch[site], p.stop[site] = p.w.startRestartableOn(site, p.srv[site], p.store[site], p.ident)
	}
	// LastScanAt is when a scan began: the second one to begin after the
	// crash shows that the first has ended.
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
	p.records[site] = len(p.w.manifest(site).Promotions)
}

// everExecuted is everything site has executed in the model, in any of its
// incarnations.
func (p *propTrial) everExecuted(site string) pitrplan.Set {
	s := pitrplan.Set{}
	for _, log := range p.model.logs(site) {
		for _, c := range log {
			s = s.Union(pitrplan.SetOf(c.g))
		}
	}
	return s
}

// copied records that site executed the copies txns, in order.
func (p *propTrial) copied(site string, txns []binlogtest.Txn) {
	for _, txn := range txns {
		p.model.log[site] = append(p.model.log[site], propCommit{pitrplan.GTID{SID: txn.SID, GNO: txn.GNO}, txn.Commit.UnixMicro()})
	}
}

// executed is everything site has executed in the model.
func (p *propTrial) executed(site string) pitrplan.Set {
	s := pitrplan.Set{}
	for _, c := range p.model.log[site] {
		s = s.Union(pitrplan.SetOf(c.g))
	}
	return s
}

// lastCommit is the newest commit of primary.
func (p *propTrial) lastCommit(primary string) pitrplan.GTID {
	return p.model.log[primary][len(p.model.log[primary])-1].g
}

// target adds a target 1 µs before primary's newest commit, so that
// commit is the model's witness.
func (p *propTrial) target(primary string) time.Time {
	t := time.UnixMicro(p.model.oct[p.lastCommit(primary)] - 1).UTC()
	p.targets = append(p.targets, t)
	return t
}

// drain applies a replica's queued copies except the last drop, which are
// lost with the old primary.
func (p *propTrial) drain(site string, drop int) {
	q := p.queue[site]
	keep := len(q) - drop
	for _, txn := range q[:keep] {
		p.srv[site].apply(txn)
		g := pitrplan.GTID{SID: txn.SID, GNO: txn.GNO}
		p.model.log[site] = append(p.model.log[site], propCommit{g, txn.Commit.UnixMicro()})
	}
	for _, txn := range q[keep:] {
		p.dead = p.dead.Union(pitrplan.SetOf(pitrplan.GTID{SID: txn.SID, GNO: txn.GNO}))
	}
	p.queue[site] = q[:0]
}

func (p *propTrial) sealed(site string) int { return len(p.srv[site].files) - 1 }

func (p *propTrial) waitUploaded(site string) {
	if p.store[site].muted.Load() {
		return
	}
	p.w.eventually("uploads of "+site, func() bool { return len(p.w.manifest(site).Files) >= p.sealed(site) })
}

// takeBackup records a consistent dump of site (its executed set now),
// then waits until every later commit is past both dump-end bounds the
// planner enforces (ceil(End) and serverClock.end + 1 s), so a later
// target never justifies PITRBackupAfterTarget.
func (p *propTrial) takeBackup(site string) {
	if p.queue[site] != nil {
		p.drain(site, 0)
	}
	b := pitrplan.Set{}
	var last int64
	p.backupIssues = map[pitrplan.GTID]int64{}
	for _, c := range p.model.log[site] {
		b = b.Union(pitrplan.SetOf(c.g))
		last = max(last, c.oct)
		p.backupIssues[c.g] = c.oct
	}
	p.backupMaxOct = last
	end := time.UnixMicro(last + 1).UTC()
	meta := backupmeta.NewMicroTime(end)
	p.backup = backupmeta.DumpMeta{Version: 1, GTIDExecuted: b.String(), End: meta, ServerClock: backupmeta.ServerClock{Start: meta, End: meta},
		Stamp: backupmeta.Stamp{Site: site, ServerID: p.id[site], CreatedAt: meta}}
	time.Sleep(time.Until(end.Add(time.Second + 10*time.Millisecond)))
}

// failover moves the primary role to replica. drop > 0 loses the last drop
// queued copies with the old primary (lossy); archiveTail first seals and
// uploads the old primary's tail, so the lost transactions are archived.
// how.lose > 0 is the opposite loss: the replica has applied the old
// primary's last transactions and the old primary loses them in a crash.
// how is the promotion; each transaction the new primary commits in its lag
// window becomes a target. It returns the new primary's promotion record
// time.
func (p *propTrial) failover(primary, replica string, drop int, archiveTail bool, how propPromotion) time.Time {
	p.waitUploaded(primary)
	if drop > 0 {
		p.lossy = true
		if archiveTail {
			p.srv[primary].rotate()
			p.waitUploaded(primary)
		}
	}
	// The replica's relay log: the copies it applies before it is
	// promoted, those a still-running applier adds after its first own
	// commit, and those lost with the old primary.
	q := p.queue[replica]
	keep := len(q) - drop
	late := 0
	if how.lag > 0 && drop == 0 {
		late = min(how.late, keep)
	}
	drained, held := q[:keep-late], q[keep-late:keep]
	for _, txn := range q[keep:] {
		p.dead = p.dead.Union(pitrplan.SetOf(pitrplan.GTID{SID: txn.SID, GNO: txn.GNO}))
	}
	p.queue[replica] = nil
	if how.mute {
		p.store[replica].muted.Store(true)
	}
	if how.lose > 0 {
		p.crash(primary, how.lose, how.restart)
	} else {
		p.srv[primary].setReadOnly(true)
	}
	p.w.waitDemoted(p.arch[primary])
	if p.lossy {
		// The old primary is down for good: no further copies.
		p.queue[primary] = nil
	} else {
		p.queue[primary] = []binlogtest.Txn{}
	}
	p.copied(replica, drained)
	p.promoted = append(p.promoted, propPromoted{site: replica, record: p.records[replica], rotate: how.rotate, set: p.executed(replica)})
	var own []binlogtest.Txn
	p.srv[replica].promote(how.rotate, drained, func() []binlogtest.Txn {
		var early []binlogtest.Txn
		for i := 0; i < how.lag; i++ {
			txn := p.newCommit(replica)
			own, early = append(own, txn), append(early, txn)
			if i == 0 && len(held) > 0 {
				p.copied(replica, held)
				early = append(early, held...)
				p.inverted = true
			}
		}
		return early
	})
	for _, txn := range own {
		p.targets = append(p.targets, txn.Commit.Add(-time.Microsecond))
	}
	p.records[replica]++
	p.w.waitRecords(replica, p.records[replica])
	p.waitUploaded(replica)
	recs := p.w.manifest(replica).Promotions
	return recs[len(recs)-1].ObservedAt
}

// sleepPast waits until the wall clock (every simulated server's) is
// strictly past at + d.
func sleepPast(at time.Time, d time.Duration) {
	time.Sleep(time.Until(at.Add(d + 200*time.Millisecond)))
}

// propConfirmDelay is the sweep's confirmation delay: its episodes last
// milliseconds, and a record is confirmed a few role probes after it.
const propConfirmDelay = 30 * time.Millisecond

// propStats counts one trial's answers.
type propStats struct {
	planned, refused int
	// contained: certified plans whose witness needed a containment anchor.
	contained int
	// crashed and reissued are the trial's (propTrial); crashedPlanned are
	// the plans certified in a crashed trial.
	crashed, reissued bool
	crashedPlanned    int
}

// runPropTrial runs one trial. containment says whether the planner's
// containment anchor is on (the caller turned it on): the oracle then
// expects the lag window to plan, and otherwise to refuse.
func runPropTrial(t *testing.T, seed int64, containment bool) (st propStats) {
	r := rand.New(rand.NewSource(seed))
	p := newPropTrial(t, propConfirmDelay)
	primary, replica := "a", "b"
	eras := 2 + r.Intn(3)
	n0 := 3 + r.Intn(5)
	// quiet: this era's primary never seals a binlog of its own accord, so
	// its transactions are archived only as the other site's copies.
	quiet := false
	crashes := 0
	// The backup is taken inside era 0, so every trial plans.
	backupAt := 2 + r.Intn(n0-1)
	backupSite := []string{"a", "b"}[r.Intn(2)]
	commits := 0
	for era := 0; era < eras; era++ {
		n := n0
		if era > 0 {
			n = 3 + r.Intn(5)
		}
		for i := 0; i < n; i++ {
			p.commit(primary)
			commits++
			if r.Intn(5) == 0 && !quiet {
				p.srv[primary].rotate()
			}
			if r.Intn(4) == 0 && p.queue[replica] != nil {
				p.drain(replica, 0)
			}
			if era == 0 && commits == backupAt {
				p.takeBackup(backupSite)
			}
			if commits > backupAt && r.Intn(2) == 0 {
				p.target(primary)
			}
		}
		if era == eras-1 || p.lossy {
			continue
		}
		// Fail over to the replica: planned (caught up) or lossy.
		drop := 0
		archiveTail := false
		if r.Intn(3) == 0 && len(p.queue[replica]) > 0 {
			drop = 1 + r.Intn(len(p.queue[replica]))
			archiveTail = r.Intn(2) == 0
		}
		// The promotion: usually with the rotation, often with commits
		// in the lag window, sometimes with a late applier.
		how := propPromotion{rotate: r.Intn(4) != 0}
		quiet = false
		if r.Intn(3) != 0 {
			how.lag = 1 + r.Intn(2)
			quiet = r.Intn(2) == 0
			if r.Intn(4) == 0 {
				how.late = 1 + r.Intn(2)
			}
		}
		// The old primary crashes and loses the end of its active binlog,
		// which the replica has applied; it rejoins as a replica. The trial
		// then runs long enough for it to be promoted again (and issue the
		// lost GTIDs again) and for the other site to archive its copies of
		// that era. Half of the time the new primary never uploads a binlog:
		// its era, and its copies of what the old primary lost, are then
		// archived by the old primary alone, which never had the lost
		// transactions again.
		if drop == 0 && crashes < 2 && r.Intn(3) == 0 {
			if n := p.srv[primary].tailOwn(); n > 0 {
				how.lose = 1 + r.Intn(min(n, 3))
				how.restart = r.Intn(2) == 0
				how.mute = r.Intn(2) == 0
				eras = max(eras, era+3+r.Intn(2))
				crashes++
			}
		}
		p.failover(primary, replica, drop, archiveTail, how)
		primary, replica = replica, primary
	}
	if r.Intn(3) != 0 {
		p.srv[primary].rotate()
	}
	p.waitUploaded(primary)
	if p.backup.Version == 0 {
		t.Fatalf("seed %d: the generator took no backup (backupAt %d, era 0 has %d commits)", seed, backupAt, n0)
	}
	p.targets = append(p.targets, time.Now().UTC().Add(time.Hour))
	o := newPropOracle(t, p, time.Microsecond)
	o.containment = containment
	t.Logf("seed %d: %d eras, backup on %s, lossy=%v (lost %s), inverted=%v, crashed=%v (lost tail %s, issued again: %v), archived %s",
		seed, eras, backupSite, p.lossy, p.dead, p.inverted, p.crashed, p.lostTail, p.reissued, o.archived)
	if os.Getenv("PITR_PLAN_SEED") != "" {
		o.describe()
	}
	o.checkRecords(fmt.Sprintf("seed %d", seed))
	res := o.run(fmt.Sprintf("seed %d", seed))
	st.crashed, st.reissued = p.crashed, p.reissued
	for _, r := range res {
		if r.err != nil {
			st.refused++
			continue
		}
		st.planned++
		if o.byContainment(r.plan) {
			if !containment {
				t.Fatalf("seed %d: the shipped planner certified T=%s X=%s through a promotion record that holds the witness or was observed within δ of it: %+v",
					seed, r.T.Format(time.RFC3339Nano), r.X, r.plan.Witness)
			}
			st.contained++
		}
		if p.crashed {
			st.crashedPlanned++
		}
	}
	if st.planned+st.refused == 0 {
		t.Fatalf("seed %d: the oracle checked nothing", seed)
	}
	return st
}

// propOracle holds the reference model's view of one finished trial.
type propOracle struct {
	t     *testing.T
	p     *propTrial
	delta time.Duration
	// containment: the planner's containment anchor (#269) is on. It
	// ships off, and then no record anchors a witness in its lag window.
	containment bool
	arch        pitrplan.Archive
	rows        map[pitrplan.RowRef]pitrplan.Row
	archived    pitrplan.Set
	B           pitrplan.Set
	maxOctB     int64

	// copies are the archived transactions, read from every row's bytes: a
	// GTID that was issued twice has copies with two commit times.
	copies []propCopy

	mu      sync.Mutex
	fetched map[pitrplan.RowRef]*pitrplan.FileIndex
}

// propCopy is one archived copy of a transaction.
type propCopy struct {
	row pitrplan.RowRef
	g   pitrplan.GTID
	oct int64
}

type propResult struct {
	mode pitrplan.Mode
	T    time.Time
	X    pitrplan.Set
	plan *pitrplan.Plan
	err  error
}

func newPropOracle(t *testing.T, p *propTrial, delta time.Duration) *propOracle {
	o := &propOracle{t: t, p: p, delta: delta, rows: map[pitrplan.RowRef]pitrplan.Row{}, archived: pitrplan.Set{},
		fetched: map[pitrplan.RowRef]*pitrplan.FileIndex{}}
	for _, s := range []string{"a", "b"} {
		sm, err := sidecar.PlanSite(p.w.manifest(s))
		if err != nil {
			t.Fatal(err)
		}
		o.arch.Sites = append(o.arch.Sites, sm)
		for _, row := range sm.Rows {
			o.rows[pitrplan.RowRef{Site: row.Site, Name: row.Name, ID: row.ID, State: row.State}] = row
			o.archived = o.archived.Union(row.Hints.GTIDs)
		}
	}
	o.B = pitrplan.MustParseSet(p.backup.GTIDExecuted)
	o.maxOctB = p.backupMaxOct
	fetch := o.fetcher(pitrplan.NewBudget(64 << 20))
	for ref := range o.rows {
		idx, err := fetch.Fetch(context.Background(), ref)
		if err != nil {
			t.Fatalf("read %s/%s: %v", ref.Site, ref.Name, err)
		}
		for i := range idx.Txns {
			o.copies = append(o.copies, propCopy{ref, idx.GTID(i), idx.Txns[i].OCT})
		}
	}
	o.fetched = map[pitrplan.RowRef]*pitrplan.FileIndex{}
	return o
}

// describe logs the trial for a replay: every server's log in the model,
// and every site's archived rows and records.
func (o *propOracle) describe() {
	short := func(g pitrplan.GTID) string { return fmt.Sprintf("%s:%d", g.SID[:1], g.GNO) }
	for _, site := range []string{"a", "b"} {
		var log []string
		for _, c := range o.p.model.log[site] {
			log = append(log, short(c.g))
		}
		o.t.Logf("model %s executed: %s", site, strings.Join(log, " "))
		for i, past := range o.p.model.past[site] {
			log = log[:0]
			for _, c := range past {
				log = append(log, short(c.g))
			}
			o.t.Logf("model %s before its crash %d: %s", site, i+1, strings.Join(log, " "))
		}
	}
	for _, c := range o.copies {
		if o.p.lostTail.Has(c.g) {
			o.t.Logf("copy of %s in %s/%s committed at %s", short(c.g), c.row.Site, c.row.Name, time.UnixMicro(c.oct).UTC().Format(time.RFC3339Nano))
		}
	}
	for _, sm := range o.arch.Sites {
		for _, row := range sm.Rows {
			o.t.Logf("row %s/%s: previous %s, holds %s", row.Site, row.Name, row.Hints.Previous, row.Hints.GTIDs)
		}
		for i, rec := range sm.Promotions {
			o.t.Logf("record %s#%d: %+v", sm.Site, i, rec)
		}
	}
	o.t.Logf("backup on %s: %s; lost %s", o.p.backup.Stamp.Site, o.B, o.p.dead)
}

// fetcher reads rows from the archive store, authenticating them like
// pitr-download does, and keeps each index so segments can be checked
// against the bytes of the file they name.
func (o *propOracle) fetcher(budget *pitrplan.Budget) pitrplan.Fetcher {
	w := o.p.w
	return pitrplan.FetchFunc(func(ctx context.Context, ref pitrplan.RowRef) (*pitrplan.FileIndex, error) {
		key, err := sidecar.ArchiveEntryKey("binlogs", ref.Site, sidecar.ManifestEntry{Name: ref.Name, ID: ref.ID})
		if err != nil {
			return nil, err
		}
		b, ok, err := w.store.Get(ctx, key)
		if err != nil || !ok {
			return nil, fmt.Errorf("get %s: %v %v", key, ok, err)
		}
		idx, err := sidecar.InspectTransactions(bytes.NewReader(b), sidecar.TxIndexOptions{Label: ref.Site + "/" + ref.Name, Budget: budget})
		if err != nil {
			return nil, err
		}
		if !sidecar.MatchesBinlogDigest(w.store, ref.ID, idx.Digest) {
			return nil, &pitrplan.Refusal{Token: pitrarchive.TokenArchiveObjectInvalid, Message: "digest mismatch"}
		}
		o.mu.Lock()
		o.fetched[ref] = idx
		o.mu.Unlock()
		return idx, nil
	})
}

// witness is the model's earliest archived commit at or after T outside
// B ∪ X.
func (o *propOracle) witness(T time.Time, X pitrplan.Set) (pitrplan.GTID, bool) {
	var w pitrplan.GTID
	best := int64(-1)
	for _, g := range o.archived.Subtract(o.B).Subtract(X).GTIDs() {
		if oc := o.p.model.oct[g]; oc >= T.UnixMicro() && (best < 0 || oc < best) {
			w, best = g, oc
		}
	}
	return w, best >= 0
}

// refD is the model's D: archived transactions committed in (maxOct(B), T)
// outside Λ ∪ B.
func (o *propOracle) refD(T time.Time, lambda pitrplan.Set) pitrplan.Set {
	d := pitrplan.Set{}
	known := lambda.Union(o.B)
	for _, g := range o.archived.GTIDs() {
		if oc := o.p.model.oct[g]; oc > o.maxOctB && oc < T.UnixMicro() && !known.Has(g) {
			d = d.Union(pitrplan.SetOf(g))
		}
	}
	return d
}

// originCopy reports whether w is archived by its origin site's server.
func (o *propOracle) originCopy(w pitrplan.GTID) bool {
	site := o.p.model.origin[w]
	for _, row := range o.rows {
		if row.Site == site && row.Hints.GTIDs.Has(w) {
			return true
		}
	}
	return false
}

func (o *propOracle) promotions(site string) []pitrarchive.PromotionRecord {
	for _, sm := range o.arch.Sites {
		if sm.Site == site {
			return sm.Promotions
		}
	}
	return nil
}

// anchorable is the model of the replica-copy anchor rule (§4.4 as
// revised): a promotion record R of w's origin site, or an archived origin
// transaction q of that site, anchors w when it is more than δ older than
// w, no record of that site was observed after it and up to δ (plus the
// probe interval) after w (a later episode or incarnation), and no record
// of the other site observed within δ of that span lacks w. The model is
// generous to refusals: it requires δ for origin anchors too.
func (o *propOracle) anchorable(w pitrplan.GTID) bool {
	site := o.p.model.origin[w]
	octW := o.p.model.oct[w]
	d := o.delta.Microseconds()
	slack := propProbeSlack.Microseconds()
	ok := func(at int64) bool {
		if at+d >= octW {
			return false
		}
		for _, sm := range o.arch.Sites {
			for _, rec := range sm.Promotions {
				obs := rec.ObservedAt.UnixMicro()
				if sm.Site == site {
					if obs > at && obs <= octW+slack+d {
						return false
					}
					continue
				}
				if obs >= at-d && obs <= octW+d && !pitrplan.MustParseSet(rec.GTIDExecuted).Has(w) {
					return false
				}
			}
		}
		return true
	}
	for _, rec := range o.promotions(site) {
		if ok(rec.ObservedAt.UnixMicro()) {
			return true
		}
	}
	for _, row := range o.rows {
		if row.Site != site {
			continue
		}
		for _, q := range row.Hints.GTIDs.GTIDs() {
			if o.p.model.origin[q] == site && ok(o.p.model.oct[q]) {
				return true
			}
		}
	}
	return false
}

// contained is the model of the containment anchor (#269): a promotion
// record of w's origin site with a promotion set that does not hold w,
// whose own observation or confirmation holds w, anchors w with that set.
// The model is generous to refusals: any other record of the site observed
// around w, and any record of the other site around w that lacks w, sets
// the record aside. The set must also explain every predecessor of w that
// is not under w's own server_uuid. With the planner's containment anchor
// off, as it ships, nothing anchors by containment.
func (o *propOracle) contained(w pitrplan.GTID, X, lambda pitrplan.Set) bool {
	if !o.containment {
		return false
	}
	site := o.p.model.origin[w]
	octW := o.p.model.oct[w]
	d := o.delta.Microseconds()
	slack := propProbeSlack.Microseconds()
	for i, rec := range o.promotions(site) {
		if rec.Kind != pitrarchive.RecordPromotion || rec.PromotionGTIDExecuted == "" {
			continue
		}
		set := pitrplan.MustParseSet(rec.GTIDExecuted)
		held := set.Has(w) || (!rec.ConfirmedAt.IsZero() && pitrplan.MustParseSet(rec.ConfirmedGTIDAdded).Has(w))
		promotionSet := pitrplan.MustParseSet(rec.PromotionGTIDExecuted)
		if !held || promotionSet.Has(w) {
			continue
		}
		clear := true
		for _, sm := range o.arch.Sites {
			for j, other := range sm.Promotions {
				obs := other.ObservedAt.UnixMicro()
				if sm.Site == site {
					pi := time.Duration(other.ProbeInterval).Microseconds()
					if j != i && obs >= octW-d-slack && obs <= octW+d+slack+pi {
						clear = false
					}
					continue
				}
				if obs >= octW-2*d-slack && obs <= octW+d+slack && !pitrplan.MustParseSet(other.GTIDExecuted).Has(w) {
					clear = false
				}
			}
		}
		rest := lambda.Subtract(X).Subtract(promotionSet)
		if clear && rest.Subtract(rest.OfUUID(w.SID)).IsEmpty() {
			return true
		}
	}
	return false
}

// copiesDisagree reports whether every archived replica copy of w sits on
// a server that executed w after a different set than w's origin did (a
// promoted server whose applier was still running): no copy yields Λ.
func (o *propOracle) copiesDisagree(w pitrplan.GTID, X, lambda pitrplan.Set) bool {
	origin := o.p.model.origin[w]
	copies, agree := 0, 0
	for site := range o.p.model.log {
		if site == origin {
			continue
		}
		archived := false
		for _, row := range o.rows {
			archived = archived || (row.Site == site && row.Hints.GTIDs.Has(w))
		}
		if !archived {
			continue
		}
		copies++
		if pred, ok := o.p.model.executedBefore(site, w); ok && pred.Subtract(X).Equal(lambda.Subtract(X)) {
			agree++
		}
	}
	return copies > 0 && agree == 0
}

// byContainment reports whether plan's witness is anchored by a promotion
// record that the time rule cannot order against it: one that holds the
// witness, or was observed within δ of it.
func (o *propOracle) byContainment(plan *pitrplan.Plan) bool {
	if plan == nil || plan.Witness == nil || plan.Witness.GroundTruth != pitrplan.GroundTruthAnchored {
		return false
	}
	w := plan.Witness.GTID
	for _, name := range plan.Witness.AnchorRecords {
		for _, sm := range o.arch.Sites {
			for i, rec := range sm.Promotions {
				if name != fmt.Sprintf("%s#%d", sm.Site, i) {
					continue
				}
				if pitrplan.MustParseSet(rec.GTIDExecuted).Has(w) || rec.ObservedAt.UnixMicro()+o.delta.Microseconds() >= plan.Witness.CommitTime {
					return true
				}
			}
		}
	}
	return false
}

// checkRecords holds the #269 evidence of every promotion record to the
// model. A promotion set is contained in the record's gtid_executed,
// contains the read-only observation, and every member of it was executed,
// in the server's own log, before the first transaction under the server's
// uuid outside that observation: the episode's first own transaction. A
// promotion the model made after a read-only period has one (unless it is
// empty), and with the rotation it is exactly what the server had executed
// when it became writable. A confirmation was observed no earlier than the delay after the
// record, adds nothing the record holds, and holds only what the server
// executed.
func (o *propOracle) checkRecords(label string) {
	t, p := o.t, o.p
	for _, sm := range o.arch.Sites {
		uuid := p.uuid[sm.Site]
		for i, rec := range sm.Promotions {
			if rec.Kind != pitrarchive.RecordPromotion {
				continue
			}
			set := pitrplan.MustParseSet(rec.GTIDExecuted)
			if rec.PromotionGTIDExecuted != "" {
				promotionSet := pitrplan.MustParseSet(rec.PromotionGTIDExecuted)
				gro := pitrplan.MustParseSet(rec.ReadOnlyGTIDExecuted)
				before := pitrplan.Set{}
				for _, c := range p.model.log[sm.Site] {
					if c.g.SID == uuid && !gro.Has(c.g) {
						break
					}
					before = before.Union(pitrplan.SetOf(c.g))
				}
				if !set.Contains(promotionSet) || !promotionSet.Contains(gro) || !before.Contains(promotionSet) {
					t.Fatalf("%s: %s#%d has promotion set %s (read-only %s, recorded %s); before its episode's first own transaction the server had executed %s",
						label, sm.Site, i, promotionSet, gro, set, before)
				}
			}
			if !rec.ConfirmedAt.IsZero() {
				added := pitrplan.MustParseSet(rec.ConfirmedGTIDAdded)
				if added.Overlaps(set) || !p.everExecuted(sm.Site).Contains(set.Union(added)) || rec.ConfirmedAt.Before(rec.ObservedAt.Add(propConfirmDelay)) {
					t.Fatalf("%s: %s#%d is confirmed at %s (observed at %s) with %s beyond %s; the server executed %s",
						label, sm.Site, i, rec.ConfirmedAt, rec.ObservedAt, added, set, p.everExecuted(sm.Site))
				}
				if rec.PromotionGTIDExecuted == "" {
					t.Fatalf("%s: %s#%d is confirmed and carries no promotion set: %+v", label, sm.Site, i, rec)
				}
			}
		}
	}
	for _, pr := range p.promoted {
		recs := o.promotions(pr.site)
		if pr.record >= len(recs) {
			t.Fatalf("%s: %s has %d records, the model promoted it %d times", label, pr.site, len(recs), pr.record+1)
		}
		rec := recs[pr.record]
		// An empty promotion set is recorded as none: without the
		// rotation it is the read-only observation, which is empty for a
		// server that had executed nothing when it was last probed.
		if rec.PromotionGTIDExecuted == "" && ((pr.rotate && !pr.set.IsEmpty()) || rec.ReadOnlyGTIDExecuted != "") {
			t.Fatalf("%s: %s#%d, a promotion after a read-only period, carries no promotion set: %+v", label, pr.site, pr.record, rec)
		}
		if got := pitrplan.MustParseSet(rec.PromotionGTIDExecuted); pr.rotate && !got.Equal(pr.set) {
			t.Fatalf("%s: %s#%d was promoted with the rotation at %s, its promotion set is %s", label, pr.site, pr.record, pr.set, got)
		}
	}
}

// bPositioned reports whether some archived file reaches exactly B at a
// position (its start, or after one of its transactions in its server's
// order): the search needs one to start replay from.
func (o *propOracle) bPositioned() bool {
	for _, row := range o.rows {
		state := row.Hints.Previous
		if state.Equal(o.B) {
			return true
		}
		for _, c := range o.p.model.log[row.Site] {
			if !row.Hints.GTIDs.Has(c.g) {
				continue
			}
			state = state.Union(pitrplan.SetOf(c.g))
			if state.Equal(o.B) {
				return true
			}
		}
	}
	return false
}

// modelLost is latest mode's loss evidence candidates (§4.8 G2 as ruled
// N1/N2): an archived transaction committed more than δ before a promotion
// record of another site was observed, missing from that record and not
// excluded. The planner counts those of them in the head's Λ.
func (o *propOracle) modelLost(X pitrplan.Set) pitrplan.Set {
	return o.modelLostIn(X, o.rows)
}

// modelLostIn is modelLost over the archived copies in rows.
func (o *propOracle) modelLostIn(X pitrplan.Set, rows map[pitrplan.RowRef]pitrplan.Row) pitrplan.Set {
	lost := pitrplan.Set{}
	d := o.delta.Microseconds()
	for _, row := range rows {
		for _, g := range row.Hints.GTIDs.GTIDs() {
			for _, sm := range o.arch.Sites {
				if sm.Site == row.Site {
					continue
				}
				for _, rec := range sm.Promotions {
					if o.p.model.oct[g] < rec.ObservedAt.UnixMicro()-d && !pitrplan.MustParseSet(rec.GTIDExecuted).Has(g) {
						lost = lost.Union(pitrplan.SetOf(g))
					}
				}
			}
		}
	}
	return lost.Subtract(X)
}

// run plans every target (and latest mode) with X = ∅ and, when the loss
// is excludable, X = the lost transactions, checking each answer against
// the model.
func (o *propOracle) run(label string) []propResult {
	t, p := o.t, o.p
	excludes := []pitrplan.Set{{}}
	if !p.dead.IsEmpty() && !p.dead.Overlaps(o.B) {
		excludes = append(excludes, p.dead)
	}
	// Excluding what a crash lost is the remedy the refusals name.
	if !p.lostTail.IsEmpty() && !p.lostTail.Overlaps(o.B) {
		excludes = append(excludes, p.lostTail)
	}
	var out []propResult
	// Every query is planned again with spilled arrays (issue #266).
	spill, spilling := newPropSpill(t)
	for _, X := range excludes {
		for _, T := range append(append([]time.Time{}, p.targets...), time.Time{}) {
			mode := pitrplan.ModeTimestamp
			if T.IsZero() {
				mode = pitrplan.ModeLatest
			}
			budget := pitrplan.NewBudget(64 << 20)
			// The rows this query downloads.
			o.mu.Lock()
			o.fetched = map[pitrplan.RowRef]*pitrplan.FileIndex{}
			o.mu.Unlock()
			plan, err := pitrplan.Run(context.Background(), pitrplan.Request{Mode: mode, Stop: T, Exclude: X, Delta: o.delta, Dump: p.backup, Budget: budget},
				o.arch, o.fetcher(budget))
			what := fmt.Sprintf("%s δ=%s %s T=%s X=%s lossy=%v", label, o.delta, mode, T.Format(time.RFC3339Nano), X, p.lossy)
			if spilling {
				o.mustMatchSpilled(spill, what, mode, T, X, plan, err)
			}
			if mode == pitrplan.ModeLatest {
				o.checkLatest(what, X, plan, err)
			} else {
				o.checkTimestamp(what, T, X, plan, err)
			}
			if err != nil {
				ref, _ := pitrplan.RefusalOf(err)
				t.Logf("%s: %s", what, ref.Token)
			}
			out = append(out, propResult{mode, T, X, plan, err})
		}
	}
	return out
}

// refusal fails the test unless err is a refusal whose token justified
// allows.
func (o *propOracle) refusal(what string, err error, justified map[pitrarchive.Token]bool) {
	ref, ok := pitrplan.RefusalOf(err)
	if !ok {
		o.t.Fatalf("%s: non-refusal error %v", what, err)
	}
	if o.p.inverted {
		// A promoted server that applied copies after its first own commit
		// ordered them differently than the old primary did. The two
		// sites' binlogs then disagree, and the planner may refuse for any
		// of the reasons that follows from (an order inversion, a backup
		// whose state no archived order passes through, a transaction
		// committed before the witness outside its history). The model
		// does not predict which; every certified plan is still checked.
		return
	}
	if !justified[ref.Token] {
		o.t.Fatalf("%s: the model does not justify the refusal (justified: %v): %v", what, justified, err)
	}
}

// checkCrashed is the oracle of a trial in which a server lost its last
// transactions in a crash and rejoined. A GTID may then name two
// transactions, so the model compares transactions, a GTID with its commit
// time. A certified timestamp plan has the model's witness: the earliest
// archived copy at or after T outside B ∪ X. Its history is what that
// transaction's origin had executed just before it, in the incarnation that
// committed it; a latest plan's is a state some server held. Λ ∖ X is that
// history's GTID set, the backup holds the very transactions of that
// history, and so does every segment: a plan that replays the transaction
// a server lost, where the history has the one issued under its GTID
// later (or neither), fails here. Any refusal is accepted: the model does
// not predict which of the planner's checks sees the loss first.
func (o *propOracle) checkCrashed(what string, mode pitrplan.Mode, T time.Time, X pitrplan.Set, plan *pitrplan.Plan, err error) {
	t, p := o.t, o.p
	if err != nil {
		if _, ok := pitrplan.RefusalOf(err); !ok {
			t.Fatalf("%s: non-refusal error %v", what, err)
		}
		return
	}
	var histories [][]propCommit
	if mode == pitrplan.ModeTimestamp {
		var w *propCopy
		for i := range o.copies {
			c := &o.copies[i]
			if c.oct >= T.UnixMicro() && !o.B.Has(c.g) && !X.Has(c.g) && (w == nil || c.oct < w.oct) {
				w = c
			}
		}
		if w == nil {
			t.Fatalf("%s: certified %+v, but the model has no witness", what, plan.Witness)
		}
		if plan.Witness == nil || plan.Witness.GTID != w.g || plan.Witness.CommitTime != w.oct {
			t.Fatalf("%s: witness %+v, the model says %s committed at %d", what, plan.Witness, w.g, w.oct)
		}
		origin := ""
		for site, uuid := range p.uuid {
			if uuid == w.g.SID {
				origin = site
			}
		}
		h, ok := p.model.historyBefore(origin, w.g, w.oct)
		if !ok {
			t.Fatalf("%s: the witness %s committed at %d is no transaction %s committed", what, w.g, w.oct, origin)
		}
		histories = [][]propCommit{h}
	} else {
		histories = p.model.heldHistories(plan.Lineage)
		if len(histories) == 0 || plan.Lineage.Subtract(X).Overlaps(p.dead) {
			t.Fatalf("%s: latest Λ %s is not a server state, or holds lost %s", what, plan.Lineage, p.dead)
		}
	}
	applied := o.checkSegments(what, X, plan)
	var why []string
	for _, h := range histories {
		reason := o.differs(h, X, plan, applied)
		if reason == "" {
			return
		}
		why = append(why, reason)
	}
	t.Fatalf("%s: certified Λ %s with witness %+v and segments %v, which is not the history: %s", what, plan.Lineage, plan.Witness, applied, strings.Join(why, "; "))
}

// differs says how plan differs from the history h, transaction by
// transaction, or returns "".
func (o *propOracle) differs(h []propCommit, X pitrplan.Set, plan *pitrplan.Plan, applied []propCommit) string {
	want := map[pitrplan.GTID]int64{}
	set := pitrplan.Set{}
	for _, c := range h {
		want[c.g] = c.oct
		set = set.Union(pitrplan.SetOf(c.g))
	}
	if !plan.Lineage.Subtract(X).Equal(set.Subtract(X)) {
		return fmt.Sprintf("Λ∖X is %s, the history holds %s", plan.Lineage.Subtract(X), set.Subtract(X))
	}
	for g, oct := range o.p.backupIssues {
		if in, ok := want[g]; ok && in != oct {
			return fmt.Sprintf("the backup holds the %s committed at %d, the history the one committed at %d", g, oct, in)
		}
	}
	for _, c := range applied {
		if want[c.g] != c.oct {
			return fmt.Sprintf("a segment replays the %s committed at %d, the history holds the one committed at %d", c.g, c.oct, want[c.g])
		}
	}
	return ""
}

func (o *propOracle) checkTimestamp(what string, T time.Time, X pitrplan.Set, plan *pitrplan.Plan, err error) {
	t, m := o.t, o.p.model
	if o.p.crashed {
		o.checkCrashed(what, pitrplan.ModeTimestamp, T, X, plan, err)
		return
	}
	justified := map[pitrarchive.Token]bool{}
	if T.UnixMicro() <= o.maxOctB || T.Before(o.p.backup.ServerClock.End.Add(time.Second)) {
		justified[pitrarchive.TokenBackupAfterTarget] = true
	}
	w, found := o.witness(T, X)
	if !found {
		justified[pitrarchive.TokenTargetNotCovered] = true
		if err == nil {
			t.Fatalf("%s: certified %+v, but the model has no witness", what, plan.Witness)
		}
		o.refusal(what, err, justified)
		return
	}
	lambda, ok := m.executedBefore(m.origin[w], w)
	if !ok {
		t.Fatalf("%s: model witness %s is not in its origin's log", what, w)
	}
	refD := o.refD(T, lambda)
	if !o.originCopy(w) && ((!o.anchorable(w) && !o.contained(w, X, lambda)) || o.copiesDisagree(w, X, lambda)) {
		justified[pitrarchive.TokenStopAmbiguous] = true
	}
	if !lambda.Contains(o.B) {
		justified[pitrarchive.TokenBackupNotOnLineage] = true
	}
	if !X.Contains(refD) {
		justified[pitrarchive.TokenArchiveDivergentHistory] = true
	} else if !o.bPositioned() || !o.archived.Union(o.B).Contains(lambda.Subtract(X)) {
		// The gap search runs after the D window, so it cannot answer for
		// an unexcluded D.
		justified[pitrarchive.TokenArchiveGap] = true
	}
	if err != nil {
		o.refusal(what, err, justified)
		return
	}
	if plan.Witness == nil || plan.Witness.GTID != w {
		t.Fatalf("%s: witness %+v, the model says %s", what, plan.Witness, w)
	}
	if !plan.Lineage.Subtract(X).Equal(lambda.Subtract(X)) {
		t.Fatalf("%s: Λ %s, %s's origin had %s", what, plan.Lineage, w, lambda)
	}
	if !X.Contains(refD) || !refD.Equal(plan.Divergent) {
		t.Fatalf("%s: model D %s (must be ⊆ X), planner D %s", what, refD, plan.Divergent)
	}
	o.checkSegments(what, X, plan)
}

func (o *propOracle) checkLatest(what string, X pitrplan.Set, plan *pitrplan.Plan, err error) {
	t, p := o.t, o.p
	if p.crashed {
		o.checkCrashed(what, pitrplan.ModeLatest, time.Time{}, X, plan, err)
		return
	}
	justified := map[pitrarchive.Token]bool{}
	lostArchived := p.dead.Intersect(o.archived).Subtract(X)
	lost := o.modelLost(X)
	if !lostArchived.IsEmpty() || !lost.IsEmpty() {
		justified[pitrarchive.TokenArchiveDivergentHistory] = true
		justified[pitrarchive.TokenArchiveSplitBrain] = true
	}
	if p.dead.Overlaps(o.B) {
		justified[pitrarchive.TokenBackupNotOnLineage] = true
	}
	replay := false
	for _, row := range o.rows {
		end := row.Hints.Previous.Union(row.Hints.GTIDs)
		if end.Contains(o.B) && !row.Hints.GTIDs.Subtract(o.B).Subtract(X).IsEmpty() {
			replay = true
		}
		if !o.archived.Union(o.B).Contains(end.Subtract(X)) {
			justified[pitrarchive.TokenArchiveGap] = true
		}
	}
	if !replay {
		justified[pitrarchive.TokenNoArchivedReplay] = true
	}
	if !o.bPositioned() {
		justified[pitrarchive.TokenArchiveGap] = true
	}
	if err != nil {
		o.refusal(what, err, justified)
		return
	}
	if !p.model.heldState(plan.Lineage) || plan.Lineage.Subtract(X).Overlaps(p.dead) {
		t.Fatalf("%s: latest Λ %s is not a server state, or holds lost %s", what, plan.Lineage, p.dead)
	}
	// Loss evidence is about the head's own transactions: a record
	// proves lost the members of Λ it lacks.
	if l := lost.Intersect(plan.Lineage); !l.IsEmpty() {
		t.Fatalf("%s: certified Λ %s although the model's loss evidence %s is not excluded", what, plan.Lineage, l)
	}
	// The planner reports the loss evidence it read (lossEvidence: a
	// downloaded occurrence). A file that holds only excluded transactions
	// is not downloaded, so the report holds every lost transaction of the
	// head with a copy in a downloaded file, and nothing that was not lost.
	o.mu.Lock()
	read := map[pitrplan.RowRef]pitrplan.Row{}
	for ref := range o.fetched {
		if row, ok := o.rows[ref]; ok {
			read[ref] = row
		}
	}
	o.mu.Unlock()
	all := o.modelLost(pitrplan.Set{}).Intersect(plan.Lineage)
	if want := o.modelLostIn(pitrplan.Set{}, read).Intersect(plan.Lineage); !plan.Lost.Contains(want) || !all.Contains(plan.Lost) || !p.dead.Contains(plan.Lost) {
		t.Fatalf("%s: reported loss evidence %s; the model's is %s (%s in the downloaded files), the lost transactions %s", what, plan.Lost, all, want, p.dead)
	}
	o.checkSegments(what, X, plan)
}

// checkSegments re-derives every segment from the indexed bytes of the file
// it names (F232): its start predecessors are that file's Previous_gtids ∪
// baseGtids ∪ the transactions before Start, it applies exactly the
// unexcluded transactions of [Start, End), and applied to B in order the
// segments yield Λ ∖ X. It returns the transactions the segments apply, as
// their files hold them.
func (o *propOracle) checkSegments(what string, X pitrplan.Set, plan *pitrplan.Plan) []propCommit {
	t := o.t
	state := o.B
	var replayed []propCommit
	for k, s := range plan.Segments {
		if s.File < 0 || s.File >= len(plan.Files) {
			t.Fatalf("%s: segment %d names file %d of %d", what, k, s.File, len(plan.Files))
		}
		pf := plan.Files[s.File]
		o.mu.Lock()
		idx := o.fetched[pf.Row]
		o.mu.Unlock()
		row, listed := o.rows[pf.Row]
		if idx == nil || !listed {
			t.Fatalf("%s: segment %d names %+v, which was not fetched (%v) or is not listed (%v)", what, k, pf.Row, idx != nil, listed)
		}
		if pf.Digest != idx.Digest || s.Start < 0 || s.End > len(idx.Txns) || s.Start > s.End {
			t.Fatalf("%s: segment %d [%d,%d) of %s (digest %s) does not fit the fetched file (%d transactions, digest %s)",
				what, k, s.Start, s.End, pf.Row.Name, pf.Digest, len(idx.Txns), idx.Digest)
		}
		pred := idx.Previous.Union(row.Base)
		for i := 0; i < s.Start; i++ {
			pred = pred.Union(pitrplan.SetOf(idx.GTID(i)))
		}
		if !pred.Equal(s.StartPredecessors) || !pred.Subtract(X).Equal(state) {
			t.Fatalf("%s: segment %d starts at %s (file says %s), state %s", what, k, s.StartPredecessors, pred, state)
		}
		applied := pitrplan.Set{}
		var n, first, last, start int64
		for i := s.Start; i < s.End; i++ {
			g := idx.GTID(i)
			if X.Has(g) {
				continue
			}
			applied = applied.Union(pitrplan.SetOf(g))
			replayed = append(replayed, propCommit{g, idx.Txns[i].OCT})
			n++
			if first == 0 {
				first, start = idx.Txns[i].OCT, idx.Txns[i].Pos
			}
			last = idx.Txns[i].OCT
		}
		if !applied.Equal(s.GTIDs) || n != s.Transactions || first != s.FirstCommit || last != s.LastCommit || applied.Overlaps(state) {
			t.Fatalf("%s: segment %d declares %s (%d, %d..%d); its file range holds %s (%d, %d..%d), state %s",
				what, k, s.GTIDs, s.Transactions, s.FirstCommit, s.LastCommit, applied, n, first, last, state)
		}
		// --start-position: the offset of the first replayed transaction.
		if s.StartPosition != start || start < 4 || start >= idx.Size {
			t.Fatalf("%s: segment %d starts at offset %d; its first replayed transaction is at %d of %d bytes", what, k, s.StartPosition, start, idx.Size)
		}
		state = state.Union(applied)
	}
	if !state.Equal(plan.Lineage.Subtract(X)) {
		t.Fatalf("%s: final %s, Λ∖X %s", what, state, plan.Lineage.Subtract(X))
	}
	return replayed
}

// TestPITRPlanProperty holds the planner as it ships, with the containment
// anchor off, to the model.
func TestPITRPlanProperty(t *testing.T) {
	// 32 seeds, so that the default run holds the shipped planner to both
	// checks on a replica copy that a time anchor accepts. A planner without
	// the inclusion check (the copy follows all of its anchor) certifies a
	// history without transactions the origin had at seed 27, and one
	// without the date check (the copy's predecessors were committed in the
	// episode that must have committed them) replays a transaction its
	// origin had lost at seed 32: 27 and 3 of the first 300 seeds. The
	// trials run real archivers on the wall clock, so which seeds fail such
	// a planner can shift by one.
	propSweep(t, false, 32, "PITR_PLAN_SEEDS")
}

// TestPITRPlanPropertyContainmentAnchors runs the trials of
// TestPITRPlanProperty with the planner's containment anchor on. The anchor
// does not ship (pitrplan.EnableContainmentAnchorsForTest has why and what
// must hold before it does); this keeps the rule held to the model until
// then. The default is 12 seeds: enough to certify plans through a
// containment anchor, which the sweep requires, and too few to fail every
// wrong variant of the rule (a planner that anchors with a whole promotion
// record instead of its promotion set first fails at seed 27). Run
// PITR_PLAN_CONTAINMENT_SEEDS=300 or more when changing the rule.
func TestPITRPlanPropertyContainmentAnchors(t *testing.T) {
	pitrplan.EnableContainmentAnchorsForTest(t)
	propSweep(t, true, 12, "PITR_PLAN_CONTAINMENT_SEEDS")
}

// propSweep runs seeds 1..n (n from the environment variable seedsEnv, else
// defaultSeeds), or the one seed of PITR_PLAN_SEED.
func propSweep(t *testing.T, containment bool, defaultSeeds int64, seedsEnv string) {
	seeds := []int64{}
	if v := os.Getenv("PITR_PLAN_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, n)
	} else {
		last := defaultSeeds
		if v := os.Getenv(seedsEnv); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			last = n
		}
		for s := int64(1); s <= last; s++ {
			seeds = append(seeds, s)
		}
	}
	planned, refused, contained := 0, 0, 0
	crashed, reissued, crashedPlanned := 0, 0, 0
	for _, seed := range seeds {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			st := runPropTrial(t, seed, containment)
			planned += st.planned
			refused += st.refused
			contained += st.contained
			crashedPlanned += st.crashedPlanned
			if st.crashed {
				crashed++
			}
			if st.reissued {
				reissued++
			}
		})
	}
	t.Logf("containment anchors on: %v. %d certified plans (%d through a containment anchor), %d refusals over %d trials; an old primary lost its tail and rejoined in %d trials (%d certified plans), and issued a lost GTID again in %d",
		containment, planned, contained, refused, len(seeds), crashed, crashedPlanned, reissued)
	if len(seeds) > 1 && planned < len(seeds) {
		t.Errorf("the sweep certified only %d plans; the generator is not exercising the planner", planned)
	}
	if containment && len(seeds) >= 10 && contained == 0 {
		t.Errorf("no plan of the sweep needed a containment anchor; the generator is not exercising the lag window")
	}
	if len(seeds) >= 30 && (reissued == 0 || crashedPlanned == 0) {
		t.Errorf("an old primary lost its tail in %d trials and issued a lost GTID again in %d, with %d certified plans; the generator is not exercising a rejoin after a loss", crashed, reissued, crashedPlanned)
	}
}

// TestPITRPlanPropertyProductionMargin plans with the production clock-skew
// margin (F231) and the production confirmation delay over a planned
// failover to b and failback to a in which b never seals a binlog of its
// era, so the era is archived only as replica copies in a's file and every
// target inside it needs b's promotion record as anchor.
//
//   - spaced: b commits more than δ after its record, which anchors those
//     targets by time;
//   - unspaced: b commits within δ of its record and is demoted before the
//     record is confirmed, so the targets refuse PITRStopAmbiguous;
//   - confirmed (#269): b commits within δ of its record and stays primary
//     until its sidecar confirms the record (2δ later). The planner as it
//     ships does not anchor with that evidence, and the targets refuse
//     PITRStopAmbiguous with the safe target, as in the unspaced case. With
//     the containment anchor on, the confirmation holds b's commits and the
//     record's promotion set does not, so the record anchors them.
//
// Every case is planned as shipped and again with the containment anchor
// on. The model oracle checks every other answer.
func TestPITRPlanPropertyProductionMargin(t *testing.T) {
	delta := pitrplan.DefaultClockSkewMargin
	// The cases build their histories in parallel and plan one at a time:
	// the containment anchor is one switch for the whole planner.
	var planning sync.Mutex
	for _, tc := range []struct {
		name            string
		spaced, confirm bool
	}{{"spaced", true, false}, {"unspaced", false, false}, {"confirmed", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newPropTrial(t, 0)
			first := p.w.manifest("a").Promotions[0].ObservedAt
			for range 3 {
				p.commit("a")
			}
			p.takeBackup("a")
			for range 3 {
				p.commit("a")
				p.target("a")
			}
			switch {
			case tc.spaced:
				// b's record must not fall within δ of a's.
				sleepPast(first, delta)
			case tc.confirm:
				// a's record, another server's, must be more than 2δ
				// before b's commits to be no fence of b's record.
				sleepPast(first, 2*delta)
			}
			recB := p.failover("a", "b", 0, false, propPromotion{rotate: true})
			if tc.spaced {
				sleepPast(recB, delta)
			}
			var eraB []time.Time
			for range 3 {
				p.commit("b")
				eraB = append(eraB, p.target("b"))
			}
			if tc.confirm {
				p.w.eventually("the confirmation of b's record", func() bool { return !p.w.manifest("b").Promotions[0].ConfirmedAt.IsZero() })
			}
			p.failover("b", "a", 0, false, propPromotion{rotate: true})
			for range 2 {
				p.commit("a")
				p.target("a")
			}
			p.srv["a"].rotate()
			p.waitUploaded("a")
			rec := p.w.manifest("b").Promotions[0]
			if rec.ConfirmedAt.IsZero() == tc.confirm {
				t.Fatalf("b's record: %+v", rec)
			}
			if tc.confirm && rec.ConfirmedAt.Before(rec.ObservedAt.Add(pitrarchive.PromotionConfirmDelay)) {
				t.Fatalf("b's record was confirmed %s after it was observed", rec.ConfirmedAt.Sub(rec.ObservedAt))
			}
			safe := "or choose a target after " + recB.Add(delta).UTC().Format("2006-01-02T15:04:05.000000Z") + " (b's promotion record at "

			plan := func(t *testing.T, containment bool) {
				o := newPropOracle(t, p, delta)
				o.containment = containment
				for _, T := range eraB {
					w, _ := o.witness(T, pitrplan.Set{})
					if o.originCopy(w) {
						t.Fatalf("the fixture archived b's commit %s on b", w)
					}
				}
				res := o.run(tc.name)
				for _, r := range res {
					inB := false
					for _, T := range eraB {
						inB = inB || r.T.Equal(T)
					}
					at := r.T.Format(time.RFC3339Nano)
					switch {
					case r.mode == pitrplan.ModeLatest && r.err != nil:
						t.Errorf("latest mode refused: %v", r.err)
					case inB && tc.spaced:
						if r.err != nil || o.byContainment(r.plan) {
							t.Errorf("T=%s inside b's era, more than δ after b's record: want a plan anchored by time, got %+v %v", at, r.plan, r.err)
						}
					case inB && tc.confirm && containment:
						if r.err != nil || !o.byContainment(r.plan) || len(r.plan.Witness.AnchorRecords) != 1 || r.plan.Witness.AnchorRecords[0] != "b#0" {
							t.Errorf("T=%s inside b's era, within δ of b's confirmed record: want a plan anchored by b#0, got %+v %v", at, r.plan, r.err)
						}
					case inB:
						ref, ok := pitrplan.RefusalOf(r.err)
						if !ok || ref.Token != pitrarchive.TokenStopAmbiguous {
							t.Errorf("T=%s inside b's era, within δ of b's record: want PITRStopAmbiguous, got %v", at, r.err)
							continue
						}
						if !strings.Contains(ref.Message, safe) || !strings.Contains(ref.Message, "which is not provably before") {
							t.Errorf("T=%s: the refusal does not name the safe target (%q): %s", at, safe, ref.Message)
						}
						switch {
						case containment && !strings.Contains(ref.Message, "it has no confirmation (confirmedAt)"):
							t.Errorf("T=%s: the refusal does not say what b's record lacks: %s", at, ref.Message)
						case !containment && (strings.Contains(ref.Message, "containment") || strings.Contains(ref.Message, "confirm") || strings.Contains(ref.Message, "promotion set")):
							// The shipped planner does not anchor with a
							// promotion set, confirmed or not: the refusal
							// must not send the operator after one.
							t.Errorf("T=%s: the shipped planner's refusal speaks of evidence it does not use: %s", at, ref.Message)
						}
					}
				}
			}
			planning.Lock()
			defer planning.Unlock()
			plan(t, false)
			t.Run("containment anchors", func(t *testing.T) {
				pitrplan.EnableContainmentAnchorsForTest(t)
				plan(t, true)
			})
		})
	}
}
