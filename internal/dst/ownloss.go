package dst

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"

	"github.com/shipstream/bloodraven/internal/controller"
)

// A site that comes back without transactions it issued itself, and the
// reclone that repairs it (controller/own_transactions_lost.go).
//
// Three pieces of MySQL behaviour are modelled here, each measured on
// MySQL 9.7:
//
//   - A replication receiver drops every event that carries the replica's
//     own server_id, whatever server_uuid issued it (ownsStream, used by
//     Cluster.Tick). A site keeps its server_id for life, so the streams a
//     site "owns" are the ones under its original uuid and under every
//     server_uuid a pod replacement or a clone gave it. Without this the
//     model would let a site refetch its own lost tail, which no server
//     does.
//   - A site can restart on an older data directory with the identity it
//     had (RegressSite): a volume snapshot that was rolled back, or a crash
//     that lost the tail of the redo log and the binary log together.
//   - CLONE INSTANCE makes the recipient a copy of the donor under a new
//     server_uuid, with no replication channel (Reclone). From then on the
//     recipient holds of its earlier streams exactly what the donor held,
//     and never receives more of them: what another site holds beyond
//     that the recipient gave up at the clone, it did not lose it
//     (supersededByClone).

// EvRegress is a site restarting on an older data directory under the
// server_uuid it had. EvReclone is a clone installed into a site.
const (
	EvRegress EventKind = "fault.regressSite"
	EvReclone EventKind = "reclone"
)

// OpRegressSite crashes Site (if it is up) and rolls its data directory
// back K ticks, keeping its server_uuid. It is generated from a PRNG
// stream of its own (ownLossFaults), with the OpRegressBoot that boots the
// site again, so it adds to a seed's schedule without changing the ops it
// had before.
const OpRegressSite OpKind = "regressSite"

// OpRegressBoot boots a site an OpRegressSite crashed: fenced (its sidecar
// won the boot race) or with the read_only=ON mysqld starts with. It is a
// kind of its own so the regress faults can be told from the schedule's
// other recoveries.
const OpRegressBoot OpKind = "regressBoot"

// ownsStream reports whether the GTID stream u was issued by site s: under
// its original uuid, or under a server_uuid a pod replacement or a clone
// gave it (replacementUUID keeps everything after the first group). Events
// of such a stream carry the site's server_id.
func ownsStream(s *siteData, u string) bool {
	if u == s.uuid || u == s.serverUUID {
		return true
	}
	return len(u) > 8 && len(s.uuid) > 8 && u[8:] == s.uuid[8:]
}

// RegressSite crashes the site (a regress needs a restart) and rolls its
// data directory back to the executed set of back ticks ago, under the
// same server_uuid. Like ReplacePod's regress it is applied only when
// every transaction it rolls back is still held by another site: losing
// the last copy of an acknowledged write is data loss no operator can
// repair, which the end-state invariants would rightly call the model's
// doing. It reports whether the data directory was rolled back.
func (c *Cluster) RegressSite(site string, back int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.byName[site]
	if !s.crashed {
		s.crashed = true
		s.incarnation++
	}
	detail := fmt.Sprintf("back=%d", back)
	regressed := false
	switch {
	case back <= 0 || len(s.history) == 0:
		detail += " regress=skipped(no history)"
	default:
		snap := s.history[max(len(s.history)-1-back, 0)]
		lost := s.executed.deficit(snap) == 0 && snap.deficit(s.executed) > 0
		switch {
		case !lost:
			detail += " regress=skipped(nothing to lose)"
		case !c.heldElsewhereLocked(s, snap):
			detail += " regress=skipped(sole holder)"
		default:
			own := s.executed[s.serverUUID] - snap[s.serverUUID]
			s.executed = snap.clone()
			s.retrieved = snap.clone()
			regressed = true
			detail += fmt.Sprintf(" regress=applied ownLost=%d executed=%s", own, s.executed)
		}
	}
	c.event(site, EvRegress, detail, "")
	return regressed
}

// Reclone applies a completed CLONE INSTANCE of recipient from donor: the
// recipient holds the donor's executed set, has no replication channel and
// boots read_only under a new server_uuid, as a new server process. It
// refuses (false) when the operator cannot reach the recipient, or the
// recipient cannot reach a running donor: the clone could not have run.
func (c *Cluster) Reclone(donor, recipient string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, r := c.byName[donor], c.byName[recipient]
	if d == nil || r == nil || d == r || c.operatorDead {
		return false
	}
	if d.crashed || r.crashed || c.opLinkDown[r.name] || c.dirLinkDown[dirKey(r.name, d.name)] {
		c.event(recipient, EvReclone, "donor="+donor, "unreachable")
		return false
	}
	r.executed = d.executed.clone()
	r.retrieved = make(gtidVec)
	r.history = nil
	r.replConfigured = false
	r.sourceHost = ""
	r.ioWant, r.sqlWant = false, false
	r.ioErr, r.sqlErr = "", ""
	r.readOnly, r.superReadOnly = true, false
	r.hasData = d.hasData
	r.binlogSeq, r.binlogPrev = 0, nil
	r.incarnation++
	r.serverUUID = replacementUUID(r.uuid, r.incarnation)
	r.clonedAt = r.incarnation
	c.event(recipient, EvReclone, fmt.Sprintf("donor=%s server_uuid=%s executed=%s", donor, r.serverUUID, r.executed), "applied")
	return true
}

// supersededByClone reports whether the stream u of site s is one a clone
// into s superseded: a stream s owned before its last Reclone. The uuid's
// first group is the incarnation that issued it (replacementUUID), and a
// clone gives the site the uuid of the incarnation it creates.
func supersededByClone(s *siteData, u string) bool {
	if s.clonedAt == 0 || len(u) < 8 {
		return false
	}
	n, err := strconv.ParseUint(u[:8], 16, 64)
	return err == nil && n < uint64(s.clonedAt)
}

// ownsLiveStream is ownsStream && !supersededByClone on a truth snapshot:
// the streams of t whose transactions t can only lack by having lost them.
func (t SiteTruth) ownsLiveStream(u string) bool {
	own := u == t.ServerUUID || (len(u) > 8 && len(t.ServerUUID) > 8 && u[8:] == t.ServerUUID[8:])
	if !own {
		return false
	}
	if t.ClonedAt == 0 || len(u) < 8 {
		return true
	}
	n, err := strconv.ParseUint(u[:8], 16, 64)
	return err != nil || n >= uint64(t.ClonedAt)
}

// checkOwnHistoryAtGrantLocked is the model-level invariant
// PromotedWithoutOwnTransactions: an operator actor must never make a
// site writable while another site holds transactions of one of that
// site's own streams that the site itself lost. Such a site cannot get
// them back (its receiver drops them), and its next write under its
// current server_uuid would be issued under a GTID that already names
// another transaction. A stream a clone into the site superseded is not
// judged: what another site holds of it beyond the site's copy the site
// gave up at the clone (supersededByClone). wasWritable excludes a
// statement on a site that was writable already (an external writer's
// doing, or its boot): nothing is granted by that. Must be called with
// c.mu held, before the grant is applied.
func (c *Cluster) checkOwnHistoryAtGrantLocked(s *siteData, wasWritable bool) {
	if wasWritable {
		return
	}
	for _, o := range c.sites {
		if o == s || o.crashed || c.opLinkDown[o.name] {
			// Only a holder the operator can read is evidence it has. A
			// site promoted while every holder of its lost transactions
			// is down or cut off is promoted on what the operator can
			// know; that history is a documented residual, not judged
			// here (README, Model fidelity notes).
			continue
		}
		for u, n := range o.executed {
			if ownsStream(s, u) && !supersededByClone(s, u) && n > s.executed[u] {
				c.violate("PromotedWithoutOwnTransactions",
					fmt.Sprintf("%s made writable with %s:%d although %s holds %s:1-%d: the site lacks transactions it issued itself and can never receive them",
						s.name, u, s.executed[u], o.name, u, n))
				return
			}
		}
	}
}

// ownLossFaultStream salts the PRNG stream of the regress faults.
const ownLossFaultStream = 0x0b5e55ed10571a11

// ownLossFaults draws a trial's data-directory regress: in one trial out
// of four, one site (three times out of five the initial primary, whose
// own stream is the one its peers hold) restarts on a data directory one
// to six ticks old, and boots again one to twelve polls later (or at
// heal-all), fenced or with the read_only it boots with. A stream of its
// own keeps the rest of every seed's schedule as it was
// (TestGenerateTrial_OwnLossFaultsOnlyAdd).
func ownLossFaults(seed uint64, names []string, warmup, faultWindow, healAt int) []Op {
	rng := rand.New(rand.NewPCG(seed, ownLossFaultStream))
	if rng.IntN(100) >= 25 {
		return nil
	}
	site := names[0]
	if rng.IntN(5) >= 3 {
		site = names[rng.IntN(len(names))]
	}
	at := warmup + rng.IntN(faultWindow)
	ops := []Op{{At: at, Kind: OpRegressSite, Site: site, K: 1 + rng.IntN(6)}}
	back := at + 1 + rng.IntN(12)
	fenced := rng.IntN(2) == 0
	if back < healAt {
		ops = append(ops, Op{At: back, Kind: OpRegressBoot, Site: site, Fenced: fenced})
	}
	return ops
}

// OpFailedClone is a clone the operator issued into Site that failed or
// was killed before it installed: the operator records what it records
// for every clone it issues, and the site keeps its data and its
// server_uuid. What that record must not do is make the operator treat the
// identity the site still runs under as one a clone superseded.
//
// OpSilentWitness cuts Site off from the operator for N polls and, in the
// same poll, restarts the operator (K=0) or replaces the group's manager
// (K=1; a restart in a trial without the actors): the new manager starts
// from the stored status while one site does not answer. A report that
// rests on that site must survive it. OpSilentWitnessHeal ends the cut.
//
// All three are drawn from a PRNG stream of their own
// (ownLossEvidenceFaults), so they add to a seed's schedule without
// changing the ops it had before.
const (
	OpFailedClone       OpKind = "failedClone"
	OpSilentWitness     OpKind = "silentWitness"
	OpSilentWitnessHeal OpKind = "silentWitnessHeal"

	EvFailedClone EventKind = "fault.failedClone"
)

// NoteFailedClone records a failed clone into site in the event log.
func (c *Cluster) NoteFailedClone(site string, recorded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	detail := "recorded"
	if !recorded {
		detail = "skipped(operator could not read the site)"
	}
	c.event(site, EvFailedClone, detail, "")
}

// ownLossEvidenceStream salts the PRNG stream of the evidence faults.
const ownLossEvidenceStream = 0x0e71de9ce5f6a110

// ownLossEvidenceFaults draws the faults that test what the operator's
// own-loss evidence survives: in one trial out of five a failed clone
// into one site; in one out of four a site that falls silent for 3 to 42
// polls as the operator restarts or its manager is replaced. A stream of
// its own keeps the rest of every seed's schedule as it was
// (TestGenerateTrial_OwnLossEvidenceFaultsOnlyAdd).
func ownLossEvidenceFaults(seed uint64, names []string, warmup, faultWindow, healAt int) []Op {
	rng := rand.New(rand.NewPCG(seed, ownLossEvidenceStream))
	var ops []Op
	if rng.IntN(100) < 20 {
		ops = append(ops, Op{At: warmup + rng.IntN(faultWindow), Kind: OpFailedClone, Site: names[rng.IntN(len(names))]})
	}
	if rng.IntN(100) < 25 {
		site := names[rng.IntN(len(names))]
		at := warmup + rng.IntN(faultWindow)
		n := 3 + rng.IntN(40)
		ops = append(ops, Op{At: at, Kind: OpSilentWitness, Site: site, N: n, K: rng.IntN(2)})
		if at+n < healAt {
			ops = append(ops, Op{At: at + n, Kind: OpSilentWitnessHeal, Site: site})
		}
	}
	return ops
}

// simSiteIdentityStore is the durable record of the server_uuids each site
// has run under and of the clones issued into it (the group annotation in
// production). It outlives every manager of the trial, as the annotation
// outlives the operator.
type simSiteIdentityStore struct {
	mu    sync.Mutex
	uuids map[string][]string
}

func (s *simSiteIdentityStore) Load(context.Context) (map[string][]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]string, len(s.uuids))
	for site, uuids := range s.uuids {
		out[site] = append([]string(nil), uuids...)
	}
	return out, nil
}

func (s *simSiteIdentityStore) Add(_ context.Context, site, serverUUID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uuids == nil {
		s.uuids = map[string][]string{}
	}
	s.uuids[site] = controller.AppendServerUUIDEntryForSim(s.uuids[site], serverUUID)
	return nil
}
