package pitrplan

import (
	"context"
	"math"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Hints are a manifest row's content-bound hints (§4.1). Known is false when
// the row's coverage is incomplete; such a row is downloaded whenever it
// could belong to a download set (§4.10).
type Hints struct {
	Known bool
	// Previous is hprev: the row's previousGtids ∪ baseGtids.
	Previous Set
	GTIDs    Set
	Empty    Set
	// Min and Max are hmin and hmax (µs); both 0 for a file with no
	// transactions, or when any transaction lacks a commit timestamp.
	Min, Max int64
}

// timesKnown reports whether hmin/hmax bound the row's transactions.
func (h Hints) timesKnown() bool { return h.Known && (h.Min != 0 || h.GTIDs.IsEmpty()) }

// lo and hi are hmin/hmax with unknown values widened to ±∞; an empty
// file has no transaction, so lo=+∞ and hi=−∞.
func (h Hints) lo() int64 {
	if !h.timesKnown() {
		return math.MinInt64
	}
	if h.GTIDs.IsEmpty() {
		return math.MaxInt64
	}
	return h.Min
}

func (h Hints) hi() int64 {
	if !h.timesKnown() {
		return math.MaxInt64
	}
	if h.GTIDs.IsEmpty() {
		return math.MinInt64
	}
	return h.Max
}

// mayMeet reports whether the row may hold a member of s.
func (h Hints) mayMeet(s Set) bool { return !h.Known || h.GTIDs.Overlaps(s) }

// RowState says where a row is listed.
type RowState uint8

const (
	// RowActive: in the manifest's Files[]. A divergent row is active.
	RowActive RowState = iota
	// RowQuarantined: in Quarantined[].Entry; never data (D-36).
	RowQuarantined
)

// Row is one manifest entry.
type Row struct {
	Site  string
	Name  string
	ID    string
	State RowState
	// Base is the row's baseGtids (bridges a clone).
	Base  Set
	Hints Hints
	// QuarantinedAt is Quarantined[].At for a quarantined row.
	QuarantinedAt time.Time
	// Divergent is the audit's MissingGTIDs when the row is listed in the
	// manifest's Divergent[] (nil otherwise).
	Divergent Set
}

// Label is "<site>/<name>".
func (r *Row) Label() string { return r.Site + "/" + r.Name }

// RowRef identifies a row for the fetcher and in coverage.json.
type RowRef struct {
	Site, Name, ID string
	State          RowState
}

func (r *Row) ref() RowRef { return RowRef{r.Site, r.Name, r.ID, r.State} }

// SiteManifest is one site's version-2 manifest, as the planner needs it.
type SiteManifest struct {
	Site string
	// Marked is the site's history-integrity marker (D-34), or nil.
	Marked     *pitrarchive.HistoryIntegrity
	Rows       []Row
	Promotions []pitrarchive.PromotionRecord
}

// Archive is every site manifest under the prefix.
type Archive struct {
	Sites []SiteManifest
}

// Fetcher downloads, authenticates (digest against the row ID, decoder
// bounds, CRCs) and indexes one row. It returns a *Refusal for an
// integrity failure, which ends planning; any other error means the row is
// unreadable, and the planner decides what that costs in context.
type Fetcher interface {
	Fetch(ctx context.Context, row RowRef) (*FileIndex, error)
}

// FetchFunc adapts a function.
type FetchFunc func(ctx context.Context, row RowRef) (*FileIndex, error)

// Fetch implements Fetcher.
func (f FetchFunc) Fetch(ctx context.Context, row RowRef) (*FileIndex, error) { return f(ctx, row) }

// PlanRule runs after Λ, P, Q and D are known and before staging (§4.12);
// it is the hook for #205.
type PlanRule func(*Plan) error

// DefaultClockSkewMargin is δ (D-50).
const DefaultClockSkewMargin = 5 * time.Second

// Request is what pitr-download asks for.
type Request struct {
	Mode Mode
	// Stop is T, exact to the µs (timestamp mode).
	Stop         time.Time
	AllowPartial bool
	// Exclude is X.
	Exclude Set
	// Delta is δ; 0 means DefaultClockSkewMargin.
	Delta time.Duration
	// Dump is dump.json.
	Dump   backupmeta.DumpMeta
	Budget *Budget
	Rules  []PlanRule
}
