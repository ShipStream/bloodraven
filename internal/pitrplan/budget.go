package pitrplan

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Refusal is a planner refusal. Its message starts with the token, so it is
// the Job's termination message unchanged (§7.1).
type Refusal struct {
	Token   pitrarchive.Token
	Message string
}

func (r *Refusal) Error() string { return string(r.Token) + ": " + r.Message }

func refuse(tok pitrarchive.Token, format string, args ...any) *Refusal {
	return &Refusal{Token: tok, Message: fmt.Sprintf(format, args...)}
}

// RefusalOf unwraps a planner refusal.
func RefusalOf(err error) (*Refusal, bool) {
	var r *Refusal
	ok := errors.As(err, &r)
	return r, ok
}

// Memory budget (D-42). Every structure whose size grows with the archive
// is charged before it is allocated, so planning refuses PITRPlanTooLarge
// instead of being OOM-killed.
const (
	// DefaultPlanMemoryBytes applies when the cgroup has no memory limit.
	DefaultPlanMemoryBytes = 512 << 20
	// MaxEventBytesCap is the largest binlog event (or decompressed
	// transaction payload) the index pass decodes.
	MaxEventBytesCap = 64 << 20

	// TxnBytes is charged per indexed transaction: the Txn record itself.
	TxnBytes = int64(unsafe.Sizeof(Txn{}))
	// occurrenceBytes is charged per indexed transaction for the
	// occurrence registry (FP comparison, lookups) and the D-51 order
	// check's rank and edge arrays.
	occurrenceBytes = 64 + 12
	// nodeBytes is charged per search position (§4.6) for every array
	// the search allocates per position: hash 8, count 8, class 4,
	// distance 4, start/goal/needed/step/inX/done 6, the bucket sort index
	// 4, class membership 4, the BFS deque 8 (it may double), and the
	// no-plan diagnosis' reach flag and queue 5.
	nodeBytes = 8 + 8 + 4 + 4 + 6 + 4 + 4 + 8 + 5
	// classBytes is charged per exact-key class besides its key's bytes:
	// the key's map slot with growth headroom and string header (64), its
	// members slice header (24), first file and span flag (5), and BFS
	// state (8).
	classBytes = 64 + 24 + 5 + 8
	// intervalBytes is charged per interval of a materialized set.
	intervalBytes = int64(unsafe.Sizeof(Interval{})) + 8
)

// memProbe, when set (tests), runs where the planner's largest transient
// structures are live: at the end of the search graph build, of the BFS and
// of the order check's graph build. TestBudgetBoundsLiveHeap measures the
// live heap there against the charges.
var memProbe func()

func probeMemory() {
	if memProbe != nil {
		memProbe()
	}
}

// Budget is the planner's incremental memory budget. It is not safe for
// concurrent use; the planner is single-threaded.
type Budget struct {
	limit, used, peak int64
	indexed           int64
}

// NewBudget returns a budget of limit bytes.
func NewBudget(limit int64) *Budget { return &Budget{limit: limit} }

// Limit is the budget.
func (b *Budget) Limit() int64 { return b.limit }

// Used is the current charge.
func (b *Budget) Used() int64 { return b.used }

// Peak is the largest charge seen.
func (b *Budget) Peak() int64 { return b.peak }

// MaxEventBytes is min(64 MiB, budget/8): the decoder bound (D-42).
func (b *Budget) MaxEventBytes() int64 {
	return min(int64(MaxEventBytesCap), b.limit/8)
}

// Charge reserves n bytes for what, or refuses PITRPlanTooLarge.
func (b *Budget) Charge(n int64, what string) error {
	if n < 0 {
		return fmt.Errorf("pitrplan: negative charge %d for %s", n, what)
	}
	if b.used+n > b.limit {
		return refuse(pitrarchive.TokenPlanTooLarge, "planning needs more than %d bytes (%s; %d transactions indexed); choose a backup closer to the target or raise the Job's memory limit",
			b.limit, what, b.indexed)
	}
	b.used += n
	b.peak = max(b.peak, b.used)
	return nil
}

// Release returns n bytes.
func (b *Budget) Release(n int64) {
	b.used = max(0, b.used-n)
}

// chargeSet charges a materialized set.
func (b *Budget) chargeSet(s Set, what string) error {
	return b.Charge(int64(s.Intervals())*intervalBytes, what)
}
