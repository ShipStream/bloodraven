package runner

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Shard names one of Total disjoint parts of a profile's scenario list.
// Index is 1-based. The zero value means "not sharded": the whole list.
//
// CI runs the full profile as Total jobs, each on its own cluster, because
// one job cannot finish every scenario inside the hosted runner's limit
// (#236).
type Shard struct {
	Index int
	Total int
}

// IsZero reports whether s selects the whole list.
func (s Shard) IsZero() bool { return s == Shard{} }

// String renders the shard as run-all's --shard takes it ("2/4"), or ""
// for the zero value.
func (s Shard) String() string {
	if s.IsZero() {
		return ""
	}
	return fmt.Sprintf("%d/%d", s.Index, s.Total)
}

// ParseShard parses "<index>/<total>" with 1 <= index <= total. The empty
// string is the zero Shard.
func ParseShard(v string) (Shard, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return Shard{}, nil
	}
	idx, total, ok := strings.Cut(v, "/")
	if !ok {
		return Shard{}, fmt.Errorf("invalid shard %q: want <index>/<total>, e.g. 2/4", v)
	}
	i, errI := strconv.Atoi(strings.TrimSpace(idx))
	n, errN := strconv.Atoi(strings.TrimSpace(total))
	if errI != nil || errN != nil {
		return Shard{}, fmt.Errorf("invalid shard %q: want <index>/<total>, e.g. 2/4", v)
	}
	if n < 1 || i < 1 || i > n {
		return Shard{}, fmt.Errorf("invalid shard %q: index must be between 1 and the total", v)
	}
	return Shard{Index: i, Total: n}, nil
}

// DefaultScenarioTimeout is the step budget of a scenario that does not set
// Timeout.
const DefaultScenarioTimeout = 5 * time.Minute

// ResetAllowance is what ShardBudget charges for the playground reset that
// run-all does before a scenario with ResetBeforeRunAll. Resets measured
// 3 to 5 minutes on a hosted runner.
const ResetAllowance = 5 * time.Minute

// ShardBudget is the weight SelectShard balances by: the scenario's own
// Timeout plus the reset run-all does before it. It is the declared
// worst case of the scenario's steps, not a measurement. The runner keeps
// no record of measured durations, and the budget is also what a job
// timeout has to cover. CleanupTimeout is left out, as in the e2e pitr
// job's guard: it is an allowance for a cleanup that hangs, not a planned
// spend.
func ShardBudget(s Scenario) time.Duration {
	b := s.Timeout
	if b <= 0 {
		b = DefaultScenarioTimeout
	}
	if s.ResetBeforeRunAll {
		b += ResetAllowance
	}
	return b
}

// SelectShard returns the scenarios of one shard, in the order they have in
// scens. The Total shards of a list are disjoint and their union is the
// list, for any list and any Total, so a scenario cannot fall out of every
// shard. A Total larger than the list leaves some shards empty.
//
// Assignment is deterministic for a given list: scenarios are taken from
// the largest ShardBudget to the smallest (ties by ID) and each goes to the
// shard with the smallest budget so far (ties to the lowest index). The
// long scenarios are spread out first and the short ones even out the rest.
// Adding or removing a scenario, or changing a Timeout, can move other
// scenarios between shards; every shard of one run must therefore come from
// the same build.
func SelectShard(scens []Scenario, shard Shard) ([]Scenario, error) {
	if shard.IsZero() {
		return scens, nil
	}
	if shard.Total < 1 || shard.Index < 1 || shard.Index > shard.Total {
		return nil, fmt.Errorf("invalid shard %d/%d: index must be between 1 and the total", shard.Index, shard.Total)
	}
	seen := make(map[string]bool, len(scens))
	for _, s := range scens {
		if seen[s.ID] {
			return nil, fmt.Errorf("cannot shard: scenario %q is listed twice", s.ID)
		}
		seen[s.ID] = true
	}
	order := make([]int, len(scens))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		sa, sb := scens[order[a]], scens[order[b]]
		if ba, bb := ShardBudget(sa), ShardBudget(sb); ba != bb {
			return ba > bb
		}
		return sa.ID < sb.ID
	})
	load := make([]time.Duration, shard.Total)
	mine := make([]bool, len(scens))
	for _, i := range order {
		target := 0
		for j := 1; j < shard.Total; j++ {
			if load[j] < load[target] {
				target = j
			}
		}
		load[target] += ShardBudget(scens[i])
		mine[i] = target == shard.Index-1
	}
	out := make([]Scenario, 0, len(scens)/shard.Total+1)
	for i, s := range scens {
		if mine[i] {
			out = append(out, s)
		}
	}
	return out, nil
}
