package runner

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestParseShard(t *testing.T) {
	cases := []struct {
		in      string
		want    Shard
		wantErr bool
	}{
		{in: "", want: Shard{}},
		{in: "  ", want: Shard{}},
		{in: "1/1", want: Shard{Index: 1, Total: 1}},
		{in: "2/4", want: Shard{Index: 2, Total: 4}},
		{in: " 4 / 4 ", want: Shard{Index: 4, Total: 4}},
		{in: "0/4", wantErr: true},
		{in: "5/4", wantErr: true},
		{in: "1/0", wantErr: true},
		{in: "-1/4", wantErr: true},
		{in: "2", wantErr: true},
		{in: "a/b", wantErr: true},
		{in: "1/2/3", wantErr: true},
		{in: "1.5/4", wantErr: true},
	}
	for _, tc := range cases {
		got, err := ParseShard(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseShard(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseShard(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
		if err == nil && tc.in != "" && !got.IsZero() {
			if back, err := ParseShard(got.String()); err != nil || back != got {
				t.Errorf("ParseShard(%q).String() = %q does not parse back", tc.in, got.String())
			}
		}
	}
}

func TestSelectShardZeroIsWholeList(t *testing.T) {
	scens := []Scenario{{ID: "a"}, {ID: "b"}}
	got, err := SelectShard(scens, Shard{})
	if err != nil || len(got) != 2 {
		t.Fatalf("zero shard: got %d scenarios, err %v; want the whole list", len(got), err)
	}
}

func TestSelectShardRejectsBadInput(t *testing.T) {
	scens := []Scenario{{ID: "a"}, {ID: "b"}}
	for _, sh := range []Shard{{Index: 0, Total: 2}, {Index: 3, Total: 2}, {Index: 1, Total: 0}, {Index: -1, Total: -1}} {
		if _, err := SelectShard(scens, sh); err == nil {
			t.Errorf("SelectShard(%+v): want an error", sh)
		}
	}
	// A repeated ID would be assigned to one shard per occurrence; the
	// partition guarantee is stated over IDs, so it is refused.
	if _, err := SelectShard([]Scenario{{ID: "a"}, {ID: "a"}}, Shard{Index: 1, Total: 2}); err == nil {
		t.Error("a list with a repeated ID must be refused")
	}
}

// assertShardsPartition fails t unless the total shards of scens are
// disjoint, cover scens, and each keeps the order of scens. It returns the
// shards. The external tests reach it through export_test.go.
func assertShardsPartition(t *testing.T, scens []Scenario, total int) [][]Scenario {
	t.Helper()
	pos := make(map[string]int, len(scens))
	for i, s := range scens {
		pos[s.ID] = i
	}
	owner := map[string]int{}
	shards := make([][]Scenario, 0, total)
	for i := 1; i <= total; i++ {
		got, err := SelectShard(scens, Shard{Index: i, Total: total})
		if err != nil {
			t.Fatalf("SelectShard %d/%d: %v", i, total, err)
		}
		last := -1
		for _, s := range got {
			p, known := pos[s.ID]
			if !known {
				t.Fatalf("shard %d/%d has %q, which is not in the list", i, total, s.ID)
			}
			if prev, dup := owner[s.ID]; dup {
				t.Fatalf("%q is in shard %d and shard %d of %d", s.ID, prev, i, total)
			}
			owner[s.ID] = i
			if p <= last {
				t.Fatalf("shard %d/%d does not keep the list order at %q", i, total, s.ID)
			}
			last = p
		}
		shards = append(shards, got)
	}
	for _, s := range scens {
		if _, ok := owner[s.ID]; !ok {
			t.Fatalf("%q is in no shard of %d: it would never run", s.ID, total)
		}
	}
	if len(owner) != len(scens) {
		t.Fatalf("%d shards hold %d scenarios, the list has %d", total, len(owner), len(scens))
	}
	return shards
}

func TestSelectShardPartitionsAnyList(t *testing.T) {
	rng := rand.New(rand.NewSource(236))
	for trial := 0; trial < 200; trial++ {
		n := rng.Intn(80)
		scens := make([]Scenario, n)
		for i := range scens {
			scens[i] = Scenario{
				ID:                fmt.Sprintf("%02d-s", i),
				ResetBeforeRunAll: rng.Intn(6) == 0,
			}
			// Zero (the default), equal budgets and a few very long ones.
			switch rng.Intn(4) {
			case 0:
			case 1:
				scens[i].Timeout = 5 * time.Minute
			case 2:
				scens[i].Timeout = time.Duration(1+rng.Intn(30)) * time.Minute
			default:
				scens[i].Timeout = time.Duration(60+rng.Intn(30)) * time.Minute
			}
		}
		for total := 1; total <= 9; total++ {
			shards := assertShardsPartition(t, scens, total)

			// Balance: taking the largest first and always filling the
			// lightest shard keeps any two shards within one scenario
			// (the largest) of each other.
			var largest, lo, hi time.Duration
			for i, shard := range shards {
				var load time.Duration
				for _, s := range shard {
					load += ShardBudget(s)
					if b := ShardBudget(s); b > largest {
						largest = b
					}
				}
				if i == 0 || load < lo {
					lo = load
				}
				if load > hi {
					hi = load
				}
			}
			if hi-lo > largest {
				t.Fatalf("trial %d, %d shards: budgets range %s to %s, more than the largest scenario (%s) apart", trial, total, lo, hi, largest)
			}
		}
	}
}

func TestSelectShardIsDeterministic(t *testing.T) {
	scens := []Scenario{
		{ID: "01-a", Timeout: 4 * time.Minute},
		{ID: "02-b", Timeout: 60 * time.Minute, ResetBeforeRunAll: true},
		{ID: "03-c"},
		{ID: "04-d", Timeout: 60 * time.Minute},
		{ID: "05-e", Timeout: 5 * time.Minute},
		{ID: "06-f", Timeout: 20 * time.Minute},
	}
	ids := func(in []Scenario) string {
		out := ""
		for _, s := range in {
			out += s.ID + " "
		}
		return out
	}
	// Largest first: 02-b (65m) -> 1, 04-d (60m) -> 2, 06-f (20m) -> 2
	// (60 < 65), then 03-c and 05-e (5m each, by ID) and 01-a (4m) to the
	// lighter shard each time: 03-c -> 1 (65 < 80), 05-e -> 1 (70 < 80),
	// 01-a -> 1 (75 < 80).
	want := map[int]string{1: "01-a 02-b 03-c 05-e ", 2: "04-d 06-f "}
	for i := 1; i <= 2; i++ {
		for range 3 {
			got, err := SelectShard(scens, Shard{Index: i, Total: 2})
			if err != nil {
				t.Fatal(err)
			}
			if ids(got) != want[i] {
				t.Fatalf("shard %d/2 = %q, want %q", i, ids(got), want[i])
			}
		}
	}
	// More shards than scenarios: the extra shards are empty, not an error.
	shards := assertShardsPartition(t, scens[:2], 5)
	empty := 0
	for _, s := range shards {
		if len(s) == 0 {
			empty++
		}
	}
	if empty != 3 {
		t.Fatalf("2 scenarios over 5 shards left %d shards empty, want 3", empty)
	}
}

func TestShardBudget(t *testing.T) {
	if got := ShardBudget(Scenario{}); got != DefaultScenarioTimeout {
		t.Errorf("no Timeout: budget %s, want the default %s", got, DefaultScenarioTimeout)
	}
	if got := ShardBudget(Scenario{Timeout: time.Hour, ResetBeforeRunAll: true, CleanupTimeout: time.Hour}); got != time.Hour+ResetAllowance {
		t.Errorf("budget %s, want Timeout plus the reset allowance (cleanup not counted)", got)
	}
}
