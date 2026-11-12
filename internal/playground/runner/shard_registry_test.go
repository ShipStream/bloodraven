package runner_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/playground/runner"
	_ "github.com/shipstream/bloodraven/internal/playground/scenarios"
)

// TestProfileShardsCoverProfile is the guarantee sharding rests on (#236):
// for every profile and any shard count, the shards are disjoint and their
// union is exactly the profile, so a registered scenario cannot fall out of
// every shard and silently stop running in CI.
func TestProfileShardsCoverProfile(t *testing.T) {
	all := runner.DefaultRegistry.List()
	for _, p := range runner.Profiles() {
		profile := runner.SelectForProfile(all, p)
		if len(profile) == 0 {
			t.Fatalf("profile %q selects nothing", p)
		}
		for total := 1; total <= 12; total++ {
			t.Run(fmt.Sprintf("%s/%d", p, total), func(t *testing.T) {
				runner.AssertShardsPartition(t, profile, total)
			})
		}
	}
}

// ciFullProfileShards reads how e2e.yml runs the full profile: the shard
// matrix, the `shards` input it passes with it, and the job timeout of a
// dispatched run.
func ciFullProfileShards(t *testing.T) (shards int, timeout time.Duration) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", "e2e.yml"))
	if err != nil {
		t.Fatal(err)
	}
	_, job, ok := strings.Cut(string(raw), "\n  e2e:\n")
	if !ok {
		t.Fatal("e2e.yml has no e2e job")
	}
	job, _, _ = strings.Cut(job, "\n\n")

	m := regexp.MustCompile(`shard: \$\{\{ fromJSON\(github\.event\.inputs\.profile == 'full' && '\[([0-9, ]+)\]' \|\| '\[0\]'\) \}\}`).FindStringSubmatch(job)
	if m == nil {
		t.Fatal("the e2e job must run the full profile as a shard matrix ('[1, 2, ...]') and every other profile as the single unsharded entry '[0]'")
	}
	list := strings.Split(m[1], ",")
	for i, v := range list {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err != nil || n != i+1 {
			t.Fatalf("the full-profile shard matrix must be 1..n in order, got [%s]", m[1])
		}
	}
	m = regexp.MustCompile(`\n\s+shards: ([0-9]+)\n`).FindStringSubmatch(job)
	if m == nil {
		t.Fatal("the e2e job must pass `shards: <n>`")
	}
	shards, _ = strconv.Atoi(m[1])
	if shards != len(list) {
		t.Fatalf("the e2e job passes shards: %d but its matrix has %d shards; a mismatch would run the wrong parts", shards, len(list))
	}
	if !strings.Contains(job, "shard: ${{ matrix.shard }}") {
		t.Fatal("the e2e job must pass `shard: ${{ matrix.shard }}`")
	}
	if !strings.Contains(job, "fail-fast: false") {
		t.Fatal("the e2e shard matrix must set fail-fast: false, so one red shard does not cancel the others")
	}
	m = regexp.MustCompile(`timeout-minutes: \$\{\{ github\.event_name == 'pull_request' && [0-9]+ \|\| ([0-9]+) \}\}`).FindStringSubmatch(job)
	if m == nil {
		t.Fatal("cannot read the e2e job's timeout-minutes for dispatched runs")
	}
	minutes, _ := strconv.Atoi(m[1])
	return shards, time.Duration(minutes) * time.Minute
}

// TestFullProfileShardsFitCIJob pins the CI layout of the full profile: with
// the shard count e2e.yml uses, every shard's scenario Timeouts (plus the
// resets run-all does) fit the job timeout with room for building the
// cluster. Like TestPITRScenariosInPITRE2EJob it bounds the Timeouts, not
// the CleanupTimeout allowances. When it fails, a scenario was added or its
// Timeout raised: add a shard in e2e.yml (matrix list and `shards`).
func TestFullProfileShardsFitCIJob(t *testing.T) {
	shards, timeout := ciFullProfileShards(t)
	if timeout > 360*time.Minute {
		t.Fatalf("job timeout %s exceeds the 360m cap of a GitHub-hosted runner job", timeout)
	}
	// kind + Calico + image load + playground/setup.sh + the suite-entry
	// baseline wait take well under 15 minutes; the rest is slack.
	const setupAllowance = 30 * time.Minute

	full := runner.SelectForProfile(runner.DefaultRegistry.List(), runner.ProfileFull)
	parts := runner.AssertShardsPartition(t, full, shards)
	for i, part := range parts {
		var budget time.Duration
		ids := make([]string, 0, len(part))
		for _, s := range part {
			budget += runner.ShardBudget(s)
			ids = append(ids, s.ID)
		}
		t.Logf("full shard %d/%d: %d scenarios, budget %s: %s", i+1, shards, len(part), budget, strings.Join(ids, ","))
		if len(part) == 0 {
			t.Errorf("full shard %d/%d is empty: fewer shards would do", i+1, shards)
		}
		if budget+setupAllowance > timeout {
			t.Errorf("full shard %d/%d budgets %s of scenario Timeouts and resets; with %s for setup that exceeds the %s job timeout: add a shard in e2e.yml", i+1, shards, budget, setupAllowance, timeout)
		}
	}
}

// TestUnshardedIsTheDefault pins that only a caller that asks for it gets a
// shard: the reusable workflow's default is the whole profile, which is what
// the smoke and release jobs (e2e.yml, release.yml) run.
func TestUnshardedIsTheDefault(t *testing.T) {
	reusable, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", "_e2e.yml"))
	if err != nil {
		t.Fatal(err)
	}
	_, shardInput, ok := strings.Cut(string(reusable), "\n      shard:\n")
	if !ok {
		t.Fatal("_e2e.yml has no shard input")
	}
	shardInput, _, _ = strings.Cut(shardInput, "\n      shards:\n")
	if !strings.Contains(shardInput, "default: 0\n") {
		t.Error("_e2e.yml's shard input must default to 0 (the whole profile)")
	}
}
