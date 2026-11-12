package dst

import (
	"context"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// What PromotedWithoutOwnTransactions judges once a site has been cloned
// into. A clone makes the site a copy of its donor under a new
// server_uuid, and its receiver never delivers more of its earlier
// streams: what another site holds of those beyond the site's copy, the
// site gave up at the clone. It did not lose it, and it cannot issue it
// again. Everything else is still a loss.
func TestModel_PromotedWithoutOwnTransactions_ClonedSites(t *testing.T) {
	ctx := context.Background()
	fired := func(c *Cluster) bool {
		for _, v := range c.Violations() {
			if v.Invariant == "PromotedWithoutOwnTransactions" {
				return true
			}
		}
		return false
	}
	// threeSites: alpha was the primary; gamma holds everything alpha
	// issued, beta lacks the newest four; alpha is down and beta was
	// promoted.
	threeSites := func(t *testing.T) *Cluster {
		t.Helper()
		c := NewCluster([]SiteSpec{
			{Name: "alpha", Host: "alpha.mysql.sim", LoadBalancerIP: "10.0.0.1", UUID: "00000000-0000-0000-0000-000000000001"},
			{Name: "beta", Host: "beta.mysql.sim", LoadBalancerIP: "10.0.0.2", UUID: "00000000-0000-0000-0000-000000000002"},
			{Name: "gamma", Host: "gamma.mysql.sim", LoadBalancerIP: "10.0.0.3", UUID: "00000000-0000-0000-0000-000000000003"},
		}, 100)
		for i := 0; i < 3; i++ {
			c.Tick()
		}
		c.SetFetchStalled("beta", true)
		for i := 0; i < 4; i++ {
			c.Tick()
		}
		alpha, beta, gamma := truthOf(c, "alpha"), truthOf(c, "beta"), truthOf(c, "gamma")
		if u := alpha.ServerUUID; gamma.Executed[u] != alpha.Executed[u] || beta.Executed[u] != alpha.Executed[u]-4 {
			t.Fatalf("fixture: alpha %s, beta %s, gamma %s; want gamma whole and beta four behind", alpha.Executed, beta.Executed, gamma.Executed)
		}
		c.Crash("alpha")
		if err := c.NewChecker("beta").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if fired(c) {
			t.Fatal("fixture: promoting beta, which lacks nothing it issued, was judged")
		}
		return c
	}

	t.Run("quiet: a witness holds what the site gave up at its clone", func(t *testing.T) {
		c := threeSites(t)
		c.Recover("alpha", true, false)
		if !c.Reclone("beta", "alpha") {
			t.Fatal("reclone refused")
		}
		c.RogueFence("beta")
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if fired(c) {
			t.Fatal("a recloned site was judged for transactions its donor never had: gamma holds them, the site discarded them at the clone")
		}
	})
	t.Run("fires: the same sets without a clone", func(t *testing.T) {
		c := threeSites(t)
		// alpha comes back on a data directory from before its last four
		// transactions, under the server_uuid it had.
		if !c.RegressSite("alpha", 4) {
			t.Fatal("the regress was not applied")
		}
		c.Recover("alpha", true, false)
		c.RogueFence("beta")
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if !fired(c) {
			t.Fatal("no violation: alpha lost four transactions it issued, gamma holds them, and no clone superseded the stream")
		}
	})
	t.Run("fires: a pod replaced on an older data directory under a fresh server_uuid", func(t *testing.T) {
		c, alphaUUID, _ := ownLossCluster(t)
		// A replacement regresses only a site that is not writable.
		c.RogueFence("alpha")
		c.ReplacePod("alpha", 3)
		alpha := truthOf(c, "alpha")
		if alpha.ServerUUID == alphaUUID || alpha.Executed[alphaUUID] >= truthOf(c, "beta").Executed[alphaUUID] {
			t.Fatalf("fixture: alpha = %+v, want a new server_uuid on older data", alpha)
		}
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if !fired(c) {
			t.Fatal("no violation: the server_uuid changed without a clone, so the earlier stream is not superseded")
		}
	})
	t.Run("fires: recloned, then regressed under its new server_uuid", func(t *testing.T) {
		c, _, _ := lossyRejoin(t)
		if !c.Reclone("beta", "alpha") {
			t.Fatal("reclone refused")
		}
		// alpha becomes the primary under its new server_uuid and beta
		// replicates what it issues.
		c.RogueFence("beta")
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		beta := c.NewChecker("beta")
		if err := beta.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{Host: "alpha.mysql.sim"}); err != nil {
			t.Fatal(err)
		}
		if err := beta.StartReplica(ctx); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 6; i++ {
			c.Tick()
		}
		newUUID := truthOf(c, "alpha").ServerUUID
		if truthOf(c, "beta").Executed[newUUID] < 3 {
			t.Fatalf("fixture: beta holds %d of alpha's new stream, want it replicated", truthOf(c, "beta").Executed[newUUID])
		}
		if fired(c) {
			t.Fatal("fixture: promoting the recloned site was judged")
		}
		if !c.RegressSite("alpha", 2) {
			t.Fatal("the regress was not applied")
		}
		c.Recover("alpha", true, false)
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if !fired(c) {
			t.Fatal("no violation: alpha lost transactions it issued after its clone, and beta holds them")
		}
	})
}
