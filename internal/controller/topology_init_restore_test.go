package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
)

func dnsCalls(d *mockDNS) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// TestDNS_WithheldUntilInitRestoreSucceeds: while an initFromBackup
// restore has not succeeded the writable seed is the active site, but the
// group hostname must not point at it, through the poll-driven reconcile
// or a promotion-time write. The first poll after the restore succeeds
// publishes it, even with an updater that cannot read the record back.
func TestDNS_WithheldUntilInitRestoreSucceeds(t *testing.T) {
	ctx := context.Background()
	seed := &mockMySQL{readOnly: false}
	peer := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(seed, peer)
	tm.SetAutoBootstrapSuppressed(true)
	tm.SetInitRestoreSeed("dc1")

	pollN(tm, 4)
	if err := tm.applyDNS(ctx, "dc1", "1.1.1.1"); err != nil {
		t.Fatalf("withheld applyDNS returned %v", err)
	}
	if got := tm.Status().ActiveSite; got != "dc1" {
		t.Fatalf("active site = %q, want the seed dc1", got)
	}
	if n := dnsCalls(dns); n != 0 {
		t.Fatalf("DNS written %d times while the restore was unfinished (last %q)", n, dns.getLastIP())
	}

	tm.SetAutoBootstrapSuppressed(false)
	if got := tm.initRestoreSeedSite(); got != "" {
		t.Fatalf("restore seed %q survived the restore", got)
	}
	pollN(tm, 1)
	if n, ip := dnsCalls(dns), dns.getLastIP(); n != 1 || ip != "1.1.1.1" {
		t.Fatalf("DNS after the restore succeeded: %d writes, last %q; want one write of the seed 1.1.1.1", n, ip)
	}
	pollN(tm, 2)
	if n := dnsCalls(dns); n != 1 {
		t.Fatalf("converged DNS rewritten: %d writes", n)
	}
}

// TestPrimaryReassert_InitRestoreSeed covers the seed of an unfinished
// initFromBackup restore that came back fenced (its mysqld or sidecar
// restarted, possibly while the operator was down) after part of the
// load landed: it holds user schemas, so the fresh-deploy seeding no
// longer applies, and its peers are the fresh servers they booted as, with
// first-boot transactions under their own server_uuid that the seed lacks.
func TestPrimaryReassert_InitRestoreSeed(t *testing.T) {
	no, yes := false, true
	const loaded = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-50"
	const peerSetup = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-3"
	for _, tc := range []struct {
		name        string
		suppressed  bool
		seed        string
		peerSchemas *bool
		peerRepl    *mysql.ReplicaStatus
		seedRepl    *mysql.ReplicaStatus
		want        bool
	}{
		{name: "fenced seed with a partial load is lifted", suppressed: true, seed: "dc1", peerSchemas: &no, want: true},
		{name: "peer with user schemas needs a human", suppressed: true, seed: "dc1", peerSchemas: &yes},
		{name: "peer with replication history needs a human", suppressed: true, seed: "dc1", peerSchemas: &no, peerRepl: &mysql.ReplicaStatus{}},
		{name: "seed with replication configured is not the seed", suppressed: true, seed: "dc1", peerSchemas: &no, seedRepl: &mysql.ReplicaStatus{}},
		{name: "no recorded seed: observed site stays fenced", suppressed: true, peerSchemas: &no},
		{name: "restore succeeded: no restore authority", seed: "dc1", peerSchemas: &no},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: loaded, replicaStatusVal: tc.seedRepl}
			peer := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: peerSetup, hasUserSchemas: tc.peerSchemas, replicaStatusVal: tc.peerRepl}
			tm, _, dns := newTestTopologyManager(seed, peer)
			setWedgedTopology(tm, "")
			tm.mu.Lock()
			// What an operator restart rehydrates: observed, not named.
			tm.lastActiveSite, tm.lastActiveSiteNamed = "dc1", false
			tm.mu.Unlock()
			tm.SetAutoBootstrapSuppressed(tc.suppressed)
			tm.SetInitRestoreSeed(tc.seed)

			got := tm.checkPrimaryReassert(context.Background())
			ro, _ := seed.CheckReadOnly(context.Background())
			if got != tc.want || ro == tc.want {
				t.Fatalf("re-assert = %v, seed read-only = %v; want re-assert %v", got, ro, tc.want)
			}
			if pro, _ := peer.CheckReadOnly(context.Background()); !pro {
				t.Fatal("peer made writable")
			}
			if n := dnsCalls(dns); n != 0 {
				t.Fatalf("re-assert published DNS (%d writes) during an unfinished restore", n)
			}
		})
	}
}

// TestPromotion_RefusedForNonSeedDuringInitRestore: nothing is cloned
// while an initFromBackup restore has not succeeded, so promoting a peer
// when the seed becomes unreachable would make an empty server the
// failover target the group serves once the restore succeeds.
func TestPromotion_RefusedForNonSeedDuringInitRestore(t *testing.T) {
	for _, tc := range []struct {
		name        string
		restore     bool
		wantPromote bool
	}{
		{name: "restore unfinished", restore: true},
		{name: "no restore (control)", wantPromote: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := &mockMySQL{readOnly: false}
			peer := &mockMySQL{readOnly: true}
			tm, _, _ := newTestTopologyManager(seed, peer)
			if tc.restore {
				tm.SetAutoBootstrapSuppressed(true)
				tm.SetInitRestoreSeed("dc1")
			}
			pollN(tm, 2)
			seed.setError(errors.New("connection refused"))
			pollN(tm, 6)
			peer.mu.Lock()
			promoted := !peer.readOnly
			peer.mu.Unlock()
			if promoted != tc.wantPromote {
				t.Fatalf("peer promoted = %v, want %v", promoted, tc.wantPromote)
			}
			tm.mu.RLock()
			target := tm.lastFailoverTarget
			tm.mu.RUnlock()
			if !tc.wantPromote && target != "" {
				t.Fatalf("lastFailoverTarget = %q recorded during the restore", target)
			}
		})
	}
}
