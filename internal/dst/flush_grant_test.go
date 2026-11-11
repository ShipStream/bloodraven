package dst

import (
	"context"
	"strings"
	"testing"
)

// TestFlushGrants: FlushAfterWriteGrant across polls (r1 L5). An actor's
// grant of writes to a site stands until the site is fenced again by anyone
// or its server restarts, and a rotation that actor sends while it stands is
// out of order: in the grant's poll for every actor, and in a later poll for
// the planned failover and the ordered update. A grant that did not leave
// the site writable does not outlive its poll, and neither does the topology
// manager's.
func TestFlushGrants(t *testing.T) {
	tm := context.Background()
	pf := pfCtx()
	upd := WithActor(context.Background(), ActorUpdate)
	// poll runs one poll's statements and returns the violations its
	// events produce, as checkPoll does.
	type fixture struct {
		c *Cluster
		g flushGrants
		p int
	}
	poll := func(f *fixture, stmts func()) []Violation {
		f.p++
		f.c.BeginPoll(f.p)
		stmts()
		out := f.g.observe(f.p, f.c.PollEvents())
		f.g.settle(f.c.Truth())
		return out
	}
	newFixture := func() *fixture {
		c, _ := flushTestCluster()
		return &fixture{c: c}
	}
	grant := func(f *fixture, ctx context.Context) func() {
		return func() {
			beta := f.c.NewChecker("beta")
			_ = beta.StopReplica(ctx)
			_ = beta.ResetReplicaAll(ctx)
			_ = beta.SetSuperReadOnly(ctx, false)
			_ = beta.SetReadOnly(ctx, false)
		}
	}
	flush := func(f *fixture, ctx context.Context) func() {
		return func() { _ = f.c.NewChecker("beta").FlushBinaryLogs(ctx) }
	}
	want := func(t *testing.T, got []Violation, fragments ...string) {
		t.Helper()
		if len(fragments) == 0 {
			if len(got) != 0 {
				t.Fatalf("violations %v, want none", got)
			}
			return
		}
		if len(got) != 1 || got[0].Invariant != "FlushAfterWriteGrant" {
			t.Fatalf("violations %v, want one FlushAfterWriteGrant", got)
		}
		for _, frag := range fragments {
			if !strings.Contains(got[0].Detail, frag) {
				t.Fatalf("detail %q lacks %q", got[0].Detail, frag)
			}
		}
	}

	t.Run("inTheGrantsPoll", func(t *testing.T) {
		for name, ctx := range map[string]context.Context{"tm": tm, "pf": pf, "upd": upd} {
			f := newFixture()
			want(t, poll(f, func() { grant(f, ctx)(); flush(f, ctx)() }), name+" sent FLUSH BINARY LOGS to beta")
		}
	})
	t.Run("rotationBeforeTheGrantIsInOrder", func(t *testing.T) {
		f := newFixture()
		want(t, poll(f, func() { flush(f, pf)(); grant(f, pf)() }))
	})
	t.Run("inALaterPoll", func(t *testing.T) {
		for name, ctx := range map[string]context.Context{"pf": pf, "upd": upd} {
			f := newFixture()
			want(t, poll(f, grant(f, ctx)))
			want(t, poll(f, func() {}))
			want(t, poll(f, flush(f, ctx)), name+" sent FLUSH BINARY LOGS to beta", "at poll 1", "not fenced or restarted since")
		}
	})
	t.Run("aRotationThatDidNotRotateStillCounts", func(t *testing.T) {
		f := newFixture()
		want(t, poll(f, grant(f, pf)))
		f.c.AddFlushFailures("beta", 1)
		want(t, poll(f, flush(f, pf)), "pf sent FLUSH BINARY LOGS to beta")
	})
	t.Run("fencedByItsSidecarSince", func(t *testing.T) {
		f := newFixture()
		want(t, poll(f, grant(f, pf)))
		want(t, poll(f, func() {
			_ = f.c.NewChecker("beta").SetSuperReadOnly(WithActor(context.Background(), ActorSidecar), true)
			flush(f, pf)()
		}))
	})
	t.Run("fencedByAnExternalWriterSince", func(t *testing.T) {
		f := newFixture()
		want(t, poll(f, grant(f, pf)))
		want(t, poll(f, func() { f.c.RogueFence("beta"); flush(f, pf)() }))
	})
	t.Run("fencedInAnEarlierPoll", func(t *testing.T) {
		// The next handoff: the site this actor made writable is the
		// source now, fenced in one pass and sealed in a later one.
		f := newFixture()
		want(t, poll(f, grant(f, pf)))
		want(t, poll(f, func() { _ = f.c.NewChecker("beta").SetSuperReadOnly(pf, true) }))
		want(t, poll(f, flush(f, pf)))
	})
	t.Run("restartedReadOnlySince", func(t *testing.T) {
		f := newFixture()
		want(t, poll(f, grant(f, pf)))
		want(t, poll(f, func() { f.c.Crash("beta") }))
		want(t, poll(f, func() { f.c.Recover("beta", false, false); flush(f, pf)() }))
	})
	t.Run("aClearThatLeftTheSiteReadOnlyDoesNotOutliveItsPoll", func(t *testing.T) {
		// super_read_only=OFF with read_only still ON: the site takes no
		// write. A new process finds the target read-only (ReplicaReset)
		// and rotates it once more before it clears again.
		f := newFixture()
		want(t, poll(f, func() {
			beta := f.c.NewChecker("beta")
			_ = beta.StopReplica(pf)
			_ = beta.ResetReplicaAll(pf)
			_ = beta.SetSuperReadOnly(pf, false)
		}))
		if siteTruth(f.c, "beta").Writable() {
			t.Fatal("fixture: beta is writable after super_read_only=OFF alone")
		}
		want(t, poll(f, flush(f, pf)))
	})
	t.Run("anotherActorsGrant", func(t *testing.T) {
		f := newFixture()
		want(t, poll(f, grant(f, pf)))
		want(t, poll(f, flush(f, upd)))
	})
	t.Run("theManagersGrantDoesNotOutliveItsPoll", func(t *testing.T) {
		// The split-brain priorities path: the manager made beta writable
		// (a re-assert), an external writer opens alpha, and the manager
		// fences alpha and runs the whole promotion on the winner beta,
		// which is writable already. By design, and so is the pre-reclone
		// rotation of a former primary. Inside one poll the order still
		// counts (inTheGrantsPoll).
		f := newFixture()
		want(t, poll(f, grant(f, tm)))
		want(t, poll(f, func() {
			beta := f.c.NewChecker("beta")
			_ = beta.StopReplica(tm)
			_ = beta.ResetReplicaAll(tm)
			_ = beta.FlushBinaryLogs(tm)
			_ = beta.SetSuperReadOnly(tm, false)
			_ = beta.SetReadOnly(tm, false)
		}))
		want(t, poll(f, flush(f, tm)))
	})
}

// TestFlushGrants_WhatEndsAGrantInsideAPoll: every way a site stops being
// writable by the actor's grant ends the grant at that event, so a rotation
// the actor sends later in the same poll is in order; an attempt that did
// not reach the server ends nothing.
func TestFlushGrants_WhatEndsAGrantInsideAPoll(t *testing.T) {
	grant := Event{Seq: 1, Site: "beta", Kind: EvSetRO, Detail: "on=false", Actor: ActorPF}
	flush := Event{Seq: 3, Site: "beta", Kind: EvFlushBinlog, Actor: ActorPF}
	for _, tc := range []struct {
		name    string
		between Event
		flagged bool
	}{
		{"nothing", Event{Site: "beta", Kind: EvKillConns, Actor: ActorPF}, true},
		{"its own fence", Event{Site: "beta", Kind: EvSetSuperRO, Detail: "on=true", Actor: ActorPF}, false},
		{"its own read_only=ON", Event{Site: "beta", Kind: EvSetRO, Detail: "on=true", Actor: ActorPF}, false},
		{"the manager's fence", Event{Site: "beta", Kind: EvSetSuperRO, Detail: "on=true"}, false},
		{"a fence that applied before the operator died", Event{Site: "beta", Kind: EvSetSuperRO, Detail: "on=true", Outcome: "appliedThenOperatorDied"}, false},
		{"a fence that failed", Event{Site: "beta", Kind: EvSetSuperRO, Detail: "on=true", Outcome: "failed"}, true},
		{"a fence of another site", Event{Site: "alpha", Kind: EvSetSuperRO, Detail: "on=true"}, true},
		{"the sidecar's self-fence", Event{Site: "beta", Kind: EvSidecarFence, Actor: ActorSidecar}, false},
		{"a self-fence that failed", Event{Site: "beta", Kind: EvSidecarFence, Outcome: "failed", Actor: ActorSidecar}, true},
		{"an external fence", Event{Site: "beta", Kind: EvWedgeFence}, false},
		{"a crash", Event{Site: "beta", Kind: EvCrash}, false},
		{"a recovery", Event{Site: "beta", Kind: EvRecoverSite}, false},
		{"a pod replacement", Event{Site: "beta", Kind: EvReplacePod}, false},
		{"a server restart", Event{Site: "beta", Kind: EvRestartSite}, false},
	} {
		tc.between.Seq = 2
		var g flushGrants
		got := g.observe(7, []Event{grant, tc.between, flush})
		if (len(got) == 1) != tc.flagged || len(got) > 1 {
			t.Errorf("%s between the grant and the rotation: violations %v, want flagged=%v", tc.name, got, tc.flagged)
		}
	}
}
