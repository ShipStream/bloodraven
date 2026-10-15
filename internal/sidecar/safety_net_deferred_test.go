package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
)

// netMysql is a local MySQL for safety-net tests: it can be down (every
// call fails, as a hung or restarting mysqld does once the caller's
// timeout fires), and records the writes the monitor issues.
type netMysql struct {
	down          bool
	readOnly      bool
	superReadOnly bool
	setErr        error
	clearErr      error
	sets          int
	clears        int
	kills         int
}

var errMysqlDown = errors.New("context deadline exceeded (mysqld not answering)")

func (m *netMysql) IsReadOnly(context.Context) (bool, error) {
	if m.down {
		return false, errMysqlDown
	}
	return m.readOnly, nil
}

func (m *netMysql) IsSuperReadOnly(context.Context) (bool, error) {
	if m.down {
		return false, errMysqlDown
	}
	return m.superReadOnly, nil
}

func (m *netMysql) SetSuperReadOnly(context.Context) error {
	m.sets++
	if m.down {
		return errMysqlDown
	}
	if m.setErr != nil {
		return m.setErr
	}
	m.superReadOnly, m.readOnly = true, true
	return nil
}

func (m *netMysql) ClearSuperReadOnly(context.Context) error {
	m.clears++
	if m.down {
		return errMysqlDown
	}
	if m.clearErr != nil {
		return m.clearErr
	}
	m.superReadOnly, m.readOnly = false, false
	return nil
}

func (m *netMysql) KillConnections(context.Context) (int, error) {
	m.kills++
	return 0, nil
}

// safetyNetRoutes answers /healthz and /peer/* as reachable, and
// /active-site with activeSite, or as unreachable when activeSite is nil.
// Operator and peer stay reachable throughout, so the lease rule never
// fires and only the safety net (or rule #1) can fence.
func safetyNetRoutes(activeSite *string) []routingRoute {
	active := func(r *http.Request) (*http.Response, error) {
		if activeSite == nil {
			return nil, errors.New("operator unreachable")
		}
		return jsonResponse(http.StatusOK, map[string]string{"activeSite": *activeSite})(r)
	}
	return []routingRoute{
		{"/active-site", active},
		{"/healthz", jsonResponse(http.StatusOK, nil)},
		{"/peer/ping", jsonResponse(http.StatusOK, nil)},
		{"/peer/active-site", jsonResponse(http.StatusNoContent, nil)},
	}
}

func newSafetyNetMonitor(m *netMysql, activeSite *string) (*FencingMonitor, *clock.FakeClock) {
	clk := clock.NewFakeClock(time.Date(2026, 10, 15, 16, 0, 0, 0, time.UTC))
	fm := newTopologyFencingMonitor(m, clk, &TopologyCache{}, safetyNetRoutes(activeSite)).WithSafetyNet(m)
	fm.SeedStartupGrace(clk.Now())
	return fm, clk
}

func site(s string) *string { return &s }

// The live failure: the sidecar starts while mysqld is hung, so the startup
// fence times out. mysqld then resumes as a writable old primary. No
// authoritative view names another active site (the operator is down and
// the peer relays nothing) and the peer is reachable, so neither rule #1
// nor the lease rule fires. Only the deferred safety net fences it.
func TestSafetyNet_DeferredFenceAfterStartupTimeout(t *testing.T) {
	for _, armed := range []bool{true, false} {
		name := "armed"
		if !armed {
			name = "unarmed monitor leaves the instance writable"
		}
		t.Run(name, func(t *testing.T) {
			m := &netMysql{down: true}
			fm, clk := newSafetyNetMonitor(m, nil)
			if armed {
				fm.ArmSafetyNet()
			}

			for range 3 {
				clk.Advance(5 * time.Second)
				fm.Check(context.Background())
			}
			if m.sets != 0 {
				t.Fatalf("fence attempted %d times while MySQL was down", m.sets)
			}

			m.down = false // resumes writable: read_only=OFF, super_read_only=OFF
			clk.Advance(5 * time.Second)
			fm.Check(context.Background())

			if !armed {
				if m.superReadOnly {
					t.Fatal("the gap this test pins is gone: an unarmed monitor fenced on its own")
				}
				return
			}
			if !m.superReadOnly {
				t.Fatal("instance still writable after MySQL answered; the timed-out startup fence was never retried")
			}
			if fm.SafetyNetPending() {
				t.Error("safety net still pending after the fence landed")
			}
			if fm.IsFenced() {
				t.Error("a safety-net fence was reported as a self-fence")
			}
			if m.kills != 0 {
				t.Error("safety-net fence evicted sessions; only rule #1 and the lease rule evict")
			}
		})
	}
}

func TestSafetyNet_DeferredResolution(t *testing.T) {
	t.Run("operator confirms active site: stays writable", func(t *testing.T) {
		m := &netMysql{}
		fm, _ := newSafetyNetMonitor(m, site("iad"))
		fm.ArmSafetyNet()

		fm.Check(context.Background())

		if m.sets != 0 || m.superReadOnly {
			t.Error("fenced the site the operator confirmed active")
		}
		if fm.SafetyNetPending() {
			t.Error("safety net still pending")
		}
	})

	t.Run("operator confirms active site: clears a fence that landed late", func(t *testing.T) {
		// The startup SET timed out on the client but the server applied
		// it when it resumed. The startup safety net would have cleared it.
		m := &netMysql{readOnly: true, superReadOnly: true}
		fm, _ := newSafetyNetMonitor(m, site("iad"))
		fm.ArmSafetyNet()

		fm.Check(context.Background())

		if m.clears != 1 || m.superReadOnly {
			t.Errorf("clears=%d superReadOnly=%v; the active site was left fenced", m.clears, m.superReadOnly)
		}
		if fm.SafetyNetPending() {
			t.Error("safety net still pending")
		}
	})

	t.Run("clear failure is retried", func(t *testing.T) {
		m := &netMysql{readOnly: true, superReadOnly: true, clearErr: errMysqlDown}
		fm, clk := newSafetyNetMonitor(m, site("iad"))
		fm.ArmSafetyNet()

		fm.Check(context.Background())
		if !fm.SafetyNetPending() {
			t.Fatal("a failed clear settled the safety net; the active site would stay fenced")
		}

		m.clearErr = nil
		clk.Advance(5 * time.Second)
		fm.Check(context.Background())
		if m.superReadOnly || fm.SafetyNetPending() {
			t.Error("retried clear did not restore the active site")
		}
	})

	t.Run("operator reports no active site: fences", func(t *testing.T) {
		m := &netMysql{}
		fm, _ := newSafetyNetMonitor(m, site(""))
		fm.ArmSafetyNet()

		fm.Check(context.Background())

		if !m.superReadOnly {
			t.Error("instance left writable although the operator did not confirm this site")
		}
	})

	t.Run("already super-fenced and not confirmed: settles without writing", func(t *testing.T) {
		m := &netMysql{readOnly: true, superReadOnly: true}
		fm, _ := newSafetyNetMonitor(m, site("pdx"))
		fm.ArmSafetyNet()

		fm.Check(context.Background())

		if m.sets != 0 || m.clears != 0 {
			t.Errorf("sets=%d clears=%d, want none", m.sets, m.clears)
		}
		if fm.SafetyNetPending() {
			t.Error("safety net still pending")
		}
	})

	t.Run("writable with another active site: rule #1 self-fences", func(t *testing.T) {
		m := &netMysql{}
		fm, _ := newSafetyNetMonitor(m, site("pdx"))
		fm.ArmSafetyNet()

		fm.Check(context.Background())

		if !m.superReadOnly || !fm.IsFenced() {
			t.Error("stale writable primary was not self-fenced by rule #1")
		}
		if m.kills != 1 {
			t.Errorf("kills=%d, want rule #1's eviction once", m.kills)
		}
		if m.sets != 1 {
			t.Errorf("sets=%d, want a single fence write", m.sets)
		}
		if fm.SafetyNetPending() {
			t.Error("safety net still pending after rule #1 took the tick")
		}
	})

	t.Run("read_only without super_read_only is fenced", func(t *testing.T) {
		m := &netMysql{readOnly: true}
		fm, _ := newSafetyNetMonitor(m, nil)
		fm.ArmSafetyNet()

		fm.Check(context.Background())

		if !m.superReadOnly {
			t.Error("read_only=ON alone lets SUPER sessions write; the safety net must set super_read_only")
		}
	})

	t.Run("failed fence is retried", func(t *testing.T) {
		m := &netMysql{setErr: errors.New("ERROR 1227: access denied")}
		fm, clk := newSafetyNetMonitor(m, nil)
		fm.ArmSafetyNet()

		fm.Check(context.Background())
		if !fm.SafetyNetPending() {
			t.Fatal("a failed fence settled the safety net")
		}

		m.setErr = nil
		clk.Advance(5 * time.Second)
		fm.Check(context.Background())
		if !m.superReadOnly || fm.SafetyNetPending() {
			t.Error("retried fence did not land")
		}
	})
}

// An outage long enough for mysqld to have restarted re-arms the safety
// net. Before liveness was decoupled from MySQL, that outage restarted the
// sidecar, which re-ran the safety net as a side effect.
func TestSafetyNet_RearmsAfterLongMySQLOutage(t *testing.T) {
	run := func(t *testing.T, outage time.Duration) (*netMysql, *FencingMonitor) {
		t.Helper()
		m := &netMysql{}
		fm, clk := newSafetyNetMonitor(m, nil)

		fm.Check(context.Background()) // healthy, writable, nothing armed
		if m.superReadOnly {
			t.Fatal("unarmed monitor fenced a healthy instance")
		}

		m.down = true
		fm.Check(context.Background())
		clk.Advance(outage)
		m.down = false // a restarted mysqld comes up writable
		fm.Check(context.Background())
		return m, fm
	}

	t.Run("long outage", func(t *testing.T) {
		m, fm := run(t, safetyNetOutageRearm)
		if !m.superReadOnly {
			t.Error("instance writable after a restart-length outage with no operator confirmation")
		}
		if fm.SafetyNetPending() {
			t.Error("safety net still pending after the fence landed")
		}
	})

	t.Run("short outage", func(t *testing.T) {
		m, fm := run(t, safetyNetOutageRearm-time.Second)
		if m.superReadOnly || fm.SafetyNetPending() {
			t.Error("a brief stall re-armed the safety net; a slow query on the active primary must not fence it")
		}
	})
}

// The safety net never clears a fence this monitor set itself, because
// only the operator lifts a self-fence.
func TestSafetyNet_NeverClearsASelfFence(t *testing.T) {
	m := &netMysql{readOnly: true, superReadOnly: true}
	fm, clk := newSafetyNetMonitor(m, site("iad"))
	fm.fenced.Store(true)

	m.down = true
	fm.Check(context.Background())
	clk.Advance(safetyNetOutageRearm)
	m.down = false
	fm.Check(context.Background())

	if m.clears != 0 || !m.superReadOnly {
		t.Error("safety net cleared this monitor's own self-fence")
	}
	if fm.SafetyNetPending() {
		t.Error("safety net still pending")
	}
}

func TestSafetyNet_DisabledWithoutIdentityOrClearer(t *testing.T) {
	m := &netMysql{}
	clk := clock.NewFakeClock(time.Now())

	noClearer := newTopologyFencingMonitor(m, clk, &TopologyCache{}, nil)
	noClearer.ArmSafetyNet()
	if noClearer.SafetyNetPending() {
		t.Error("armed without WithSafetyNet")
	}

	noTopology := newTestFencingMonitor(m, clk).WithSafetyNet(m)
	noTopology.ArmSafetyNet()
	if noTopology.SafetyNetPending() {
		t.Error("armed without the identity RunSafetyNet requires")
	}
}

// RunSafetyNet reports whether it settled, so the caller knows whether to
// arm the monitor.
func TestRunSafetyNetReportsWhetherItSettled(t *testing.T) {
	cases := []struct {
		name        string
		activeSite  string
		mock        *mockMysqlQuerier
		wantSettled bool
	}{
		{"standby fenced", "site1", &mockMysqlQuerier{}, true},
		{"active cleared", "site2", &mockMysqlQuerier{}, true},
		{"fence failed", "site1", &mockMysqlQuerier{setReadOnlyErr: errMysqlDown}, false},
		{"active clear failed", "site2", &mockMysqlQuerier{clearReadOnlyErr: errMysqlDown}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := fakeOperator(tc.activeSite, http.StatusOK)
			defer op.Close()
			srv := NewServer(tc.mock, ":0", testLogger())

			got := srv.RunSafetyNet(context.Background(), safetyNetConfig(op.Listener.Addr().String(), "site2"))
			if got != tc.wantSettled {
				t.Errorf("settled = %v, want %v", got, tc.wantSettled)
			}
		})
	}

	t.Run("fence failed on the active site: no clear attempted", func(t *testing.T) {
		op := fakeOperator("site2", http.StatusOK)
		defer op.Close()
		mock := &mockMysqlQuerier{setReadOnlyErr: errMysqlDown}
		srv := NewServer(mock, ":0", testLogger())

		srv.RunSafetyNet(context.Background(), safetyNetConfig(op.Listener.Addr().String(), "site2"))
		if mock.clearReadOnlyCalled {
			t.Error("cleared after an unconfirmed fence; the monitor decides from the instance's actual state")
		}
	})
}

// /livez reports a wedged fencing loop, and only a wedged one.
func TestLivezReportsAStalledFencingLoop(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	fm := newTestFencingMonitor(newMockFencer(false), clk)
	srv := NewServer(hangingMysql{}, ":0", testLogger())
	srv.SetFencing(fm)

	get := func() (int, LivezStatus) {
		w := httptest.NewRecorder()
		srv.httpServer.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/livez", nil))
		var body LivezStatus
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode /livez: %v", err)
		}
		return w.Code, body
	}

	if code, body := get(); code != http.StatusOK || body.FencingMonitor == nil || body.FencingMonitor.Started {
		t.Fatalf("before the loop starts: %d %+v, want 200 and started=false", code, body.FencingMonitor)
	}

	fm.beat()
	clk.Advance(fm.StallThreshold())
	if code, _ := get(); code != http.StatusOK {
		t.Errorf("/livez = %d at the stall threshold, want 200", code)
	}

	clk.Advance(time.Second)
	code, body := get()
	if code != http.StatusServiceUnavailable {
		t.Errorf("/livez = %d past the stall threshold, want 503", code)
	}
	if body.Status == "ok" {
		t.Errorf("status = %q for a stalled loop", body.Status)
	}
}

// The stall threshold must leave room for the slowest healthy tick: every
// request timing out and a full fence sequence against a slow MySQL.
func TestStallThresholdCoversTheSlowestHealthyTick(t *testing.T) {
	fm := newTestFencingMonitor(newMockFencer(false), clock.NewFakeClock(time.Now()))
	requests := 2 + 2*len(fm.peerAddrs)
	slowest := fm.checkInterval + time.Duration(requests)*httpRequestTimeout + evaluateTimeout
	if fm.StallThreshold() <= slowest {
		t.Errorf("StallThreshold %s does not exceed the slowest healthy tick %s", fm.StallThreshold(), slowest)
	}
}
