package dst

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

// The sidecar's self-fencing is the production mitigation for several
// windows the operator alone cannot close: a stale primary returning from a
// partition, an operator that dies mid-promotion, a site that can reach its
// peer but not the control plane. Representing it as a "rogue fence" fault
// modeled only its *effect*, and only at moments the schedule picked — never
// its decision, its lease arithmetic, or its interaction with a promotion in
// flight.
//
// So this file runs the real sidecar.FencingMonitor, one per site, on the
// trial's fake clock:
//
//   - the real Check → checkBloodraven / checkActiveSite / checkPeer /
//     checkPeerTopology / evaluate → doFence path, unmodified;
//   - the real sidecar.TopologyCache, including peer relay and Adopt's
//     newer-only rule;
//   - HTTP served in-process by simTransport, so the endpoints exercise the
//     real request construction, status-code handling, and JSON decoding
//     without a listener, a goroutine, or a nondeterministic wire.
//
// Check is driven synchronously once per poll from the harness goroutine
// rather than by FencingMonitor.Run, so ordering stays exact.

// simSidecar is one site's fencing actor.
type simSidecar struct {
	site    string
	monitor *sidecar.FencingMonitor
	cache   *sidecar.TopologyCache
}

// buildSidecars creates one sidecar per site. Called once at setup; a site
// that crashes gets a fresh monitor on recovery (see tickSidecars), because
// a restarted container starts with an empty topology cache and a full lease
// grace period.
func (r *trialRunner) buildSidecars() {
	r.sidecars = make([]*simSidecar, len(r.trial.SiteNames))
	for i, name := range r.trial.SiteNames {
		r.sidecars[i] = r.newSidecar(name)
	}
}

func (r *trialRunner) newSidecar(site string) *simSidecar {
	s := &simSidecar{
		site:  site,
		cache: &sidecar.TopologyCache{},
	}

	peers := make([]string, 0, len(r.trial.SiteNames)-1)
	for _, other := range r.trial.SiteNames {
		if other != site {
			peers = append(peers, sidecarAddr(other))
		}
	}

	s.monitor = sidecar.NewFencingMonitorFull(
		&simFencer{c: r.cluster, site: site},
		operatorAddr,
		peers,
		simPollInterval,
		time.Duration(r.trial.SidecarLeasePolls)*simPollInterval,
		r.logger.With("actor", "sidecar", "site", site),
		r.clk,
		&http.Client{Transport: &simTransport{r: r, from: site}},
	)
	if r.trial.SidecarTopology {
		s.monitor = s.monitor.WithTopology(site, "sim", "sim", s.cache)
	}

	// Run seeds the lease timestamps before its first tick; the harness
	// drives Check directly, so it seeds them here instead.
	s.monitor.SeedStartupGrace(r.clk.Now())
	return s
}

// tickSidecars runs one fencing check per live site. The first site rotates
// deterministically each tick, covering both sides of peer-relay ordering
// without introducing scheduler nondeterminism. A crashed site runs nothing;
// the first tick after it comes back uses a fresh monitor, matching a
// restarted sidecar container.
func (r *trialRunner) tickSidecars(ctx context.Context) {
	n := len(r.trial.SiteNames)
	if n == 0 {
		return
	}
	start := (int(r.trial.Seed%uint64(n)) + r.sidecarTick) % n
	r.sidecarTick++
	for offset := 0; offset < n; offset++ {
		i := (start + offset) % n
		name := r.trial.SiteNames[i]
		if r.cluster.SiteCrashed(name) {
			// Mark it for reconstruction on return.
			r.sidecars[i] = nil
			continue
		}
		if r.sidecars[i] == nil {
			r.sidecars[i] = r.newSidecar(name)
		}
		r.sidecars[i].monitor.Check(ctx)
	}
}

// ---------------------------------------------------------------------------
// The sidecar's view of its own MySQL
// ---------------------------------------------------------------------------

// simFencer implements sidecar.Fencer against one site.
//
// The sidecar shares a pod with its MySQL, so it is reachable whenever the
// site is up — an operator↔site partition does not blind it, and neither
// does an operator that has died. It does share the injected
// ambiguous/failing-mutation counters, which model the server itself being
// flaky: that is what makes the monitor's ambiguous-write path (doFence →
// fenceLanded) reachable at all.
type simFencer struct {
	c    *Cluster
	site string
}

var _ sidecar.Fencer = (*simFencer)(nil)

var errSidecarSiteDown = errors.New("dial tcp 127.0.0.1:3306: connect: connection refused (sim: local mysqld down)")

func (f *simFencer) IsReadOnly(_ context.Context) (bool, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	s := f.c.byName[f.site]
	if s.crashed {
		return false, errSidecarSiteDown
	}
	return s.readOnly, nil
}

func (f *simFencer) IsSuperReadOnly(_ context.Context) (bool, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	s := f.c.byName[f.site]
	if s.crashed {
		return false, errSidecarSiteDown
	}
	return s.superReadOnly, nil
}

func (f *simFencer) SetSuperReadOnly(_ context.Context) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	s := f.c.byName[f.site]
	if s.crashed {
		f.c.event(f.site, EvSidecarFence, "", "unreachable")
		return errSidecarSiteDown
	}
	if f.c.failMuts[f.site] > 0 {
		f.c.failMuts[f.site]--
		f.c.event(f.site, EvSidecarFence, "", "failed")
		return errNotApplied
	}
	s.superReadOnly = true
	s.readOnly = true // super_read_only=ON implies read_only=ON
	if f.c.ambiguousMuts[f.site] > 0 {
		f.c.ambiguousMuts[f.site]--
		f.c.event(f.site, EvSidecarFence, "", "ambiguous")
		return errAmbiguous
	}
	f.c.event(f.site, EvSidecarFence, "", "")
	return nil
}

func (f *simFencer) KillConnections(_ context.Context) (int, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	if f.c.byName[f.site].crashed {
		return 0, errSidecarSiteDown
	}
	f.c.event(f.site, EvSidecarKill, "", "")
	return 0, nil
}

// ---------------------------------------------------------------------------
// In-process HTTP
// ---------------------------------------------------------------------------

const operatorAddr = "bloodraven.sim:8080"

func sidecarAddr(site string) string { return site + ".sidecar.sim:8081" }

func siteFromSidecarAddr(addr string) string {
	return strings.TrimSuffix(addr, ".sidecar.sim:8081")
}

// simTransport serves the operator's auxiliary endpoints and every peer
// sidecar's endpoints from the model, in-process. from is the site whose
// sidecar is making the request, which is what decides reachability.
type simTransport struct {
	r    *trialRunner
	from string
}

var _ http.RoundTripper = (*simTransport)(nil)

// errSimUnreachable stands in for a dial failure. The monitor only ever
// checks err != nil, so the text is for repro logs.
var errSimUnreachable = errors.New("dial tcp: i/o timeout (sim: unreachable)")

// simResponse collects a handler's output. Hand-rolled rather than
// httptest.NewRecorder so package dst carries no dependency on httptest,
// whose init() registers command-line flags.
type simResponse struct {
	code   int
	header http.Header
	body   bytes.Buffer
}

func newSimResponse() *simResponse {
	return &simResponse{code: http.StatusOK, header: make(http.Header)}
}

func (w *simResponse) Header() http.Header         { return w.header }
func (w *simResponse) WriteHeader(code int)        { w.code = code }
func (w *simResponse) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *simResponse) writeString(s string)        { w.body.WriteString(s) }

func (w *simResponse) result() *http.Response {
	return &http.Response{
		StatusCode:    w.code,
		Status:        fmt.Sprintf("%d %s", w.code, http.StatusText(w.code)),
		Header:        w.header,
		Body:          io.NopCloser(bytes.NewReader(w.body.Bytes())),
		ContentLength: int64(w.body.Len()),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
	}
}

func (t *simTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := newSimResponse()
	switch req.URL.Host {
	case operatorAddr:
		if !t.operatorReachable() {
			return nil, errSimUnreachable
		}
		if err := t.serveOperator(rec, req.URL); err != nil {
			return nil, t.modelError(err)
		}
	default:
		peer := siteFromSidecarAddr(req.URL.Host)
		// byName is built once in NewCluster and never mutated, so this
		// membership check needs no lock.
		if _, ok := t.r.cluster.byName[peer]; !ok {
			return nil, t.modelError(fmt.Errorf("dst: sidecar request to unknown host %q", req.URL.Host))
		}
		if !t.peerReachable(peer) {
			return nil, errSimUnreachable
		}
		if err := t.servePeer(rec, req.URL, peer); err != nil {
			return nil, t.modelError(err)
		}
	}
	resp := rec.result()
	resp.Request = req
	return resp, nil
}

// modelError promotes a transport/schema gap to a harness violation before
// returning it to FencingMonitor. The monitor correctly treats HTTP errors as
// reachability failures, but an unmodeled endpoint must fail the trial loudly
// instead of masquerading as an ordinary partition.
func (t *simTransport) modelError(err error) error {
	t.r.cluster.mu.Lock()
	poll := t.r.cluster.poll
	t.r.cluster.mu.Unlock()
	t.r.violations = append(t.r.violations, Violation{Invariant: "ModelHole", Poll: poll, Detail: err.Error()})
	return err
}

// operatorReachable: the sidecar talks to the operator over the same network
// path the operator uses to reach this site's MySQL, so an operator↔site
// partition cuts both. A dead operator process answers nothing — which is
// the whole point of running these actors alongside mid-Execute crashes.
func (t *simTransport) operatorReachable() bool {
	c := t.r.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.operatorDead && !c.opLinkDown[t.from] && !c.byName[t.from].crashed
}

// peerReachable: sidecar-to-sidecar traffic follows the same directed
// site-to-site link replication does (from → peer).
func (t *simTransport) peerReachable(peer string) bool {
	c := t.r.cluster
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.byName[peer].crashed && !c.dirLinkDown[dirKey(t.from, peer)]
}

// serveOperator mirrors the operator's auxiliary mux (cmd/bloodraven:
// newAuxMux). Only the two endpoints the FencingMonitor calls are served;
// the response shapes must be kept in step with that mux, which is why the
// activeSite payload uses the same key the real handler encodes.
func (t *simTransport) serveOperator(w *simResponse, u *url.URL) error {
	switch u.Path {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
		w.writeString("ok")
		return nil
	case "/active-site":
		q := u.Query()
		if q.Get("namespace") == "" || q.Get("group") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return nil
		}
		w.Header().Set("Content-Type", "application/json")
		return json.NewEncoder(w).Encode(map[string]string{
			"namespace":  q.Get("namespace"),
			"group":      q.Get("group"),
			"activeSite": t.r.tm.Status().ActiveSite,
		})
	default:
		return fmt.Errorf("dst: unmodeled operator endpoint %q", u.Path)
	}
}

// servePeer mirrors sidecar.Server's peer routes (internal/sidecar/server.go:
// handlePeerPing, handlePeerActiveSite). The handlers themselves are
// unexported, so this is a copy; sidecar.TopologySnapshot is shared, so the
// JSON shape cannot drift even if these few lines do.
func (t *simTransport) servePeer(w *simResponse, u *url.URL, peer string) error {
	switch u.Path {
	case "/peer/ping":
		w.WriteHeader(http.StatusOK)
		w.writeString("pong")
		return nil
	case "/peer/active-site":
		sc := t.r.sidecarFor(peer)
		if sc == nil {
			// The peer disappeared between the reachability check and topology
			// lookup; return no relay rather than dereferencing a stale actor.
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
		snap := sc.cache.Snapshot()
		if snap.ActiveSite == "" || snap.ObservedAt.IsZero() {
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
		w.Header().Set("Content-Type", "application/json")
		return json.NewEncoder(w).Encode(snap)
	default:
		return fmt.Errorf("dst: unmodeled peer endpoint %q", u.Path)
	}
}

// sidecarFor returns the named site's sidecar, or nil when its container is
// down.
func (r *trialRunner) sidecarFor(site string) *simSidecar {
	for i, name := range r.trial.SiteNames {
		if name == site {
			return r.sidecars[i]
		}
	}
	return nil
}

// selfFencedSites reports which sites' monitors currently consider
// themselves self-fenced, in declared order. Surfaced on TrialResult so a
// repro can tell an operator fence from a sidecar one at a glance.
func (r *trialRunner) selfFencedSites() []string {
	var out []string
	for i, name := range r.trial.SiteNames {
		if sc := r.sidecars[i]; sc != nil && sc.monitor.IsFenced() {
			out = append(out, name)
		}
	}
	return out
}
