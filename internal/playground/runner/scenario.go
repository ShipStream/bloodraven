// Package runner glues the chaos primitives into a Scenario type and
// an executor that runs Precheck → Steps → Cleanup with deadline
// tracking and forensic capture on failure.
package runner

import (
	"context"
	"log/slog"
	"time"

	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
	pgdragonfly "github.com/shipstream/bloodraven/internal/playground/dragonfly"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
	pgwait "github.com/shipstream/bloodraven/internal/playground/wait"
)

// Phase is the lifecycle stage of a scenario step. Stamped into
// stdout, scenario.log, and failure.txt so failures are pinpointed.
type Phase string

const (
	PhasePrecheck Phase = "precheck"
	PhaseInject   Phase = "inject"
	PhaseObserve  Phase = "observe"
	PhaseVerify   Phase = "verify"
	PhaseSettle   Phase = "settle"
	PhaseCleanup  Phase = "cleanup"
)

// Env is the per-run handle threaded into every scenario function.
// Long-lived dependencies (Kube, Chaos, Wait, Metrics) are reused
// across steps; per-site clients are obtained via the lazy openers
// and torn down by the executor.
type Env struct {
	Namespace string
	FG        string

	// StartTime is captured at the top of Executor.Run, before any
	// step executes. Scenarios pass this as the `since` argument to
	// Wait.UntilLog so matches do not bleed in from prior runs whose
	// lines may still be in the operator/sidecar log history. The
	// tailer also uses it as PodLogOptions.SinceTime so kube pre-filters
	// at the source.
	StartTime time.Time

	Kube    *pgkube.Client
	Chaos   *pgchaos.Actions
	Wait    *pgwait.Helper
	Metrics *pgmetrics.Scraper
	// RefreshMetrics reopens the operator /metrics port-forward after
	// scenarios intentionally restart the operator pod.
	RefreshMetrics func(context.Context) error
	Logger         *slog.Logger
	Capture        *Capture

	Creds pgmysql.Credentials

	// MySQL returns a *pgmysql.SiteClient for a site. The first call
	// opens a port-forward; subsequent calls for the same site reuse
	// the cached client. The executor closes all clients on scenario
	// exit.
	MySQL func(site string) (*pgmysql.SiteClient, error)

	// Sidecar returns a sidecar Probe for a site, with the same
	// caching semantics as MySQL.
	Sidecar func(site string) (*pgsidecar.Probe, error)

	// Dragonfly returns a *pgdragonfly.SiteClient connected to the
	// named site's Dragonfly pod. Each call dials a fresh
	// port-forward — Dragonfly scenarios deliberately re-open after
	// pod restarts (master-kill, demote-and-rejoin) so the pinned
	// SPDY tunnel stays bound to the right pod identity. The
	// executor closes any clients still open when the scenario
	// exits via a tracker registered through this opener.
	Dragonfly func(site string) (*pgdragonfly.SiteClient, error)

	// Logs returns a Tailer for a component. "operator" tails the
	// operator pod; "sidecar:<site>" tails a site's sidecar; "mysql:<site>"
	// tails the mysql container.
	Logs func(component string) (*pglogs.Tailer, error)
}

// Step is the unit of work the executor labels and times.
type Step struct {
	Phase Phase
	Name  string
	Do    func(ctx context.Context, env *Env) error
}

// Scenario is the top-level chaos test definition.
type Scenario struct {
	ID         string
	Title      string
	Hypothesis string
	Risk       string
	DocLink    string
	Timeout    time.Duration
	// ResetBeforeRunAll tells the CLI that this scenario must start
	// from a freshly reset playground when invoked by run-all. Single
	// scenario runs still use Precheck to report the prerequisite
	// explicitly instead of wiping data by surprise.
	ResetBeforeRunAll bool
	Precheck          func(ctx context.Context, env *Env) error
	Steps             []Step
	Cleanup           func(ctx context.Context, env *Env) error
}
