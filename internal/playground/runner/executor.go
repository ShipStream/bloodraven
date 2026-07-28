package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
	pgdragonfly "github.com/shipstream/bloodraven/internal/playground/dragonfly"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
	pgwait "github.com/shipstream/bloodraven/internal/playground/wait"
)

// ExecutorConfig captures CLI-level knobs the executor honors.
type ExecutorConfig struct {
	Namespace  string
	FG         string
	ResultsDir string
	NoCleanup  bool
	Logger     *slog.Logger

	// Force, when true, deletes any existing chaos in-progress marker
	// before preflight runs. Surfaces a "stomping prior run" banner.
	Force bool
}

// Executor runs a single Scenario against a live playground cluster.
// One Executor per scenario; run-all instantiates a fresh one each
// time so per-scenario tailer maps stay independent.
type Executor struct {
	K   *pgkube.Client
	Cfg ExecutorConfig

	// tailers is the live ring-buffered tailer set populated by
	// buildEnv. It's read by forensic.Persist on failure.
	tailers map[string]*pglogs.Tailer
}

// Result describes how a scenario run ended.
type Result struct {
	ID          string
	Title       string
	Passed      bool
	Failure     string
	Phase       Phase
	StepName    string
	StartTime   time.Time
	Duration    time.Duration
	CapturePath string
	CleanupErr  error
}

// Run executes a scenario end-to-end.
func (e *Executor) Run(ctx context.Context, s Scenario) Result {
	logger := e.Cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	res := Result{
		ID:        s.ID,
		Title:     s.Title,
		StartTime: time.Now(),
	}

	timeout := s.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	scenarioCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	captureDir := filepath.Join(
		e.Cfg.ResultsDir,
		res.StartTime.UTC().Format("20060102T150405Z"),
		s.ID,
	)
	cap := &Capture{Dir: captureDir}
	res.CapturePath = captureDir

	env, envCloser, err := e.buildEnv(scenarioCtx, logger, cap, res.StartTime)
	if err != nil {
		res.Failure = "build env: " + err.Error()
		res.Phase = PhasePrecheck
		res.Duration = time.Since(res.StartTime)
		return res
	}
	defer envCloser()

	fail := func(phase Phase, stepName, msg string) Result {
		res.Failure = msg
		res.Phase = phase
		res.StepName = stepName
		res.Duration = time.Since(res.StartTime)
		failureBlock := fmt.Sprintf(
			"scenario %s failed in phase=%s step=%q\n%s",
			s.ID, phase, stepName, msg,
		)
		// Forensic capture must run on a fresh context: scenario-level
		// timeouts (the most common failure mode) cancel scenarioCtx
		// before fail() runs, which would leave Persist unable to
		// fetch pods/events/logs/metrics — exactly when we need them
		// most. Use a bounded background context so the capture is
		// best-effort but still happens.
		forensicsCtx, forensicsCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer forensicsCancel()
		_ = cap.Persist(forensicsCtx, e.K, e.Cfg.Namespace, e.Cfg.FG, env.Metrics, e.tailers, failureBlock)
		if !e.Cfg.NoCleanup {
			res.CleanupErr = e.cleanup(env, s)
		}
		return res
	}

	cap.Note(fmt.Sprintf("scenario %s start: %s", s.ID, s.Title))

	// Preflight: marker check + (on --force) delete prior marker. Runs
	// before the scenario's own Precheck so a prior run's leftover state
	// surfaces with a specific error and remediation, not a generic
	// converge timeout.
	if err := e.preflight(scenarioCtx, env, s.ID, captureDir); err != nil {
		return fail(PhasePrecheck, "Preflight", err.Error())
	}

	if s.Precheck != nil {
		if err := e.runWithLog(scenarioCtx, env, PhasePrecheck, "Precheck", s.Precheck); err != nil {
			return fail(PhasePrecheck, "Precheck", err.Error())
		}
	}

	// Set in-progress marker after Precheck has confirmed the cluster
	// is healthy enough to actually run. Skipped under NoCleanup so
	// `--no-cleanup` (forensics mode) leaves the marker behind for the
	// next run to find.
	if !e.Cfg.NoCleanup {
		if err := e.setMarker(scenarioCtx, s.ID, captureDir); err != nil {
			env.Logger.Warn("set chaos marker failed", "err", err)
			env.Capture.Note(fmt.Sprintf("preflight: set chaos marker failed: %v", err))
			// Non-fatal: a marker we can't set is a UX regression for
			// the next run, not a reason to abort this one.
		}
	}

	for _, step := range s.Steps {
		if err := e.runWithLog(scenarioCtx, env, step.Phase, step.Name, step.Do); err != nil {
			return fail(step.Phase, step.Name, err.Error())
		}
	}

	res.Passed = true
	res.Duration = time.Since(res.StartTime)
	cap.Note(fmt.Sprintf("scenario %s passed (elapsed=%s)", s.ID, res.Duration.Round(time.Millisecond)))
	if !e.Cfg.NoCleanup {
		// Clear marker on the pass path. Failures clear in cleanup()
		// (best-effort, appended to errs). NoCleanup skips both — the
		// `--no-cleanup` contract is "leave forensics behind" and the
		// marker is part of that.
		clearCtx, cancelClear := context.WithTimeout(context.Background(), 10*time.Second)
		if err := e.K.ClearChaosMarkerNamed(clearCtx, e.Cfg.Namespace, e.Cfg.FG); err != nil {
			env.Logger.Warn("clear chaos marker failed", "err", err)
		}
		cancelClear()
		res.CleanupErr = e.cleanup(env, s)
		if res.CleanupErr != nil {
			res.Passed = false
			res.Phase = PhaseCleanup
			res.StepName = "Cleanup"
			res.Failure = "cleanup failed: " + res.CleanupErr.Error()
			res.Duration = time.Since(res.StartTime)
			failureBlock := fmt.Sprintf(
				"scenario %s failed in phase=%s step=%q\n%s",
				s.ID, res.Phase, res.StepName, res.Failure,
			)
			forensicsCtx, forensicsCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer forensicsCancel()
			_ = cap.Persist(forensicsCtx, e.K, e.Cfg.Namespace, e.Cfg.FG, env.Metrics, e.tailers, failureBlock)
		}
	}
	return res
}

func (e *Executor) runWithLog(ctx context.Context, env *Env, phase Phase, name string, fn func(context.Context, *Env) error) error {
	start := time.Now()
	env.Logger.Info("step", "phase", phase, "name", name)
	env.Capture.Note(fmt.Sprintf("[%s] %s — start", phase, name))
	err := fn(ctx, env)
	dur := time.Since(start)
	if err != nil {
		env.Logger.Error("step failed", "phase", phase, "name", name, "elapsed", dur.Round(time.Millisecond), "err", err)
		env.Capture.Note(fmt.Sprintf("[%s] %s — FAIL after %s: %v", phase, name, dur.Round(time.Millisecond), err))
		return err
	}
	env.Logger.Info("step done", "phase", phase, "name", name, "elapsed", dur.Round(time.Millisecond))
	env.Capture.Note(fmt.Sprintf("[%s] %s — ok (%s)", phase, name, dur.Round(time.Millisecond)))
	return nil
}

func (e *Executor) cleanup(env *Env, s Scenario) error {
	cleanCtx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	var errs []error
	if err := env.Chaos.Revert(cleanCtx); err != nil {
		errs = append(errs, fmt.Errorf("revert: %w", err))
	}
	if s.Cleanup != nil {
		if err := s.Cleanup(cleanCtx, env); err != nil {
			errs = append(errs, fmt.Errorf("scenario cleanup: %w", err))
		}
	}
	if err := env.Chaos.GlobalRecover(cleanCtx); err != nil {
		errs = append(errs, fmt.Errorf("global recover: %w", err))
	}
	// Wait for the cluster to converge back to a healthy N-site
	// state so the next scenario's Precheck does not race a still-
	// recovering cluster (run-all surfaced this: scenario N's chaos
	// scales a site to 0; scenario N+1 starts before that pod has
	// even gone Ready). The cleanup context has the budget; if we
	// time out, that's logged but the per-scenario result is still
	// recorded as the success/failure that occurred BEFORE cleanup.
	if err := waitForClusterReconverge(cleanCtx, env); err != nil {
		errs = append(errs, fmt.Errorf("post-cleanup reconverge: %w", err))
	}
	// Clear the in-progress marker last. Best-effort: append to errs
	// rather than fail the whole cleanup, since a stuck marker only
	// matters for the next run's preflight.
	if err := e.K.ClearChaosMarkerNamed(cleanCtx, e.Cfg.Namespace, e.Cfg.FG); err != nil {
		errs = append(errs, fmt.Errorf("clear chaos marker: %w", err))
	}
	return errors.Join(errs...)
}

// preflight runs before Precheck. It honors --force (delete any prior
// marker, banner) and otherwise refuses to start if a live or
// abandoned marker is present, with an error that names the exact
// remediation flag.
func (e *Executor) preflight(ctx context.Context, env *Env, scenarioID, captureDir string) error {
	if e.Cfg.Force {
		if err := e.K.ClearChaosMarkerNamed(ctx, e.Cfg.Namespace, e.Cfg.FG); err != nil {
			env.Logger.Warn("force-clear chaos marker failed", "err", err)
		} else {
			env.Logger.Warn("--force: deleted prior chaos in-progress marker")
			env.Capture.Note("preflight: --force deleted prior chaos in-progress marker")
		}
		return nil
	}

	m, err := e.K.ReadChaosMarkerNamed(ctx, e.Cfg.Namespace, e.Cfg.FG)
	if err != nil {
		// Don't refuse a run because of a parse error on the marker;
		// surface it but keep going. A malformed marker is a chaos
		// runner bug, not a state-of-the-cluster problem.
		env.Logger.Warn("read chaos marker failed", "err", err)
		return nil
	}
	if m == nil {
		return nil
	}

	age := time.Since(m.StartedAt).Round(time.Second)
	host, _ := os.Hostname()
	switch {
	case m.Host == host && m.PID > 0 && processAlive(m.PID):
		return fmt.Errorf("in-progress: scenario %s started %s ago on host %s (pid %d) — wait for it to finish, or pass --force to override",
			m.Scenario, age, m.Host, m.PID)
	case m.Host == host && m.PID > 0:
		return fmt.Errorf("abandoned chaos run: scenario %s died at %s (pid %d, host %s), capture in %s — re-run with --force, --auto-reset, or ./playground/reset-mysql.sh",
			m.Scenario, m.StartedAt.UTC().Format(time.RFC3339), m.PID, m.Host, m.CaptureDir)
	default:
		// Different host. We can't kill -0 across machines, so treat as live.
		return fmt.Errorf("in-progress: scenario %s started %s ago on host %s (pid %d) — wait for it to finish, or pass --force to override",
			m.Scenario, age, m.Host, m.PID)
	}
}

// setMarker writes the in-progress marker on the MFG. Called after
// Precheck succeeds. Surfaces ErrChaosMarkerConflict as a specific
// error so the caller can produce a better message.
func (e *Executor) setMarker(ctx context.Context, scenarioID, captureDir string) error {
	host, _ := os.Hostname()
	m := pgkube.ChaosMarker{
		Scenario:   scenarioID,
		StartedAt:  time.Now().UTC(),
		PID:        os.Getpid(),
		Host:       host,
		CaptureDir: captureDir,
	}
	return e.K.SetChaosMarkerNamed(ctx, e.Cfg.Namespace, e.Cfg.FG, m)
}

// processAlive reports whether a pid on the local host is still
// alive. Mirrors `kill -0 pid`.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// On Unix os.FindProcess always succeeds; signal 0 probes liveness.
	return p.Signal(nil) == nil
}

// waitForClusterReconverge polls the MFG until every site is in
// {writable, read-only} (i.e. the operator has finished any in-flight
// promotion/recovery), the cluster reports Ready=True, and optional
// Dragonfly topology has returned to Ready. This is the cleanup gate
// for run-all; it must be at least as strict as the next scenario's
// shared precheck for states that cleanup itself can leave behind.
func waitForClusterReconverge(ctx context.Context, env *Env) error {
	return waitForClusterReconvergeStable(ctx, env, 10*time.Second)
}

func waitForClusterReconvergeStable(ctx context.Context, env *Env, stableFor time.Duration) error {
	var healthySince time.Time
	_, err := env.Wait.UntilCR(ctx, env.Namespace, "cluster reconverged after chaos cleanup",
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			ready := pgkube.ReadyCondition(mfg) == "True"
			var bad []string
			if mfg.Spec.FailoverCooldown != nil && mfg.Status.LastFailover != nil {
				remaining := mfg.Spec.FailoverCooldown.Duration - time.Since(mfg.Status.LastFailover.Time)
				if remaining > 0 {
					bad = append(bad, fmt.Sprintf("cooldown=%s", remaining.Round(time.Second)))
				}
			}
			if mfg.Status.UpdatePhase != "" {
				bad = append(bad, fmt.Sprintf("updatePhase=%s", mfg.Status.UpdatePhase))
			}
			for _, s := range mfg.Status.Sites {
				if s.State != "writable" && s.State != "read-only" {
					bad = append(bad, fmt.Sprintf("%s=%s", s.Name, s.State))
				}
				if s.RecoveryState == "RecoveryBlocked" {
					bad = append(bad, fmt.Sprintf("%s=blocked", s.Name))
				}
				if s.Name != mfg.Status.ActiveSite && isPromotableStatusSite(mfg, s.Name) && !s.Replicating {
					bad = append(bad, fmt.Sprintf("%s=not-replicating", s.Name))
				}
			}
			bad = append(bad, dragonflyReconvergeProblems(mfg)...)
			healthy := ready && len(bad) == 0 && mfg.Status.ActiveSite != ""
			if healthy {
				if healthySince.IsZero() {
					healthySince = time.Now()
				}
			} else {
				healthySince = time.Time{}
			}
			stable := time.Duration(0)
			if !healthySince.IsZero() {
				stable = time.Since(healthySince)
			}
			done := healthy && stable >= stableFor
			msg := fmt.Sprintf("ready=%v active=%q bad=%v stableFor=%s/%s",
				ready, mfg.Status.ActiveSite, bad, stable.Round(time.Second), stableFor)
			return done, msg, nil
		},
	)
	return err
}

func dragonflyReconvergeProblems(mfg *v1alpha1.MysqlFailoverGroup) []string {
	if mfg.Spec.Dragonfly == nil || !mfg.Spec.Dragonfly.Enabled {
		return nil
	}
	df := mfg.Status.Dragonfly
	if df == nil {
		return []string{"dragonfly=status-missing"}
	}
	var bad []string
	if df.Phase != v1alpha1.DragonflyPhaseReady {
		bad = append(bad, fmt.Sprintf("dragonfly.phase=%s", df.Phase))
	}
	if df.ActiveSite == "" {
		bad = append(bad, "dragonfly.activeSite=empty")
	} else if mfg.Status.ActiveSite != "" && df.ActiveSite != mfg.Status.ActiveSite {
		bad = append(bad, fmt.Sprintf("dragonfly.activeSite=%s mysql.activeSite=%s", df.ActiveSite, mfg.Status.ActiveSite))
	}
	masters := 0
	for _, s := range df.Sites {
		switch s.Role {
		case v1alpha1.DragonflyRoleMaster:
			masters++
			if s.Name != df.ActiveSite {
				bad = append(bad, fmt.Sprintf("dragonfly.%s=master-active-mismatch", s.Name))
			}
			if !s.Ready || !s.Reachable {
				bad = append(bad, fmt.Sprintf("dragonfly.%s=master-not-ready", s.Name))
			}
		case v1alpha1.DragonflyRoleReplica:
			if !s.Ready || !s.Reachable {
				bad = append(bad, fmt.Sprintf("dragonfly.%s=replica-not-ready", s.Name))
			}
			if s.LinkStatus != "up" {
				bad = append(bad, fmt.Sprintf("dragonfly.%s.link=%s", s.Name, s.LinkStatus))
			}
			if s.SyncInProgress {
				bad = append(bad, fmt.Sprintf("dragonfly.%s=syncing", s.Name))
			}
			if s.LastIOSecondsAgo < 0 {
				bad = append(bad, fmt.Sprintf("dragonfly.%s=never-synced", s.Name))
			}
		case v1alpha1.DragonflyRoleUnreachable:
			bad = append(bad, fmt.Sprintf("dragonfly.%s=unreachable", s.Name))
		case v1alpha1.DragonflyRoleStaleMaster:
			bad = append(bad, fmt.Sprintf("dragonfly.%s=stale-master", s.Name))
		case v1alpha1.DragonflyRoleUnconfigured, v1alpha1.DragonflyRoleUnknown, "":
			bad = append(bad, fmt.Sprintf("dragonfly.%s=%s", s.Name, s.Role))
		}
	}
	if masters != 1 {
		bad = append(bad, fmt.Sprintf("dragonfly.masters=%d", masters))
	}
	return bad
}

func isPromotableStatusSite(mfg *v1alpha1.MysqlFailoverGroup, siteName string) bool {
	for _, site := range mfg.Spec.Sites {
		if site.Name == siteName {
			return site.IsPromotable()
		}
	}
	return false
}

func (e *Executor) buildEnv(ctx context.Context, logger *slog.Logger, cap *Capture, startTime time.Time) (*Env, func(), error) {
	creds, err := pgmysql.LoadCredentials(ctx, e.K, e.Cfg.Namespace)
	if err != nil {
		return nil, func() {}, fmt.Errorf("load credentials: %w", err)
	}
	scraper, err := pgmetrics.NewScraper(ctx, e.K, e.Cfg.Namespace)
	if err != nil {
		return nil, func() {}, fmt.Errorf("metrics scraper: %w", err)
	}
	dragonflyPassword, err := pgdragonfly.LoadPassword(ctx, e.K, e.Cfg.Namespace)
	if err != nil {
		scraper.Close()
		return nil, func() {}, fmt.Errorf("load dragonfly password: %w", err)
	}

	var (
		mu          sync.Mutex
		mysqlMap    = map[string]*pgmysql.SiteClient{}
		sidecarMap  = map[string]*pgsidecar.Probe{}
		tailerMap   = map[string]*pglogs.Tailer{}
		dragonflies []*pgdragonfly.SiteClient // tracked for executor-driven Close
	)
	tailerCtx, cancelTailers := context.WithCancel(context.Background())

	openMySQL := func(site string) (*pgmysql.SiteClient, error) {
		mu.Lock()
		if c, ok := mysqlMap[site]; ok {
			mu.Unlock()
			return c, nil
		}
		mu.Unlock()
		c, err := pgmysql.Open(ctx, e.K, e.Cfg.Namespace, e.Cfg.FG, site, creds)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		mysqlMap[site] = c
		mu.Unlock()
		return c, nil
	}
	openSidecar := func(site string) (*pgsidecar.Probe, error) {
		mu.Lock()
		if p, ok := sidecarMap[site]; ok {
			mu.Unlock()
			return p, nil
		}
		mu.Unlock()
		p, err := pgsidecar.Open(ctx, e.K, e.Cfg.Namespace, e.Cfg.FG, site)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		sidecarMap[site] = p
		mu.Unlock()
		return p, nil
	}
	openLogs := func(component string) (*pglogs.Tailer, error) {
		mu.Lock()
		if t, ok := tailerMap[component]; ok {
			mu.Unlock()
			return t, nil
		}
		mu.Unlock()
		var pod, container string
		switch {
		case component == "operator":
			p, err := e.K.FindPodWithLabel(ctx, e.Cfg.Namespace, pgmetrics.OperatorPodSelector)
			if err != nil {
				return nil, err
			}
			pod = p.Name
			container = "operator"
		case strings.HasPrefix(component, "sidecar:"):
			site := strings.TrimPrefix(component, "sidecar:")
			p, err := e.K.GetSiteMysqlPod(ctx, e.Cfg.Namespace, e.Cfg.FG, site)
			if err != nil {
				return nil, err
			}
			pod = p.Name
			container = "sidecar"
		case strings.HasPrefix(component, "mysql:"):
			site := strings.TrimPrefix(component, "mysql:")
			p, err := e.K.GetSiteMysqlPod(ctx, e.Cfg.Namespace, e.Cfg.FG, site)
			if err != nil {
				return nil, err
			}
			pod = p.Name
			container = "mysql"
		default:
			return nil, fmt.Errorf("unknown log component %q", component)
		}
		t := pglogs.New(pglogs.Source{
			Namespace: e.Cfg.Namespace,
			Pod:       pod,
			Container: container,
			SinceTime: startTime,
		}, 4096)
		t.Start(tailerCtx, e.K)
		mu.Lock()
		tailerMap[component] = t
		mu.Unlock()
		return t, nil
	}

	openDragonfly := func(site string) (*pgdragonfly.SiteClient, error) {
		// Each call dials a fresh client. The active Service's
		// selected pod can flip mid-scenario (master-kill,
		// failover) and a cached client would stay pinned to a
		// stale pod identity, hiding the very routing flip a
		// Dragonfly scenario is meant to observe. The executor
		// closes any client still open at scenario exit.
		c, err := pgdragonfly.Open(ctx, e.K, e.Cfg.Namespace, e.Cfg.FG, site, dragonflyPassword)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		dragonflies = append(dragonflies, c)
		mu.Unlock()
		return c, nil
	}

	var env *Env
	refreshMetrics := func(ctx context.Context) error {
		fresh, err := pgmetrics.NewScraper(ctx, e.K, e.Cfg.Namespace)
		if err != nil {
			return fmt.Errorf("refresh metrics scraper: %w", err)
		}
		mu.Lock()
		old := env.Metrics
		env.Metrics = fresh
		mu.Unlock()
		if old != nil {
			old.Close()
		}
		return nil
	}

	env = &Env{
		Namespace:      e.Cfg.Namespace,
		FG:             e.Cfg.FG,
		StartTime:      startTime,
		Kube:           e.K,
		Chaos:          pgchaos.New(e.K, e.Cfg.Namespace, e.Cfg.FG),
		Wait:           pgwait.NewHelperForFG(e.K, logger, e.Cfg.FG),
		Metrics:        scraper,
		RefreshMetrics: refreshMetrics,
		Logger:         logger,
		Capture:        cap,
		Creds:          creds,
		MySQL:          openMySQL,
		Sidecar:        openSidecar,
		Logs:           openLogs,
		Dragonfly:      openDragonfly,
	}
	e.tailers = tailerMap

	closer := func() {
		cancelTailers()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range mysqlMap {
			_ = c.Close()
		}
		for _, p := range sidecarMap {
			p.Close()
		}
		for _, c := range dragonflies {
			_ = c.Close()
		}
		if env.Metrics != nil {
			env.Metrics.Close()
		}
	}
	return env, closer, nil
}
