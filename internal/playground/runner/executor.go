package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
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

	env, envCloser, err := e.buildEnv(scenarioCtx, logger, cap)
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
		_ = cap.Persist(scenarioCtx, e.K, e.Cfg.Namespace, env.Metrics, e.tailers, failureBlock)
		if !e.Cfg.NoCleanup {
			res.CleanupErr = e.cleanup(env, s)
		}
		return res
	}

	cap.Note(fmt.Sprintf("scenario %s start: %s", s.ID, s.Title))

	if s.Precheck != nil {
		if err := e.runWithLog(scenarioCtx, env, PhasePrecheck, "Precheck", s.Precheck); err != nil {
			return fail(PhasePrecheck, "Precheck", err.Error())
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
		res.CleanupErr = e.cleanup(env, s)
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
	cleanCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	return errors.Join(errs...)
}

func (e *Executor) buildEnv(ctx context.Context, logger *slog.Logger, cap *Capture) (*Env, func(), error) {
	creds, err := pgmysql.LoadCredentials(ctx, e.K, e.Cfg.Namespace)
	if err != nil {
		return nil, func() {}, fmt.Errorf("load credentials: %w", err)
	}
	scraper, err := pgmetrics.NewScraper(ctx, e.K, e.Cfg.Namespace)
	if err != nil {
		return nil, func() {}, fmt.Errorf("metrics scraper: %w", err)
	}

	var (
		mu         sync.Mutex
		mysqlMap   = map[string]*pgmysql.SiteClient{}
		sidecarMap = map[string]*pgsidecar.Probe{}
		tailerMap  = map[string]*pglogs.Tailer{}
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
		t := pglogs.New(pglogs.Source{Namespace: e.Cfg.Namespace, Pod: pod, Container: container}, 4096)
		t.Start(tailerCtx, e.K)
		mu.Lock()
		tailerMap[component] = t
		mu.Unlock()
		return t, nil
	}

	env := &Env{
		Namespace: e.Cfg.Namespace,
		FG:        e.Cfg.FG,
		Kube:      e.K,
		Chaos:     pgchaos.New(e.K, e.Cfg.Namespace, e.Cfg.FG),
		Wait:      pgwait.NewHelper(e.K, logger),
		Metrics:   scraper,
		Logger:    logger,
		Capture:   cap,
		Creds:     creds,
		MySQL:     openMySQL,
		Sidecar:   openSidecar,
		Logs:      openLogs,
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
		scraper.Close()
	}
	return env, closer, nil
}
