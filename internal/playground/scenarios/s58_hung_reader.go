package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	// s58FreezeHold keeps the reader stopped through the whole failover.
	s58FreezeHold = 3 * time.Minute
	// s58BackoffSoak is how long the reader stays unreachable before the
	// primary is lost: long enough for its probe backoff to grow.
	s58BackoffSoak = 30 * time.Second
	// s58FailoverBudget bounds primary-loss-to-promotion while the reader
	// is hung and backed off. The same failover takes ~7s with a healthy
	// reader. Before the fixes, a reader that was down stretched the whole
	// poll loop: 24s with a hung reader, 65s with a reader down for 90s.
	s58FailoverBudget = 15 * time.Second
)

func init() {
	runner.Register(scenario58HungReader())
}

type s58State struct {
	topo     readerTopology
	freeze   *pgchaos.Freeze
	killedAt time.Time
}

// scenario58HungReader checks that a site that is down does not slow
// failover for the rest of the group (chaos round 2026-10-15, F8 and
// F13). The reader's mysqld is frozen with SIGSTOP until the operator has
// declared it unreachable and backed off its probes; then the primary is
// scaled to 0, and promotion must take about as long as with a healthy
// reader.
//
// Before the fixes the poll loop's backoff was shared: one site that
// stayed down slowed every site's polling to a 30s cap, and every poll
// also waited on the hung site's probe timeout. Still open: while a hung
// site is being declared unreachable (its first failureThreshold polls),
// each poll waits on its probe, so a primary lost in that window is
// detected more slowly. This scenario starts after that window.
func scenario58HungReader() runner.Scenario {
	st := &s58State{}
	return runner.Scenario{
		ID:    "58-hung-reader-does-not-slow-failover",
		Title: "A hung reader does not slow failover of the primary",
		Hypothesis: fmt.Sprintf("With the reader's mysqld frozen by SIGSTOP and declared unreachable for %s, "+
			"scaling the primary to 0 promotes the candidate within %s, close to the healthy-reader failover time.",
			s58BackoffSoak, s58FailoverBudget),
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#58-hung-reader",
		Timeout: 5 * time.Minute,
		Precheck: func(ctx context.Context, env *runner.Env) error {
			topo, err := resolveReaderTopology(ctx, env)
			if err != nil {
				return err
			}
			st.topo = topo
			return nil
		},
		Steps: []runner.Step{
			{
				Phase: runner.PhaseInject,
				Name:  "SIGSTOP the reader's mysqld until it is declared unreachable",
				Do: func(ctx context.Context, env *runner.Env) error {
					var err error
					st.freeze, err = env.Chaos.FreezeSiteMysqld(ctx, st.topo.reader, s58FreezeHold)
					if err != nil {
						return err
					}
					env.Capture.Note(fmt.Sprintf("froze reader %s mysqld pid %s", st.topo.reader, st.freeze.PID))
					waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
					defer cancel()
					if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "reader declared unreachable",
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							rs := statusSiteByName(mfg, st.topo.reader)
							if rs == nil {
								return false, "reader missing from status", nil
							}
							return rs.State == "unreachable", "reader state=" + rs.State, nil
						}); err != nil {
						return err
					}
					env.Capture.Note(fmt.Sprintf("reader unreachable %s after the freeze; soaking %s",
						time.Since(st.freeze.FrozenAt).Round(time.Second), s58BackoffSoak))
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(s58BackoffSoak):
					}
					return nil
				},
			},
			{
				Phase: runner.PhaseInject,
				Name:  "scale the primary to 0",
				Do: func(ctx context.Context, env *runner.Env) error {
					st.killedAt = time.Now()
					env.Capture.Note(fmt.Sprintf("scaling primary %s to 0", st.topo.active))
					return env.Chaos.ScaleSiteToZero(ctx, st.topo.active)
				},
			},
			{
				Phase: runner.PhaseVerify,
				Name:  fmt.Sprintf("candidate promoted within %s", s58FailoverBudget),
				Do: func(ctx context.Context, env *runner.Env) error {
					candidate := st.topo.standby
					waitCtx, cancel := context.WithDeadline(ctx, st.killedAt.Add(s58FailoverBudget))
					defer cancel()
					_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "candidate promoted",
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							return mfg.Status.ActiveSite == candidate,
								fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite), nil
						})
					if err != nil {
						return fmt.Errorf("failover with a hung reader took longer than %s: %w", s58FailoverBudget, err)
					}
					env.Capture.Note(fmt.Sprintf("promoted %s %s after the primary loss",
						candidate, time.Since(st.killedAt).Round(100*time.Millisecond)))
					return nil
				},
			},
			{
				Phase: runner.PhaseCleanup,
				Name:  "thaw the reader",
				Do: func(ctx context.Context, env *runner.Env) error {
					return st.freeze.Thaw(ctx)
				},
			},
		},
	}
}
