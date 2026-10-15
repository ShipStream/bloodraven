package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	// s57FreezeHold is how long the primary's mysqld stays stopped. It
	// covers detection, promotion and a sidecar liveness window (3 x
	// 10s), so a liveness probe that still followed MySQL would restart
	// the sidecar inside it.
	s57FreezeHold = 50 * time.Second
	// s57DetectBudget bounds freeze-to-promotion. With refused
	// connections failover takes ~7s; a hung server costs a read timeout
	// per probe, and promotion also waits out the fence attempt on the
	// hung primary. Before the fix each poll re-probed the hung site
	// several times and promotion took ~48s; after it, ~25s.
	s57DetectBudget = 35 * time.Second
	// s57FenceBudget bounds thaw-to-read-only on the old primary. The
	// sidecar's topology-mismatch rule fences as soon as its first check
	// after the thaw runs; before the fix the sidecar had been restarted
	// during the hang and was not running at thaw (21-43s writable).
	s57FenceBudget = 10 * time.Second
)

func init() {
	runner.Register(scenario57HungPrimary())
}

type s57State struct {
	original, peer  string
	podName         string
	sidecarRestarts int32
	freeze          *pgchaos.Freeze
}

// scenario57HungPrimary freezes the primary's mysqld with SIGSTOP,
// a hung server rather than a dead one (chaos round 2026-10-15, F2/F3).
// Connections are accepted and every query blocks to the client's read
// timeout, which no pod-kill or scale-to-0 scenario exercises.
//
// Hard assertions:
//   - failover completes within s57DetectBudget of the freeze;
//   - the frozen pod's sidecar is not restarted (its liveness probe does
//     not depend on MySQL);
//   - after the thaw the old primary is read-only within s57FenceBudget.
func scenario57HungPrimary() runner.Scenario {
	st := &s57State{}
	return runner.Scenario{
		ID:    "57-hung-primary-detected-and-fenced",
		Title: "SIGSTOPped primary is replaced promptly and fenced when it resumes",
		Hypothesis: fmt.Sprintf("With the primary's mysqld frozen by SIGSTOP for %s, the operator promotes the peer "+
			"within %s, the frozen site's sidecar is never restarted, and the old primary is read-only within %s "+
			"of resuming.", s57FreezeHold, s57DetectBudget, s57FenceBudget),
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#57-hung-primary",
		Timeout:  6 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s57Freeze(st),
			s57ObserveFailover(st),
			s57VerifyFencedAfterThaw(st),
			s57VerifySidecarNotRestarted(st),
		},
	}
}

func s57Freeze(st *s57State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  fmt.Sprintf("SIGSTOP the primary's mysqld for %s", s57FreezeHold),
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			st.original = mfg.Status.ActiveSite
			if st.peer, err = PeerOf(mfg, st.original); err != nil {
				return err
			}
			pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, st.original)
			if err != nil {
				return err
			}
			st.podName = pod.Name
			if st.sidecarRestarts, err = env.Kube.SidecarRestartCount(ctx, env.Namespace, pod.Name); err != nil {
				return err
			}
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			st.freeze, err = env.Chaos.FreezeSiteMysqld(ctx, st.original, s57FreezeHold)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("froze mysqld pid %s on %s (pod %s, sidecar restarts %d) at %s",
				st.freeze.PID, st.original, st.podName, st.sidecarRestarts, st.freeze.FrozenAt.Format(time.RFC3339)))
			return nil
		},
	}
}

func s57ObserveFailover(st *s57State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  fmt.Sprintf("peer promoted within %s of the freeze", s57DetectBudget),
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithDeadline(ctx, st.freeze.FrozenAt.Add(s57DetectBudget))
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "activeSite moves to the peer",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					return mfg.Status.ActiveSite == st.peer,
						fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite), nil
				})
			if err != nil {
				return fmt.Errorf("hung primary not replaced within %s: %w", s57DetectBudget, err)
			}
			env.Capture.Note(fmt.Sprintf("promotion observed %s after the freeze",
				time.Since(st.freeze.FrozenAt).Round(100*time.Millisecond)))
			return nil
		},
	}
}

func s57VerifyFencedAfterThaw(st *s57State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  fmt.Sprintf("old primary read-only within %s of resuming", s57FenceBudget),
		Do: func(ctx context.Context, env *runner.Env) error {
			thawedAt, err := st.freeze.WaitThawed(ctx)
			if err != nil {
				return err
			}
			// A fresh connection, opened after the thaw: the handshake
			// would block on a stopped mysqld, and a cached client's
			// port-forward may have broken during the hang.
			client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, st.original, env.Creds)
			if err != nil {
				return fmt.Errorf("open %s: %w", st.original, err)
			}
			defer func() { _ = client.Close() }()
			deadline := thawedAt.Add(s57FenceBudget)
			for {
				qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				ro, err := client.SuperReadOnly(qctx)
				cancel()
				if err == nil && ro {
					env.Capture.Note(fmt.Sprintf("old primary %s super_read_only=ON %s after thaw",
						st.original, time.Since(thawedAt).Round(100*time.Millisecond)))
					return nil
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("old primary %s still not super_read_only %s after resuming (last err: %v)",
						st.original, time.Since(thawedAt).Round(time.Second), err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(250 * time.Millisecond):
				}
			}
		},
	}
}

func s57VerifySidecarNotRestarted(st *s57State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "frozen site's sidecar was not restarted",
		Do: func(ctx context.Context, env *runner.Env) error {
			// A liveness failure inside the hang would surface as a
			// restart shortly after it; give the kubelet a probe period.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(15 * time.Second):
			}
			restarts, err := env.Kube.SidecarRestartCount(ctx, env.Namespace, st.podName)
			if err != nil {
				return err
			}
			if restarts != st.sidecarRestarts {
				return fmt.Errorf("sidecar on %s restarted %d time(s) during the hang; the fencing agent was down "+
					"while it was needed", st.podName, restarts-st.sidecarRestarts)
			}
			return nil
		},
	}
}
