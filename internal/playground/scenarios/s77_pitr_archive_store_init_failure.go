package scenarios

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario77PITRArchiveStoreInitFailure())
}

const (
	s77ID     = "77-pitr-archive-store-init-failure"
	s77DBName = "chaos_s77_pitr"
	// s77DegradedWindow is how long the degraded group is watched: more
	// than two operator archiver polls (30 s each) and a dozen archiver
	// scans (archivePollInterval 5 s), each of which retries the store.
	s77DegradedWindow = 75 * time.Second
	// s77ConditionWait bounds a PITRArchiveReady transition: the first
	// poll that sees a failure sets False (polls are 30 s apart); the
	// return to True takes two clean rounds after the store comes up.
	s77ConditionWait = 4 * time.Minute
	// s77StorageInitPrefix starts every store init error in
	// /archiver/status.lastError.
	s77StorageInitPrefix = "archive store init: "
)

// s77BinlogMagic starts every MySQL binary log file.
var s77BinlogMagic = []byte{0xfe, 'b', 'i', 'n'}

// scenario77PITRArchiveStoreInitFailure is #214 item 5 (the v1.6.0
// degraded start, PR #200): a sidecar whose PITR archive store cannot be
// reached keeps serving instead of exiting. Enabling an encrypted PITR
// profile while RustFS is scaled to 0 rolls every site onto sidecars that
// start against dead storage. The rollout must finish, every pod must stay
// Ready, nothing fails over, and the operator only reports
// PITRArchiveReady=False/ArchiveStoreInitFailed. Once RustFS is back the
// running sidecars create the archive and archive a sealed binlog,
// encrypted, without a pod restart.
func scenario77PITRArchiveStoreInitFailure() runner.Scenario {
	return runner.Scenario{
		ID:    s77ID,
		Title: "PITR archive storage down at sidecar start: degraded start, then recovery without a restart",
		Hypothesis: "With RustFS scaled to 0, enabling an encrypted PITR profile still completes the ordered update: every MySQL " +
			"pod becomes Ready with both containers, no emergency failover runs, the group stays Ready with a writable " +
			"primary, every sidecar reports enabled, storeReady=false and an 'archive store init: ' error that it retries, " +
			"and the group reports PITRArchiveReady=False/ArchiveStoreInitFailed with one Warning PITRArchiveNotReady. " +
			"When RustFS returns, the same pods create the archive (binding Bound, PITRArchiveReady=True, Normal " +
			"PITRArchiveRecovered), and a forced FLUSH BINARY LOGS archives the sealed binlog as a BRV1 object that " +
			"decrypts with the run's passphrase to a MySQL binary log.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#77-pitr-archive-store-init-failure",
		// The steps took about 5 minutes on a four-node kind cluster
		// (the rollout about 3 of them). The bounded waits add up to
		// more: the rollout (12 min), the condition (4), the degraded
		// window (1.25), the binding after recovery (12) and the upload
		// (3).
		Timeout: 30 * time.Minute,
		// pitrCleanup restores spec.backup (one more roll), deletes the
		// passphrase Secret, releases the binding and deletes the prefix.
		CleanupTimeout: pitrCleanupBudget(0),
		Precheck:       assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s77Prepare(),
			s77StopStorage(),
			s77EnablePITR(),
			s77WaitRolledDegraded(),
			s77AssertInitFailureReported(),
			s77AssertServingWhileDegraded(),
			s77RestoreStorage(),
			s77AssertRecoveredInPlace(),
			s77AssertEncryptedBinlogArchived(),
		},
		Cleanup: s77Cleanup,
	}
}

func s77Prepare() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "create the RustFS bucket and the run's passphrase Secret",
		Do: func(ctx context.Context, env *runner.Env) error {
			// The emergency-failover check reads the operator log from
			// here on.
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			if err := env.Chaos.EnsureRustFSBucket(ctx, backupE2EBucket); err != nil {
				return err
			}
			runStem := "s77-" + backupRunStamp(env)
			prefix := "e2e/" + s77ID + "/" + runStem
			if err := ctxStash(ctx, env, "backupRunStem", runStem); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "backupPrefix", prefix); err != nil {
				return err
			}
			pitrTrackCR(ctx, env, pitrArchivePrefixKind, prefix)
			secret, err := ensureBackupPassphraseSecret(ctx, env)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("prefix=%s passphrase Secret=%s", prefix, secret))
			return nil
		},
	}
}

func s77StopStorage() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale RustFS to 0 and wait until no RustFS pod exists",
		Do: func(ctx context.Context, env *runner.Env) error {
			pod, err := env.Chaos.ScaleRustFSToZero(ctx)
			if err != nil {
				return err
			}
			env.Capture.Note("scaled RustFS to 0 (pod was " + emptyAsNone(pod) + ")")
			return s77WaitRustFSGone(ctx, env, 2*time.Minute)
		},
	}
}

func s77EnablePITR() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "enable encrypted PITR on a new prefix while RustFS is down",
		Do: func(ctx context.Context, env *runner.Env) error {
			stashSet(env, "pitrEnabledAt", time.Now().UTC().Format(time.RFC3339Nano))
			return patchBackupSpec(ctx, env, pitrBackupSpec(ctxFetch(env, "backupPrefix"), ctxFetch(env, backupPassphraseSecretKey)))
		},
	}
}

func s77WaitRolledDegraded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "the ordered update finishes with RustFS down: every site runs the PITR render and every pod is Ready",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, pitrArchiveSwitchWait)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "PITR rollout done with storage down", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				ready := conditionTrueForGeneration(mfg.Status.Conditions, v1alpha1.ConditionReady, mfg.Generation)
				idle, updating := orderedUpdateIdleAtGeneration(mfg)
				rolled, rollout := s77RolloutLive(waitCtx, env, mfg)
				pods, podsMsg := s77PodsServingLive(waitCtx, env, mfg)
				return ready && idle && mfg.Status.ActiveSite != "" && rolled && pods,
					fmt.Sprintf("ready=%v updating=%s active=%q rollout=%v (%s) pods=%v (%s)", ready, updating, mfg.Status.ActiveSite, rolled, rollout, pods, podsMsg), nil
			})
			if err != nil {
				return err
			}
			if err := s77AssertRustFSDown(ctx, env); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("rolled onto PITR with RustFS down; active=%s binding=%s", mfg.Status.ActiveSite, s77BindingSummary(mfg)))
			return nil
		},
	}
}

func s77AssertInitFailureReported() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "PITRArchiveReady=False/ArchiveStoreInitFailed, a PITRArchiveNotReady Warning, and every sidecar reports and retries the init error",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			candidates := s77Sites(mfg)
			wantPrefix := ctxFetch(env, "backupPrefix") + "/binlogs"

			// Every site's sidecar reports the init error: reader sites
			// archive too.
			first := map[string]int64{}
			for _, site := range candidates {
				st, err := s77ArchiverStatus(ctx, env, site)
				if err != nil {
					return err
				}
				if err := s77CheckStoreInitFailure(st, wantPrefix, site == active); err != nil {
					return fmt.Errorf("site %s: %w", site, err)
				}
				first[site] = st.UploadFailures
				env.Capture.Note(fmt.Sprintf("%s archiver: %s storeReady=%v archive=%+v", site, archiverSummary(st), *st.StoreReady, *st.Archive))
			}

			waitCtx, cancel := context.WithTimeout(ctx, s77ConditionWait)
			defer cancel()
			mfg, err = env.Wait.UntilCR(waitCtx, env.Namespace, "PITRArchiveReady=False/ArchiveStoreInitFailed naming every site", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				ok, msg := s77InitFailedCondition(mfg, candidates)
				return ok, msg, nil
			})
			if err != nil {
				return err
			}
			c := findCondition(mfg.Status.Conditions, v1alpha1.ConditionPITRArchiveReady)
			env.Capture.Note(fmt.Sprintf("PITRArchiveReady=%s/%s %q; status.pitr.message=%q", c.Status, c.Reason, c.Message, mfg.Status.PITR.Message))

			enabledAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, "pitrEnabledAt"))
			if err != nil {
				return fmt.Errorf("parse pitrEnabledAt: %w", err)
			}
			evCtx, evCancel := context.WithTimeout(ctx, time.Minute)
			defer evCancel()
			ev, err := waitForMFGEvent(evCtx, env, enabledAt, v1alpha1.EventPITRArchiveNotReady, "("+v1alpha1.ReasonArchiveStoreInitFailed+")")
			if err != nil {
				return err
			}
			if ev.Type != corev1.EventTypeWarning {
				return fmt.Errorf("event %s is %s, want Warning: %s", ev.Reason, ev.Type, ev.Message)
			}
			env.Capture.Note("event: " + ev.Message)

			// The archiver retries: each failed attempt counts once more.
			retryCtx, retryCancel := context.WithTimeout(ctx, time.Minute)
			defer retryCancel()
			for _, site := range candidates {
				if err := s77WaitRetried(retryCtx, env, site, wantPrefix, site == active, first[site]); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func s77AssertServingWhileDegraded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "degraded but serving: no failover, no restart, pods Ready, sidecars not fenced, group Ready, primary writable",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			base, err := s77TakePods(ctx, env, mfg)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			lastFailover := s77FailoverRecord(mfg)
			candidates := s77Sites(mfg)
			deadline := time.Now().Add(s77DegradedWindow)
			checks := 0
			for {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				if mfg.Status.ActiveSite != active || s77FailoverRecord(mfg) != lastFailover {
					return fmt.Errorf("the group failed over while degraded: active %s -> %s, lastFailover %s -> %s", active, mfg.Status.ActiveSite, lastFailover, s77FailoverRecord(mfg))
				}
				if !conditionTrueForGeneration(mfg.Status.Conditions, v1alpha1.ConditionReady, mfg.Generation) {
					return fmt.Errorf("group not Ready while degraded: %s", conditionsSummary(mfg.Status.Conditions))
				}
				if ok, msg := s77InitFailedCondition(mfg, candidates); !ok {
					return fmt.Errorf("PITRArchiveReady left False/ArchiveStoreInitFailed while storage is down: %s", msg)
				}
				now, err := s77TakePods(ctx, env, mfg)
				if err != nil {
					return err
				}
				if d := s77PodsDiff(base, now); len(d) > 0 {
					return fmt.Errorf("pods changed while degraded: %s", strings.Join(d, "; "))
				}
				for _, site := range candidates {
					if err := s77SidecarServing(ctx, env, site); err != nil {
						return err
					}
				}
				checks++
				if time.Now().After(deadline) {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}
			if err := s77AssertWritable(ctx, env, active); err != nil {
				return err
			}
			if err := pitrCreateMarkerTable(ctx, env, active, s77DBName); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, active, s77DBName, 1, "degraded"); err != nil {
				return err
			}
			if err := s77AssertNoEmergencyFailover(env); err != nil {
				return err
			}
			body, err := json.Marshal(base)
			if err != nil {
				return err
			}
			stashSet(env, "degradedPods", string(body))
			env.Capture.Note(fmt.Sprintf("degraded for %s (%d checks): active=%s unchanged, pods %s unchanged, a marker row committed", s77DegradedWindow, checks, active, s77PodsString(base)))
			return nil
		},
	}
}

func s77RestoreStorage() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale RustFS back to 1",
		Do: func(ctx context.Context, env *runner.Env) error {
			stashSet(env, "storageBackAt", time.Now().UTC().Format(time.RFC3339Nano))
			return env.Chaos.ScaleRustFSToOne(ctx)
		},
	}
}

func s77AssertRecoveredInPlace() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the running sidecars create the archive: binding Bound, archivers Verified, PITRArchiveReady=True, PITRArchiveRecovered, no pod restarted",
		Do: func(ctx context.Context, env *runner.Env) error {
			prefix := ctxFetch(env, "backupPrefix")
			mfg, binding, err := pitrWaitArchiveBound(ctx, env, prefix)
			if err != nil {
				return err
			}
			if binding.Encryption.Mode != v1alpha1.EncryptionModePassphrase || binding.Encryption.KeySecretName == "" {
				return fmt.Errorf("binding %s is not an encrypted identity: %s", binding.ID, s77BindingSummary(mfg))
			}
			waitCtx, cancel := context.WithTimeout(ctx, s77ConditionWait)
			defer cancel()
			for _, site := range s77Sites(mfg) {
				st, err := s77WaitArchiverVerified(waitCtx, env, site, prefix+"/binlogs", binding)
				if err != nil {
					return err
				}
				env.Capture.Note(fmt.Sprintf("%s archiver recovered: %s archive=%+v", site, archiverSummary(st), *st.Archive))
			}
			if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "PITRArchiveReady=True/Archiving", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				c := findCondition(mfg.Status.Conditions, v1alpha1.ConditionPITRArchiveReady)
				if c == nil {
					return false, "no PITRArchiveReady condition", nil
				}
				return c.Status == metav1.ConditionTrue && c.Reason == v1alpha1.ReasonArchiving, fmt.Sprintf("%s/%s %q", c.Status, c.Reason, c.Message), nil
			}); err != nil {
				return err
			}
			backAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, "storageBackAt"))
			if err != nil {
				return fmt.Errorf("parse storageBackAt: %w", err)
			}
			ev, err := waitForMFGEvent(waitCtx, env, backAt, v1alpha1.EventPITRArchiveRecovered, "")
			if err != nil {
				return err
			}
			if ev.Type != corev1.EventTypeNormal {
				return fmt.Errorf("event %s is %s, want Normal: %s", ev.Reason, ev.Type, ev.Message)
			}
			env.Capture.Note(fmt.Sprintf("binding %s Bound (archive %s, key snapshot %s); event: %s", binding.ID, binding.ArchiveID, binding.Encryption.KeySecretName, ev.Message))
			stashSet(env, "archiveID", binding.ArchiveID)
			return s77AssertPodsSinceDegraded(ctx, env)
		},
	}
}

func s77AssertEncryptedBinlogArchived() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "FLUSH BINARY LOGS: the sealed binlog is archived as BRV1 and decrypts with the run's passphrase",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, _, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, active, s77DBName, 2, "recovered"); err != nil {
				return err
			}
			seal, err := pitrStartSealWatch(ctx, env, active)
			if err != nil {
				return err
			}
			defer seal.Close()
			c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return err
			}
			defer c.Close()
			conn, err := c.DB.Conn(ctx)
			if err != nil {
				return fmt.Errorf("pin connection on %s: %w", active, err)
			}
			err = seal.flush(ctx, conn)
			_ = conn.Close()
			if err != nil {
				return err
			}
			if err := seal.wait(ctx, env, true); err != nil {
				return err
			}
			files, err := pitrReadManifest(ctx, env, seal.key)
			if err != nil {
				return err
			}
			var row *pitrManifestFile
			for i := range files {
				if files[i].Name == seal.file {
					row = &files[i]
				}
			}
			if row == nil || row.RemotePath == "" {
				return fmt.Errorf("manifest %s has no row with an object for %s", seal.key, seal.file)
			}
			data, found, err := env.Chaos.ReadRustFSObject(ctx, backupE2EBucket, row.RemotePath)
			if err != nil || !found {
				return fmt.Errorf("read archived %s at %s: found=%v err=%v", seal.file, row.RemotePath, found, err)
			}
			if err := s77CheckEncryptedBinlog(data, s69RunPassphrase(env)); err != nil {
				return fmt.Errorf("archived %s at %s: %w", seal.file, row.RemotePath, err)
			}
			env.Capture.Note(fmt.Sprintf("%s archived by %s as %s (%d bytes, BRV1, decrypts to a binary log)", seal.file, active, row.RemotePath, len(data)))
			if err := s77AssertNoEmergencyFailover(env); err != nil {
				return err
			}
			return s77AssertPodsSinceDegraded(ctx, env)
		},
	}
}

func s77Cleanup(ctx context.Context, env *runner.Env) error {
	// The executor's revert has already scaled RustFS back to 1; the
	// cleanup below reads and deletes the run's prefix there.
	if err := pitrCleanup(ctx, env, s77DBName); err != nil {
		return fmt.Errorf("s77 cleanup: %w", err)
	}
	return nil
}

// ---- helpers ----

// s77Sites lists every site in spec order. Every site's sidecar runs the
// archiver, ReadOnly reader sites included.
func s77Sites(mfg *v1alpha1.MysqlFailoverGroup) []string {
	out := make([]string, 0, len(mfg.Spec.Sites))
	for _, s := range mfg.Spec.Sites {
		out = append(out, s.Name)
	}
	return out
}

// s77AssertWritable reads read_only and super_read_only on a fresh
// connection: the runner's cached client may hold a connection to a pod
// the PITR rollout replaced.
func s77AssertWritable(ctx context.Context, env *runner.Env, site string) error {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s: %w", site, err)
	}
	defer c.Close()
	ro, err := c.ScalarInt(ctx, "SELECT @@global.read_only + @@global.super_read_only")
	if err != nil {
		return fmt.Errorf("read read_only on %s: %w", site, err)
	}
	if ro != 0 {
		return fmt.Errorf("active site %s is read-only while degraded", site)
	}
	return nil
}

// s77RolloutLive is pitrRolloutRestored for the run's
// profile, with the site Deployments read from the cluster.
func s77RolloutLive(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup) (bool, string) {
	deployments := map[string]*appsv1.Deployment{}
	for _, site := range mfg.Spec.Sites {
		dep, err := env.Kube.GetDeployment(ctx, env.Namespace, fmt.Sprintf("mysql-%s-%s", mfg.Name, site.Name))
		if err != nil {
			return false, fmt.Sprintf("read Deployment of site %s: %v", site.Name, err)
		}
		deployments[site.Name] = dep
	}
	return pitrRolloutRestored(mfg, deployments, true, backupE2EProfile)
}

func s77WaitRustFSGone(ctx context.Context, env *runner.Env, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		err := s77AssertRustFSDown(waitCtx, env)
		if err == nil {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("RustFS still has a pod: %w (last: %v)", waitCtx.Err(), err)
		case <-time.After(2 * time.Second):
		}
	}
}

// s77AssertRustFSDown fails while the RustFS Deployment wants a replica or
// any RustFS pod exists.
func s77AssertRustFSDown(ctx context.Context, env *runner.Env) error {
	dep, err := env.Kube.GetDeployment(ctx, env.Namespace, "rustfs")
	if err != nil {
		return err
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
		return fmt.Errorf("RustFS Deployment replicas=%v, want 0", dep.Spec.Replicas)
	}
	pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=rustfs"})
	if err != nil {
		return err
	}
	if len(pods.Items) > 0 {
		return fmt.Errorf("RustFS pod %s still exists (phase %s)", pods.Items[0].Name, pods.Items[0].Status.Phase)
	}
	return nil
}

// s77PodsServingLive reports whether every site's MySQL pod is running,
// Ready, not being deleted, and has every container ready.
func s77PodsServingLive(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup) (bool, string) {
	var pending []string
	for _, site := range mfg.Spec.Sites {
		pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, site.Name)
		if err != nil {
			pending = append(pending, fmt.Sprintf("%s: %v", site.Name, err))
			continue
		}
		if err := s77PodServing(pod); err != nil {
			pending = append(pending, fmt.Sprintf("%s: %v", site.Name, err))
		}
	}
	if len(pending) > 0 {
		return false, strings.Join(pending, "; ")
	}
	return true, "every pod Ready"
}

// s77PodServing requires a running, Ready pod that is not being deleted,
// with a ready mysql and sidecar container and no other unready one.
func s77PodServing(pod *corev1.Pod) error {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
		return fmt.Errorf("pod %s phase=%s ready=%v deleting=%v", pod.Name, pod.Status.Phase, podReady(pod), pod.DeletionTimestamp != nil)
	}
	seen := map[string]bool{}
	for _, cs := range pod.Status.ContainerStatuses {
		if !cs.Ready {
			return fmt.Errorf("pod %s container %s not ready", pod.Name, cs.Name)
		}
		seen[cs.Name] = true
	}
	for _, want := range []string{"mysql", "sidecar"} {
		if !seen[want] {
			return fmt.Errorf("pod %s has no %s container status", pod.Name, want)
		}
	}
	return nil
}

// s77Pod is what a restart changes: the pod's UID and its containers'
// restart counts.
type s77Pod struct {
	Name     string           `json:"name"`
	UID      string           `json:"uid"`
	Restarts map[string]int32 `json:"restarts"`
}

func s77TakePods(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup) (map[string]s77Pod, error) {
	out := map[string]s77Pod{}
	for _, site := range mfg.Spec.Sites {
		pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, site.Name)
		if err != nil {
			return nil, err
		}
		if err := s77PodServing(pod); err != nil {
			return nil, fmt.Errorf("site %s: %w", site.Name, err)
		}
		p := s77Pod{Name: pod.Name, UID: string(pod.UID), Restarts: map[string]int32{}}
		for _, cs := range pod.Status.ContainerStatuses {
			p.Restarts[cs.Name] = cs.RestartCount
		}
		out[site.Name] = p
	}
	return out, nil
}

// s77PodsDiff names every site whose pod was replaced, lost a container
// status or restarted a container between base and now.
func s77PodsDiff(base, now map[string]s77Pod) []string {
	sites := make([]string, 0, len(base))
	for site := range base {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	var diff []string
	for _, site := range sites {
		b := base[site]
		n, ok := now[site]
		if !ok {
			diff = append(diff, fmt.Sprintf("site %s has no pod", site))
			continue
		}
		if n.UID != b.UID {
			diff = append(diff, fmt.Sprintf("site %s pod %s replaced by %s", site, b.Name, n.Name))
			continue
		}
		names := make([]string, 0, len(b.Restarts))
		for c := range b.Restarts {
			names = append(names, c)
		}
		sort.Strings(names)
		for _, c := range names {
			got, ok := n.Restarts[c]
			if !ok {
				diff = append(diff, fmt.Sprintf("site %s pod %s has no %s container status", site, b.Name, c))
			} else if got != b.Restarts[c] {
				diff = append(diff, fmt.Sprintf("site %s pod %s container %s restarted (%d -> %d)", site, b.Name, c, b.Restarts[c], got))
			}
		}
	}
	return diff
}

func s77PodsString(pods map[string]s77Pod) string {
	sites := make([]string, 0, len(pods))
	for site, p := range pods {
		sites = append(sites, site+"="+p.Name)
	}
	sort.Strings(sites)
	return strings.Join(sites, ",")
}

// s77AssertPodsSinceDegraded fails when any pod was replaced or restarted
// a container since the degraded snapshot.
func s77AssertPodsSinceDegraded(ctx context.Context, env *runner.Env) error {
	var base map[string]s77Pod
	if err := json.Unmarshal([]byte(ctxFetch(env, "degradedPods")), &base); err != nil {
		return fmt.Errorf("the degraded pod snapshot was not recorded: %w", err)
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	now, err := s77TakePods(ctx, env, mfg)
	if err != nil {
		return err
	}
	if d := s77PodsDiff(base, now); len(d) > 0 {
		return fmt.Errorf("recovery restarted pods: %s", strings.Join(d, "; "))
	}
	return nil
}

func s77FailoverRecord(mfg *v1alpha1.MysqlFailoverGroup) string {
	if mfg.Status.LastFailover == nil {
		return "<none>"
	}
	return fmt.Sprintf("%s@%s", mfg.Status.LastFailoverTarget(), mfg.Status.LastFailoverTime())
}

// s77AssertNoEmergencyFailover fails when the operator logged the
// emergency path's "initiating failover" since the scenario started. The
// ordered update's handoff does not log it.
func s77AssertNoEmergencyFailover(env *runner.Env) error {
	tail, err := env.Logs("operator")
	if err != nil {
		return err
	}
	if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("initiating failover")); hit {
		return fmt.Errorf("the operator ran an emergency failover: %s", line)
	}
	return nil
}

func s77ArchiverStatus(ctx context.Context, env *runner.Env, site string) (*pgsidecar.ArchiverStatusResponse, error) {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return nil, fmt.Errorf("open sidecar %s: %w", site, err)
	}
	defer probe.Close()
	st, err := probe.ArchiverStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("archiver status of %s: %w", site, err)
	}
	return st, nil
}

// s77CheckStoreInitFailure is the documented /archiver/status of a
// sidecar whose archive store cannot be reached: archiving configured
// (enabled, the S3 store, the profile's prefix, the encrypted identity
// awaiting its descriptor) but stopped, with the init error.
func s77CheckStoreInitFailure(st *pgsidecar.ArchiverStatusResponse, wantPrefix string, wantPrimary bool) error {
	var problems []string
	if !st.Enabled {
		problems = append(problems, "enabled=false")
	}
	if st.StoreReady == nil {
		problems = append(problems, "storeReady absent")
	} else if *st.StoreReady {
		problems = append(problems, "storeReady=true")
	}
	if !strings.HasPrefix(st.LastError, s77StorageInitPrefix) {
		problems = append(problems, fmt.Sprintf("lastError=%q, want %q...", st.LastError, s77StorageInitPrefix))
	}
	if st.StorageType != string(v1alpha1.BackupStorageS3) {
		problems = append(problems, "storageType="+st.StorageType)
	}
	if strings.Trim(st.ManifestPrefix, "/") != strings.Trim(wantPrefix, "/") {
		problems = append(problems, fmt.Sprintf("manifestPrefix=%q, want %q", st.ManifestPrefix, wantPrefix))
	}
	if st.Primary != wantPrimary {
		problems = append(problems, fmt.Sprintf("primary=%v, want %v", st.Primary, wantPrimary))
	}
	if st.UploadFailures < 1 {
		problems = append(problems, "uploadFailures=0")
	}
	if a := st.Archive; a == nil {
		problems = append(problems, "no archive block")
	} else {
		if a.EncryptionMode != pitrarchive.EncryptionPassphrase {
			problems = append(problems, fmt.Sprintf("archive.encryptionMode=%q, want %s", a.EncryptionMode, pitrarchive.EncryptionPassphrase))
		}
		if a.State != pitrarchive.StateAwaitingDescriptor || a.ArchiveID != "" {
			problems = append(problems, fmt.Sprintf("archive state=%s id=%q, want %s without an ID", a.State, a.ArchiveID, pitrarchive.StateAwaitingDescriptor))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("archiver does not report a store init failure: %s", strings.Join(problems, "; "))
	}
	return nil
}

// s77WaitRetried waits until site's archiver has counted another failed
// attempt since first, still reporting the init failure.
func s77WaitRetried(ctx context.Context, env *runner.Env, site, wantPrefix string, wantPrimary bool, first int64) error {
	var last string
	for {
		st, err := s77ArchiverStatus(ctx, env, site)
		if err != nil {
			last = err.Error()
		} else {
			if err := s77CheckStoreInitFailure(st, wantPrefix, wantPrimary); err != nil {
				return fmt.Errorf("site %s: %w", site, err)
			}
			last = archiverSummary(st)
			if st.UploadFailures > first {
				env.Capture.Note(fmt.Sprintf("%s retried: uploadFailures %d -> %d", site, first, st.UploadFailures))
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("site %s did not retry the store: %w (last: %s)", site, ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

// s77InitFailedCondition reports PITRArchiveReady=False with reason
// ArchiveStoreInitFailed, a message naming every site's init error, and
// status.pitr.message carrying them too.
func s77InitFailedCondition(mfg *v1alpha1.MysqlFailoverGroup, sites []string) (bool, string) {
	c := findCondition(mfg.Status.Conditions, v1alpha1.ConditionPITRArchiveReady)
	if c == nil {
		return false, "no PITRArchiveReady condition"
	}
	summary := fmt.Sprintf("%s/%s %q", c.Status, c.Reason, c.Message)
	if c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonArchiveStoreInitFailed {
		return false, summary
	}
	pitrMsg := ""
	if mfg.Status.PITR != nil {
		pitrMsg = mfg.Status.PITR.Message
	}
	for _, site := range sites {
		want := site + ": " + s77StorageInitPrefix
		if !strings.Contains(c.Message, want) {
			return false, summary + " (missing " + want + ")"
		}
		if !strings.Contains(pitrMsg, want) {
			return false, fmt.Sprintf("%s; status.pitr.message=%q lacks %q", summary, pitrMsg, want)
		}
	}
	return true, summary
}

// s77SidecarServing checks a sidecar's readiness endpoint and that it has
// not fenced itself.
func s77SidecarServing(ctx context.Context, env *runner.Env, site string) error {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return fmt.Errorf("open sidecar %s: %w", site, err)
	}
	defer probe.Close()
	ok, body, err := probe.Health(ctx)
	if err != nil {
		return fmt.Errorf("sidecar %s /health: %w", site, err)
	}
	if !ok {
		return fmt.Errorf("sidecar %s /health is not OK while degraded: %s", site, body)
	}
	st, err := probe.Status(ctx)
	if err != nil {
		return fmt.Errorf("sidecar %s /status: %w", site, err)
	}
	if st.SelfFenced {
		return fmt.Errorf("sidecar %s fenced itself while degraded", site)
	}
	return nil
}

// s77WaitArchiverVerified waits until site's archiver reports a ready
// store, no error, and the binding's archive Verified under its key
// snapshot.
func s77WaitArchiverVerified(ctx context.Context, env *runner.Env, site, wantPrefix string, b *v1alpha1.PITRArchiveBinding) (*pgsidecar.ArchiverStatusResponse, error) {
	var last string
	for {
		st, err := s77ArchiverStatus(ctx, env, site)
		if err != nil {
			last = err.Error()
		} else {
			last = archiverSummary(st)
			a := st.Archive
			if st.Enabled && st.StoreReady != nil && *st.StoreReady && st.LastError == "" &&
				strings.Trim(st.ManifestPrefix, "/") == strings.Trim(wantPrefix, "/") && a != nil &&
				a.State == pitrarchive.StateVerified && a.ArchiveID == b.ArchiveID && a.KeyRevision == b.Encryption.KeySecretName &&
				a.EncryptionMode == pitrarchive.EncryptionPassphrase {
				return st, nil
			}
			if a != nil {
				last += fmt.Sprintf(" archive=%+v", *a)
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("site %s archiver not Verified on archive %s: %w (last: %s)", site, b.ArchiveID, ctx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

// s77CheckEncryptedBinlog requires an archived binlog object in the BRV1
// format that decrypts with passphrase to a MySQL binary log.
func s77CheckEncryptedBinlog(data, passphrase []byte) error {
	if !bytes.HasPrefix(data, backupcrypto.Magic[:]) {
		n := min(len(data), 8)
		return fmt.Errorf("does not start with the BRV1 magic (first bytes %q)", data[:n])
	}
	if len(passphrase) == 0 {
		return errors.New("this run stashed no passphrase")
	}
	plain, err := pitrDecryptObject(data, passphrase)
	if err != nil {
		return fmt.Errorf("does not decrypt with the run's passphrase: %w", err)
	}
	if !bytes.HasPrefix(plain, s77BinlogMagic) {
		n := min(len(plain), 8)
		return fmt.Errorf("decrypts to %q..., not a MySQL binary log", plain[:n])
	}
	return nil
}

func s77BindingSummary(mfg *v1alpha1.MysqlFailoverGroup) string {
	parts := make([]string, 0, len(mfg.Status.PITRArchiveBindings))
	for _, b := range mfg.Status.PITRArchiveBindings {
		parts = append(parts, fmt.Sprintf("{%s %s %s/%s key=%s archive=%q msg=%q}", b.ID, b.Destination, b.State, b.Reason, b.Encryption.KeySecretName, b.ArchiveID, b.Message))
	}
	if len(parts) == 0 {
		return "<none>"
	}
	return strings.Join(parts, ",")
}
