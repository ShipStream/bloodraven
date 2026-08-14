package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario50EncryptedPodReplace())
}

type s50RunState struct {
	activeSite  string
	replicaSite string

	// sealedSecret / sealedVersion are captured before the pod is
	// deleted so the verify step can prove the site came back on the
	// SAME escrow version rather than re-bootstrapping a new keyring.
	sealedSecret  string
	sealedVersion int32

	// rowCount is read before the kill so the post-replace read proves
	// the data actually decrypted rather than returning an empty set.
	rowCount int64
}

// scenario50EncryptedPodReplace covers EXP-12 ("data readable after pod /
// node replace") and the durable half of EXP-04 ("restart at any phase
// must not brick the group") from the encryption chaos plan.
//
// The property is the one the whole sealed rendering exists to provide:
// the keyring lives only in a Secret and on tmpfs, so replacing the pod
// throws away every in-memory copy. If the Secret projection, the
// component manifest copy, or the my.cnf/keyring pairing is wrong in any
// way, the replacement pod does not come back — InnoDB aborts startup
// with "Check keyring fail" and the site is bricked until an admin
// intervenes. Nothing short of actually destroying a pod tests that.
//
// It also asserts the adopt-atomicity invariant (#136) as a structural
// precondition on every site: a Deployment whose ConfigMap carries
// encryption settings must also carry keyring wiring, and vice versa. A
// site that violates it is restartable-looking right up until it
// restarts, which is exactly the failure mode #136 described.
//
// Quarantined for the same reason as scenario 48: it requires a
// playground brought up with TLS and spec.encryptionAtRest via
// ./playground/enable-encryption.sh, which is not the shared baseline.
func scenario50EncryptedPodReplace() runner.Scenario {
	state := &s50RunState{}
	return runner.Scenario{
		ID:    "50-encrypted-pod-replace",
		Title: "A sealed site survives losing its pod: the escrow Secret re-projects, mysqld restarts, and the data still decrypts",
		Hypothesis: "Every encrypted site pairs encryption my.cnf with keyring wiring (adopt atomicity). Deleting the " +
			"sealed replica's pod destroys its tmpfs keyring; the replacement pod becomes Ready without an InnoDB " +
			"keyring abort, the site stays on the SAME escrow version (no re-bootstrap), MySQL reports a read-only " +
			"keyring outside /var/lib/mysql, and previously written rows read back.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#50-encrypted-pod-replace",
		Timeout: 15 * time.Minute,
		Quarantine: "requires the dedicated TLS + spec.encryptionAtRest baseline; " +
			"CI and local encryption jobs run this scenario explicitly",
		Precheck: s50Precheck(state),
		Steps: []runner.Step{
			s50VerifyAdoptAtomicity(state),
			s50CaptureSealedState(state),
			s50DeleteSealedPod(state),
			s50VerifyReplacementDecrypts(state),
		},
		// Nothing is injected that outlives the pod deletion — the
		// Deployment controller recreates the pod — so cleanup only has to
		// exist for the executor's contract.
		Cleanup: func(context.Context, *runner.Env) error { return nil },
	}
}

func s50Precheck(state *s50RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s50RunState{}
		if err := AssertHealthyBaseline(ctx, env); err != nil {
			return err
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return err
		}
		if err := s50RequireEncryptedBaseline(mfg); err != nil {
			return err
		}
		state.activeSite = mfg.Status.ActiveSite
		state.replicaSite, err = PeerOf(mfg, state.activeSite)
		if err != nil {
			return err
		}
		return nil
	}
}

// s50RequireEncryptedBaseline is shared by the encryption scenarios so
// they all fail with the same actionable message rather than timing out
// on an unencrypted playground.
func s50RequireEncryptedBaseline(mfg *v1alpha1.MysqlFailoverGroup) error {
	if !mfg.Spec.EncryptionEnabled() {
		return fmt.Errorf(
			"spec.encryptionAtRest is not enabled on %s/%s; run ./playground/enable-encryption.sh first",
			mfg.Namespace, mfg.Name)
	}
	if mfg.Spec.TLS == nil {
		return fmt.Errorf("spec.tls is not set; encryption at rest requires it (the escrow push and encrypted CLONE both need TLS)")
	}
	if mfg.Status.ActiveSite == "" {
		return fmt.Errorf("no active site reported; cluster has not settled")
	}
	if mfg.Status.EncryptionAtRest == nil || !mfg.Status.EncryptionAtRest.Sealed {
		return fmt.Errorf("not every site is sealed yet: %s", s48UnsealedSummary(mfg.Status.EncryptionAtRest))
	}
	return nil
}

// s50VerifyAdoptAtomicity is the #136 invariant expressed as a live
// cluster check. It runs before anything is injected, so a violation is
// reported as a precondition failure rather than being blamed on the
// pod kill.
func s50VerifyAdoptAtomicity(state *s50RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "every site's ConfigMap and Deployment agree about encryption (adopt atomicity)",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			for _, site := range mfg.Spec.Sites {
				deploy, err := env.Kube.GetDeployment(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, site.Name))
				if err != nil {
					return fmt.Errorf("get deployment for %s: %w", site.Name, err)
				}
				configName := s50DeploymentConfigMapName(deploy)
				if configName == "" {
					return fmt.Errorf("site %s has no config volume in its rendered pod spec", site.Name)
				}
				cm, err := env.Kube.GetConfigMap(ctx, env.Namespace, configName)
				if err != nil {
					return fmt.Errorf("site %s references ConfigMap %q which cannot be read — the pod cannot restart: %w",
						site.Name, configName, err)
				}
				cnfEncrypted := strings.Contains(cm.Data["bloodraven.cnf"], "binlog-encryption=ON")
				hasKeyring := s50HasKeyringVolume(deploy)
				if cnfEncrypted != hasKeyring {
					return fmt.Errorf(
						"site %s is not restartable: ConfigMap %s encryption=%v but keyring wiring present=%v — "+
							"a pod restart would abort with 'Check keyring fail'",
						site.Name, configName, cnfEncrypted, hasKeyring)
				}
			}
			env.Logger.Info("adopt atomicity holds on every site")
			return nil
		},
	}
}

func s50CaptureSealedState(state *s50RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "record the replica's escrow version and a row count to verify against after the replace",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			s := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
			if s == nil || s.Phase != v1alpha1.KeyringPhaseSealed {
				return fmt.Errorf("replica %s is not sealed: %+v", state.replicaSite, s)
			}
			state.sealedSecret = s.KeyringSecret
			state.sealedVersion = s.KeyringVersion

			// Write through the primary so the row is guaranteed to be in
			// an encrypted tablespace created after encryption was on, then
			// wait for the replica to have it.
			primary, err := env.MySQL(state.activeSite)
			if err != nil {
				return fmt.Errorf("open mysql on %s: %w", state.activeSite, err)
			}
			for _, q := range []string{
				"CREATE DATABASE IF NOT EXISTS chaos_encryption",
				"CREATE TABLE IF NOT EXISTS chaos_encryption.sealed_rows (id INT PRIMARY KEY AUTO_INCREMENT, payload VARCHAR(64))",
				fmt.Sprintf("INSERT INTO chaos_encryption.sealed_rows (payload) VALUES ('replace-%d')", time.Now().UnixNano()),
			} {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("seed encrypted row via %s (%q): %w", state.activeSite, q, err)
				}
			}

			replica, err := env.MySQL(state.replicaSite)
			if err != nil {
				return fmt.Errorf("open mysql on %s: %w", state.replicaSite, err)
			}
			deadline := time.Now().Add(90 * time.Second)
			for {
				count, err := replica.ScalarInt(ctx, "SELECT COUNT(*) FROM chaos_encryption.sealed_rows")
				if err == nil && count > 0 {
					state.rowCount = count
					break
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("replica %s never received the seeded row (last err=%v)", state.replicaSite, err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
			env.Logger.Info("captured pre-replace state",
				"site", state.replicaSite, "escrow", state.sealedSecret,
				"version", state.sealedVersion, "rows", state.rowCount)
			return nil
		},
	}
}

// s50DeleteSealedPod is the injection: destroy the pod, and with it every
// in-memory and tmpfs copy of the keyring. The only remaining copy is the
// escrow Secret.
func s50DeleteSealedPod(state *s50RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "delete the sealed replica's pod, destroying its tmpfs keyring",
		Do: func(ctx context.Context, env *runner.Env) error {
			pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, state.replicaSite)
			if err != nil {
				return fmt.Errorf("find pod for %s: %w", state.replicaSite, err)
			}
			env.Logger.Info("deleting sealed pod", "site", state.replicaSite, "pod", pod.Name)
			if err := env.Chaos.DeleteSitePod(ctx, state.replicaSite); err != nil {
				return fmt.Errorf("delete pod for %s: %w", state.replicaSite, err)
			}

			// Wait for a genuinely different pod to be Ready. Comparing
			// names matters: the Deployment recreates quickly enough that
			// a bare "is there a ready pod" check can observe the old one.
			deadline := time.Now().Add(8 * time.Minute)
			for {
				fresh, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, state.replicaSite)
				if err == nil && fresh.Name != pod.Name && s50PodReady(fresh) {
					env.Logger.Info("replacement pod is ready", "site", state.replicaSite, "pod", fresh.Name)
					return nil
				}
				if time.Now().After(deadline) {
					detail := "pod not found"
					if err == nil {
						detail = fmt.Sprintf("pod=%s phase=%s ready=%v", fresh.Name, fresh.Status.Phase, s50PodReady(fresh))
						if logs, logErr := env.Kube.PodLogTailLines(ctx, env.Namespace, fresh.Name, "mysql", 50); logErr == nil {
							if strings.Contains(string(logs), "Check keyring fail") {
								return fmt.Errorf(
									"replacement pod for %s aborted with 'Check keyring fail' — the sealed rendering "+
										"is not self-sufficient and the site is bricked (%s)", state.replicaSite, detail)
							}
							env.Capture.Note(fmt.Sprintf("mysql tail for %s:\n%s", fresh.Name, string(logs)))
						}
					}
					return fmt.Errorf("replacement pod for %s never became ready: %s", state.replicaSite, detail)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}
		},
	}
}

// s50VerifyReplacementDecrypts is the payoff: the site is back on the
// same escrow version, MySQL holds a read-only keyring off the data
// volume, and the rows written before the kill read back.
func s50VerifyReplacementDecrypts(state *s50RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the replacement pod is sealed on the same escrow version and the data decrypts",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Wait.UntilCR(ctx, env.Namespace,
				fmt.Sprintf("site %s back to Sealed after the pod replace", state.replicaSite),
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if m.Status.EncryptionAtRest == nil {
						return false, "status.encryptionAtRest not populated", nil
					}
					s := m.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
					if s == nil {
						return false, "no status entry for " + state.replicaSite, nil
					}
					if s.Phase != v1alpha1.KeyringPhaseSealed {
						return false, fmt.Sprintf("phase=%s (%s)", s.Phase, s.Message), nil
					}
					return true, "", nil
				})
			if err != nil {
				return fmt.Errorf("waiting for %s to re-seal: %w", state.replicaSite, err)
			}

			after := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
			// Replacing a pod must not mint a new keyring. A version bump
			// here would mean the site re-bootstrapped — i.e. it came up
			// with keys that do not match what its tablespaces were
			// written with, which is only survivable by luck.
			if after.KeyringVersion != state.sealedVersion || after.KeyringSecret != state.sealedSecret {
				return fmt.Errorf(
					"site %s came back on escrow %s v%d, want the pre-replace %s v%d — the pod re-bootstrapped its keyring "+
						"instead of re-projecting the escrowed one",
					state.replicaSite, after.KeyringSecret, after.KeyringVersion, state.sealedSecret, state.sealedVersion)
			}

			// A fresh port-forward: env.MySQL caches per site and the old
			// client is pinned to the destroyed pod's sandbox.
			db, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, state.replicaSite, env.Creds)
			if err != nil {
				return fmt.Errorf("open mysql on %s after the replace: %w", state.replicaSite, err)
			}
			defer db.Close()

			readOnly, err := db.ScalarString(ctx,
				"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Read_only'")
			if err != nil {
				return fmt.Errorf("query keyring status on %s: %w", state.replicaSite, err)
			}
			if !strings.EqualFold(readOnly, "Yes") {
				return fmt.Errorf("site %s came back with a writable keyring (Read_only=%q)", state.replicaSite, readOnly)
			}
			dataFile, err := db.ScalarString(ctx,
				"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Data_file'")
			if err != nil {
				return fmt.Errorf("query keyring data file on %s: %w", state.replicaSite, err)
			}
			if strings.HasPrefix(dataFile, "/var/lib/mysql") {
				return fmt.Errorf(
					"site %s keyring data file is %q after the replace — it landed on the data PVC, so a stolen volume carries the key",
					state.replicaSite, dataFile)
			}

			// The actual decrypt. Reading rows out of a tablespace created
			// under encryption is what proves the re-projected Secret holds
			// the right master key.
			count, err := db.ScalarInt(ctx, "SELECT COUNT(*) FROM chaos_encryption.sealed_rows")
			if err != nil {
				return fmt.Errorf("reading encrypted rows on %s after the replace failed: %w", state.replicaSite, err)
			}
			if count < state.rowCount {
				return fmt.Errorf("site %s reads %d rows after the replace, had %d before", state.replicaSite, count, state.rowCount)
			}

			env.Logger.Info("sealed pod replaced and data decrypted",
				"site", state.replicaSite, "escrow", after.KeyringSecret,
				"version", after.KeyringVersion, "rows", count)
			return nil
		},
	}
}

// --- small helpers ---------------------------------------------------

func s50DeploymentConfigMapName(deploy *appsv1.Deployment) string {
	for _, v := range deploy.Spec.Template.Spec.Volumes {
		if v.Name == "config" && v.ConfigMap != nil {
			return v.ConfigMap.Name
		}
	}
	return ""
}

func s50PodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func s50HasKeyringVolume(deploy *appsv1.Deployment) bool {
	for _, v := range deploy.Spec.Template.Spec.Volumes {
		if v.Name == "keyring" {
			return true
		}
	}
	return false
}
