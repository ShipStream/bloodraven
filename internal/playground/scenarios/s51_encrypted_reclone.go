package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario51EncryptedReclone())
}

type s51RunState struct {
	activeSite  string
	replicaSite string

	// escrowBefore is the replica's escrow version at the moment the
	// reclone is requested. It must never go backwards; it is allowed to
	// stay put, because a recipient seeded from its own escrow already
	// holds a usable master key and the clone rewraps under that one.
	escrowBefore int32
	secretBefore string

	// rowCount is the donor's row count, used to prove the clone carried
	// real (encrypted) data rather than producing an empty datadir.
	rowCount int64

	// annotated records that the reclone annotation was applied, so
	// cleanup only tries to clear one that exists.
	annotated bool
}

// scenario51EncryptedReclone covers EXP-06 from the encryption chaos
// plan: CLONE INSTANCE into a sealed recipient.
//
// This is the one lifecycle path where the operator must deliberately
// UNSEAL a site and then let it run with a writable keyring. MySQL's
// clone re-encrypts the donor's tablespace keys under the recipient's own
// master key, so the operator rolls the pod onto the unsealed rendering
// FIRST, waits for it to be Ready, and only then starts the clone
// (RequestKeyringUnseal returning false is what defers it).
//
// What this asserts that a unit test cannot: that the gate is actually
// WIRED in the running operator, including on groups that adopted
// encryption after their topology manager was built. The gate is always
// set; RequestKeyringUnseal is a no-op while encryption is off.
//
// Note what is deliberately NOT asserted: that the clone mints a new
// escrow version. A recipient seeded from its own escrow already holds a
// usable master key and the clone rewraps under that one, so the version
// legitimately stays put (verified against MySQL 9.7).
//
// Destructive by design — it wipes and rebuilds the replica's datadir —
// so it is quarantined out of the batch profiles alongside the other
// encryption scenarios.
func scenario51EncryptedReclone() runner.Scenario {
	state := &s51RunState{}
	return runner.Scenario{
		ID:    "51-encrypted-reclone",
		Title: "Recloning a sealed replica unseals it first, clones through a writable keyring, then re-seals against a verified escrow",
		Hypothesis: "Annotating reclone-site on a Sealed replica moves it to Unsealed/Clone and re-renders its pod onto a " +
			"memory-backed keyring BEFORE any CLONE INSTANCE runs; the clone completes; the site returns to Sealed against " +
			"an escrow no older than the pre-clone one, with a read-only keyring still off the data PVC and the donor's " +
			"rows readable.",
		Risk:    "high",
		DocLink: "playground/chaos-scenarios.md#51-encrypted-reclone",
		Timeout: 20 * time.Minute,
		Quarantine: "destructive (wipes the replica datadir) and requires the dedicated TLS + " +
			"spec.encryptionAtRest baseline; CI and local encryption jobs run this scenario explicitly.",
		Precheck: s51Precheck(state),
		Steps: []runner.Step{
			s51SeedDonorData(state),
			s51RequestReclone(state),
			s51ObserveUnsealBeforeClone(state),
			s51VerifyResealedAfterClone(state),
		},
		Cleanup: s51Cleanup(state),
	}
}

func s51Precheck(state *s51RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s51RunState{}
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
		// Recloning the primary would take the group down; the interlock
		// in the operator does not stop us from asking, so refuse here.
		if state.replicaSite == state.activeSite {
			return fmt.Errorf("refusing to reclone the active primary %s", state.activeSite)
		}
		return nil
	}
}

// s51SeedDonorData writes rows on the primary so the clone has something
// to carry, and records the count the recipient must end up with.
func s51SeedDonorData(state *s51RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "seed encrypted rows on the donor and record the pre-clone escrow version",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			before := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
			if before == nil || before.Phase != v1alpha1.KeyringPhaseSealed {
				return fmt.Errorf("replica %s is not sealed before the reclone: %+v", state.replicaSite, before)
			}
			state.escrowBefore = before.KeyringVersion
			state.secretBefore = before.KeyringSecret

			db, err := env.MySQL(state.activeSite)
			if err != nil {
				return fmt.Errorf("open mysql on %s: %w", state.activeSite, err)
			}
			for _, q := range []string{
				"CREATE DATABASE IF NOT EXISTS chaos_encryption",
				"CREATE TABLE IF NOT EXISTS chaos_encryption.clone_rows (id INT PRIMARY KEY AUTO_INCREMENT, payload VARCHAR(64))",
				fmt.Sprintf("INSERT INTO chaos_encryption.clone_rows (payload) VALUES ('clone-%d')", time.Now().UnixNano()),
			} {
				if _, err := db.Exec(ctx, q); err != nil {
					return fmt.Errorf("seed donor row via %s (%q): %w", state.activeSite, q, err)
				}
			}
			state.rowCount, err = db.ScalarInt(ctx, "SELECT COUNT(*) FROM chaos_encryption.clone_rows")
			if err != nil {
				return fmt.Errorf("count donor rows: %w", err)
			}
			env.Logger.Info("donor seeded",
				"site", state.activeSite, "rows", state.rowCount,
				"replicaEscrow", state.secretBefore, "version", state.escrowBefore)
			return nil
		},
	}
}

// s51RequestReclone submits the cold-reclone annotation. The recipient
// has no divergent GTID, so the interlock requires the explicit
// confirm=<group> token — asserting the operator accepts the documented
// form is itself worth pinning.
func s51RequestReclone(state *s51RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "annotate the sealed replica for a cold reclone",
		Do: func(ctx context.Context, env *runner.Env) error {
			value := fmt.Sprintf("%s:confirm=%s", state.replicaSite, env.FG)
			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
				"bloodraven.shipstream.io/reclone-site", value); err != nil {
				return fmt.Errorf("set reclone annotation: %w", err)
			}
			state.annotated = true
			env.Logger.Info("reclone requested", "value", value)
			return nil
		},
	}
}

// s51ObserveUnsealBeforeClone checks the clone gate: the site must reach
// Unsealed/Clone and its pod must actually re-render onto a writable
// keyring before CLONE INSTANCE runs. Escrowed/Clone is the #144
// livelock and must fail.
func s51ObserveUnsealBeforeClone(state *s51RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "the recipient unseals onto a writable keyring before the clone runs",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()

			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s observed Unsealed/Clone", state.replicaSite),
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if m.Status.EncryptionAtRest == nil {
						return false, "status.encryptionAtRest not populated", nil
					}
					s := m.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
					if s == nil {
						return false, "no status entry for " + state.replicaSite, nil
					}
					if s.UnsealReason == v1alpha1.UnsealReasonClone &&
						(s.Phase == v1alpha1.KeyringPhaseEscrowed || s.Phase == v1alpha1.KeyringPhaseSealed) {
						return false, fmt.Sprintf("phase=%s reason=Clone — the #144 livelock", s.Phase),
							fmt.Errorf("site %s advanced to %s while still held for clone", state.replicaSite, s.Phase)
					}
					if s.UnsealReason == v1alpha1.UnsealReasonClone && s.Phase == v1alpha1.KeyringPhaseUnsealed {
						return true, "", nil
					}
					return false, fmt.Sprintf("phase=%s reason=%s v%d (%s)",
						s.Phase, s.UnsealReason, s.KeyringVersion, s.Message), nil
				})
			if err != nil {
				return fmt.Errorf("waiting for the clone unseal: %w", err)
			}

			// Caught it mid-window. The status flip is only the operator's
			// intent — the rendering follows on a later reconcile, and the
			// clone itself is deferred until that Deployment has rolled and
			// gone Ready (deploymentReadyWithUnsealedKeyring). So poll for
			// the re-render rather than demanding it be simultaneous with
			// the status write; asserting instantaneously is a race, not an
			// invariant.
			renderCtx, renderCancel := context.WithTimeout(ctx, 3*time.Minute)
			defer renderCancel()
			var last string
			for {
				deploy, err := env.Kube.GetDeployment(renderCtx, env.Namespace,
					pgkube.MysqlDeploymentName(env.FG, state.replicaSite))
				if err == nil {
					found := false
					for _, v := range deploy.Spec.Template.Spec.Volumes {
						if v.Name != "keyring" {
							continue
						}
						found = true
						if v.EmptyDir != nil {
							env.Logger.Info("recipient re-rendered onto a memory-backed keyring before the clone",
								"site", state.replicaSite)
							return nil
						}
						last = fmt.Sprintf("keyring volume is still %+v", v.VolumeSource)
					}
					if !found {
						last = "no keyring volume in the rendered pod spec"
					}
				} else {
					last = err.Error()
				}
				select {
				case <-renderCtx.Done():
					return fmt.Errorf(
						"site %s reported Unsealed/Clone but its Deployment never re-rendered onto a writable "+
							"keyring (%s) — CLONE INSTANCE cannot rewrap tablespace keys into a read-only keyring",
						state.replicaSite, last)
				case <-time.After(3 * time.Second):
				}
			}
		},
	}
}

// s51VerifyResealedAfterClone waits out the clone and asserts the
// end state: an escrow version no older than pre-clone, the operator
// consumed the reclone annotation, a read-only keyring off the data
// volume, and the donor's rows present.
func s51VerifyResealedAfterClone(state *s51RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the recipient re-seals without regressing the escrow version, with the cloned data readable",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()

			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s re-sealed after the clone (was v%d) and the operator consumed the annotation",
					state.replicaSite, state.escrowBefore),
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if m.Status.EncryptionAtRest == nil {
						return false, "status.encryptionAtRest not populated", nil
					}
					s := m.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
					if s == nil {
						return false, "no status entry for " + state.replicaSite, nil
					}
					if s.Phase == v1alpha1.KeyringPhaseFailed {
						return false, fmt.Sprintf("phase=Failed (%s)", s.Message),
							fmt.Errorf("recipient %s failed during the encrypted reclone: %s", state.replicaSite, s.Message)
					}
					if s.Phase != v1alpha1.KeyringPhaseSealed {
						return false, fmt.Sprintf("phase=%s reason=%s (%s)", s.Phase, s.UnsealReason, s.Message), nil
					}
					// The operator clears the annotation only after the
					// clone actually finishes. Sealed alone is not proof:
					// the replica starts sealed, so a never-consumed
					// annotation plus rows arriving via replication would
					// otherwise false-pass.
					if _, present := m.GetAnnotations()["bloodraven.shipstream.io/reclone-site"]; present {
						return false, "reclone annotation still present (clone not completed)", nil
					}
					return true, "", nil
				})
			if err != nil {
				return fmt.Errorf("waiting for the recipient to re-seal after the clone: %w", err)
			}

			after := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
			// The escrow version is NOT required to advance. A recipient
			// that was seeded from its own existing escrow already holds a
			// usable master key, and CLONE INSTANCE rewraps the donor's
			// tablespace keys under that same key rather than creating a
			// new one — so the keyring bytes, and therefore the escrow
			// version, legitimately stay put. (Verified against MySQL 9.7:
			// a reclone of a sealed site came back on the same v2.) What
			// must hold is that it never goes BACKWARDS onto a superseded
			// version, and that the operator re-verified the digest before
			// re-sealing, which reaching Sealed already proves.
			if after.KeyringVersion < state.escrowBefore {
				return fmt.Errorf(
					"recipient re-sealed against escrow v%d, older than the pre-clone v%d",
					after.KeyringVersion, state.escrowBefore)
			}
			if after.KeyringSecret == "" || after.KeyringDigest == "" {
				return fmt.Errorf("recipient re-sealed without a recorded escrow: %+v", after)
			}

			// Fresh port-forward: the clone replaced the pod.
			db, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, state.replicaSite, env.Creds)
			if err != nil {
				return fmt.Errorf("open mysql on %s after the clone: %w", state.replicaSite, err)
			}
			defer db.Close()

			readOnly, err := db.ScalarString(ctx,
				"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Read_only'")
			if err != nil {
				return fmt.Errorf("query keyring status on %s: %w", state.replicaSite, err)
			}
			if !strings.EqualFold(readOnly, "Yes") {
				return fmt.Errorf(
					"site %s left the reclone with a writable keyring (Read_only=%q) — the unseal window must close again",
					state.replicaSite, readOnly)
			}
			dataFile, err := db.ScalarString(ctx,
				"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Data_file'")
			if err != nil {
				return fmt.Errorf("query keyring data file on %s: %w", state.replicaSite, err)
			}
			if strings.HasPrefix(dataFile, "/var/lib/mysql") {
				return fmt.Errorf(
					"site %s keyring data file is %q after the clone — the clone put the key on the data PVC",
					state.replicaSite, dataFile)
			}

			count, err := db.ScalarInt(ctx, "SELECT COUNT(*) FROM chaos_encryption.clone_rows")
			if err != nil {
				return fmt.Errorf("reading cloned encrypted rows on %s failed: %w", state.replicaSite, err)
			}
			if count < state.rowCount {
				return fmt.Errorf("recipient %s has %d rows after the clone, donor had %d",
					state.replicaSite, count, state.rowCount)
			}
			encrypted, err := db.ScalarInt(ctx,
				"SELECT COUNT(*) FROM information_schema.INNODB_TABLESPACES WHERE ENCRYPTION='Y'")
			if err != nil {
				return fmt.Errorf("count encrypted tablespaces on %s: %w", state.replicaSite, err)
			}
			if encrypted == 0 {
				return fmt.Errorf("site %s reports zero encrypted tablespaces after the clone", state.replicaSite)
			}

			env.Logger.Info("encrypted reclone completed",
				"site", state.replicaSite,
				"from", state.secretBefore, "to", after.KeyringSecret,
				"rows", count, "encryptedTablespaces", encrypted)
			return nil
		},
	}
}

func s51Cleanup(state *s51RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		if !state.annotated {
			return nil
		}
		// The operator clears the annotation once the reclone is accepted;
		// this only covers the paths that bailed out before that.
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return fmt.Errorf("read MFG to clear leftover reclone annotation: %w", err)
		}
		if _, present := mfg.GetAnnotations()["bloodraven.shipstream.io/reclone-site"]; !present {
			state.annotated = false
			return nil
		}
		if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
			"bloodraven.shipstream.io/reclone-site", ""); err != nil {
			return fmt.Errorf("clear reclone annotation: %w", err)
		}
		state.annotated = false
		return nil
	}
}
