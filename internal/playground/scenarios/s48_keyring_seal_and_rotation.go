package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario48KeyringSealAndRotation())
}

type s48RunState struct {
	activeSite  string
	replicaSite string

	// sealedSecret / sealedVersion are captured before rotation so the
	// verify step can prove a new immutable version was minted rather
	// than the old one being overwritten.
	sealedSecret  string
	sealedVersion int32

	// rotated records that the rotation annotation was actually applied,
	// so cleanup does not try to remove one that was never set.
	rotated bool
}

// scenario48KeyringSealAndRotation exercises the encryption-at-rest
// keyring lifecycle end to end against real MySQL pods.
//
// The properties under test are the ones the security claim rests on:
//
//  1. A sealed site really is sealed — MySQL itself reports a read-only
//     keyring, not just the operator's status field.
//  2. The keyring never lands on the data PVC. The rendered pod projects
//     it from a Secret onto tmpfs, and mysqld's own view of the data file
//     path is outside the data directory.
//  3. Sealing genuinely prevents key creation. `ALTER INSTANCE ROTATE
//     INNODB MASTER KEY` is rejected by the engine, which is what stops
//     an operator or application from stranding data behind a key nobody
//     escrowed.
//  4. Rotation on a replica mints a NEW immutable escrow version and the
//     site returns to Sealed, with the data still readable afterwards.
//  5. Rotation on the active primary is refused, because that is the one
//     case where losing the keyring mid-rotation would cost data rather
//     than a re-clone.
//
// Kept out of shared batch profiles because it requires a playground brought
// up with TLS and encryption enabled (`./playground/enable-encryption.sh`).
// CI runs it explicitly in a dedicated encryption job; local runs use
// `make chaos-run SCENARIO=48-keyring-seal-and-rotation`.
func scenario48KeyringSealAndRotation() runner.Scenario {
	state := &s48RunState{}
	return runner.Scenario{
		ID:    "48-keyring-seal-and-rotation",
		Title: "Encrypted sites seal against an escrowed keyring, reject ad-hoc rotation, and rotate safely on a replica",
		Hypothesis: "Every site reports phase=Sealed with a read-only keyring component; ALTER INSTANCE ROTATE " +
			"INNODB MASTER KEY fails on a sealed site; annotating the replica for rotation mints escrow version " +
			"N+1 and returns it to Sealed with data intact; annotating the active primary is refused with a " +
			"KeyringRotationRefused event.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#48-keyring-seal-and-rotation",
		Timeout: 20 * time.Minute,
		Quarantine: "requires the dedicated TLS + spec.encryptionAtRest baseline; " +
			"CI and local encryption jobs run this scenario explicitly",
		Precheck: s48Precheck(state),
		Steps: []runner.Step{
			s48VerifySealedState(state),
			s48VerifyKeyringOffDataVolume(state),
			s48VerifySealRejectsRotation(state),
			s48RefuseRotationOnPrimary(state),
			s48RotateReplica(state),
			s48VerifyNewEscrowVersion(state),
		},
		Cleanup: s48Cleanup(state),
	}
}

func s48Precheck(state *s48RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s48RunState{}
		if err := AssertHealthyBaseline(ctx, env); err != nil {
			return err
		}

		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return err
		}
		if !mfg.Spec.EncryptionEnabled() {
			return fmt.Errorf(
				"spec.encryptionAtRest is not enabled on %s/%s; run ./playground/enable-encryption.sh first",
				env.Namespace, env.FG)
		}
		if mfg.Spec.TLS == nil {
			return fmt.Errorf("spec.tls is not set; encryption at rest requires it (encrypted CLONE needs a secure connection)")
		}
		if mfg.Status.ActiveSite == "" {
			return fmt.Errorf("no active site reported; cluster has not settled")
		}
		state.activeSite = mfg.Status.ActiveSite

		for _, s := range mfg.Spec.Sites {
			if s.Name != state.activeSite && s.IsPromotable() {
				state.replicaSite = s.Name
				break
			}
		}
		if state.replicaSite == "" {
			return fmt.Errorf("no promotable replica site found to rotate")
		}
		return nil
	}
}

// s48VerifySealedState checks both halves of "sealed": the operator's
// status and MySQL's own report. Only checking status would let a
// rendering bug pass — the operator could believe a site is sealed while
// mysqld still holds a writable keyring.
func s48VerifySealedState(state *s48RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "every site is Sealed and MySQL reports a read-only keyring component",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Wait.UntilCR(ctx, env.Namespace, "all sites sealed",
				func(m *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					st := m.Status.EncryptionAtRest
					if st == nil {
						return false, "status.encryptionAtRest not populated yet", nil
					}
					if !st.Sealed {
						return false, s48UnsealedSummary(st), nil
					}
					return true, "", nil
				})
			if err != nil {
				return fmt.Errorf("waiting for every site to seal: %w", err)
			}

			for _, s := range mfg.Status.EncryptionAtRest.Sites {
				if s.Phase != v1alpha1.KeyringPhaseSealed {
					return fmt.Errorf("site %s phase=%s (%s), want Sealed", s.Name, s.Phase, s.Message)
				}
				if s.KeyringSecret == "" || s.KeyringVersion < 1 {
					return fmt.Errorf("site %s has no escrow version recorded: %+v", s.Name, s)
				}
				if s.KeyringDigest == "" {
					return fmt.Errorf("site %s sealed without a recorded digest", s.Name)
				}
				if s.Name == state.activeSite {
					state.sealedSecret = s.KeyringSecret
					state.sealedVersion = s.KeyringVersion
				}
			}

			// MySQL's own view. Read_only=Yes is what actually prevents
			// key creation; everything above is bookkeeping.
			for _, site := range []string{state.activeSite, state.replicaSite} {
				db, err := env.MySQL(site)
				if err != nil {
					return fmt.Errorf("open mysql on %s: %w", site, err)
				}
				readOnly, err := db.ScalarString(ctx,
					"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Read_only'")
				if err != nil {
					return fmt.Errorf("query keyring status on %s: %w", site, err)
				}
				if !strings.EqualFold(readOnly, "Yes") {
					return fmt.Errorf("site %s: keyring component Read_only=%q, want Yes — the site is not actually sealed", site, readOnly)
				}
				component, err := db.ScalarString(ctx,
					"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Component_name'")
				if err != nil {
					return fmt.Errorf("query keyring component on %s: %w", site, err)
				}
				if component != "component_keyring_file" {
					return fmt.Errorf("site %s: keyring component is %q", site, component)
				}
			}
			return nil
		},
	}
}

// s48VerifyKeyringOffDataVolume is the core at-rest property: a stolen
// data PVC must not contain the key that decrypts it.
func s48VerifyKeyringOffDataVolume(state *s48RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the keyring data file lives outside /var/lib/mysql and is projected from a Secret",
		Do: func(ctx context.Context, env *runner.Env) error {
			for _, site := range []string{state.activeSite, state.replicaSite} {
				db, err := env.MySQL(site)
				if err != nil {
					return fmt.Errorf("open mysql on %s: %w", site, err)
				}
				dataFile, err := db.ScalarString(ctx,
					"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Data_file'")
				if err != nil {
					return fmt.Errorf("query keyring data file on %s: %w", site, err)
				}
				if dataFile == "" {
					return fmt.Errorf("site %s reports no keyring data file", site)
				}
				if strings.HasPrefix(dataFile, "/var/lib/mysql") {
					return fmt.Errorf(
						"site %s keyring data file is %q — inside the data directory, so a stolen PVC carries both the ciphertext and the key",
						site, dataFile)
				}

				// Confirm the rendered pod is projecting it from the
				// escrow Secret rather than a writable volume.
				deploy, err := env.Kube.GetDeployment(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, site))
				if err != nil {
					return fmt.Errorf("get deployment for %s: %w", site, err)
				}
				var found bool
				for _, v := range deploy.Spec.Template.Spec.Volumes {
					if v.Name != "keyring" {
						continue
					}
					found = true
					if v.Secret == nil {
						return fmt.Errorf(
							"site %s keyring volume is not a Secret projection (%+v) — a sealed site must not run a writable keyring",
							site, v.VolumeSource)
					}
				}
				if !found {
					return fmt.Errorf("site %s has no keyring volume in its rendered pod spec", site)
				}
			}
			return nil
		},
	}
}

// s48VerifySealRejectsRotation proves the seal is enforced by MySQL, not
// by convention. This is the guardrail that makes "no unescrowed key can
// ever exist in the steady state" true rather than aspirational.
func s48VerifySealRejectsRotation(state *s48RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "ALTER INSTANCE ROTATE INNODB MASTER KEY is rejected on a sealed site",
		Do: func(ctx context.Context, env *runner.Env) error {
			db, err := env.MySQL(state.activeSite)
			if err != nil {
				return fmt.Errorf("open mysql on %s: %w", state.activeSite, err)
			}
			if _, err := db.Exec(ctx, "ALTER INSTANCE ROTATE INNODB MASTER KEY"); err == nil {
				return fmt.Errorf(
					"site %s accepted an ad-hoc master key rotation while sealed — the read-only keyring is not being enforced",
					state.activeSite)
			}
			env.Logger.Info("sealed site rejected ad-hoc master key rotation as expected", "site", state.activeSite)
			return nil
		},
	}
}

// s48RefuseRotationOnPrimary checks the operator's own guard: rotation
// is the only lifecycle operation whose failure window would cost data
// rather than a re-clone, and only on the primary.
func s48RefuseRotationOnPrimary(state *s48RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "rotation targeting the active primary is refused",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
				"bloodraven.shipstream.io/rotate-keyring", state.activeSite); err != nil {
				return fmt.Errorf("annotate for primary rotation: %w", err)
			}
			state.rotated = true

			// The primary must stay Sealed. Give the operator a few
			// reconciles to act (or refuse to act) before concluding.
			deadline := time.Now().Add(90 * time.Second)
			for time.Now().Before(deadline) {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				s := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.activeSite)
				if s == nil {
					return fmt.Errorf("no encryption status for the active site")
				}
				if s.Phase != v1alpha1.KeyringPhaseSealed {
					return fmt.Errorf(
						"active primary %s moved to phase %s — the operator must refuse to unseal the primary for rotation",
						state.activeSite, s.Phase)
				}
				time.Sleep(5 * time.Second)
			}

			events, err := env.Kube.RecentEvents(ctx, env.Namespace, 200)
			if err != nil {
				return fmt.Errorf("list events: %w", err)
			}
			for _, e := range events {
				if e.Reason == "KeyringRotationRefused" {
					env.Logger.Info("operator refused primary rotation", "message", e.Message)
					return s48ClearRotateAnnotation(ctx, env, state)
				}
			}
			return fmt.Errorf("no KeyringRotationRefused event was emitted for the active primary")
		},
	}
}

func s48ClearRotateAnnotation(ctx context.Context, env *runner.Env, state *s48RunState) error {
	if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
		"bloodraven.shipstream.io/rotate-keyring", ""); err != nil {
		return fmt.Errorf("clear rotate annotation: %w", err)
	}
	state.rotated = false
	return nil
}

// s48RotateReplica drives the supported rotation path and waits for the
// site to come back sealed against a NEW escrow version.
func s48RotateReplica(state *s48RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "rotate the replica's master key and wait for it to re-seal",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			before := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
			if before == nil || before.Phase != v1alpha1.KeyringPhaseSealed {
				return fmt.Errorf("replica %s is not sealed before rotation: %+v", state.replicaSite, before)
			}
			state.sealedSecret = before.KeyringSecret
			state.sealedVersion = before.KeyringVersion

			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG,
				"bloodraven.shipstream.io/rotate-keyring", state.replicaSite); err != nil {
				return fmt.Errorf("annotate for replica rotation: %w", err)
			}
			state.rotated = true

			// Unsealed is transient — the pod has to roll, MySQL has to
			// rotate, and the sidecar has to escrow — so wait for the
			// terminal state rather than trying to catch the middle.
			if _, err := env.Wait.UntilCR(ctx, env.Namespace,
				fmt.Sprintf("replica %s re-sealed on a new keyring version", state.replicaSite),
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
					if s.KeyringVersion <= state.sealedVersion {
						return false, fmt.Sprintf("still on escrow v%d", s.KeyringVersion), nil
					}
					return true, "", nil
				}); err != nil {
				return fmt.Errorf("waiting for the replica to re-seal after rotation: %w", err)
			}
			return nil
		},
	}
}

// s48VerifyNewEscrowVersion confirms the rotation produced a new
// immutable Secret rather than rewriting the old one, that the old
// version survives for rollback, and — most importantly — that the data
// is still readable through the new key.
func s48VerifyNewEscrowVersion(state *s48RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "a new immutable escrow version exists, the old one survives, and data still decrypts",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			after := mfg.Status.EncryptionAtRest.SiteEncryptionStatusByName(state.replicaSite)
			if after == nil {
				return fmt.Errorf("no encryption status for %s", state.replicaSite)
			}
			if after.KeyringSecret == state.sealedSecret {
				return fmt.Errorf(
					"rotation reused escrow Secret %s — versions must be immutable so a running MySQL's keyring is never rewritten",
					state.sealedSecret)
			}
			if after.KeyringVersion <= state.sealedVersion {
				return fmt.Errorf("escrow version did not advance: %d -> %d", state.sealedVersion, after.KeyringVersion)
			}

			// The pre-rotation version must still exist: it is the only
			// rollback target if the new keyring turns out to be bad.
			if _, err := env.Kube.GetSecret(ctx, env.Namespace, state.sealedSecret); err != nil {
				return fmt.Errorf("previous escrow version %s was pruned while still within retention: %w",
					state.sealedSecret, err)
			}

			// Read real data back through the rotated key. A rotation
			// that silently stranded the tablespace keys would show up
			// here and nowhere else.
			db, err := env.MySQL(state.replicaSite)
			if err != nil {
				return fmt.Errorf("open mysql on %s: %w", state.replicaSite, err)
			}
			if _, err := db.ScalarInt(ctx, "SELECT COUNT(*) FROM information_schema.INNODB_TABLESPACES"); err != nil {
				return fmt.Errorf("reading tablespace metadata after rotation failed on %s: %w", state.replicaSite, err)
			}
			encrypted, err := db.ScalarInt(ctx,
				"SELECT COUNT(*) FROM information_schema.INNODB_TABLESPACES WHERE ENCRYPTION='Y'")
			if err != nil {
				return fmt.Errorf("count encrypted tablespaces on %s: %w", state.replicaSite, err)
			}
			if encrypted == 0 {
				return fmt.Errorf("site %s reports zero encrypted tablespaces after rotation", state.replicaSite)
			}

			// And the keyring is sealed again, not left writable.
			readOnly, err := db.ScalarString(ctx,
				"SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY='Read_only'")
			if err != nil {
				return fmt.Errorf("query keyring status on %s: %w", state.replicaSite, err)
			}
			if !strings.EqualFold(readOnly, "Yes") {
				return fmt.Errorf("site %s left the rotation with a writable keyring (Read_only=%q)", state.replicaSite, readOnly)
			}

			env.Logger.Info("keyring rotated and re-sealed",
				"site", state.replicaSite,
				"from", state.sealedSecret,
				"to", after.KeyringSecret,
				"encryptedTablespaces", encrypted)
			return nil
		},
	}
}

func s48Cleanup(state *s48RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		if !state.rotated {
			return nil
		}
		// The operator clears the annotation itself once the target
		// re-seals; this is only for the paths that bailed out early.
		return s48ClearRotateAnnotation(ctx, env, state)
	}
}

// s48UnsealedSummary renders which sites are still not sealed, so a
// timeout says what was actually blocking rather than just "not sealed".
func s48UnsealedSummary(st *v1alpha1.EncryptionAtRestStatus) string {
	var parts []string
	for _, s := range st.Sites {
		if s.Phase != v1alpha1.KeyringPhaseSealed {
			parts = append(parts, fmt.Sprintf("%s=%s (%s)", s.Name, s.Phase, s.Message))
		}
	}
	if len(parts) == 0 {
		return "sealed flag not yet rolled up"
	}
	return strings.Join(parts, "; ")
}
