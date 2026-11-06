package scenarios

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario69PITREncryptionBindingRefusal())
}

const (
	s69ID     = "69-pitr-encryption-binding-refusal"
	s69DBName = "chaos_s69_pitr"
	// Profiles beside A (backupE2EProfile). The alias carries A's identity
	// while A's encryption is put back (step 8); B is a same-storage alias
	// with other bytes (step 9); D reads P1 through another endpoint
	// spelling (step 10); C is the procedure's new prefix (step 12).
	s69ProfileAlias = "s69-alias"
	s69ProfileB     = "s69-b"
	s69ProfileC     = "s69-c"
	s69ProfileD     = "s69-d"
	// s69ReadEndpoint reaches the same RustFS Service as backupE2EEndpoint
	// (the pod's DNS search path completes it) under another host
	// spelling, which NormalizeEndpoint keeps distinct: a read destination
	// over P1's objects that no binding covers yet.
	s69ReadEndpoint = "http://rustfs.bloodraven-playground.svc:9000"
	// s69CELMessage is part of the root rule's message (D-25).
	s69CELMessage = "cannot change while that profile archives PITR binlogs to the same storage"
	// s69NoRollWindow is the "two operator polls" a refused rotation is
	// watched for; the shorter window serves the other no-roll checks (a
	// roll starts within seconds of the reconcile that decides it).
	s69NoRollWindow      = 2 * pitrOperatorArchiverPollInterval
	s69ShortNoRollWindow = 30 * time.Second
	s69BindingWait       = 5 * time.Minute
	// s69Timeout is unmeasured: 4 rolls, 3 pod restarts, 2 backups, 3
	// verifications, 3 check Jobs, an operator restart and about 4
	// minutes of no-roll windows add up to roughly 35-40 minutes on the CI
	// runner (s31's backup and verification took 3 minutes on #206).
	s69Timeout     = 60 * time.Minute
	s69RolloutWait = 12 * time.Minute
	// s69VerificationHoldPrefix mirrors the operator's
	// annotationVerificationHoldPrefix (the key is it plus the
	// verification's UID, the value its name).
	s69VerificationHoldPrefix = "hold.verification.shipstream.io/"
	// s69SecretKind is the pitrTrackCR kind of the extra passphrase
	// Secrets this run creates ("<name>/<uid>").
	s69SecretKind = "s69Secret"
	// Marker ids, in history order. M2b is the witness after M2's fence:
	// a timestamp target is covered only once a transaction at or after it
	// is archived.
	s69IDBaseline = 1
	s69IDM1       = 2
	s69IDM2       = 3
	s69IDM2b      = 4
	s69IDM3       = 5
	s69IDM4       = 6
	// The baseline note of a Bound write binding with no stamped backup
	// (IMPL-203 §4.5).
	s69BaselineNote = "no full backup is stamped with archive"
)

// scenario69PITREncryptionBindingRefusal is IMPL-203 §7.4 (#203): the
// encryption identity of a PITR archive is bound to an operator-made
// immutable key snapshot, so a rotated or changed passphrase is refused
// without touching what pods mount, even across pod restarts, an operator
// restart and a status wipe; and the documented procedure (new prefix, new
// passphrase, full backup, release) works end to end.
func scenario69PITREncryptionBindingRefusal() runner.Scenario {
	return runner.Scenario{
		ID:    s69ID,
		Title: "PITR archive encryption binding: refused key and encryption changes, restarts, status loss and release",
		Hypothesis: "An encrypted PITR archive binds to an immutable, ownerless key snapshot the pods mount (optional) instead of " +
			"the user's Secret. Admission refuses encryption edits on the archiving profile; an in-place rotation, a real " +
			"encryption change made while PITR was off, and a same-storage alias with other bytes are refused on " +
			"PITRArchiveBindingReady without a roll, and writers restarted afterwards keep archiving with the accepted key; an " +
			"old dump still verifies after the edit; a read binding with the wrong key waits on a check Job and binds once the " +
			"key is fixed; bindings survive an operator restart with a status wipe without a check Job or a roll; the new-prefix " +
			"procedure binds through a check Job, the old archive stays readable with its user Secret deleted, and a release is " +
			"rejected, refused while in use, then completes.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#69-pitr-encryption-binding-refusal",
		Timeout:           s69Timeout,
		CleanupTimeout:    pitrCleanupBudget(4 * time.Minute),
		ResetBeforeRunAll: true,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s69Configure(),
			s69AssertBound(),
			s69BackupAndM1(),
			s69AdmissionRefusals(),
			s69RotateInPlace(),
			s69RestartedWriters(),
			s69VerifyOldDump(),
			s69WhitespaceEdit(),
			s69RefusedEncryptionEdit(),
			s69SameStorageAlias(),
			s69ReadBindingWrongKey(),
			s69StatusWipe(),
			s69Procedure(),
			s69Release(),
		},
		Cleanup: s69Cleanup,
	}
}

func s69Configure() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure encrypted profile A on prefix P1",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := pitrConfigureArchive(ctx, env, s69ID, "s69", "p1", true); err != nil {
				return err
			}
			return ctxStash(ctx, env, "prefixP1", ctxFetch(env, "backupPrefix"))
		},
	}
}

// Step 1.
func s69AssertBound() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "P1 binding Bound: pods mount the immutable, ownerless key snapshot, not the user Secret",
		Do: func(ctx context.Context, env *runner.Env) error {
			userSecret := ctxFetch(env, backupPassphraseSecretKey)
			p1 := ctxFetch(env, "prefixP1")
			mfg, entry, err := s69WaitBinding(ctx, env, "P1 binding Bound and PITRArchiveBindingReady=True", p1, backupE2EEndpoint,
				func(mfg *v1alpha1.MysqlFailoverGroup, b *v1alpha1.PITRArchiveBinding) (bool, string) {
					ok := s69BoundBound(b) && b.Encryption.Mode == v1alpha1.PITRArchiveEncryptionPassphrase && b.Encryption.KeySecretName != "" &&
						s69BindingConditionIs(mfg, metav1.ConditionTrue, "")
					return ok, s69BindingSummary(mfg, b)
				})
			if err != nil {
				return err
			}
			key := entry.Encryption.KeySecretName
			if key == userSecret {
				return fmt.Errorf("binding %s names the user Secret %s; it must name an operator-made snapshot", entry.ID, key)
			}
			if entry.ArchiveID != ctxFetch(env, "archiveID") {
				return fmt.Errorf("binding %s archiveID=%q, want status.pitr.archive's %q", entry.ID, entry.ArchiveID, ctxFetch(env, "archiveID"))
			}
			if err := s69AssertArchiveKeySnapshot(ctx, env, mfg, entry, s69RunPassphrase(env)); err != nil {
				return err
			}
			if err := s69AssertRenders(ctx, env, mfg, key, userSecret); err != nil {
				return err
			}
			// status.pitr.archive echoes the revision every counted sidecar
			// reported (Δ2); it follows the archiver poll.
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			if _, err := env.Wait.UntilCR(waitCtx, env.Namespace, "status.pitr.archive.keyRevision is the snapshot", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				ok := mfg.Status.PITR != nil && mfg.Status.PITR.Archive != nil && mfg.Status.PITR.Archive.KeyRevision == key
				return ok, pitrArchiveSummary(mfg), nil
			}); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "bindingP1", entry.ID); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("P1 binding %s Bound with snapshot %s (archive %s)", entry.ID, key, entry.ArchiveID))
			return ctxStash(ctx, env, "keySecretP1", key)
		},
	}
}

// Step 2.
func s69BackupAndM1() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "backup β1 records its own ownerless dump-key snapshot; marker M1, flush",
		Do: func(ctx context.Context, env *runner.Env) error {
			a, b, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := pitrCreateMarkerTable(ctx, env, a, s69DBName); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s69DBName, s69IDBaseline, "baseline"); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return err
			}
			if err := pitrBackup(ctx, env, s69ID, "b1"); err != nil {
				return err
			}
			bk, err := getMysqlBackup(ctx, env, ctxFetch(env, "backup:b1"))
			if err != nil {
				return err
			}
			if err := s69AssertDumpKeySnapshot(ctx, env, bk, s69RunPassphrase(env)); err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s69DBName, s69IDM1, "M1"); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return err
			}
			return pitrFlush(ctx, env, a, true)
		},
	}
}

// Step 3.
func s69AdmissionRefusals() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "admission (dry run) refuses removing encryption, a same-bytes Secret rename and a key change on the archiving profile",
		Do: func(ctx context.Context, env *runner.Env) error {
			copyName, err := s69CreateSecret(ctx, env, "copy", s69RunPassphrase(env))
			if err != nil {
				return err
			}
			edits := []struct {
				what string
				fn   func(*v1alpha1.BackupProfile)
			}{
				{"remove encryption", func(p *v1alpha1.BackupProfile) { p.Encryption = nil }},
				{"rename passphraseSecret to a same-bytes copy", func(p *v1alpha1.BackupProfile) { p.Encryption.PassphraseSecret.Name = copyName }},
				{"change passphraseSecret.key", func(p *v1alpha1.BackupProfile) { p.Encryption.PassphraseSecret.Key = "passphrase-copy" }},
			}
			for _, e := range edits {
				err := s69EditBackupDryRun(ctx, env, func(b *v1alpha1.BackupSpec) error {
					p := s69Profile(b, backupE2EProfile)
					if p == nil || p.Encryption == nil {
						return fmt.Errorf("profile %s has no encryption to edit", backupE2EProfile)
					}
					e.fn(p)
					return nil
				})
				if err == nil {
					return fmt.Errorf("admission accepted %q on the archiving profile", e.what)
				}
				if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), s69CELMessage) {
					return fmt.Errorf("%q: want the root rule's refusal (%q), got %w", e.what, s69CELMessage, err)
				}
				env.Capture.Note(fmt.Sprintf("admission refused %q: %v", e.what, err))
			}
			return nil
		},
	}
}

// Step 4.
func s69RotateInPlace() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "rotate the user Secret in place: KeyChangeRefused, no roll for two operator polls",
		Do: func(ctx context.Context, env *runner.Env) error {
			before, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			if err := s69StashSnapshot(ctx, env, "rotation", before); err != nil {
				return err
			}
			start := time.Now()
			if err := s69SetSecretPassphrase(ctx, env, ctxFetch(env, backupPassphraseSecretKey), s69RandomPassphrase()); err != nil {
				return err
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionFalse, "KeyChangeRefused", nil); err != nil {
				return err
			}
			if _, err := s69WaitEvent(ctx, env, start, "PITRArchiveKeyChangeRefused", ""); err != nil {
				return err
			}
			if err := s69AssertP1Unchanged(ctx, env); err != nil {
				return err
			}
			return s69AssertNoRoll(ctx, env, before, s69NoRollWindow)
		},
	}
}

// Step 5.
func s69RestartedWriters() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "restart the replica's then the primary's pod; M2 is archived by a restarted pod with the accepted key; witness M2b, flush",
		Do: func(ctx context.Context, env *runner.Env) error {
			rotation, err := s69FetchSnapshot(env, "rotation")
			if err != nil {
				return err
			}
			_, b, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := s69RestartSitePod(ctx, env, b, ctxFetch(env, "prefixP1")); err != nil {
				return err
			}
			a, b, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := s69QuiesceForRestart(ctx, env, a, b); err != nil {
				return err
			}
			if err := s69RestartSitePod(ctx, env, a, ctxFetch(env, "prefixP1")); err != nil {
				return err
			}
			// Both pods started after the rotation.
			now, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			for _, site := range []string{a, b} {
				if now.Pods[site] == rotation.Pods[site] {
					return fmt.Errorf("site %s still runs pod %s from before the rotation", site, now.Pods[site])
				}
			}
			if err := s69MarkerArchivedByNewPod(ctx, env, "m2", s69IDM2, "M2", rotation); err != nil {
				return err
			}
			// The fence's flush archived exactly the history before T; the
			// verifications at T (steps 6, 10 and 13) need a transaction at
			// or after T in the archive to prove T covered.
			return s69ArchiveWitness(ctx, env, s69IDM2b, "M2b")
		},
	}
}

// s69ArchiveWitness commits marker phase (id) on the current primary after
// a fence, waits until the replica applied it, and seals and archives it.
// It is after the fence's T, so no expectation at T includes it.
func s69ArchiveWitness(ctx context.Context, env *runner.Env, id int, phase string) error {
	a, b, err := activeAndReplica(ctx, env)
	if err != nil {
		return err
	}
	if err := pitrInsertMarker(ctx, env, a, s69DBName, id, phase); err != nil {
		return err
	}
	if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
		return err
	}
	return pitrFlush(ctx, env, a, true)
}

// Step 6.
func s69VerifyOldDump() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "verify β1 to a target after M2: the old dump decrypts with its own snapshot after the Secret edit",
		Do: func(ctx context.Context, env *runner.Env) error {
			e, err := s69ExpectationAtM2(env, "")
			if err != nil {
				return err
			}
			return pitrVerify(ctx, env, s69ID, s69DBName, "old-dump", e)
		},
	}
}

// Step 7.
func s69WhitespaceEdit() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "a whitespace-only edit (original bytes plus a newline) is accepted without a roll",
		Do: func(ctx context.Context, env *runner.Env) error {
			before, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			if err := s69SetSecretPassphrase(ctx, env, ctxFetch(env, backupPassphraseSecretKey), append(append([]byte{}, s69RunPassphrase(env)...), '\n')); err != nil {
				return err
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionTrue, "", nil); err != nil {
				return err
			}
			return s69AssertNoRoll(ctx, env, before, s69ShortNoRollWindow)
		},
	}
}

// Step 8 (K11).
func s69RefusedEncryptionEdit() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "a real encryption removal made while PITR was off is refused; restarted writers keep the binding's key; restored without a roll",
		Do: func(ctx context.Context, env *runner.Env) error {
			p1 := ctxFetch(env, "prefixP1")
			key := ctxFetch(env, "keySecretP1")
			stepStart, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			// Disabling PITR rolls every site; leave nothing unarchived.
			if a, b, err := activeAndReplica(ctx, env); err != nil {
				return err
			} else if err := s69QuiesceForRestart(ctx, env, a, b); err != nil {
				return err
			}
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				b.PITR.Enabled = false
				return nil
			}); err != nil {
				return err
			}
			if err := s69WaitRolled(ctx, env, stepStart, ""); err != nil {
				return fmt.Errorf("disabling PITR: %w", err)
			}
			// The rule admits it: PITR is off in the old object.
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				p := s69Profile(b, backupE2EProfile)
				if p == nil {
					return fmt.Errorf("profile %s missing", backupE2EProfile)
				}
				p.Encryption = nil
				return nil
			}); err != nil {
				return fmt.Errorf("remove A's encryption with PITR off: %w", err)
			}
			disabled, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			enabledAt := time.Now()
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				b.PITR.Enabled = true
				return nil
			}); err != nil {
				return err
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionFalse, "EncryptionChangeRefused", nil); err != nil {
				return err
			}
			if _, err := s69WaitEvent(ctx, env, enabledAt, "PITRArchiveEncryptionChangeRefused", ""); err != nil {
				return err
			}
			if err := s69WaitRolled(ctx, env, disabled, p1); err != nil {
				return fmt.Errorf("re-enabling PITR: %w", err)
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			// The render is the binding's encrypted identity, not the
			// profile's plaintext one.
			if err := s69AssertRenders(ctx, env, mfg, key, ctxFetch(env, backupPassphraseSecretKey)); err != nil {
				return err
			}
			if err := s69AssertP1Unchanged(ctx, env); err != nil {
				return err
			}
			a, b, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := s69QuiesceForRestart(ctx, env, a, b); err != nil {
				return err
			}
			if err := s69RestartSitePod(ctx, env, a, p1); err != nil {
				return err
			}
			if err := s69MarkerArchivedByNewPod(ctx, env, "m3", s69IDM3, "M3", stepStart); err != nil {
				return err
			}
			// Put A's encryption back. The rule refuses adding it while A
			// archives with PITR on, so the identity moves through an alias
			// (a profileName change is admitted and renders identically:
			// the profile name is not rendered, K7), A is edited while it is
			// not the PITR profile, and the alias goes again. None of this
			// rolls.
			restored, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			userSecret := ctxFetch(env, backupPassphraseSecretKey)
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				a := s69Profile(b, backupE2EProfile)
				if a == nil {
					return fmt.Errorf("profile %s missing", backupE2EProfile)
				}
				alias := *a.DeepCopy()
				alias.Name = s69ProfileAlias
				alias.Encryption = backupE2EEncryption(userSecret)
				b.Profiles = append(b.Profiles, alias)
				b.PITR.ProfileName = s69ProfileAlias
				return nil
			}); err != nil {
				return fmt.Errorf("switch to the alias of A with A's encryption: %w", err)
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionTrue, "", nil); err != nil {
				return err
			}
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				s69Profile(b, backupE2EProfile).Encryption = backupE2EEncryption(userSecret)
				return nil
			}); err != nil {
				return fmt.Errorf("restore A's encryption while the alias archives: %w", err)
			}
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				b.PITR.ProfileName = backupE2EProfile
				b.Profiles = s69WithoutProfile(b.Profiles, s69ProfileAlias)
				return nil
			}); err != nil {
				return fmt.Errorf("switch back to A: %w", err)
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionTrue, "", nil); err != nil {
				return err
			}
			return s69AssertNoRoll(ctx, env, restored, s69ShortNoRollWindow)
		},
	}
}

// Step 9.
func s69SameStorageAlias() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "a same-storage alias with other bytes is refused naming both profiles, without a roll; M4 is archived",
		Do: func(ctx context.Context, env *runner.Env) error {
			secretB, err := s69CreateSecret(ctx, env, "b", s69RandomPassphrase())
			if err != nil {
				return err
			}
			before, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			start := time.Now()
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				a := s69Profile(b, backupE2EProfile)
				if a == nil {
					return fmt.Errorf("profile %s missing", backupE2EProfile)
				}
				alias := *a.DeepCopy()
				alias.Name = s69ProfileB
				alias.Encryption = backupE2EEncryption(secretB)
				b.Profiles = append(b.Profiles, alias)
				b.PITR.ProfileName = s69ProfileB
				return nil
			}); err != nil {
				return err
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionFalse, "KeyChangeRefused", []string{backupE2EProfile, s69ProfileB}); err != nil {
				return err
			}
			if _, err := s69WaitEvent(ctx, env, start, "PITRArchiveKeyChangeRefused", ""); err != nil {
				return err
			}
			if err := s69AssertNoRoll(ctx, env, before, s69ShortNoRollWindow); err != nil {
				return err
			}
			if err := s69AssertP1Unchanged(ctx, env); err != nil {
				return err
			}
			a, b, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			if err := pitrInsertMarker(ctx, env, a, s69DBName, s69IDM4, "M4"); err != nil {
				return err
			}
			if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
				return err
			}
			// The flush waits until the sealed file is in a's manifest,
			// which only the stashed original bytes decrypt.
			if err := pitrFlush(ctx, env, a, true); err != nil {
				return err
			}
			if err := s69AssertArchiverKeyRevision(ctx, env, a, ctxFetch(env, "keySecretP1")); err != nil {
				return err
			}
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				b.PITR.ProfileName = backupE2EProfile
				b.Profiles = s69WithoutProfile(b.Profiles, s69ProfileB)
				return nil
			}); err != nil {
				return err
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionTrue, "", nil); err != nil {
				return err
			}
			return s69AssertNoRoll(ctx, env, before, s69ShortNoRollWindow)
		},
	}
}

// Step 10.
func s69ReadBindingWrongKey() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "a read binding with the wrong key (C1) is refused by a check Job and the verification waits; the right key binds it",
		Do: func(ctx context.Context, env *runner.Env) error {
			p1 := ctxFetch(env, "prefixP1")
			secretD, err := s69CreateSecret(ctx, env, "d", s69RandomPassphrase())
			if err != nil {
				return err
			}
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				a := s69Profile(b, backupE2EProfile)
				if a == nil {
					return fmt.Errorf("profile %s missing", backupE2EProfile)
				}
				d := *a.DeepCopy()
				d.Name = s69ProfileD
				d.Storage.S3.EndpointURL = s69ReadEndpoint
				d.Encryption = backupE2EEncryption(secretD)
				b.Profiles = append(b.Profiles, d)
				return nil
			}); err != nil {
				return err
			}
			e, err := s69ExpectationAtM2(env, s69ProfileD)
			if err != nil {
				return err
			}
			start := time.Now()
			name, err := pitrStartVerify(ctx, env, s69ID, s69DBName, "read-d", e)
			if err != nil {
				return err
			}
			refused, err := s69WatchReadBinding(ctx, env, p1, s69ReadEndpoint, start, name, func(b *v1alpha1.PITRArchiveBinding) bool {
				return b.State == v1alpha1.PITRArchiveBindingRefused && b.Reason == "KeyMismatch"
			}, true)
			if err != nil {
				return err
			}
			if _, err := s69WaitEvent(ctx, env, start, "PITRArchiveBindingRefused", ""); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("read binding %s refused: %s", refused.ID, refused.Message))
			fixed := time.Now()
			if err := s69SetSecretPassphrase(ctx, env, secretD, s69RunPassphrase(env)); err != nil {
				return err
			}
			bound, err := s69WatchReadBinding(ctx, env, p1, s69ReadEndpoint, fixed, "", s69BoundBound, false)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "bindingD", bound.ID); err != nil {
				return err
			}
			return pitrFinishVerify(ctx, env, name, e)
		},
	}
}

// Step 11.
func s69StatusWipe() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "operator down, status.pitrArchiveBindings wiped, operator up: bindings rebuilt from acceptance annotations, no check Job, no roll",
		Do: func(ctx context.Context, env *runner.Env) error {
			before, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			want := map[string]string{}
			for _, k := range []string{"bindingP1", "bindingD"} {
				id := ctxFetch(env, k)
				b := pitrFindBindingByID(mfg, id)
				if b == nil || !s69BoundBound(b) {
					return fmt.Errorf("binding %s (%s) is not Bound before the wipe: %s", id, k, s69BindingSummary(mfg, b))
				}
				want[id] = b.Encryption.KeySecretName
			}
			if err := env.Chaos.ScaleOperatorToZero(ctx); err != nil {
				return err
			}
			if err := s69WaitOperatorGone(ctx, env); err != nil {
				return err
			}
			wipedAt := time.Now()
			if err := env.Kube.PatchMFGStatusNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{Op: "remove", Path: "/status/pitrArchiveBindings"}}); err != nil {
				return fmt.Errorf("wipe status.pitrArchiveBindings: %w", err)
			}
			if mfg, err = env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG); err != nil {
				return err
			} else if len(mfg.Status.PITRArchiveBindings) != 0 {
				return fmt.Errorf("status.pitrArchiveBindings survived the wipe: %d entries", len(mfg.Status.PITRArchiveBindings))
			}
			if err := env.Kube.ScaleDeployment(ctx, env.Namespace, "bloodraven", 1); err != nil {
				return err
			}
			upCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			err = env.Chaos.WaitForOperatorAvailable(upCtx)
			cancel()
			if err != nil {
				return err
			}
			if env.RefreshMetrics != nil {
				if err := env.RefreshMetrics(ctx); err != nil {
					env.Capture.Note("refresh operator metrics after restart: " + err.Error())
				}
			}
			if err := s69WaitRebuilt(ctx, env, want, wipedAt); err != nil {
				return err
			}
			if err := s69WaitRecoveryLog(ctx, env, want, wipedAt); err != nil {
				return err
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionTrue, "", nil); err != nil {
				return err
			}
			return s69AssertSnapshotsEqual(ctx, env, before)
		},
	}
}

// Step 12.
func s69Procedure() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "procedure: profile C on a new prefix P2 binds through a check Job (Absent) and one roll; β2 stamps P2; A's user Secret is deleted",
		Do: func(ctx context.Context, env *runner.Env) error {
			p2 := ctxFetch(env, "backupRunStem")
			if p2 == "" {
				return errors.New("no run stem recorded")
			}
			p2 = "e2e/" + s69ID + "/" + p2 + "-p2"
			pitrTrackCR(ctx, env, pitrArchivePrefixKind, p2)
			if err := ctxStash(ctx, env, "prefixP2", p2); err != nil {
				return err
			}
			passC := s69RandomPassphrase()
			secretC, err := s69CreateSecret(ctx, env, "c", passC)
			if err != nil {
				return err
			}
			pitrStashArchivePassphrase(env, passC)
			before, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			if err := s69EditBackup(ctx, env, func(b *v1alpha1.BackupSpec) error {
				a := s69Profile(b, backupE2EProfile)
				if a == nil {
					return fmt.Errorf("profile %s missing", backupE2EProfile)
				}
				c := *a.DeepCopy()
				c.Name = s69ProfileC
				c.Storage.S3.Prefix = p2
				c.Encryption = backupE2EEncryption(secretC)
				b.Profiles = append(b.Profiles, c)
				b.PITR.ProfileName = s69ProfileC
				return nil
			}); err != nil {
				return err
			}
			entry, err := s69WaitEstablished(ctx, env, p2)
			if err != nil {
				return err
			}
			if err := s69WaitRolled(ctx, env, before, p2); err != nil {
				return fmt.Errorf("switching to P2: %w", err)
			}
			after, err := s69TakeSnapshot(ctx, env)
			if err != nil {
				return err
			}
			if err := s69AssertNoRoll(ctx, env, after, s69ShortNoRollWindow); err != nil {
				return fmt.Errorf("after the one roll to P2: %w", err)
			}
			if err := s69AssertP1Unchanged(ctx, env); err != nil {
				return fmt.Errorf("P1 must be retained: %w", err)
			}
			if err := s69WaitCondition(ctx, env, metav1.ConditionTrue, "", []string{s69BaselineNote}); err != nil {
				return err
			}
			if err := pitrBackupOfProfile(ctx, env, s69ID, "b2", s69ProfileC); err != nil {
				return err
			}
			b2, err := getMysqlBackup(ctx, env, ctxFetch(env, "backup:b2"))
			if err != nil {
				return err
			}
			if b2.Status.PITRArchiveID != entry.ArchiveID {
				return fmt.Errorf("β2 %s stamps pitrArchiveID=%q, want P2's %q", b2.Name, b2.Status.PITRArchiveID, entry.ArchiveID)
			}
			if err := s69AssertDumpKeySnapshot(ctx, env, b2, passC); err != nil {
				return err
			}
			if err := s69WaitBaselineNoteGone(ctx, env); err != nil {
				return err
			}
			// A's user Secret goes; P1's history stays readable through the
			// retained binding (step 13).
			return deleteBackupPassphraseSecret(ctx, env)
		},
	}
}

// Step 13.
func s69Release() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "release P1: wrong confirm rejected, refused while a verification reads A, released after it; release D",
		Do: func(ctx context.Context, env *runner.Env) error {
			idP1, idD := ctxFetch(env, "bindingP1"), ctxFetch(env, "bindingD")
			keyP1 := ctxFetch(env, "keySecretP1")
			e, err := s69ExpectationAtM2(env, backupE2EProfile)
			if err != nil {
				return err
			}

			// The rejection needs no reader; it runs first so the refusal
			// below is tried as soon as the verification holds P1's
			// snapshot, well before it can finish.
			rejectedAt := time.Now()
			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, brcontroller.ReleasePITRArchiveBindingAnnotation, pitrReleaseValue(idP1, "not-"+env.FG)); err != nil {
				return err
			}
			if _, err := s69WaitEvent(ctx, env, rejectedAt, "PITRArchiveBindingReleaseRejected", ""); err != nil {
				return err
			}
			if err := s69WaitAnnotationGone(ctx, env); err != nil {
				return err
			}
			if err := s69AssertP1Unchanged(ctx, env); err != nil {
				return fmt.Errorf("after a rejected release: %w", err)
			}

			name, err := pitrStartVerify(ctx, env, s69ID, s69DBName, "retained", e)
			if err != nil {
				return err
			}
			// Phase alone does not say the verification reads P1 yet: a
			// release before its hold is stamped would win (K9).
			if err := s69WaitVerificationHold(ctx, env, name, keyP1); err != nil {
				return err
			}
			refusedAt := time.Now()
			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, brcontroller.ReleasePITRArchiveBindingAnnotation, pitrReleaseValue(idP1, env.FG)); err != nil {
				return err
			}
			if _, err := s69WaitEvent(ctx, env, refusedAt, "PITRArchiveBindingReleaseRefused", ""); err != nil {
				return err
			}
			if err := s69AssertP1Unchanged(ctx, env); err != nil {
				return fmt.Errorf("after a refused release: %w", err)
			}
			// The verification reads the retained archive with A's user
			// Secret deleted.
			if err := pitrFinishVerify(ctx, env, name, e); err != nil {
				return err
			}
			// The refused annotation stays and is retried once nothing uses
			// the binding.
			if _, err := s69WaitEvent(ctx, env, refusedAt, "PITRArchiveBindingReleased", ""); err != nil {
				return err
			}
			if err := s69WaitReleased(ctx, env, idP1, keyP1); err != nil {
				return err
			}
			if err := pitrReleaseBinding(ctx, env, idD); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("released P1 (%s) and D (%s)", idP1, idD))
			return nil
		},
	}
}

func s69Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	// A release annotation left by a failed step must not fire later.
	if mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG); err == nil {
		if _, ok := mfg.Annotations[brcontroller.ReleasePITRArchiveBindingAnnotation]; ok {
			if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, brcontroller.ReleasePITRArchiveBindingAnnotation, ""); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := pitrCleanup(ctx, env, s69DBName); err != nil {
		errs = append(errs, err)
	}
	for _, rec := range pitrTrackedCRs(env, s69SecretKind) {
		name, uid, _ := strings.Cut(rec, "/")
		u := types.UID(uid)
		err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &u}})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete Secret %s: %w", name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("s69 cleanup: %w", err)
	}
	return nil
}

// ---- helpers ----

func s69RunPassphrase(env *runner.Env) []byte {
	passes := pitrArchivePassphrases(env)
	if len(passes) == 0 {
		return nil
	}
	return passes[0]
}

func s69RandomPassphrase() []byte {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return []byte(hex.EncodeToString(b))
}

// s69CreateSecret creates a passphrase Secret for this run and tracks it
// for cleanup (UID precondition).
func s69CreateSecret(ctx context.Context, env *runner.Env, suffix string, pass []byte) (string, error) {
	name := "s69-" + suffix + "-" + backupRunStamp(env)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"chaos.playground.bloodraven.io/created-by-e2e": "true"},
		},
		Data: map[string][]byte{"passphrase": pass},
	}
	created, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Create(ctx, sec, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create Secret %s: %w", name, err)
	}
	pitrTrackCR(ctx, env, s69SecretKind, name+"/"+string(created.UID))
	return name, nil
}

func s69SetSecretPassphrase(ctx context.Context, env *runner.Env, name string, pass []byte) error {
	sec, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get Secret %s: %w", name, err)
	}
	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	sec.Data["passphrase"] = pass
	sec.StringData = nil
	if _, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update Secret %s: %w", name, err)
	}
	env.Capture.Note(fmt.Sprintf("set Secret %s key passphrase (%d bytes)", name, len(pass)))
	return nil
}

func s69Profile(b *v1alpha1.BackupSpec, name string) *v1alpha1.BackupProfile {
	for i := range b.Profiles {
		if b.Profiles[i].Name == name {
			return &b.Profiles[i]
		}
	}
	return nil
}

func s69WithoutProfile(profiles []v1alpha1.BackupProfile, name string) []v1alpha1.BackupProfile {
	out := profiles[:0]
	for _, p := range profiles {
		if p.Name != name {
			out = append(out, p)
		}
	}
	return out
}

// s69BackupPatch builds the JSON patch replacing spec.backup with fn's edit
// of the live one. The scenario is the only spec writer; a resourceVersion
// test would fail on every concurrent status write.
func s69BackupPatch(ctx context.Context, env *runner.Env, fn func(*v1alpha1.BackupSpec) error) ([]byte, error) {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return nil, err
	}
	if mfg.Spec.Backup == nil || mfg.Spec.Backup.PITR == nil {
		return nil, errors.New("spec.backup.pitr is not configured")
	}
	b := mfg.Spec.Backup.DeepCopy()
	if err := fn(b); err != nil {
		return nil, err
	}
	return json.Marshal([]pgkube.JSONPatchOp{{Op: "replace", Path: "/spec/backup", Value: b}})
}

func s69EditBackup(ctx context.Context, env *runner.Env, fn func(*v1alpha1.BackupSpec) error) error {
	body, err := s69BackupPatch(ctx, env, fn)
	if err != nil {
		return err
	}
	target := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: env.Namespace, Name: env.FG}}
	if err := env.Kube.Controller.Patch(ctx, target, client.RawPatch(types.JSONPatchType, body)); err != nil {
		return fmt.Errorf("patch spec.backup: %w", err)
	}
	return nil
}

func s69EditBackupDryRun(ctx context.Context, env *runner.Env, fn func(*v1alpha1.BackupSpec) error) error {
	body, err := s69BackupPatch(ctx, env, fn)
	if err != nil {
		return err
	}
	target := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: env.Namespace, Name: env.FG}}
	return env.Kube.Controller.Patch(ctx, target, client.RawPatch(types.JSONPatchType, body), client.DryRunAll)
}

// s69Destination is the normalized destination of an e2e-bucket profile.
func s69Destination(prefix, endpoint string) (string, error) {
	d, err := pitrarchive.NewS3Destination(backupE2EBucket, prefix, endpoint)
	if err != nil {
		return "", err
	}
	return d.String(), nil
}

func s69FindBinding(mfg *v1alpha1.MysqlFailoverGroup, destination string) *v1alpha1.PITRArchiveBinding {
	for i := range mfg.Status.PITRArchiveBindings {
		if mfg.Status.PITRArchiveBindings[i].Destination == destination {
			return &mfg.Status.PITRArchiveBindings[i]
		}
	}
	return nil
}

func s69BoundBound(b *v1alpha1.PITRArchiveBinding) bool {
	return b != nil && b.State == v1alpha1.PITRArchiveBindingBound && b.Reason == "Bound"
}

func s69BindingSummary(mfg *v1alpha1.MysqlFailoverGroup, b *v1alpha1.PITRArchiveBinding) string {
	cond := "<none>"
	if c := findCondition(mfg.Status.Conditions, pitrBindingReadyCondition); c != nil {
		cond = fmt.Sprintf("%s/%s gen=%d/%d %q", c.Status, c.Reason, c.ObservedGeneration, mfg.Generation, c.Message)
	}
	if b == nil {
		return fmt.Sprintf("entry=<none> (%d entries) condition=%s", len(mfg.Status.PITRArchiveBindings), cond)
	}
	return fmt.Sprintf("entry{id=%s %s/%s key=%s archive=%s check=%v msg=%q} condition=%s", b.ID, b.State, b.Reason, b.Encryption.KeySecretName, b.ArchiveID, b.Check, b.Message, cond)
}

// s69BindingConditionIs reports whether PITRArchiveBindingReady has status
// (and reason, when set) at the current generation.
func s69BindingConditionIs(mfg *v1alpha1.MysqlFailoverGroup, status metav1.ConditionStatus, reason string) bool {
	c := findCondition(mfg.Status.Conditions, pitrBindingReadyCondition)
	return c != nil && c.Status == status && (reason == "" || c.Reason == reason) && c.ObservedGeneration == mfg.Generation
}

func s69WaitBinding(ctx context.Context, env *runner.Env, what, prefix, endpoint string, pred func(*v1alpha1.MysqlFailoverGroup, *v1alpha1.PITRArchiveBinding) (bool, string)) (*v1alpha1.MysqlFailoverGroup, *v1alpha1.PITRArchiveBinding, error) {
	dest, err := s69Destination(prefix, endpoint)
	if err != nil {
		return nil, nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, s69BindingWait)
	defer cancel()
	mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, what, func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ok, msg := pred(mfg, s69FindBinding(mfg, dest))
		return ok, msg, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return mfg, s69FindBinding(mfg, dest), nil
}

// s69WaitCondition waits for PITRArchiveBindingReady at the current
// generation with status and reason ("" = any), and a message holding
// every snippet.
func s69WaitCondition(ctx context.Context, env *runner.Env, status metav1.ConditionStatus, reason string, snippets []string) error {
	waitCtx, cancel := context.WithTimeout(ctx, s69BindingWait)
	defer cancel()
	_, err := env.Wait.UntilCR(waitCtx, env.Namespace, fmt.Sprintf("%s=%s/%s", pitrBindingReadyCondition, status, reason), func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ok := s69BindingConditionIs(mfg, status, reason)
		if c := findCondition(mfg.Status.Conditions, pitrBindingReadyCondition); ok && c != nil {
			for _, s := range snippets {
				if !strings.Contains(c.Message, s) {
					ok = false
				}
			}
		}
		return ok, s69BindingSummary(mfg, nil), nil
	})
	return err
}

func s69WaitBaselineNoteGone(ctx context.Context, env *runner.Env) error {
	waitCtx, cancel := context.WithTimeout(ctx, s69BindingWait)
	defer cancel()
	_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "baseline note dropped after β2", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		c := findCondition(mfg.Status.Conditions, pitrBindingReadyCondition)
		return c != nil && c.Status == metav1.ConditionTrue && !strings.Contains(c.Message, s69BaselineNote), s69BindingSummary(mfg, nil), nil
	})
	return err
}

func s69WaitEvent(ctx context.Context, env *runner.Env, notBefore time.Time, reason, snippet string) (corev1.Event, error) {
	waitCtx, cancel := context.WithTimeout(ctx, s69BindingWait)
	defer cancel()
	ev, err := waitForMFGEvent(waitCtx, env, notBefore, reason, snippet)
	if err != nil {
		return ev, err
	}
	if ev.Type != corev1.EventTypeWarning && reason != "PITRArchiveBindingReleased" && reason != "PITRArchiveBound" {
		return ev, fmt.Errorf("event %s is %s, want Warning: %s", reason, ev.Type, ev.Message)
	}
	env.Capture.Note(fmt.Sprintf("event %s %s: %s", ev.Type, reason, ev.Message))
	return ev, nil
}

// s69AssertP1Unchanged checks P1's entry is still Bound/Bound with the
// snapshot step 1 recorded.
func s69AssertP1Unchanged(ctx context.Context, env *runner.Env) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	b := pitrFindBindingByID(mfg, ctxFetch(env, "bindingP1"))
	if !s69BoundBound(b) || b.Encryption.KeySecretName != ctxFetch(env, "keySecretP1") || b.ArchiveID != ctxFetch(env, "archiveID") {
		return fmt.Errorf("P1 binding changed: %s (want Bound/Bound with %s, archive %s)", s69BindingSummary(mfg, b), ctxFetch(env, "keySecretP1"), ctxFetch(env, "archiveID"))
	}
	return nil
}

// s69AssertArchiveKeySnapshot checks the entry's snapshot (§4.3): an
// immutable Secret with no owner, the group's labels, the acceptance
// annotation with a keyCheck, and exactly the accepted bytes.
func s69AssertArchiveKeySnapshot(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup, b *v1alpha1.PITRArchiveBinding, want []byte) error {
	s, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, b.Encryption.KeySecretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get key snapshot %s: %w", b.Encryption.KeySecretName, err)
	}
	var problems []string
	if s.Immutable == nil || !*s.Immutable {
		problems = append(problems, "not immutable")
	}
	if len(s.OwnerReferences) != 0 {
		problems = append(problems, fmt.Sprintf("has ownerReferences %v", s.OwnerReferences))
	}
	for k, v := range map[string]string{pitrSnapshotAppLabel: pitrKeySnapshotApp, pitrGroupUIDLabel: string(mfg.UID), pitrBindingIDLabel: b.ID} {
		if s.Labels[k] != v {
			problems = append(problems, fmt.Sprintf("label %s=%q, want %q", k, s.Labels[k], v))
		}
	}
	if s.Annotations[pitrDestinationAnnotation] != b.Destination {
		problems = append(problems, fmt.Sprintf("annotation %s=%q, want %q", pitrDestinationAnnotation, s.Annotations[pitrDestinationAnnotation], b.Destination))
	}
	var acc struct {
		SchemaVersion int    `json:"schemaVersion"`
		Destination   string `json:"destination"`
		ArchiveID     string `json:"archiveID"`
		KeyCheck      string `json:"keyCheck"`
	}
	if err := json.Unmarshal([]byte(s.Annotations[pitrAcceptedAnnotation]), &acc); err != nil {
		problems = append(problems, fmt.Sprintf("annotation %s does not decode: %v", pitrAcceptedAnnotation, err))
	} else if acc.SchemaVersion != 1 || acc.KeyCheck == "" || acc.ArchiveID != b.ArchiveID || acc.Destination != b.Destination {
		problems = append(problems, fmt.Sprintf("acceptance %+v does not record the entry (archive %s, destination %s) with a keyCheck", acc, b.ArchiveID, b.Destination))
	}
	if got := s.Data[pitrSnapshotPassphraseKey]; string(got) != string(backupcrypto.TrimPassphrase(want)) {
		problems = append(problems, "does not hold the accepted passphrase")
	}
	if len(problems) > 0 {
		return fmt.Errorf("key snapshot %s: %s", s.Name, strings.Join(problems, "; "))
	}
	return nil
}

// s69AssertDumpKeySnapshot checks a backup's recorded dump key (Δ1, Δ24):
// its own immutable, ownerless snapshot of the profile's bytes at launch.
func s69AssertDumpKeySnapshot(ctx context.Context, env *runner.Env, b *v1alpha1.MysqlBackup, want []byte) error {
	if !b.Status.Encrypted || b.Status.ArtifactStorage == nil || b.Status.ArtifactStorage.PassphraseSecret == nil {
		return fmt.Errorf("MysqlBackup %s: encrypted=%v, no recorded dump key", b.Name, b.Status.Encrypted)
	}
	ref := b.Status.ArtifactStorage.PassphraseSecret
	if !strings.HasPrefix(ref.Name, "mysqlbackup-") || ref.Key != pitrSnapshotPassphraseKey {
		return fmt.Errorf("MysqlBackup %s records %s/%s, want its own mysqlbackup-…-dump-key snapshot, key %s", b.Name, ref.Name, ref.Key, pitrSnapshotPassphraseKey)
	}
	s, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get dump key %s: %w", ref.Name, err)
	}
	var problems []string
	if s.Immutable == nil || !*s.Immutable {
		problems = append(problems, "not immutable")
	}
	if len(s.OwnerReferences) != 0 {
		problems = append(problems, fmt.Sprintf("has ownerReferences %v", s.OwnerReferences))
	}
	if s.Labels[pitrSnapshotAppLabel] != pitrDumpKeySnapshotApp || s.Labels["bloodraven.shipstream.io/mysqlbackup-uid"] != string(b.UID) {
		problems = append(problems, fmt.Sprintf("labels %v do not name the dump key of %s", s.Labels, b.UID))
	}
	if string(s.Data[pitrSnapshotPassphraseKey]) != string(backupcrypto.TrimPassphrase(want)) {
		problems = append(problems, "does not hold the profile's bytes at launch")
	}
	if len(problems) > 0 {
		return fmt.Errorf("dump key %s of %s: %s", s.Name, b.Name, strings.Join(problems, "; "))
	}
	env.Capture.Note(fmt.Sprintf("MysqlBackup %s decrypts with its own dump key %s", b.Name, s.Name))
	return nil
}

// s69AssertRenders checks every primary-candidate site Deployment renders
// the snapshot (optional mount, revision echo, pitr-render annotation) and
// never the user Secret.
func s69AssertRenders(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup, key, userSecret string) error {
	for _, site := range PrimaryCandidates(mfg) {
		dep, err := env.Kube.GetDeployment(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, site))
		if err != nil {
			return err
		}
		if err := s69CheckRender(dep, key, userSecret); err != nil {
			return fmt.Errorf("site %s: %w", site, err)
		}
	}
	return nil
}

func s69CheckRender(dep *appsv1.Deployment, key, userSecret string) error {
	var mounted bool
	for _, v := range dep.Spec.Template.Spec.Volumes {
		if v.Secret == nil {
			continue
		}
		switch v.Secret.SecretName {
		case userSecret:
			return fmt.Errorf("volume %s mounts the user Secret %s", v.Name, userSecret)
		case key:
			if v.Secret.Optional == nil || !*v.Secret.Optional {
				return fmt.Errorf("volume %s mounts snapshot %s without optional: true", v.Name, key)
			}
			mounted = true
		}
	}
	if !mounted {
		return fmt.Errorf("no volume mounts the key snapshot %s", key)
	}
	revision := ""
	for _, c := range dep.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			switch e.Name {
			case "BLOODRAVEN_PITR_KEY_REVISION":
				revision = e.Value
			case "BLOODRAVEN_PITR_PROFILE_NAME":
				return fmt.Errorf("container %s still renders BLOODRAVEN_PITR_PROFILE_NAME", c.Name)
			}
		}
	}
	if revision != key {
		return fmt.Errorf("BLOODRAVEN_PITR_KEY_REVISION=%q, want %s", revision, key)
	}
	var render struct {
		Enabled        bool   `json:"enabled"`
		EncryptionMode string `json:"encryptionMode"`
		KeySecretName  string `json:"keySecretName"`
	}
	if err := json.Unmarshal([]byte(dep.Annotations[pitrRenderAnnotation]), &render); err != nil {
		return fmt.Errorf("annotation %s does not decode: %w", pitrRenderAnnotation, err)
	}
	if !render.Enabled || render.EncryptionMode != string(v1alpha1.PITRArchiveEncryptionPassphrase) || render.KeySecretName != key {
		return fmt.Errorf("annotation %s=%s does not render the binding's identity (Passphrase, %s)", pitrRenderAnnotation, dep.Annotations[pitrRenderAnnotation], key)
	}
	return nil
}

// s69Snapshot is what a roll would change: each site Deployment's
// spec-hash and the UID of its MySQL pod.
type s69Snapshot struct {
	SpecHash map[string]string `json:"specHash"`
	Pods     map[string]string `json:"pods"`
}

func s69TakeSnapshot(ctx context.Context, env *runner.Env) (s69Snapshot, error) {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return s69Snapshot{}, err
	}
	snap := s69Snapshot{SpecHash: map[string]string{}, Pods: map[string]string{}}
	for _, s := range mfg.Spec.Sites {
		dep, err := env.Kube.GetDeployment(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, s.Name))
		if err != nil {
			return s69Snapshot{}, err
		}
		snap.SpecHash[s.Name] = dep.Annotations["shipstream.io/spec-hash"]
		pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, s.Name)
		if err != nil {
			return s69Snapshot{}, err
		}
		snap.Pods[s.Name] = string(pod.UID)
	}
	return snap, nil
}

// s69SnapshotDiff names what differs between two snapshots.
func s69SnapshotDiff(a, b s69Snapshot) []string {
	var diff []string
	for site, h := range a.SpecHash {
		if b.SpecHash[site] != h {
			diff = append(diff, fmt.Sprintf("site %s spec-hash %q -> %q", site, h, b.SpecHash[site]))
		}
		if b.Pods[site] != a.Pods[site] {
			diff = append(diff, fmt.Sprintf("site %s pod %s -> %s", site, a.Pods[site], b.Pods[site]))
		}
	}
	return diff
}

func s69StashSnapshot(ctx context.Context, env *runner.Env, key string, s s69Snapshot) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return ctxStash(ctx, env, "snapshot:"+key, string(body))
}

func s69FetchSnapshot(env *runner.Env, key string) (s69Snapshot, error) {
	var s s69Snapshot
	if err := json.Unmarshal([]byte(ctxFetch(env, "snapshot:"+key)), &s); err != nil {
		return s, fmt.Errorf("snapshot %q was not recorded: %w", key, err)
	}
	return s, nil
}

func s69AssertSnapshotsEqual(ctx context.Context, env *runner.Env, before s69Snapshot) error {
	now, err := s69TakeSnapshot(ctx, env)
	if err != nil {
		return err
	}
	if d := s69SnapshotDiff(before, now); len(d) > 0 {
		return fmt.Errorf("the group rolled: %s", strings.Join(d, "; "))
	}
	return nil
}

// s69AssertNoRoll fails as soon as a spec-hash or a pod changes within
// window.
func s69AssertNoRoll(ctx context.Context, env *runner.Env, before s69Snapshot, window time.Duration) error {
	deadline := time.Now().Add(window)
	for {
		if err := s69AssertSnapshotsEqual(ctx, env, before); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			env.Capture.Note(fmt.Sprintf("no roll for %s", window))
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// s69WaitRolled waits until every primary-candidate site's spec-hash and
// pod changed from before and the group settled (s69WaitSettled).
func s69WaitRolled(ctx context.Context, env *runner.Env, before s69Snapshot, prefix string) error {
	waitCtx, cancel := context.WithTimeout(ctx, s69RolloutWait)
	defer cancel()
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	var last string
	for {
		mfg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, env.FG)
		if err == nil {
			var now s69Snapshot
			if now, err = s69TakeSnapshot(waitCtx, env); err == nil {
				pending := []string{}
				for _, site := range PrimaryCandidates(mfg) {
					if now.SpecHash[site] == before.SpecHash[site] || now.Pods[site] == before.Pods[site] {
						pending = append(pending, site)
					}
				}
				if len(pending) == 0 {
					break
				}
				last = fmt.Sprintf("sites not rolled yet: %v", pending)
			}
		}
		if err != nil {
			last = err.Error()
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("roll did not happen: %w (last: %s)", waitCtx.Err(), last)
		case <-tick.C:
		}
	}
	return s69WaitSettled(ctx, env, prefix)
}

// s69WaitSettled waits until the group is Ready at its generation with no
// update in progress, every non-active primary candidate replicates (the
// failover target included, after a failover), and, with a prefix, the
// active archiver archives there.
func s69WaitSettled(ctx context.Context, env *runner.Env, prefix string) error {
	waitCtx, cancel := context.WithTimeout(ctx, s69RolloutWait)
	defer cancel()
	mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "group settled", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		ready := conditionTrueForGeneration(mfg.Status.Conditions, "Ready", mfg.Generation)
		return ready && mfg.Status.UpdatePhase == "" && mfg.Status.ActiveSite != "",
			fmt.Sprintf("ready=%v updatePhase=%q active=%q", ready, mfg.Status.UpdatePhase, mfg.Status.ActiveSite), nil
	})
	if err != nil {
		return err
	}
	for _, site := range PrimaryCandidates(mfg) {
		if site == mfg.Status.ActiveSite {
			continue
		}
		if err := pitrWaitSiteReplicating(waitCtx, env, site); err != nil {
			return err
		}
	}
	if prefix == "" {
		return nil
	}
	_, err = waitForArchiverReady(waitCtx, env, mfg.Status.ActiveSite, prefix+"/binlogs")
	return err
}

// s69QuiesceForRestart readies the primary a for a pod restart that may
// fail over to b: b has applied everything a executed, and a's binlog is
// sealed and archived, so the failover loses nothing and leaves no
// unarchived tail on a (FU-1). b must also be a failover target the
// operator accepts.
func s69QuiesceForRestart(ctx context.Context, env *runner.Env, a, b string) error {
	// A failover needs a target the operator accepts: replicating and not
	// RecoveryBlocked.
	if err := pitrWaitSiteReplicating(ctx, env, b); err != nil {
		return err
	}
	if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
		return err
	}
	return pitrFlush(ctx, env, a, true)
}

// s69RestartSitePod deletes site's MySQL pod (graceful) and waits for its
// replacement and a settled group; the group may fail over.
func s69RestartSitePod(ctx context.Context, env *runner.Env, site, prefix string) error {
	old, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, site)
	if err != nil {
		return err
	}
	if err := env.Kube.DeleteSitePod(ctx, env.Namespace, env.FG, site, nil); err != nil {
		return fmt.Errorf("delete %s's pod: %w", site, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, s69RolloutWait)
	defer cancel()
	var last string
	for {
		pod, err := env.Kube.GetSiteMysqlPod(waitCtx, env.Namespace, env.FG, site)
		if err != nil {
			last = err.Error()
		} else if pod.UID == old.UID {
			last = "old pod still present"
		} else if !s69PodServing(pod) {
			last = "new pod " + pod.Name + " not Ready"
		} else {
			break
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("site %s pod not replaced: %w (last: %s)", site, waitCtx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
	env.Capture.Note(fmt.Sprintf("restarted %s's pod %s", site, old.Name))
	return s69WaitSettled(ctx, env, prefix)
}

// s69PodServing is a running, Ready pod that is not being deleted.
func s69PodServing(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning && podReady(pod)
}

// s69MarkerArchivedByNewPod writes a marker on the current primary, takes
// a flushing fence (stashed as fenceKey) and proves the file holding it was
// archived by that site's current pod, which is not in before, with the
// accepted snapshot: the site's manifest has the sealed file, the
// archiver reports Verified with the snapshot as its key revision, and the
// object decrypts with the bytes stashed before any rotation.
func s69MarkerArchivedByNewPod(ctx context.Context, env *runner.Env, fenceKey string, id int, phase string, before s69Snapshot) error {
	a, b, err := activeAndReplica(ctx, env)
	if err != nil {
		return err
	}
	pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, a)
	if err != nil {
		return err
	}
	if string(pod.UID) == before.Pods[a] {
		return fmt.Errorf("primary %s still runs pod %s, which started before this step", a, pod.Name)
	}
	if err := pitrInsertMarker(ctx, env, a, s69DBName, id, phase); err != nil {
		return err
	}
	if err := waitForReplicaGTID(ctx, env, a, b); err != nil {
		return err
	}
	fence, err := pitrTakeFence(ctx, env, a, true)
	if err != nil {
		return err
	}
	if err := pitrStashFence(ctx, env, fenceKey, fence); err != nil {
		return err
	}
	if !strings.HasSuffix(fence.Manifest, "/manifest-"+a+".json") {
		return fmt.Errorf("%s was sealed into manifest %s, not primary %s's", fence.File, fence.Manifest, a)
	}
	if err := s69AssertArchiverKeyRevision(ctx, env, a, ctxFetch(env, "keySecretP1")); err != nil {
		return err
	}
	files, err := pitrReadManifest(ctx, env, fence.Manifest)
	if err != nil {
		return err
	}
	var row *pitrManifestFile
	for i := range files {
		if files[i].Name == fence.File {
			row = &files[i]
		}
	}
	if row == nil || row.RemotePath == "" {
		return fmt.Errorf("manifest %s has no row with an object for %s", fence.Manifest, fence.File)
	}
	data, found, err := env.Chaos.ReadRustFSObject(ctx, backupE2EBucket, row.RemotePath)
	if err != nil || !found {
		return fmt.Errorf("read archived %s at %s: found=%v err=%v", fence.File, row.RemotePath, found, err)
	}
	if !backupcrypto.LooksEncrypted(data) {
		return fmt.Errorf("archived %s at %s is not encrypted", fence.File, row.RemotePath)
	}
	if _, err := pitrDecryptObject(data, s69RunPassphrase(env)); err != nil {
		return fmt.Errorf("archived %s at %s does not decrypt with the original bytes: %w", fence.File, row.RemotePath, err)
	}
	env.Capture.Note(fmt.Sprintf("%s archived by %s's pod %s as %s, encrypted with the accepted key", phase, a, pod.Name, row.RemotePath))
	return nil
}

func s69AssertArchiverKeyRevision(ctx context.Context, env *runner.Env, site, key string) error {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return err
	}
	defer probe.Close()
	st, err := probe.ArchiverStatus(ctx)
	if err != nil {
		return err
	}
	if st.Archive == nil || st.Archive.State != pitrarchive.StateVerified || st.Archive.KeyRevision != key {
		return fmt.Errorf("archiver on %s does not report Verified with key revision %s: %s archive=%+v", site, key, archiverSummary(st), st.Archive)
	}
	return nil
}

// s69ExpectationAtM2 is a verification of β1 at M2's fence.
func s69ExpectationAtM2(env *runner.Env, archiveProfile string) (pitrExpectation, error) {
	f, err := pitrFetchFence(env, "m2")
	if err != nil {
		return pitrExpectation{}, err
	}
	return pitrExpectation{
		Backup: "b1", Mode: "timestamp", Fence: f, Outcome: v1alpha1.PointInTimeTargetCovered,
		Markers: []string{"baseline", "M1", "M2"}, ArchiveProfile: archiveProfile,
	}, nil
}

// s69WatchReadBinding polls the read binding of prefix at endpoint until
// done holds, recording whether a check Job ran since notBefore (a Job
// with the check labels, or a recorded binding.check). With verification
// set, the verification must stay unfinished and report
// PITRArchiveBindingNotReady while it waits.
func s69WatchReadBinding(ctx context.Context, env *runner.Env, prefix, endpoint string, notBefore time.Time, verification string, done func(*v1alpha1.PITRArchiveBinding) bool, wantWaiting bool) (*v1alpha1.PITRArchiveBinding, error) {
	dest, err := s69Destination(prefix, endpoint)
	if err != nil {
		return nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, s69BindingWait)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	checked := false
	var last string
	for {
		jobs, err := s69CheckJobsSince(waitCtx, env, notBefore)
		if err == nil && len(jobs) > 0 {
			checked = true
		}
		mfg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, env.FG)
		if err != nil {
			last = err.Error()
		} else {
			b := s69FindBinding(mfg, dest)
			if b != nil && b.Check != nil {
				checked = true
			}
			last = s69BindingSummary(mfg, b) + fmt.Sprintf(" checkSeen=%v", checked)
			if b != nil && done(b) && checked {
				if !wantWaiting {
					return b, nil
				}
				v, err := getMysqlBackupVerification(waitCtx, env, verification)
				if err != nil {
					return nil, err
				}
				if v.Status.Phase == v1alpha1.VerificationPhaseSucceeded || v.Status.Phase == v1alpha1.VerificationPhaseFailed {
					return nil, fmt.Errorf("verification %s finished (%s) on a refused read binding: %s", verification, v.Status.Phase, verificationStatusSummary(v))
				}
				if s69VerificationMentions(v, "PITRArchiveBindingNotReady") {
					return b, nil
				}
				last += "; verification " + verificationStatusSummary(v)
			}
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("read binding %s: %w (last: %s)", dest, waitCtx.Err(), last)
		case <-tick.C:
		}
	}
}

func s69VerificationMentions(v *v1alpha1.MysqlBackupVerification, token string) bool {
	if strings.Contains(v.Status.Message, token) {
		return true
	}
	for _, c := range v.Status.Conditions {
		if c.Reason == token || strings.Contains(c.Message, token) {
			return true
		}
	}
	return false
}

func s69CheckJobsSince(ctx context.Context, env *runner.Env, notBefore time.Time) ([]string, error) {
	list, err := env.Kube.Kubernetes.BatchV1().Jobs(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: pitrCheckJobSelector})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, j := range list.Items {
		if !j.CreationTimestamp.Time.Before(notBefore.Add(-2 * time.Second)) {
			names = append(names, j.Name)
		}
	}
	return names, nil
}

// s69WaitEstablished follows the procedure's new write binding: it must
// pass through Pending/Establishing (a check Job found the prefix empty)
// and end Bound/Bound on the archive the sidecars report.
func s69WaitEstablished(ctx context.Context, env *runner.Env, prefix string) (*v1alpha1.PITRArchiveBinding, error) {
	dest, err := s69Destination(prefix, backupE2EEndpoint)
	if err != nil {
		return nil, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, s69RolloutWait)
	defer cancel()
	establishing := false
	mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "P2 binding Establishing then Bound", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		b := s69FindBinding(mfg, dest)
		if b != nil && b.State == v1alpha1.PITRArchiveBindingPending && b.Reason == "Establishing" {
			establishing = true
		}
		done := establishing && s69BoundBound(b) && pitrObservedArchiveID(mfg, prefix) == b.ArchiveID && b.ArchiveID != ""
		return done, s69BindingSummary(mfg, b) + fmt.Sprintf(" establishingSeen=%v archive=%s", establishing, pitrArchiveSummary(mfg)), nil
	})
	if err != nil {
		return nil, err
	}
	b := s69FindBinding(mfg, dest)
	env.Capture.Note(fmt.Sprintf("P2 binding %s Bound with %s (archive %s) after Establishing", b.ID, b.Encryption.KeySecretName, b.ArchiveID))
	return b, nil
}

func s69WaitOperatorGone(ctx context.Context, env *runner.Env) error {
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for {
		n, err := env.Kube.PodCount(waitCtx, env.Namespace, pgmetrics.OperatorPodSelector)
		if err == nil && n == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("operator pods still present (%d): %w", n, waitCtx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// s69WaitRebuilt waits until every wanted binding is back Bound/Bound with
// the same snapshot, and fails if a check Job or a recorded check appears
// meanwhile or in the half minute after (recovery needs no Job).
func s69WaitRebuilt(ctx context.Context, env *runner.Env, want map[string]string, since time.Time) error {
	waitCtx, cancel := context.WithTimeout(ctx, s69BindingWait)
	defer cancel()
	var rebuiltAt time.Time
	var last string
	for {
		if jobs, err := s69CheckJobsSince(waitCtx, env, since); err == nil && len(jobs) > 0 {
			return fmt.Errorf("check Jobs %v ran after the status wipe; recovery must rebuild from the acceptance annotations", jobs)
		}
		mfg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, env.FG)
		if err != nil {
			last = err.Error()
		} else {
			all := true
			for id, key := range want {
				b := pitrFindBindingByID(mfg, id)
				if b != nil && b.Check != nil {
					return fmt.Errorf("binding %s records a check Job %s after the wipe", id, b.Check.JobName)
				}
				if !s69BoundBound(b) || b.Encryption.KeySecretName != key {
					all = false
					last = s69BindingSummary(mfg, b)
				}
			}
			if all && rebuiltAt.IsZero() {
				rebuiltAt = time.Now()
				env.Capture.Note(fmt.Sprintf("bindings %v rebuilt after the status wipe", want))
			}
			if !rebuiltAt.IsZero() && time.Since(rebuiltAt) >= s69ShortNoRollWindow {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("bindings not rebuilt: %w (last: %s)", waitCtx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

// s69WaitRecoveryLog finds the restarted operator's `pitr archive binding
// changed` line with cause=recovery for every wanted binding.
func s69WaitRecoveryLog(ctx context.Context, env *runner.Env, want map[string]string, since time.Time) error {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last string
	for {
		pod, err := env.Kube.FindPodWithLabel(waitCtx, env.Namespace, pgmetrics.OperatorPodSelector)
		if err == nil && pod.CreationTimestamp.Time.Before(since.Add(-2*time.Second)) {
			err = fmt.Errorf("operator pod %s predates the wipe", pod.Name)
		}
		if err == nil {
			var raw []byte
			if raw, err = env.Kube.PodLogTailLines(waitCtx, env.Namespace, pod.Name, "operator", 20000); err == nil {
				missing := []string{}
				for id := range want {
					pred := pglogs.Structured("pitr archive binding changed", map[string]string{"bindingID": id, "cause": "recovery", "state": "Bound"})
					found := false
					for _, line := range strings.Split(string(raw), "\n") {
						if pred(line) {
							found = true
							break
						}
					}
					if !found {
						missing = append(missing, id)
					}
				}
				if len(missing) == 0 {
					return nil
				}
				last = fmt.Sprintf("no recovery line for %v in %s", missing, pod.Name)
			}
		}
		if err != nil {
			last = err.Error()
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("operator log `pitr archive binding changed` cause=recovery: %w (last: %s)", waitCtx.Err(), last)
		case <-time.After(3 * time.Second):
		}
	}
}

// s69WaitVerificationHold waits until the key snapshot carries the
// running verification's hold (hold.verification.shipstream.io/<uid> =
// name), which the operator's release in-use scan honours.
func s69WaitVerificationHold(ctx context.Context, env *runner.Env, name, snapshot string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last string
	for {
		v, err := getMysqlBackupVerification(waitCtx, env, name)
		switch {
		case err != nil:
			last = err.Error()
		case v.Status.Phase == v1alpha1.VerificationPhaseSucceeded || v.Status.Phase == v1alpha1.VerificationPhaseFailed:
			return fmt.Errorf("verification %s finished before the release was tried: %s", name, verificationStatusSummary(v))
		default:
			s, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(waitCtx, snapshot, metav1.GetOptions{})
			if err != nil {
				last = err.Error()
			} else if s.Annotations[s69VerificationHoldPrefix+string(v.UID)] == name {
				env.Capture.Note(fmt.Sprintf("verification %s holds key snapshot %s", name, snapshot))
				return nil
			} else {
				last = fmt.Sprintf("snapshot %s carries no hold of %s; verification %s", snapshot, name, verificationStatusSummary(v))
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("verification %s hold on %s: %w (last: %s)", name, snapshot, waitCtx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

func s69WaitAnnotationGone(ctx context.Context, env *runner.Env) error {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "release annotation removed", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		v, ok := mfg.Annotations[brcontroller.ReleasePITRArchiveBindingAnnotation]
		return !ok, fmt.Sprintf("annotation=%q", v), nil
	})
	return err
}

// s69WaitReleased waits until the released entry, its annotation and every
// snapshot of its id are gone.
func s69WaitReleased(ctx context.Context, env *runner.Env, id, key string) error {
	waitCtx, cancel := context.WithTimeout(ctx, s69BindingWait)
	defer cancel()
	mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "binding "+id+" released", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		_, pending := mfg.Annotations[brcontroller.ReleasePITRArchiveBindingAnnotation]
		b := pitrFindBindingByID(mfg, id)
		return !pending && b == nil, fmt.Sprintf("annotation=%v %s", pending, s69BindingSummary(mfg, b)), nil
	})
	if err != nil {
		return err
	}
	snaps, err := pitrListKeySnapshots(ctx, env, string(mfg.UID), id)
	if err != nil {
		return err
	}
	if len(snaps) != 0 {
		return fmt.Errorf("binding %s released but %d of its snapshots remain", id, len(snaps))
	}
	if _, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, key, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return fmt.Errorf("snapshot %s of released binding %s still exists (err=%v)", key, id, err)
	}
	return nil
}
