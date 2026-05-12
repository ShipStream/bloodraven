package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const verifyUsage = `Create an on-demand MysqlBackupVerification.

Usage:
  kubectl bloodraven verify-backup <group> --profile <name> [flags]

Creates a verification CR that restores the most recent successful
backup (or --backup if given) into an ephemeral mysqld and reports
whether the dump loads cleanly.

Flags:
  --profile string       backup profile name (required)
  --backup string        pin to a specific MysqlBackup (default: latest Succeeded)
  --triggered-by string  label recorded on the CR (default: "manual")
  --wait                 block until the verification reaches Succeeded or Failed
  --timeout DURATION     timeout for --wait (default: 30m)
  --kubeconfig string    path to kubeconfig
  --context string       kubeconfig context
  --namespace, -n        namespace
`

func runVerifyBackup(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("verify-backup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cf          commonFlags
		profile     string
		backupName  string
		triggeredBy string
		wait        bool
		timeout     time.Duration
	)
	registerCommonFlags(fs, &cf)
	fs.StringVar(&profile, "profile", "", "backup profile name (required)")
	fs.StringVar(&backupName, "backup", "", "pin to a specific MysqlBackup (default: latest Succeeded)")
	fs.StringVar(&triggeredBy, "triggered-by", "manual", "label recorded on the CR")
	fs.BoolVar(&wait, "wait", false, "block until the verification reaches Succeeded or Failed")
	fs.DurationVar(&timeout, "timeout", 30*time.Minute, "timeout for --wait")
	fs.Usage = func() { fmt.Fprint(stderr, verifyUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}
	positional := fs.Args()
	if len(positional) != 1 {
		fmt.Fprint(stderr, verifyUsage)
		return fmt.Errorf("verify-backup requires exactly one positional argument (group name)")
	}
	group := positional[0]
	if group == "" {
		return fmt.Errorf("group must be non-empty")
	}
	if profile == "" {
		return fmt.Errorf("--profile is required")
	}

	cl, ns, err := cf.newClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var fg v1alpha1.MysqlFailoverGroup
	if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: group}, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("MysqlFailoverGroup %s/%s not found", ns, group)
		}
		return fmt.Errorf("get failover group: %w", err)
	}
	if !groupHasBackupProfile(&fg, profile) {
		return fmt.Errorf("profile %q is not defined in spec.backup.profiles of %s/%s", profile, ns, group)
	}
	if backupName != "" {
		var ref v1alpha1.MysqlBackup
		if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: backupName}, &ref); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("MysqlBackup %s/%s not found", ns, backupName)
			}
			return fmt.Errorf("get MysqlBackup: %w", err)
		}
		// Catch a fat-finger combination client-side (e.g.
		// --backup=orders-nightly-abcde --profile=weekly) so the
		// verification doesn't spin up a pod that's only going to
		// fail with a controller-side BackupGroupMismatch event.
		if ref.Spec.FailoverGroupRef.Name != group {
			return fmt.Errorf("MysqlBackup %s/%s belongs to failover group %q, not %q",
				ns, backupName, ref.Spec.FailoverGroupRef.Name, group)
		}
		if ref.Spec.ProfileName != profile {
			return fmt.Errorf("MysqlBackup %s/%s was taken with profile %q, not %q",
				ns, backupName, ref.Spec.ProfileName, profile)
		}
	}

	cr := buildVerificationCR(&fg, ns, group, profile, backupName, triggeredBy)
	if err := cl.Create(ctx, cr); err != nil {
		return fmt.Errorf("create MysqlBackupVerification: %w", err)
	}
	fmt.Fprintf(stdout, "Created MysqlBackupVerification %s/%s (profile=%s, triggeredBy=%s)\n", cr.Namespace, cr.Name, profile, triggeredBy)
	if backupName != "" {
		fmt.Fprintf(stdout, "  Pinned to backup: %s\n", backupName)
	}
	if !wait {
		fmt.Fprintln(stdout, "Watch with: kubectl get mysqlbackupverification", cr.Name, "-n", ns, "-w")
		return nil
	}
	return waitForVerification(stdout, stderr, cl, ns, cr.Name, timeout)
}

// buildVerificationCR mirrors trigger_verification.go's CR shape: it
// pulls VerificationSpec defaults off the named profile so a manual
// verification produces a CR equivalent to one a scheduled CronJob
// would create.
func buildVerificationCR(fg *v1alpha1.MysqlFailoverGroup, ns, group, profile, backupName, triggeredBy string) *v1alpha1.MysqlBackupVerification {
	if triggeredBy == "" {
		triggeredBy = "manual"
	}
	cr := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: generateNameWithInfix(group, profile, "verify"),
			Namespace:    ns,
			Labels: map[string]string{
				"shipstream.io/failover-group": group,
				"shipstream.io/backup-profile": profile,
				"app.kubernetes.io/managed-by": "bloodraven",
			},
		},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: group},
			ProfileName:      profile,
			TriggeredBy:      triggeredBy,
		},
	}
	if backupName != "" {
		cr.Spec.BackupRef = &corev1.LocalObjectReference{Name: backupName}
	}
	if vs := profileVerificationSpec(fg, profile); vs != nil {
		cr.Spec.KeepOnFailure = vs.KeepOnFailure
		cr.Spec.TTLSecondsAfterFinished = vs.TTLSecondsAfterFinished
		if vs.Storage != nil {
			cr.Spec.Storage = vs.Storage.DeepCopy()
		}
		if vs.PointInTime != nil {
			cr.Spec.PointInTime = vs.PointInTime.DeepCopy()
		}
		if vs.SanityCheck != nil {
			cr.Spec.SanityCheck = vs.SanityCheck.DeepCopy()
		}
	}
	return cr
}

func profileVerificationSpec(fg *v1alpha1.MysqlFailoverGroup, profile string) *v1alpha1.VerificationSpec {
	if fg.Spec.Backup == nil {
		return nil
	}
	for i := range fg.Spec.Backup.Profiles {
		if fg.Spec.Backup.Profiles[i].Name == profile {
			return fg.Spec.Backup.Profiles[i].Verification
		}
	}
	return nil
}

func waitForVerification(stdout, stderr io.Writer, cl client.Client, ns, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	startedAt := time.Now()
	last := v1alpha1.VerificationPhase("")
	consecutiveErrors := 0

	for {
		var v v1alpha1.MysqlBackupVerification
		getCtx, getCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.Get(getCtx, client.ObjectKey{Namespace: ns, Name: name}, &v)
		getCancel()
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("MysqlBackupVerification %s/%s disappeared during wait", ns, name)
			}
			if ctx.Err() != nil {
				return fmt.Errorf("timed out after %s waiting for MysqlBackupVerification %s (last phase: %s)", timeout, name, last)
			}
			consecutiveErrors++
			if consecutiveErrors >= maxWaitGetFailures {
				return fmt.Errorf("get MysqlBackupVerification during wait: %w (gave up after %d consecutive failures)", err, consecutiveErrors)
			}
			fmt.Fprintf(stderr, "warning: get MysqlBackupVerification %s/%s failed: %v; retrying in 5s\n", ns, name, err)
			select {
			case <-ctx.Done():
				return fmt.Errorf("timed out after %s waiting for MysqlBackupVerification %s (last phase: %s)", timeout, name, last)
			case <-tick.C:
				continue
			}
		}
		consecutiveErrors = 0
		if v.Status.Phase != last {
			elapsed := time.Since(startedAt).Round(time.Second)
			fmt.Fprintf(stdout, "[%s] phase: %s — %s\n", elapsed, emptyDash(string(v.Status.Phase)), v.Status.Message)
			last = v.Status.Phase
		}
		switch v.Status.Phase {
		case v1alpha1.VerificationPhaseSucceeded:
			fmt.Fprintln(stdout, "Verification succeeded.")
			if v.Status.BackupRef != nil {
				fmt.Fprintf(stdout, "  Verified backup: %s\n", v.Status.BackupRef.Name)
			}
			if v.Status.DurationSeconds > 0 {
				fmt.Fprintf(stdout, "  Duration: %ds\n", v.Status.DurationSeconds)
			}
			return nil
		case v1alpha1.VerificationPhaseFailed:
			fmt.Fprintf(stderr, "Verification failed: %s\n", v.Status.Message)
			return fmt.Errorf("verification %s reached Failed", name)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s waiting for MysqlBackupVerification %s (last phase: %s)", timeout, name, last)
		case <-tick.C:
		}
	}
}
