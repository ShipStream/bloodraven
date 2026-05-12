package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const backupUsage = `Create an on-demand MysqlBackup for a profile.

Usage:
  kubectl bloodraven backup <group> --profile <name> [flags]

Flags:
  --profile string       profile name from spec.backup.profiles (required)
  --source-site string   force a specific site as the dump source
  --triggered-by string  free-form label recorded on the CR (default: "manual")
  --wait                 block until the backup reaches Succeeded or Failed
  --timeout DURATION     timeout for --wait (default: 2h)
  --kubeconfig string    path to kubeconfig
  --context string       kubeconfig context
  --namespace, -n        namespace
`

func runBackup(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cf          commonFlags
		profile     string
		sourceSite  string
		triggeredBy string
		wait        bool
		timeout     time.Duration
	)
	registerCommonFlags(fs, &cf)
	fs.StringVar(&profile, "profile", "", "backup profile name (required)")
	fs.StringVar(&sourceSite, "source-site", "", "force a specific site as the dump source")
	fs.StringVar(&triggeredBy, "triggered-by", "manual", "free-form label recorded on the CR")
	fs.BoolVar(&wait, "wait", false, "block until the backup reaches Succeeded or Failed")
	fs.DurationVar(&timeout, "timeout", 2*time.Hour, "timeout for --wait")
	fs.Usage = func() { fmt.Fprint(stderr, backupUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}
	positional := fs.Args()
	if len(positional) != 1 {
		fmt.Fprint(stderr, backupUsage)
		return fmt.Errorf("backup requires exactly one positional argument (group name)")
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

	// Validate the profile exists so we fail fast with a clean error
	// instead of letting the operator stamp Failed{ProfileNotFound}
	// onto a freshly-created CR.
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
	if sourceSite != "" && !groupHasSite(&fg, sourceSite) {
		return fmt.Errorf("--source-site %q is not defined in spec.sites of %s/%s", sourceSite, ns, group)
	}

	backup := buildBackupCR(ns, group, profile, sourceSite, triggeredBy)
	if err := cl.Create(ctx, backup); err != nil {
		return fmt.Errorf("create MysqlBackup: %w", err)
	}
	fmt.Fprintf(stdout, "Created MysqlBackup %s/%s (profile=%s, triggeredBy=%s)\n", backup.Namespace, backup.Name, profile, triggeredBy)
	if sourceSite != "" {
		fmt.Fprintf(stdout, "  Source site override: %s\n", sourceSite)
	}

	if !wait {
		fmt.Fprintln(stdout, "Watch with: kubectl get mysqlbackup", backup.Name, "-n", ns, "-w")
		return nil
	}
	return waitForBackup(stdout, stderr, cl, ns, backup.Name, timeout)
}

func buildBackupCR(ns, group, profile, sourceSite, triggeredBy string) *v1alpha1.MysqlBackup {
	if triggeredBy == "" {
		triggeredBy = "manual"
	}
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: backupGenerateName(group, profile),
			Namespace:    ns,
			Labels: map[string]string{
				"shipstream.io/failover-group": group,
				"shipstream.io/backup-profile": profile,
				"app.kubernetes.io/managed-by": "bloodraven",
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef:   v1alpha1.LocalGroupRef{Name: group},
			ProfileName:        profile,
			SourceSiteOverride: sourceSite,
			TriggeredBy:        triggeredBy,
		},
	}
}

// backupGenerateName mirrors the controller-side trigger-backup helper:
// "<group>-<profile>-". Trimmed to fit the 253-char resource-name DNS
// budget after Kubernetes appends a 5-char random suffix.
func backupGenerateName(group, profile string) string {
	return generateNameWithInfix(group, profile, "")
}

// generateNameWithInfix returns "<group>-<profile>-<infix>-" trimmed
// to fit the 253-char DNS subdomain budget after Kubernetes appends
// its 5-char random suffix. The infix is empty for `MysqlBackup` and
// "verify" for `MysqlBackupVerification`.
//
// The operator does not require this name shape — only the labels
// carry semantic meaning — but matching the controller's convention
// keeps logs and dashboards consistent. We trim conservatively so a
// long group+profile pair doesn't trigger an apiserver 422 ("must be
// no more than 253 characters") that's painful to debug.
func generateNameWithInfix(group, profile, infix string) string {
	var prefix string
	if infix == "" {
		prefix = fmt.Sprintf("%s-%s-", group, profile)
	} else {
		prefix = fmt.Sprintf("%s-%s-%s-", group, profile, infix)
	}
	// 5 chars for the server-generated suffix; leave one extra byte
	// of headroom for the trailing "-" we re-attach when truncating.
	const maxPrefix = 253 - 6
	if len(prefix) > maxPrefix {
		// Trim to maxPrefix-1 so re-appending the required trailing
		// dash always lands at exactly maxPrefix.
		trimmed := strings.TrimRight(prefix[:maxPrefix-1], "-")
		prefix = trimmed + "-"
	}
	return prefix
}

func groupHasBackupProfile(fg *v1alpha1.MysqlFailoverGroup, profile string) bool {
	if fg.Spec.Backup == nil {
		return false
	}
	for _, p := range fg.Spec.Backup.Profiles {
		if p.Name == profile {
			return true
		}
	}
	return false
}

func waitForBackup(stdout, stderr io.Writer, cl client.Client, ns, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	startedAt := time.Now()
	last := v1alpha1.BackupPhase("")
	consecutiveErrors := 0

	for {
		var b v1alpha1.MysqlBackup
		getCtx, getCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.Get(getCtx, client.ObjectKey{Namespace: ns, Name: name}, &b)
		getCancel()
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("MysqlBackup %s/%s disappeared during wait", ns, name)
			}
			if ctx.Err() != nil {
				return fmt.Errorf("timed out after %s waiting for MysqlBackup %s (last phase: %s)", timeout, name, last)
			}
			consecutiveErrors++
			if consecutiveErrors >= maxWaitGetFailures {
				return fmt.Errorf("get MysqlBackup during wait: %w (gave up after %d consecutive failures)", err, consecutiveErrors)
			}
			fmt.Fprintf(stderr, "warning: get MysqlBackup %s/%s failed: %v; retrying in 5s\n", ns, name, err)
			select {
			case <-ctx.Done():
				return fmt.Errorf("timed out after %s waiting for MysqlBackup %s (last phase: %s)", timeout, name, last)
			case <-tick.C:
				continue
			}
		}
		consecutiveErrors = 0
		if b.Status.Phase != last {
			elapsed := time.Since(startedAt).Round(time.Second)
			fmt.Fprintf(stdout, "[%s] phase: %s — %s\n", elapsed, emptyDash(string(b.Status.Phase)), b.Status.Message)
			last = b.Status.Phase
		}
		switch b.Status.Phase {
		case v1alpha1.BackupPhaseSucceeded:
			fmt.Fprintf(stdout, "Backup succeeded: %s (size: %s)\n", emptyDash(b.Status.Location), emptyDash(b.Status.Size))
			return nil
		case v1alpha1.BackupPhaseFailed:
			fmt.Fprintf(stderr, "Backup failed: %s\n", b.Status.Message)
			return fmt.Errorf("backup %s reached Failed", name)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s waiting for MysqlBackup %s (last phase: %s)", timeout, name, last)
		case <-tick.C:
		}
	}
}
