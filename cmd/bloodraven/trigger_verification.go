package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// runTriggerVerification creates a MysqlBackupVerification CR in-cluster
// and exits. Invoked by operator-managed verification CronJobs via
// `bloodraven trigger-verification ...`. Runs with the operator's
// ServiceAccount so it inherits the RBAC to create the CR.
func runTriggerVerification(args []string) {
	fs := flag.NewFlagSet("trigger-verification", flag.ExitOnError)
	var (
		group     = fs.String("group", "", "MysqlFailoverGroup name (required)")
		profile   = fs.String("profile", "", "backup profile name (required)")
		namespace = fs.String("namespace", "", "namespace (defaults to POD_NAMESPACE or 'default')")
	)
	_ = fs.Parse(args)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *group == "" || *profile == "" {
		logger.Error("--group and --profile are required")
		os.Exit(2)
	}
	ns := *namespace
	if ns == "" {
		ns = os.Getenv("POD_NAMESPACE")
	}
	if ns == "" {
		ns = "default"
	}

	scheme := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	cfg := ctrl.GetConfigOrDie()
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		logger.Error("build client", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Fetch the group's VerificationSpec so the scheduled CR inherits
	// spec.pointInTime / spec.sanityCheck. The Phase 1 reconciler was
	// happy to leave these unset, but Phase 2 uses them to drive PITR
	// replay and sanity-check evaluation.
	var fg v1alpha1.MysqlFailoverGroup
	if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: *group}, &fg); err != nil {
		logger.Error("get failover group", "error", err, "group", *group)
		os.Exit(1)
	}
	var verifSpec *v1alpha1.VerificationSpec
	if fg.Spec.Backup != nil {
		for i := range fg.Spec.Backup.Profiles {
			if fg.Spec.Backup.Profiles[i].Name == *profile {
				verifSpec = fg.Spec.Backup.Profiles[i].Verification
				break
			}
		}
	}

	labels := map[string]string{
		"shipstream.io/failover-group": *group,
		"shipstream.io/backup-profile": *profile,
		"app.kubernetes.io/managed-by": "bloodraven",
	}

	verify := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s-verify-", *group, *profile),
			Namespace:    ns,
			Labels:       labels,
		},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: *group},
			ProfileName:      *profile,
			TriggeredBy:      "schedule",
		},
	}
	if verifSpec != nil {
		verify.Spec.KeepOnFailure = verifSpec.KeepOnFailure
		verify.Spec.TTLSecondsAfterFinished = verifSpec.TTLSecondsAfterFinished
		if verifSpec.Storage != nil {
			verify.Spec.Storage = verifSpec.Storage.DeepCopy()
		}
		if verifSpec.PointInTime != nil {
			verify.Spec.PointInTime = verifSpec.PointInTime.DeepCopy()
		}
		if verifSpec.SanityCheck != nil {
			verify.Spec.SanityCheck = verifSpec.SanityCheck.DeepCopy()
		}
	}

	if err := cl.Create(ctx, verify); err != nil {
		logger.Error("create mysqlbackupverification", "error", err, "group", *group, "profile", *profile)
		os.Exit(1)
	}
	logger.Info("created mysqlbackupverification", "name", verify.Name, "namespace", verify.Namespace)
}
