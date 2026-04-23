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
	"github.com/shipstream/bloodraven/internal/util"
)

// runTriggerBackup creates a MysqlBackup CR in-cluster and exits. It is
// invoked by the schedule CronJob pods via `bloodraven trigger-backup ...`.
// The command runs with the operator's ServiceAccount, so it inherits the
// RBAC to create MysqlBackup resources.
func runTriggerBackup(args []string) {
	fs := flag.NewFlagSet("trigger-backup", flag.ExitOnError)
	var (
		group     = fs.String("group", "", "MysqlFailoverGroup name (required)")
		profile   = fs.String("profile", "", "backup profile name (required)")
		schedule  = fs.String("schedule", "", "schedule name (optional; used for labelling)")
		namespace = fs.String("namespace", "", "namespace (defaults to POD_NAMESPACE or 'default')")
	)
	_ = fs.Parse(args)

	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo)

	if *group == "" || *profile == "" {
		logger.Error("missing required flags", "error", "--group and --profile must be set")
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

	trigger := "manual"
	if *schedule != "" {
		trigger = "schedule/" + *schedule
	}

	labels := map[string]string{
		"shipstream.io/failover-group":  *group,
		"shipstream.io/backup-profile":  *profile,
		"app.kubernetes.io/managed-by":  "bloodraven",
	}
	if *schedule != "" {
		labels["shipstream.io/backup-schedule"] = *schedule
	}

	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-%s-", *group, *profile),
			Namespace:    ns,
			Labels:       labels,
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: *group},
			ProfileName:      *profile,
			TriggeredBy:      trigger,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Create(ctx, backup); err != nil {
		logger.Error("create mysqlbackup", "error", err, "group", *group, "profile", *profile)
		os.Exit(1)
	}
	logger.Info("created mysqlbackup", "name", backup.Name, "namespace", backup.Namespace, "trigger", trigger)
}
