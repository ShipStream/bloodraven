package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	authorizationv1 "k8s.io/api/authorization/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/internal/storagemigration"
)

const migrateStorageUsage = `Rewrite every Bloodraven object at the storage version of the installed CRDs.

Usage:
  kubectl bloodraven migrate-storage [flags]

Run it once after an upgrade whose CRDs change the storage version
(shipstream.io/v1alpha1 to v1beta1 in v1.8.0), with the CRDs applied and the
new operator running. It works on every namespace:

  1. checks that you hold the permissions it needs, and lists the ones you
     lack;
  2. checks that every Bloodraven CRD stores objects at the new version;
  3. checks every MysqlFailoverGroup, MysqlDatabase, MysqlBackup,
     MysqlBackupVerification and MysqlStandbyCluster against the installed
     schema, and lists each one that does not conform;
  4. writes each object back unchanged, which makes the API server store it
     at the new version;
  5. sets status.storedVersions of each CRD to the new version.

Nothing is written unless steps 1 to 3 pass for the whole cluster. Objects
are never deleted or recreated, and MySQL is not touched. Running it again
is safe: a kind that is already migrated is skipped.

The account needs get, list and update on the five kinds in every
namespace, get on their CustomResourceDefinitions and update on
customresourcedefinitions/status. examples/storage-migrator-clusterrole.yaml
is a ClusterRole with exactly that.

Flags:
  --dry-run              make every check, report what would be done, write nothing
  --kubeconfig string    path to kubeconfig
  --context string       kubeconfig context
`

func runMigrateStorage(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("migrate-storage", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, migrateStorageUsage) }
	var (
		cf     commonFlags
		dryRun bool
	)
	registerCommonFlags(fs, &cf)
	fs.BoolVar(&dryRun, "dry-run", false, "make every check and write nothing")
	positional, err := parseFlagsAndArgs(fs, args)
	if err != nil {
		return err
	}
	if len(positional) > 0 {
		return fmt.Errorf("migrate-storage takes no arguments (it works on every namespace); got %q", positional)
	}

	cfg, _, err := cf.resolve()
	if err != nil {
		return err
	}
	scheme := k8sruntime.NewScheme()
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))
	utilruntime.Must(authorizationv1.AddToScheme(scheme))
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	// An interrupt stops between two objects. What was rewritten stays
	// rewritten, and status.storedVersions is only changed at the end.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return storagemigration.Run(ctx, storagemigration.Options{Client: cl, Dynamic: dyn, DryRun: dryRun, Out: stdout})
}
