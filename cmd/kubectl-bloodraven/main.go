// Command kubectl-bloodraven is the `kubectl bloodraven` plugin: a
// thin, deterministic wrapper over the operator's CR surface so
// operators don't have to memorise annotation grammars or hand-craft
// MysqlBackup CRs.
//
// The plugin only writes the resources the operator already reads
// (annotations on MysqlFailoverGroup, MysqlBackup, and
// MysqlBackupVerification CRs). It never talks to MySQL directly,
// which keeps the day-2 surface area honest and lets RBAC for the
// plugin be exactly what an admin already has on the failover-group
// API.
//
// Each subcommand uses its own *flag.FlagSet (no third-party CLI
// framework) so the binary stays small and the help output is easy
// to reason about. Subcommands write to stdout for human-readable
// output and stderr for errors.
package main

import (
	"flag"
	"fmt"
	"os"
)

const usage = `kubectl-bloodraven manages Bloodraven MysqlFailoverGroup, MysqlBackup,
and MysqlBackupVerification resources.

Usage:
  kubectl bloodraven <command> [options]

Commands:
  status         Show health for one or every MysqlFailoverGroup
  promote        Trigger a planned (graceful) failover via annotation
  reclone        Trigger a re-clone of a divergent site via annotation
  backup         Create an on-demand MysqlBackup CR for a profile
  verify-backup  Create an on-demand MysqlBackupVerification CR
  version        Print plugin version metadata
  help           Show this help text

Global flags (accepted by every command):
  --kubeconfig string   Path to kubeconfig (defaults to $KUBECONFIG or ~/.kube/config)
  --context string      Kubeconfig context to use
  --namespace, -n       Namespace (defaults to the kubeconfig context's namespace, or "default")
  --output, -o string   "table" (default) or "json" / "yaml" / "wide" on commands that support it

Run "kubectl bloodraven <command> --help" for command-specific options.
`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		// flag.ErrHelp is the sentinel `flag` returns when -h/--help is
		// passed against a flag.ContinueOnError FlagSet. We've already
		// printed the per-command usage at that point, so swallow the
		// error and exit 0 like every other CLI does.
		if err == flag.ErrHelp {
			return
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run is the testable entry point: it dispatches on the first argument
// and lets each subcommand parse its own flags against the remainder.
func run(args []string, stdout, stderr *os.File) error {
	if len(args) == 0 {
		fmt.Fprint(stdout, usage)
		return nil
	}
	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	case "version":
		return runVersion(args[1:], stdout)
	case "status":
		return runStatus(args[1:], stdout, stderr)
	case "promote":
		return runPromote(args[1:], stdout, stderr)
	case "reclone":
		return runReclone(args[1:], stdout, stderr)
	case "backup":
		return runBackup(args[1:], stdout, stderr)
	case "verify-backup":
		return runVerifyBackup(args[1:], stdout, stderr)
	default:
		fmt.Fprintln(stderr, "unknown command:", args[0])
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}
