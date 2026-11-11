package controller

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// mysqlDataDir is where the data volume is mounted in the mysql
	// container. --log-bin and the relay log flags name the same path.
	mysqlDataDir = "/var/lib/mysql"

	// The files MySQL keeps in <datadir>/#clone while an in-place CLONE
	// INSTANCE is staged. They are MySQL-internal names (InnoDB's clone
	// code); TestMySQLLauncherNative runs the rendered launcher against the
	// real image through every state below, so an upstream rename fails CI
	// instead of silently disabling the rotation.
	//
	// mysqlCloneReplaceFiles lists the cloned files that replace the
	// recipient's own. It appears during the clone's file copy, which
	// follows the step that drops the recipient's data, and the next
	// mysqld start consumes it: it installs the listed files, or discards
	// them when the clone did not finish.
	//
	// mysqlCloneStatusError exists from the start of a clone until it
	// completes. A mysqld that starts with it present rolls the clone
	// back. mysqlCloneStatusInProgress is InnoDB's other marker of an
	// unfinished clone. 9.7 writes it in none of the states the native
	// test produces; the launcher honours it so that a server that does is
	// never read as a finished clone.
	mysqlCloneDir              = "#clone"
	mysqlCloneReplaceFiles     = "#replace_files"
	mysqlCloneStatusError      = "#status_error"
	mysqlCloneStatusInProgress = "#status_in_progress"

	// mysqlLauncherLogPrefix starts every line the launcher writes to the
	// mysql container's stderr.
	mysqlLauncherLogPrefix = "bloodraven-launcher:"
)

// mysqlImageEntrypoints are the image entrypoints the launcher hands off
// to, in order: the Docker Official Image's, then the one Oracle's
// community-server image ships. A custom spec.image must provide one of
// them (or a docker-entrypoint.sh on PATH): the launcher replaces the
// image's ENTRYPOINT, and Kubernetes has no way to wrap an entrypoint
// without naming it.
var mysqlImageEntrypoints = []string{
	"/usr/local/bin/docker-entrypoint.sh",
	"/entrypoint.sh",
}

// mysqlServerArgs returns the mysqld flags of a site's mysql container
// (without the binary name).
func mysqlServerArgs(serverID uint32, tls bool) []string {
	args := []string{
		fmt.Sprintf("--server-id=%d", serverID),
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		"--log-bin=" + mysqlDataDir + "/mysql-bin",
		"--relay-log=" + mysqlDataDir + "/mysql-relay-bin",
		"--relay-log-index=" + mysqlDataDir + "/mysql-relay-bin.index",
		"--log-replica-updates=ON",
		"--skip-replica-start=ON",
		"--plugin-load-add=mysql_clone.so",
		// Fail-closed boot: mysqld never accepts application writes
		// before the operator (or the sidecar, when the operator names
		// this site active) lifts the fence. Without it, a restarted
		// MySQL is writable until the sidecar's next tick: an old
		// primary next to the active one, or a reader whose datadir was
		// replaced, reads as a second writable site. read_only, not
		// super_read_only: the image's first-boot init runs its SQL as
		// root through a server started with these same flags, and
		// super_read_only fails it with ERROR 1290. A command-line flag
		// also outranks a spec.mysqlConf override.
		"--read-only=ON",
	}
	if tls {
		// Keep the server-side TLS contract on the mysqld command line as
		// well as in bloodraven.cnf. The sidecar verifies the per-site
		// Service SAN from its first connection; allowing mysqld to fall
		// back to its auto-generated server-cert.pem fails every sidecar
		// query, leaving the pod unready and the sidecar unable to fence.
		args = append(args,
			"--ssl-ca=/etc/mysql/tls/ca.crt",
			"--ssl-cert=/etc/mysql/tls/tls.crt",
			"--ssl-key=/etc/mysql/tls/tls.key",
			"--require-secure-transport=ON",
		)
	}
	return args
}

// mysqlLauncher returns the mysql container's Command and Args: a bash
// script that prepares the container and then execs the image's own
// entrypoint with "mysqld" and mysqlArgs (the mysqld flags, without the
// binary name), so the entrypoint's config validation, uid switch,
// first-boot initialization and final exec behave as they do for a plain
// `docker run mysql:… --flag`.
//
// The script has three parts:
//
//  1. Server identity (serverUUIDRotationScript): on the start that
//     installs a finished CLONE, remove auto.cnf so mysqld generates a new
//     server_uuid.
//  2. With spec.encryptionAtRest: the keyring component files and the
//     private mysqld copy (encryptionMysqlLauncher).
//  3. The handoff to the image entrypoint.
//
// Part 1 runs first and cannot fail the container; a failure in part 2
// leaves the clone staged, and the next start repeats part 1.
func mysqlLauncher(fg *v1alpha1.MysqlFailoverGroup, mysqlArgs []string) (command, args []string) {
	keyringFile := ""
	if fg.Spec.EncryptionEnabled() {
		keyringFile = fg.Spec.KeyringDataFilePath()
	}
	script := "set -euo pipefail\n" + serverUUIDRotationScript(mysqlDataDir, keyringFile)
	args = []string{"mysqld"}
	if fg.Spec.EncryptionEnabled() {
		steps, prefix := encryptionMysqlLauncher(fg)
		script += steps
		args = append(args, prefix...)
	}
	script += mysqlEntrypointHandoffScript()
	// bash -ec '<script>' mysqld --flag…  →  $0=mysqld, $1=--flag…
	return []string{"/bin/bash", "-ec", script}, append(args, mysqlArgs...)
}

// serverUUIDRotationScript is the launcher step that gives a CLONE
// recipient a new server_uuid (issue #265).
//
// CLONE INSTANCE does not copy auto.cnf, so a recipient returns from a
// clone with the server_uuid it had before. Its GTID state is the donor's:
// whatever the recipient committed under its own uuid and the donor never
// received is gone from gtid_executed. Promoted again, the recipient would
// number new transactions from the donor's high-water mark for that uuid
// and reissue GTIDs that a PITR archive, a replica or a backup already
// holds for different transactions. A new server_uuid per clone makes that
// impossible: the recipient's next own transaction is <new uuid>:1.
//
// Nothing the operator owns runs between CLONE and the restart: mysqld
// restarts itself (in a container it exits with error 3707 and the kubelet
// restarts the container, without the init containers). So the decision is
// made here, from what the clone left in the data directory, with the rule
// mysqld itself applies at startup:
//
//   - #clone/#replace_files without #clone/#status_error: the clone
//     finished and this start installs it. Remove auto.cnf.
//   - both present: the clone was interrupted (mysqld killed mid-copy) and
//     this start rolls it back. Keep auto.cnf: the outcome is a failed
//     clone, which the operator reports and retries; the retry rotates.
//   - neither: an ordinary start, or the first boot of an empty volume.
//
// mysqld consumes #replace_files during the same start, so the rule needs
// no marker of its own and holds at every crash point. A crash before the
// unlink leaves the clone staged, and the next start repeats the step.
// After the unlink the old server_uuid is gone for good: mysqld writes a
// new auto.cnf early in its start, before it installs the clone, so a
// crash between the two makes the next start remove that one as well and
// generate another; no transaction was ever committed under it. The clone
// status files that persist (#view_status, #view_progress,
// #status_recovery) are not consulted, so a later ordinary restart never
// rotates.
//
// With encryption at rest, keyringFile is the keyring data file ("" without
// it), and a finished clone found while that file is read-only is not
// rotated. The operator unseals a site before it clones into it, so the
// start that installs a clone normally has a writable keyring; a sealed
// pod finds a staged clone only when that restart did not happen before
// the site was sealed again. A server with a new server_uuid stores a new
// binary log encryption key when it starts
// (MySQLReplicationKey_<server_uuid>), and with a read-only keyring mysqld
// aborts there (MY-013288, verified on 9.7): keeping the old identity is
// what lets the site start.
//
// The step fails safe. If auto.cnf cannot be removed, #clone cannot be
// inspected or the keyring is sealed, the server starts with its old
// identity and the launcher says so on stderr; the operator compares
// @@server_uuid across every clone it runs and reports an unchanged one as
// CloneServerUUIDNotRotated.
//
// datadir is the data directory (mysqlDataDir in a pod; tests run the step
// against scratch paths). Both paths are written into the script as
// single-quoted words, so nothing in them is expanded or executed.
func serverUUIDRotationScript(datadir, keyringFile string) string {
	return fmt.Sprintf(`log() { echo "%[1]s $*" >&2; }
datadir=%[2]s
keyring=%[7]s
clonedir="$datadir/%[3]s"
if [ -d "$clonedir" ] && { [ ! -r "$clonedir" ] || [ ! -x "$clonedir" ]; }; then
  log "WARNING: cannot inspect $clonedir as uid $(id -u); if a clone is staged there, server_uuid is NOT rotated"
elif [ -e "$clonedir/%[4]s" ]; then
  previous=unknown
  if [ -r "$datadir/auto.cnf" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
      case "$line" in server-uuid=*|server_uuid=*) previous=${line#*=} ;; esac
    done < "$datadir/auto.cnf"
  fi
  if [ -e "$clonedir/%[5]s" ] || [ -e "$clonedir/%[6]s" ]; then
    log "an interrupted clone is staged in $clonedir; mysqld rolls it back and server_uuid stays $previous"
  elif [ -n "$keyring" ] && [ -e "$keyring" ] && [ ! -w "$keyring" ]; then
    log "WARNING: a finished clone is staged in $clonedir but the keyring $keyring is read-only (the site is sealed), and a new server_uuid needs a new binary log encryption key there; server_uuid is NOT rotated (stays $previous)"
  elif rm -f -- "$datadir/auto.cnf" 2>/dev/null && [ ! -e "$datadir/auto.cnf" ]; then
    sync "$datadir" 2>/dev/null || true
    log "a finished clone is staged in $clonedir; removed auto.cnf so mysqld generates a new server_uuid (previous: $previous)"
  else
    log "WARNING: a finished clone is staged in $clonedir but $datadir/auto.cnf could not be removed as uid $(id -u); server_uuid is NOT rotated (stays $previous)"
  fi
fi
`,
		mysqlLauncherLogPrefix,
		shellSingleQuote(datadir),
		mysqlCloneDir,
		mysqlCloneReplaceFiles,
		mysqlCloneStatusError, mysqlCloneStatusInProgress,
		shellSingleQuote(keyringFile),
	)
}

// shellSingleQuote returns s as one single-quoted shell word. Inside single
// quotes the shell expands nothing; a single quote in s closes the word,
// is added escaped, and reopens it.
func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// mysqlEntrypointHandoffScript is the launcher's last step: exec the image
// entrypoint with the launcher's own argv ("mysqld" and the flags). An
// image without one fails the container with exit code 127 and a line
// that names what was looked for; falling back to mysqld itself would
// skip the entrypoint's first-boot initialization and uid switch.
func mysqlEntrypointHandoffScript() string {
	return mysqlEntrypointHandoffScriptFor(mysqlImageEntrypoints)
}

// mysqlEntrypointHandoffScriptFor is mysqlEntrypointHandoffScript for the
// given entrypoint paths (tests hand it scratch paths). The paths are
// written into the script unquoted: they must not contain whitespace or
// shell metacharacters.
func mysqlEntrypointHandoffScriptFor(candidates []string) string {
	return fmt.Sprintf(`entrypoint=
for candidate in %[1]s; do
  if [ -x "$candidate" ]; then
    entrypoint=$candidate
    break
  fi
done
if [ -z "$entrypoint" ]; then
  entrypoint=$(command -v docker-entrypoint.sh 2>/dev/null) || entrypoint=
fi
if [ -z "$entrypoint" ]; then
  log "no MySQL image entrypoint found (looked for %[1]s, then docker-entrypoint.sh on PATH); spec.image must provide one"
  exit 127
fi
exec "$entrypoint" "$0" "$@"
`, strings.Join(candidates, " "))
}
