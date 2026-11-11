package controller

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestMySQLLauncherNative is opt-in (BLOODRAVEN_MYSQL_NATIVE_TEST=1, docker;
// CI job test-pitr-native). It starts two servers of the default MySQL
// image through the launcher mysqlLauncher renders, the way the kubelet
// starts the mysql container (Command replaces the image ENTRYPOINT, Args
// its CMD, the data directory is a volume that outlives the container),
// and drives the recipient through every state the server_uuid rotation
// has to tell apart:
//
//   - first boot of an empty volume (the image's initialization runs);
//   - an ordinary restart (server_uuid unchanged);
//   - an in-place CLONE INSTANCE and the restart that installs it (a new
//     server_uuid, the donor's data and GTID state, replication from the
//     donor with SOURCE_AUTO_POSITION, and the first transaction the
//     recipient commits afterwards is <new uuid>:1, not the next number
//     of the uuid whose history the clone discarded);
//   - an ordinary restart after the clone, with the clone's status files
//     still in #clone (unchanged);
//   - a clone killed mid-copy and the restart that rolls it back
//     (unchanged, by the launcher's own log line);
//   - two more clones in a row (a new server_uuid each time);
//   - the donor (unchanged throughout).
//
// It also asserts the files MySQL leaves in #clone between a clone and its
// restart, because the launcher's rule reads them and their names are
// MySQL-internal: an upstream change fails here with the file listing,
// not as a silently disabled rotation.
//
// Three renderings run it: the image default (the launcher runs as root
// and the entrypoint drops to mysql), a pod pinned to the mysql uid by
// spec.containerSecurityContext with every capability dropped (the
// launcher runs as that uid), and
// spec.encryptionAtRest with the rendered my.cnf, keyring component files
// and keyring-init script (every table, the redo and undo logs and the
// binary log encrypted). The encrypted run goes on to the keyring states
// (runSealed): a recloned site sealed again, and a clone that a sealed pod
// finds staged, where the launcher must not rotate.
func TestMySQLLauncherNative(t *testing.T) {
	if os.Getenv("BLOODRAVEN_MYSQL_NATIVE_TEST") != "1" {
		t.Skip("opt-in native MySQL launcher test (BLOODRAVEN_MYSQL_NATIVE_TEST=1, docker)")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("docker is required")
	}
	image := os.Getenv("BLOODRAVEN_MYSQL_NATIVE_IMAGE")
	if image == "" {
		image = defaultMySQLImage
	}
	encrypted := newTestFG()
	encrypted.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	for _, tc := range []struct {
		name string
		user string // docker --user; "" keeps the image default (root)
		fg   *v1alpha1.MysqlFailoverGroup
	}{
		{name: "image default user", fg: newTestFG()},
		{name: "runAsUser mysql", user: fmt.Sprintf("%d:%d", mysqlDataUID, mysqlDataGID), fg: newTestFG()},
		{name: "encryption at rest", fg: encrypted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newLauncherNativeEnv(t, docker, image, tc.user, tc.fg)
			env.run()
		})
	}
}

const (
	launcherNativeRootPassword = "launcher-native-root"
	launcherNativeReplPassword = "launcher-native-repl"
)

type launcherNativeEnv struct {
	t       *testing.T
	docker  string
	image   string
	user    string
	fg      *v1alpha1.MysqlFailoverGroup
	network string
}

type launcherNativeServer struct {
	env      *launcherNativeEnv
	name     string // container name and network alias
	volume   string
	serverID uint32
	// Encryption only: the keyring volume, and the host directories
	// mounted as conf.d and as the launcher's keyring component source.
	keyringVolume string
	conf          string
	keyringSrc    string
}

func newLauncherNativeEnv(t *testing.T, docker, image, user string, fg *v1alpha1.MysqlFailoverGroup) *launcherNativeEnv {
	t.Helper()
	env := &launcherNativeEnv{t: t, docker: docker, image: image, user: user, fg: fg,
		network: fmt.Sprintf("bloodraven-launcher-native-%d", time.Now().UnixNano())}
	if out, err := exec.Command(docker, "network", "create", env.network).CombinedOutput(); err != nil {
		t.Fatalf("create network: %v %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command(docker, "network", "rm", env.network).Run() })
	return env
}

// start runs a server the way the kubelet runs the mysql container of the
// group (without TLS): mysqlLauncher's Command as the entrypoint and its
// Args as the command, over a fresh named volume. With encryption it also
// gets what the pod's volumes and init containers provide: the rendered
// my.cnf, the keyring component sources and a keyring volume prepared by
// the keyring-init script.
func (e *launcherNativeEnv) start(role string, serverID uint32) *launcherNativeServer {
	e.t.Helper()
	s := &launcherNativeServer{env: e, name: e.network + "-" + role, volume: e.network + "-" + role, serverID: serverID}
	volumes := []string{s.volume}
	if e.fg.Spec.EncryptionEnabled() {
		kr := e.fg.Spec.EffectiveKeyring()
		s.keyringVolume = s.volume + "-keyring"
		volumes = append(volumes, s.keyringVolume)
		// The mounted directories and files must be readable by the
		// container's users, whatever the host's umask is.
		host := e.t.TempDir()
		s.conf = filepath.Join(host, "conf")
		s.keyringSrc = filepath.Join(host, "keyring-src")
		for _, dir := range []string{s.conf, s.keyringSrc} {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				e.t.Fatal(err)
			}
			if err := os.Chmod(dir, 0o755); err != nil {
				e.t.Fatal(err)
			}
		}
		s.writeMounted(s.conf, "bloodraven.cnf", generateMyCnf(e.fg, false, e.fg.Spec.Sites[0]))
		// keyring-init, as the init container runs it for an unsealed
		// site with no escrowed keyring yet.
		if out, err := exec.Command(e.docker, "run", "--rm", "--network", "none", "-v", s.keyringVolume+":"+kr.DataFileDir,
			"--entrypoint", "sh", e.image, "-c", keyringInitScript(false), "keyring-init", e.fg.Spec.KeyringDataFilePath(), kr.DataFileDir, "/nonexistent").CombinedOutput(); err != nil {
			e.t.Fatalf("keyring-init for %s: %v %s", s.name, err, out)
		}
	}
	e.t.Cleanup(func() {
		_ = exec.Command(e.docker, "rm", "-f", "-v", s.name).Run()
		for _, v := range volumes {
			_ = exec.Command(e.docker, "volume", "rm", "-f", v).Run()
		}
	})
	s.runContainer(false)
	return s
}

func (s *launcherNativeServer) writeMounted(dir, name, content string) {
	s.env.t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		s.env.t.Fatal(err)
	}
	if err := os.Chmod(p, 0o644); err != nil {
		s.env.t.Fatal(err)
	}
}

// runContainer creates and starts the server's container on its volumes.
// sealed (encryption only) is the rendering of a site whose keyring is
// escrowed: the component config says read_only and the keyring volume is
// mounted read-only, as the Secret that holds a sealed keyring is.
func (s *launcherNativeServer) runContainer(sealed bool) {
	e := s.env
	e.t.Helper()
	command, args := mysqlLauncher(e.fg, mysqlServerArgs(s.serverID, false))
	run := []string{"run", "-d", "--name", s.name, "--network", e.network,
		"-v", s.volume + ":" + mysqlDataDir, "-e", "MYSQL_ROOT_PASSWORD=" + launcherNativeRootPassword,
		// The test's root logs in over TCP; the Docker Official Image
		// creates root@'%' by default, Oracle's image only on request.
		"-e", "MYSQL_ROOT_HOST=%", "-e", "TZ=UTC"}
	if e.user != "" {
		// The hardened rendering the docs give for the image: a non-root
		// user, no capabilities, no privilege escalation.
		run = append(run, "--user", e.user, "--cap-drop", "ALL", "--security-opt", "no-new-privileges")
	}
	if e.fg.Spec.EncryptionEnabled() {
		for name, content := range keyringConfigMapData(e.fg, sealed) {
			s.writeMounted(s.keyringSrc, name, content)
		}
		keyringMount := s.keyringVolume + ":" + e.fg.Spec.EffectiveKeyring().DataFileDir
		if sealed {
			keyringMount += ":ro"
		}
		run = append(run,
			"-v", s.conf+":/etc/mysql/conf.d:ro",
			"-v", s.keyringSrc+":"+keyringComponentSrcMount+":ro",
			"-v", keyringMount)
	}
	run = append(run, "--entrypoint", command[0], e.image)
	run = append(run, command[1:]...)
	run = append(run, args...)
	if out, err := exec.Command(e.docker, run...).CombinedOutput(); err != nil {
		e.t.Fatalf("start %s: %v %s", s.name, err, out)
	}
}

// recreate replaces the server's container with a new one on the same
// volumes, as a pod roll does (sealing and unsealing a site roll its pod),
// and returns the server_uuid it comes up with. The new container's log
// starts empty.
func (s *launcherNativeServer) recreate(sealed bool) string {
	s.env.t.Helper()
	if s.running() {
		if out, err := exec.Command(s.env.docker, "stop", "-t", "60", s.name).CombinedOutput(); err != nil {
			s.env.t.Fatalf("stop %s: %v %s", s.name, err, out)
		}
	}
	if out, err := exec.Command(s.env.docker, "rm", "-f", s.name).CombinedOutput(); err != nil {
		s.env.t.Fatalf("remove %s: %v %s", s.name, err, out)
	}
	s.runContainer(sealed)
	return s.waitUp()
}

// trySQL runs q as root over TCP. The image's first-boot initialization
// runs a temporary server on the socket only, so a TCP answer is the real
// server's.
func (s *launcherNativeServer) trySQL(q string) (string, error) {
	out, err := exec.Command(s.env.docker, "exec", "-e", "MYSQL_PWD="+launcherNativeRootPassword, s.name,
		"mysql", "-uroot", "-h127.0.0.1", "-P3306", "-N", "-B", "-e", q).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func (s *launcherNativeServer) sql(q string) string {
	s.env.t.Helper()
	out, err := s.trySQL(q)
	if err != nil {
		s.env.t.Fatalf("%s: %s: %v %s", s.name, q, err, out)
	}
	return out
}

// gtidExecuted is @@global.gtid_executed on one line (the server separates
// uuids with a newline, which mysql --batch prints as the two characters
// \n).
func (s *launcherNativeServer) gtidExecuted() string {
	s.env.t.Helper()
	return strings.NewReplacer(`\n`, "", "\n", "").Replace(s.sql("SELECT @@global.gtid_executed"))
}

func (s *launcherNativeServer) running() bool {
	out, err := exec.Command(s.env.docker, "inspect", "-f", "{{.State.Running}}", s.name).CombinedOutput()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

func (s *launcherNativeServer) logs() string {
	out, _ := exec.Command(s.env.docker, "logs", s.name).CombinedOutput()
	return string(out)
}

// launcherLines are the lines the launcher itself logged, over every start
// of the container.
func (s *launcherNativeServer) launcherLines() []string {
	var lines []string
	for _, line := range strings.Split(s.logs(), "\n") {
		if strings.HasPrefix(line, mysqlLauncherLogPrefix) {
			lines = append(lines, line)
		}
	}
	return lines
}

func (s *launcherNativeServer) launcherLineCount(substr string) int {
	n := 0
	for _, line := range s.launcherLines() {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// waitUp waits for the server to answer over TCP and returns its
// server_uuid.
func (s *launcherNativeServer) waitUp() string {
	s.env.t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		out, err := s.trySQL("SELECT @@global.server_uuid")
		if err == nil && len(out) == 36 {
			return out
		}
		if !s.running() {
			s.env.t.Fatalf("%s exited instead of starting:\n%s", s.name, tailLines(s.logs(), 40))
		}
		if time.Now().After(deadline) {
			s.env.t.Fatalf("%s did not start: %v %s\n%s", s.name, err, out, tailLines(s.logs(), 40))
		}
		time.Sleep(time.Second)
	}
}

func (s *launcherNativeServer) waitExited() {
	s.env.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for s.running() {
		if time.Now().After(deadline) {
			s.env.t.Fatalf("%s is still running:\n%s", s.name, tailLines(s.logs(), 40))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// restart is an ordinary restart: a clean shutdown, then the same
// container started again.
func (s *launcherNativeServer) restart() string {
	s.env.t.Helper()
	if out, err := exec.Command(s.env.docker, "restart", "-t", "60", s.name).CombinedOutput(); err != nil {
		s.env.t.Fatalf("restart %s: %v %s", s.name, err, out)
	}
	return s.waitUp()
}

// startAgain starts the exited container again, as the kubelet restarts a
// container whose mysqld exited: same command, same volume, no init
// containers.
func (s *launcherNativeServer) startAgain() string {
	s.env.t.Helper()
	if out, err := exec.Command(s.env.docker, "start", s.name).CombinedOutput(); err != nil {
		s.env.t.Fatalf("start %s: %v %s", s.name, err, out)
	}
	return s.waitUp()
}

// cloneFiles lists <datadir>/#clone through a helper container, so it
// works while the server's container is stopped.
func (s *launcherNativeServer) cloneFiles() map[string]bool {
	s.env.t.Helper()
	out, err := exec.Command(s.env.docker, "run", "--rm", "--network", "none", "-v", s.volume+":/d:ro",
		"--entrypoint", "/bin/bash", s.env.image, "-c", `ls -A "/d/`+mysqlCloneDir+`" 2>/dev/null || true`).CombinedOutput()
	if err != nil {
		s.env.t.Fatalf("list %s/%s: %v %s", s.name, mysqlCloneDir, err, out)
	}
	files := map[string]bool{}
	for _, f := range strings.Fields(string(out)) {
		files[f] = true
	}
	return files
}

// autoCnfUUID is the server-uuid in the volume's auto.cnf.
func (s *launcherNativeServer) autoCnfUUID() string {
	s.env.t.Helper()
	out, err := exec.Command(s.env.docker, "exec", s.name, "cat", mysqlDataDir+"/auto.cnf").CombinedOutput()
	if err != nil {
		s.env.t.Fatalf("read %s auto.cnf: %v %s", s.name, err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "server-uuid="); ok {
			return v
		}
	}
	s.env.t.Fatalf("%s auto.cnf has no server-uuid:\n%s", s.name, out)
	return ""
}

// cloneStatement is the CLONE INSTANCE the operator issues (bootstrap.go).
func (e *launcherNativeEnv) cloneStatement(donor *launcherNativeServer) string {
	return fmt.Sprintf("CLONE INSTANCE FROM 'repl'@'%s':3306 IDENTIFIED BY '%s'", donor.name, launcherNativeReplPassword)
}

// prepareClone is what BootstrapReplica does on the recipient before the
// statement: lift super_read_only (read_only stays ON) and name the donor.
// mibPerSecond throttles the copy; 0 does not.
func (s *launcherNativeServer) prepareClone(donor *launcherNativeServer, mibPerSecond int) {
	s.env.t.Helper()
	s.sql(fmt.Sprintf("SET GLOBAL super_read_only = 0; SET GLOBAL clone_valid_donor_list = '%s:3306'; SET GLOBAL clone_max_data_bandwidth = %d; SET GLOBAL clone_max_network_bandwidth = %d",
		donor.name, mibPerSecond, mibPerSecond))
}

// clone runs an in-place clone of donor on s to completion and waits for
// mysqld to exit. With no supervisor in the container (the Docker Official
// Image) the clone ends in error 3707 and a shutdown, which
// isCloneConnectionDrop treats as the expected end of a clone. An image
// whose entrypoint declares one (Oracle's sets MYSQLD_PARENT_PID) returns
// from the statement and mysqld exits to be restarted. Either way the
// container stops and the kubelet starts it again.
func (s *launcherNativeServer) clone(donor *launcherNativeServer) {
	s.env.t.Helper()
	s.prepareClone(donor, 0)
	if out, err := s.trySQL(s.env.cloneStatement(donor)); err != nil && !isCloneConnectionDrop(fmt.Errorf("%s", out)) {
		s.env.t.Fatalf("%s: CLONE INSTANCE failed: %v %s", s.name, err, out)
	}
	s.waitExited()
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func (e *launcherNativeEnv) run() {
	t := e.t
	donor := e.start("donor", 1)
	recipient := e.start("recipient", 2)

	// First boot of an empty volume: the launcher finds nothing staged and
	// the image entrypoint initializes the data directory. The root login
	// with MYSQL_ROOT_PASSWORD proves the entrypoint's init ran.
	donorUUID := donor.waitUp()
	uuid0 := recipient.waitUp()
	if lines := recipient.launcherLines(); len(lines) != 0 {
		t.Fatalf("the launcher logged on the first boot of an empty volume: %q", lines)
	}
	if got := recipient.autoCnfUUID(); got != uuid0 {
		t.Fatalf("auto.cnf has %s, the server reports %s", got, uuid0)
	}
	if ro := recipient.sql("SELECT @@global.read_only"); ro != "1" {
		t.Fatalf("recipient read_only = %s after first boot, want 1 (the rendered --read-only=ON)", ro)
	}

	encrypted := e.fg.Spec.EncryptionEnabled()
	if encrypted {
		for _, s := range []*launcherNativeServer{donor, recipient} {
			if got := s.sql("SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY = 'Component_status'"); got != "Active" {
				t.Fatalf("%s: keyring component status = %q, want Active (the launcher's encryption steps)", s.name, got)
			}
			if got := s.sql("SELECT CONCAT(@@global.default_table_encryption, @@global.innodb_redo_log_encrypt, @@global.innodb_undo_log_encrypt, @@global.binlog_encryption)"); got != "1111" {
				t.Fatalf("%s: encryption settings = %q, want every one ON (the rendered my.cnf)", s.name, got)
			}
		}
	}

	// An ordinary restart keeps the identity.
	if got := recipient.restart(); got != uuid0 {
		t.Fatalf("server_uuid changed across an ordinary restart: %s -> %s", uuid0, got)
	}

	// The recipient is an old primary: it committed transactions under its
	// own uuid, of which the donor received only :1-3.
	recipient.sql("SET GLOBAL read_only = 0; CREATE DATABASE old_primary; CREATE TABLE old_primary.t (id INT PRIMARY KEY); INSERT INTO old_primary.t VALUES (1), (2); SET GLOBAL read_only = 1")
	donor.sql("SET GLOBAL read_only = 0")
	donor.sql(fmt.Sprintf("CREATE USER 'repl'@'%%' IDENTIFIED BY '%s'; GRANT REPLICATION SLAVE, BACKUP_ADMIN, CLONE_ADMIN ON *.* TO 'repl'@'%%'", launcherNativeReplPassword))
	donor.sql("CREATE DATABASE app; CREATE TABLE app.t (id INT PRIMARY KEY); INSERT INTO app.t VALUES (1), (2), (3)")
	for i := 1; i <= 3; i++ {
		donor.sql(fmt.Sprintf("SET GTID_NEXT = '%s:%d'; BEGIN; COMMIT; SET GTID_NEXT = 'AUTOMATIC'", uuid0, i))
	}
	if encrypted {
		if got := donor.sql("SELECT CREATE_OPTIONS FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 't'"); !strings.Contains(got, "ENCRYPTION='Y'") {
			t.Fatalf("app.t on the donor has options %q, want ENCRYPTION='Y'", got)
		}
	}
	before := recipient.gtidExecuted()
	if !strings.Contains(before, uuid0+":1-") {
		t.Fatalf("recipient gtid_executed = %s, want its own history under %s", before, uuid0)
	}

	// Clone 1. Between the clone and the restart the data directory holds
	// the staged clone: the state the launcher's rule reads.
	recipient.clone(donor)
	staged := recipient.cloneFiles()
	if !staged[mysqlCloneReplaceFiles] || staged[mysqlCloneStatusError] || staged[mysqlCloneStatusInProgress] {
		t.Fatalf("after a finished clone %s holds %v; want %s without %s or %s (the launcher's evidence of a clone to install)",
			mysqlCloneDir, staged, mysqlCloneReplaceFiles, mysqlCloneStatusError, mysqlCloneStatusInProgress)
	}
	uuid1 := recipient.startAgain()
	if uuid1 == uuid0 || uuid1 == donorUUID {
		t.Fatalf("server_uuid after the clone = %s; want a new one (before: %s, donor: %s)\n%s", uuid1, uuid0, donorUUID, tailLines(recipient.logs(), 40))
	}
	if got := recipient.autoCnfUUID(); got != uuid1 {
		t.Fatalf("auto.cnf has %s, the server reports %s", got, uuid1)
	}
	if n := recipient.launcherLineCount("removed auto.cnf"); n != 1 {
		t.Fatalf("launcher rotation lines = %d, want 1: %q", n, recipient.launcherLines())
	}
	if n := recipient.launcherLineCount("previous: " + uuid0); n != 1 {
		t.Fatalf("the rotation line does not name the previous server_uuid %s: %q", uuid0, recipient.launcherLines())
	}
	if st := recipient.sql("SELECT STATE FROM performance_schema.clone_status"); st != "Completed" {
		t.Fatalf("clone_status = %q, want Completed", st)
	}
	if after := recipient.cloneFiles(); after[mysqlCloneReplaceFiles] {
		t.Fatalf("%s survived the start that installed the clone: %v", mysqlCloneReplaceFiles, after)
	}
	// The donor's data and GTID state, none of the recipient's own.
	donorGtid := donor.gtidExecuted()
	if got := recipient.gtidExecuted(); got != donorGtid {
		t.Fatalf("recipient gtid_executed after the clone = %s, want the donor's %s", got, donorGtid)
	}
	if got := recipient.sql("SELECT GROUP_CONCAT(id ORDER BY id) FROM app.t"); got != "1,2,3" {
		t.Fatalf("app.t on the recipient = %q, want 1,2,3", got)
	}
	if got := recipient.sql("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'old_primary'"); got != "0" {
		t.Fatalf("the recipient's own schema survived the clone")
	}
	if ro := recipient.sql("SELECT @@global.read_only"); ro != "1" {
		t.Fatalf("recipient read_only = %s after the clone, want 1", ro)
	}

	// Replication from the donor with auto-position, as SetupReplication
	// configures it.
	recipient.sql(fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST = '%s', SOURCE_PORT = 3306, SOURCE_USER = 'repl', SOURCE_PASSWORD = '%s', SOURCE_AUTO_POSITION = 1, GET_SOURCE_PUBLIC_KEY = 1; START REPLICA",
		donor.name, launcherNativeReplPassword))
	donor.sql("INSERT INTO app.t VALUES (4)")
	deadline := time.Now().Add(time.Minute)
	for recipient.sql("SELECT GROUP_CONCAT(id ORDER BY id) FROM app.t") != "1,2,3,4" {
		if time.Now().After(deadline) {
			t.Fatalf("the recloned recipient did not replicate from the donor:\n%s", recipient.sql("SHOW REPLICA STATUS\\G"))
		}
		time.Sleep(time.Second)
	}

	// Promoted, the recipient numbers its transactions under the new
	// uuid from 1. Without the rotation this INSERT would be <uuid0>:4, a
	// GTID the old primary had already used for another transaction.
	recipient.sql("STOP REPLICA; RESET REPLICA ALL; SET GLOBAL read_only = 0; INSERT INTO app.t VALUES (100); SET GLOBAL read_only = 1")
	promoted := recipient.gtidExecuted()
	if !gtidSetHas(promoted, uuid1, "1") {
		t.Fatalf("gtid_executed after the recloned server's first transaction = %s, want %s:1", promoted, uuid1)
	}
	if !gtidSetHas(promoted, uuid0, "1-3") {
		t.Fatalf("gtid_executed = %s: the old uuid %s must still end at 3 (a reissued GTID would extend it)", promoted, uuid0)
	}

	if encrypted {
		// The recipient's keyring holds the donor's keys, under the
		// donor's server_uuid, and now serves a server with a third one:
		// new encrypted tables and a master key rotation must work.
		recipient.sql("SET GLOBAL read_only = 0; CREATE TABLE app.after_clone (id INT PRIMARY KEY); INSERT INTO app.after_clone VALUES (1); ALTER INSTANCE ROTATE INNODB MASTER KEY; SET GLOBAL read_only = 1")
		if got := recipient.sql("SELECT CREATE_OPTIONS FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'after_clone'"); !strings.Contains(got, "ENCRYPTION='Y'") {
			t.Fatalf("a table created after the clone has options %q, want ENCRYPTION='Y'", got)
		}
	}

	// An ordinary restart after the clone: MySQL's clone status files are
	// still there and do not rotate again.
	if got := recipient.restart(); got != uuid1 {
		t.Fatalf("server_uuid changed across an ordinary restart after a clone: %s -> %s", uuid1, got)
	}
	if left := recipient.cloneFiles(); len(left) == 0 {
		t.Fatalf("%s is empty after the clone; the test expects MySQL to keep its status files there", mysqlCloneDir)
	}
	if n := recipient.launcherLineCount("removed auto.cnf"); n != 1 {
		t.Fatalf("launcher rotation lines = %d after an ordinary restart, want 1: %q", n, recipient.launcherLines())
	}

	// A clone killed mid-copy. The throttle keeps the copy of the system
	// tablespaces running for about a minute.
	recipient.prepareClone(donor, 1)
	cloneDone := make(chan string, 1)
	go func() {
		out, _ := recipient.trySQL(e.cloneStatement(donor))
		cloneDone <- out
	}()
	deadline = time.Now().Add(time.Minute)
	for {
		probe := fmt.Sprintf(`test -e "%[1]s/%[2]s/%[3]s" && test -e "%[1]s/%[2]s/%[4]s"`, mysqlDataDir, mysqlCloneDir, mysqlCloneReplaceFiles, mysqlCloneStatusError)
		if exec.Command(e.docker, "exec", recipient.name, "/bin/bash", "-c", probe).Run() == nil {
			break
		}
		select {
		case out := <-cloneDone:
			t.Fatalf("the throttled clone ended before it could be killed: %s (%s holds %v)", out, mysqlCloneDir, recipient.cloneFiles())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("a running clone never showed %s and %s in %s: %v (the launcher's evidence of an unfinished clone)",
				mysqlCloneReplaceFiles, mysqlCloneStatusError, mysqlCloneDir, recipient.cloneFiles())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if out, err := exec.Command(e.docker, "kill", recipient.name).CombinedOutput(); err != nil {
		t.Fatalf("kill %s: %v %s", recipient.name, err, out)
	}
	<-cloneDone
	recipient.waitExited()
	if killed := recipient.cloneFiles(); !killed[mysqlCloneReplaceFiles] || !killed[mysqlCloneStatusError] {
		t.Fatalf("after a killed clone %s holds %v; want %s and %s", mysqlCloneDir, killed, mysqlCloneReplaceFiles, mysqlCloneStatusError)
	}
	if got := recipient.startAgain(); got != uuid1 {
		t.Fatalf("server_uuid changed across the rollback of a killed clone: %s -> %s", uuid1, got)
	}
	if n := recipient.launcherLineCount("an interrupted clone is staged"); n != 1 {
		t.Fatalf("launcher lines for the interrupted clone = %d, want 1: %q", n, recipient.launcherLines())
	}
	if n := recipient.launcherLineCount("removed auto.cnf"); n != 1 {
		t.Fatalf("launcher rotation lines = %d after a killed clone, want 1: %q", n, recipient.launcherLines())
	}
	// What the operator sees of a killed clone: no data, no Completed
	// status. verifyCloneOutcome fails the bootstrap on it and
	// promotionDataRegression keeps the site from promotion.
	if got := recipient.gtidExecuted(); got != "" {
		t.Fatalf("gtid_executed after a killed clone = %q, want empty (the clone drops the recipient's data first)", got)
	}
	if st := recipient.sql("SELECT STATE FROM performance_schema.clone_status"); st == "Completed" {
		t.Fatalf("clone_status = Completed after a killed clone")
	}
	if rolledBack := recipient.cloneFiles(); rolledBack[mysqlCloneReplaceFiles] || rolledBack[mysqlCloneStatusError] {
		t.Fatalf("the rollback left %v in %s", rolledBack, mysqlCloneDir)
	}

	// The retry, and one more clone straight after it: each rotates.
	seen := map[string]bool{uuid0: true, uuid1: true, donorUUID: true}
	for i := 2; i <= 3; i++ {
		recipient.clone(donor)
		got := recipient.startAgain()
		if seen[got] {
			t.Fatalf("clone %d left server_uuid %s, which an earlier incarnation (or the donor) had", i, got)
		}
		seen[got] = true
		if n := recipient.launcherLineCount("removed auto.cnf"); n != i {
			t.Fatalf("launcher rotation lines = %d after clone %d, want %d: %q", n, i, i, recipient.launcherLines())
		}
		if got, want := recipient.gtidExecuted(), donor.gtidExecuted(); got != want {
			t.Fatalf("recipient gtid_executed after clone %d = %s, want the donor's %s", i, got, want)
		}
		if got := recipient.sql("SELECT GROUP_CONCAT(id ORDER BY id) FROM app.t"); got != "1,2,3,4" {
			t.Fatalf("app.t on the recipient after clone %d = %q, want 1,2,3,4", i, got)
		}
	}

	if encrypted {
		e.runSealed(donor, recipient)
	}

	// A donor is never rotated: it served every clone above and restarts
	// with its identity.
	if got := donor.restart(); got != donorUUID {
		t.Fatalf("the donor's server_uuid changed across a restart: %s -> %s", donorUUID, got)
	}
	if lines := donor.launcherLines(); len(lines) != 0 {
		t.Fatalf("the launcher logged on the donor: %q", lines)
	}
}

// runSealed covers what encryption at rest adds to a clone: the keyring.
// A server with a new server_uuid stores a new binary log encryption key
// (MySQLReplicationKey_<server_uuid>) when it starts.
func (e *launcherNativeEnv) runSealed(donor, recipient *launcherNativeServer) {
	t := e.t
	last := recipient.autoCnfUUID()

	// Sealed, as the operator leaves a recloned site once its keyring is
	// escrowed: the key stored on the clone's restart is in the keyring, so
	// a start on a read-only keyring needs no write, and the binary log is
	// still encrypted with it.
	if got := recipient.recreate(true); got != last {
		t.Fatalf("server_uuid changed across the sealing roll: %s -> %s", last, got)
	}
	if got := recipient.sql("SELECT STATUS_VALUE FROM performance_schema.keyring_component_status WHERE STATUS_KEY = 'Read_only'"); got != "Yes" {
		t.Fatalf("the sealed recipient's keyring read_only = %q, want Yes", got)
	}
	recipient.sql("SET GLOBAL read_only = 0; INSERT INTO app.t VALUES (200); SET GLOBAL read_only = 1")
	if got := recipient.sql("SELECT GROUP_CONCAT(id ORDER BY id) FROM app.t"); got != "1,2,3,4,200" {
		t.Fatalf("app.t on the sealed recipient = %q, want 1,2,3,4,200", got)
	}
	if lines := recipient.launcherLines(); len(lines) != 0 {
		t.Fatalf("the launcher logged on a sealed start with nothing staged: %q", lines)
	}

	// A clone whose restart did not happen before the site was sealed
	// again (the operator unseals before every clone, so this takes a
	// failed wait for the restart): the sealed pod finds the clone
	// staged. Rotating there would abort mysqld, which cannot store the
	// new key (MY-013288), on every start. The launcher keeps the old
	// identity and says so, and the server installs the clone.
	if got := recipient.recreate(false); got != last {
		t.Fatalf("server_uuid changed across the unsealing roll: %s -> %s", last, got)
	}
	recipient.clone(donor)
	if got := recipient.recreate(true); got != last {
		t.Fatalf("a clone installed by a sealed pod left server_uuid %s, want the previous %s kept\n%s", got, last, tailLines(recipient.logs(), 40))
	}
	if n := recipient.launcherLineCount("is read-only (the site is sealed)"); n != 1 || recipient.launcherLineCount("NOT rotated (stays "+last+")") != 1 {
		t.Fatalf("launcher lines for a clone staged under a sealed keyring: %q", recipient.launcherLines())
	}
	if n := recipient.launcherLineCount("removed auto.cnf"); n != 0 {
		t.Fatalf("the launcher rotated under a sealed keyring: %q", recipient.launcherLines())
	}
	if got, want := recipient.gtidExecuted(), donor.gtidExecuted(); got != want {
		t.Fatalf("recipient gtid_executed after the clone installed under a sealed keyring = %s, want the donor's %s", got, want)
	}
	if st := recipient.sql("SELECT STATE FROM performance_schema.clone_status"); st != "Completed" {
		t.Fatalf("clone_status = %q after the clone installed under a sealed keyring, want Completed", st)
	}
}

// gtidSetHas reports whether the GTID set text holds exactly the interval
// list intervals for uuid ("1-3", "1").
func gtidSetHas(set, uuid, intervals string) bool {
	for _, entry := range strings.Split(set, ",") {
		if u, iv, ok := strings.Cut(strings.TrimSpace(entry), ":"); ok && u == uuid {
			return iv == intervals
		}
	}
	return false
}
