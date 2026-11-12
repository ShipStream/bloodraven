package dragonfly

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestDragonflyNativeKillClients is opt-in
// (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1, docker; about 10 seconds). It runs a
// real master with a replica attached and three client connections, and
// checks what KillClients rests on:
//
//   - the server refuses `CLIENT KILL TYPE NORMAL` (the form the operator
//     sent before), with a reply that is a refusal, not a transport error;
//   - KillClients closes the three client connections, a subscriber among
//     them, and reports three;
//   - it does not close its own connection;
//   - it does not close the replica's links: the replica stays linked and
//     receives what is written afterwards.
//
// The image is BLOODRAVEN_DRAGONFLY_NATIVE_IMAGE, by default the supported
// floor. This is the test that fails when a Dragonfly release renames the
// replication connections or changes `CLIENT LIST`.
func TestDragonflyNativeKillClients(t *testing.T) {
	if os.Getenv("BLOODRAVEN_DRAGONFLY_NATIVE_TEST") != "1" {
		t.Skip("opt-in native Dragonfly test (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1, docker)")
	}
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal("docker is required")
	}
	image := os.Getenv("BLOODRAVEN_DRAGONFLY_NATIVE_IMAGE")
	if image == "" {
		image = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0"
	}
	run := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(docker, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return strings.TrimSpace(string(out))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	network, master, replica := "brdfk-net-"+suffix, "brdfk-master-"+suffix, "brdfk-replica-"+suffix
	run("network", "create", network)
	t.Cleanup(func() {
		_ = exec.Command(docker, "rm", "-f", master, replica).Run()
		_ = exec.Command(docker, "network", "rm", network).Run()
	})
	flags := []string{"--port=6379", "--bind=0.0.0.0", "--break_replication_on_master_restart", "--proactor_threads=2"}
	for _, name := range []string{master, replica} {
		run(append([]string{"run", "-d", "--name", name, "--network", network, "-p", "127.0.0.1::6379", image}, flags...)...)
	}
	addrOf := func(name string) string { return strings.SplitN(run("port", name, "6379/tcp"), "\n", 2)[0] }
	masterAddr, replicaAddr := addrOf(master), addrOf(replica)
	dial := func(addr string) *Client {
		t.Helper()
		var lastErr error
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
			c, err := New(ctx, Config{Addr: addr})
			if err != nil {
				lastErr = err
				continue
			}
			if err := c.Ping(ctx); err != nil {
				lastErr = err
				_ = c.Close()
				continue
			}
			return c
		}
		t.Fatalf("dial %s: %v", addr, lastErr)
		return nil
	}
	replicaState := func() (ReplicationInfo, int64) {
		t.Helper()
		c := dial(replicaAddr)
		defer func() { _ = c.Close() }()
		info, err := c.InfoReplication(ctx)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := c.KeyCount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return info, keys
	}
	waitReplica := func(what string, keys int64) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			info, n := replicaState()
			if info.MasterLinkStatus == "up" && !info.MasterSyncInProgress && n == keys {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: replica reports %+v with %d keys, want linked with %d", what, info, n, keys)
			}
		}
	}

	operator := dial(masterAddr)
	defer func() { _ = operator.Close() }()
	if err := operator.exec(ctx, "SET", "k:0", "v"); err != nil {
		t.Fatal(err)
	}
	rc := dial(replicaAddr)
	if err := rc.ReplicaOf(ctx, master, 6379); err != nil {
		t.Fatalf("REPLICAOF: %v", err)
	}
	_ = rc.Close()
	waitReplica("first sync", 1)

	// Three application clients: a plain one, a named one, a subscriber.
	plain, named, subscriber := dial(masterAddr), dial(masterAddr), dial(masterAddr)
	defer func() { _ = plain.Close(); _ = named.Close(); _ = subscriber.Close() }()
	if err := named.exec(ctx, "CLIENT", "SETNAME", "app1"); err != nil {
		t.Fatal(err)
	}
	// The reply to SUBSCRIBE is an array, which the string reader refuses;
	// the connection is subscribed all the same.
	_ = subscriber.exec(ctx, "SUBSCRIBE", "ch1")

	// The form the operator sent before is not one the server has.
	typeErr := operator.exec(ctx, "CLIENT", "KILL", "TYPE", "NORMAL")
	if !IsCommandRefused(typeErr) {
		t.Fatalf("CLIENT KILL TYPE NORMAL: %v; want the server's refusal (it has no TYPE filter)", typeErr)
	}
	t.Logf("CLIENT KILL TYPE NORMAL: %v", typeErr)
	list, err := operator.execReply(ctx, "CLIENT", "LIST")
	if err != nil {
		t.Fatalf("CLIENT LIST: %v", err)
	}
	if n := strings.Count(list, "name=repl_ctrl_") + strings.Count(list, "name=repl_flow_"); n < 2 {
		t.Fatalf("CLIENT LIST shows %d replication connections named repl_ctrl_/repl_flow_, want the replica's links:\n%s", n, list)
	}

	killed, err := operator.KillClients(ctx)
	if err != nil || killed != 3 {
		t.Fatalf("KillClients = %d, %v; want the 3 client connections closed\n%s", killed, err, list)
	}
	for name, c := range map[string]*Client{"plain": plain, "named": named} {
		if err := c.Ping(ctx); err == nil {
			t.Fatalf("the %s client still answers after KillClients", name)
		}
	}
	if err := operator.Ping(ctx); err != nil {
		t.Fatalf("KillClients closed its own connection: %v", err)
	}
	after, err := operator.execReply(ctx, "CLIENT", "LIST")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(after, "name=app1") || !strings.Contains(after, "name=repl_ctrl_") {
		t.Fatalf("CLIENT LIST after KillClients, want no app1 and the replication links still there:\n%s", after)
	}
	// The replica was not cut off: it gets what is written now.
	if err := operator.exec(ctx, "SET", "k:1", "v"); err != nil {
		t.Fatal(err)
	}
	waitReplica("after KillClients", 2)
	// Nothing left to close but itself.
	if killed, err := operator.KillClients(ctx); err != nil || killed != 0 {
		t.Fatalf("second KillClients = %d, %v; want 0", killed, err)
	}
}
