package dragonfly

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestDragonflyNativeRestartedMaster is opt-in
// (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1, docker; about 20 seconds). It runs a
// real master and replica with the flag the operator always sets
// (--break_replication_on_master_restart), replaces the master's process
// at the same address the way a recreated pod does, and checks the facts
// the Dragonfly manager's handling of a restarted master rests on:
//
//   - while the master is gone, REPLICAOF to its address is refused and
//     the replica keeps its keys, its role and the replication id it
//     followed;
//   - the new instance has another master_replid and no keys; the replica
//     does not attach to it by itself: it stays a replica with the link
//     down, holding every key and the old id;
//   - REPLTAKEOVER on that replica is refused before the master is asked
//     (TakeoverOutcomeUnknown is false for the reply);
//   - REPLICAOF NO ONE makes the replica a master that still holds every
//     key, and the restarted instance then syncs from it;
//   - a second pair shows what the manager must not do: REPLICAOF of such
//     a replica to the restarted master empties it.
//
// It also checks, on the real server, the reply IsCommandRefused is
// written for: an ACL user without DEBUG asking for the replica position.
func TestDragonflyNativeRestartedMaster(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dialAddr := func(addr string) *Client {
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
	type state struct {
		info ReplicationInfo
		keys int64
	}
	observe := func(addr string) state {
		t.Helper()
		c := dialAddr(addr)
		defer func() { _ = c.Close() }()
		info, err := c.InfoReplication(ctx)
		if err != nil {
			t.Fatalf("INFO replication on %s: %v", addr, err)
		}
		keys, err := c.KeyCount(ctx)
		if err != nil {
			t.Fatalf("INFO keyspace on %s: %v", addr, err)
		}
		return state{info, keys}
	}
	const keys = 300
	flags := []string{"--port=6379", "--bind=0.0.0.0", "--break_replication_on_master_restart", "--proactor_threads=2"}

	// pair starts a master and a replica in sync and returns their
	// addresses and a function that replaces the master's process. The
	// master's address belongs to a holder container whose network
	// namespace the Dragonfly container joins, so that it stays while the
	// process is replaced, as a Service address does for a recreated pod.
	pair := func(tag string) (masterAddr, replicaAddr, masterHost string, replaceMaster func(stopOnly bool)) {
		t.Helper()
		suffix := tag + strconv.FormatInt(time.Now().UnixNano(), 36)
		network, holder, master, replica := "brdfr-net-"+suffix, "brdfr-holder-"+suffix, "brdfr-master-"+suffix, "brdfr-replica-"+suffix
		run("network", "create", network)
		t.Cleanup(func() {
			_ = exec.Command(docker, "rm", "-f", master, replica, holder).Run()
			_ = exec.Command(docker, "network", "rm", network).Run()
		})
		run("run", "-d", "--name", holder, "--network", network, "-p", "127.0.0.1::6379", "--entrypoint", "sleep", image, "infinity")
		startMaster := func() {
			run(append([]string{"run", "-d", "--name", master, "--network", "container:" + holder, image}, flags...)...)
		}
		startMaster()
		run(append([]string{"run", "-d", "--name", replica, "--network", network, "-p", "127.0.0.1::6379", image}, flags...)...)
		addrOf := func(name string) string {
			return strings.SplitN(run("port", name, "6379/tcp"), "\n", 2)[0]
		}
		masterAddr, replicaAddr = addrOf(holder), addrOf(replica)
		mc := dialAddr(masterAddr)
		for i := 0; i < keys; i++ {
			if err := mc.exec(ctx, "SET", fmt.Sprintf("k:%d", i), "v"); err != nil {
				t.Fatalf("SET on the master: %v", err)
			}
		}
		_ = mc.Close()
		rc := dialAddr(replicaAddr)
		if err := rc.ReplicaOf(ctx, holder, 6379); err != nil {
			t.Fatalf("REPLICAOF: %v", err)
		}
		_ = rc.Close()
		for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			s := observe(replicaAddr)
			if s.info.MasterLinkStatus == "up" && !s.info.MasterSyncInProgress && s.keys == keys {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replica did not sync: %+v", s)
			}
		}
		return masterAddr, replicaAddr, holder, func(stopOnly bool) {
			run("rm", "-f", master)
			if !stopOnly {
				startMaster()
			}
		}
	}

	masterAddr, replicaAddr, masterHost, replaceMaster := pair("a")
	before := observe(masterAddr)
	synced := observe(replicaAddr)
	if before.info.MasterReplID == "" || synced.info.MasterReplID != before.info.MasterReplID {
		t.Fatalf("master_replid: master %q, replica %q; want the replica to report its master's id", before.info.MasterReplID, synced.info.MasterReplID)
	}

	// An ACL user without DEBUG: the refusal the sync check must not wait on.
	rc := dialAddr(replicaAddr)
	if err := rc.exec(ctx, "ACL", "SETUSER", "nodebug", "on", ">pw", "+@all", "-debug"); err != nil {
		t.Fatalf("ACL SETUSER: %v", err)
	}
	_ = rc.Close()
	limited, err := New(ctx, Config{Addr: replicaAddr})
	if err != nil {
		t.Fatal(err)
	}
	if err := limited.exec(ctx, "AUTH", "nodebug", "pw"); err != nil {
		t.Fatalf("AUTH nodebug: %v", err)
	}
	_, refusal := limited.execValue(ctx, "DEBUG", "REPLICA", "OFFSET")
	_ = limited.Close()
	if !IsCommandRefused(refusal) {
		t.Fatalf("DEBUG REPLICA OFFSET as a user without DEBUG: %v; want a reply IsCommandRefused recognises", refusal)
	}
	t.Logf("refusal for a user without DEBUG: %v", refusal)

	// The master's process is gone; its address refuses connections.
	replaceMaster(true)
	var down state
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if down = observe(replicaAddr); down.info.MasterLinkStatus == "down" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica still reports its link up 10s after the master was killed: %+v", down.info)
		}
	}
	rc = dialAddr(replicaAddr)
	reissued := rc.ReplicaOf(ctx, masterHost, 6379)
	_ = rc.Close()
	t.Logf("REPLICAOF to the dead master: %v", reissued)
	if reissued == nil {
		t.Fatal("REPLICAOF to a dead master succeeded")
	}
	if s := observe(replicaAddr); s.keys != keys || s.info.Role != "slave" || s.info.MasterReplID != before.info.MasterReplID {
		t.Fatalf("after a refused REPLICAOF the replica reports %+v with %d keys; want a replica with %d keys following %q",
			s.info, s.keys, keys, before.info.MasterReplID)
	}

	// The master is back as a new, empty instance.
	replaceMaster(false)
	restarted := observe(masterAddr)
	if restarted.info.Role != "master" || restarted.keys != 0 || restarted.info.MasterReplID == "" || restarted.info.MasterReplID == before.info.MasterReplID {
		t.Fatalf("restarted master reports %+v with %d keys; want an empty master with a new master_replid (was %q)",
			restarted.info, restarted.keys, before.info.MasterReplID)
	}
	// The replica does not attach to it by itself.
	time.Sleep(3 * time.Second)
	left := observe(replicaAddr)
	if left.info.Role != "slave" || left.info.MasterLinkStatus != "down" || left.keys != keys || left.info.MasterReplID != before.info.MasterReplID || left.info.MasterSyncInProgress {
		t.Fatalf("3s after the master returned the replica reports %+v with %d keys; want a link-down replica with %d keys following %q",
			left.info, left.keys, keys, before.info.MasterReplID)
	}
	t.Logf("restarted master: connected_slaves=%d replid=%s; replica: link=%s lastIO=%d replid=%s keys=%d",
		restarted.info.ConnectedSlaves, restarted.info.MasterReplID, left.info.MasterLinkStatus, left.info.MasterLastIOSecondsAgo, left.info.MasterReplID, left.keys)

	rc = dialAddr(replicaAddr)
	takeoverErr := rc.ReplTakeover(ctx, 2*time.Second)
	_ = rc.Close()
	if takeoverErr == nil || TakeoverOutcomeUnknown(takeoverErr) {
		t.Fatalf("REPLTAKEOVER on a replica of the earlier instance: %v; want a refusal that leaves no outcome open", takeoverErr)
	}
	t.Logf("REPLTAKEOVER on the replica of the earlier instance: %v", takeoverErr)

	// What the manager does: promote the replica. It keeps every key.
	rc = dialAddr(replicaAddr)
	if err := rc.ReplicaOfNoOne(ctx); err != nil {
		t.Fatalf("REPLICAOF NO ONE: %v", err)
	}
	_ = rc.Close()
	if s := observe(replicaAddr); s.info.Role != "master" || s.keys != keys {
		t.Fatalf("after REPLICAOF NO ONE the replica reports role %q with %d keys; want a master with %d", s.info.Role, s.keys, keys)
	}
	// The restarted instance, attached to it, gets the cache back.
	replicaIP := run("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", strings.Replace(masterHost, "holder", "replica", 1))
	mc := dialAddr(masterAddr)
	if err := mc.ReplicaOf(ctx, replicaIP, 6379); err != nil {
		t.Fatalf("REPLICAOF on the restarted instance: %v", err)
	}
	_ = mc.Close()
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		s := observe(masterAddr)
		if s.info.Role == "slave" && s.info.MasterLinkStatus == "up" && s.keys == keys {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the restarted instance did not sync from the promoted replica: %+v with %d keys", s.info, s.keys)
		}
	}

	// What the manager must not do, on a second pair: REPLICAOF of the
	// left-behind replica to the restarted master empties it.
	masterAddr, replicaAddr, masterHost, replaceMaster = pair("b")
	replaceMaster(false)
	if s := observe(masterAddr); s.keys != 0 {
		t.Fatalf("restarted master holds %d keys, want none", s.keys)
	}
	rc = dialAddr(replicaAddr)
	if err := rc.ReplicaOf(ctx, masterHost, 6379); err != nil {
		t.Fatalf("REPLICAOF to the restarted master: %v", err)
	}
	_ = rc.Close()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		s := observe(replicaAddr)
		if s.keys == 0 && s.info.MasterLinkStatus == "up" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("REPLICAOF to the restarted master left the replica with %d keys (link %s); the guard rests on it emptying the replica",
				s.keys, s.info.MasterLinkStatus)
		}
	}
}
