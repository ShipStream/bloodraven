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

// TestDragonflyNativeSyncProof is opt-in (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1,
// docker; about 30 seconds). It runs the client against a real Dragonfly
// master and replica and checks what the planned-failover and rollout sync
// check rests on:
//
//   - a master prints no replication offset in INFO, and gives one per
//     shard through DFLY REPLICAOFFSET; a replica gives one per flow
//     through DEBUG REPLICA OFFSET; the two are on the same scale;
//   - a replica cut off from its master without a TCP reset still answers,
//     and its position is behind the master's: Position.ReachedBy refuses
//     it (its own link answers are logged, not asserted: on v1.38 and v2.1
//     they stay "up" for more than ten seconds);
//   - once the link is back the replica reaches the master's position;
//   - REPLTAKEOVER on a caught-up replica keeps every key, makes it a
//     master with a position, and the old master's process exits.
//
// The image is BLOODRAVEN_DRAGONFLY_NATIVE_IMAGE, by default the version
// the docs name as the supported floor. This is the test that fails when a
// Dragonfly release renames one of the two commands.
func TestDragonflyNativeSyncProof(t *testing.T) {
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
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	network, master, replica := "brdf-net-"+suffix, "brdf-master-"+suffix, "brdf-replica-"+suffix
	run("network", "create", network)
	t.Cleanup(func() {
		_ = exec.Command(docker, "rm", "-f", master, replica).Run()
		_ = exec.Command(docker, "network", "rm", network).Run()
	})
	// The master lives on the test network only. The replica is published
	// from the default bridge and joined to the test network, so that
	// network can be taken away from it (the replication link stalls
	// without a reset) while the test still reaches it.
	run("run", "-d", "--name", master, "--network", network, "-p", "127.0.0.1::6379", image, "--proactor_threads=2")
	run("run", "-d", "--name", replica, "-p", "127.0.0.1::6379", image, "--proactor_threads=2")
	run("network", "connect", network, replica)
	addrOf := func(name string) string {
		t.Helper()
		// "127.0.0.1:49153"
		return strings.SplitN(run("port", name, "6379/tcp"), "\n", 2)[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dial := func(name string) *Client {
		t.Helper()
		addr := addrOf(name)
		var lastErr error
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
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
		t.Fatalf("dial %s (%s): %v", name, addr, lastErr)
		return nil
	}
	position := func(name string) Position {
		t.Helper()
		c := dial(name)
		defer func() { _ = c.Close() }()
		pos, err := c.ReplicationPosition(ctx)
		if err != nil {
			t.Fatalf("ReplicationPosition(%s): %v", name, err)
		}
		return pos
	}
	write := func(prefix string, n int) {
		t.Helper()
		c := dial(master)
		defer func() { _ = c.Close() }()
		for i := 0; i < n; i++ {
			if err := c.exec(ctx, "SET", fmt.Sprintf("%s:%d", prefix, i), "v"); err != nil {
				t.Fatalf("SET on the master: %v", err)
			}
		}
	}
	waitReached := func(what string, within time.Duration) {
		t.Helper()
		var detail string
		for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
			src := position(master)
			c := dial(replica)
			info, infoErr := c.InfoReplication(ctx)
			persist, _ := c.InfoPersistence(ctx)
			_ = c.Close()
			if infoErr != nil || !CandidateSyncReady(info, persist) {
				detail = fmt.Sprintf("replica not in stable sync: %+v (%v)", info, infoErr)
				continue
			}
			// Read after the source, with no writer running: the replica
			// has to get there.
			for inner := time.Now().Add(2 * time.Second); time.Now().Before(inner); time.Sleep(100 * time.Millisecond) {
				var reached bool
				if reached, detail = src.ReachedBy(position(replica)); reached {
					return
				}
			}
		}
		t.Fatalf("%s: replica did not reach the master: %s", what, detail)
	}

	write("seed", 200)
	rc := dial(replica)
	if err := rc.ReplicaOf(ctx, master, 6379); err != nil {
		t.Fatalf("REPLICAOF: %v", err)
	}
	_ = rc.Close()
	waitReached("after the full sync", 60*time.Second)

	mc := dial(master)
	masterInfo, err := mc.InfoReplication(ctx)
	_ = mc.Close()
	if err != nil {
		t.Fatalf("INFO replication on the master: %v", err)
	}
	t.Logf("master INFO: master_repl_offset reported=%v connected_slaves=%d", masterInfo.MasterReplOffsetReported, masterInfo.ConnectedSlaves)
	src, dst := position(master), position(replica)
	if src.Role != "master" || src.Scalar || len(src.Flows) != 2 || src.ReplID == "" {
		t.Fatalf("master position = %+v, want a per-flow position of 2 shards with a replication id", src)
	}
	if dst.Role != "slave" || dst.Scalar || len(dst.Flows) != 2 || dst.ReplID != src.ReplID {
		t.Fatalf("replica position = %+v, want 2 flows following %q", dst, src.ReplID)
	}

	// Cut the replica off; the master moves on.
	run("network", "disconnect", network, replica)
	write("cut", 200)
	src = position(master)
	c := dial(replica)
	info, err := c.InfoReplication(ctx)
	persist, _ := c.InfoPersistence(ctx)
	_ = c.Close()
	if err != nil {
		t.Fatalf("INFO replication on the cut-off replica: %v", err)
	}
	t.Logf("cut-off replica: link=%s lastIO=%d fullSync=%v; link checks pass=%v",
		info.MasterLinkStatus, info.MasterLastIOSecondsAgo, info.MasterSyncInProgress, CandidateSyncReady(info, persist))
	dst = position(replica)
	if reached, detail := src.ReachedBy(dst); reached {
		t.Fatalf("ReachedBy() = true for a replica cut off before 200 writes: source %+v target %+v", src, dst)
	} else {
		t.Logf("cut-off replica refused: %s", detail)
	}
	if src.Total() <= dst.Total() {
		t.Fatalf("master total %d is not ahead of the cut-off replica's %d", src.Total(), dst.Total())
	}

	// Back on the network, the replica catches up.
	run("network", "connect", network, replica)
	waitReached("after the link came back", 90*time.Second)

	// REPLTAKEOVER keeps every key and ends the old master's process.
	mc = dial(master)
	keys, err := mc.KeyCount(ctx)
	_ = mc.Close()
	if err != nil || keys != 400 {
		t.Fatalf("master key count = %d, %v; want 400", keys, err)
	}
	rc = dial(replica)
	if err := rc.ReplTakeover(ctx, 5*time.Second); err != nil {
		t.Fatalf("REPLTAKEOVER: %v", err)
	}
	_ = rc.Close()
	rc = dial(replica)
	got, err := rc.KeyCount(ctx)
	if err != nil || got != keys {
		t.Fatalf("promoted replica key count = %d, %v; want %d", got, err, keys)
	}
	promoted, err := rc.ReplicationPosition(ctx)
	_ = rc.Close()
	if err != nil || promoted.Role != "master" {
		t.Fatalf("promoted replica position = %+v, %v; want a master's position", promoted, err)
	}
	t.Logf("promoted replica position: %+v (total %d)", promoted, promoted.Total())
	exited := false
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		if run("inspect", "-f", "{{.State.Running}}", master) == "false" {
			exited = true
			break
		}
	}
	if !exited {
		t.Fatal("the old master's process is still running 20s after a successful REPLTAKEOVER")
	}
}
