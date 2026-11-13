package dragonfly

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDragonflyNativeRestartedReplicaCatchUp is opt-in
// (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1, docker; about 30 seconds). It checks
// the catch-up signal a template rollout rests on (#217) in the shape the
// rollout produces: the replica's process is replaced by a new, empty one,
// which is attached to a master that holds a large cache and keeps taking
// writes.
//
// The signal is CandidateSyncReady (the replica's own INFO answers: link
// up, data received, no full sync, not loading) together with
// Position.ReachedBy (every flow's DEBUG REPLICA OFFSET count has reached
// the master's DFLY REPLICAOFFSET LSN, under the master's master_replid).
// A master prints no offset in INFO on these releases, so that field is
// not part of it.
//
//   - The new instance, before it is attached, is refused: it answers as a
//     master under another replication id.
//   - From REPLICAOF on, the test samples both sites as fast as it can.
//     For every sample it reads the master's key count, then the master's
//     position, then the replica. Whenever the signal says "caught up",
//     the replica must hold at least the keys the master held when its
//     position was read. The writer only adds keys, so a replica that is
//     in its full sync, or behind, and passes would be caught here. At
//     least one sample during the sync must be refused.
//   - With the writer still running, REPLTAKEOVER on the caught-up replica
//     keeps every key: the seed and every SET the master answered OK to.
//
// The image is BLOODRAVEN_DRAGONFLY_NATIVE_IMAGE, by default the supported
// floor.
func TestDragonflyNativeRestartedReplicaCatchUp(t *testing.T) {
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
	network, master, replica := "brdfc-net-"+suffix, "brdfc-master-"+suffix, "brdfc-replica-"+suffix
	run("network", "create", network)
	t.Cleanup(func() {
		_ = exec.Command(docker, "rm", "-f", master, replica).Run()
		_ = exec.Command(docker, "network", "rm", network).Run()
	})
	flags := []string{"--port=6379", "--bind=0.0.0.0", "--break_replication_on_master_restart", "--proactor_threads=2"}
	startReplica := func() {
		run(append([]string{"run", "-d", "--name", replica, "--network", network, "-p", "127.0.0.1::6379", image}, flags...)...)
	}
	run(append([]string{"run", "-d", "--name", master, "--network", network, "-p", "127.0.0.1::6379", image}, flags...)...)
	startReplica()
	addrOf := func(name string) string {
		t.Helper()
		return strings.SplitN(run("port", name, "6379/tcp"), "\n", 2)[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	masterAddr := addrOf(master)

	// sample is one look at the pair, in the order the operator reads it.
	type sample struct {
		masterKeys  int64
		source      Position
		info        ReplicationInfo
		target      Position
		targetErr   error
		replicaKeys int64
		ready       bool
		detail      string
	}
	look := func(mc, rc *Client) sample {
		t.Helper()
		var s sample
		var err error
		if s.masterKeys, err = mc.KeyCount(ctx); err != nil {
			t.Fatalf("INFO keyspace on the master: %v", err)
		}
		if s.source, err = mc.ReplicationPosition(ctx); err != nil {
			t.Fatalf("ReplicationPosition on the master: %v", err)
		}
		if s.info, err = rc.InfoReplication(ctx); err != nil {
			t.Fatalf("INFO replication on the replica: %v", err)
		}
		persist, err := rc.InfoPersistence(ctx)
		if err != nil {
			t.Fatalf("INFO persistence on the replica: %v", err)
		}
		linked := CandidateSyncReady(s.info, persist)
		s.target, s.targetErr = rc.ReplicationPosition(ctx)
		if s.replicaKeys, err = rc.KeyCount(ctx); err != nil {
			t.Fatalf("INFO keyspace on the replica: %v", err)
		}
		switch {
		case !linked:
			s.detail = fmt.Sprintf("not in stable sync (role=%s link=%s lastIO=%d fullSync=%v loading=%v)",
				s.info.Role, s.info.MasterLinkStatus, s.info.MasterLastIOSecondsAgo, s.info.MasterSyncInProgress, persist.Loading)
		case s.targetErr != nil:
			s.detail = "no position: " + s.targetErr.Error()
		default:
			s.ready, s.detail = s.source.ReachedBy(s.target)
		}
		return s
	}

	const seeded = 1500000
	mc := dialAddr(masterAddr)
	defer func() { _ = mc.Close() }()
	if err := mc.exec(ctx, "DEBUG", "POPULATE", strconv.Itoa(seeded), "seed", "128"); err != nil {
		t.Fatalf("DEBUG POPULATE: %v", err)
	}
	if keys, err := mc.KeyCount(ctx); err != nil || keys != seeded {
		t.Fatalf("master key count = %d, %v; want %d", keys, err, seeded)
	}

	// The replica as it was before the rollout: attached and caught up.
	rc := dialAddr(addrOf(replica))
	if err := rc.ReplicaOf(ctx, master, 6379); err != nil {
		t.Fatalf("REPLICAOF: %v", err)
	}
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if s := look(mc, rc); s.ready {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("the replica did not catch up before the restart: %s", s.detail)
		}
	}
	_ = rc.Close()

	// The writer: one new key per SET, counted when the master said OK.
	var acked atomic.Int64
	stopWriter := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		wc, err := New(ctx, Config{Addr: masterAddr})
		if err != nil {
			return
		}
		defer func() { _ = wc.Close() }()
		for i := 0; ; i++ {
			select {
			case <-stopWriter:
				return
			default:
			}
			if err := wc.exec(ctx, "SET", "w:"+strconv.Itoa(i), "v"); err != nil {
				// The master stops answering once it has been taken over.
				return
			}
			acked.Add(1)
		}
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			close(stopWriter)
			writer.Wait()
		}
	}
	defer stop()

	// The rollout replaces the replica's pod: a new, empty process.
	run("rm", "-f", replica)
	startReplica()
	rc = dialAddr(addrOf(replica))
	defer func() { _ = rc.Close() }()
	if s := look(mc, rc); s.ready || s.info.Role != "master" || s.replicaKeys != 0 {
		t.Fatalf("the new, unattached instance: ready=%v role=%q keys=%d; want it refused as an empty master", s.ready, s.info.Role, s.replicaKeys)
	} else {
		t.Logf("unattached instance refused: role=%s, %s", s.info.Role, s.detail)
	}

	if err := rc.ReplicaOf(ctx, master, 6379); err != nil {
		t.Fatalf("REPLICAOF after the restart: %v", err)
	}
	refused, refusedInFullSync, refusedBehind, passed := 0, 0, 0, 0
	lastDetail := ""
	for deadline := time.Now().Add(2 * time.Minute); passed < 20; {
		if time.Now().After(deadline) {
			t.Fatalf("the restarted replica did not catch up with the master under writes: %s", lastDetail)
		}
		s := look(mc, rc)
		if !s.ready {
			refused++
			if s.info.MasterSyncInProgress {
				refusedInFullSync++
			} else if s.info.MasterLinkStatus == "up" {
				refusedBehind++
			}
			if s.detail != lastDetail && refused <= 12 {
				t.Logf("refused: %s (replica holds %d of %d keys)", s.detail, s.replicaKeys, s.masterKeys)
			}
			lastDetail = s.detail
			continue
		}
		passed++
		if s.replicaKeys < s.masterKeys {
			t.Fatalf("the signal said caught up while the replica held %d keys and the master had %d when its position was read "+
				"(source %+v, target %+v, replica INFO %+v)", s.replicaKeys, s.masterKeys, s.source, s.target, s.info)
		}
	}
	if refused == 0 {
		t.Fatalf("no sample was refused while a replica synced %d keys from scratch: the test saw nothing of the sync", seeded)
	}
	t.Logf("%d samples refused during the sync (%d with a full sync in progress, %d linked and behind), then %d passed",
		refused, refusedInFullSync, refusedBehind, passed)

	// REPLTAKEOVER with the writer running keeps every acknowledged write.
	if err := rc.ReplTakeover(ctx, 10*time.Second); err != nil {
		t.Fatalf("REPLTAKEOVER: %v", err)
	}
	stop()
	_ = rc.Close()
	rc = dialAddr(addrOf(replica))
	got, err := rc.KeyCount(ctx)
	if err != nil {
		t.Fatalf("INFO keyspace on the promoted replica: %v", err)
	}
	// A SET whose reply was cut off by the takeover may have been applied.
	if want := seeded + acked.Load(); got != want && got != want+1 {
		t.Fatalf("promoted replica holds %d keys; want %d (%d seeded + %d acknowledged writes)", got, want, seeded, acked.Load())
	}
	info, err := rc.InfoReplication(ctx)
	if err != nil || info.Role != "master" {
		t.Fatalf("promoted replica INFO = %+v, %v; want a master", info, err)
	}
	t.Logf("REPLTAKEOVER on %s kept %d keys (%d seeded, %d acknowledged writes)", image, got, seeded, acked.Load())
}
