package controller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// TestDragonflyNativeManagerDoesNotPromoteAStaleReplica is opt-in
// (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1, docker; about 20 seconds). It runs
// the Dragonfly manager against a real master and replica, cuts the replica
// off from the master while the master lives on (the master's password is
// changed and the replication connections are closed, which is what a wrong
// `masterauth` gives), lets the master move on (a FLUSHALL and new keys),
// then replaces the master's process with a new, empty instance, and checks:
//
//   - while it is cut off, the replica reports the link down, no full sync,
//     the replication id of the master that is still running, and its old
//     keys; the manager's REPLICAOF toward the master is refused and leaves
//     them;
//   - after the master's restart the replica meets every condition of the
//     restarted-master promotion (another replication id than the master's,
//     link down, keys held, the master empty);
//   - the manager does not promote it: it re-attaches it, the replica takes
//     the restarted master's content, the record stays on the master, and
//     DragonflyStaleReplicaNotPromoted is emitted once.
//
// The image is BLOODRAVEN_DRAGONFLY_NATIVE_IMAGE, by default the supported
// floor. This is the test that fails when a Dragonfly release changes what
// a replica reports while it cannot reach a running master.
func TestDragonflyNativeManagerDoesNotPromoteAStaleReplica(t *testing.T) {
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

	fg := fgWithDragonflyEnabledAndActive() // sites dc1 (MySQL active) and dc2
	fg.Spec.Dragonfly.Auth = nil
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
	host := func(site string) string {
		h, _ := splitHostPort(dragonflyAddr(fg, site), 6379)
		return h
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	network, holder, dc1, dc2 := "brdfs-net-"+suffix, "brdfs-holder-"+suffix, "brdfs-dc1-"+suffix, "brdfs-dc2-"+suffix
	run("network", "create", network)
	t.Cleanup(func() {
		_ = exec.Command(docker, "rm", "-f", dc1, dc2, holder).Run()
		_ = exec.Command(docker, "network", "rm", network).Run()
	})
	flags := append(buildDragonflyArgs(&v1alpha1.DragonflySpec{}, 6379, 9999), "--proactor_threads=2")
	run("run", "-d", "--name", holder, "--network", network, "--network-alias", host("dc1"), "-p", "127.0.0.1::6379",
		"--entrypoint", "sleep", image, "infinity")
	startDC1 := func() {
		run(append([]string{"run", "-d", "--name", dc1, "--network", "container:" + holder, image}, flags...)...)
	}
	startDC1()
	run(append([]string{"run", "-d", "--name", dc2, "--network", network, "--network-alias", host("dc2"), "-p", "127.0.0.1::6379", image}, flags...)...)
	published := map[string]string{
		dragonflyAddr(fg, "dc1"): strings.SplitN(run("port", holder, "6379/tcp"), "\n", 2)[0],
		dragonflyAddr(fg, "dc2"): strings.SplitN(run("port", dc2, "6379/tcp"), "\n", 2)[0],
	}
	// dc1's password changes during the test; the operator knows it (the
	// replica does not).
	var (
		pwMu        sync.Mutex
		dc1Password string
	)
	password := func(addr string) string {
		pwMu.Lock()
		defer pwMu.Unlock()
		if addr == dragonflyAddr(fg, "dc1") {
			return dc1Password
		}
		return ""
	}
	setDC1Password := func(pw string) {
		pwMu.Lock()
		dc1Password = pw
		pwMu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	connect := func(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
		hostPort, ok := published[addr]
		if !ok {
			return nil, fmt.Errorf("no container for %s", addr)
		}
		return dragonfly.New(ctx, dragonfly.Config{Addr: hostPort, Password: password(addr)})
	}
	type state struct {
		info dragonfly.ReplicationInfo
		keys int64
	}
	observe := func(site string) state {
		t.Helper()
		var lastErr error
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
			c, err := connect(ctx, dragonflyAddr(fg, site), "")
			if err != nil {
				lastErr = err
				continue
			}
			info, err := c.InfoReplication(ctx)
			if err != nil {
				lastErr = err
				_ = c.Close()
				continue
			}
			keys, err := c.KeyCount(ctx)
			_ = c.Close()
			if err != nil {
				lastErr = err
				continue
			}
			return state{info, keys}
		}
		t.Fatalf("observe %s: %v", site, lastErr)
		return state{}
	}
	// cli runs redis-cli in dc1's container, with its current password.
	cli := func(args ...string) string {
		t.Helper()
		full := []string{"exec", dc1, "redis-cli"}
		if pw := password(dragonflyAddr(fg, "dc1")); pw != "" {
			full = append(full, "-a", pw, "--no-auth-warning")
		}
		return run(append(full, args...)...)
	}
	observe("dc1")
	observe("dc2")
	const keys = 300
	cli("DEBUG", "POPULATE", strconv.Itoa(keys), "old")

	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(256)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 2*time.Second)
	mgr.SetConnector(connect)
	tickUntil := func(what string, within time.Duration, done func() bool) {
		t.Helper()
		for deadline := time.Now().Add(within); ; time.Sleep(300 * time.Millisecond) {
			mgr.Tick(ctx)
			if done() {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: not within %s; dc1=%+v dc2=%+v status=%+v", what, within, observe("dc1"), observe("dc2"), dragonflyStatusOf(t, c, key))
			}
		}
	}
	tickUntil("dc2 in sync with dc1", 60*time.Second, func() bool {
		s := observe("dc2")
		return s.info.Role == "slave" && s.info.MasterLinkStatus == "up" && !s.info.MasterSyncInProgress && s.keys == keys
	})
	mgr.Tick(ctx) // a sighting of the pair in sync
	liveID := observe("dc1").info.MasterReplID

	// The replica is cut off while the master lives on: the master gets a
	// password the replica does not have, and its replication connections
	// are closed. The master then flushes and takes new keys.
	cli("CONFIG", "SET", "requirepass", "pw2")
	setDC1Password("pw2")
	for _, line := range strings.Split(cli("CLIENT", "LIST"), "\n") {
		if !strings.Contains(line, "name=repl_") {
			continue
		}
		cli("CLIENT", "KILL", "ID", strings.TrimPrefix(strings.Fields(line)[0], "id="))
	}
	cli("FLUSHALL")
	cli("DEBUG", "POPULATE", "50", "new")
	var cut state
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if cut = observe("dc2"); cut.info.MasterLinkStatus == "down" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dc2 still reports its link up 15s after it was cut off: %+v", cut.info)
		}
	}
	// Three polls beside the running master. Each also re-issues REPLICAOF
	// to the replica, which it refuses.
	for i := 0; i < 3; i++ {
		mgr.Tick(ctx)
	}
	cut = observe("dc2")
	if cut.info.Role != "slave" || cut.info.MasterLinkStatus != "down" || cut.info.MasterSyncInProgress ||
		cut.info.MasterReplID != liveID || cut.keys != keys {
		t.Fatalf("dc2 cut off from the running master reports %+v with %d keys; want a link-down replica following %q with its %d old keys",
			cut.info, cut.keys, liveID, keys)
	}
	if m := observe("dc1"); m.info.Role != "master" || m.info.MasterReplID != liveID || m.keys != 50 {
		t.Fatalf("dc1 reports %+v with %d keys; want the same master instance, moved on to 50 keys", m.info, m.keys)
	}
	t.Logf("cut off from the running master: link=%s lastIO=%ds replid same as the master's, %d stale keys beside the master's 50",
		cut.info.MasterLinkStatus, cut.info.MasterLastIOSecondsAgo, cut.keys)
	if st := dragonflyStatusOf(t, c, key); st.ActiveSite != "dc1" || st.LastPromotionTarget != "" {
		t.Fatalf("status.dragonfly = %+v while the master lived; want dc1 active and no promotion", st)
	}

	// The master's pod is recreated: a new, empty instance without the
	// password. The stale replica now looks like the holder of the cache.
	run("rm", "-f", dc1)
	setDC1Password("")
	startDC1()
	restarted := observe("dc1")
	left := observe("dc2")
	if restarted.info.Role != "master" || restarted.keys != 0 || restarted.info.MasterReplID == liveID {
		t.Fatalf("restarted dc1 reports %+v with %d keys; want a new, empty master", restarted.info, restarted.keys)
	}
	if left.info.Role != "slave" || left.info.MasterLinkStatus != "down" || left.info.MasterReplID != liveID || left.keys != keys {
		t.Fatalf("dc2 after the master restarted reports %+v with %d keys; want a link-down replica of %q with %d keys",
			left.info, left.keys, liveID, keys)
	}

	tickUntil("the stale replica re-attached to the restarted master", 60*time.Second, func() bool {
		s := observe("dc2")
		return s.info.Role == "slave" && s.info.MasterLinkStatus == "up" && !s.info.MasterSyncInProgress && s.info.MasterReplID == restarted.info.MasterReplID
	})
	if s := observe("dc2"); s.keys != 0 {
		t.Fatalf("dc2 holds %d keys after it was re-attached to the empty master; want 0", s.keys)
	}
	if s := observe("dc1"); s.info.Role != "master" {
		t.Fatalf("dc1 reports role %q; want it to stay the master", s.info.Role)
	}
	st := dragonflyStatusOf(t, c, key)
	if st.ActiveSite != "dc1" || st.LastPromotionTarget != "" {
		t.Fatalf("status.dragonfly activeSite=%q lastPromotionTarget=%q; want dc1 and no promotion: the stale copy must not become the cache",
			st.ActiveSite, st.LastPromotionTarget)
	}
	stale := 0
	for _, ev := range drainEventsCh(rec.Events, 256, 100*time.Millisecond) {
		if strings.Contains(ev, v1alpha1.EventDragonflySessionsLost) {
			t.Fatalf("a promotion was made: %s", ev)
		}
		if strings.Contains(ev, v1alpha1.EventDragonflyStaleReplicaNotPromoted) {
			stale++
		}
	}
	if stale != 1 {
		t.Fatalf("DragonflyStaleReplicaNotPromoted emitted %d times, want once", stale)
	}
}
