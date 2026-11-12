package controller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// TestDragonflyNativeManagerKeepsCacheOverRestartedMaster is opt-in
// (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1, docker; about 30 seconds). It runs
// the Dragonfly manager against a real master and replica through the real
// client, replaces the master's process at the same address the way a
// recreated pod does, lets a client write to the new instance before the
// manager looks, and checks what the manager decides:
//
//   - it does not send the replica REPLICAOF (which empties it): it
//     promotes it with REPLICAOF NO ONE, and the replica keeps every key;
//   - it then attaches the restarted instance to the promoted replica,
//     which brings the cache back to the MySQL active site;
//   - once that site is in sync, Dragonfly follows MySQL back to it with
//     REPLTAKEOVER, losing nothing, and the record stays there.
//
// The image is BLOODRAVEN_DRAGONFLY_NATIVE_IMAGE, by default the supported
// floor. This is the test that fails when a Dragonfly release changes what
// a replica reports after its master restarted (master_replid, link state,
// keys kept).
func TestDragonflyNativeManagerKeepsCacheOverRestartedMaster(t *testing.T) {
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

	// Each site's address is a network alias, as a Service name is. dc1's
	// belongs to a holder container whose network namespace its Dragonfly
	// joins, so the address stays while the process is replaced.
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	network, holder, dc1, dc2 := "brdfm-net-"+suffix, "brdfm-holder-"+suffix, "brdfm-dc1-"+suffix, "brdfm-dc2-"+suffix
	run("network", "create", network)
	t.Cleanup(func() {
		_ = exec.Command(docker, "rm", "-f", dc1, dc2, holder).Run()
		_ = exec.Command(docker, "network", "rm", network).Run()
	})
	// The flags the operator renders for a Dragonfly container.
	flags := append(buildDragonflyArgs(&v1alpha1.DragonflySpec{}, 6379, 9999), "--proactor_threads=2")
	if !strings.Contains(strings.Join(flags, " "), "--break_replication_on_master_restart") {
		t.Fatalf("operator flags %v do not set --break_replication_on_master_restart; the restart handling rests on it", flags)
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dial := func(site string) *dragonfly.Client {
		t.Helper()
		var lastErr error
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
			c, err := dragonfly.New(ctx, dragonfly.Config{Addr: published[dragonflyAddr(fg, site)]})
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
		t.Fatalf("dial %s: %v", site, lastErr)
		return nil
	}
	type state struct {
		info dragonfly.ReplicationInfo
		keys int64
	}
	observe := func(site string) state {
		t.Helper()
		c := dial(site)
		defer func() { _ = c.Close() }()
		info, err := c.InfoReplication(ctx)
		if err != nil {
			t.Fatalf("INFO replication on %s: %v", site, err)
		}
		keys, err := c.KeyCount(ctx)
		if err != nil {
			t.Fatalf("INFO keyspace on %s: %v", site, err)
		}
		return state{info, keys}
	}
	set := func(site, prefix string, n int) {
		t.Helper()
		out, err := exec.Command(docker, "exec", map[string]string{"dc1": dc1, "dc2": dc2}[site], "sh", "-c",
			fmt.Sprintf(`i=0; while [ $i -lt %d ]; do redis-cli SET %s:$i v >/dev/null || exit 1; i=$((i+1)); done`, n, prefix)).CombinedOutput()
		if err != nil {
			t.Fatalf("writing %d keys to %s: %v\n%s", n, site, err, out)
		}
	}
	dial("dc1").Close() //nolint:errcheck // wait for the server
	dial("dc2").Close() //nolint:errcheck
	const keys = 300
	set("dc1", "k", keys)

	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(256), slog.Default(), key, 2*time.Second)
	mgr.SetConnector(func(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
		real, ok := published[addr]
		if !ok {
			return nil, fmt.Errorf("no container for %s", addr)
		}
		return dragonfly.New(ctx, dragonfly.Config{Addr: real})
	})
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

	// The manager wires dc2 as dc1's replica itself.
	tickUntil("dc2 in sync with dc1", 60*time.Second, func() bool {
		s := observe("dc2")
		return s.info.Role == "slave" && s.info.MasterLinkStatus == "up" && !s.info.MasterSyncInProgress && s.keys == keys
	})
	mgr.Tick(ctx) // a sighting of the pair in sync
	oldID := observe("dc1").info.MasterReplID

	// dc1's pod is recreated: a new, empty instance at the same address.
	// A client writes to it before the manager's next cycle.
	run("rm", "-f", dc1)
	startDC1()
	dial("dc1").Close() //nolint:errcheck
	set("dc1", "new", 5)
	restarted, left := observe("dc1"), observe("dc2")
	if restarted.info.MasterReplID == oldID || restarted.keys != 5 {
		t.Fatalf("restarted dc1 reports replid %q with %d keys; want a new id and the 5 new keys", restarted.info.MasterReplID, restarted.keys)
	}
	if left.info.Role != "slave" || left.info.MasterLinkStatus != "down" || left.info.MasterReplID != oldID || left.keys != keys {
		t.Fatalf("dc2 after its master restarted reports %+v with %d keys; want a link-down replica of %q with %d keys",
			left.info, left.keys, oldID, keys)
	}

	mgr.Tick(ctx)
	promoted := observe("dc2")
	if promoted.info.Role != "master" || promoted.keys != keys {
		t.Fatalf("after the cycle that saw the restarted master, dc2 reports role %q with %d keys; want a master with %d (not re-attached and emptied)",
			promoted.info.Role, promoted.keys, keys)
	}
	st := dragonflyStatusOf(t, c, key)
	if st.ActiveSite != "dc2" || st.LastPromotionTarget != "dc2" {
		t.Fatalf("status.dragonfly activeSite=%q lastPromotionTarget=%q, want dc2", st.ActiveSite, st.LastPromotionTarget)
	}

	// The restarted instance is attached to dc2 and gets the cache back.
	tickUntil("restarted dc1 in sync with dc2", 60*time.Second, func() bool {
		s := observe("dc1")
		return s.info.Role == "slave" && s.info.MasterLinkStatus == "up" && !s.info.MasterSyncInProgress && s.keys == keys
	})
	if s := observe("dc2"); s.info.Role != "master" || s.keys != keys {
		t.Fatalf("dc2 reports role %q with %d keys while dc1 synced from it", s.info.Role, s.keys)
	}

	// Dragonfly follows MySQL (active on dc1) back once the promotion is no
	// longer recent: a REPLTAKEOVER, which loses nothing. The stamp is
	// aged here; in a cluster this is pendingPromotionActiveSiteTTL later.
	var live v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, key, &live); err != nil {
		t.Fatal(err)
	}
	aged := metav1.NewTime(time.Now().Add(-2 * pendingPromotionActiveSiteTTL))
	live.Status.Dragonfly.LastPromotionTime = &aged
	if err := c.Status().Update(ctx, &live); err != nil {
		t.Fatal(err)
	}
	tickUntil("Dragonfly back on the MySQL active site", 60*time.Second, func() bool {
		return dragonflyStatusOf(t, c, key).ActiveSite == "dc1"
	})
	if s := observe("dc1"); s.info.Role != "master" || s.keys != keys {
		t.Fatalf("dc1 after the takeover back reports role %q with %d keys; want a master with %d", s.info.Role, s.keys, keys)
	}
	// It stays there: dc2's process exits after the takeover (in a cluster
	// its pod is recreated), and the record does not move again.
	for i := 0; i < 3; i++ {
		mgr.Tick(ctx)
	}
	if got := dragonflyStatusOf(t, c, key).ActiveSite; got != "dc1" {
		t.Fatalf("status.dragonfly.activeSite = %q after three more cycles, want dc1", got)
	}
	if s := observe("dc1"); s.info.Role != "master" || s.keys != keys {
		t.Fatalf("dc1 reports role %q with %d keys after three more cycles; want a master with %d", s.info.Role, s.keys, keys)
	}
}
