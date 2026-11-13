package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// TestDragonflyNativeRolloutKeepsCache is opt-in
// (BLOODRAVEN_DRAGONFLY_NATIVE_TEST=1, docker; about a minute). It runs the
// group's Dragonfly template rollout (reconcileDragonflyResources) and the
// Dragonfly manager against two real Dragonfly servers, started with the
// flags the operator renders, while a client keeps writing to whichever
// site status names as the master. The test stands in for the StatefulSet
// controller and the kubelet: when a site's pod template changed or its pod
// was deleted, it replaces that site's process with a new, empty one at the
// same address.
//
// It checks what the strict rollout (#217) promises, on the real server:
//
//   - the site that holds the master is never replaced while it is the
//     master: at every step the master holds at least the seeded keys;
//   - the restarted replica is not taken over to before it finished its
//     full sync and reached the master's per-flow position (DFLY
//     REPLICAOFFSET against DEBUG REPLICA OFFSET): the takeover keeps every
//     key, including every write the client got an OK for;
//   - each site's process is replaced exactly once, the former master's
//     after the takeover;
//   - nothing is promoted with REPLICAOF NO ONE.
//
// The image is BLOODRAVEN_DRAGONFLY_NATIVE_IMAGE, by default the supported
// floor.
func TestDragonflyNativeRolloutKeepsCache(t *testing.T) {
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
	sites := []string{"dc1", "dc2"}
	host := func(site string) string {
		h, _ := splitHostPort(dragonflyAddr(fg, site), 6379)
		return h
	}

	// Each site's address is a network alias on a holder container whose
	// network namespace its Dragonfly joins, so the address stays while the
	// process is replaced, as a Service's does for a recreated pod.
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	network := "brdfo-net-" + suffix
	holder := map[string]string{"dc1": "brdfo-h1-" + suffix, "dc2": "brdfo-h2-" + suffix}
	proc := map[string]string{"dc1": "brdfo-dc1-" + suffix, "dc2": "brdfo-dc2-" + suffix}
	run("network", "create", network)
	t.Cleanup(func() {
		_ = exec.Command(docker, "rm", "-f", proc["dc1"], proc["dc2"], holder["dc1"], holder["dc2"]).Run()
		_ = exec.Command(docker, "network", "rm", network).Run()
	})
	flags := append(buildDragonflyArgs(&v1alpha1.DragonflySpec{}, 6379, 9999), "--proactor_threads=2")
	published := map[string]string{}
	start := func(site string) {
		run(append([]string{"run", "-d", "--name", proc[site], "--network", "container:" + holder[site], image}, flags...)...)
	}
	for _, site := range sites {
		run("run", "-d", "--name", holder[site], "--network", network, "--network-alias", host(site), "-p", "127.0.0.1::6379",
			"--entrypoint", "sleep", image, "infinity")
		start(site)
		published[dragonflyAddr(fg, site)] = strings.SplitN(run("port", holder[site], "6379/tcp"), "\n", 2)[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	connector := func(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
		hostPort, ok := published[addr]
		if !ok {
			return nil, fmt.Errorf("no container for %s", addr)
		}
		return dragonfly.New(ctx, dragonfly.Config{Addr: hostPort})
	}
	type state struct {
		info dragonfly.ReplicationInfo
		keys int64
	}
	// look reads a site once; ok is false when it does not answer.
	look := func(site string) (state, bool) {
		lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		c, err := dragonfly.New(lctx, dragonfly.Config{Addr: published[dragonflyAddr(fg, site)]})
		if err != nil {
			return state{}, false
		}
		defer func() { _ = c.Close() }()
		info, err := c.InfoReplication(lctx)
		if err != nil {
			return state{}, false
		}
		keys, err := c.KeyCount(lctx)
		if err != nil {
			return state{}, false
		}
		return state{info, keys}, true
	}
	waitUp := func(site string) {
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
			if _, ok := look(site); ok {
				return
			}
		}
		t.Fatalf("site %s did not come up", site)
	}
	for _, site := range sites {
		waitUp(site)
	}
	const seeded = 200000
	run("exec", proc["dc1"], "redis-cli", "DEBUG", "POPULATE", strconv.Itoa(seeded), "seed", "64")
	if s, _ := look("dc1"); s.keys != seeded {
		t.Fatalf("dc1 holds %d keys after DEBUG POPULATE, want %d", s.keys, seeded)
	}

	r, c := newReconciler(fg)
	recorder := record.NewFakeRecorder(4096)
	r.Recorder = recorder
	r.dragonflyConnector = connector
	key := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	mgr := NewDragonflyManager(c, recorder, slog.Default(), key, 2*time.Second)
	mgr.SetConnector(connector)

	stsOf := func(site string) *appsv1.StatefulSet {
		t.Helper()
		var sts appsv1.StatefulSet
		if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site)}, &sts); err != nil {
			t.Fatalf("get StatefulSet %s: %v", site, err)
		}
		return &sts
	}
	templateOf := func(site string) string {
		t.Helper()
		raw, err := json.Marshal(stsOf(site).Spec.Template)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	podKey := func(site string) types.NamespacedName {
		return types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site) + "-0"}
	}
	createPod := func(site, role string) {
		t.Helper()
		pod := dragonflyPodForTest(fg, site, image, true)
		pod.Labels[v1alpha1.DragonflyRoleLabel] = role
		if err := c.Create(ctx, pod); err != nil {
			t.Fatalf("create pod of %s: %v", site, err)
		}
	}

	if _, err := r.reconcileDragonflyResources(ctx, freshGroup(t, c, fg)); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	running := map[string]string{}
	replaced := map[string]int{}
	for _, site := range sites {
		role := "replica"
		if site == "dc1" {
			role = "master"
		}
		createPod(site, role)
		markDragonflyStatefulSetRolledOutForTest(t, c, site)
		running[site] = templateOf(site)
	}

	// kubelet replaces the process of a site whose pod template changed or
	// whose pod was deleted: one new, empty instance at the same address.
	kubelet := func() {
		t.Helper()
		for _, site := range sites {
			var pod corev1.Pod
			err := c.Get(ctx, podKey(site), &pod)
			if err != nil && !apierrors.IsNotFound(err) {
				t.Fatalf("get pod of %s: %v", site, err)
			}
			deleted := apierrors.IsNotFound(err)
			if !deleted && templateOf(site) == running[site] {
				continue
			}
			sts := stsOf(site)
			sts.Status.ReadyReplicas = 0
			if err := c.Status().Update(ctx, sts); err != nil {
				t.Fatalf("update StatefulSet %s status: %v", site, err)
			}
			if !deleted {
				if err := c.Delete(ctx, &pod); err != nil {
					t.Fatalf("delete pod of %s: %v", site, err)
				}
			}
			run("rm", "-f", proc[site])
			start(site)
			waitUp(site)
			replaced[site]++
			running[site] = templateOf(site)
			createPod(site, "replica")
			markDragonflyStatefulSetRolledOutForTest(t, c, site)
			t.Logf("kubelet: replaced the process of %s (pod deleted=%v)", site, deleted)
		}
	}

	// The client: SET of a new key per write to the site status names,
	// retried with the same key until it gets an OK.
	var activeAddr atomic.Value
	activeAddr.Store(published[dragonflyAddr(fg, "dc1")])
	var acked atomic.Int64
	stopWriter := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		var conn net.Conn
		var rd *bufio.Reader
		addr := ""
		for i := 0; ; {
			select {
			case <-stopWriter:
				if conn != nil {
					_ = conn.Close()
				}
				return
			default:
			}
			if want := activeAddr.Load().(string); conn == nil || want != addr {
				if conn != nil {
					_ = conn.Close()
				}
				addr = want
				var err error
				if conn, err = net.DialTimeout("tcp", addr, time.Second); err != nil {
					conn = nil
					time.Sleep(20 * time.Millisecond)
					continue
				}
				rd = bufio.NewReader(conn)
			}
			k := "w:" + strconv.Itoa(i)
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			_, werr := fmt.Fprintf(conn, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(k), k)
			line, rerr := rd.ReadString('\n')
			if werr != nil || rerr != nil || !strings.HasPrefix(line, "+OK") {
				_ = conn.Close()
				conn = nil
				time.Sleep(20 * time.Millisecond)
				continue
			}
			acked.Add(1)
			i++
			time.Sleep(2 * time.Millisecond)
		}
	}()
	writerStopped := false
	stop := func() {
		if !writerStopped {
			writerStopped = true
			close(stopWriter)
			writer.Wait()
		}
	}
	defer stop()

	step := func() time.Duration {
		t.Helper()
		requeue, err := r.reconcileDragonflyResources(ctx, freshGroup(t, c, fg))
		if err != nil {
			t.Fatalf("reconcileDragonflyResources: %v", err)
		}
		// The client follows status before any process is replaced, as the
		// active Service follows the pod labels: a recreated pod is not
		// routed to (its template labels it a replica), so the client must
		// not reach the new, empty instance of a former master either.
		route := func() string {
			active := dragonflyStatusOf(t, c, key).ActiveSite
			activeAddr.Store(published[dragonflyAddr(fg, active)])
			return active
		}
		route()
		mgr.Tick(ctx)
		active := route()
		time.Sleep(50 * time.Millisecond) // the client's write in flight ends
		kubelet()
		// The cache is never lost: whenever the site status names answers
		// as a master, it holds the seeded keys.
		if s, ok := look(active); ok && s.info.Role == "master" && s.keys < seeded {
			t.Fatalf("status names %s as the Dragonfly master and it holds %d keys, fewer than the %d seeded: the cache was lost",
				active, s.keys, seeded)
		}
		return requeue
	}
	inSync := func(replica, master string) bool {
		rs, ok := look(replica)
		ms, mok := look(master)
		return ok && mok && ms.info.Role == "master" && rs.info.Role == "slave" && rs.info.MasterLinkStatus == "up" &&
			!rs.info.MasterSyncInProgress && rs.keys >= seeded
	}
	for deadline := time.Now().Add(90 * time.Second); !inSync("dc2", "dc1"); time.Sleep(300 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("dc2 did not get in sync with dc1 before the rollout")
		}
		step()
	}
	if replaced["dc1"] != 0 || replaced["dc2"] != 0 {
		t.Fatalf("processes replaced before any template change: %v", replaced)
	}

	// The template change: any field the render owns.
	live := freshGroup(t, c, fg)
	live.Spec.Dragonfly.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("500m")
	if err := c.Update(ctx, live); err != nil {
		t.Fatalf("update spec.dragonfly.resources: %v", err)
	}
	takeoverSeen := false
	done := false
	for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline); time.Sleep(300 * time.Millisecond) {
		requeue := step()
		active := dragonflyStatusOf(t, c, key).ActiveSite
		if active == "dc2" && !takeoverSeen {
			takeoverSeen = true
			if replaced["dc2"] != 1 {
				t.Fatalf("the master was taken over to dc2 after %d replacements of dc2, want 1 (replica first)", replaced["dc2"])
			}
			t.Logf("takeover to dc2 after %d acknowledged writes", acked.Load())
		}
		if !takeoverSeen && replaced["dc1"] != 0 {
			t.Fatal("dc1's process was replaced while status named it the Dragonfly master")
		}
		if requeue == 0 && takeoverSeen && inSync("dc1", "dc2") {
			done = true
			break
		}
	}
	stop()
	if !done {
		d1, _ := look("dc1")
		d2, _ := look("dc2")
		t.Fatalf("the rollout did not finish: status=%+v dc1=%+v dc2=%+v replaced=%v events=%v", dragonflyStatusOf(t, c, key), d1, d2, replaced,
			drainEventsCh(recorder.Events, 4096, 50*time.Millisecond))
	}

	// Every key survived: the seed and every write that got an OK. A write
	// whose reply was lost was retried with the same key, so the count is
	// exact up to the one write in flight when the writer stopped.
	want := seeded + acked.Load()
	var master, replica state
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		master, _ = look("dc2")
		replica, _ = look("dc1")
		if (master.keys == want || master.keys == want+1) && replica.keys == master.keys {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("keys after the rollout: dc2 (master) %d, dc1 (replica) %d; want %d (%d seeded + %d acknowledged writes) on both",
				master.keys, replica.keys, want, seeded, acked.Load())
		}
	}
	if master.info.Role != "master" || replica.info.Role != "slave" || replica.info.MasterLinkStatus != "up" {
		t.Fatalf("after the rollout dc2 reports %+v and dc1 %+v; want dc2 the master and dc1 its linked replica", master.info, replica.info)
	}
	if replaced["dc1"] != 1 || replaced["dc2"] != 1 {
		t.Fatalf("processes replaced: %v, want each site once", replaced)
	}
	for _, site := range sites {
		sts := stsOf(site)
		if got := sts.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]; got.String() != "500m" {
			t.Fatalf("StatefulSet %s cpu request = %s, want the new template", site, got.String())
		}
		if v := sts.Annotations[v1alpha1.DragonflyRolloutInFlightAnnotation]; v != "" {
			t.Fatalf("StatefulSet %s is still marked in flight (%s) after the rollout", site, v)
		}
	}
	events := drainEventsCh(recorder.Events, 4096, 50*time.Millisecond)
	for _, reason := range []string{v1alpha1.EventDragonflySessionsLost, v1alpha1.EventDragonflyRestartedInPlace, v1alpha1.EventDragonflySyncUnproven} {
		if eventsContain(events, reason) {
			t.Fatalf("the rollout emitted %s: %v", reason, events)
		}
	}
	t.Logf("rollout done on %s: %d keys on both sites (%d seeded, %d acknowledged writes), each process replaced once", image, master.keys, seeded, acked.Load())
}
