package component

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// keyedDF is a stubDF that holds keys.
type keyedDF struct {
	*stubDF
	keys int64
}

func (k *keyedDF) KeyCount(context.Context) (int64, error) { return k.keys, nil }

// dfMasterLossGroup is a two-site group whose Dragonfly master is iad (the
// MySQL active site), with the manager's connector answering for pdx with
// replica and failing every dial of iad.
func dfMasterLossGroup(t *testing.T, replica controller.DragonflyConnection) (*controller.DragonflyManager, client.Client, types.NamespacedName) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites:     []v1alpha1.SiteSpec{{Name: "iad", ServerID: 101}, {Name: "pdx", ServerID: 102}},
			Dragonfly: &v1alpha1.DragonflySpec{Enabled: true, Image: "example.invalid/dragonfly:v1", Port: 6379},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "iad", Dragonfly: &v1alpha1.DragonflyStatus{ActiveSite: "iad"}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	key := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	mgr := controller.NewDragonflyManager(c, record.NewFakeRecorder(64), logger, key, time.Second)
	mgr.SetConnector(func(_ context.Context, addr, _ string) (controller.DragonflyConnection, error) {
		if strings.Contains(addr, "iad") {
			return nil, errors.New("dial tcp: i/o timeout")
		}
		return replica, nil
	})
	return mgr, c, key
}

func dfActiveSite(t *testing.T, c client.Client, key types.NamespacedName) string {
	t.Helper()
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Dragonfly == nil {
		t.Fatal("status.dragonfly not set")
	}
	return got.Status.Dragonfly.ActiveSite
}

// A Dragonfly master that only the operator cannot reach (the replica's
// link to it is up) is replaced after spec.failureThreshold consecutive
// missed polls, not on the first one.
func TestDragonflyOnlyPromotionWaitsForTheFailureThreshold(t *testing.T) {
	replica := &keyedDF{keys: 3, stubDF: &stubDF{info: dragonfly.ReplicationInfo{
		Role: "slave", MasterHost: "orders-dragonfly-iad.ns.svc.cluster.local", MasterPort: 6379,
		MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0}}}
	mgr, c, key := dfMasterLossGroup(t, replica)

	mgr.Tick(context.Background())
	mgr.Tick(context.Background())
	if got := dfActiveSite(t, c, key); got != "iad" {
		t.Fatalf("dragonfly activeSite = %q after two missed polls, want iad", got)
	}
	mgr.Tick(context.Background())
	if got := dfActiveSite(t, c, key); got != "pdx" {
		t.Fatalf("dragonfly activeSite = %q after three missed polls, want pdx", got)
	}
}

// A replica that holds the cache and reports its link to the master down
// does not shorten the wait: that is what a killed master leaves, and also
// what a refused path to a live master leaves for hours. One missed poll of
// the master beside it promotes nothing.
func TestDragonflyOnlyPromotionWithALinkDownReplicaWaitsTheSame(t *testing.T) {
	replica := &keyedDF{keys: 3, stubDF: &stubDF{info: dragonfly.ReplicationInfo{
		Role: "slave", MasterHost: "orders-dragonfly-iad.ns.svc.cluster.local", MasterPort: 6379,
		MasterLinkStatus: "down", MasterLastIOSecondsAgo: 1}}}
	mgr, c, key := dfMasterLossGroup(t, replica)

	mgr.Tick(context.Background())
	mgr.Tick(context.Background())
	if got := dfActiveSite(t, c, key); got != "iad" {
		t.Fatalf("dragonfly activeSite = %q after two missed polls beside a link-down replica, want iad", got)
	}
	mgr.Tick(context.Background())
	if got := dfActiveSite(t, c, key); got != "pdx" {
		t.Fatalf("dragonfly activeSite = %q after three missed polls, want pdx", got)
	}
}

// recordingDF is a keyedDF that records the role commands it is sent.
type recordingDF struct {
	*keyedDF
	replicaOf []string
	noOne     int
	takeovers int
}

func (r *recordingDF) ReplicaOf(_ context.Context, host string, _ int32) error {
	r.replicaOf = append(r.replicaOf, host)
	return nil
}

func (r *recordingDF) ReplicaOfNoOne(context.Context) error { r.noOne++; return nil }

func (r *recordingDF) ReplTakeover(context.Context, time.Duration) error {
	r.takeovers++
	return errors.New("ERR Full sync not done")
}

// A master that is killed and back before the threshold answers as a new
// instance: a new replication id, and no keys or the few a client wrote
// since. Its replica still follows the earlier instance and holds the
// cache. It is promoted with REPLICAOF NO ONE; it is not sent the REPLICAOF
// that would empty it. (Dragonfly v1.38.0 and v2.1: the replica keeps its
// keys and master_replid with the link down until it is sent REPLICAOF.)
func TestDragonflyReplicaIsPromotedOverAMasterThatRestartedWithoutTheCache(t *testing.T) {
	const oldID, newID = "8c03c0ed9d972e36e6aaaa5a8222cabea1de09fd", "785aff8f33fc1d1f538aba5a75b76441669bdc2f"
	for _, tc := range []struct {
		name       string
		masterKeys int64
	}{
		{"the restarted master holds no keys", 0},
		{"a client already wrote to the restarted master", 49},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replica := &recordingDF{keyedDF: &keyedDF{keys: 300, stubDF: &stubDF{info: dragonfly.ReplicationInfo{
				Role: "slave", MasterHost: "orders-dragonfly-iad.ns.svc.cluster.local", MasterPort: 6379,
				MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0, MasterReplID: oldID}}}}
			master := &keyedDF{keys: 300, stubDF: &stubDF{info: dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1, MasterReplID: oldID}}}
			mgr, c, key := dfMasterLossGroup(t, replica)
			masterUp := true
			mgr.SetConnector(func(_ context.Context, addr, _ string) (controller.DragonflyConnection, error) {
				if strings.Contains(addr, "iad") {
					if !masterUp {
						return nil, errors.New("dial tcp: connection refused")
					}
					return master, nil
				}
				return replica, nil
			})

			mgr.Tick(context.Background()) // in sync
			masterUp = false
			replica.info.MasterLinkStatus = "down"
			mgr.Tick(context.Background()) // one missed poll
			if got := dfActiveSite(t, c, key); got != "iad" {
				t.Fatalf("dragonfly activeSite = %q after one missed poll, want iad", got)
			}
			replica.replicaOf = nil

			masterUp = true
			master.info.MasterReplID, master.keys = newID, tc.masterKeys
			mgr.Tick(context.Background())
			if got := dfActiveSite(t, c, key); got != "pdx" {
				t.Fatalf("dragonfly activeSite = %q, want pdx: the replica holds the cache the restarted master lost", got)
			}
			if len(replica.replicaOf) != 0 || replica.noOne != 1 || replica.takeovers != 0 {
				t.Fatalf("replica was sent REPLICAOF %v, REPLICAOF NO ONE %d times, REPLTAKEOVER %d times; want only one REPLICAOF NO ONE",
					replica.replicaOf, replica.noOne, replica.takeovers)
			}
		})
	}
}
