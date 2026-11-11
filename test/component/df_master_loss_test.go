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

// A killed master leaves a replica that holds the cache and reports its
// link down. It is promoted on the first missed poll: the master's pod is
// back within seconds as an empty master, and the cache would go with the
// reattach.
func TestDragonflyOnlyPromotionOfAKilledMasterDoesNotWait(t *testing.T) {
	replica := &keyedDF{keys: 3, stubDF: &stubDF{info: dragonfly.ReplicationInfo{
		Role: "slave", MasterHost: "orders-dragonfly-iad.ns.svc.cluster.local", MasterPort: 6379,
		MasterLinkStatus: "down", MasterLastIOSecondsAgo: 1}}}
	mgr, c, key := dfMasterLossGroup(t, replica)

	mgr.Tick(context.Background())
	if got := dfActiveSite(t, c, key); got != "pdx" {
		t.Fatalf("dragonfly activeSite = %q after the first missed poll of a killed master, want pdx", got)
	}
}
