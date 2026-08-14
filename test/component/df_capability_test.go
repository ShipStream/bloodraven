package component

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// stubDF is a DragonflyConnection used through the exported manager APIs.
type stubDF struct {
	info                    dragonfly.ReplicationInfo
	replTakeoverErr         error
	replTakeoverUnsupported bool
}

func (s *stubDF) Ping(context.Context) error { return nil }
func (s *stubDF) InfoReplication(context.Context) (dragonfly.ReplicationInfo, error) {
	return s.info, nil
}
func (s *stubDF) InfoPersistence(context.Context) (dragonfly.PersistenceInfo, error) {
	return dragonfly.PersistenceInfo{}, nil
}
func (s *stubDF) ReplicaOf(context.Context, string, int32) error { return nil }
func (s *stubDF) ReplicaOfNoOne(context.Context) error           { return nil }
func (s *stubDF) ReplTakeover(context.Context, time.Duration) error {
	return s.replTakeoverErr
}
func (s *stubDF) Save(context.Context) error                   { return nil }
func (s *stubDF) ClientKillType(context.Context, string) error { return nil }
func (s *stubDF) HasCommand(context.Context, string) (bool, error) {
	return !s.replTakeoverUnsupported, nil
}
func (s *stubDF) Close() error { return nil }

func TestDragonflyCapabilityProbeAndLoudFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}},
			Dragonfly: &v1alpha1.DragonflySpec{
				Enabled: true,
				Image:   "example.invalid/dragonfly:old",
				Port:    6379,
			},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "iad"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		Build()
	key := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	rec := record.NewFakeRecorder(16)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	mgr := controller.NewDragonflyManager(c, rec, logger, key, time.Second)

	missing := &stubDF{
		info:                    dragonfly.ReplicationInfo{Role: "master"},
		replTakeoverUnsupported: true,
		replTakeoverErr:         context.DeadlineExceeded,
	}
	replica := &stubDF{
		info: dragonfly.ReplicationInfo{
			Role: "slave", MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0,
		},
		replTakeoverUnsupported: true,
		replTakeoverErr:         context.DeadlineExceeded,
	}
	mgr.SetConnector(func(_ context.Context, addr, _ string) (controller.DragonflyConnection, error) {
		if strings.Contains(addr, "iad") {
			return missing, nil
		}
		return replica, nil
	})

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Dragonfly == nil || got.Status.Dragonfly.ReplTakeoverSupported == nil || *got.Status.Dragonfly.ReplTakeoverSupported {
		t.Fatalf("probe did not surface missing capability: %+v", got.Status.Dragonfly)
	}

	probeEvents := drainRec(rec, 4, 100*time.Millisecond)
	if !eventHas(probeEvents, controller.ReasonDragonflyReplTakeoverUnsupported) {
		t.Fatalf("missing unsupported event: %v", probeEvents)
	}

	before := readDFCounter(t, metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, "pdx", "sessions_lost"))
	if ok := mgr.TryEmergencyPromote(context.Background(), "pdx", "iad"); !ok {
		t.Fatal("fallback must still return true")
	}
	after := readDFCounter(t, metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, "pdx", "sessions_lost"))
	if after != before+1 {
		t.Errorf("sessions_lost = %v, want %v", after, before+1)
	}
	fbEvents := drainRec(rec, 4, 100*time.Millisecond)
	if !eventHas(fbEvents, controller.ReasonDragonflySessionsLost) {
		t.Fatalf("missing sessions-lost event: %v", fbEvents)
	}
}

func drainRec(rec *record.FakeRecorder, max int, wait time.Duration) []string {
	out := make([]string, 0, max)
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for i := 0; i < max; i++ {
		select {
		case ev := <-rec.Events:
			out = append(out, ev)
		case <-deadline.C:
			return out
		}
	}
	return out
}

func eventHas(events []string, needle string) bool {
	for _, ev := range events {
		if strings.Contains(ev, needle) {
			return true
		}
	}
	return false
}

func readDFCounter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m io_prometheus_client.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if m.Counter == nil {
		return 0
	}
	return *m.Counter.Value
}
