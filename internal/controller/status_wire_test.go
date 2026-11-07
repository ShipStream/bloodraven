package controller

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// K074/K076: /status and /ws/status carry the failover record in one
// shape (lastFailover{time, target, promotionGtidExecuted}), and
// /ws/status carries the ordered update's current phase.
func TestStatusWireShapesAgree(t *testing.T) {
	fg := newTestFG()
	tm := NewTopologyManager(mustTopologyConfig(fg), []mysql.Checker{&mockMySQL{}, &mockMySQL{}}, NewFailoverController(testLogger()),
		nil, nil, BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	if rest := tm.Status(); rest.LastFailover != nil {
		t.Fatalf("/status lastFailover before any failover = %+v, want omitted", rest.LastFailover)
	}
	at := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	tm.mu.Lock()
	tm.lastFailover, tm.lastFailoverTarget, tm.promotionGtidExecuted = at, "dc2", "uuid:1-10"
	tm.mu.Unlock()
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.updater.setPhase("UpdatingReplica")

	restRaw, err := json.Marshal(tm.Status())
	if err != nil {
		t.Fatal(err)
	}
	tm.mu.RLock()
	wsRaw, err := json.Marshal(tm.topologyMessageLocked(nil, ""))
	tm.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	type wire struct {
		LastFailover *platform.FailoverRecord `json:"lastFailover"`
		UpdatePhase  string                   `json:"updatePhase"`
	}
	var rest, ws wire
	if err := json.Unmarshal(restRaw, &rest); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(wsRaw, &ws); err != nil {
		t.Fatal(err)
	}
	want := platform.FailoverRecord{Time: at.Format(time.RFC3339), Target: "dc2", PromotionGtidExecuted: "uuid:1-10"}
	if rest.LastFailover == nil || *rest.LastFailover != want || ws.LastFailover == nil || *ws.LastFailover != want {
		t.Fatalf("/status lastFailover %+v, /ws/status %+v; want both %+v", rest.LastFailover, ws.LastFailover, want)
	}
	if ws.UpdatePhase != "UpdatingReplica" {
		t.Fatalf("/ws/status updatePhase = %q, want the updater's phase", ws.UpdatePhase)
	}
}
