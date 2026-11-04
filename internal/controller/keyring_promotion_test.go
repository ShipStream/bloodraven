package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestRotationBlockedSites(t *testing.T) {
	if got := rotationBlockedSites(nil); got != nil {
		t.Fatalf("nil fg = %v, want nil", got)
	}
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			EncryptionAtRest: &v1alpha1.EncryptionAtRestSpec{Enabled: true},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			EncryptionAtRest: &v1alpha1.EncryptionAtRestStatus{
				Sites: []v1alpha1.SiteEncryptionStatus{
					{Name: "pdx", UnsealReason: v1alpha1.UnsealReasonRotation},
					{Name: "iad", UnsealReason: v1alpha1.UnsealReasonClone},
					{Name: "fra", UnsealReason: v1alpha1.UnsealReasonRotation},
				},
			},
		},
	}
	got := rotationBlockedSites(fg)
	if len(got) != 2 || got[0] != "fra" || got[1] != "pdx" {
		t.Fatalf("got %v, want sorted [fra pdx]", got)
	}
	fg.Spec.EncryptionAtRest.Enabled = false
	if got := rotationBlockedSites(fg); got != nil {
		t.Fatalf("encryption off = %v, want nil", got)
	}
}

func TestReserveKeyringRotation_RefusedWhileOrderedUpdateInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(tm *TopologyManager)
	}{
		{"pending", func(tm *TopologyManager) { tm.deploymentUpdatePending = true }},
		{"running", func(tm *TopologyManager) { tm.deploymentUpdateRunning = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
			tm.mu.Lock()
			tc.set(tm)
			tm.mu.Unlock()
			ok, why := tm.ReserveKeyringRotation("dc2")
			if ok || why != "an ordered update is in progress" {
				t.Fatalf("ReserveKeyringRotation = %v, %q; want refused for the ordered update", ok, why)
			}
			tm.mu.RLock()
			blocked := tm.keyringRotationBlockedLocked("dc2")
			tm.mu.RUnlock()
			if blocked {
				t.Fatal("a refused reservation must not block promotion")
			}
		})
	}
}

func TestReserveKeyringRotation_BlocksPromotionUntilStampedOrReleased(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	if ok, why := tm.ReserveKeyringRotation("dc2"); !ok {
		t.Fatalf("reservation refused with no update in flight: %s", why)
	}
	tm.mu.RLock()
	block := tm.handoffBlockLocked("dc2")
	dirty := tm.keyringRotationBlockedDirty
	tm.mu.RUnlock()
	if !block.rotation || !dirty {
		t.Fatalf("reserved site: rotation=%v dirty=%v, want both true", block.rotation, dirty)
	}
	if err := tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Fatal("live handoff check accepted a reserved site")
	}
	if _, err := tm.PlannedPromote(context.Background(), "dc2", "dc1"); !errors.Is(err, errKeyringRotationBlocked) {
		t.Fatalf("PlannedPromote onto a reserved site: err = %v, want errKeyringRotationBlocked", err)
	}

	// The durable stamp supersedes the reservation; clearing the stamp
	// then unblocks the site.
	tm.SetKeyringRotationBlocked([]string{"dc2"})
	tm.SetKeyringRotationBlocked(nil)
	tm.mu.RLock()
	blocked := tm.keyringRotationBlockedLocked("dc2")
	tm.mu.RUnlock()
	if blocked {
		t.Fatal("reservation outlived the durable stamp that superseded it")
	}

	// A reservation whose stamp never persisted is released explicitly.
	tm.ReserveKeyringRotation("dc2")
	tm.ReleaseKeyringRotationReservations([]string{"dc2"})
	if err := tm.liveHandoffRefusal("dc2")(); err != nil {
		t.Fatalf("released reservation still blocks the handoff: %v", err)
	}
}

func TestHandoffBlockErr(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block handoffBlock
		want  string
	}{
		{"eligible", handoffBlock{}, ""},
		{"rotation", handoffBlock{rotation: true}, "UnsealReason=Rotation"},
		{"reclone", handoffBlock{reclonePending: true}, "pending post-restore reclone"},
		{"divergent", handoffBlock{divergence: siteRecovery{state: recoveryStateBlocked, divergentCount: 3}}, "3 transactions the primary lacks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.block.err("iad")
			if tc.want == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
