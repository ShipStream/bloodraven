package controller

import (
	"context"
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestImageTag(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  string
		want string
	}{
		{"plain tag", "ghcr.io/shipstream/bloodraven-sidecar:0.9.0", "0.9.0"},
		{"no registry", "bloodraven-sidecar:0.9.0", "0.9.0"},
		{"registry with port", "registry.local:5000/shipstream/bloodraven:1.2.3", "1.2.3"},
		{"registry port, no tag", "registry.local:5000/shipstream/bloodraven", ""},
		{"digest pin", "ghcr.io/shipstream/bloodraven@sha256:abc123", ""},
		{"tag and digest", "ghcr.io/shipstream/bloodraven:0.9.0@sha256:abc123", ""},
		{"untagged", "bloodraven", ""},
		{"empty", "", ""},
		{"whitespace", "   ", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := imageTag(tc.ref); got != tc.want {
				t.Errorf("imageTag(%q) = %q, want %q", tc.ref, got, tc.want)
			}
		})
	}
}

// drainSkewEvents collects events the recorder buffered, without
// blocking when none were emitted.
func drainSkewEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestWarnOnSidecarVersionSkew(t *testing.T) {
	// operatorImageFromEnv is package state shared with the backup
	// schedule reconciler; restore it so test order cannot matter.
	saved := operatorImageFromEnv
	t.Cleanup(func() { operatorImageFromEnv = saved })

	for _, tc := range []struct {
		name          string
		operatorImage string
		sidecarImage  string
		tls           bool
		wantEvent     bool
		wantContains  []string
	}{
		{
			name:          "matching tags stay quiet",
			operatorImage: "ghcr.io/shipstream/bloodraven:0.9.0",
			sidecarImage:  "ghcr.io/shipstream/bloodraven-sidecar:0.9.0",
			wantEvent:     false,
		},
		{
			name:          "skew warns",
			operatorImage: "ghcr.io/shipstream/bloodraven:0.10.0",
			sidecarImage:  "ghcr.io/shipstream/bloodraven-sidecar:0.9.0",
			wantEvent:     true,
			wantContains:  []string{"SidecarVersionSkew", "0.9.0", "0.10.0"},
		},
		{
			name:          "skew on a TLS group explains the specific breakage",
			operatorImage: "ghcr.io/shipstream/bloodraven:0.10.0",
			sidecarImage:  "ghcr.io/shipstream/bloodraven-sidecar:0.9.0",
			tls:           true,
			wantEvent:     true,
			wantContains:  []string{"BLOODRAVEN_MYSQL_TLS_", "super_read_only"},
		},
		{
			name:          "digest-pinned sidecar is not guessed at",
			operatorImage: "ghcr.io/shipstream/bloodraven:0.10.0",
			sidecarImage:  "ghcr.io/shipstream/bloodraven-sidecar@sha256:abc",
			wantEvent:     false,
		},
		{
			name:          "unknown operator image stays quiet",
			operatorImage: "",
			sidecarImage:  "ghcr.io/shipstream/bloodraven-sidecar:0.9.0",
			wantEvent:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operatorImageFromEnv = tc.operatorImage

			fg := newTestFG()
			fg.Spec.SidecarImage = tc.sidecarImage
			if tc.tls {
				fg = encTestFG()
				fg.Spec.SidecarImage = tc.sidecarImage
			}

			rec := record.NewFakeRecorder(10)
			r := &MysqlFailoverGroupReconciler{Recorder: rec}
			r.warnOnSidecarVersionSkew(context.Background(), fg)

			events := drainSkewEvents(rec)
			if !tc.wantEvent {
				if len(events) != 0 {
					t.Fatalf("expected no event, got %v", events)
				}
				return
			}
			if len(events) != 1 {
				t.Fatalf("expected exactly one event, got %v", events)
			}
			for _, want := range tc.wantContains {
				if !strings.Contains(events[0], want) {
					t.Errorf("event should mention %q: %s", want, events[0])
				}
			}
		})
	}
}

// TestWarnOnSidecarVersionSkew_NeverBlocks documents the deliberate
// choice: skew is reported, never enforced. A re-tagged or locally built
// image is indistinguishable from a real skew, and failing the reconcile
// on a version string would turn that ambiguity into an outage.
func TestWarnOnSidecarVersionSkew_NeverBlocks(t *testing.T) {
	saved := operatorImageFromEnv
	t.Cleanup(func() { operatorImageFromEnv = saved })
	operatorImageFromEnv = "ghcr.io/shipstream/bloodraven:9.9.9"

	fg := encTestFG()
	fg.Spec.SidecarImage = "some-local-build:dev"
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, _ := encReconciler(t, scriptedKeyring(nil), objs...)

	// Reconcile must still succeed and render the site Deployment.
	reconcileOnce(t, r)
	if d := getDeployment(t, r, "dc1"); d == nil {
		t.Fatal("version skew must not stop the Deployment from rendering")
	}
}
