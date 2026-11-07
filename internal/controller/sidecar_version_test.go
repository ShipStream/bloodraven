package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
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
		defaultImage  string
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
			name:          "unpinned group on a matching default stays quiet",
			operatorImage: "ghcr.io/shipstream/bloodraven:0.10.0",
			defaultImage:  "ghcr.io/shipstream/bloodraven-sidecar:0.10.0",
			wantEvent:     false,
		},
		{
			name:          "unpinned group on a skewed default names the default",
			operatorImage: "ghcr.io/shipstream/bloodraven:0.10.0",
			defaultImage:  "mirror.example/bloodraven-sidecar:0.9.0",
			wantEvent:     true,
			wantContains:  []string{"BLOODRAVEN_SIDECAR_IMAGE", "0.9.0", "0.10.0"},
		},
		{
			name:          "pin wins over the default",
			operatorImage: "ghcr.io/shipstream/bloodraven:0.10.0",
			sidecarImage:  "ghcr.io/shipstream/bloodraven-sidecar:0.9.0",
			defaultImage:  "ghcr.io/shipstream/bloodraven-sidecar:0.10.0",
			wantEvent:     true,
			wantContains:  []string{"spec.sidecar.image", "0.9.0"},
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
			defaultImage := tc.defaultImage
			if defaultImage == "" {
				defaultImage = testSidecarImage
			}
			SetSidecarImageDefault(defaultImage)
			t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })

			fg := newTestFG()
			if tc.tls {
				fg = encTestFG()
			}
			fg.Spec.Sidecar.Image = tc.sidecarImage

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
	fg.Spec.Sidecar.Image = "some-local-build:dev"
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

// TestEffectiveSidecarImage pins A-52: spec.sidecar.image is a pin, an
// empty one means the operator's default, and with neither the render is
// refused instead of producing a container without an image.
func TestEffectiveSidecarImage(t *testing.T) {
	t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })
	fg := newTestFG()

	SetSidecarImageDefault("registry.example/bloodraven-sidecar:2.0.0")
	if got, err := effectiveSidecarImage(fg); err != nil || got != "registry.example/bloodraven-sidecar:2.0.0" {
		t.Fatalf("unpinned = %q, %v; want the operator default", got, err)
	}
	fg.Spec.Sidecar.Image = "registry.example/bloodraven-sidecar:1.9.0"
	if got, err := effectiveSidecarImage(fg); err != nil || got != "registry.example/bloodraven-sidecar:1.9.0" {
		t.Fatalf("pinned = %q, %v; want the pin", got, err)
	}
	SetSidecarImageDefault("")
	if got, err := effectiveSidecarImage(fg); err != nil || got != "registry.example/bloodraven-sidecar:1.9.0" {
		t.Fatalf("pinned without a default = %q, %v; want the pin", got, err)
	}
	fg.Spec.Sidecar.Image = ""
	if _, err := effectiveSidecarImage(fg); !errors.Is(err, errSidecarImageUnset) {
		t.Fatalf("unpinned without a default: err = %v, want errSidecarImageUnset", err)
	}
}

// TestSpecHashFollowsSidecarDefault: a new operator default changes the
// spec hash of a group that pins no image, which is what rolls it, and
// leaves a pinned group's hash alone.
func TestSpecHashFollowsSidecarDefault(t *testing.T) {
	t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })
	unpinned := newTestFG()
	pinned := newTestFG()
	pinned.Spec.Sidecar.Image = "registry.example/bloodraven-sidecar:1.9.0"
	site := unpinned.Spec.Sites[0]

	SetSidecarImageDefault("registry.example/bloodraven-sidecar:2.0.0")
	u1 := ComputeSpecHash(unpinned, site, "", pitrRenderInputs{})
	p1 := ComputeSpecHash(pinned, site, "", pitrRenderInputs{})
	SetSidecarImageDefault("registry.example/bloodraven-sidecar:2.1.0")
	u2 := ComputeSpecHash(unpinned, site, "", pitrRenderInputs{})
	p2 := ComputeSpecHash(pinned, site, "", pitrRenderInputs{})
	if u1 == u2 {
		t.Fatal("a new sidecar default must change an unpinned group's spec hash")
	}
	if p1 != p2 {
		t.Fatal("a new sidecar default must not change a pinned group's spec hash")
	}
}

// TestSidecarImageRendered: the rendered sidecar and init containers run
// the effective image, and an unpinned group with no operator default is
// refused with the named error.
func TestSidecarImageRendered(t *testing.T) {
	t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })
	SetSidecarImageDefault("registry.example/bloodraven-sidecar:2.0.0")
	fg := newTestFG()
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, _ := encReconciler(t, scriptedKeyring(nil), objs...)
	reconcileOnce(t, r)
	d := getDeployment(t, r, "dc1")
	if d == nil {
		t.Fatal("Deployment not rendered")
	}
	var checked int
	for _, c := range append(append([]corev1.Container(nil), d.Spec.Template.Spec.InitContainers...), d.Spec.Template.Spec.Containers...) {
		if c.Name == "mysql" || !strings.Contains(c.Name, "sidecar") {
			continue
		}
		checked++
		if c.Image != "registry.example/bloodraven-sidecar:2.0.0" {
			t.Errorf("container %s image = %q, want the operator default", c.Name, c.Image)
		}
	}
	if checked == 0 {
		t.Fatal("no sidecar container rendered")
	}

	SetSidecarImageDefault("")
	dep := &appsv1.Deployment{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(d), dep); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
	var live v1alpha1.MysqlFailoverGroup
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(fg), &live); err != nil {
		t.Fatal(err)
	}
	err := r.reconcileDeployment(context.Background(), &live, live.Spec.Sites[0], 101, live.Spec.Image, groupPITRRender{})
	if !errors.Is(err, errSidecarImageUnset) {
		t.Fatalf("reconcileDeployment without a sidecar image: err = %v, want errSidecarImageUnset", err)
	}
}
