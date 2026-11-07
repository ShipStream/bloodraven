package controller

import (
	"context"
	"testing"
)

// K109: a site removed and re-added under its name with another serverID
// finds its retained Deployment rendered with the old --server-id; the
// spec hash covers the server ID, so that Deployment is drift and rolls.
func TestComputeSpecHash_CoversServerID(t *testing.T) {
	fg := newTestFG()
	site := fg.Spec.Sites[1]
	site.ServerID = 103
	before := ComputeSpecHash(fg, site, "", testPITRRender(fg))
	site.ServerID = 104
	if after := ComputeSpecHash(fg, site, "", testPITRRender(fg)); after == before {
		t.Fatal("a changed serverID left the spec hash unchanged")
	}
}

// K064: with no sidecar image to render (no pin, no operator default) the
// drift set is not evaluated, so the ordered updater is not handed drift
// the render then refuses to apply.
func TestSpecDriftView_RefusesWithoutSidecarImage(t *testing.T) {
	runner, c, fg, _ := lifecycleRunner(t)
	SetSidecarImageDefault("")
	t.Cleanup(func() { SetSidecarImageDefault(testSidecarImage) })
	fg.Spec.Sidecar.Image = ""
	if _, _, ok := runner.specDriftView(context.Background(), c, fg); ok {
		t.Fatal("drift was evaluated for a group whose Deployments cannot be rendered")
	}
}
