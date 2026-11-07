package component

import (
	"os"
	"testing"

	"github.com/shipstream/bloodraven/internal/controller"
)

// TestMain stands in for the chart's BLOODRAVEN_SIDECAR_IMAGE: groups that
// leave spec.sidecar.image empty render this image.
func TestMain(m *testing.M) {
	controller.SetSidecarImageDefault("bloodraven-sidecar:component")
	os.Exit(m.Run())
}
