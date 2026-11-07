package controller

import (
	"os"
	"testing"
)

// testSidecarImage is the operator's default sidecar image in this
// package's tests, standing in for the chart's BLOODRAVEN_SIDECAR_IMAGE.
const testSidecarImage = "bloodraven-sidecar:test"

func TestMain(m *testing.M) {
	SetSidecarImageDefault(testSidecarImage)
	os.Exit(m.Run())
}
