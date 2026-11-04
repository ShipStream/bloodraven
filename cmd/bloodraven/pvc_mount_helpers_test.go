package main

import (
	"context"
	"testing"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

// useTempDirMountVerifier makes subcommands accept PVC mounts under the
// system temporary directory for the rest of the test. Only for tests
// that drive a subcommand end to end and cannot pass a verifier in the
// config; it swaps a package variable, so callers must not run in
// parallel.
func useTempDirMountVerifier(t *testing.T) {
	t.Helper()
	prev := newArchiveStore
	newArchiveStore = func(ctx context.Context, cfg *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
		if cfg.PVC != nil {
			pvc := *cfg.PVC
			pvc.MountVerifier = sidecar.TempDirMountVerifierForTest
			withVerifier := *cfg
			withVerifier.PVC = &pvc
			cfg = &withVerifier
		}
		return prev(ctx, cfg)
	}
	t.Cleanup(func() { newArchiveStore = prev })
}
