package sidecar

// testPVCConfig builds a PVC archive config whose mount check accepts
// temporary directories, so stores can live in t.TempDir().
func testPVCConfig(mount string) *PITRPVCConfig {
	return &PITRPVCConfig{MountPath: mount, MountVerifier: TempDirMountVerifierForTest}
}
