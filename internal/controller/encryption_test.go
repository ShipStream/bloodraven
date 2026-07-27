package controller

import (
	"encoding/json"
	"path"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// encTestFG returns the standard two-site fixture with encryption and
// TLS enabled (encryption requires TLS).
func encTestFG() *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Spec.TLS = &v1alpha1.TLSSpec{
		SecretName: "mysql-tls",
		IssuerRef:  v1alpha1.IssuerRef{Name: "ca", Kind: "Issuer"},
	}
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	return fg
}

func setSitePhase(fg *v1alpha1.MysqlFailoverGroup, site string, phase v1alpha1.SiteKeyringPhase, secret string) {
	if fg.Status.EncryptionAtRest == nil {
		fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{}
	}
	st := fg.Status.EncryptionAtRest
	for i := range st.Sites {
		if st.Sites[i].Name == site {
			st.Sites[i].Phase = phase
			st.Sites[i].KeyringSecret = secret
			return
		}
	}
	st.Sites = append(st.Sites, v1alpha1.SiteEncryptionStatus{
		Name: site, Phase: phase, KeyringSecret: secret,
	})
}

// --- my.cnf ---------------------------------------------------------

func TestEncryptionMySQLSettings_Disabled(t *testing.T) {
	if got := encryptionMySQLSettings(newTestFG()); got != nil {
		t.Errorf("expected nil settings when encryption is off, got %v", got)
	}
}

func TestEncryptionMySQLSettings_Coverage(t *testing.T) {
	fg := encTestFG()
	got := encryptionMySQLSettings(fg)
	want := map[string]string{
		"default-table-encryption":         "ON",
		"table-encryption-privilege-check": "ON",
		"innodb-redo-log-encrypt":          "ON",
		"innodb-undo-log-encrypt":          "ON",
		"binlog-encryption":                "ON",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("setting %s = %q, want %q", k, got[k], v)
		}
	}

	off := false
	fg.Spec.EncryptionAtRest.Coverage = &v1alpha1.EncryptionCoverageSpec{RedoLog: &off}
	got = encryptionMySQLSettings(fg)
	if got["innodb-redo-log-encrypt"] != "OFF" {
		t.Errorf("redo log = %q, want OFF", got["innodb-redo-log-encrypt"])
	}
	if got["binlog-encryption"] != "ON" {
		t.Errorf("unset coverage should stay ON, got %q", got["binlog-encryption"])
	}
}

// TestEncryptionSettingsOverrideUserConf is the security-relevant one:
// the whole claim rests on these settings being what the spec asked for,
// so a spec.mysqlConf entry naming the same key must not win.
func TestEncryptionSettingsOverrideUserConf(t *testing.T) {
	fg := encTestFG()
	fg.Spec.MysqlConf = map[string]string{
		"innodb_redo_log_encrypt":  "OFF",
		"binlog-encryption":        "OFF",
		"default_table_encryption": "OFF",
	}
	cnf := generateMyCnf(fg, fg.Spec.Sites[0])
	for _, want := range []string{
		"innodb-redo-log-encrypt=ON",
		"binlog-encryption=ON",
		"default-table-encryption=ON",
	} {
		if !strings.Contains(cnf, want) {
			t.Errorf("generated my.cnf must contain %q despite a user override:\n%s", want, cnf)
		}
	}
}

func TestGenerateMyCnf_NoEncryptionKeysWhenDisabled(t *testing.T) {
	cnf := generateMyCnf(newTestFG(), newTestFG().Spec.Sites[0])
	for _, forbidden := range []string{"default-table-encryption", "binlog-encryption", "innodb-redo-log-encrypt"} {
		if strings.Contains(cnf, forbidden) {
			t.Errorf("my.cnf must not mention %q when encryption is disabled:\n%s", forbidden, cnf)
		}
	}
}

// --- component files ------------------------------------------------

func TestKeyringManifestIsValidJSON(t *testing.T) {
	var m map[string]any
	if err := json.Unmarshal([]byte(keyringManifestJSON()), &m); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	if m["components"] != "file://component_keyring_file" {
		t.Errorf("components = %v", m["components"])
	}
}

func TestKeyringComponentConfig(t *testing.T) {
	// component_keyring_file refuses to initialize unless BOTH keys are
	// present, and InnoDB then aborts startup. Assert on the decoded
	// shape rather than a string so a formatting change cannot silently
	// drop one.
	for _, readOnly := range []bool{true, false} {
		var m map[string]any
		raw := keyringComponentConfigJSON("/run/mysql-keyring/keyring", readOnly)
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("component config is not valid JSON: %v\n%s", err, raw)
		}
		if m["path"] != "/run/mysql-keyring/keyring" {
			t.Errorf("path = %v", m["path"])
		}
		if m["read_only"] != readOnly {
			t.Errorf("read_only = %v, want %v", m["read_only"], readOnly)
		}
	}
}

func TestKeyringConfigMapData(t *testing.T) {
	if got := keyringConfigMapData(newTestFG(), false); got != nil {
		t.Errorf("expected no keyring keys when encryption is disabled, got %v", got)
	}
	sealed := keyringConfigMapData(encTestFG(), true)
	if !strings.Contains(sealed[keyringComponentKey], `"read_only": true`) {
		t.Errorf("sealed config should be read-only:\n%s", sealed[keyringComponentKey])
	}
	unsealed := keyringConfigMapData(encTestFG(), false)
	if !strings.Contains(unsealed[keyringComponentKey], `"read_only": false`) {
		t.Errorf("unsealed config should be writable:\n%s", unsealed[keyringComponentKey])
	}
}

func TestKeyringInitScript(t *testing.T) {
	// Fresh bootstrap: MySQL needs the file to exist (it aborts startup
	// on a missing keyring data file) but an empty one is fine.
	fresh := keyringInitScript("/run/mysql-keyring/keyring", false)
	if !strings.Contains(fresh, `: > "/run/mysql-keyring/keyring"`) {
		t.Errorf("fresh bootstrap must create an empty keyring:\n%s", fresh)
	}
	if strings.Contains(fresh, keyringSeedMountPath) {
		t.Errorf("fresh bootstrap must not reference a seed:\n%s", fresh)
	}

	seeded := keyringInitScript("/run/mysql-keyring/keyring", true)
	if !strings.Contains(seeded, path.Join(keyringSeedMountPath, "keyring")) {
		t.Errorf("seeded unseal must copy from the escrow Secret:\n%s", seeded)
	}
	for _, want := range []string{"chmod 600", `id -u`, "chown 999:999"} {
		if !strings.Contains(seeded, want) {
			t.Errorf("init script missing %q:\n%s", want, seeded)
		}
	}

	// The DIRECTORY has to be writable by mysqld's uid, not just the
	// file: component_keyring_file stores a new key by creating a temp
	// file alongside the keyring and renaming over it. Preparing only
	// the file leaves a root-owned directory and MySQL fails
	// --initialize with an opaque "InnoDB Database creation was aborted
	// with error Generic error" (verified against mysql:9.7).
	if !strings.Contains(fresh, `chmod 700 "/run/mysql-keyring"`) {
		t.Errorf("init script must prepare the keyring directory, not just the file:\n%s", fresh)
	}
	if !strings.Contains(fresh, `chown 999:999 "/run/mysql-keyring"`) {
		t.Errorf("init script must chown the keyring directory:\n%s", fresh)
	}

	// ...but only as root. A memory-backed emptyDir is root-owned 0777,
	// so a hardened non-root init container cannot chmod it and does not
	// need to. Doing it unconditionally trips `set -e` and wedges the pod.
	rootOnly := fresh[strings.Index(fresh, `if [ "$(id -u)" = "0" ]`):]
	for _, mustBeGuarded := range []string{
		`chown 999:999 "/run/mysql-keyring"`,
		`chmod 700 "/run/mysql-keyring"`,
	} {
		if !strings.Contains(rootOnly, mustBeGuarded) {
			t.Errorf("%q must be inside the root-only branch:\n%s", mustBeGuarded, fresh)
		}
	}
}

// --- pod fragments --------------------------------------------------

func findVolume(vols []corev1.Volume, name string) *corev1.Volume {
	for i := range vols {
		if vols[i].Name == name {
			return &vols[i]
		}
	}
	return nil
}

func findMount(mounts []corev1.VolumeMount, mountPath string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].MountPath == mountPath {
			return &mounts[i]
		}
	}
	return nil
}

func TestBuildEncryptionFragments_Disabled(t *testing.T) {
	frags := buildEncryptionFragments(newTestFG(), newTestFG().Spec.Sites[0], false, "", false)
	if len(frags.PodVolumes) != 0 || len(frags.MysqlVolumeMounts) != 0 ||
		len(frags.InitContainers) != 0 || len(frags.SidecarEnv) != 0 {
		t.Errorf("expected empty fragments when encryption is off: %+v", frags)
	}
}

func TestBuildEncryptionFragments_Sealed(t *testing.T) {
	fg := encTestFG()
	frags := buildEncryptionFragments(fg, fg.Spec.Sites[0], true, "mysql-lion-dc1-keyring-v2", false)

	vol := findVolume(frags.PodVolumes, keyringVolumeName)
	if vol == nil {
		t.Fatal("keyring volume missing")
	}
	if vol.Secret == nil {
		t.Fatal("sealed keyring must be a Secret projection, not an emptyDir — " +
			"a memory emptyDir would let mysqld write keys nobody escrowed")
	}
	if vol.Secret.SecretName != "mysql-lion-dc1-keyring-v2" {
		t.Errorf("sealed against %q", vol.Secret.SecretName)
	}
	if vol.Secret.DefaultMode == nil || *vol.Secret.DefaultMode != keyringSecretMode {
		t.Errorf("keyring mode = %v, want %o (mysqld's uid must be able to read it)", vol.Secret.DefaultMode, keyringSecretMode)
	}

	// A sealed site has no writable keyring, so it needs neither the
	// seeding init container nor an escrow credential.
	if len(frags.InitContainers) != 0 {
		t.Errorf("sealed site should not need a keyring-init container: %+v", frags.InitContainers)
	}
	if findVolume(frags.PodVolumes, keyringTokenVolumeName) != nil {
		t.Error("a sealed site must not carry an escrow token — there is nothing for it to escrow")
	}
	for _, e := range frags.SidecarEnv {
		if e.Name == "BLOODRAVEN_KEYRING_ESCROW" {
			t.Error("escrow must not be armed on a sealed site")
		}
	}

	mount := findMount(frags.MysqlVolumeMounts, fg.Spec.EffectiveKeyring().DataFileDir)
	if mount == nil || !mount.ReadOnly {
		t.Errorf("sealed keyring mount must be read-only: %+v", mount)
	}
}

func TestBuildEncryptionFragments_UnsealedFresh(t *testing.T) {
	fg := encTestFG()
	frags := buildEncryptionFragments(fg, fg.Spec.Sites[0], false, "", false)

	vol := findVolume(frags.PodVolumes, keyringVolumeName)
	if vol == nil || vol.EmptyDir == nil {
		t.Fatalf("unsealed keyring must be an emptyDir: %+v", vol)
	}
	if vol.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Error("keyring emptyDir must be memory-backed — the whole point is that " +
			"key material never reaches a node disk")
	}
	if vol.EmptyDir.SizeLimit == nil {
		t.Error("memory-backed keyring volume must be size-limited")
	}

	if len(frags.InitContainers) != 1 || frags.InitContainers[0].Name != "keyring-init" {
		t.Fatalf("expected one keyring-init container: %+v", frags.InitContainers)
	}
	if findVolume(frags.PodVolumes, keyringSeedVolumeName) != nil {
		t.Error("a fresh bootstrap has nothing to seed from")
	}
	if tok := findVolume(frags.PodVolumes, keyringTokenVolumeName); tok == nil || tok.Secret == nil {
		t.Fatal("unsealed site needs the escrow token mounted")
	}

	env := map[string]string{}
	for _, e := range frags.SidecarEnv {
		env[e.Name] = e.Value
	}
	if env["BLOODRAVEN_KEYRING_ESCROW"] != "1" {
		t.Error("escrow must be armed on an unsealed site")
	}
	if env["BLOODRAVEN_KEYRING_FILE"] != "/run/mysql-keyring/keyring" {
		t.Errorf("keyring file env = %q", env["BLOODRAVEN_KEYRING_FILE"])
	}
	if _, ok := env["BLOODRAVEN_KEYRING_ROTATE"]; ok {
		t.Error("rotation must not be requested unless asked for")
	}
}

func TestBuildEncryptionFragments_UnsealedSeeded(t *testing.T) {
	fg := encTestFG()
	frags := buildEncryptionFragments(fg, fg.Spec.Sites[0], false, "mysql-lion-dc1-keyring-v4", true)

	seed := findVolume(frags.PodVolumes, keyringSeedVolumeName)
	if seed == nil || seed.Secret == nil || seed.Secret.SecretName != "mysql-lion-dc1-keyring-v4" {
		t.Fatalf("unseal from an existing version must mount the seed: %+v", seed)
	}
	if len(frags.InitContainers) != 1 {
		t.Fatalf("expected keyring-init container: %+v", frags.InitContainers)
	}
	if m := findMount(frags.InitContainers[0].VolumeMounts, keyringSeedMountPath); m == nil {
		t.Error("keyring-init must mount the seed so it can copy the previous keyring in")
	}

	var rotate bool
	for _, e := range frags.SidecarEnv {
		if e.Name == "BLOODRAVEN_KEYRING_ROTATE" && e.Value == "1" {
			rotate = true
		}
	}
	if !rotate {
		t.Error("rotation unseal must arm the sidecar's rotation step")
	}
}

func TestBuildEncryptionFragments_ComponentFilePaths(t *testing.T) {
	fg := encTestFG()
	fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{
		MysqldDir: "/opt/mysql/bin",
		PluginDir: "/opt/mysql/lib/plugin",
	}
	frags := buildEncryptionFragments(fg, fg.Spec.Sites[0], true, "s", false)

	// MySQL only reads the global manifest from the mysqld directory and
	// the global component config from plugin_dir. Both have to be
	// subPath mounts: mounting a volume over either directory would hide
	// the mysqld binary or the component .so.
	manifest := findMount(frags.MysqlVolumeMounts, "/opt/mysql/bin/mysqld.my")
	if manifest == nil {
		t.Fatal("manifest must be mounted next to the mysqld binary")
	}
	if manifest.SubPath != keyringManifestKey {
		t.Errorf("manifest subPath = %q, want %q", manifest.SubPath, keyringManifestKey)
	}
	if manifest.Name != "config" {
		t.Errorf("manifest should come from the per-site ConfigMap volume, got %q", manifest.Name)
	}

	cnf := findMount(frags.MysqlVolumeMounts, "/opt/mysql/lib/plugin/component_keyring_file.cnf")
	if cnf == nil || cnf.SubPath != keyringComponentKey {
		t.Fatalf("component config must be subPath-mounted into plugin_dir: %+v", cnf)
	}
}

func TestKeyringDigestStable(t *testing.T) {
	a := keyringDigest([]byte("abc"))
	if !strings.HasPrefix(a, "sha256:") {
		t.Errorf("digest = %q, want sha256: prefix", a)
	}
	if a != keyringDigest([]byte("abc")) {
		t.Error("digest must be deterministic")
	}
	if a == keyringDigest([]byte("abd")) {
		t.Error("digest must change with content")
	}
}

// --- spec hash ------------------------------------------------------

func TestComputeSpecHash_SealTransitionRollsPod(t *testing.T) {
	// Sealing changes the keyring volume from a memory emptyDir to a
	// Secret projection and flips read_only in the component config.
	// If the hash did not move, the ordered-update path would never roll
	// the pod and the site would sit "sealed" in status while still
	// running a writable keyring.
	base := encTestFG()
	setSitePhase(base, "dc1", v1alpha1.KeyringPhaseUnsealed, "")
	unsealed := ComputeSpecHash(base, base.Spec.Sites[0], nil, nil)

	sealed := encTestFG()
	setSitePhase(sealed, "dc1", v1alpha1.KeyringPhaseSealed, "mysql-lion-dc1-keyring-v1")
	sealedHash := ComputeSpecHash(sealed, sealed.Spec.Sites[0], nil, nil)

	if unsealed == sealedHash {
		t.Fatal("spec hash must change when a site is sealed")
	}

	// A new escrow version (rotation) must also roll the pod, otherwise
	// the pod keeps projecting the old Secret.
	v2 := encTestFG()
	setSitePhase(v2, "dc1", v1alpha1.KeyringPhaseSealed, "mysql-lion-dc1-keyring-v2")
	if ComputeSpecHash(v2, v2.Spec.Sites[0], nil, nil) == sealedHash {
		t.Fatal("spec hash must change when the escrow version changes")
	}
}

func TestComputeSpecHash_UnchangedWhenEncryptionDisabled(t *testing.T) {
	// Existing unencrypted clusters must not roll just because the
	// operator learned about encryption.
	a := ComputeSpecHash(newTestFG(), newTestFG().Spec.Sites[0], nil, nil)
	b := ComputeSpecHash(newTestFG(), newTestFG().Spec.Sites[0], nil, nil)
	if a != b {
		t.Fatal("hash must be stable")
	}
	if strings.Contains(a, "encryption") {
		t.Fatal("sanity: hash is a hex digest")
	}
}
