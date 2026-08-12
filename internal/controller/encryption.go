package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// Bump when encryption pod rendering changes without a corresponding
	// CRD field change, so already-encrypted pods roll forward onto the
	// new rendering. ComputeSpecHash includes this value.
	encryptionPodRenderVersion = "encryption-pod-render-v17"

	// ConfigMap keys carrying the two files MySQL insists on reading
	// from image-owned directories. They live in the existing per-site
	// ConfigMap; the mysql container copies them into place before
	// starting mysqld (see encryptionMysqlLauncher).
	keyringManifestKey  = "keyring-manifest.json"
	keyringComponentKey = "keyring-component.json"

	// Volume names. Kept short and stable — they appear in the rendered
	// PodSpec and therefore in the Deployment diff.
	keyringVolumeName      = "keyring"
	keyringSeedVolumeName  = "keyring-seed"
	keyringTokenVolumeName = "keyring-token"

	// Mount points for the two operator-managed Secrets that never go
	// near mysqld: the seed (previous escrow version, consumed by the
	// init container) and the escrow bearer token (consumed by the
	// sidecar).
	keyringSeedMountPath  = "/run/bloodraven/keyring-seed"
	keyringTokenMountPath = "/run/bloodraven/keyring-token"

	// keyringComponentSrcMount is where the per-site ConfigMap is
	// mounted into the mysql container so the launcher can copy the
	// component files into place.
	keyringComponentSrcMount = "/run/bloodraven/keyring-component-src"

	// Official MySQL Community image entrypoint. Used by the encryption
	// launcher so we still get the image's init/user-switch logic.
	mysqlDockerEntrypoint = "/usr/local/bin/docker-entrypoint.sh"

	// mysqlRuntimeBinDir holds a private copy of mysqld plus its global
	// component manifest. Ubuntu hosts can carry a path-attached
	// /usr/sbin/mysqld AppArmor profile. That host profile attaches after
	// the container runtime's Unconfined choice and then denies files in
	// the container overlay, which MySQL misleadingly reports as a missing
	// errmsg.sys and an unloaded keyring component. Executing the identical
	// binary from this Bloodraven-owned path avoids that host-only profile.
	mysqlRuntimeBinDir = "/opt/bloodraven/mysql/bin"

	// keyringVolumeSizeLimit caps the memory-backed keyring emptyDir.
	// A file keyring holding a handful of master keys is well under 1
	// KiB; 16 MiB is generous headroom that still bounds the blast
	// radius of a runaway write against the pod's memory limit.
	keyringVolumeSizeLimit = "16Mi"

	// keyringSecretMode is the projection mode for the sealed keyring
	// Secret. Secret volumes are root-owned and Bloodraven does not
	// mandate an fsGroup (spec.podSecurityContext is opt-in), so the
	// file has to be world-readable for mysqld's uid to open it. This is
	// acceptable under the documented threat model: a live process
	// inside the MySQL pod is already out of scope, and the point of the
	// feature is that the key never reaches a persistent disk.
	keyringSecretMode int32 = 0o444

	// keyringTokenMode variants for the escrow bearer token. Unlike the
	// keyring itself the token is read only by the sidecar container,
	// which runs the escrow agent directly as uid 999 — no image
	// entrypoint drops privileges in between — so when the pod declares
	// an fsGroup, kubelet's chgrp of the projection plus the fsGroup
	// supplementary group are enough and the token can stay off the
	// world-readable path.
	//
	// Without an fsGroup the projection is root:root and 0400 is
	// unreadable by uid 999: open() returns EACCES on every retry, the
	// keyring never escrows, and the group sits at
	// phase=Unsealed/reason=Bootstrap indefinitely. That was the 0.9.0
	// behavior. 0444 matches what keyringSecretMode already accepts for
	// the far more sensitive keyring file, so it adds no exposure a
	// reader of this pod does not already have.
	keyringTokenModeShared  int32 = 0o444
	keyringTokenModeFSGroup int32 = 0o440

	// Uid/gid the official MySQL images run mysqld as. Reused from the
	// PITR wiring (mysqlDataUID/mysqlDataGID in pitr.go) for the sidecar
	// security context; spelled out here for the keyring file chown so
	// the init script reads standalone.
	keyringFileOwner = "999:999"
)

// encryptionFragments is the bundle of additions the encryption-at-rest
// feature contributes to the MySQL pod spec. The zero value means
// "encryption is off"; callers append unconditionally.
type encryptionFragments struct {
	// InitContainers are prepended to the pod's init containers. Empty
	// when the site is sealed — a sealed keyring needs no preparation,
	// kubelet projects the Secret directly onto the expected path.
	InitContainers []corev1.Container

	// MysqlVolumeMounts are added to the mysql container: the keyring
	// directory plus the two subPath-projected component files.
	MysqlVolumeMounts []corev1.VolumeMount

	// SidecarVolumeMounts give the escrow agent read access to the live
	// keyring and, while unsealed, to the bearer token.
	SidecarVolumeMounts []corev1.VolumeMount

	// SidecarEnv configures the escrow agent.
	SidecarEnv []corev1.EnvVar

	// PodVolumes are appended to the pod's volumes.
	PodVolumes []corev1.Volume
}

// keyringManifestJSON is the global component manifest (`mysqld.my`
// next to the mysqld binary). Projected via subPath into MysqldDir.
//
// read_local_manifest is true so the datadir-local copy written by the
// config init container is also consulted. That local copy is the
// fallback for environments (notably GHA kind) that have been observed
// to ignore a valid global subPath mount while still running with the
// encryption my.cnf and reporting MY-013712 / no keyring component.
func keyringManifestJSON() string {
	// Relative URN (file://name) resolved against plugin_dir. Absolute
	// file:///… URNs fail with MY-013709 on MySQL 9.7. Do not set
	// read_local_manifest: keep global-only loading simple; a local
	// datadir copy is still planted as a secondary path.
	return "{\n  \"components\": \"file://component_keyring_file\"\n}\n"
}

// keyringLocalManifestJSON is the datadir-local component manifest.
// Local manifests do not take the read_local_manifest key (that flag is
// global-only); they only list components.
func keyringLocalManifestJSON() string {
	return "{\n  \"components\": \"file://component_keyring_file\"\n}\n"
}

// encryptionConfigInitSnippet is appended to the operator-managed
// config init container when encryption is on. It plants a local
// mysqld.my in the datadir so the keyring component still loads when
// the global subPath mount is ignored by the runtime.
//
// Only writes into an already-initialized datadir: placing any file in
// an empty datadir makes `mysqld --initialize` abort with "data
// directory has files in it". Fresh encrypted-from-birth still relies
// on the global mount for the first initialize.
func encryptionConfigInitSnippet() string {
	// Inline the local-manifest bytes rather than reusing the ConfigMap
	// global file: that one carries read_local_manifest, which is not a
	// local-manifest field.
	return fmt.Sprintf(`
if [ -d /var/lib/mysql/mysql ] || [ -f /var/lib/mysql/ibdata1 ]; then
  cat > /var/lib/mysql/mysqld.my <<'BR_KEYRING_LOCAL_MANIFEST'
%s
BR_KEYRING_LOCAL_MANIFEST
  if [ "$(id -u)" = "0" ]; then
    chown 999:999 /var/lib/mysql/mysqld.my
  fi
  chmod 644 /var/lib/mysql/mysqld.my
fi`, strings.TrimRight(keyringLocalManifestJSON(), "\n"))
}

// keyringComponentConfigJSON is the content of the global
// `component_keyring_file.cnf`. MySQL reads this only from plugin_dir.
//
// path and read_only are mandatory: component_keyring_file refuses to
// initialize if either is missing. read_local_config is set false for
// the same reason as the manifest: a local config under the data
// directory must not override the operator-managed global file.
func keyringComponentConfigJSON(dataFilePath string, readOnly bool) string {
	return fmt.Sprintf("{\n  \"read_local_config\": false,\n  \"path\": %q,\n  \"read_only\": %t\n}\n", dataFilePath, readOnly)
}

// encryptionMySQLSettings returns the my.cnf settings that turn on
// data-at-rest encryption for the configured coverage. Returns nil when
// encryption is disabled.
//
// The keys are returned in generateMyCnf's hyphenated spelling. They are
// applied as operator-owned invariants (after user overrides), so a
// stray spec.mysqlConf entry cannot silently drop coverage that the
// security claim depends on.
func encryptionMySQLSettings(fg *v1alpha1.MysqlFailoverGroup) map[string]string {
	if !fg.Spec.EncryptionEnabled() {
		return nil
	}
	cov := fg.Spec.EffectiveEncryptionCoverage()
	onOff := func(b *bool) string {
		if b != nil && *b {
			return "ON"
		}
		return "OFF"
	}
	return map[string]string{
		"default-table-encryption":         onOff(cov.Tables),
		"table-encryption-privilege-check": onOff(cov.PrivilegeCheck),
		"innodb-redo-log-encrypt":          onOff(cov.RedoLog),
		"innodb-undo-log-encrypt":          onOff(cov.UndoLog),
		"binlog-encryption":                onOff(cov.BinaryLog),
	}
}

// buildEncryptionFragments renders the pod-spec contributions for one
// site.
//
//   - sealed=true renders the steady state: the keyring is a read-only
//     projection of escrowSecret and the component config carries
//     "read_only": true, so mysqld cannot add keys at all.
//   - sealed=false renders an unsealed site: the keyring is a
//     memory-backed emptyDir, seeded from escrowSecret when one exists
//     (clone/rotation) or created empty (fresh bootstrap), and the
//     sidecar escrow agent is armed.
//
// escrowSecret may be empty, which is only valid when sealed=false —
// there is nothing to seal against yet.
func buildEncryptionFragments(
	fg *v1alpha1.MysqlFailoverGroup,
	site v1alpha1.SiteSpec,
	sealed bool,
	escrowSecret string,
	rotate bool,
) encryptionFragments {
	out := encryptionFragments{}
	if !fg.Spec.EncryptionEnabled() {
		return out
	}

	kr := fg.Spec.EffectiveKeyring()
	dataFilePath := path.Join(kr.DataFileDir, v1alpha1.KeyringDataFileName)

	// ConfigMap sources for the launcher (mysqld.my + component cnf).
	// Keep image plugin_dir / lc-messages-dir; staging those onto
	// emptyDir hit Permission denied on dlopen in GHA kind.
	out.MysqlVolumeMounts = append(out.MysqlVolumeMounts,
		corev1.VolumeMount{
			Name:      "config",
			MountPath: keyringComponentSrcMount,
			ReadOnly:  true,
		},
		corev1.VolumeMount{
			Name:      keyringVolumeName,
			MountPath: kr.DataFileDir,
			ReadOnly:  sealed,
		},
	)

	// The sidecar reads the keyring to report its digest. In the sealed
	// phase that is a drift check ("does the live file still match the
	// escrow?"); in the unsealed phase it is the escrow source.
	out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
		Name:      keyringVolumeName,
		MountPath: kr.DataFileDir,
		ReadOnly:  true,
	})
	out.SidecarEnv = append(out.SidecarEnv,
		corev1.EnvVar{Name: "BLOODRAVEN_KEYRING_ENABLED", Value: "1"},
		corev1.EnvVar{Name: "BLOODRAVEN_KEYRING_FILE", Value: dataFilePath},
	)
	if cov := fg.Spec.EffectiveEncryptionCoverage(); cov.SystemTablespace != nil && *cov.SystemTablespace {
		// Safe in both renderings: encrypting the `mysql` tablespace
		// reuses the existing master key rather than creating one, so it
		// does not need a writable keyring.
		out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
			Name: "BLOODRAVEN_KEYRING_ENCRYPT_SYSTEM_TABLESPACE", Value: "1",
		})
	}

	if sealed {
		out.PodVolumes = append(out.PodVolumes, corev1.Volume{
			Name: keyringVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  escrowSecret,
					DefaultMode: int32Ptr(keyringSecretMode),
					Items: []corev1.KeyToPath{
						{Key: v1alpha1.KeyringDataFileName, Path: v1alpha1.KeyringDataFileName},
					},
				},
			},
		})
		return out
	}

	// ---- unsealed rendering ----------------------------------------

	sizeLimit := resource.MustParse(keyringVolumeSizeLimit)
	out.PodVolumes = append(out.PodVolumes, corev1.Volume{
		Name: keyringVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: &sizeLimit,
			},
		},
	})

	// Escrow token: minted per site, mounted into the sidecar only, and
	// only while the site is unsealed. A sealed site has nothing to
	// escrow, so it carries no credential that could be used to
	// overwrite one.
	out.PodVolumes = append(out.PodVolumes, corev1.Volume{
		Name: keyringTokenVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  v1alpha1.KeyringTokenSecretName(fg.Name, site.Name),
				DefaultMode: int32Ptr(keyringTokenMode(fg)),
				Items: []corev1.KeyToPath{
					{Key: v1alpha1.KeyringTokenKey, Path: v1alpha1.KeyringTokenKey},
				},
			},
		},
	})
	out.SidecarVolumeMounts = append(out.SidecarVolumeMounts, corev1.VolumeMount{
		Name:      keyringTokenVolumeName,
		MountPath: keyringTokenMountPath,
		ReadOnly:  true,
	})
	out.SidecarEnv = append(out.SidecarEnv,
		corev1.EnvVar{Name: "BLOODRAVEN_KEYRING_ESCROW", Value: "1"},
		corev1.EnvVar{Name: "BLOODRAVEN_KEYRING_ESCROW_URL", Value: defaultKeyringEscrowURL(fg)},
		corev1.EnvVar{Name: "BLOODRAVEN_KEYRING_ESCROW_CA_FILE", Value: "/etc/mysql/tls/ca.crt"},
		corev1.EnvVar{
			Name:  "BLOODRAVEN_KEYRING_TOKEN_FILE",
			Value: path.Join(keyringTokenMountPath, v1alpha1.KeyringTokenKey),
		},
	)
	if rotate {
		out.SidecarEnv = append(out.SidecarEnv, corev1.EnvVar{
			Name: "BLOODRAVEN_KEYRING_ROTATE", Value: "1",
		})
	}

	// Seed volume: the escrow version this site is being unsealed FROM.
	// Absent on a fresh bootstrap, where the keyring starts empty and
	// MySQL populates it during initialization.
	initMounts := []corev1.VolumeMount{
		{Name: keyringVolumeName, MountPath: kr.DataFileDir},
	}
	if escrowSecret != "" {
		out.PodVolumes = append(out.PodVolumes, corev1.Volume{
			Name: keyringSeedVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  escrowSecret,
					DefaultMode: int32Ptr(keyringSecretMode),
					Items: []corev1.KeyToPath{
						{Key: v1alpha1.KeyringDataFileName, Path: v1alpha1.KeyringDataFileName},
					},
				},
			},
		})
		initMounts = append(initMounts, corev1.VolumeMount{
			Name:      keyringSeedVolumeName,
			MountPath: keyringSeedMountPath,
			ReadOnly:  true,
		})
	}

	out.InitContainers = append(out.InitContainers, corev1.Container{
		Name:    "keyring-init",
		Image:   effectiveMySQLImage(fg),
		Command: []string{"sh", "-c"},
		Args: []string{
			keyringInitScript(escrowSecret != ""),
			"keyring-init",
			dataFilePath,
			path.Dir(dataFilePath),
			path.Join(keyringSeedMountPath, v1alpha1.KeyringDataFileName),
		},
		VolumeMounts:    initMounts,
		SecurityContext: fg.Spec.ContainerSecurityContext.DeepCopy(),
		Resources:       defaultInitContainerResources(),
	})

	return out
}

// encryptionMysqlLauncher returns the container Command and Args that
// materialize keyring component files onto image-owned paths and hand
// off to the official MySQL entrypoint.
//
// mysqlArgs are the mysqld flags (starting with --server-id=..., not
// including the "mysqld" binary name). They become $1..$n after a $0 of
// "mysqld" so the entrypoint sees the same argv shape as a normal
// `docker run mysql:… --flag` invocation.
func encryptionMysqlLauncher(fg *v1alpha1.MysqlFailoverGroup, mysqlArgs []string) (command, args []string) {
	kr := fg.Spec.EffectiveKeyring()
	mysqlImageBasedir := path.Dir(kr.MysqldDir)
	mysqlImageBinary := path.Join(kr.MysqldDir, "mysqld")
	mysqlRuntimeBinary := path.Join(mysqlRuntimeBinDir, "mysqld")
	mysqlRuntimeBasedir := path.Dir(mysqlRuntimeBinDir)
	pluginRelative := strings.TrimPrefix(strings.TrimPrefix(kr.PluginDir, mysqlImageBasedir), "/")
	mysqlRuntimePluginDir := path.Join(mysqlRuntimeBasedir, pluginRelative)
	manifestDst := mysqlRuntimeBinary + ".my"
	componentDst := path.Join(kr.PluginDir, "component_keyring_file.cnf")
	// bash -ec '<script>' mysqld --flag…  →  $0=mysqld, $1=--flag…
	// Copy mysqld away from the host-profiled /usr/sbin path, materialize
	// its adjacent manifest and the component config as real overlay files,
	// then put the private binary first on PATH. The entrypoint still sees
	// argv[0] == "mysqld", so its config validation, uid switch, fresh-db
	// initialization, and final exec behavior remain unchanged.
	script := fmt.Sprintf(`set -euo pipefail
src=%q
mkdir -p %q
mkdir -p %q
ln -sfn %q %q
ln -sfn %q %q
cp %q %q
cp "$src/%s" %q
cp "$src/%s" %q
chmod 755 %q
chmod 644 %q %q
test -x %q
test -r %q
test -r %q
test -x %q
export PATH=%q:"$PATH"
exec %s "$0" "$@"
`,
		keyringComponentSrcMount,
		mysqlRuntimeBinDir,
		path.Dir(mysqlRuntimePluginDir),
		kr.PluginDir, mysqlRuntimePluginDir,
		path.Join(mysqlImageBasedir, "share"), path.Join(mysqlRuntimeBasedir, "share"),
		mysqlImageBinary, mysqlRuntimeBinary,
		keyringManifestKey, manifestDst,
		keyringComponentKey, componentDst,
		mysqlRuntimeBinary,
		manifestDst, componentDst,
		mysqlRuntimeBinary,
		manifestDst, componentDst,
		path.Join(kr.PluginDir, "component_keyring_file.so"),
		mysqlRuntimeBinDir,
		mysqlDockerEntrypoint,
	)
	command = []string{"/bin/bash", "-ec", script}
	// Relocating mysqld makes it derive basedir/plugin_dir from the private
	// path. Restore the source image's layout so the relative component URN,
	// errmsg.sys, and the rest of the official entrypoint keep resolving
	// exactly as they did for the image binary.
	args = append([]string{
		"mysqld",
		"--basedir=" + mysqlImageBasedir,
		"--plugin-dir=" + kr.PluginDir,
	}, mysqlArgs...)
	return command, args
}

// keyringTokenMode picks the projection mode for the escrow bearer
// token. See the keyringTokenMode* constants for why the mode depends on
// whether the pod declares an fsGroup.
//
// This deliberately does not apply to the keyring volume: that one is
// read by mysqld, which the official image entrypoint starts through
// gosu. gosu resets the process's supplementary groups from the image's
// group database, dropping the fsGroup kubelet injected, so group
// permissions cannot be relied on there.
func keyringTokenMode(fg *v1alpha1.MysqlFailoverGroup) int32 {
	if fg.Spec.PodSecurityContext != nil && fg.Spec.PodSecurityContext.FSGroup != nil {
		return keyringTokenModeFSGroup
	}
	return keyringTokenModeShared
}

// keyringInitScript prepares the memory-backed keyring before mysqld
// starts.
//
// Two things have to be true or InnoDB aborts startup:
//
//  1. The data file must contain a valid component_keyring_file document.
//     A missing or zero-length file disables the component. With startup
//     encryption enabled, InnoDB then fails with "Check keyring fail".
//     Fresh bootstrap therefore starts from the canonical empty document;
//     MySQL adds the first master key while initializing encrypted state.
//  2. The containing DIRECTORY must be writable by mysqld's uid, not
//     just the file. component_keyring_file writes a new key by creating
//     a temporary file alongside the keyring and renaming over it, so a
//     directory mysqld cannot write to fails with "Failed to store key,
//     please check if keyring is loaded" — during --initialize that
//     surfaces as an opaque "InnoDB Database creation was aborted with
//     error Generic error".
//
// Ownership: with no spec.containerSecurityContext the init container
// runs as root while the official MySQL entrypoint drops mysqld to uid
// 999, so both the directory and the file have to be chowned. When a
// security context pins both containers to the same non-root uid they
// are already owned correctly and the chown is skipped rather than
// failed.
func keyringInitScript(seeded bool) string {
	seed := `printf '%s\n' '{"version":"1.0","elements":[]}' > "$data_file"`
	if seeded {
		seed = `cp "$seed_file" "$data_file"`
	}
	// The directory operations are root-only on purpose. A memory-backed
	// emptyDir is created root-owned mode 0777 (or fsGroup-writable when
	// spec.podSecurityContext sets one), so a non-root init container
	// both cannot chmod it and does not need to — it is already writable
	// by mysqld's uid. Attempting it unconditionally would trip `set -e`
	// and wedge the pod on any hardened, non-root deployment.
	return fmt.Sprintf(`set -e
data_file=$1
data_dir=$2
seed_file=$3
if [ ! -s "$data_file" ]; then
  %s
fi
chmod 600 "$data_file"
if [ "$(id -u)" = "0" ]; then
  chown %s "$data_file"
  chown %s "$data_dir"
  chmod 700 "$data_dir"
fi
`, seed, keyringFileOwner, keyringFileOwner)
}

func defaultKeyringEscrowURL(fg *v1alpha1.MysqlFailoverGroup) string {
	if configured := strings.TrimSpace(os.Getenv("BLOODRAVEN_DEFAULT_ESCROW_URL")); configured != "" {
		return configured
	}
	return fmt.Sprintf("https://bloodraven.%s.svc.cluster.local:8443/keyring/escrow", fg.Namespace)
}

// keyringConfigMapData returns the two ConfigMap entries carrying the
// keyring manifest and component config for a site. Returns nil when
// encryption is disabled so the ConfigMap stays byte-identical to what
// pre-encryption releases produced.
func keyringConfigMapData(fg *v1alpha1.MysqlFailoverGroup, sealed bool) map[string]string {
	if !fg.Spec.EncryptionEnabled() {
		return nil
	}
	return map[string]string{
		keyringManifestKey: keyringManifestJSON(),
		keyringComponentKey: keyringComponentConfigJSON(
			fg.Spec.KeyringDataFilePath(), sealed),
	}
}

// keyringDigest returns the canonical "sha256:<hex>" digest Bloodraven
// uses to compare a live keyring file against its escrowed copy.
func keyringDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// effectiveMySQLImage centralizes the spec.image default so the
// keyring-init container cannot drift from the mysql container's image.
func effectiveMySQLImage(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.Image != "" {
		return fg.Spec.Image
	}
	return defaultMySQLImage
}
