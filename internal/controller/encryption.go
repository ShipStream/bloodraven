package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// Bump when encryption pod rendering changes without a corresponding
	// CRD field change, so already-encrypted pods roll forward onto the
	// new rendering. ComputeSpecHash includes this value.
	encryptionPodRenderVersion = "encryption-pod-render-v1"

	// ConfigMap keys carrying the two files MySQL insists on reading
	// from image-owned directories. They live in the existing per-site
	// ConfigMap and are projected with subPath mounts, which is the only
	// way to drop a single file into a directory that already has
	// content (mounting a volume over /usr/sbin would hide mysqld).
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

// keyringManifestJSON is the content of the global component manifest
// (`mysqld.my`). MySQL reads this only from the directory holding the
// mysqld binary, which is why it is projected with a subPath mount
// rather than written by an init container.
func keyringManifestJSON() string {
	return "{\n  \"components\": \"file://component_keyring_file\"\n}\n"
}

// keyringComponentConfigJSON is the content of the global
// `component_keyring_file.cnf`. MySQL reads this only from plugin_dir.
//
// Both keys are mandatory: component_keyring_file refuses to initialize
// if either "path" or "read_only" is missing, and InnoDB then refuses to
// start because it cannot find a keyring.
func keyringComponentConfigJSON(dataFilePath string, readOnly bool) string {
	return fmt.Sprintf("{\n  \"path\": %q,\n  \"read_only\": %t\n}\n", dataFilePath, readOnly)
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

	// The two files MySQL will only read from image-owned directories.
	// Both come from the per-site ConfigMap already mounted as "config".
	out.MysqlVolumeMounts = append(out.MysqlVolumeMounts,
		corev1.VolumeMount{
			Name:      "config",
			MountPath: path.Join(kr.MysqldDir, "mysqld.my"),
			SubPath:   keyringManifestKey,
			ReadOnly:  true,
		},
		corev1.VolumeMount{
			Name:      "config",
			MountPath: path.Join(kr.PluginDir, "component_keyring_file.cnf"),
			SubPath:   keyringComponentKey,
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
				DefaultMode: int32Ptr(0o400),
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
		Name:  "keyring-init",
		Image: effectiveMySQLImage(fg),
		Command: []string{
			"sh", "-c", keyringInitScript(dataFilePath, escrowSecret != ""),
		},
		VolumeMounts:    initMounts,
		SecurityContext: fg.Spec.ContainerSecurityContext.DeepCopy(),
		Resources:       defaultInitContainerResources(),
	})

	return out
}

// keyringInitScript prepares the memory-backed keyring before mysqld
// starts.
//
// Two things have to be true or InnoDB aborts startup:
//
//  1. The data file must exist. component_keyring_file reports "Failed
//     to read keyring file" for a missing one and InnoDB then fails with
//     "Check keyring fail". A zero-length file is accepted and is
//     exactly what a fresh bootstrap needs.
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
func keyringInitScript(dataFilePath string, seeded bool) string {
	dir := path.Dir(dataFilePath)
	seed := fmt.Sprintf(": > %q", dataFilePath)
	if seeded {
		seed = fmt.Sprintf("cp %q %q",
			path.Join(keyringSeedMountPath, v1alpha1.KeyringDataFileName), dataFilePath)
	}
	// The directory operations are root-only on purpose. A memory-backed
	// emptyDir is created root-owned mode 0777 (or fsGroup-writable when
	// spec.podSecurityContext sets one), so a non-root init container
	// both cannot chmod it and does not need to — it is already writable
	// by mysqld's uid. Attempting it unconditionally would trip `set -e`
	// and wedge the pod on any hardened, non-root deployment.
	return fmt.Sprintf(`set -e
if [ ! -s %[1]q ]; then
  %[2]s
fi
chmod 600 %[1]q
if [ "$(id -u)" = "0" ]; then
  chown %[3]s %[1]q
  chown %[3]s %[4]q
  chmod 700 %[4]q
fi
`, dataFilePath, seed, keyringFileOwner, dir)
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
