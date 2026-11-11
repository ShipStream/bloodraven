package sidecar

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

const (
	minPeerCheckInterval     = time.Second
	minLeaseTimeout          = 3 * time.Second
	leaseTimeoutPeerRatio    = 3
	defaultLeaseTimeout      = 20 * time.Second
	defaultPeerCheckInterval = 5 * time.Second
)

// Config holds sidecar configuration parsed from environment variables.
type Config struct {
	// MysqlDSN is the DSN for connecting to the local MySQL instance.
	MysqlDSN string

	// PodName is the name of the pod, used for logging.
	PodName string

	// ListenAddr is the HTTP listen address (default ":8080").
	ListenAddr string

	// PeerAddresses lists every peer sidecar address this sidecar
	// should monitor for liveness (one per non-self site). Empty in a
	// one-site configuration — the self-fencing monitor then relies
	// solely on BloodravenAddress reachability.
	PeerAddresses []string

	// BloodravenAddress is the address of the Bloodraven operator's auxiliary HTTP server.
	BloodravenAddress string

	// LeaseTimeout is how long both Bloodraven AND every peer must be
	// unreachable before self-fencing. A quorum of one-or-more-peer-
	// reachable is enough to keep the site writable; self-fencing
	// triggers only when every peer AND the operator are silent.
	LeaseTimeout time.Duration

	// PeerCheckInterval is how often the fencing monitor checks Bloodraven and peers.
	PeerCheckInterval time.Duration

	// MySite is the site this sidecar belongs to.
	MySite string

	// PodNamespace is the namespace of the pod.
	PodNamespace string

	// FailoverGroup is the name of the MysqlFailoverGroup CR this sidecar belongs to.
	FailoverGroup string

	// PITR holds the binlog archiver configuration. Nil when PITR is
	// disabled for the failover group — in that case the archiver
	// goroutine is never started.
	PITR *PITRConfig

	// Keyring holds the encryption-at-rest escrow configuration. Nil
	// when spec.encryptionAtRest is disabled for the failover group.
	Keyring *KeyringConfig
}

// PITRConfig controls the binlog archiver goroutine that runs inside
// the sidecar. All fields are populated by the operator via env vars
// on the pod spec. The archiver itself decides when to be active —
// only the primary site uploads every sealed binlog; a replica's
// archiver polls its own role, uploads at most the sealed tail of its
// server's last writable era, and takes over after failover.
type PITRConfig struct {
	// BinlogDir is the directory holding the MySQL binary logs. The
	// archiver watches the index file inside this directory with
	// inotify. Matches MySQL's log_bin_basename directory.
	BinlogDir string

	// BinlogIndex is the basename of the binlog index file
	// (e.g. "mysql-bin.index"). Combined with BinlogDir this yields
	// the file the archiver watches for rotation events.
	BinlogIndex string

	// PollInterval is the belt-and-suspenders period between manual
	// scans of the binlog directory, for cases where inotify misses
	// an event (e.g. filesystem quirks or missed wakeups). Default
	// 60s.
	PollInterval time.Duration

	// StorageType selects the archive backend: "S3" or "PVC".
	StorageType string

	// ManifestPrefix is the logical prefix (S3 prefix or PVC subpath)
	// under which per-site manifests and binlog objects live. Always
	// rooted at the profile's base prefix with "binlogs/" appended.
	ManifestPrefix string

	// StateDir, when set, is a writable directory that outlives the
	// sidecar container (an emptyDir). The archiver keeps a
	// history-integrity violation it could not yet persist in an archive
	// manifest there, so a container restart does not forget it (D-34).
	// "" holds the pending marker in memory only.
	StateDir string

	// S3 is populated when StorageType is "S3".
	S3 *PITRS3Config

	// PVC is populated when StorageType is "PVC".
	PVC *PITRPVCConfig

	// PassphraseFile, when non-empty, turns on client-side envelope
	// encryption (AES-256-GCM + HKDF-SHA256) for every archived
	// binlog file and manifest. The file must contain the passphrase
	// bytes (trailing whitespace is stripped). The operator owns the
	// lifecycle of the Secret; the sidecar just reads the mounted
	// file at startup.
	PassphraseFile string

	// AllowPlaintextFallback opts the encrypted-store wrapper into
	// legacy mixed-encryption behavior: objects lacking the BRV1 magic
	// header are passed through as plaintext on Get/GetFile instead of
	// raising ErrTamperedOrDowngrade. Default false. Only enable this
	// for a time-bounded migration when an operator is knowingly moving
	// an unencrypted prefix to encryption — it disables the tamper
	// detection guarantee that AES-GCM otherwise provides.
	AllowPlaintextFallback bool

	// Archive is the archive identity this writer expects (§3.7): its
	// group, normalized destination and encryption identity. Writers
	// refuse to touch a prefix whose descriptor disagrees.
	Archive ArchiveIdentity

	// RoleProbeInterval is how often the archiver probes the server's role
	// to write promotion records (D-43). The sidecar sets it to
	// PEER_CHECK_INTERVAL; zero means defaultPeerCheckInterval.
	RoleProbeInterval time.Duration

	// PromotionConfirmDelay is how long after a promotion record's
	// observation the read of the observation that confirms it must start
	// (issue #269). Zero means pitrarchive.PromotionConfirmDelay (2δ),
	// which is what the planner's rules assume; no environment variable
	// sets it. Tests that run episodes in milliseconds shorten it.
	PromotionConfirmDelay time.Duration
}

// ArchiveIdentity is what a writer expects the archive descriptor to hold.
type ArchiveIdentity struct {
	// Source is the owning MysqlFailoverGroup. Verification needs only the
	// UID; creating a descriptor also needs the namespace and name.
	Source pitrarchive.Source
	// Destination is the normalized destination (BLOODRAVEN_PITR_DESTINATION).
	Destination pitrarchive.Destination
	// Encryption is the profile's encryption identity.
	Encryption pitrarchive.EncryptionIdentity
	// ArchiveID is the ID the operator rendered (BLOODRAVEN_PITR_ARCHIVE_ID).
	// When set it must be a supported derivation for Source.UID and
	// Destination; the descriptor is checked against every derivation.
	ArchiveID string
	// KeyRevision is the archive key snapshot the operator mounted (#203);
	// echoed in status only.
	KeyRevision string
}

// validate checks the identity a writer needs. It does not require the
// source namespace and name, which only descriptor creation uses.
func (id ArchiveIdentity) validate() error {
	if id.Source.UID == "" {
		return fmt.Errorf("archive identity: no failover group UID (BLOODRAVEN_FAILOVER_GROUP_UID)")
	}
	if id.Destination.IsZero() {
		return fmt.Errorf("archive identity: no destination (BLOODRAVEN_PITR_DESTINATION)")
	}
	if err := id.Encryption.Validate(); err != nil {
		return fmt.Errorf("archive identity: %w", err)
	}
	if id.ArchiveID != "" && !pitrarchive.IsDerivedArchiveID(id.ArchiveID, id.Source.UID, id.Destination) {
		return fmt.Errorf("archive identity: BLOODRAVEN_PITR_ARCHIVE_ID %q is not derived from uid %s and destination %s",
			id.ArchiveID, id.Source.UID, id.Destination)
	}
	return nil
}

// ArchiveIdentityFromEnv reads the archive identity env vars rendered for
// writers (BLOODRAVEN_FAILOVER_GROUP_UID, BLOODRAVEN_PITR_ARCHIVE_ID,
// BLOODRAVEN_PITR_DESTINATION, BLOODRAVEN_PITR_ENCRYPTION_MODE,
// BLOODRAVEN_PITR_KEY_REVISION, plus POD_NAMESPACE and FAILOVER_GROUP for
// the source) and checks them against cfg's storage settings, so a writer
// can only write where its identity says it does.
func ArchiveIdentityFromEnv(cfg *PITRConfig) (ArchiveIdentity, error) {
	id := ArchiveIdentity{
		Source: pitrarchive.Source{
			Namespace: os.Getenv("POD_NAMESPACE"),
			Name:      os.Getenv("FAILOVER_GROUP"),
			UID:       types.UID(os.Getenv("BLOODRAVEN_FAILOVER_GROUP_UID")),
		},
		ArchiveID:   os.Getenv("BLOODRAVEN_PITR_ARCHIVE_ID"),
		KeyRevision: os.Getenv("BLOODRAVEN_PITR_KEY_REVISION"),
	}
	raw := os.Getenv("BLOODRAVEN_PITR_DESTINATION")
	if raw == "" {
		return id, fmt.Errorf("BLOODRAVEN_PITR_DESTINATION is required")
	}
	dest, err := pitrarchive.ParseDestination(raw)
	if err != nil {
		return id, fmt.Errorf("parse BLOODRAVEN_PITR_DESTINATION: %w", err)
	}
	id.Destination = dest
	switch mode := pitrarchive.EncryptionMode(os.Getenv("BLOODRAVEN_PITR_ENCRYPTION_MODE")); mode {
	case pitrarchive.EncryptionNone:
		id.Encryption = pitrarchive.EncryptionIdentity{Mode: mode}
	case pitrarchive.EncryptionPassphrase:
		id.Encryption = pitrarchive.EncryptionIdentity{Mode: mode, Algorithm: pitrarchive.AlgorithmAES256GCM}
	default:
		return id, fmt.Errorf("BLOODRAVEN_PITR_ENCRYPTION_MODE=%q; must be %s or %s", mode, pitrarchive.EncryptionNone, pitrarchive.EncryptionPassphrase)
	}
	if err := id.validate(); err != nil {
		return id, err
	}
	if err := id.checkStorage(cfg); err != nil {
		return id, err
	}
	return id, nil
}

// checkStorage refuses an identity that disagrees with the store settings
// it will be used with.
func (id ArchiveIdentity) checkStorage(cfg *PITRConfig) error {
	d := id.Destination
	if d.KeyPrefix != cfg.ManifestPrefix {
		return fmt.Errorf("archive identity: destination %s has key prefix %q, BLOODRAVEN_PITR_MANIFEST_PREFIX is %q", d, d.KeyPrefix, cfg.ManifestPrefix)
	}
	switch cfg.StorageType {
	case "S3":
		if d.Kind != pitrarchive.DestinationS3 || cfg.S3 == nil || d.Bucket != cfg.S3.Bucket {
			return fmt.Errorf("archive identity: destination %s does not name the configured S3 bucket", d)
		}
		endpoint, err := pitrarchive.NormalizeEndpoint(cfg.S3.EndpointURL)
		if err != nil {
			return fmt.Errorf("archive identity: %w", err)
		}
		if endpoint != d.Endpoint {
			return fmt.Errorf("archive identity: destination %s has endpoint %q, the store uses %q", d, d.Endpoint, endpoint)
		}
	case "PVC":
		if d.Kind != pitrarchive.DestinationPVC {
			return fmt.Errorf("archive identity: destination %s is not a PVC destination", d)
		}
		if ns := id.Source.Namespace; ns != "" && d.Namespace != ns {
			return fmt.Errorf("archive identity: destination %s is in namespace %q, not %q", d, d.Namespace, ns)
		}
	default:
		return fmt.Errorf("archive identity: unknown storage type %q", cfg.StorageType)
	}
	if (id.Encryption.Mode == pitrarchive.EncryptionPassphrase) != (cfg.PassphraseFile != "") {
		return fmt.Errorf("archive identity: encryption mode %s but passphrase file %q", id.Encryption.Mode, cfg.PassphraseFile)
	}
	return nil
}

// PITRS3Config is the S3-specific archiver config.
type PITRS3Config struct {
	Bucket      string
	Region      string
	EndpointURL string
	// AWSCredsDir is a directory containing the AWS_ACCESS_KEY_ID,
	// AWS_SECRET_ACCESS_KEY, (optional) AWS_SESSION_TOKEN, and
	// (optional) AWS_REGION files, mirroring the backup-job secret
	// mount layout. When empty the archiver falls back to the default
	// AWS SDK credential chain (IRSA, instance profile, env vars).
	AWSCredsDir string
	// DefaultRegion signs requests when no region is configured anywhere.
	// The cmd/bloodraven Job steps that read or write next to mysqlsh's
	// dump set it to "us-east-1" (what mysqlsh assumes): artifact cleanup,
	// dump-meta and the backup stamp writer, so a plain-AWS profile
	// without a region keeps working there. The sidecar leaves it empty
	// and keeps the SDK's behaviour.
	DefaultRegion string
}

// PITRPVCConfig is the local-disk archiver config.
type PITRPVCConfig struct {
	// MountPath is where the backup PVC is mounted inside the sidecar.
	// Binlog objects land under MountPath + "/binlogs/" + site.
	// The store refuses to start unless MountPath is itself a mount
	// point, so a pod without the volume cannot archive into the
	// container filesystem.
	MountPath string

	// Writable makes store construction probe write access under
	// WritePrefix. Writers (the archiver, pitr-quarantine,
	// encrypt-upload) set it; readers leave it false because their
	// volumes are mounted read-only.
	Writable bool

	// WritePrefix is the storage-relative directory the caller writes
	// objects under. The probe runs in its deepest existing directory,
	// and also in PITRConfig.ManifestPrefix's when that is set (manifests
	// live there). The archiver sets it to <ManifestPrefix>/<site>.
	WritePrefix string

	// MountVerifier replaces the /proc/self/mountinfo mount check. For
	// tests only (see TempDirMountVerifierForTest); nil in production.
	MountVerifier func(path string) error
}

// ConfigFromEnv reads sidecar configuration from environment variables.
func ConfigFromEnv() (*Config, error) {
	// Prefer mounted-file credentials (MYSQL_CREDS_DIR/{username,
	// password}) over env-var credentials so the hottest secret in the
	// system doesn't end up in /proc/<pid>/environ or crash dumps (AUDIT
	// L2). Falls through to MYSQL_USER / MYSQL_PASSWORD. The files are read
	// verbatim, not trimmed: the operator sets the MySQL account password
	// to the Secret's exact bytes, as the env vars used to carry them.
	var dsn string
	credsDir := os.Getenv("MYSQL_CREDS_DIR")
	if credsDir != "" {
		user, err := readCredentialFile(credsDir + "/username")
		if err != nil {
			return nil, fmt.Errorf("read MYSQL_CREDS_DIR/username: %w", err)
		}
		password, err := readCredentialFile(credsDir + "/password")
		if err != nil {
			return nil, fmt.Errorf("read MYSQL_CREDS_DIR/password: %w", err)
		}
		if user == "" {
			return nil, fmt.Errorf("MYSQL_CREDS_DIR/username is empty")
		}
		dsn = fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/", user, password)
	} else {
		user := os.Getenv("MYSQL_USER")
		password := os.Getenv("MYSQL_PASSWORD")
		if user == "" {
			return nil, fmt.Errorf("one of MYSQL_CREDS_DIR or MYSQL_USER is required")
		}
		dsn = fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/", user, password)
	}

	// When the group has spec.tls, mysqld runs with
	// require_secure_transport=ON and rejects the plaintext loopback
	// connection the DSN above describes. Nothing happens here when the
	// operator did not configure TLS, so non-TLS groups keep the exact
	// DSN previous releases produced.
	tlsName, err := registerMySQLTLS()
	if err != nil {
		return nil, err
	}
	if tlsName != "" {
		dsn, err = withMySQLTLSConfig(dsn, tlsName)
		if err != nil {
			return nil, err
		}
	}

	podName := os.Getenv("MY_POD_NAME")
	if podName == "" {
		podName = "unknown"
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	var peerAddresses []string
	if v := os.Getenv("PEER_ADDRESSES"); v != "" {
		for _, part := range strings.Split(v, ",") {
			if s := strings.TrimSpace(part); s != "" {
				peerAddresses = append(peerAddresses, s)
			}
		}
	} else if v := os.Getenv("PEER_ADDRESS"); v != "" {
		// Backwards-compatibility shim for the single-peer env var.
		// Retained purely so an older operator can still drive a new
		// sidecar during a rolling operator upgrade; the operator
		// itself only emits PEER_ADDRESSES.
		peerAddresses = []string{v}
	}
	bloodravenAddress := os.Getenv("BLOODRAVEN_ADDRESS")

	leaseTimeout := defaultLeaseTimeout
	if v := os.Getenv("LEASE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse LEASE_TIMEOUT: %w", err)
		}
		leaseTimeout = d
	}

	peerCheckInterval := defaultPeerCheckInterval
	if v := os.Getenv("PEER_CHECK_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse PEER_CHECK_INTERVAL: %w", err)
		}
		peerCheckInterval = d
	}
	leaseTimeout, peerCheckInterval = normalizeFenceDurations(leaseTimeout, peerCheckInterval)

	mySite := os.Getenv("MY_SITE")
	podNamespace := os.Getenv("POD_NAMESPACE")
	failoverGroup := os.Getenv("FAILOVER_GROUP")

	pitr, err := pitrConfigFromEnv()
	if err != nil {
		return nil, err
	}
	if pitr != nil {
		pitr.RoleProbeInterval = peerCheckInterval
	}

	keyring, err := keyringConfigFromEnv()
	if err != nil {
		return nil, err
	}

	return &Config{
		MysqlDSN:          dsn,
		PodName:           podName,
		ListenAddr:        listenAddr,
		PeerAddresses:     peerAddresses,
		BloodravenAddress: bloodravenAddress,
		LeaseTimeout:      leaseTimeout,
		PeerCheckInterval: peerCheckInterval,
		MySite:            mySite,
		PodNamespace:      podNamespace,
		FailoverGroup:     failoverGroup,
		PITR:              pitr,
		Keyring:           keyring,
	}, nil
}

func normalizeFenceDurations(leaseTimeout, peerCheckInterval time.Duration) (time.Duration, time.Duration) {
	if peerCheckInterval < minPeerCheckInterval {
		slog.Warn("clamping unsafe sidecar peer check interval",
			"configured", peerCheckInterval.String(),
			"minimum", minPeerCheckInterval.String())
		peerCheckInterval = minPeerCheckInterval
	}
	minLease := minLeaseTimeout
	if ratioMin := time.Duration(leaseTimeoutPeerRatio) * peerCheckInterval; ratioMin > minLease {
		minLease = ratioMin
	}
	if leaseTimeout < minLease {
		slog.Warn("clamping unsafe sidecar lease timeout",
			"configured", leaseTimeout.String(),
			"minimum", minLease.String(),
			"peerCheckInterval", peerCheckInterval.String())
		leaseTimeout = minLease
	}
	return leaseTimeout, peerCheckInterval
}

// pitrConfigFromEnv returns the PITR archiver config, or nil when
// BLOODRAVEN_PITR_ENABLED is unset/false. The operator is responsible
// for wiring these env vars onto the sidecar container only when the
// referenced failover group has spec.backup.pitr.enabled=true and the
// referenced profile exists.
func pitrConfigFromEnv() (*PITRConfig, error) {
	enabled := os.Getenv("BLOODRAVEN_PITR_ENABLED")
	if enabled == "" || enabled == "0" || enabled == "false" {
		return nil, nil
	}

	binlogDir := os.Getenv("BLOODRAVEN_PITR_BINLOG_DIR")
	if binlogDir == "" {
		binlogDir = "/var/lib/mysql"
	}
	binlogIndex := os.Getenv("BLOODRAVEN_PITR_BINLOG_INDEX")
	if binlogIndex == "" {
		binlogIndex = "mysql-bin.index"
	}

	pollInterval := 60 * time.Second
	if v := os.Getenv("BLOODRAVEN_PITR_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse BLOODRAVEN_PITR_POLL_INTERVAL: %w", err)
		}
		pollInterval = d
	}

	storageType := os.Getenv("BLOODRAVEN_PITR_STORAGE_TYPE")
	manifestPrefix := os.Getenv("BLOODRAVEN_PITR_MANIFEST_PREFIX")

	cfg := &PITRConfig{
		BinlogDir:      binlogDir,
		BinlogIndex:    binlogIndex,
		PollInterval:   pollInterval,
		StorageType:    storageType,
		ManifestPrefix: manifestPrefix,
		StateDir:       os.Getenv("BLOODRAVEN_PITR_STATE_DIR"),
	}

	switch storageType {
	case "S3":
		cfg.S3 = &PITRS3Config{
			Bucket:      os.Getenv("BLOODRAVEN_PITR_S3_BUCKET"),
			Region:      os.Getenv("BLOODRAVEN_PITR_S3_REGION"),
			EndpointURL: os.Getenv("BLOODRAVEN_PITR_S3_ENDPOINT_URL"),
			AWSCredsDir: os.Getenv("BLOODRAVEN_PITR_AWS_CREDS_DIR"),
		}
		if cfg.S3.Bucket == "" {
			return nil, fmt.Errorf("BLOODRAVEN_PITR_ENABLED=1 with STORAGE_TYPE=S3 but BLOODRAVEN_PITR_S3_BUCKET is empty")
		}
	case "PVC":
		cfg.PVC = &PITRPVCConfig{
			MountPath: os.Getenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH"),
			Writable:  true,
		}
		if cfg.PVC.MountPath == "" {
			return nil, fmt.Errorf("BLOODRAVEN_PITR_ENABLED=1 with STORAGE_TYPE=PVC but BLOODRAVEN_PITR_PVC_MOUNT_PATH is empty")
		}
	default:
		return nil, fmt.Errorf("BLOODRAVEN_PITR_ENABLED=1 but STORAGE_TYPE=%q; must be S3 or PVC", storageType)
	}

	// Encryption passphrase path. Optional: when unset the archiver
	// uploads plaintext binlogs (matching the pre-encryption default).
	cfg.PassphraseFile = os.Getenv("BLOODRAVEN_PITR_PASSPHRASE_FILE")

	// Legacy plaintext-fallback opt-in. Default: strict.
	if v := os.Getenv("BLOODRAVEN_PITR_ALLOW_PLAINTEXT_FALLBACK"); v == "1" || v == "true" {
		cfg.AllowPlaintextFallback = true
	}

	identity, err := ArchiveIdentityFromEnv(cfg)
	if err != nil {
		return nil, err
	}
	cfg.Archive = identity

	return cfg, nil
}
