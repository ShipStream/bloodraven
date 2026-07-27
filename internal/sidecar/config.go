package sidecar

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
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
// only the primary site uploads binlogs; the replica's archiver stays
// idle, polling its own role, and takes over after failover.
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
}

// PITRPVCConfig is the local-disk archiver config.
type PITRPVCConfig struct {
	// MountPath is where the backup PVC is mounted inside the sidecar.
	// Binlog objects land under MountPath + "/binlogs/" + site.
	MountPath string
}

// ConfigFromEnv reads sidecar configuration from environment variables.
func ConfigFromEnv() (*Config, error) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		// Prefer mounted-file credentials (MYSQL_CREDS_DIR/{username,
		// password}) over env-var credentials so the hottest secret in
		// the system doesn't end up in /proc/<pid>/environ or crash
		// dumps (AUDIT L2). Falls through to MYSQL_USER / MYSQL_PASSWORD
		// for backward compatibility.
		credsDir := os.Getenv("MYSQL_CREDS_DIR")
		if credsDir != "" {
			user, err := readTrimFile(credsDir + "/username")
			if err != nil {
				return nil, fmt.Errorf("read MYSQL_CREDS_DIR/username: %w", err)
			}
			password, err := readTrimFile(credsDir + "/password")
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
				return nil, fmt.Errorf("one of MYSQL_DSN, MYSQL_CREDS_DIR, or MYSQL_USER is required")
			}
			dsn = fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/", user, password)
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

	return cfg, nil
}
