package sidecar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/metrics"
)

// KeyringAgent escrows the local MySQL keyring file to the operator.
//
// Bloodraven runs encrypted sites in one of two renderings. Sealed sites
// project the keyring read-only from a Kubernetes Secret, so nothing can
// change and the agent only reports the live digest as a drift check.
// Unsealed sites — initial bootstrap, a CLONE INSTANCE recipient, or an
// admin-triggered rotation — run the keyring on a memory-backed
// emptyDir, and the agent is responsible for getting every byte MySQL
// writes there into durable escrow.
//
// The agent never decides that escrow is "good enough". It retries until
// the operator confirms the exact digest it observed, and the operator
// refuses to seal (and therefore refuses to consider the site protected)
// until that confirmation matches what it independently reads back from
// the escrow Secret.
type KeyringAgent struct {
	cfg    *KeyringConfig
	mysql  keyringQuerier
	logger *slog.Logger

	// identity, mirrored from the sidecar Config so the operator can
	// route the push to the right failover group and site.
	namespace string
	group     string
	site      string

	httpClient *http.Client

	mu     sync.Mutex
	status KeyringStatus

	// rotated records that this process already issued the rotation
	// statement. Rotation is idempotent from a safety standpoint (an
	// extra rotation only rewraps tablespace keys again), so a sidecar
	// restart re-running it is harmless — this flag just avoids doing it
	// on every poll tick.
	rotated bool

	// topology is the operator-authoritative active-site cache shared
	// with the fencing monitor. Replicated DDL (system tablespace
	// encryption) and empty-keyring master-key bootstrap must run only
	// on that site. Nil means "unknown" and fail-closes those writes.
	topology *TopologyCache
}

// KeyringConfig is the operator-supplied keyring wiring, parsed from
// environment variables. Nil when encryption-at-rest is disabled for the
// failover group.
type KeyringConfig struct {
	// Path is the absolute path of the keyring data file.
	Path string

	// EscrowArmed is true when this pod renders an unsealed keyring and
	// the agent must push changes to the operator. False on sealed pods,
	// where the agent only reports status.
	EscrowArmed bool

	// TokenFile holds the per-site bearer token the operator issued for
	// escrow pushes. Only mounted on unsealed pods.
	TokenFile string

	// EscrowURL is the operator's TLS-only keyring escrow endpoint.
	EscrowURL string

	// EscrowCAFile contains the CA that issued the operator escrow
	// listener's certificate.
	EscrowCAFile string

	// Rotate asks the agent to issue ALTER INSTANCE ROTATE INNODB MASTER
	// KEY once MySQL is reachable. Only ever set together with
	// EscrowArmed — rotating against a read-only keyring fails at the
	// engine level, which is exactly the protection sealing provides.
	Rotate bool

	// EncryptSystemTablespace asks the agent to run
	// `ALTER TABLESPACE mysql ENCRYPTION='Y'` when it observes the
	// system tablespace still in the clear.
	//
	// default_table_encryption does not cover the `mysql` tablespace, so
	// without this the data dictionary — every schema, table, and column
	// name — stays readable on a stolen PVC even though the row data is
	// encrypted. The statement reuses the existing master key, so it is
	// safe against a sealed, read-only keyring.
	EncryptSystemTablespace bool

	// PollInterval is how often the agent re-checks the keyring file.
	PollInterval time.Duration
}

// KeyringComponentStatus mirrors performance_schema.keyring_component_status.
type KeyringComponentStatus struct {
	Name     string `json:"name,omitempty"`
	Status   string `json:"status,omitempty"`
	DataFile string `json:"dataFile,omitempty"`
	ReadOnly bool   `json:"readOnly"`
}

// KeyringCoverage is what the instance actually reports about
// encryption, as opposed to what my.cnf asked for.
type KeyringCoverage struct {
	SystemTablespaceEncrypted bool  `json:"systemTablespaceEncrypted"`
	UnencryptedTablespaces    int64 `json:"unencryptedTablespaces"`
	RedoLogEncrypted          bool  `json:"redoLogEncrypted"`
	UndoLogEncrypted          bool  `json:"undoLogEncrypted"`
	BinlogEncrypted           bool  `json:"binlogEncrypted"`
}

// KeyringStatus is the sidecar's /keyring/status payload. It must stay
// wire-compatible with mysql.KeyringStatus on the operator side.
type KeyringStatus struct {
	Enabled     bool   `json:"enabled"`
	Path        string `json:"path,omitempty"`
	Present     bool   `json:"present"`
	Size        int64  `json:"size"`
	Digest      string `json:"digest,omitempty"`
	EscrowArmed bool   `json:"escrowArmed"`

	EscrowedDigest  string     `json:"escrowedDigest,omitempty"`
	EscrowedVersion int32      `json:"escrowedVersion,omitempty"`
	EscrowedSecret  string     `json:"escrowedSecret,omitempty"`
	LastEscrowAt    *time.Time `json:"lastEscrowAt,omitempty"`
	LastError       string     `json:"lastError,omitempty"`

	RotateRequested bool   `json:"rotateRequested"`
	RotateDone      bool   `json:"rotateDone"`
	RotateError     string `json:"rotateError,omitempty"`

	Component *KeyringComponentStatus `json:"component,omitempty"`
	Coverage  *KeyringCoverage        `json:"coverage,omitempty"`
}

// keyringQuerier is the MySQL surface the keyring agent needs. Kept
// separate from mysqlQuerier so the many existing sidecar test fakes
// don't have to grow methods they never exercise.
type keyringQuerier interface {
	KeyringComponentStatus(ctx context.Context) (*KeyringComponentStatus, error)
	EncryptionCoverage(ctx context.Context) (*KeyringCoverage, error)
	RotateInnoDBMasterKey(ctx context.Context) error
	EncryptSystemTablespace(ctx context.Context) error
	IsReadOnly(ctx context.Context) (bool, error)
}

// escrowRequest is the POST body the agent sends to the operator.
// Wire-compatible with controller.KeyringEscrowRequest.
type escrowRequest struct {
	Namespace string `json:"namespace"`
	Group     string `json:"group"`
	Site      string `json:"site"`
	Digest    string `json:"digest"`
	Keyring   string `json:"keyring"` // base64
}

// escrowResponse is the operator's reply.
type escrowResponse struct {
	Version int32  `json:"version"`
	Digest  string `json:"digest"`
	Secret  string `json:"secret"`
}

// NewKeyringAgent builds an agent. cfg must be non-nil.
func NewKeyringAgent(cfg *KeyringConfig, sidecarCfg *Config, mysql keyringQuerier, logger *slog.Logger) (*KeyringAgent, error) {
	httpClient := &http.Client{Timeout: 10 * time.Second}
	if cfg.EscrowArmed {
		var err error
		httpClient, err = newEscrowHTTPClient(cfg.EscrowURL, cfg.EscrowCAFile)
		if err != nil {
			return nil, err
		}
	}
	return &KeyringAgent{
		cfg:        cfg,
		mysql:      mysql,
		logger:     logger,
		namespace:  sidecarCfg.PodNamespace,
		group:      sidecarCfg.FailoverGroup,
		site:       sidecarCfg.MySite,
		httpClient: httpClient,
		status: KeyringStatus{
			Enabled:         true,
			Path:            cfg.Path,
			EscrowArmed:     cfg.EscrowArmed,
			RotateRequested: cfg.Rotate,
		},
	}, nil
}

// WithTopology attaches the shared operator-authoritative active-site
// cache. Call once after construction and before Run. Returns the
// receiver so it can be chained. A nil cache leaves replicated keyring
// writes fail-closed.
func (a *KeyringAgent) WithTopology(cache *TopologyCache) *KeyringAgent {
	a.topology = cache
	return a
}

// isAuthoritativePrimary reports whether the operator currently names
// this site as the active primary. Unknown (nil cache or empty
// ActiveSite) is not authority — MySQL coming up writable after a pod
// roll is not enough to issue replicated DDL.
func (a *KeyringAgent) isAuthoritativePrimary() bool {
	if a.topology == nil {
		return false
	}
	snap := a.topology.Snapshot()
	return snap.ActiveSite != "" && snap.ActiveSite == a.site
}

func newEscrowHTTPClient(rawURL, caFile string) (*http.Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ESCROW_URL must be an https URL")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read escrow CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ESCROW_CA_FILE contains no valid certificates")
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
		}},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return fmt.Errorf("escrow redirects are not allowed")
		},
	}, nil
}

// Snapshot returns the current status for /keyring/status.
func (a *KeyringAgent) Snapshot() KeyringStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

// Run drives the agent until ctx is cancelled.
func (a *KeyringAgent) Run(ctx context.Context) {
	interval := a.cfg.PollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	a.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tick(ctx)
		}
	}
}

func (a *KeyringAgent) tick(ctx context.Context) {
	// Rotation first: it is the thing that changes the file, and doing
	// it before the read means the same tick escrows the result.
	if a.cfg.Rotate && a.cfg.EscrowArmed && !a.rotated {
		rotCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := a.mysql.RotateInnoDBMasterKey(rotCtx)
		cancel()
		if err != nil {
			// Tracked separately from LastError so a subsequent
			// successful escrow does not erase the fact that the
			// rotation itself failed — the admin still needs to know.
			a.mu.Lock()
			a.status.RotateError = fmt.Sprintf("rotate innodb master key: %v", err)
			a.mu.Unlock()
			a.logger.Error("keyring rotation failed", "error", err, "site", a.site)
			metrics.KeyringRotationsTotal.WithLabelValues(a.group, a.site, "failure").Inc()
			// Fall through: even a failed rotation may have written the
			// keyring, so we still want to escrow whatever is on disk.
		} else {
			a.rotated = true
			a.mu.Lock()
			a.status.RotateDone = true
			a.status.RotateError = ""
			a.mu.Unlock()
			a.logger.Info("rotated innodb master key", "site", a.site)
			metrics.KeyringRotationsTotal.WithLabelValues(a.group, a.site, "success").Inc()
		}
	}

	// Sample MySQL and perform the system-tablespace fix-up before reading
	// the keyring. ALTER TABLESPACE may add a master key; escrowing bytes
	// captured before that statement can make the next sealed restart
	// unrecoverable even though the operator verified the stale digest.
	viewErr := a.refreshMySQLView(ctx)

	raw, err := os.ReadFile(a.cfg.Path)
	if err != nil {
		a.mu.Lock()
		a.status.Present = false
		a.status.Size = 0
		a.status.Digest = ""
		if !errors.Is(err, os.ErrNotExist) {
			a.status.LastError = fmt.Sprintf("read keyring: %v", err)
		} else {
			a.status.LastError = ""
		}
		a.mu.Unlock()
		return
	}

	digest := ""
	if len(raw) > 0 && !emptyKeyringDocument(raw) {
		sum := sha256.Sum256(raw)
		digest = "sha256:" + hex.EncodeToString(sum[:])
	}

	a.mu.Lock()
	a.status.Present = true
	a.status.Size = int64(len(raw))
	a.status.Digest = digest
	alreadyEscrowed := a.status.EscrowedDigest == digest && digest != ""
	a.mu.Unlock()

	if !a.cfg.EscrowArmed || digest == "" || alreadyEscrowed {
		a.setOperationalError(viewErr)
		return
	}
	pushErr := a.push(ctx, raw, digest)
	a.setOperationalError(errors.Join(viewErr, pushErr))
	if pushErr != nil {
		a.logger.Warn("keyring escrow push failed, will retry",
			"error", pushErr, "site", a.site, "digest", digest)
		metrics.KeyringEscrowPushesTotal.WithLabelValues(a.group, a.site, "failure").Inc()
		return
	}
	metrics.KeyringEscrowPushesTotal.WithLabelValues(a.group, a.site, "success").Inc()
}

// emptyKeyringDocument recognizes the valid bootstrap file written by
// keyring-init before MySQL has created any keys. It must not be escrowed:
// sealing a pod against this document leaves encrypted redo/binlogs without
// a recoverable master key on the next restart.
//
// Unknown or malformed content is not classified as empty here. MySQL owns
// the on-disk format, and refusing a future valid format would strand an
// otherwise healthy site; the sealed restart and component-status checks
// remain the final validation.
func emptyKeyringDocument(raw []byte) bool {
	var doc struct {
		Elements *[]json.RawMessage `json:"elements"`
	}
	return json.Unmarshal(raw, &doc) == nil && doc.Elements != nil && len(*doc.Elements) == 0
}

// refreshMySQLView samples the keyring component status and the
// encryption coverage. Failures are recorded but never fatal: MySQL may
// simply not be up yet, and the escrow path must not depend on it.
func (a *KeyringAgent) refreshMySQLView(ctx context.Context) error {
	if a.mysql == nil {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	comp, err := a.mysql.KeyringComponentStatus(qCtx)
	if err != nil {
		err = fmt.Errorf("keyring component status: %w", err)
	}
	cov, coverageErr := a.mysql.EncryptionCoverage(qCtx)
	if coverageErr != nil {
		coverageErr = fmt.Errorf("encryption coverage: %w", coverageErr)
		cov = nil
	}

	a.mu.Lock()
	a.status.Component = comp
	a.status.Coverage = cov
	a.mu.Unlock()
	viewErr := errors.Join(err, coverageErr)

	needWrite := false
	emptyKeyring := false
	if a.cfg.EscrowArmed && comp != nil && comp.Status == "Active" && !comp.ReadOnly {
		if raw, readErr := os.ReadFile(a.cfg.Path); readErr == nil && emptyKeyringDocument(raw) {
			emptyKeyring = true
			needWrite = true
		}
	}
	if a.cfg.EncryptSystemTablespace && cov != nil && !cov.SystemTablespaceEncrypted {
		needWrite = true
	}
	if !needWrite {
		return viewErr
	}

	// Only the operator-authoritative primary may create keys or
	// rewrite tablespaces. Replicas wait for the primary's binlog (or
	// for promotion). IsReadOnly() alone is not enough: MySQL does not
	// persist read_only across a pod replacement, so a just-rolled
	// replica is writable until fencing finishes. Issuing ALTER
	// TABLESPACE there plants a GTID the real primary does not have.
	if !a.isAuthoritativePrimary() {
		return viewErr
	}
	readOnly, roErr := a.mysql.IsReadOnly(qCtx)
	if roErr != nil {
		return errors.Join(viewErr, fmt.Errorf("query read_only: %w", roErr))
	}
	if readOnly {
		return viewErr
	}

	// Bootstrap: an empty component_keyring_file document has no master
	// key, so ALTER TABLESPACE ENCRYPTION fails with Error 3185.
	// binlog/redo encryption usually seeds the keyring at startup, but
	// on some adoption paths the keyring stays empty until something
	// creates a key. ROTATE INNODB MASTER KEY is the explicit
	// create-if-missing path and is safe on a writable keyring.
	if emptyKeyring {
		writeCtx, writeCancel := context.WithTimeout(ctx, 30*time.Second)
		err := a.mysql.RotateInnoDBMasterKey(writeCtx)
		writeCancel()
		if err != nil {
			a.logger.Warn("could not bootstrap innodb master key", "error", err, "site", a.site)
			return errors.Join(viewErr, fmt.Errorf("bootstrap innodb master key: %w", err))
		}
		a.logger.Info("bootstrapped innodb master key into empty keyring", "site", a.site)
	}

	// Close the one coverage gap that default_table_encryption cannot:
	// the `mysql` system tablespace holding the data dictionary. This is
	// replicated DDL, so running it on the primary carries it to the
	// replicas.
	if a.cfg.EncryptSystemTablespace && cov != nil && !cov.SystemTablespaceEncrypted {
		writeCtx, writeCancel := context.WithTimeout(ctx, 30*time.Second)
		err := a.mysql.EncryptSystemTablespace(writeCtx)
		writeCancel()
		if err != nil {
			a.logger.Warn("could not encrypt the mysql system tablespace", "error", err, "site", a.site)
			return errors.Join(viewErr, fmt.Errorf("encrypt system tablespace: %w", err))
		}
		a.logger.Info("encrypted the mysql system tablespace", "site", a.site)
	}
	return viewErr
}

// push sends the keyring to the operator and records the confirmed
// version. The operator is the only writer of escrow Secrets; the agent
// holds no Kubernetes credentials at all.
func (a *KeyringAgent) push(ctx context.Context, raw []byte, digest string) error {
	token, err := readTrimFile(a.cfg.TokenFile)
	if err != nil {
		return fmt.Errorf("read escrow token: %w", err)
	}
	if token == "" {
		return fmt.Errorf("escrow token is empty")
	}

	body, err := json.Marshal(escrowRequest{
		Namespace: a.namespace,
		Group:     a.group,
		Site:      a.site,
		Digest:    digest,
		Keyring:   base64.StdEncoding.EncodeToString(raw),
	})
	if err != nil {
		return fmt.Errorf("marshal escrow request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, a.cfg.EscrowURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create escrow request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post escrow: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Drain so HTTP/1.x can reuse the connection on retry, but never
		// propagate the body into LastError. The sidecar status is copied
		// into the MysqlFailoverGroup status, and an upstream error page
		// may contain credentials, tokens, or key material. The HTTP
		// status is sufficient and safe diagnostics.
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("escrow rejected with HTTP status %d", resp.StatusCode)
	}

	var out escrowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode escrow response: %w", err)
	}
	// The operator echoes the digest it actually stored. Treating a
	// mismatch as success would defeat the entire point of the gate.
	if out.Digest != digest {
		return fmt.Errorf("operator stored digest %q but we sent %q", out.Digest, digest)
	}

	a.mu.Lock()
	a.status.EscrowedDigest = out.Digest
	a.status.EscrowedVersion = out.Version
	a.status.EscrowedSecret = out.Secret
	now := time.Now().UTC()
	a.status.LastEscrowAt = &now
	a.mu.Unlock()

	a.logger.Info("keyring escrowed",
		"site", a.site, "version", out.Version, "secret", out.Secret, "digest", out.Digest)
	return nil
}

func (a *KeyringAgent) setError(msg string) {
	a.mu.Lock()
	a.status.LastError = msg
	a.mu.Unlock()
}

func (a *KeyringAgent) setOperationalError(err error) {
	if err == nil {
		a.setError("")
		return
	}
	a.setError(err.Error())
}

func envBool(name string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}
	return value, nil
}

// keyringConfigFromEnv builds the keyring wiring from the operator's env
// vars. Returns nil when encryption-at-rest is not enabled for this
// failover group.
func keyringConfigFromEnv() (*KeyringConfig, error) {
	enabled, err := envBool("BLOODRAVEN_KEYRING_ENABLED")
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, nil
	}
	path := os.Getenv("BLOODRAVEN_KEYRING_FILE")
	if path == "" {
		return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ENABLED=1 but BLOODRAVEN_KEYRING_FILE is empty")
	}

	cfg := &KeyringConfig{
		Path:         path,
		PollInterval: 5 * time.Second,
	}
	escrow, err := envBool("BLOODRAVEN_KEYRING_ESCROW")
	if err != nil {
		return nil, err
	}
	if escrow {
		cfg.EscrowArmed = true
		cfg.TokenFile = os.Getenv("BLOODRAVEN_KEYRING_TOKEN_FILE")
		if cfg.TokenFile == "" {
			return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ESCROW=1 but BLOODRAVEN_KEYRING_TOKEN_FILE is empty")
		}
		cfg.EscrowURL = strings.TrimSpace(os.Getenv("BLOODRAVEN_KEYRING_ESCROW_URL"))
		if cfg.EscrowURL == "" {
			return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ESCROW=1 but BLOODRAVEN_KEYRING_ESCROW_URL is empty")
		}
		cfg.EscrowCAFile = strings.TrimSpace(os.Getenv("BLOODRAVEN_KEYRING_ESCROW_CA_FILE"))
		if cfg.EscrowCAFile == "" {
			return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ESCROW=1 but BLOODRAVEN_KEYRING_ESCROW_CA_FILE is empty")
		}
	}
	rotate, err := envBool("BLOODRAVEN_KEYRING_ROTATE")
	if err != nil {
		return nil, err
	}
	if rotate {
		if !cfg.EscrowArmed {
			// A rotation on a sealed pod would be rejected by MySQL
			// anyway, and quietly accepting the flag would hide an
			// operator bug. Fail loudly at startup instead.
			return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ROTATE=1 requires BLOODRAVEN_KEYRING_ESCROW=1")
		}
		cfg.Rotate = true
	}
	encryptSystemTablespace, err := envBool("BLOODRAVEN_KEYRING_ENCRYPT_SYSTEM_TABLESPACE")
	if err != nil {
		return nil, err
	}
	if encryptSystemTablespace {
		cfg.EncryptSystemTablespace = true
	}
	if v := os.Getenv("BLOODRAVEN_KEYRING_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse BLOODRAVEN_KEYRING_POLL_INTERVAL: %w", err)
		}
		cfg.PollInterval = d
	}
	return cfg, nil
}
