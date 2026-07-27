package sidecar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
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

	bloodravenAddress string
	httpClient        *http.Client

	mu     sync.Mutex
	status KeyringStatus

	// rotated records that this process already issued the rotation
	// statement. Rotation is idempotent from a safety standpoint (an
	// extra rotation only rewraps tablespace keys again), so a sidecar
	// restart re-running it is harmless — this flag just avoids doing it
	// on every poll tick.
	rotated bool
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

	EscrowedDigest  string    `json:"escrowedDigest,omitempty"`
	EscrowedVersion int32     `json:"escrowedVersion,omitempty"`
	EscrowedSecret  string    `json:"escrowedSecret,omitempty"`
	LastEscrowAt    time.Time `json:"lastEscrowAt,omitempty"`
	LastError       string    `json:"lastError,omitempty"`

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
func NewKeyringAgent(cfg *KeyringConfig, sidecarCfg *Config, mysql keyringQuerier, logger *slog.Logger) *KeyringAgent {
	return &KeyringAgent{
		cfg:               cfg,
		mysql:             mysql,
		logger:            logger,
		namespace:         sidecarCfg.PodNamespace,
		group:             sidecarCfg.FailoverGroup,
		site:              sidecarCfg.MySite,
		bloodravenAddress: sidecarCfg.BloodravenAddress,
		httpClient:        &http.Client{Timeout: 10 * time.Second},
		status: KeyringStatus{
			Enabled:         true,
			Path:            cfg.Path,
			EscrowArmed:     cfg.EscrowArmed,
			RotateRequested: cfg.Rotate,
		},
	}
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

	raw, err := os.ReadFile(a.cfg.Path)
	if err != nil {
		a.mu.Lock()
		a.status.Present = false
		a.status.Size = 0
		a.status.Digest = ""
		if !errors.Is(err, os.ErrNotExist) {
			a.status.LastError = fmt.Sprintf("read keyring: %v", err)
		}
		a.mu.Unlock()
		return
	}

	digest := ""
	if len(raw) > 0 {
		sum := sha256.Sum256(raw)
		digest = "sha256:" + hex.EncodeToString(sum[:])
	}

	a.mu.Lock()
	a.status.Present = true
	a.status.Size = int64(len(raw))
	a.status.Digest = digest
	alreadyEscrowed := a.status.EscrowedDigest == digest && digest != ""
	a.mu.Unlock()

	a.refreshMySQLView(ctx)

	if !a.cfg.EscrowArmed || digest == "" || alreadyEscrowed {
		return
	}
	if err := a.push(ctx, raw, digest); err != nil {
		a.setError(err.Error())
		a.logger.Warn("keyring escrow push failed, will retry",
			"error", err, "site", a.site, "digest", digest)
		metrics.KeyringEscrowPushesTotal.WithLabelValues(a.group, a.site, "failure").Inc()
		return
	}
	metrics.KeyringEscrowPushesTotal.WithLabelValues(a.group, a.site, "success").Inc()
}

// refreshMySQLView samples the keyring component status and the
// encryption coverage. Failures are recorded but never fatal: MySQL may
// simply not be up yet, and the escrow path must not depend on it.
func (a *KeyringAgent) refreshMySQLView(ctx context.Context) {
	if a.mysql == nil {
		return
	}
	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	comp, err := a.mysql.KeyringComponentStatus(qCtx)
	if err != nil {
		return
	}
	cov, err := a.mysql.EncryptionCoverage(qCtx)
	if err != nil {
		cov = nil
	}

	a.mu.Lock()
	a.status.Component = comp
	if cov != nil {
		a.status.Coverage = cov
	}
	a.mu.Unlock()

	// Close the one coverage gap that default_table_encryption cannot:
	// the `mysql` system tablespace holding the data dictionary. Only
	// attempt it on a writable instance — this is replicated DDL, so
	// running it on the primary carries it to the replicas, and running
	// it on a read-only replica would just fail.
	if a.cfg.EncryptSystemTablespace && cov != nil && !cov.SystemTablespaceEncrypted {
		readOnly, err := a.mysql.IsReadOnly(qCtx)
		if err != nil || readOnly {
			return
		}
		if err := a.mysql.EncryptSystemTablespace(qCtx); err != nil {
			a.setError(fmt.Sprintf("encrypt system tablespace: %v", err))
			a.logger.Warn("could not encrypt the mysql system tablespace", "error", err, "site", a.site)
			return
		}
		a.logger.Info("encrypted the mysql system tablespace", "site", a.site)
	}
}

// push sends the keyring to the operator and records the confirmed
// version. The operator is the only writer of escrow Secrets; the agent
// holds no Kubernetes credentials at all.
func (a *KeyringAgent) push(ctx context.Context, raw []byte, digest string) error {
	if a.bloodravenAddress == "" {
		return fmt.Errorf("no bloodraven address configured")
	}
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
	url := fmt.Sprintf("http://%s/keyring/escrow", a.bloodravenAddress)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
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
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("escrow rejected with status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(msg)))
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
	a.status.LastEscrowAt = time.Now().UTC()
	a.status.LastError = ""
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

// keyringConfigFromEnv builds the keyring wiring from the operator's env
// vars. Returns nil when encryption-at-rest is not enabled for this
// failover group.
func keyringConfigFromEnv() (*KeyringConfig, error) {
	if v := os.Getenv("BLOODRAVEN_KEYRING_ENABLED"); v == "" || v == "0" || v == "false" {
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
	if v := os.Getenv("BLOODRAVEN_KEYRING_ESCROW"); v == "1" || v == "true" {
		cfg.EscrowArmed = true
		cfg.TokenFile = os.Getenv("BLOODRAVEN_KEYRING_TOKEN_FILE")
		if cfg.TokenFile == "" {
			return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ESCROW=1 but BLOODRAVEN_KEYRING_TOKEN_FILE is empty")
		}
	}
	if v := os.Getenv("BLOODRAVEN_KEYRING_ROTATE"); v == "1" || v == "true" {
		if !cfg.EscrowArmed {
			// A rotation on a sealed pod would be rejected by MySQL
			// anyway, and quietly accepting the flag would hide an
			// operator bug. Fail loudly at startup instead.
			return nil, fmt.Errorf("BLOODRAVEN_KEYRING_ROTATE=1 requires BLOODRAVEN_KEYRING_ESCROW=1")
		}
		cfg.Rotate = true
	}
	if v := os.Getenv("BLOODRAVEN_KEYRING_ENCRYPT_SYSTEM_TABLESPACE"); v == "1" || v == "true" {
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
