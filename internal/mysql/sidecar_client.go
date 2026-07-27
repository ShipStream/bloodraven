package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ArchiverStatus mirrors the sidecar's /archiver/status JSON response.
// Must stay wire-compatible with internal/sidecar.Status. The operator
// polls this periodically to emit PITR metrics and populate
// MysqlFailoverGroup.status.pitr.
type ArchiverStatus struct {
	Enabled            bool      `json:"enabled"`
	Primary            bool      `json:"primary"`
	LastScanAt         time.Time `json:"lastScanAt"`
	FilesArchived      int64     `json:"filesArchived"`
	LastError          string    `json:"lastError,omitempty"`
	StorageType        string    `json:"storageType"`
	ManifestPrefix     string    `json:"manifestPrefix"`
	Site               string    `json:"site"`
	UploadFailures     int64     `json:"uploadFailures"`
	LastUploadAt       time.Time `json:"lastUploadAt,omitempty"`
	BacklogFiles       int64     `json:"backlogFiles"`
	ManifestFileCount  int64     `json:"manifestFileCount"`
	ManifestBytes      int64     `json:"manifestBytes"`
	OldestArchivedTime time.Time `json:"oldestArchivedTime,omitempty"`
	NewestArchivedTime time.Time `json:"newestArchivedTime,omitempty"`
}

// KeyringComponentStatus mirrors the sidecar's view of
// performance_schema.keyring_component_status.
type KeyringComponentStatus struct {
	Name     string `json:"name,omitempty"`
	Status   string `json:"status,omitempty"`
	DataFile string `json:"dataFile,omitempty"`
	ReadOnly bool   `json:"readOnly"`
}

// KeyringCoverage mirrors the sidecar's observed encryption coverage.
type KeyringCoverage struct {
	SystemTablespaceEncrypted bool  `json:"systemTablespaceEncrypted"`
	UnencryptedTablespaces    int64 `json:"unencryptedTablespaces"`
	RedoLogEncrypted          bool  `json:"redoLogEncrypted"`
	UndoLogEncrypted          bool  `json:"undoLogEncrypted"`
	BinlogEncrypted           bool  `json:"binlogEncrypted"`
}

// KeyringStatus mirrors the sidecar's /keyring/status JSON response.
// Must stay wire-compatible with internal/sidecar.KeyringStatus.
//
// The payload carries digests only — never key material. The operator
// uses Digest vs EscrowedDigest to decide whether a site's live keyring
// is fully captured in escrow, which is the gate on sealing a site.
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

// SidecarClient is an HTTP client for communicating with a sidecar.
type SidecarClient struct {
	httpClient *http.Client
	baseURL    string
}

// NewSidecarClient creates a new SidecarClient for the given base URL.
// The baseURL should include the scheme and host, e.g. "http://mysql-lion-dc1.shared-lion.svc.cluster.local:8080".
func NewSidecarClient(baseURL string) *SidecarClient {
	return &SidecarClient{
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
		baseURL: baseURL,
	}
}

// Ping checks if the sidecar is reachable via the /peer/ping endpoint.
func (c *SidecarClient) Ping(ctx context.Context) error {
	url := c.baseURL + "/peer/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ping sidecar: %w", err)
	}
	// Drain then close so net/http can return the connection to the
	// keep-alive pool — Ping runs on the sidecar health hot path.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sidecar ping returned status %d", resp.StatusCode)
	}

	return nil
}

// GetArchiverStatus fetches the PITR archiver's snapshot from the
// sidecar's /archiver/status endpoint. Returns a Status with Enabled=false
// when the sidecar responds 200 but PITR isn't configured on that site.
func (c *SidecarClient) GetArchiverStatus(ctx context.Context) (*ArchiverStatus, error) {
	url := c.baseURL + "/archiver/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get archiver status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sidecar returned status %d", resp.StatusCode)
	}

	var status ArchiverStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode archiver status: %w", err)
	}
	return &status, nil
}

// GetKeyringStatus fetches the encryption-at-rest keyring snapshot from
// the sidecar's /keyring/status endpoint. Returns a KeyringStatus with
// Enabled=false when the sidecar responds 200 but encryption is not
// configured for that site.
func (c *SidecarClient) GetKeyringStatus(ctx context.Context) (*KeyringStatus, error) {
	url := c.baseURL + "/keyring/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get keyring status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// A sidecar from a release that predates encryption-at-rest.
		// Treat it as "encryption not running here" rather than an
		// error so a partially-upgraded group still reconciles.
		return &KeyringStatus{Enabled: false}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sidecar returned status %d", resp.StatusCode)
	}

	var status KeyringStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode keyring status: %w", err)
	}
	return &status, nil
}
