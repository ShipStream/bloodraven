package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// ArchiverStatus mirrors the sidecar's /archiver/status JSON response.
// Must stay wire-compatible with internal/sidecar.Status. The operator
// polls this periodically to emit PITR metrics and populate
// MysqlFailoverGroup.status.pitr.
type ArchiverStatus struct {
	Enabled bool `json:"enabled"`
	// StoreReady is false while archival is configured but cannot reach a
	// verified archive session: the store cannot be built, the configured
	// archive identity is unusable, or establishing or opening the archive
	// is failing on a transient store error (LastError says why). A
	// refusal of a reachable archive keeps it true and is reported in
	// Archive. nil when the sidecar did not report it: PITR is not
	// configured, or the sidecar predates the field. nil is not false.
	StoreReady     *bool     `json:"storeReady,omitempty"`
	Primary        bool      `json:"primary"`
	LastScanAt     time.Time `json:"lastScanAt"`
	FilesArchived  int64     `json:"filesArchived"`
	LastError      string    `json:"lastError,omitempty"`
	StorageType    string    `json:"storageType"`
	ManifestPrefix string    `json:"manifestPrefix"`
	Site           string    `json:"site"`
	UploadFailures int64     `json:"uploadFailures"`
	// ConsecutiveFailedScans counts archiver scans in a row that ended
	// with LastError. nil from a sidecar that predates the field.
	ConsecutiveFailedScans *int64    `json:"consecutiveFailedScans,omitempty"`
	LastUploadAt           time.Time `json:"lastUploadAt,omitempty"`
	BacklogFiles           int64     `json:"backlogFiles"`
	ManifestFileCount      int64     `json:"manifestFileCount"`
	ManifestBytes          int64     `json:"manifestBytes"`
	OldestArchivedTime     time.Time `json:"oldestArchivedTime,omitempty"`
	NewestArchivedTime     time.Time `json:"newestArchivedTime,omitempty"`
	// Archive is the archive identity this sidecar observes (§3.7): its
	// destination, verified descriptor ID, encryption mode, state, refusal
	// reason and key revision. nil when PITR is not configured.
	Archive *pitrarchive.ArchiveStatus `json:"archive,omitempty"`
	// DivergentRows and QuarantinedRows count the Divergent and
	// Quarantined entries in this site's own manifest. A divergent row is
	// evidence the planner reads, not an archiver failure.
	DivergentRows   int64 `json:"divergentRows"`
	QuarantinedRows int64 `json:"quarantinedRows"`
	// ReissuableGTIDs is the part of those rows' transactions the site's
	// server no longer holds and could issue again: the ones under its own
	// current server_uuid. The runner warns PITRArchiveReissueRiskDetected
	// when a site that reports a non-empty set is promoted. The empty
	// string means the sidecar's last history audit found none. nil means
	// no verdict: the sidecar predates the field, no audit has completed
	// in its process, or its server has no history yet.
	ReissuableGTIDs *string `json:"reissuableGtids,omitempty"`
	// PromotionRecordedAt is the ObservedAt of the promotion record that
	// gates the current writable episode's uploads; zero while no record
	// is confirmed (D-43).
	PromotionRecordedAt time.Time `json:"promotionRecordedAt,omitzero"`
	// HistoryIntegrity is the site's history-integrity marker (D-34):
	// the one persisted in its manifest, or a pending one the sidecar
	// detected before the manifest existed (held for the pod's lifetime
	// only); nil when unmarked.
	HistoryIntegrity *pitrarchive.HistoryIntegrity `json:"historyIntegrity,omitempty"`
	// Tail is what the archiver's last scan decided about the sealed local
	// binary logs this site still has to archive. nil when PITR is not
	// configured, before the first scan that read the binary log index, and
	// from a sidecar that predates the field (which archives nothing while
	// its server is read-only).
	Tail *ArchiverTailStatus `json:"tail,omitempty"`
}

// The states of ArchiverTailStatus.
const (
	// ArchiverTailDrained: at the last scan every sealed local binary log
	// this site has to archive was in its manifest. A writable site has to
	// archive every sealed file; a read-only one only its writable-era
	// tail, the sealed files up to the last one that holds a transaction
	// the server originated. A read-only site with no such file is Drained.
	ArchiverTailDrained = "Drained"
	// ArchiverTailPending: at least one such file is not archived yet and
	// the archiver is working on it (PendingFiles).
	ArchiverTailPending = "Pending"
	// ArchiverTailBlocked: the scan could not decide or could not upload
	// (the role probe or the archive session failed, the history-integrity
	// marker is set, the site's records do not allow the upload, ...).
	// Reason says why.
	ArchiverTailBlocked = "Blocked"
)

// ArchiverTailStatus is the `tail` block of /archiver/status. The operator
// waits on it before a reclone wipes a site's binary logs: after its own
// FLUSH BINARY LOGS it needs a scan that saw the rotation (ActiveFile is
// the file the server wrote after that FLUSH, or a later one) and ended
// Drained.
type ArchiverTailStatus struct {
	// State is ArchiverTailDrained, ArchiverTailPending or
	// ArchiverTailBlocked.
	State string `json:"state"`
	// ActiveFile is the binary log the server was writing at the last
	// scan (the last entry of the index), and NewestSealedFile the entry
	// before it ("" when there is none). Both are of the index read State
	// is about, and "" from a scan that could not read the index.
	ActiveFile       string `json:"activeFile,omitempty"`
	NewestSealedFile string `json:"newestSealedFile,omitempty"`
	// PendingFiles counts the sealed files the site still has to archive.
	PendingFiles int64 `json:"pendingFiles"`
	// Reason says why the state is Blocked.
	Reason string `json:"reason,omitempty"`
}

// KeyringComponentStatus mirrors the sidecar's view of
// performance_schema.keyring_component_status.
type KeyringComponentStatus struct {
	Name     string `json:"name,omitempty"`
	Status   string `json:"status,omitempty"`
	DataFile string `json:"dataFile,omitempty"`
	ReadOnly bool   `json:"readOnly"`
}

// KeyringCoverage mirrors the sidecar's observed encryption coverage. A
// flag is nil when the sidecar did not report it.
type KeyringCoverage struct {
	SystemTablespaceEncrypted *bool `json:"systemTablespaceEncrypted,omitempty"`
	UnencryptedTablespaces    int64 `json:"unencryptedTablespaces"`
	RedoLogEncrypted          *bool `json:"redoLogEncrypted,omitempty"`
	UndoLogEncrypted          *bool `json:"undoLogEncrypted,omitempty"`
	BinlogEncrypted           *bool `json:"binlogEncrypted,omitempty"`
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
