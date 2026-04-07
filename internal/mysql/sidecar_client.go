package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// SidecarStatus is the JSON response from a sidecar's /status endpoint.
type SidecarStatus struct {
	Role                string `json:"role"`
	ReadOnly            bool   `json:"read_only"`
	SuperReadOnly       bool   `json:"super_read_only"`
	GtidExecuted        string `json:"gtid_executed"`
	ReplicaIORunning    bool   `json:"replica_io_running"`
	ReplicaSQLRunning   bool   `json:"replica_sql_running"`
	SecondsBehindSource *int64 `json:"seconds_behind_source"`
	ServerID            int    `json:"server_id"`
	Uptime              int64  `json:"uptime"`
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

// GetStatus fetches the MySQL status from the sidecar's /status endpoint.
func (c *SidecarClient) GetStatus(ctx context.Context) (*SidecarStatus, error) {
	url := c.baseURL + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get sidecar status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sidecar returned status %d", resp.StatusCode)
	}

	var status SidecarStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode sidecar status: %w", err)
	}

	return &status, nil
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
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sidecar ping returned status %d", resp.StatusCode)
	}

	return nil
}
