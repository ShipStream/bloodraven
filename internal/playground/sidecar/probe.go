// Package sidecar opens an HTTP client to a sidecar container's port
// 8080 endpoints via a port-forwarded SPDY tunnel.
package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// Probe is a port-forwarded HTTP client to a single site's sidecar.
type Probe struct {
	Site string
	pf   *pgkube.PortForward
	cli  *http.Client
	base string
}

// Close releases the SPDY tunnel.
func (p *Probe) Close() {
	if p.pf != nil {
		p.pf.Stop()
	}
}

// Open opens a sidecar probe for a site.
func Open(ctx context.Context, k *pgkube.Client, namespace, fg, site string) (*Probe, error) {
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for {
		pod, err := k.GetSiteMysqlPod(ctx, namespace, fg, site)
		if err != nil {
			last = err
		} else {
			pf, err := k.PortForwardPod(ctx, namespace, pod.Name, 8080)
			if err == nil {
				probe := &Probe{
					Site: site,
					pf:   pf,
					cli:  &http.Client{Timeout: 4 * time.Second},
					base: fmt.Sprintf("http://127.0.0.1:%d", pf.LocalPort),
				}
				healthCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
				ok, body, healthErr := probe.Health(healthCtx)
				cancel()
				if healthErr == nil && ok {
					return probe, nil
				}
				probe.Close()
				if healthErr != nil {
					last = fmt.Errorf("sidecar health check for site %s: %w", site, healthErr)
				} else {
					last = fmt.Errorf("sidecar health check for site %s returned not ready: %s", site, body)
				}
			} else {
				last = fmt.Errorf("port-forward sidecar for site %s: %w", site, err)
				if !retryableOpenError(err) {
					return nil, last
				}
			}
		}
		if time.Now().After(deadline) {
			return nil, last
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("open sidecar probe for site %s: %w (last: %v)", site, ctx.Err(), last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func retryableOpenError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "pod not found") ||
		strings.Contains(msg, "unable to upgrade connection") ||
		strings.Contains(msg, "failed before ready") ||
		strings.Contains(msg, "network namespace for sandbox") ||
		strings.Contains(msg, "connection refused")
}

// Health calls GET /health and returns true on 200.
func (p *Probe) Health(ctx context.Context) (bool, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/health", nil)
	if err != nil {
		return false, "", err
	}
	resp, err := p.cli.Do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode == http.StatusOK, string(body), nil
}

// StatusResponse mirrors internal/sidecar.StatusInfo. Duplicated here
// rather than imported to keep internal/sidecar private to its
// package and avoid an import cycle around the metrics dependency.
type StatusResponse struct {
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

// Status calls GET /status and decodes the JSON response.
func (p *Probe) Status(ctx context.Context) (*StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sidecar /status returned %d: %s", resp.StatusCode, body)
	}
	var s StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, fmt.Errorf("decode status: %w", err)
	}
	return &s, nil
}

// ArchiverStatusResponse mirrors internal/sidecar.Status. Duplicated here to
// keep the playground probe decoupled from the sidecar implementation package.
type ArchiverStatusResponse struct {
	Enabled            bool      `json:"enabled"`
	Primary            bool      `json:"primary"`
	LastScanAt         time.Time `json:"lastScanAt"`
	FilesArchived      int64     `json:"filesArchived"`
	LastError          string    `json:"lastError"`
	StorageType        string    `json:"storageType"`
	ManifestPrefix     string    `json:"manifestPrefix"`
	Site               string    `json:"site"`
	UploadFailures     int64     `json:"uploadFailures"`
	LastUploadAt       time.Time `json:"lastUploadAt"`
	BacklogFiles       int64     `json:"backlogFiles"`
	ManifestFileCount  int64     `json:"manifestFileCount"`
	ManifestBytes      int64     `json:"manifestBytes"`
	OldestArchivedTime time.Time `json:"oldestArchivedTime"`
	NewestArchivedTime time.Time `json:"newestArchivedTime"`
}

// ArchiverStatus calls GET /archiver/status and decodes the JSON response.
func (p *Probe) ArchiverStatus(ctx context.Context) (*ArchiverStatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/archiver/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sidecar /archiver/status returned %d: %s", resp.StatusCode, body)
	}
	var s ArchiverStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, fmt.Errorf("decode archiver/status: %w", err)
	}
	return &s, nil
}

// PeerActiveSiteResponse mirrors internal/sidecar.TopologySnapshot.
type PeerActiveSiteResponse struct {
	ActiveSite string    `json:"activeSite"`
	ObservedAt time.Time `json:"observedAt"`
}

// PeerActiveSite calls GET /peer/active-site. Returns
// (nil, nil) on 204 No Content (sidecar has no view yet).
func (p *Probe) PeerActiveSite(ctx context.Context) (*PeerActiveSiteResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/peer/active-site", nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sidecar /peer/active-site returned %d: %s", resp.StatusCode, body)
	}
	var r PeerActiveSiteResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("decode peer/active-site: %w", err)
	}
	return &r, nil
}
