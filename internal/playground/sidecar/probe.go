// Package sidecar opens an HTTP client to a sidecar container's port
// 8080 endpoints via a port-forwarded SPDY tunnel.
package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	pod, err := k.GetSiteMysqlPod(ctx, namespace, fg, site)
	if err != nil {
		return nil, err
	}
	pf, err := k.PortForwardPod(ctx, namespace, pod.Name, 8080)
	if err != nil {
		return nil, fmt.Errorf("port-forward sidecar for site %s: %w", site, err)
	}
	return &Probe{
		Site: site,
		pf:   pf,
		cli:  &http.Client{Timeout: 4 * time.Second},
		base: fmt.Sprintf("http://127.0.0.1:%d", pf.LocalPort),
	}, nil
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
