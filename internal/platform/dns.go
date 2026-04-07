package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DNSUpdater updates DNS records for failover.
type DNSUpdater interface {
	UpdateAZRecord(ctx context.Context, ip string) error
}

type cloudflareDNS struct {
	apiToken string
	zoneID   string
	az       string
	client   *http.Client
}

func NewCloudflareDNS(apiToken, zoneID, az string) DNSUpdater {
	return &cloudflareDNS{
		apiToken: apiToken,
		zoneID:   zoneID,
		az:       az,
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

type cfListResponse struct {
	Result []struct {
		ID string `json:"id"`
	} `json:"result"`
	Success bool `json:"success"`
}

type cfUpdateRequest struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
}

func (c *cloudflareDNS) UpdateAZRecord(ctx context.Context, ip string) error {
	recordName := fmt.Sprintf("%s.az.shipstream.app", c.az)

	// Find existing record
	recordID, err := c.findRecord(ctx, recordName)
	if err != nil {
		return err
	}

	// Update it
	body := cfUpdateRequest{
		Type:    "A",
		Name:    recordName,
		Content: ip,
		TTL:     60,
	}
	data, _ := json.Marshal(body)

	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records/%s", c.zoneID, recordID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("cloudflare update: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cloudflare update failed (%d): %s", resp.StatusCode, respBody)
	}
	return nil
}

func (c *cloudflareDNS) findRecord(ctx context.Context, name string) (string, error) {
	url := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/dns_records?type=A&name=%s", c.zoneID, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("cloudflare list: %w", err)
	}
	defer resp.Body.Close()

	var result cfListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode cloudflare response: %w", err)
	}
	if !result.Success || len(result.Result) == 0 {
		return "", fmt.Errorf("DNS record %s not found", name)
	}
	return result.Result[0].ID, nil
}
