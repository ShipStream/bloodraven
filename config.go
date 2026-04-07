package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all watcher configuration from environment variables.
type Config struct {
	AZ    string
	DC1   DCConfig
	DC2   DCConfig

	CloudflareAPIToken string
	CloudflareZoneID   string

	PollInterval      time.Duration
	FailureThreshold  int
	RecoveryThreshold int

	ListenAddr string
}

// DCConfig holds per-DC configuration.
type DCConfig struct {
	Name     string
	MysqlDSN string
	LBIP     string
}

func LoadConfig() (Config, error) {
	var missing []string
	require := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			missing = append(missing, key)
		}
		return v
	}

	pollSec := envOrDefault("POLL_INTERVAL", "2")
	pollInt, err := strconv.Atoi(pollSec)
	if err != nil {
		return Config{}, fmt.Errorf("invalid POLL_INTERVAL: %w", err)
	}

	failThresh, err := strconv.Atoi(envOrDefault("FAILURE_THRESHOLD", "3"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid FAILURE_THRESHOLD: %w", err)
	}

	recThresh, err := strconv.Atoi(envOrDefault("RECOVERY_THRESHOLD", "2"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid RECOVERY_THRESHOLD: %w", err)
	}

	cfg := Config{
		AZ: require("AZ"),
		DC1: DCConfig{
			Name:     require("DC1_NAME"),
			MysqlDSN: require("DC1_MYSQL_DSN"),
			LBIP:     require("DC1_LB_IP"),
		},
		DC2: DCConfig{
			Name:     require("DC2_NAME"),
			MysqlDSN: require("DC2_MYSQL_DSN"),
			LBIP:     require("DC2_LB_IP"),
		},
		CloudflareAPIToken: require("CLOUDFLARE_API_TOKEN"),
		CloudflareZoneID:   require("CLOUDFLARE_ZONE_ID"),
		PollInterval:       time.Duration(pollInt) * time.Second,
		FailureThreshold:   failThresh,
		RecoveryThreshold:  recThresh,
		ListenAddr:         envOrDefault("LISTEN_ADDR", ":8080"),
	}

	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
