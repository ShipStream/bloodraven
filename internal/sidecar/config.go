package sidecar

import (
	"fmt"
	"os"
	"time"
)

// Config holds sidecar configuration parsed from environment variables.
type Config struct {
	// MysqlDSN is the DSN for connecting to the local MySQL instance.
	MysqlDSN string

	// PodName is the name of the pod, used for logging.
	PodName string

	// ListenAddr is the HTTP listen address (default ":8080").
	ListenAddr string

	// PeerAddress is the address of the peer sidecar (e.g. "mysql-lion-pdx.shared-lion.svc.cluster.local:8080").
	PeerAddress string

	// BloodravenAddress is the address of the Bloodraven operator's auxiliary HTTP server.
	BloodravenAddress string

	// LeaseTimeout is how long both Bloodraven and peer must be unreachable before self-fencing.
	LeaseTimeout time.Duration

	// PeerCheckInterval is how often the fencing monitor checks Bloodraven and peer.
	PeerCheckInterval time.Duration

	// MySite is the site this sidecar belongs to.
	MySite string

	// PodNamespace is the namespace of the pod.
	PodNamespace string

	// FailoverGroup is the name of the MysqlFailoverGroup CR this sidecar belongs to.
	FailoverGroup string
}

// ConfigFromEnv reads sidecar configuration from environment variables.
func ConfigFromEnv() (*Config, error) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("MYSQL_DSN is required")
	}

	podName := os.Getenv("MY_POD_NAME")
	if podName == "" {
		podName = "unknown"
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	peerAddress := os.Getenv("PEER_ADDRESS")
	bloodravenAddress := os.Getenv("BLOODRAVEN_ADDRESS")

	leaseTimeout := 20 * time.Second
	if v := os.Getenv("LEASE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse LEASE_TIMEOUT: %w", err)
		}
		leaseTimeout = d
	}

	peerCheckInterval := 5 * time.Second
	if v := os.Getenv("PEER_CHECK_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("parse PEER_CHECK_INTERVAL: %w", err)
		}
		peerCheckInterval = d
	}

	mySite := os.Getenv("MY_SITE")
	podNamespace := os.Getenv("POD_NAMESPACE")
	failoverGroup := os.Getenv("FAILOVER_GROUP")

	return &Config{
		MysqlDSN:          dsn,
		PodName:           podName,
		ListenAddr:        listenAddr,
		PeerAddress:       peerAddress,
		BloodravenAddress: bloodravenAddress,
		LeaseTimeout:      leaseTimeout,
		PeerCheckInterval: peerCheckInterval,
		MySite:            mySite,
		PodNamespace:      podNamespace,
		FailoverGroup:     failoverGroup,
	}, nil
}
