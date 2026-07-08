package sidecar

import (
	"testing"
	"time"
)

func TestConfigFromEnv_MYSQL_DSN(t *testing.T) {
	t.Setenv("MYSQL_DSN", "root:pass@tcp(127.0.0.1:3306)/")
	t.Setenv("MYSQL_USER", "")
	t.Setenv("MYSQL_PASSWORD", "")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MysqlDSN != "root:pass@tcp(127.0.0.1:3306)/" {
		t.Errorf("unexpected DSN: %s", cfg.MysqlDSN)
	}
}

func TestNormalizeFenceDurations(t *testing.T) {
	tests := []struct {
		name      string
		lease     time.Duration
		peer      time.Duration
		wantLease time.Duration
		wantPeer  time.Duration
	}{
		{"defaults unchanged", 20 * time.Second, 5 * time.Second, 20 * time.Second, 5 * time.Second},
		{"peer clamps to one second", 20 * time.Second, 0, 20 * time.Second, time.Second},
		{"negative peer clamps", 20 * time.Second, -time.Second, 20 * time.Second, time.Second},
		{"lease clamps to three seconds", 0, time.Second, 3 * time.Second, time.Second},
		{"lease clamps to ratio", 5 * time.Second, 4 * time.Second, 12 * time.Second, 4 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLease, gotPeer := normalizeFenceDurations(tt.lease, tt.peer)
			if gotLease != tt.wantLease || gotPeer != tt.wantPeer {
				t.Fatalf("normalizeFenceDurations() = (%s, %s), want (%s, %s)", gotLease, gotPeer, tt.wantLease, tt.wantPeer)
			}
		})
	}
}

func TestConfigFromEnv_MYSQL_USER_PASSWORD(t *testing.T) {
	t.Setenv("MYSQL_DSN", "")
	t.Setenv("MYSQL_USER", "operator")
	t.Setenv("MYSQL_PASSWORD", "secret")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MysqlDSN != "operator:secret@tcp(127.0.0.1:3306)/" {
		t.Errorf("unexpected DSN: %s", cfg.MysqlDSN)
	}
}

func TestConfigFromEnv_NeitherSet(t *testing.T) {
	t.Setenv("MYSQL_DSN", "")
	t.Setenv("MYSQL_USER", "")
	t.Setenv("MYSQL_PASSWORD", "")

	_, err := ConfigFromEnv()
	if err == nil {
		t.Fatal("expected error when neither MYSQL_DSN nor MYSQL_USER is set")
	}
}
