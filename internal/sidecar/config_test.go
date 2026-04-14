package sidecar

import (
	"testing"
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
