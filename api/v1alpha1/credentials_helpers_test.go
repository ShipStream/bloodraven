package v1alpha1

import (
	"testing"
)

func TestUsesCredentials(t *testing.T) {
	t.Run("legacy mode", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{SecretName: "my-secret"}
		if spec.UsesCredentials() {
			t.Error("expected legacy mode")
		}
	})

	t.Run("credentials mode", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: &CredentialsSpec{OperatorSecret: "op"},
		}
		if !spec.UsesCredentials() {
			t.Error("expected credentials mode")
		}
	})
}

func TestEffectiveOperatorSecretName(t *testing.T) {
	t.Run("legacy", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{SecretName: "legacy-secret"}
		if got := spec.EffectiveOperatorSecretName(); got != "legacy-secret" {
			t.Errorf("got %q, want %q", got, "legacy-secret")
		}
	})

	t.Run("credentials", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: &CredentialsSpec{OperatorSecret: "op-secret"},
		}
		if got := spec.EffectiveOperatorSecretName(); got != "op-secret" {
			t.Errorf("got %q, want %q", got, "op-secret")
		}
	})
}

func TestEffectiveBackupSecretName(t *testing.T) {
	tests := []struct {
		name string
		spec MysqlFailoverGroupSpec
		want string
	}{
		{
			name: "legacy",
			spec: MysqlFailoverGroupSpec{SecretName: "legacy"},
			want: "legacy",
		},
		{
			name: "credentials with dedicated backup",
			spec: MysqlFailoverGroupSpec{
				Credentials: &CredentialsSpec{
					OperatorSecret: "op",
					BackupSecret:   "backup",
				},
			},
			want: "backup",
		},
		{
			name: "credentials without backup falls back to operator",
			spec: MysqlFailoverGroupSpec{
				Credentials: &CredentialsSpec{OperatorSecret: "op"},
			},
			want: "op",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.spec.EffectiveBackupSecretName(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAllReferencedSecretNames(t *testing.T) {
	t.Run("legacy", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{SecretName: "s"}
		names := spec.AllReferencedSecretNames()
		if len(names) != 1 || names[0] != "s" {
			t.Errorf("got %v", names)
		}
	})

	t.Run("credentials all set", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: &CredentialsSpec{
				OperatorSecret: "op",
				AppSecret:      "app",
				ReadOnlySecret: "ro",
				MonitorSecret:  "mon",
				BackupSecret:   "bak",
			},
		}
		names := spec.AllReferencedSecretNames()
		if len(names) != 5 {
			t.Errorf("expected 5 names, got %d: %v", len(names), names)
		}
	})

	t.Run("deduplication", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: &CredentialsSpec{
				OperatorSecret: "shared",
				BackupSecret:   "shared",
			},
		}
		names := spec.AllReferencedSecretNames()
		if len(names) != 1 {
			t.Errorf("expected dedup to 1, got %d: %v", len(names), names)
		}
	})

	t.Run("credentials partial", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: &CredentialsSpec{
				OperatorSecret: "op",
				AppSecret:      "app",
			},
		}
		names := spec.AllReferencedSecretNames()
		if len(names) != 2 {
			t.Errorf("expected 2, got %d: %v", len(names), names)
		}
	})
}
