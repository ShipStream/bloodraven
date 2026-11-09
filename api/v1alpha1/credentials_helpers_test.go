package v1alpha1

import (
	"testing"
)

func TestEffectiveBackupSecretName(t *testing.T) {
	tests := []struct {
		name string
		spec MysqlFailoverGroupSpec
		want string
	}{
		{
			name: "dedicated backup secret",
			spec: MysqlFailoverGroupSpec{
				Credentials: CredentialsSpec{
					OperatorSecretName: "op",
					BackupSecretName:   "backup",
				},
			},
			want: "backup",
		},
		{
			name: "without backup secret falls back to operator",
			spec: MysqlFailoverGroupSpec{
				Credentials: CredentialsSpec{OperatorSecretName: "op"},
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
	t.Run("all set", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: CredentialsSpec{
				OperatorSecretName: "op",
				AppSecretName:      "app",
				ReadOnlySecretName: "ro",
				MonitorSecretName:  "mon",
				BackupSecretName:   "bak",
			},
		}
		names := spec.AllReferencedSecretNames()
		if len(names) != 5 {
			t.Errorf("expected 5 names, got %d: %v", len(names), names)
		}
	})

	t.Run("deduplication", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: CredentialsSpec{
				OperatorSecretName: "shared",
				BackupSecretName:   "shared",
			},
		}
		names := spec.AllReferencedSecretNames()
		if len(names) != 1 {
			t.Errorf("expected dedup to 1, got %d: %v", len(names), names)
		}
	})

	t.Run("partial", func(t *testing.T) {
		spec := MysqlFailoverGroupSpec{
			Credentials: CredentialsSpec{
				OperatorSecretName: "op",
				AppSecretName:      "app",
			},
		}
		names := spec.AllReferencedSecretNames()
		if len(names) != 2 {
			t.Errorf("expected 2, got %d: %v", len(names), names)
		}
	})
}
