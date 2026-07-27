package v1alpha1

import (
	"testing"
	"time"
)

func encFG(spec *EncryptionAtRestSpec) *MysqlFailoverGroup {
	return &MysqlFailoverGroup{
		Spec: MysqlFailoverGroupSpec{
			Sites: []SiteSpec{{Name: "iad"}, {Name: "pdx"}},

			EncryptionAtRest: spec,
		},
	}
}

func TestEncryptionEnabled(t *testing.T) {
	tests := []struct {
		name string
		spec *EncryptionAtRestSpec
		want bool
	}{
		{"nil block", nil, false},
		{"present but off", &EncryptionAtRestSpec{Enabled: false}, false},
		{"on", &EncryptionAtRestSpec{Enabled: true}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := encFG(tt.spec).Spec.EncryptionEnabled(); got != tt.want {
				t.Errorf("EncryptionEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEffectiveKeyringDefaults(t *testing.T) {
	// Callers construct MysqlFailoverGroup objects in-memory all over
	// the codebase (tests, the standby-cluster template path), so the
	// defaults must not depend on having gone through admission.
	got := encFG(&EncryptionAtRestSpec{Enabled: true}).Spec.EffectiveKeyring()
	if got.DataFileDir != DefaultKeyringDataFileDir {
		t.Errorf("DataFileDir = %q, want %q", got.DataFileDir, DefaultKeyringDataFileDir)
	}
	if got.MysqldDir != DefaultKeyringMysqldDir {
		t.Errorf("MysqldDir = %q, want %q", got.MysqldDir, DefaultKeyringMysqldDir)
	}
	if got.PluginDir != DefaultKeyringPluginDir {
		t.Errorf("PluginDir = %q, want %q", got.PluginDir, DefaultKeyringPluginDir)
	}
	if got.RetainVersions != DefaultKeyringRetainVersions {
		t.Errorf("RetainVersions = %d, want %d", got.RetainVersions, DefaultKeyringRetainVersions)
	}
	if got.EscrowTimeoutSeconds != DefaultKeyringEscrowTimeoutSec {
		t.Errorf("EscrowTimeoutSeconds = %d, want %d", got.EscrowTimeoutSeconds, DefaultKeyringEscrowTimeoutSec)
	}
}

func TestEffectiveKeyringOverrides(t *testing.T) {
	fg := encFG(&EncryptionAtRestSpec{
		Enabled: true,
		Keyring: &KeyringSpec{
			DataFileDir:          "/run/keys",
			PluginDir:            "/usr/lib/mysql/plugin",
			RetainVersions:       9,
			EscrowTimeoutSeconds: 45,
		},
	})
	got := fg.Spec.EffectiveKeyring()
	if got.DataFileDir != "/run/keys" {
		t.Errorf("DataFileDir = %q", got.DataFileDir)
	}
	if got.PluginDir != "/usr/lib/mysql/plugin" {
		t.Errorf("PluginDir = %q", got.PluginDir)
	}
	// Unset field still defaults.
	if got.MysqldDir != DefaultKeyringMysqldDir {
		t.Errorf("MysqldDir = %q, want default", got.MysqldDir)
	}
	if got.RetainVersions != 9 {
		t.Errorf("RetainVersions = %d", got.RetainVersions)
	}
	if fg.Spec.EscrowTimeout() != 45*time.Second {
		t.Errorf("EscrowTimeout() = %v", fg.Spec.EscrowTimeout())
	}
	if want := "/run/keys/keyring"; fg.Spec.KeyringDataFilePath() != want {
		t.Errorf("KeyringDataFilePath() = %q, want %q", fg.Spec.KeyringDataFilePath(), want)
	}
}

func TestEffectiveEncryptionCoverage(t *testing.T) {
	off := false

	t.Run("disabled returns all-nil", func(t *testing.T) {
		got := encFG(nil).Spec.EffectiveEncryptionCoverage()
		if got.Tables != nil || got.BinaryLog != nil {
			t.Errorf("expected zero-value coverage when encryption is off, got %+v", got)
		}
	})

	t.Run("enabled defaults everything on", func(t *testing.T) {
		got := encFG(&EncryptionAtRestSpec{Enabled: true}).Spec.EffectiveEncryptionCoverage()
		for name, v := range map[string]*bool{
			"tables":           got.Tables,
			"privilegeCheck":   got.PrivilegeCheck,
			"redoLog":          got.RedoLog,
			"undoLog":          got.UndoLog,
			"binaryLog":        got.BinaryLog,
			"systemTablespace": got.SystemTablespace,
		} {
			if v == nil || !*v {
				t.Errorf("coverage %s = %v, want true", name, v)
			}
		}
	})

	t.Run("explicit false is honoured", func(t *testing.T) {
		got := encFG(&EncryptionAtRestSpec{
			Enabled:  true,
			Coverage: &EncryptionCoverageSpec{BinaryLog: &off},
		}).Spec.EffectiveEncryptionCoverage()
		if got.BinaryLog == nil || *got.BinaryLog {
			t.Errorf("binaryLog = %v, want explicit false", got.BinaryLog)
		}
		if got.Tables == nil || !*got.Tables {
			t.Error("unset tables should still default to true")
		}
	})
}

func TestSecretNaming(t *testing.T) {
	if got, want := KeyringSecretName("orders", "iad", 3), "mysql-orders-iad-keyring-v3"; got != want {
		t.Errorf("KeyringSecretName = %q, want %q", got, want)
	}
	if got, want := KeyringTokenSecretName("orders", "iad"), "mysql-orders-iad-keyring-token"; got != want {
		t.Errorf("KeyringTokenSecretName = %q, want %q", got, want)
	}
}

func TestEffectiveSitePhaseAndSealed(t *testing.T) {
	t.Run("encryption off", func(t *testing.T) {
		fg := encFG(nil)
		if got := fg.EffectiveSitePhase("iad"); got != "" {
			t.Errorf("phase = %q, want empty when encryption is off", got)
		}
		if fg.SiteKeyringSealed("iad") {
			t.Error("SiteKeyringSealed must be false when encryption is off")
		}
	})

	t.Run("no status defaults to Pending", func(t *testing.T) {
		fg := encFG(&EncryptionAtRestSpec{Enabled: true})
		if got := fg.EffectiveSitePhase("iad"); got != KeyringPhasePending {
			t.Errorf("phase = %q, want Pending", got)
		}
		if fg.SiteKeyringSealed("iad") {
			t.Error("a Pending site must not render sealed")
		}
	})

	// Escrowed must render sealed: flipping the rendering is what starts
	// the roll that then advances the phase to Sealed. If it did not,
	// the state machine would deadlock waiting for a roll it never asked
	// for.
	for _, tc := range []struct {
		phase      SiteKeyringPhase
		wantSealed bool
	}{
		{KeyringPhasePending, false},
		{KeyringPhaseUnsealed, false},
		{KeyringPhaseEscrowed, true},
		{KeyringPhaseSealed, true},
		{KeyringPhaseFailed, false},
	} {
		t.Run("phase "+string(tc.phase), func(t *testing.T) {
			fg := encFG(&EncryptionAtRestSpec{Enabled: true})
			fg.Status.EncryptionAtRest = &EncryptionAtRestStatus{
				Sites: []SiteEncryptionStatus{{Name: "iad", Phase: tc.phase}},
			}
			if got := fg.SiteKeyringSealed("iad"); got != tc.wantSealed {
				t.Errorf("SiteKeyringSealed(%s) = %v, want %v", tc.phase, got, tc.wantSealed)
			}
		})
	}

	t.Run("failed sealed site preserves rendering", func(t *testing.T) {
		fg := encFG(&EncryptionAtRestSpec{Enabled: true})
		fg.Status.EncryptionAtRest = &EncryptionAtRestStatus{Sites: []SiteEncryptionStatus{{
			Name: "iad", Phase: KeyringPhaseFailed, KeyringSecret: "mysql-orders-iad-keyring-v1",
		}}}
		if !fg.SiteKeyringSealed("iad") {
			t.Fatal("a steady-state escrow failure must not roll away the surviving keyring")
		}
		fg.Status.EncryptionAtRest.Sites[0].UnsealReason = UnsealReasonRotation
		if fg.SiteKeyringSealed("iad") {
			t.Fatal("an unsealed rotation failure must remain writable for retries")
		}
	})
}

func TestSiteEncryptionStatusByName(t *testing.T) {
	var nilStatus *EncryptionAtRestStatus
	if got := nilStatus.SiteEncryptionStatusByName("iad"); got != nil {
		t.Error("nil receiver must return nil, not panic")
	}
	st := &EncryptionAtRestStatus{Sites: []SiteEncryptionStatus{{Name: "pdx", KeyringVersion: 2}}}
	if got := st.SiteEncryptionStatusByName("pdx"); got == nil || got.KeyringVersion != 2 {
		t.Errorf("lookup failed: %+v", got)
	}
	if got := st.SiteEncryptionStatusByName("nope"); got != nil {
		t.Error("unknown site must return nil")
	}
}
