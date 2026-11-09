package controller

import (
	"testing"
)

// R3-04: the topology manager resolves the replication account as the
// start check and the init script do. A Secret the start check refuses
// (one replication key without the other, or the operator account with
// another password) must not make bootstrap, clone or old-primary recovery
// present a password MySQL does not hold for the account: auto-bootstrap
// stays disabled instead.
func TestStartManager_ReplicationCredentials(t *testing.T) {
	tests := []struct {
		name               string
		extra              map[string]string
		wantUser, wantPass string // "" means auto-bootstrap disabled
	}{
		{name: "neither key: the operator replicates", wantUser: "op", wantPass: "pw"},
		{name: "dedicated account", extra: map[string]string{replicationUserKey: "replicator", replicationPasswordKey: "repl-pw"},
			wantUser: "replicator", wantPass: "repl-pw"},
		{name: "password only", extra: map[string]string{replicationPasswordKey: "repl-pw"}},
		{name: "user only", extra: map[string]string{replicationUserKey: "replicator"}},
		{name: "operator account with another password", extra: map[string]string{replicationUserKey: "op", replicationPasswordKey: "repl-pw"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newStartEventHarness(t)
			data := lifecycleSecretData()
			for k, v := range tt.extra {
				data[k] = []byte(v)
			}
			h.setSecretData(data)
			h.sync()
			mt := registeredManager(h.r, FailoverGroupNamespacedName(h.fg))
			if mt == nil || mt.tm == nil {
				t.Fatal("no manager started")
			}
			cfg := mt.tm.bootstrapCfg
			if cfg.ReplUser != tt.wantUser || cfg.ReplPassword != tt.wantPass {
				t.Fatalf("replication account = %q/%q, want %q/%q", cfg.ReplUser, cfg.ReplPassword, tt.wantUser, tt.wantPass)
			}
			if (mt.tm.bootstrap != nil) != (tt.wantUser != "") {
				t.Fatalf("bootstrap controller set = %v, want %v", mt.tm.bootstrap != nil, tt.wantUser != "")
			}
		})
	}
}
