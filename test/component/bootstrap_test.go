package component

import (
	"context"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
)

func testBootstrapLogger() *testLogHelper {
	return &testLogHelper{}
}

type testLogHelper struct{}

func TestBootstrap_CloneFromPrimary(t *testing.T) {
	logger := newTestHarness(t).logger
	bc := controller.NewBootstrapController(logger)

	primary := &mockMySQL{readOnly: false}
	replica := &mockMySQL{readOnly: true}

	err := bc.BootstrapReplica(context.Background(), controller.BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:  "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		UseSSL:       true,
		CloneTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("BootstrapReplica failed: %v", err)
	}

	// Verify clone donor list was set.
	replica.mu.Lock()
	donor := replica.cloneDonorList
	cloned := replica.cloneInstanceCalled
	replica.mu.Unlock()

	if donor != "primary.example.com:3306" {
		t.Errorf("clone donor list: got %q, want %q", donor, "primary.example.com:3306")
	}
	if !cloned {
		t.Error("CloneInstance should have been called")
	}
}

func TestBootstrap_PrimaryReadOnly_Fails(t *testing.T) {
	logger := newTestHarness(t).logger
	bc := controller.NewBootstrapController(logger)

	primary := &mockMySQL{readOnly: true} // primary is read-only -- should fail
	replica := &mockMySQL{readOnly: true}

	err := bc.BootstrapReplica(context.Background(), controller.BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:  "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		CloneTimeout: 30 * time.Second,
	})
	if err == nil {
		t.Fatal("BootstrapReplica should fail when primary is read-only")
	}

	// Clone should NOT have been called.
	replica.mu.Lock()
	cloned := replica.cloneInstanceCalled
	replica.mu.Unlock()

	if cloned {
		t.Error("CloneInstance should not be called when primary is read-only")
	}
}

func TestBootstrap_SetupReplication(t *testing.T) {
	logger := newTestHarness(t).logger
	bc := controller.NewBootstrapController(logger)

	replica := &mockMySQL{readOnly: false}

	err := bc.SetupReplication(context.Background(), replica, controller.ReplicationSetupOpts{
		SourceHost:   "primary.example.com",
		ReplUser:     "repl",
		ReplPassword: "secret",
		UseSSL:       true,
	})
	if err != nil {
		t.Fatalf("SetupReplication failed: %v", err)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	// Verify super_read_only was set.
	if !replica.superReadOnly {
		t.Error("super_read_only should be set on replica")
	}

	// Verify replication source was configured.
	if !replica.replicationSourceSet {
		t.Error("ChangeReplicationSource should have been called")
	}
	if replica.changeReplicationOpts.Host != "primary.example.com" {
		t.Errorf("replication source host: got %q, want %q",
			replica.changeReplicationOpts.Host, "primary.example.com")
	}
	if replica.changeReplicationOpts.User != "repl" {
		t.Errorf("replication source user: got %q, want %q",
			replica.changeReplicationOpts.User, "repl")
	}
	if replica.changeReplicationOpts.Password != "secret" {
		t.Errorf("replication source password: got %q, want %q",
			replica.changeReplicationOpts.Password, "secret")
	}
	if !replica.changeReplicationOpts.UseSSL {
		t.Error("replication source should use SSL")
	}

	// Verify replica was started.
	if !replica.replicaStarted {
		t.Error("StartReplica should have been called")
	}
}

func TestBootstrap_SetupReplication_NoSSL(t *testing.T) {
	logger := newTestHarness(t).logger
	bc := controller.NewBootstrapController(logger)

	replica := &mockMySQL{readOnly: false}

	err := bc.SetupReplication(context.Background(), replica, controller.ReplicationSetupOpts{
		SourceHost:   "primary.example.com",
		ReplUser:     "repl",
		ReplPassword: "secret",
		UseSSL:       false,
	})
	if err != nil {
		t.Fatalf("SetupReplication failed: %v", err)
	}

	replica.mu.Lock()
	defer replica.mu.Unlock()

	if replica.changeReplicationOpts.UseSSL {
		t.Error("replication source should not use SSL when UseSSL=false")
	}
}

// Verify that mockMySQL satisfies the mysql.Checker interface at compile time.
var _ mysql.Checker = (*mockMySQL)(nil)
