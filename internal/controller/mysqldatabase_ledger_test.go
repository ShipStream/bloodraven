package controller

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// ledgerTestUser builds the tenantUserInput shape the ledger helpers consume:
// only the entry's secretName and the resolved username matter to them.
func ledgerTestUser(secretName, username string) tenantUserInput {
	return tenantUserInput{
		entry:    v1alpha1.MysqlDatabaseUser{SecretName: secretName},
		username: username,
		hosts:    []string{"%"},
	}
}

func ledgerState(secretName, username, pending string) v1alpha1.MysqlDatabaseUserState {
	return v1alpha1.MysqlDatabaseUserState{
		SecretName:      secretName,
		Username:        username,
		PendingUsername: pending,
		Hosts:           []string{"%"},
	}
}

// TestStampUsersWriteAhead pins the write-ahead state machine: an entry must
// be attributed by the persisted ledger before its SQL runs, and a rotation
// must record the target without losing the account still live in MySQL.
func TestStampUsersWriteAhead(t *testing.T) {
	tests := []struct {
		name  string
		start []v1alpha1.MysqlDatabaseUserState
		users []tenantUserInput
		want  []v1alpha1.MysqlDatabaseUserState
	}{
		{
			name:  "first apply appends the entry",
			users: []tenantUserInput{ledgerTestUser("support", "acme_support")},
			want:  []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
		},
		{
			name:  "no users leaves the ledger untouched",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			want:  []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
		},
		{
			name:  "re-apply of the recorded account is a no-op",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			users: []tenantUserInput{ledgerTestUser("support", "acme_support")},
			want:  []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
		},
		{
			name:  "empty username on an existing entry is backfilled",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "", "")},
			users: []tenantUserInput{ledgerTestUser("support", "acme_support")},
			want:  []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
		},
		{
			name:  "rotation records the target as pending",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			users: []tenantUserInput{ledgerTestUser("support", "acme_support_v2")},
			want:  []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "acme_support_v2")},
		},
		{
			name:  "rotation retried keeps the same pending record",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "acme_support_v2")},
			users: []tenantUserInput{ledgerTestUser("support", "acme_support_v2")},
			want:  []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "acme_support_v2")},
		},
		{
			name:  "the Secret changing again mid-rotation re-points pending",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "acme_support_v2")},
			users: []tenantUserInput{ledgerTestUser("support", "acme_support_v3")},
			want:  []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "acme_support_v3")},
		},
		{
			name:  "hosts accumulate until Ready",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			users: []tenantUserInput{{entry: v1alpha1.MysqlDatabaseUser{SecretName: "support"}, username: "acme_support", hosts: []string{"10.0.0.1"}}},
			want: []v1alpha1.MysqlDatabaseUserState{{
				SecretName: "support", Username: "acme_support", Hosts: []string{"%", "10.0.0.1"},
			}},
		},
		{
			name:  "a pre-hosts record unions with the default",
			start: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "acme_support"}},
			users: []tenantUserInput{{entry: v1alpha1.MysqlDatabaseUser{SecretName: "support"}, username: "acme_support", hosts: []string{"10.0.0.1"}}},
			want: []v1alpha1.MysqlDatabaseUserState{{
				SecretName: "support", Username: "acme_support", Hosts: []string{"%", "10.0.0.1"},
			}},
		},
		{
			name:  "a new entry is appended alongside an existing one",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			users: []tenantUserInput{ledgerTestUser("support", "acme_support"), ledgerTestUser("bi", "acme_bi")},
			want: []v1alpha1.MysqlDatabaseUserState{
				ledgerState("support", "acme_support", ""),
				ledgerState("bi", "acme_bi", ""),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := &v1alpha1.MysqlDatabaseStatus{AppliedUsers: copyAppliedUsers(tc.start)}
			stampUsersWriteAhead(st, tc.users)
			if !reflect.DeepEqual(st.AppliedUsers, tc.want) {
				t.Fatalf("appliedUsers = %+v, want %+v", st.AppliedUsers, tc.want)
			}
		})
	}
}

// TestRollbackUserWriteAhead: a PreExistingUser refusal ran no SQL for that
// entry, so the ledger must return to exactly its pre-stamp shape — or a
// later deletionPolicy: Delete would drop an account this CR refused to adopt.
func TestRollbackUserWriteAhead(t *testing.T) {
	tests := []struct {
		name    string
		prior   []v1alpha1.MysqlDatabaseUserState
		stamped []v1alpha1.MysqlDatabaseUserState
		secret  string
		want    []v1alpha1.MysqlDatabaseUserState
	}{
		{
			name:    "first apply refusal removes the appended entry",
			stamped: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			secret:  "support",
			want:    []v1alpha1.MysqlDatabaseUserState{},
		},
		{
			name:    "rotation refusal clears only the pending target",
			prior:   []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			stamped: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "acme_support_v2")},
			secret:  "support",
			want:    []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
		},
		{
			name:  "sibling entries are never touched",
			prior: []v1alpha1.MysqlDatabaseUserState{ledgerState("bi", "acme_bi", "")},
			stamped: []v1alpha1.MysqlDatabaseUserState{
				ledgerState("bi", "acme_bi", ""),
				ledgerState("support", "acme_support", ""),
			},
			secret: "support",
			want:   []v1alpha1.MysqlDatabaseUserState{ledgerState("bi", "acme_bi", "")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := &v1alpha1.MysqlDatabaseStatus{AppliedUsers: copyAppliedUsers(tc.stamped)}
			rollbackUserWriteAhead(st, priorApplyState{appliedUsers: copyAppliedUsers(tc.prior)}, tc.secret)
			if len(st.AppliedUsers) != len(tc.want) {
				t.Fatalf("appliedUsers = %+v, want %+v", st.AppliedUsers, tc.want)
			}
			for i := range tc.want {
				if !reflect.DeepEqual(st.AppliedUsers[i], tc.want[i]) {
					t.Fatalf("appliedUsers = %+v, want %+v", st.AppliedUsers, tc.want)
				}
			}
		})
	}
}

// TestUserAttributed pins the adoption gate's memory: an account is adoptable
// only when the pre-stamp ledger already names it — under any entry, because
// the ledger records what this CR created and a secretName rename must
// transfer the record rather than wedge on PreExistingUser.
func TestUserAttributed(t *testing.T) {
	prior := priorApplyState{appliedUsers: []v1alpha1.MysqlDatabaseUserState{
		ledgerState("support", "acme_support", "acme_support_v2"),
		ledgerState("bi", "acme_bi", ""),
	}}
	tests := []struct {
		name     string
		secret   string
		username string
		want     bool
	}{
		{name: "recorded username", secret: "support", username: "acme_support", want: true},
		{name: "in-flight rotation target", secret: "support", username: "acme_support_v2", want: true},
		{name: "unknown account", secret: "support", username: "someone_else", want: false},
		{name: "another entry's recorded account transfers", secret: "bi", username: "acme_support", want: true},
		{name: "another entry's pending rotation target transfers", secret: "bi", username: "acme_support_v2", want: true},
		{name: "a secretName the ledger never saw still transfers by username", secret: "support-renamed", username: "acme_support", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := userAttributed(prior, tc.secret, tc.username); got != tc.want {
				t.Fatalf("userAttributed(%q, %q) = %v, want %v", tc.secret, tc.username, got, tc.want)
			}
		})
	}
}

// TestLedgerUsernames: the drop paths depend on this returning every account
// a ledger entry may have created, including an abandoned rotation target.
func TestLedgerUsernames(t *testing.T) {
	tests := []struct {
		name  string
		state v1alpha1.MysqlDatabaseUserState
		want  []string
	}{
		{name: "settled entry", state: ledgerState("support", "acme_support", ""), want: []string{"acme_support"}},
		{
			name:  "rotation in flight yields both",
			state: ledgerState("support", "acme_support", "acme_support_v2"),
			want:  []string{"acme_support", "acme_support_v2"},
		},
		{
			name:  "pending equal to current is not duplicated",
			state: ledgerState("support", "acme_support", "acme_support"),
			want:  []string{"acme_support"},
		},
		{
			name:  "pending-only record still names its account",
			state: ledgerState("support", "", "acme_support_v2"),
			want:  []string{"acme_support_v2"},
		},
		{name: "empty record names nothing", state: ledgerState("support", "", ""), want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ledgerUsernames(tc.state); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ledgerUsernames() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReadyAppliedUsers: the Ready ledger is exactly the current spec, with
// pendings resolved — nothing else may survive a successful apply.
func TestReadyAppliedUsers(t *testing.T) {
	if got := readyAppliedUsers(nil); got != nil {
		t.Fatalf("readyAppliedUsers(nil) = %+v, want nil", got)
	}
	got := readyAppliedUsers([]tenantUserInput{
		ledgerTestUser("support", "acme_support_v2"),
		ledgerTestUser("bi", "acme_bi"),
	})
	want := []v1alpha1.MysqlDatabaseUserState{
		ledgerState("support", "acme_support_v2", ""),
		ledgerState("bi", "acme_bi", ""),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("readyAppliedUsers() = %+v, want %+v", got, want)
	}
}

// TestSiblingPrincipalClaimIn covers the union-of-claims guard every users[]
// drop path shares.
func TestSiblingPrincipalClaimIn(t *testing.T) {
	sibling := func(mutate func(*v1alpha1.MysqlDatabase)) v1alpha1.MysqlDatabase {
		m := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
			m.Name = "tenant-sibling"
			m.Spec.DatabaseName = "sibling_wms"
			m.Spec.Owner.SecretName = "sibling-owner"
		})
		mutate(m)
		return *m
	}
	tests := []struct {
		name    string
		item    v1alpha1.MysqlDatabase
		want    bool
		wantHow string
	}{
		{
			name:    "recorded owner",
			item:    sibling(func(m *v1alpha1.MysqlDatabase) { m.Status.OwnerUser = "acme_support" }),
			want:    true,
			wantHow: "owner",
		},
		{
			name:    "pending owner",
			item:    sibling(func(m *v1alpha1.MysqlDatabase) { m.Status.PendingOwnerUser = "acme_support" }),
			want:    true,
			wantHow: "owner",
		},
		{
			name: "spec.grants entry",
			item: sibling(func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{
					Username:   "acme_support",
					Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
				}}
			}),
			want:    true,
			wantHow: "spec.grants[]",
		},
		{
			name: "users ledger record",
			item: sibling(func(m *v1alpha1.MysqlDatabase) {
				m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{ledgerState("s-support", "acme_support", "")}
			}),
			want:    true,
			wantHow: "status.appliedUsers",
		},
		{
			name: "users ledger pending record",
			item: sibling(func(m *v1alpha1.MysqlDatabase) {
				m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{ledgerState("s-support", "other", "acme_support")}
			}),
			want:    true,
			wantHow: "status.appliedUsers",
		},
		{
			name: "a sibling on another group does not claim it",
			item: sibling(func(m *v1alpha1.MysqlDatabase) {
				m.Spec.GroupRef.Name = "other-group"
				m.Status.OwnerUser = "acme_support"
			}),
			want: false,
		},
		{
			name: "a terminating sibling does not claim it",
			item: sibling(func(m *v1alpha1.MysqlDatabase) {
				now := metav1.Now()
				m.DeletionTimestamp = &now
				m.Status.OwnerUser = "acme_support"
			}),
			want: false,
		},
		{
			name: "an unrelated sibling does not claim it",
			item: sibling(func(m *v1alpha1.MysqlDatabase) { m.Status.OwnerUser = "someone_else" }),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			list := &v1alpha1.MysqlDatabaseList{Items: []v1alpha1.MysqlDatabase{tc.item}}
			referrer, how, claimed := siblingPrincipalClaimIn(list, "tenant-acme", "main", "acme_support")
			if claimed != tc.want {
				t.Fatalf("claimed = %v (referrer %q, how %q), want %v", claimed, referrer, how, tc.want)
			}
			if claimed && how != tc.wantHow {
				t.Fatalf("how = %q, want %q", how, tc.wantHow)
			}
		})
	}

	t.Run("self is never a claimant", func(t *testing.T) {
		self := mdbTestCR(func(m *v1alpha1.MysqlDatabase) { m.Status.OwnerUser = "acme_support" })
		list := &v1alpha1.MysqlDatabaseList{Items: []v1alpha1.MysqlDatabase{*self}}
		if _, _, claimed := siblingPrincipalClaimIn(list, self.Name, "main", "acme_support"); claimed {
			t.Fatal("a CR claimed its own principal")
		}
	})
}
