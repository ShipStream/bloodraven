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
// be attributed by the persisted ledger before its SQL runs, a rotation must
// record the target without losing the account still live in MySQL, and each
// recorded name carries its own hosts.
func TestStampUsersWriteAhead(t *testing.T) {
	withHosts := func(s v1alpha1.MysqlDatabaseUserState, hosts, pendingHosts []string) v1alpha1.MysqlDatabaseUserState {
		s.Hosts, s.PendingHosts = hosts, pendingHosts
		return s
	}
	userOn := func(secretName, username string, hosts ...string) tenantUserInput {
		return tenantUserInput{entry: v1alpha1.MysqlDatabaseUser{SecretName: secretName}, username: username, hosts: hosts}
	}
	tests := []struct {
		name    string
		start   []v1alpha1.MysqlDatabaseUserState
		users   []tenantUserInput
		carried map[string][]string
		want    []v1alpha1.MysqlDatabaseUserState
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
			name:  "rotation records the target as pending with its own hosts",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			users: []tenantUserInput{userOn("support", "acme_support_v2", "10.0.0.2")},
			want: []v1alpha1.MysqlDatabaseUserState{
				withHosts(ledgerState("support", "acme_support", "acme_support_v2"), []string{"%"}, []string{"10.0.0.2"}),
			},
		},
		{
			name: "rotation retried unions the same target's hosts",
			start: []v1alpha1.MysqlDatabaseUserState{
				withHosts(ledgerState("support", "acme_support", "acme_support_v2"), []string{"%"}, []string{"10.0.0.2"}),
			},
			users: []tenantUserInput{userOn("support", "acme_support_v2", "10.0.0.3")},
			want: []v1alpha1.MysqlDatabaseUserState{
				withHosts(ledgerState("support", "acme_support", "acme_support_v2"), []string{"%"}, []string{"10.0.0.2", "10.0.0.3"}),
			},
		},
		{
			name: "the Secret changing again mid-rotation replaces pending and its hosts",
			start: []v1alpha1.MysqlDatabaseUserState{
				withHosts(ledgerState("support", "acme_support", "acme_support_v2"), []string{"%"}, []string{"10.0.0.2"}),
			},
			users: []tenantUserInput{userOn("support", "acme_support_v3", "10.0.0.3")},
			want: []v1alpha1.MysqlDatabaseUserState{
				withHosts(ledgerState("support", "acme_support", "acme_support_v3"), []string{"%"}, []string{"10.0.0.3"}),
			},
		},
		{
			name:  "a legacy pending record without pendingHosts materializes the shared hosts",
			start: []v1alpha1.MysqlDatabaseUserState{withHosts(ledgerState("support", "acme_support", "acme_support_v2"), []string{"10.0.0.1"}, nil)},
			users: []tenantUserInput{userOn("support", "acme_support_v2", "10.0.0.2")},
			want: []v1alpha1.MysqlDatabaseUserState{
				withHosts(ledgerState("support", "acme_support", "acme_support_v2"), []string{"10.0.0.1"}, []string{"10.0.0.1", "10.0.0.2"}),
			},
		},
		{
			name:  "hosts accumulate until Ready",
			start: []v1alpha1.MysqlDatabaseUserState{ledgerState("support", "acme_support", "")},
			users: []tenantUserInput{userOn("support", "acme_support", "10.0.0.1")},
			want:  []v1alpha1.MysqlDatabaseUserState{withHosts(ledgerState("support", "acme_support", ""), []string{"%", "10.0.0.1"}, nil)},
		},
		{
			name:  "a pre-hosts record unions with the default",
			start: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "acme_support"}},
			users: []tenantUserInput{userOn("support", "acme_support", "10.0.0.1")},
			want:  []v1alpha1.MysqlDatabaseUserState{withHosts(ledgerState("support", "acme_support", ""), []string{"%", "10.0.0.1"}, nil)},
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
		{
			name:    "hosts carried from an overwritten record stay recorded under the receiving entry",
			users:   []tenantUserInput{userOn("bi", "acme_app_v2", "10.0.0.5")},
			carried: map[string][]string{"acme_app_v2": {"10.0.0.2"}},
			want:    []v1alpha1.MysqlDatabaseUserState{withHosts(ledgerState("bi", "acme_app_v2", ""), []string{"10.0.0.5", "10.0.0.2"}, nil)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := &v1alpha1.MysqlDatabaseStatus{AppliedUsers: cloneLedger(tc.start)}
			stampUsersWriteAhead(st, tc.users, tc.carried)
			if !reflect.DeepEqual(st.AppliedUsers, tc.want) {
				t.Fatalf("appliedUsers = %+v, want %+v", st.AppliedUsers, tc.want)
			}
		})
	}
}

func cloneLedger(states []v1alpha1.MysqlDatabaseUserState) []v1alpha1.MysqlDatabaseUserState {
	if states == nil {
		return nil
	}
	out := make([]v1alpha1.MysqlDatabaseUserState, len(states))
	for i := range states {
		states[i].DeepCopyInto(&out[i])
	}
	return out
}

// TestStampOwnerWriteAhead pins the owner half of the write-ahead, including
// the pending-hosts invariant: pendingOwnerHosts is replaced — never unioned —
// when the rotation target changes, so a host checked only for the previous
// target is never recorded for the new one.
func TestStampOwnerWriteAhead(t *testing.T) {
	tests := []struct {
		name    string
		start   v1alpha1.MysqlDatabaseStatus
		user    string
		hosts   []string
		carried map[string][]string
		want    v1alpha1.MysqlDatabaseStatus
	}{
		{
			name:  "first apply records the owner on its hosts",
			user:  "a",
			hosts: []string{"h1"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}},
		},
		{
			name:  "host addition unions onto the recorded owner",
			start: v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}},
			user:  "a",
			hosts: []string{"h2"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1", "h2"}},
		},
		{
			name:  "a pre-hosts owner keeps its implied '%'",
			start: v1alpha1.MysqlDatabaseStatus{OwnerUser: "a"},
			user:  "a",
			hosts: []string{"h1"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"%", "h1"}},
		},
		{
			name:  "rotation records the target with only its own hosts",
			start: v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}},
			user:  "b",
			hosts: []string{"h2"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h2"}},
		},
		{
			name:  "retrying the same target unions its hosts",
			start: v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h2"}},
			user:  "b",
			hosts: []string{"h3"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h2", "h3"}},
		},
		{
			name:  "a new target replaces pending hosts",
			start: v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h2"}},
			user:  "c",
			hosts: []string{"h3"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "c", PendingOwnerHosts: []string{"h3"}},
		},
		{
			name:  "leftover pending hosts without a pending name are not inherited",
			start: v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerHosts: []string{"h2"}},
			user:  "c",
			hosts: []string{"h3"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "c", PendingOwnerHosts: []string{"h3"}},
		},
		{
			name:  "a legacy pending target materializes the shared hosts",
			start: v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1", "h2"}, PendingOwnerUser: "b"},
			user:  "b",
			hosts: []string{"h2"},
			want:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1", "h2"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h1", "h2"}},
		},
		{
			name:    "carried hosts are recorded for the receiving owner",
			start:   v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}},
			user:    "b",
			hosts:   []string{"h3"},
			carried: map[string][]string{"b": {"h2"}},
			want:    v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h3", "h2"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := tc.start.DeepCopy()
			stampOwnerWriteAhead(st, tc.user, tc.hosts, tc.carried)
			if !reflect.DeepEqual(*st, tc.want) {
				t.Fatalf("status = %+v, want %+v", *st, tc.want)
			}
		})
	}
}

// TestReplacedPendingHosts: only a pending record the stamp will overwrite is
// carried, and only with that name's own hosts.
func TestReplacedPendingHosts(t *testing.T) {
	st := &v1alpha1.MysqlDatabaseStatus{
		OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h2"},
		AppliedUsers: []v1alpha1.MysqlDatabaseUserState{
			{SecretName: "s1", Username: "u1", Hosts: []string{"h1"}, PendingUsername: "u2", PendingHosts: []string{"h4"}},
			{SecretName: "s2", Username: "v1", Hosts: []string{"h1"}, PendingUsername: "v2", PendingHosts: []string{"h5"}},
		},
	}
	users := []tenantUserInput{
		{entry: v1alpha1.MysqlDatabaseUser{SecretName: "s1"}, username: "u3"}, // overwrites u2
		{entry: v1alpha1.MysqlDatabaseUser{SecretName: "s2"}, username: "v2"}, // retries v2
	}
	got := replacedPendingHosts(st, "c", users)
	want := map[string][]string{"b": {"h2"}, "u2": {"h4"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replacedPendingHosts = %v, want %v", got, want)
	}
	if got := replacedPendingHosts(st, "b", nil); len(got) != 0 {
		t.Fatalf("retrying the owner target carried %v, want nothing", got)
	}
}

// TestWithdrawUnexecuted: records of principals none of whose statements ran
// are narrowed to what the pre-reconcile snapshot already attributed — so a
// user@host that was only verified absent never stays on record — while
// principals that executed keep their stamp.
func TestWithdrawUnexecuted(t *testing.T) {
	support := func(username string) tenantUserInput {
		return tenantUserInput{entry: v1alpha1.MysqlDatabaseUser{SecretName: "support"}, username: username}
	}
	tests := []struct {
		name     string
		prior    v1alpha1.MysqlDatabaseStatus
		stamped  v1alpha1.MysqlDatabaseStatus
		progress applyProgress
		verified preflightResult
		users    []tenantUserInput
		want     v1alpha1.MysqlDatabaseStatus
	}{
		{
			name:    "nothing executed on a first apply withdraws everything",
			stamped: v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}, AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
			users:   []tenantUserInput{support("s")},
			want:    v1alpha1.MysqlDatabaseStatus{AppliedUsers: []v1alpha1.MysqlDatabaseUserState{}},
		},
		{
			name:     "schema executed, owner refused: schema stays, owner and users withdrawn",
			stamped:  v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}, AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
			progress: applyProgress{schema: true},
			users:    []tenantUserInput{support("s")},
			want:     v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, AppliedUsers: []v1alpha1.MysqlDatabaseUserState{}},
		},
		{
			name:     "a rotation whose owner SQL never executed withdraws the pending target",
			prior:    v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
			stamped:  v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1", "h2"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h2"}},
			progress: applyProgress{schema: true},
			want:     v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
		},
		{
			name:     "an executed owner keeps its stamp",
			stamped:  v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
			progress: applyProgress{schema: true, owner: true},
			want:     v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
		},
		{
			name:     "an unexecuted users rotation keeps the prior entry and drops the target",
			prior:    v1alpha1.MysqlDatabaseStatus{AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
			stamped:  v1alpha1.MysqlDatabaseStatus{AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}, PendingUsername: "t", PendingHosts: []string{"h1"}}}},
			progress: applyProgress{schema: true, owner: true},
			users:    []tenantUserInput{support("t")},
			want:     v1alpha1.MysqlDatabaseStatus{AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
		},
		{
			name:     "an executed users entry keeps its stamp",
			stamped:  v1alpha1.MysqlDatabaseStatus{AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
			progress: applyProgress{schema: true, owner: true, users: map[string]bool{"support": true}},
			users:    []tenantUserInput{support("s")},
			want:     v1alpha1.MysqlDatabaseStatus{AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
		},
		{
			name: "a record carried from another surface stays attributed",
			// The owner's pending b@h2 was overwritten by the rotation to c
			// and carried to the users entry, whose SQL never ran.
			prior:    v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h2"}},
			stamped:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "c", PendingOwnerHosts: []string{"h3"}, AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "b", Hosts: []string{"h5", "h2"}}}},
			progress: applyProgress{schema: true, owner: true},
			users:    []tenantUserInput{support("b")},
			want:     v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "c", PendingOwnerHosts: []string{"h3"}, AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "b", Hosts: []string{"h2"}}}},
		},
		{
			// A previous reconcile stamped these records, then its
			// withdrawal patch failed: they attribute themselves, but this
			// preflight saw the accounts absent, so they still go.
			name:     "a prior-attributed account verified absent is withdrawn",
			prior:    v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}, AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
			stamped:  v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}, AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: "support", Username: "s", Hosts: []string{"h1"}}}},
			verified: preflightResult{absent: map[mysqlAccount]bool{{user: "a", host: "h1"}: true, {user: "s", host: "h1"}: true}},
			users:    []tenantUserInput{support("s")},
			want:     v1alpha1.MysqlDatabaseStatus{AppliedUsers: []v1alpha1.MysqlDatabaseUserState{}},
		},
		{
			name:     "a prior-attributed account that exists is kept",
			prior:    v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
			stamped:  v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1", "h2"}},
			verified: preflightResult{dbExists: true, absent: map[mysqlAccount]bool{{user: "a", host: "h2"}: true}},
			want:     v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
		},
		{
			name:     "the schema record stays while an account record remains",
			prior:    v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
			stamped:  v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
			verified: preflightResult{},
			want:     v1alpha1.MysqlDatabaseStatus{DatabaseCreated: true, OwnerUser: "a", OwnerHosts: []string{"h1"}},
		},
		{
			name:     "a legacy pending target's hosts are pinned before the shared list narrows",
			prior:    v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b"},
			stamped:  v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1", "h2"}, PendingOwnerUser: "b"},
			verified: preflightResult{dbExists: true},
			progress: applyProgress{schema: true},
			want:     v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h1"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := tc.stamped.DeepCopy()
			withdrawUnexecuted(st, &tc.prior, tc.progress, tc.verified, tc.users)
			if !reflect.DeepEqual(*st, tc.want) {
				t.Fatalf("status = %+v, want %+v", *st, tc.want)
			}
		})
	}
}

// TestOwnAccountAttributed pins the adoption memory: per user@host, and
// CR-wide across the owner, the owner rotation target and every ledger
// record, each with its own hosts.
func TestOwnAccountAttributed(t *testing.T) {
	prior := &v1alpha1.MysqlDatabaseStatus{
		OwnerUser: "owner", OwnerHosts: []string{"h1"},
		PendingOwnerUser: "owner_v2", PendingOwnerHosts: []string{"h2"},
		AppliedUsers: []v1alpha1.MysqlDatabaseUserState{
			{SecretName: "support", Username: "acme_support", Hosts: []string{"h1"}, PendingUsername: "acme_support_v2", PendingHosts: []string{"h3"}},
			{SecretName: "legacy", Username: "acme_bi", PendingUsername: "acme_bi_v2"},
		},
	}
	tests := []struct {
		name, username, host string
		want                 bool
	}{
		{name: "recorded owner on its host", username: "owner", host: "h1", want: true},
		{name: "recorded owner on another host", username: "owner", host: "h2", want: false},
		{name: "owner rotation target on its host", username: "owner_v2", host: "h2", want: true},
		{name: "owner rotation target on the owner's host", username: "owner_v2", host: "h1", want: false},
		{name: "ledger username", username: "acme_support", host: "h1", want: true},
		{name: "ledger username on the pending name's host", username: "acme_support", host: "h3", want: false},
		{name: "ledger rotation target", username: "acme_support_v2", host: "h3", want: true},
		{name: "ledger rotation target on the username's host", username: "acme_support_v2", host: "h1", want: false},
		{name: "legacy entry defaults to '%'", username: "acme_bi", host: "%", want: true},
		{name: "legacy pending falls back to the shared hosts", username: "acme_bi_v2", host: "%", want: true},
		{name: "unknown account", username: "someone_else", host: "h1", want: false},
		{name: "empty username", username: "", host: "%", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ownAccountAttributed(prior, tc.username, tc.host); got != tc.want {
				t.Fatalf("ownAccountAttributed(%q, %q) = %v, want %v", tc.username, tc.host, got, tc.want)
			}
		})
	}
}

// TestMaterializeLegacyPendingHosts: a legacy pending name gets its borrowed
// hosts pinned; a settled record and a pending record with hosts are left
// alone.
func TestMaterializeLegacyPendingHosts(t *testing.T) {
	st := &v1alpha1.MysqlDatabaseStatus{
		OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b",
		AppliedUsers: []v1alpha1.MysqlDatabaseUserState{
			{SecretName: "legacy", Username: "u", PendingUsername: "v"},
			{SecretName: "new", Username: "x", Hosts: []string{"h1"}, PendingUsername: "y", PendingHosts: []string{"h2"}},
			{SecretName: "settled", Username: "z", Hosts: []string{"h3"}},
		},
	}
	materializeLegacyPendingHosts(st)
	want := &v1alpha1.MysqlDatabaseStatus{
		OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b", PendingOwnerHosts: []string{"h1"},
		AppliedUsers: []v1alpha1.MysqlDatabaseUserState{
			{SecretName: "legacy", Username: "u", PendingUsername: "v", PendingHosts: []string{"%"}},
			{SecretName: "new", Username: "x", Hosts: []string{"h1"}, PendingUsername: "y", PendingHosts: []string{"h2"}},
			{SecretName: "settled", Username: "z", Hosts: []string{"h3"}},
		},
	}
	if !reflect.DeepEqual(st, want) {
		t.Fatalf("status = %+v, want %+v", *st, *want)
	}
	// Growing the shared list afterwards no longer moves the pending name.
	stampOwnerWriteAhead(st, "a", []string{"h2"}, nil)
	if got := pendingOwnerHosts(st); !reflect.DeepEqual(got, []string{"h1"}) {
		t.Fatalf("pendingOwnerHosts after growing ownerHosts = %v, want [h1]", got)
	}
}

// TestPendingHostHelpers pins the legacy fallbacks and the pending-hosts
// invariant: pending hosts mean nothing without a pending name.
func TestPendingHostHelpers(t *testing.T) {
	if got := pendingOwnerHosts(&v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", PendingOwnerHosts: []string{"h2"}}); got != nil {
		t.Fatalf("pendingOwnerHosts(no pending name) = %v, want nil", got)
	}
	if got := pendingOwnerHosts(&v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", OwnerHosts: []string{"h1"}, PendingOwnerUser: "b"}); !reflect.DeepEqual(got, []string{"h1"}) {
		t.Fatalf("pendingOwnerHosts(legacy) = %v, want the shared hosts", got)
	}
	if got := pendingOwnerHosts(&v1alpha1.MysqlDatabaseStatus{OwnerUser: "a", PendingOwnerUser: "b"}); !reflect.DeepEqual(got, []string{"%"}) {
		t.Fatalf("pendingOwnerHosts(pre-hosts legacy) = %v, want %%", got)
	}
	if got := ledgerPendingHosts(v1alpha1.MysqlDatabaseUserState{Username: "a", PendingHosts: []string{"h2"}}); got != nil {
		t.Fatalf("ledgerPendingHosts(no pending name) = %v, want nil", got)
	}
	if got := ledgerPendingHosts(v1alpha1.MysqlDatabaseUserState{Username: "a", Hosts: []string{"h1"}, PendingUsername: "b"}); !reflect.DeepEqual(got, []string{"h1"}) {
		t.Fatalf("ledgerPendingHosts(legacy) = %v, want the shared hosts", got)
	}
	state := v1alpha1.MysqlDatabaseUserState{Username: "a", Hosts: []string{"h1"}, PendingUsername: "b", PendingHosts: []string{"h2"}}
	if got := ledgerHostsForName(state, "a"); !reflect.DeepEqual(got, []string{"h1"}) {
		t.Fatalf("ledgerHostsForName(a) = %v", got)
	}
	if got := ledgerHostsForName(state, "b"); !reflect.DeepEqual(got, []string{"h2"}) {
		t.Fatalf("ledgerHostsForName(b) = %v", got)
	}
}

// TestCurrentPrincipalClaims: every surface is a claim; only the owner and
// users[] are managed.
func TestCurrentPrincipalClaims(t *testing.T) {
	claims := currentPrincipalClaims("owner",
		[]v1alpha1.MysqlDatabaseGrant{{Username: "maester"}},
		[]tenantUserInput{{entry: v1alpha1.MysqlDatabaseUser{SecretName: "support"}, username: "acme_support", hosts: []string{"h2"}}})
	if c := claims["owner"]; !c.managed || c.surface != "spec.owner" {
		t.Fatalf("owner claim = %+v", c)
	}
	if c := claims["maester"]; c.managed || c.surface != "spec.grants[]" {
		t.Fatalf("grants claim = %+v", c)
	}
	if c := claims["acme_support"]; !c.managed || c.surface != `spec.users[] entry "support"` {
		t.Fatalf("users claim = %+v", c)
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
