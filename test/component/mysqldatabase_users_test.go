package component

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// These tests cover spec.users[] — the Secret-backed additional principals —
// against the same in-memory MySQL model as the owner/grants tests. The
// first consumer is the per-tenant support_ro reader, which is why the
// fixtures look the way they do.

const (
	mdbSupportSecretName = "support-ro-mysql"
	mdbSupportUser       = "acme_support"
	mdbSupportPass       = "support-pw-1"
)

func mdbSupportSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: mdbSupportSecretName, Namespace: mdbNamespace},
		Data: map[string][]byte{
			"username": []byte(mdbSupportUser),
			"password": []byte(mdbSupportPass),
		},
	}
}

// withSupportUser declares the support_ro-shaped users[] entry: SELECT-only
// with resource limits.
func withSupportUser(m *v1alpha1.MysqlDatabase) {
	m.Spec.Users = []v1alpha1.MysqlDatabaseUser{{
		SecretName: mdbSupportSecretName,
		Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
		ResourceLimits: &v1alpha1.MysqlUserResourceLimits{
			MaxUserConnections: 5,
			MaxQueriesPerHour:  3600,
		},
	}}
}

func (h *mdbHarness) updateSupportSecret(mutate func(*corev1.Secret)) {
	h.t.Helper()
	var s corev1.Secret
	key := types.NamespacedName{Namespace: mdbNamespace, Name: mdbSupportSecretName}
	if err := h.client.Get(context.Background(), key, &s); err != nil {
		h.t.Fatalf("get support secret: %v", err)
	}
	mutate(&s)
	if err := h.client.Update(context.Background(), &s); err != nil {
		h.t.Fatalf("update support secret: %v", err)
	}
}

func TestMysqlDatabaseUsersCreateGrantAndLimits(t *testing.T) {
	cr := mdbCR(withSupportUser)
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	mdb := h.requireReady()

	if pw, ok := h.server.password(mdbSupportUser); !ok || pw != mdbSupportPass {
		t.Fatalf("support user password = %q (present=%v), want %q", pw, ok, mdbSupportPass)
	}
	if privs, ok := h.server.grantsFor(mdbDatabase, mdbSupportUser); !ok || strings.Join(privs, ",") != "SELECT" {
		t.Fatalf("support grants = %v (present=%v), want [SELECT]", privs, ok)
	}
	if limits, ok := h.server.resourceLimitsFor(mdbSupportUser); !ok || limits != "5/3600" {
		t.Fatalf("support resource limits = %q (present=%v), want 5/3600", limits, ok)
	}
	if len(mdb.Status.AppliedUsers) != 1 ||
		mdb.Status.AppliedUsers[0].SecretName != mdbSupportSecretName ||
		mdb.Status.AppliedUsers[0].Username != mdbSupportUser ||
		mdb.Status.AppliedUsers[0].PendingUsername != "" {
		t.Fatalf("status.appliedUsers = %+v, want one settled entry for %s/%s",
			mdb.Status.AppliedUsers, mdbSupportSecretName, mdbSupportUser)
	}
	// users[] principals are tracked in appliedUsers, not appliedGrants.
	for _, g := range mdb.Status.AppliedGrants {
		if g == mdbSupportUser {
			t.Fatal("support user leaked into status.appliedGrants; the ledger owns users[] principals")
		}
	}
	// No credential material anywhere in status.
	if strings.Contains(mdb.Status.Message, mdbSupportPass) || strings.Contains(mdb.Status.LastAppliedHash, mdbSupportPass) {
		t.Fatal("status leaked the support password")
	}
	h.server.assertNoGrantOption(t)
}

func TestMysqlDatabaseUsersUnchangedReapplyIssuesNoStatements(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()
	after := h.server.statementCount()

	for i := 0; i < 3; i++ {
		h.reconcile()
	}
	if extra := h.server.statementsSince(after); len(extra) != 0 {
		t.Fatalf("re-applying an unchanged CR with users[] issued %d statements: %v", len(extra), extra)
	}
}

// TestMysqlDatabaseUsersPasswordRotation: rotation is a Secret write and
// nothing else, exactly as for the owner — the user Secret is part of the
// apply hash and the Secret watch index.
func TestMysqlDatabaseUsersPasswordRotation(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()

	h.updateSupportSecret(func(s *corev1.Secret) { s.Data["password"] = []byte("support-pw-2") })
	before := h.server.statementCount()
	h.reconcile()
	h.requireReady()

	if pw, _ := h.server.password(mdbSupportUser); pw != "support-pw-2" {
		t.Fatalf("support password = %q after rotation, want support-pw-2", pw)
	}
	// ALTER, never a drop: the account is re-identified, not re-created.
	for _, stmt := range h.server.statementsSince(before) {
		if strings.HasPrefix(stmt, "DROP USER") {
			t.Fatalf("password rotation dropped a user: %q", stmt)
		}
	}
}

func TestMysqlDatabaseUsersUsernameRotationCreatesBeforeDrop(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()

	h.updateSupportSecret(func(s *corev1.Secret) {
		s.Data["username"] = []byte("acme_support_v2")
		s.Data["password"] = []byte("support-pw-2")
	})

	before := h.server.statementCount()
	h.reconcile()
	mdb := h.requireReady()

	createAt, dropAt := -1, -1
	for i, stmt := range h.server.statementsSince(before) {
		switch {
		case strings.Contains(stmt, "CREATE USER IF NOT EXISTS 'acme_support_v2'"):
			createAt = i
		case stmt == "DROP USER IF EXISTS 'acme_support'@'%'":
			dropAt = i
		}
	}
	if createAt == -1 || dropAt == -1 {
		t.Fatalf("rotation statements missing: create=%d drop=%d", createAt, dropAt)
	}
	if createAt > dropAt {
		t.Fatalf("rotation dropped the old principal (statement %d) before creating the new one (%d)", dropAt, createAt)
	}
	if h.server.hasUser(mdbSupportUser) {
		t.Fatal("old support principal survived the rotation")
	}
	if len(mdb.Status.AppliedUsers) != 1 ||
		mdb.Status.AppliedUsers[0].Username != "acme_support_v2" ||
		mdb.Status.AppliedUsers[0].PendingUsername != "" {
		t.Fatalf("status.appliedUsers = %+v, want the settled rotated entry", mdb.Status.AppliedUsers)
	}
}

// TestMysqlDatabaseUsersRotationSurvivesFailedReadyStamp is the users[]
// analog of the pendingOwnerUser wedge regression: a rotation that ran its
// MySQL work but crashed before the Ready stamp must converge on retry,
// because the ledger's pendingUsername write-ahead attributes the
// already-created account to this entry.
func TestMysqlDatabaseUsersRotationSurvivesFailedReadyStamp(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()

	h.updateSupportSecret(func(s *corev1.Secret) {
		s.Data["username"] = []byte("acme_support_v2")
		s.Data["password"] = []byte("support-pw-2")
	})

	// Simulate the crash: the new account exists, the old one is gone, and
	// status still carries the pre-rotation ledger plus the write-ahead.
	h.server.removeUser(mdbSupportUser)
	h.server.addUser("acme_support_v2", "support-pw-2")
	mdb := h.get()
	mdb.Status.Phase = v1alpha1.MysqlDatabasePhaseCreating
	mdb.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{{
		SecretName:      mdbSupportSecretName,
		Username:        mdbSupportUser,
		PendingUsername: "acme_support_v2",
	}}
	mdb.Status.LastAppliedHash = ""
	if err := h.client.Status().Update(context.Background(), mdb); err != nil {
		t.Fatalf("simulate crashed-rotation status: %v", err)
	}

	h.reconcile()

	mdb = h.requireReady() // must converge, not Failed/PreExistingUser
	if len(mdb.Status.AppliedUsers) != 1 ||
		mdb.Status.AppliedUsers[0].Username != "acme_support_v2" ||
		mdb.Status.AppliedUsers[0].PendingUsername != "" {
		t.Fatalf("status.appliedUsers = %+v, want the settled rotated entry", mdb.Status.AppliedUsers)
	}
	if pw, ok := h.server.password("acme_support_v2"); !ok || pw != "support-pw-2" {
		t.Fatalf("rotated principal password = %q (present=%v), want the rotated Secret value", pw, ok)
	}
}

// TestMysqlDatabaseUsersRemovalRevokesAndDrops: users[] is desired state on
// the way out. Removing the entry revokes and drops the principal — via the
// status.appliedUsers ledger, so it works even when the Secret is already
// gone (the offboarding ordering: KV cleanup can precede the CR edit).
func TestMysqlDatabaseUsersRemovalRevokesAndDrops(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()

	// The Secret disappears first; the ledger, not the Secret, names the
	// account to remove.
	var s corev1.Secret
	key := types.NamespacedName{Namespace: mdbNamespace, Name: mdbSupportSecretName}
	if err := h.client.Get(context.Background(), key, &s); err != nil {
		t.Fatalf("get support secret: %v", err)
	}
	if err := h.client.Delete(context.Background(), &s); err != nil {
		t.Fatalf("delete support secret: %v", err)
	}
	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Users = nil })

	before := h.server.statementCount()
	h.reconcile()
	mdb := h.requireReady()

	if h.server.hasUser(mdbSupportUser) {
		t.Fatal("removed users[] principal survived in MySQL")
	}
	if len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("status.appliedUsers = %+v after removal, want empty", mdb.Status.AppliedUsers)
	}
	revoked := false
	for _, stmt := range h.server.statementsSince(before) {
		if strings.HasPrefix(stmt, "REVOKE") && strings.Contains(stmt, "'"+mdbSupportUser+"'") {
			revoked = true
		}
	}
	if !revoked {
		t.Fatal("no REVOKE for the removed principal; DROP USER alone is not the contract (a vetoed drop must still lose its rights)")
	}
}

// TestMysqlDatabaseUsersReservedUsernameFails is the escalation gate: a
// users[] Secret naming a group-level credential must fail loudly before any
// SQL touches that account — otherwise a tenant CR could reset the
// operator's or replicator's password via ALTER USER.
func TestMysqlDatabaseUsersReservedUsernameFails(t *testing.T) {
	support := mdbSupportSecret()
	support.Data["username"] = []byte("operator")
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), support)

	h.reconcile()

	mdb := h.get()
	requireFailed(t, mdb, "UserReserved")
	if pw, _ := h.server.password("operator"); pw != "op-pw" {
		t.Fatalf("group operator password was reset to %q by a tenant users[] entry", pw)
	}
	if h.server.statementCount() != 0 {
		t.Fatalf("reserved-user refusal still executed %d statements: %v",
			h.server.statementCount(), h.server.statementsSince(0))
	}
}

// TestMysqlDatabaseUsersRefusesPreExistingAccount: an account that exists
// without a ledger attribution is someone else's; CREATE USER IF NOT EXISTS
// + ALTER USER must not capture it, and deletion must not drop it.
func TestMysqlDatabaseUsersRefusesPreExistingAccount(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.addUser(mdbSupportUser, "foreign-password")

	h.reconcile()

	mdb := h.get()
	requireFailed(t, mdb, "PreExistingUser")
	if pw, _ := h.server.password(mdbSupportUser); pw != "foreign-password" {
		t.Fatalf("foreign account password was reset to %q", pw)
	}
	// The write-ahead was rolled back: the ledger must not claim the
	// refused account, or deletion would drop it.
	for _, state := range mdb.Status.AppliedUsers {
		if state.SecretName == mdbSupportSecretName {
			t.Fatalf("status.appliedUsers still claims the refused entry: %+v", state)
		}
	}

	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
	h.delete()
	h.reconcile()
	if !h.server.hasUser(mdbSupportUser) {
		t.Fatal("deletion dropped a pre-existing account the CR refused to adopt")
	}
}

func TestMysqlDatabaseDeletePolicyDropsUsersPrincipals(t *testing.T) {
	cr := mdbCR(withSupportUser, func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()

	h.delete()
	h.reconcile()

	if h.server.hasUser(mdbSupportUser) {
		t.Fatal("deletionPolicy=Delete left the users[] principal in place; the ledger authorizes its drop")
	}
	if h.server.hasUser(mdbOwnerUser) {
		t.Fatal("deletionPolicy=Delete left the owner in place")
	}
	if _, ok := h.server.database(mdbDatabase); ok {
		t.Fatal("deletionPolicy=Delete left the database in place")
	}
}

func TestMysqlDatabaseDeleteRetainLeavesUsersPrincipals(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()
	before := h.server.statementCount()

	h.delete()
	h.reconcile()

	if extra := h.server.statementsSince(before); len(extra) != 0 {
		t.Fatalf("Retain deletion issued %d MySQL statements: %v", len(extra), extra)
	}
	if !h.server.hasUser(mdbSupportUser) {
		t.Fatal("Retain deletion dropped the users[] principal")
	}
}

// TestMysqlDatabaseUsersLimitRemovalResetsToZero: resourceLimits are desired
// state on the way out too — omitting a previously-set limit must render 0
// (no account-level cap), not silently leave the old cap enforced.
func TestMysqlDatabaseUsersLimitRemovalResetsToZero(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()
	if limits, _ := h.server.resourceLimitsFor(mdbSupportUser); limits != "5/3600" {
		t.Fatalf("resource limits = %q, want 5/3600", limits)
	}

	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Users[0].ResourceLimits = nil })
	h.reconcile()
	h.requireReady()

	if limits, ok := h.server.resourceLimitsFor(mdbSupportUser); !ok || limits != "0/0" {
		t.Fatalf("resource limits after removal = %q (present=%v), want 0/0", limits, ok)
	}
}

// TestMysqlDatabaseUsersDropsAbandonedRotationTarget: a rotation whose drop
// of the previous account failed transiently leaves the ledger recording
// both names. If the Secret is then reverted to the previous account, the
// rotation target is no longer desired state and no longer nameable after
// the Ready stamp rewrites the ledger — so this reconcile is the last one
// that can drop it. Missing that is a lingering credential on the tenant
// schema, exactly what users[] exists to prevent.
func TestMysqlDatabaseUsersDropsAbandonedRotationTarget(t *testing.T) {
	const abandoned = "acme_support_v2"

	// The ledger a crashed/failed rotation leaves behind: settled on
	// mdbSupportUser (which the Secret still names), with the rotation
	// target it created recorded as pending.
	cr := mdbCR(withSupportUser, func(m *v1alpha1.MysqlDatabase) {
		m.Status.DatabaseCreated = true
		m.Status.OwnerUser = mdbOwnerUser
		m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{{
			SecretName:      mdbSupportSecretName,
			Username:        mdbSupportUser,
			PendingUsername: abandoned,
		}}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.addUser(mdbOwnerUser, mdbOwnerPass)
	h.server.addUser(mdbSupportUser, mdbSupportPass)
	h.server.addUser(abandoned, "rotated-pw")

	h.reconcile()
	h.requireReady()

	if h.server.hasUser(abandoned) {
		t.Fatalf("abandoned rotation target %q survived the reconcile that last knew its name", abandoned)
	}
	if !h.server.hasUser(mdbSupportUser) {
		t.Fatalf("the settled users[] principal %q was dropped", mdbSupportUser)
	}
	for _, state := range h.get().Status.AppliedUsers {
		if state.PendingUsername != "" {
			t.Fatalf("Ready ledger still carries a pending username: %+v", state)
		}
	}
}

// TestMysqlDatabaseUsersSecretRenameTransfersAccount: renaming the Secret an
// entry points at (an ESO target renamed, a chart refactor) while the
// username inside stays the same is a re-key of the ledger record, not a
// PreExistingUser refusal and not a drop-and-recreate. The account is this
// CR's — the ledger says so under the old key — so the new entry adopts it,
// the old entry's removal pass leaves it alone, and the credential never
// goes away.
func TestMysqlDatabaseUsersSecretRenameTransfersAccount(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()

	const renamed = "support-ro-mysql-v2"
	renamedSecret := mdbSupportSecret()
	renamedSecret.Name = renamed
	if err := h.client.Create(context.Background(), renamedSecret); err != nil {
		t.Fatalf("create renamed support secret: %v", err)
	}
	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Users[0].SecretName = renamed })

	before := h.server.statementCount()
	h.reconcile()
	mdb := h.requireReady() // not Failed/PreExistingUser

	// The ordinary apply (GRANT + surplus REVOKE of undeclared privileges)
	// is expected; the removal pass's REVOKE ALL and DROP USER are not.
	for _, stmt := range h.server.statementsSince(before) {
		if strings.HasPrefix(stmt, "DROP USER") ||
			(strings.HasPrefix(stmt, "REVOKE") && strings.Contains(stmt, "ALL PRIVILEGES ON")) {
			t.Fatalf("secretName rename ran %q; the account is current desired state under the new key", stmt)
		}
	}
	if !h.server.hasUser(mdbSupportUser) {
		t.Fatal("secretName rename lost the principal")
	}
	if len(mdb.Status.AppliedUsers) != 1 ||
		mdb.Status.AppliedUsers[0].SecretName != renamed ||
		mdb.Status.AppliedUsers[0].Username != mdbSupportUser ||
		mdb.Status.AppliedUsers[0].PendingUsername != "" {
		t.Fatalf("status.appliedUsers = %+v, want the record re-keyed to %q", mdb.Status.AppliedUsers, renamed)
	}
	if !h.sawEvent("UserTransferred") {
		t.Fatal("no UserTransferred event; operators need to see why the removal pass did not fire UserRemoved")
	}
}

// TestMysqlDatabaseUsersNewEntryTakesRotatedName: entry A rotates from
// acme_support to acme_support_v2 in the same reconcile that a new entry B
// declares acme_support. A's rotation drop must not take B's account with
// it — the name is current desired state under B.
func TestMysqlDatabaseUsersNewEntryTakesRotatedName(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	h.requireReady()

	const biSecret = "bi-mysql"
	bi := mdbSupportSecret()
	bi.Name = biSecret
	bi.Data["password"] = []byte("bi-pw") // same username as the support entry's old name
	if err := h.client.Create(context.Background(), bi); err != nil {
		t.Fatalf("create bi secret: %v", err)
	}
	h.updateSupportSecret(func(s *corev1.Secret) {
		s.Data["username"] = []byte("acme_support_v2")
		s.Data["password"] = []byte("support-pw-2")
	})
	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Users = append(m.Spec.Users, v1alpha1.MysqlDatabaseUser{
			SecretName: biSecret,
			Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
		})
	})

	h.reconcile()
	mdb := h.requireReady()

	if !h.server.hasUser(mdbSupportUser) {
		t.Fatal("rotation of the support entry dropped the account the new bi entry now declares")
	}
	if !h.server.hasUser("acme_support_v2") {
		t.Fatal("rotated support principal missing")
	}
	if pw, _ := h.server.password(mdbSupportUser); pw != "bi-pw" {
		t.Fatalf("transferred account password = %q, want the bi Secret's value", pw)
	}
	if len(mdb.Status.AppliedUsers) != 2 {
		t.Fatalf("status.appliedUsers = %+v, want both entries settled", mdb.Status.AppliedUsers)
	}
}

// sawEvent drains the fake recorder and reports whether an event with the
// given reason was emitted so far.
func (h *mdbHarness) sawEvent(reason string) bool {
	h.t.Helper()
	for {
		select {
		case e := <-h.rec.Events:
			if strings.Contains(e, " "+reason+" ") {
				return true
			}
		default:
			return false
		}
	}
}

// --- hosts ---------------------------------------------------------------
//
// A principal with several hosts is one record in the CR and one MySQL
// account per host underneath: created, rotated, granted and dropped by a
// single statement each. The motivating shape is a platform with a few
// static egress IPs.

var threeHosts = []string{"35.1.2.3", "35.4.5.6", "35.7.8.9"}

func withOwnerHosts(hosts ...string) func(*v1alpha1.MysqlDatabase) {
	return func(m *v1alpha1.MysqlDatabase) { m.Spec.Owner.Hosts = hosts }
}

func withSupportHosts(hosts ...string) func(*v1alpha1.MysqlDatabase) {
	return func(m *v1alpha1.MysqlDatabase) { m.Spec.Users[0].Hosts = hosts }
}

func TestMysqlDatabaseHostsCreateOneAccountPerHostInOneStatement(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withOwnerHosts(threeHosts...), withSupportUser, withSupportHosts("10.0.0.0/24", "10.0.1.%")),
		mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())

	h.reconcile()
	mdb := h.requireReady()

	if got := h.server.hostsOf(mdbOwnerUser); !reflect.DeepEqual(got, []string{"35.1.2.3", "35.4.5.6", "35.7.8.9"}) {
		t.Fatalf("owner accounts = %v, want one per host", got)
	}
	if h.server.hasAccount(mdbOwnerUser, "%") {
		t.Fatal("owner got a '%' account although hosts were declared")
	}
	if got := h.server.hostsOf(mdbSupportUser); !reflect.DeepEqual(got, []string{"10.0.0.0/24", "10.0.1.%"}) {
		t.Fatalf("support accounts = %v", got)
	}
	for _, host := range threeHosts {
		if privs, ok := h.server.grantsForAccount(mdbDatabase, mdbOwnerUser, host); !ok || privs[0] != "ALL PRIVILEGES" {
			t.Fatalf("owner@%s grants = %v (present=%v)", host, privs, ok)
		}
	}
	// One CREATE USER and one ALTER USER for the owner, naming all three.
	creates := 0
	for _, stmt := range h.server.statementsSince(0) {
		if strings.HasPrefix(stmt, "CREATE USER IF NOT EXISTS '"+mdbOwnerUser+"'") {
			creates++
			for _, host := range threeHosts {
				if !strings.Contains(stmt, "'"+mdbOwnerUser+"'@'"+host+"'") {
					t.Fatalf("owner CREATE USER %q does not name host %s", stmt, host)
				}
			}
		}
	}
	if creates != 1 {
		t.Fatalf("owner CREATE USER statements = %d, want exactly 1 multi-account statement", creates)
	}
	if !reflect.DeepEqual(mdb.Status.OwnerHosts, threeHosts) {
		t.Fatalf("status.ownerHosts = %v, want %v", mdb.Status.OwnerHosts, threeHosts)
	}
	if len(mdb.Status.AppliedUsers) != 1 || !reflect.DeepEqual(mdb.Status.AppliedUsers[0].Hosts, []string{"10.0.0.0/24", "10.0.1.%"}) {
		t.Fatalf("status.appliedUsers = %+v, want the hosts recorded", mdb.Status.AppliedUsers)
	}
}

func TestMysqlDatabaseHostsRemovalDropsOnlyThatHost(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withOwnerHosts(threeHosts...), withSupportUser, withSupportHosts(threeHosts...)),
		mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Owner.Hosts = threeHosts[:2]
		m.Spec.Users[0].Hosts = threeHosts[1:]
	})
	before := h.server.statementCount()
	h.reconcile()
	mdb := h.requireReady()

	if got := h.server.hostsOf(mdbOwnerUser); !reflect.DeepEqual(got, threeHosts[:2]) {
		t.Fatalf("owner accounts after host removal = %v, want %v", got, threeHosts[:2])
	}
	if got := h.server.hostsOf(mdbSupportUser); !reflect.DeepEqual(got, threeHosts[1:]) {
		t.Fatalf("support accounts after host removal = %v, want %v", got, threeHosts[1:])
	}
	for _, stmt := range h.server.statementsSince(before) {
		if strings.HasPrefix(stmt, "DROP USER") {
			if strings.Contains(stmt, "'35.4.5.6'") {
				t.Fatalf("%q dropped a host both principals still declare", stmt)
			}
		}
	}
	if !reflect.DeepEqual(mdb.Status.OwnerHosts, threeHosts[:2]) {
		t.Fatalf("status.ownerHosts = %v, want settled to %v", mdb.Status.OwnerHosts, threeHosts[:2])
	}
	if !h.sawEvent("OwnerHostsRemoved") {
		t.Fatal("no OwnerHostsRemoved event")
	}
}

func TestMysqlDatabaseHostsAdditionAndPasswordRotationTouchEveryAccount(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser, withSupportHosts(threeHosts[0])),
		mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()

	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Users[0].Hosts = threeHosts })
	h.updateSupportSecret(func(s *corev1.Secret) { s.Data["password"] = []byte("support-pw-2") })
	before := h.server.statementCount()
	h.reconcile()
	h.requireReady()

	if got := h.server.hostsOf(mdbSupportUser); !reflect.DeepEqual(got, threeHosts) {
		t.Fatalf("support accounts = %v, want %v", got, threeHosts)
	}
	alters := 0
	for _, stmt := range h.server.statementsSince(before) {
		if strings.HasPrefix(stmt, "ALTER USER '"+mdbSupportUser+"'") {
			alters++
		}
	}
	if alters != 1 {
		t.Fatalf("ALTER USER statements for the support principal = %d, want 1 covering all hosts", alters)
	}
	for _, host := range threeHosts {
		if !h.server.hasAccount(mdbSupportUser, host) {
			t.Fatalf("missing %s@%s", mdbSupportUser, host)
		}
		if limits, ok := h.server.resourceLimits[accountKey(mdbSupportUser, host)]; !ok || limits != "5/3600" {
			t.Fatalf("%s@%s limits = %q, want 5/3600 (limits are per account)", mdbSupportUser, host, limits)
		}
	}
	if pw, _ := h.server.password(mdbSupportUser); pw != "support-pw-2" {
		t.Fatalf("rotated password = %q", pw)
	}
}

func TestMysqlDatabaseHostsUsernameRotationDropsOldNameOnEveryHost(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withOwnerHosts(threeHosts...)), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.reconcile()
	h.requireReady()

	var s corev1.Secret
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: mdbOwnerSecret().Name}, &s); err != nil {
		t.Fatalf("get owner secret: %v", err)
	}
	s.Data["username"] = []byte("acme_app_v2")
	if err := h.client.Update(context.Background(), &s); err != nil {
		t.Fatalf("update owner secret: %v", err)
	}
	h.reconcile()
	mdb := h.requireReady()

	if h.server.hasUser(mdbOwnerUser) {
		t.Fatalf("old owner survived on hosts %v", h.server.hostsOf(mdbOwnerUser))
	}
	if got := h.server.hostsOf("acme_app_v2"); !reflect.DeepEqual(got, threeHosts) {
		t.Fatalf("new owner accounts = %v, want %v", got, threeHosts)
	}
	if mdb.Status.OwnerUser != "acme_app_v2" || !reflect.DeepEqual(mdb.Status.OwnerHosts, threeHosts) {
		t.Fatalf("status owner = %q hosts %v", mdb.Status.OwnerUser, mdb.Status.OwnerHosts)
	}
}

func TestMysqlDatabaseHostsDeletePolicyDropsEveryAccount(t *testing.T) {
	cr := mdbCR(withOwnerHosts(threeHosts...), withSupportUser, withSupportHosts(threeHosts...), func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()

	h.delete()
	h.reconcile()
	if h.server.hasUser(mdbOwnerUser) || h.server.hasUser(mdbSupportUser) {
		t.Fatalf("Delete left accounts behind: owner %v support %v", h.server.hostsOf(mdbOwnerUser), h.server.hostsOf(mdbSupportUser))
	}
}

// TestMysqlDatabaseHostsUpgradeFromPreHostsStatus: a CR reconciled before
// the hosts field existed has status.ownerUser but no status.ownerHosts, and
// its account lives on '%'. Declaring hosts must move it — create the new
// accounts and drop the '%' one — off the implied ["%"] record, not wedge or
// leak the '%' account.
func TestMysqlDatabaseHostsUpgradeFromPreHostsStatus(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()

	// Strip the hosts records, as a status written by the previous
	// operator version would be.
	mdb := h.get()
	mdb.Status.OwnerHosts = nil
	for i := range mdb.Status.AppliedUsers {
		mdb.Status.AppliedUsers[i].Hosts = nil
	}
	if err := h.client.Status().Update(context.Background(), mdb); err != nil {
		t.Fatalf("strip hosts from status: %v", err)
	}

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Owner.Hosts = threeHosts[:1]
		m.Spec.Users[0].Hosts = threeHosts[:1]
	})
	h.reconcile()
	mdb = h.requireReady()

	for _, user := range []string{mdbOwnerUser, mdbSupportUser} {
		if h.server.hasAccount(user, "%") {
			t.Fatalf("%s@%% survived; the pre-hosts record implies '%%' and must be dropped", user)
		}
		if !h.server.hasAccount(user, threeHosts[0]) {
			t.Fatalf("%s@%s missing", user, threeHosts[0])
		}
	}
	if !reflect.DeepEqual(mdb.Status.OwnerHosts, threeHosts[:1]) {
		t.Fatalf("status.ownerHosts = %v", mdb.Status.OwnerHosts)
	}
}
