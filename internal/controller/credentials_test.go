package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestCredentialStatementErrorLabelRedactsPasswords(t *testing.T) {
	password := "super-secret-password"
	stmt := "ALTER USER 'app'@'%' IDENTIFIED BY '" + password + "'"

	label := credentialStatementErrorLabel("app", stmt)

	if strings.Contains(label, password) {
		t.Fatalf("label leaked password: %q", label)
	}
	if strings.Contains(strings.ToUpper(label), "IDENTIFIED BY") {
		t.Fatalf("label leaked credential SQL: %q", label)
	}
	if !strings.Contains(label, "app credential statement") {
		t.Fatalf("label = %q, want role context", label)
	}
}

func TestCredentialStatementErrorLabelKeepsNonSensitiveContext(t *testing.T) {
	stmt := "GRANT SELECT ON *.* TO 'app'@'%'"

	label := credentialStatementErrorLabel("app", stmt)

	if !strings.Contains(label, stmt) {
		t.Fatalf("label = %q, want non-sensitive statement context", label)
	}
}

// TestOpenAdminConnectionRootFallback pins the fallback contract the
// extraction into openAdminConnection changed, and its doc comment now
// states: root is attempted only when the operator connect fails AND the
// Secret carries a non-empty MYSQL_ROOT_PASSWORD. An empty or absent root
// password must not turn an operator-connect failure into an
// empty-password root login attempt.
func TestOpenAdminConnectionRootFallback(t *testing.T) {
	fixtures := func(rootPass string, includeKey bool) (*v1alpha1.MysqlFailoverGroup, *corev1.Secret) {
		fg := &v1alpha1.MysqlFailoverGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: mdbTestNamespace},
			Spec: v1alpha1.MysqlFailoverGroupSpec{
				Credentials: &v1alpha1.CredentialsSpec{OperatorSecret: "mysql-operator"},
				Sites:       []v1alpha1.SiteSpec{{Name: "dc1", Role: "primary-candidate"}},
			},
		}
		fg.Status.ActiveSite = "dc1"
		data := map[string][]byte{
			"username": []byte("operator"),
			"password": []byte("op-pw"),
		}
		if includeKey {
			data["MYSQL_ROOT_PASSWORD"] = []byte(rootPass)
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mysql-operator", Namespace: mdbTestNamespace},
			Data:       data,
		}
		return fg, secret
	}

	cases := []struct {
		name          string
		rootPass      string
		includeKey    bool
		operatorFails bool
		wantAttempts  []string
		wantErrPart   string
	}{
		{
			name:         "operator connects, root never attempted",
			rootPass:     "root-pw",
			includeKey:   true,
			wantAttempts: []string{"operator"},
		},
		{
			name:          "operator fails, root password set",
			rootPass:      "root-pw",
			includeKey:    true,
			operatorFails: true,
			wantAttempts:  []string{"operator", "root"},
		},
		{
			name:          "operator fails, root password empty",
			rootPass:      "",
			includeKey:    true,
			operatorFails: true,
			wantAttempts:  []string{"operator"},
			wantErrPart:   "connect to primary as operator",
		},
		{
			name:          "operator fails, root password key absent",
			includeKey:    false,
			operatorFails: true,
			wantAttempts:  []string{"operator"},
			wantErrPart:   "connect to primary as operator",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg, secret := fixtures(tc.rootPass, tc.includeKey)
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg, secret).Build()

			var attempts []string
			open := func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
				attempts = append(attempts, user)
				if tc.operatorFails && user == "operator" {
					return nil, errors.New("connection refused")
				}
				return &sql.DB{}, nil
			}

			db, err := openAdminConnection(context.Background(), c, fg, open)

			if len(attempts) != len(tc.wantAttempts) {
				t.Fatalf("dial attempts = %v, want %v", attempts, tc.wantAttempts)
			}
			for i := range tc.wantAttempts {
				if attempts[i] != tc.wantAttempts[i] {
					t.Fatalf("dial attempts = %v, want %v", attempts, tc.wantAttempts)
				}
			}
			if tc.wantErrPart == "" {
				if err != nil {
					t.Fatalf("openAdminConnection() error = %v, want nil", err)
				}
				if db == nil {
					t.Fatal("openAdminConnection() returned a nil *sql.DB without an error")
				}
			} else {
				if err == nil {
					t.Fatal("openAdminConnection() error = nil, want the operator-connect failure")
				}
				if !strings.Contains(err.Error(), tc.wantErrPart) {
					t.Fatalf("error = %q, want it to name %q (the error must point at the operator user, not root)", err, tc.wantErrPart)
				}
			}
		})
	}
}

// TestReconcileCredentialsRefusesTenantClaimedUsername is the reciprocal
// half of the disjoint-principals contract: group credential reconciliation
// runs ALTER USER plus global grants, and must fail closed before any SQL
// when a resolved role username belongs to a live MysqlDatabase.
func TestReconcileCredentialsRefusesTenantClaimedUsername(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: mdbTestNamespace},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Credentials: &v1alpha1.CredentialsSpec{
				OperatorSecret: "mysql-operator",
				AppSecret:      "mysql-app",
			},
			Sites: []v1alpha1.SiteSpec{{Name: "dc1", Role: "primary-candidate"}},
		},
	}
	fg.Status.ActiveSite = "dc1"

	operatorSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-operator", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"username": []byte("operator"), "password": []byte("op-pw")},
	}
	appSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-app", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"username": []byte("tenant_app"), "password": []byte("app-pw")},
	}
	mdb := &v1alpha1.MysqlDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-acme", Namespace: mdbTestNamespace},
		Spec: v1alpha1.MysqlDatabaseSpec{
			GroupRef:     v1alpha1.LocalGroupRef{Name: "main"},
			DatabaseName: "acme_wms",
			Owner:        v1alpha1.MysqlDatabaseOwner{SecretName: "acme-mysql-owner"},
		},
	}
	mdb.Status.OwnerUser = "tenant_app"

	c := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlDatabase{}).
		WithObjects(fg, operatorSecret, appSecret, mdb).
		Build()
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme()}

	err := r.reconcileCredentials(context.Background(), fg)
	if err == nil {
		t.Fatal("reconcileCredentials() = nil, want a refusal: the app role resolves to a tenant-owned username")
	}
	if !strings.Contains(err.Error(), "tenant_app") || !strings.Contains(err.Error(), "tenant-acme") {
		t.Fatalf("error = %q, want it to name the username and the claiming MysqlDatabase", err)
	}
}

func TestComputeCredentialHashIsKeyedOnContent(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	sec := newTestSecret()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(sec, fg.DeepCopy()).Build()
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme()}

	hash := func() string {
		t.Helper()
		h, err := r.computeCredentialHash(ctx, fg)
		if err != nil {
			t.Fatalf("compute credential hash: %v", err)
		}
		return h
	}
	getSecret := func() *corev1.Secret {
		t.Helper()
		var s corev1.Secret
		if err := c.Get(ctx, client.ObjectKey{Namespace: fg.Namespace, Name: fg.Spec.SecretName}, &s); err != nil {
			t.Fatalf("get secret: %v", err)
		}
		return &s
	}

	base := hash()

	// The published hash must not be reproducible from the Secret
	// contents alone: that is exactly an offline password verifier.
	unkeyed := sha256.New()
	fmt.Fprintf(unkeyed, "grants=%s\n", credentialGrantsVersion)
	fmt.Fprintf(unkeyed, "secrets=%s\n", secretContentDigest(make([]byte, digestKeyBytes),
		map[string]map[string][]byte{fg.Spec.SecretName: getSecret().Data}))
	if hex.EncodeToString(unkeyed.Sum(nil))[:16] == base {
		t.Fatal("credential hash is reproducible without the group's digest key")
	}

	// Metadata-only edits must not re-run the grants.
	meta := getSecret()
	meta.Labels = map[string]string{"touched": "yes"}
	if err := c.Update(ctx, meta); err != nil {
		t.Fatalf("update secret labels: %v", err)
	}
	if got := hash(); got != base {
		t.Fatalf("label-only Secret edit changed the hash: %s -> %s", base, got)
	}

	// A password rotation must re-run the grants.
	rotated := getSecret()
	rotated.Data["dsn"] = []byte("root:rotated@tcp(localhost:3306)/mysql")
	if err := c.Update(ctx, rotated); err != nil {
		t.Fatalf("rotate secret: %v", err)
	}
	if got := hash(); got == base {
		t.Fatal("hash did not change after the password was rotated")
	}
}

// credGrantDB is a database/sql connector that records executed
// statements and answers the app admin-privilege probe with adminPrivs.
type credGrantDB struct {
	adminPrivs int64
	stmts      []string
}

func (d *credGrantDB) Connect(context.Context) (driver.Conn, error) { return credGrantConn{d}, nil }
func (d *credGrantDB) Driver() driver.Driver                        { return nil }

type credGrantConn struct{ d *credGrantDB }

func (c credGrantConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}
func (c credGrantConn) Close() error              { return nil }
func (c credGrantConn) Begin() (driver.Tx, error) { return nil, errors.New("tx not supported") }
func (c credGrantConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.d.stmts = append(c.d.stmts, q)
	return driver.RowsAffected(0), nil
}
func (c credGrantConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if q != appAdminPrivilegeQuery {
		return nil, errors.New("unexpected query: " + q)
	}
	return &credGrantRows{v: c.d.adminPrivs}, nil
}

type credGrantRows struct {
	v    int64
	done bool
}

func (r *credGrantRows) Columns() []string { return []string{"n"} }
func (r *credGrantRows) Close() error      { return nil }
func (r *credGrantRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.v
	return nil
}

// TestReconcileAppRoleNarrowsGrant pins the app grant: no GRANT ALL,
// and an account still holding administrative privileges from an older
// release has its global privileges reset before the narrowed grant.
func TestReconcileAppRoleNarrowsGrant(t *testing.T) {
	role := credentialRole{
		name:                 "app",
		secretName:           "app\x00pw",
		grants:               []string{"GRANT " + appGlobalPrivileges + " ON *.* TO '%s'@'%%'"},
		resetAdminPrivileges: true,
	}
	for _, tc := range []struct {
		name       string
		adminPrivs int64
		wantRevoke bool
	}{
		{name: "already narrow", adminPrivs: 0},
		{name: "legacy grant all", adminPrivs: 3, wantRevoke: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &credGrantDB{adminPrivs: tc.adminPrivs}
			db := sql.OpenDB(fake)
			defer db.Close()
			if err := reconcileRole(context.Background(), db, role); err != nil {
				t.Fatalf("reconcileRole: %v", err)
			}
			revokeAt, grantAt := -1, -1
			for i, stmt := range fake.stmts {
				if strings.Contains(stmt, "ALL PRIVILEGES") && strings.HasPrefix(stmt, "GRANT") {
					t.Fatalf("app role must not be granted ALL PRIVILEGES: %q", stmt)
				}
				if stmt == "REVOKE ALL PRIVILEGES ON *.* FROM 'app'@'%'" {
					revokeAt = i
				}
				if strings.HasPrefix(stmt, "GRANT ") {
					grantAt = i
				}
			}
			if (revokeAt >= 0) != tc.wantRevoke {
				t.Fatalf("revoke present=%v, want %v; stmts=%q", revokeAt >= 0, tc.wantRevoke, fake.stmts)
			}
			if tc.wantRevoke && revokeAt > grantAt {
				t.Fatalf("revoke must precede the grant; stmts=%q", fake.stmts)
			}
		})
	}
}

func TestAppGrantExcludesAdminPrivileges(t *testing.T) {
	for _, priv := range []string{"ALL", "SUPER", "CREATE USER", "SHUTDOWN", "FILE", "PROCESS", "RELOAD", "REPLICATION", "GRANT OPTION"} {
		for _, p := range strings.Split(appGlobalPrivileges, ", ") {
			if p == priv || strings.HasPrefix(p, priv+" ") {
				t.Errorf("appGlobalPrivileges includes %q", p)
			}
		}
	}
	script := generateCredentialsModeInitScript(credentialsModeGroup())
	if strings.Contains(script, "create_user_with_grants app \"GRANT ALL") {
		t.Fatal("init script still grants ALL to the app user")
	}
	if !strings.Contains(script, appGlobalPrivileges) {
		t.Fatal("init script does not grant the app privilege list")
	}
}

// TestRestrictSystemSchema pins the app role's mysql schema restriction:
// it runs only once partial_revokes is confirmed on every site (a replica
// with it OFF stops on the replicated revoke), and only for roles that ask.
func TestRestrictSystemSchema(t *testing.T) {
	roles := []credentialRole{
		{name: "operator", secretName: "op\x00pw"},
		{name: "app", secretName: "o'app\x00pw", restrictSystemSchema: true},
		{name: "readonly", secretName: "ro\x00pw"},
	}

	t.Run("not confirmed", func(t *testing.T) {
		fake := &credGrantDB{}
		db := sql.OpenDB(fake)
		defer db.Close()
		err := restrictSystemSchema(context.Background(), db, roles, func(context.Context) error {
			return errors.New(`site "dr": connection refused`)
		})
		if err == nil || !strings.Contains(err.Error(), "partial_revokes not confirmed") || !strings.Contains(err.Error(), `site "dr"`) {
			t.Fatalf("restrictSystemSchema error = %v, want the unconfirmed site", err)
		}
		if len(fake.stmts) != 0 {
			t.Fatalf("statements ran before partial_revokes was confirmed: %q", fake.stmts)
		}
	})

	t.Run("confirmed", func(t *testing.T) {
		fake := &credGrantDB{}
		db := sql.OpenDB(fake)
		defer db.Close()
		if err := restrictSystemSchema(context.Background(), db, roles, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
		want := "REVOKE INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY TABLES, " +
			"LOCK TABLES, EXECUTE, CREATE VIEW, CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER ON mysql.* FROM 'o''app'@'%'"
		if len(fake.stmts) != 1 || fake.stmts[0] != want {
			t.Fatalf("stmts = %q, want only %q", fake.stmts, want)
		}
	})

	t.Run("no restricted role skips the site check", func(t *testing.T) {
		db := sql.OpenDB(&credGrantDB{})
		defer db.Close()
		err := restrictSystemSchema(context.Background(), db, roles[:1], func(context.Context) error {
			t.Fatal("ensure called without a restricted role")
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

// Every schema-level write or DDL privilege the app role holds globally
// must be in the mysql schema revoke; SHOW DATABASES is global-only and
// SELECT and SHOW VIEW do not write.
func TestAppSystemSchemaRevokeCoversWritePrivileges(t *testing.T) {
	revoked := strings.TrimSuffix(strings.TrimPrefix(appSystemSchemaRevoke, "REVOKE "), " ON mysql.* FROM '%s'@'%%'")
	got := map[string]bool{}
	for _, p := range strings.Split(revoked, ", ") {
		got[p] = true
	}
	for _, p := range strings.Split(appGlobalPrivileges, ", ") {
		readOnly := p == "SELECT" || p == "SHOW VIEW" || p == "SHOW DATABASES"
		if got[p] == readOnly {
			t.Errorf("privilege %q: revoked on mysql.* = %t, want %t", p, got[p], !readOnly)
		}
		delete(got, p)
	}
	for p := range got {
		t.Errorf("revoke lists %q, which appGlobalPrivileges does not grant", p)
	}
}
