package controller

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
