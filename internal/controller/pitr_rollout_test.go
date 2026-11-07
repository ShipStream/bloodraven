package controller

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// pitrRolloutFG is a group archiving to an encrypted S3 profile, with its
// write binding Bound (K6-eligible: this group created the archive).
func pitrRolloutFG(credentials bool) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	if credentials {
		fg = newTestFGWithCredentials()
	}
	fg.UID = "lion-uid"
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "archive"},
		Profiles: []v1alpha1.BackupProfile{{
			Name: "archive",
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3,
				S3:   &v1alpha1.S3Storage{Bucket: "backups", Region: "us-east-1", CredentialsSecret: "s3-creds"},
			},
			Encryption: &v1alpha1.BackupEncryptionSpec{
				PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "archive-passphrase"},
			},
		}},
	}
	bindAllTestPITRArchives(fg)
	return fg
}

// With the write binding Bound, the render (and so the spec hash) is the
// binding's identity plus the profile's access fields: an encryption or
// selector edit, a same-prefix alias, the profile name, and the PVC's size
// and storage class change nothing (rows 2, 9, 10, K7, Δ29); an access
// field does (row 10a, K13).
func TestComputeSpecHashPITRRender(t *testing.T) {
	for _, credentials := range []bool{false, true} {
		fg := pitrRolloutFG(credentials)
		for _, tc := range []struct {
			name     string
			change   func(*v1alpha1.MysqlFailoverGroup)
			wantSame bool
		}{
			{"row 2: encryption removed", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Profiles[0].Encryption = nil }, true},
			{"row 5: secret renamed", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name = "rotated"
			}, true},
			{"selector key", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Key = "next"
			}, true},
			{"row 9/10: same-prefix alias made active", func(fg *v1alpha1.MysqlFailoverGroup) {
				alias := *fg.Spec.Backup.Profiles[0].DeepCopy()
				alias.Name = "alias"
				alias.Encryption.PassphraseSecret.Name = "other-bytes"
				fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, alias)
				fg.Spec.Backup.PITR.ProfileName = "alias"
			}, true},
			{"unused profile", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, v1alpha1.BackupProfile{Name: "unused", Encryption: &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "unused"}}})
			}, true},
			{"row 10a: region", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Profiles[0].Storage.S3.Region = "eu-west-1" }, false},
			{"row 10a: credentials Secret", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Storage.S3.CredentialsSecret = "s3-creds-2"
			}, false},
			{"row 10a: endpoint spelling", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Storage.S3.EndpointURL = "https://s3.amazonaws.com:443"
			}, false},
			{"row 10a: poll interval", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.PITR.ArchivePollInterval = &metav1.Duration{Duration: 30 * time.Second}
			}, false},
			{"row 22: PITR disabled", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.PITR.Enabled = false }, false},
		} {
			t.Run(tc.name+"/credentials="+strconv.FormatBool(credentials), func(t *testing.T) {
				changed := fg.DeepCopy()
				tc.change(changed)
				same := ComputeSpecHash(fg, fg.Spec.Sites[0], "", testPITRRender(fg)) == ComputeSpecHash(changed, changed.Spec.Sites[0], "", testPITRRender(changed))
				if same != tc.wantSame {
					t.Fatalf("same hash = %t, want %t (render %s → %s)", same, tc.wantSame, testPITRRender(fg).Canonical(), testPITRRender(changed).Canonical())
				}
			})
		}
	}
}

// Δ29: neither the profile name nor the backup PVC's size or storage class
// is a render input.
func TestComputeSpecHashPITRIgnoresProfileNameAndPVCSizing(t *testing.T) {
	fg := pitrRolloutFG(false)
	fg.Spec.Backup.Profiles[0].Storage = v1alpha1.BackupStorage{Type: v1alpha1.BackupStoragePVC,
		PVC: &v1alpha1.PVCStorage{ClaimName: "archive", Size: resource.MustParse("10Gi"), StorageClassName: "fast"}}
	fg.Spec.Backup.Profiles[0].Encryption = nil
	fg.Status.PITRArchiveBindings = nil
	bindAllTestPITRArchives(fg)
	base := ComputeSpecHash(fg, fg.Spec.Sites[0], "", testPITRRender(fg))

	changed := fg.DeepCopy()
	changed.Spec.Backup.Profiles[0].Storage.PVC.Size = resource.MustParse("20Gi")
	changed.Spec.Backup.Profiles[0].Storage.PVC.StorageClassName = "slow"
	changed.Spec.Backup.Profiles[0].Name = "renamed"
	changed.Spec.Backup.PITR.ProfileName = "renamed"
	if got := ComputeSpecHash(changed, changed.Spec.Sites[0], "", testPITRRender(changed)); got != base {
		t.Fatalf("profile name or PVC sizing changed the hash: %s → %s", testPITRRender(fg).Canonical(), testPITRRender(changed).Canonical())
	}
}

// Δ15, K10, O10 (rows 33, 33a, 33b): the pod digest reads only the keys
// the pods consume. A passphrase kept in the TLS Secret or a credentials
// Secret does not enter it; the root password and the certificates do; no
// PITR passphrase Secret is read at all.
func TestSpecSecretInputsConsumedKeysOnly(t *testing.T) {
	ctx := context.Background()
	key := bytes.Repeat([]byte{1}, digestKeyBytes)
	for _, credentials := range []bool{false, true} {
		t.Run("credentials="+strconv.FormatBool(credentials), func(t *testing.T) {
			fg := pitrRolloutFG(credentials)
			fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: "mysql-tls"}
			// Row 33a: the PITR passphrase lives in the TLS Secret.
			fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret = v1alpha1.PassphraseSecretRef{Name: "mysql-tls", Key: "pitr-passphrase"}
			tls := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "mysql-tls", Namespace: fg.Namespace},
				Data: map[string][]byte{"ca.crt": []byte("ca"), "tls.crt": []byte("crt"), "tls.key": []byte("key"), "pitr-passphrase": []byte("p1")}}
			objs := []client.Object{tls}
			for _, s := range newTestCredentialSecrets() {
				objs = append(objs, s)
			}
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
			digest := func() string {
				t.Helper()
				in, err := specSecretInputs(ctx, c, fg)
				if err != nil {
					t.Fatal(err)
				}
				return secretContentDigest(key, in)
			}
			update := func(name, k, v string) {
				t.Helper()
				var s corev1.Secret
				if err := c.Get(ctx, client.ObjectKey{Namespace: fg.Namespace, Name: name}, &s); err != nil {
					t.Fatal(err)
				}
				s.Data[k] = []byte(v)
				if err := c.Update(ctx, &s); err != nil {
					t.Fatal(err)
				}
			}
			base := digest()
			update("mysql-tls", "pitr-passphrase", "p2")
			if digest() != base {
				t.Fatal("row 33a: a passphrase in the TLS Secret changed the pod digest")
			}
			update("mysql-tls", "unrelated", "x")
			if digest() != base {
				t.Fatal("an unconsumed TLS key changed the pod digest")
			}
			update("mysql-tls", "tls.crt", "crt2")
			after := digest()
			if after == base {
				t.Fatal("a certificate change did not change the pod digest")
			}
			if !credentials {
				return
			}
			// Row 33: a passphrase kept in a credentials Secret.
			update("mysql-app-creds", "pitr-passphrase", "p")
			if digest() != after {
				t.Fatal("row 33: an unconsumed credentials key changed the pod digest")
			}
			// Row 33b: the root password rolls the pods.
			update("mysql-operator-creds", "MYSQL_ROOT_PASSWORD", "root-2")
			if digest() == after {
				t.Fatal("row 33b: MYSQL_ROOT_PASSWORD change did not change the pod digest")
			}
		})
	}
}

// The pod digest never reads a passphrase Secret, so neither its absence
// nor a read failure on it matters.
func TestSpecSecretInputsNeverReadsPassphrase(t *testing.T) {
	ctx := context.Background()
	fg := pitrRolloutFG(false)
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == "archive-passphrase" {
				return errors.New("must not be read")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()
	if _, err := specSecretInputs(ctx, c, fg); err != nil {
		t.Fatalf("specSecretInputs read the PITR passphrase: %v", err)
	}
}

// Δ15: the credential hash reads the role keys only. A missing passphrase
// Secret no longer fails it, a passphrase edit in a role Secret does not
// change it, and the operator Secret's replication account is hashed.
func TestComputeCredentialHashConsumedKeysOnly(t *testing.T) {
	ctx := context.Background()
	fg := pitrRolloutFG(true)
	fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret = v1alpha1.PassphraseSecretRef{Name: "missing-passphrase"}
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, c := newReconciler(objs...)
	h1, err := r.computeCredentialHash(ctx, fg)
	if err != nil {
		t.Fatalf("a missing passphrase Secret failed the credential hash: %v", err)
	}
	var app corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: fg.Namespace, Name: "mysql-app-creds"}, &app); err != nil {
		t.Fatal(err)
	}
	app.Data["passphrase"] = []byte("shared")
	if err := c.Update(ctx, &app); err != nil {
		t.Fatal(err)
	}
	if h2, err := r.computeCredentialHash(ctx, fg); err != nil || h2 != h1 {
		t.Fatalf("unconsumed key changed the credential hash: %s → %s (%v)", h1, h2, err)
	}
	app.Data["password"] = []byte("app-pass-2")
	if err := c.Update(ctx, &app); err != nil {
		t.Fatal(err)
	}
	if h3, err := r.computeCredentialHash(ctx, fg); err != nil || h3 == h1 {
		t.Fatalf("password change did not change the credential hash (%v)", err)
	}

	keys := credentialHashSecretKeys(newTestFG())
	want := "username,password,MYSQL_ROOT_PASSWORD,MYSQL_REPLICATION_USER,MYSQL_REPLICATION_PASSWORD"
	if got := keys[newTestFG().Spec.Credentials.OperatorSecret]; strings.Join(got, ",") != want {
		t.Fatalf("credential hash reads %v of the operator Secret, want %s", got, want)
	}
}
