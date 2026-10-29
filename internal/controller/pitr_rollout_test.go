package controller

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func pitrRolloutFG(credentials bool) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	if credentials {
		fg = newTestFGWithCredentials()
	}
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "archive"},
		Profiles: []v1alpha1.BackupProfile{{
			Name: "archive",
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3,
				S3:   &v1alpha1.S3Storage{Bucket: "backups"},
			},
			Encryption: &v1alpha1.BackupEncryptionSpec{
				PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "archive-passphrase"},
			},
		}},
	}
	return fg
}

func TestComputeSpecHashPITREncryption(t *testing.T) {
	for _, credentials := range []bool{false, true} {
		fg := pitrRolloutFG(credentials)
		for _, tc := range []struct {
			name     string
			change   func(*v1alpha1.MysqlFailoverGroup)
			wantSame bool
		}{
			{"disable", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Profiles[0].Encryption = nil }, false},
			{"rename secret", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name = "rotated"
			}, false},
			{"select key", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Key = "next"
			}, false},
			{"explicit default key", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Key = "passphrase"
			}, true},
			{"explicit default algorithm", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Profiles[0].Encryption.Algorithm = "AES-256-GCM" }, true},
			// A future algorithm must enter the hash even before it is accepted by the API.
			{"algorithm", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles[0].Encryption.Algorithm = "future-algorithm"
			}, false},
			{"unused profile", func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, v1alpha1.BackupProfile{Name: "unused", Encryption: &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "unused"}}})
			}, true},
		} {
			t.Run(tc.name+"/credentials="+strconv.FormatBool(credentials), func(t *testing.T) {
				changed := fg.DeepCopy()
				tc.change(changed)
				same := ComputeSpecHash(fg, fg.Spec.Sites[0], "") == ComputeSpecHash(changed, changed.Spec.Sites[0], "")
				if same != tc.wantSame {
					t.Fatalf("same hash = %t, want %t", same, tc.wantSame)
				}
			})
		}
	}
}

func TestSpecSecretInputsPITRPassphrase(t *testing.T) {
	ctx := context.Background()
	for _, credentials := range []bool{false, true} {
		fg := pitrRolloutFG(credentials)
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "archive-passphrase", Namespace: fg.Namespace},
			Data:       map[string][]byte{"passphrase": bytes.Repeat([]byte("a"), 32), "next": bytes.Repeat([]byte("b"), 32)},
		}
		c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(sec).Build()
		inputs, err := specSecretInputs(ctx, c, fg)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(inputs["pitr.passphrase"]["passphrase"], sec.Data["passphrase"]) {
			t.Errorf("credentials=%t: selected PITR passphrase missing from digest inputs", credentials)
		}
		key := bytes.Repeat([]byte{1}, digestKeyBytes)
		before := secretContentDigest(key, inputs)
		fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Key = "passphrase"
		explicitDefault, err := specSecretInputs(ctx, c, fg)
		if err != nil {
			t.Fatal(err)
		}
		if secretContentDigest(key, explicitDefault) != before {
			t.Error("explicit default key changed the digest")
		}
		if before == secretContentDigest(bytes.Repeat([]byte{2}, digestKeyBytes), inputs) {
			t.Errorf("credentials=%t: passphrase fingerprint must depend on the per-group key", credentials)
		}
		sec.Data["passphrase"] = bytes.Repeat([]byte("c"), 32)
		if err := c.Update(ctx, sec); err != nil {
			t.Fatal(err)
		}
		rotated, err := specSecretInputs(ctx, c, fg)
		if err != nil {
			t.Fatal(err)
		}
		if before == secretContentDigest(key, rotated) {
			t.Errorf("credentials=%t: byte rotation did not change digest", credentials)
		}
		fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Key = "next"
		selected, err := specSecretInputs(ctx, c, fg)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(selected["pitr.passphrase"]["next"], sec.Data["next"]) || len(selected["pitr.passphrase"]) != 1 {
			t.Errorf("credentials=%t: digest must include the selected passphrase key only", credentials)
		}
		if credentials {
			if len(selected["cred."+sec.Name]) != len(sec.Data) {
				t.Error("existing credentials-mode inputs must be preserved")
			}
		} else {
			sec.Data["passphrase"] = []byte("unselected change")
			if err := c.Update(ctx, sec); err != nil {
				t.Fatal(err)
			}
			unselected, err := specSecretInputs(ctx, c, fg)
			if err != nil {
				t.Fatal(err)
			}
			if secretContentDigest(key, selected) != secretContentDigest(key, unselected) {
				t.Error("unselected passphrase data changed legacy digest")
			}
		}
	}
}

func TestSpecSecretInputsPITRReadFailure(t *testing.T) {
	ctx := context.Background()
	for _, credentials := range []bool{false, true} {
		fg := pitrRolloutFG(credentials)
		readErr := errors.New("temporary API failure")
		digestKey := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: digestKeySecretName(fg.Name), Namespace: fg.Namespace},
			Data:       map[string][]byte{digestKeyDataKey: bytes.Repeat([]byte{1}, digestKeyBytes)},
		}
		c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(digestKey).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "archive-passphrase" {
					return readErr
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
		if _, err := specSecretInputs(ctx, c, fg); !errors.Is(err, readErr) {
			t.Errorf("credentials=%t: read failure was hidden: %v", credentials, err)
		}
		runner := &TopologyManagerRunner{client: c, logger: testLogger()}
		tm := &TopologyManager{specDriftSites: []string{"dc2"}}
		runner.checkSpecDrift(ctx, fg, tm)
		if strings.Join(tm.specDriftSites, ",") != "dc2" {
			t.Fatalf("read failure replaced the current drift set: %v", tm.specDriftSites)
		}
		fg.Spec.Backup.PITR.Enabled = false
		if _, err := specSecretInputs(ctx, c, fg); err != nil {
			t.Errorf("inactive PITR should not add a passphrase dependency: %v", err)
		}
	}
}

func TestCheckSpecDriftPITREncryption(t *testing.T) {
	ctx := context.Background()
	for _, credentials := range []bool{false, true} {
		for _, transition := range []string{"enable", "disable", "rename", "select key", "rotate bytes"} {
			t.Run(transition+"/credentials="+strconv.FormatBool(credentials), func(t *testing.T) {
				fg := pitrRolloutFG(credentials)
				enc := fg.Spec.Backup.Profiles[0].Encryption.DeepCopy()
				if transition == "enable" {
					fg.Spec.Backup.Profiles[0].Encryption = nil
				}
				key := bytes.Repeat([]byte{1}, digestKeyBytes)
				sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "archive-passphrase", Namespace: fg.Namespace}, Data: map[string][]byte{"passphrase": bytes.Repeat([]byte("a"), 32), "next": bytes.Repeat([]byte("b"), 32)}}
				digestKey := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: digestKeySecretName(fg.Name), Namespace: fg.Namespace}, Data: map[string][]byte{digestKeyDataKey: key}}
				c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(sec, digestKey).Build()
				inputs, err := specSecretInputs(ctx, c, fg)
				if err != nil {
					t.Fatal(err)
				}
				for _, site := range fg.Spec.Sites {
					deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: resourceName(fg.Name, site.Name), Namespace: fg.Namespace, Annotations: map[string]string{specHashAnnotation: ComputeSpecHash(fg, site, secretContentDigest(key, inputs))}}}
					if err := c.Create(ctx, deploy); err != nil {
						t.Fatal(err)
					}
				}
				runner := &TopologyManagerRunner{client: c, logger: testLogger()}
				tm := &TopologyManager{}
				runner.checkSpecDrift(ctx, fg, tm)
				if len(tm.specDriftSites) != 0 {
					t.Fatalf("unexpected initial drift: %v", tm.specDriftSites)
				}
				switch transition {
				case "enable":
					fg.Spec.Backup.Profiles[0].Encryption = enc
				case "disable":
					fg.Spec.Backup.Profiles[0].Encryption = nil
				case "rename":
					fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name = "new-passphrase"
				case "select key":
					fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Key = "next"
				case "rotate bytes":
					sec.Data["passphrase"] = bytes.Repeat([]byte("c"), 32)
					if err := c.Update(ctx, sec); err != nil {
						t.Fatal(err)
					}
				}
				runner.checkSpecDrift(ctx, fg, tm)
				if strings.Join(tm.specDriftSites, ",") != "dc1,dc2" {
					t.Fatalf("encryption change queued %v, want both sites", tm.specDriftSites)
				}
				// Applying the new desired spec uses the existing Deployment renderer.
				objects := []client.Object{fg, sec}
				for _, credential := range newTestCredentialSecrets() {
					objects = append(objects, credential)
				}
				r, _ := newReconciler(objects...)
				reconcileOnce(t, r)
				pod := getDeployment(t, r, "dc1").Spec.Template.Spec
				vol := findVolume(pod.Volumes, "pitr-passphrase")
				if transition == "disable" {
					if vol != nil {
						t.Fatal("disabled encryption still projects a passphrase")
					}
				} else if vol == nil || vol.Secret == nil || vol.Secret.SecretName != fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name {
					t.Fatal("renderer did not install the desired passphrase mount")
				}
			})
		}
	}
}
