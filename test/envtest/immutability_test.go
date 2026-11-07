//go:build envtest

package envtest

// Admission tests for WS-E E6: name bounds and grammar (A-15), site
// removal refusal (A-16), site storage and zone immutability (A-17), the
// backup PVC union (A-18), loadBalancerIP (A-19), MysqlBackup and
// MysqlBackupVerification execution fields (A-22), initFromBackup fixed at
// creation (A-23), encryption cannot be turned off (A-24), the confirm
// timestamp (A-26), and house rule T-1: the operator's own typed writes of
// non-canonical values are accepted. The CRDs installing at all is the CEL
// cost check; CI runs this file on the 1.31 floor too, where status writes
// are validated without ratcheting.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// e6Case is one transition rule: an object is created from build, an
// update that leaves the guarded fields alone is accepted (a metadata
// write and a status write), and change is refused with fragment.
type e6Case struct {
	name     string
	build    func(ns, name string) client.Object
	change   func(client.Object)
	fragment string
}

func e6FG(mutate func(*v1alpha1.MysqlFailoverGroup)) func(ns, name string) client.Object {
	return func(ns, name string) client.Object {
		fg := newTestFG(ns)
		fg.Name = name
		if mutate != nil {
			mutate(fg)
		}
		return fg
	}
}

func e6OnFG(f func(*v1alpha1.MysqlFailoverGroup)) func(client.Object) {
	return func(o client.Object) { f(o.(*v1alpha1.MysqlFailoverGroup)) }
}

func e6Backup(mutate func(*v1alpha1.MysqlBackup)) func(ns, name string) client.Object {
	return func(ns, name string) client.Object {
		b := &v1alpha1.MysqlBackup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
		}
		if mutate != nil {
			mutate(b)
		}
		return b
	}
}

func e6OnBackup(f func(*v1alpha1.MysqlBackup)) func(client.Object) {
	return func(o client.Object) { f(o.(*v1alpha1.MysqlBackup)) }
}

func e6Verification(mutate func(*v1alpha1.MysqlBackupVerification)) func(ns, name string) client.Object {
	return func(ns, name string) client.Object {
		v := e8Verification(ns, name)
		if mutate != nil {
			mutate(v)
		}
		return v
	}
}

func e6OnVerification(f func(*v1alpha1.MysqlBackupVerification)) func(client.Object) {
	return func(o client.Object) { f(o.(*v1alpha1.MysqlBackupVerification)) }
}

// e6StatusPatch is a status write that changes nothing in spec, per kind.
func e6StatusPatch(o client.Object) []byte {
	switch o.(type) {
	case *v1alpha1.MysqlFailoverGroup:
		return []byte(`{"status":{"observedGeneration":1}}`)
	default:
		return []byte(`{"status":{"phase":"Pending"}}`)
	}
}

// e6Fresh reads obj back into a new object of its type.
func e6Fresh(t *testing.T, obj client.Object) client.Object {
	t.Helper()
	fresh := obj.DeepCopyObject().(client.Object)
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func e6Run(t *testing.T, ns string, cases []e6Case) {
	t.Helper()
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := tc.build(ns, fmt.Sprintf("t%d", i))
			if err := k8sClient.Create(ctx, obj); err != nil {
				t.Fatalf("create: %v", err)
			}
			unchanged := e6Fresh(t, obj)
			labels := unchanged.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels["e6"] = "unchanged"
			unchanged.SetLabels(labels)
			if err := k8sClient.Update(ctx, unchanged); err != nil {
				t.Fatalf("an update that leaves the spec unchanged was refused: %v", err)
			}
			if err := k8sClient.Status().Patch(ctx, e6Fresh(t, obj), client.RawPatch(types.MergePatchType, e6StatusPatch(obj))); err != nil {
				t.Fatalf("a status write was refused: %v", err)
			}
			changed := e6Fresh(t, obj)
			tc.change(changed)
			e4WantInvalid(t, tc.name, k8sClient.Update(ctx, changed), tc.fragment)
		})
	}
}

// e6WithInit gives the group an initFromBackup block with every field
// the rule compares.
func e6WithInit(fg *v1alpha1.MysqlFailoverGroup) {
	// The archive profile the restore reads must exist when the group is
	// created (E16).
	if fg.Spec.Backup == nil {
		fg.Spec.Backup = &v1alpha1.BackupSpec{}
	}
	fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, pitrS3Profile("nightly-s3", "orders", ""))
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source:      v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "orders-nightly"}},
		LoadOptions: &v1alpha1.LoadOptions{Threads: 4, ExcludeSchemas: []string{"scratch"}},
		Decryption:  &v1alpha1.BackupDecryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "old-key"}},
		PointInTime: &v1alpha1.PointInTimeSpec{
			ArchiveProfileName: "nightly-s3",
			StopTime:           metav1.Time{Time: time.Date(2026, 4, 15, 9, 30, 0, 0, time.UTC)},
			ExcludeGtids:       "3e11fa47-71ca-11e1-9e33-c80aa9429562:5",
		},
	}
}

func e6WithS3Init(fg *v1alpha1.MysqlFailoverGroup) {
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.RestoreSource{S3: &v1alpha1.S3Storage{Bucket: "b", Prefix: "orders", Region: "us-east-1", CredentialsSecretName: "s3"}},
	}
}

func e6WithEncryption(fg *v1alpha1.MysqlFailoverGroup) {
	fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: "mysql-tls"}
	fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
}

func e6WithPVCProfile(pvc v1alpha1.PVCStorage) func(*v1alpha1.MysqlFailoverGroup) {
	return func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Backup = &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{{
			Name:    "local",
			Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStoragePVC, PVC: pvc.DeepCopy()},
		}}}
	}
}

func e6WithPITRProfile(fg *v1alpha1.MysqlFailoverGroup) {
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		Profiles: []v1alpha1.BackupProfile{{
			Name:       "archive",
			Storage:    v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{Bucket: "b", Prefix: "orders", CredentialsSecretName: "s3"}},
			Encryption: &v1alpha1.BackupEncryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "archive-key"}},
		}},
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "archive"},
	}
}

func e6Provisioned(class string, size v1alpha1.StorageQuantity) v1alpha1.PVCStorage {
	p := &v1alpha1.ProvisionedVolumeSpec{Size: size}
	if class != "" {
		p.StorageClassName = &class
	}
	return v1alpha1.PVCStorage{Provisioned: p}
}

// TestE6_TransitionRules: every MysqlFailoverGroup, MysqlBackup and
// MysqlBackupVerification transition rule accepts an unchanged update and
// a status write, and refuses the change it guards.
func TestE6_TransitionRules(t *testing.T) {
	ns := createNamespace(t, "e6-transitions")
	withReader := func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader", ServerID: 103, Role: v1alpha1.SiteRoleReadOnly,
			Zone: "lion-reader", Storage: fg.Spec.Sites[0].Storage})
	}
	const (
		sites      = "spec.sites cannot be removed or renamed"
		initFixed  = "spec.initFromBackup is fixed at creation"
		encOff     = "spec.encryptionAtRest.enabled cannot be turned off"
		keyring    = "cannot change while encryption is enabled"
		pitrEnc    = "cannot change while that profile archives PITR binlogs"
		pvc        = "pvc.provisioned is immutable"
		pvcUnion   = "exactly one of claimName or provisioned must be set"
		groupRef   = "spec.failoverGroupRef is immutable"
		profile    = "spec.profileName is immutable"
		sourceSite = "spec.sourceSite is immutable"
		backupRef  = "spec.backupRef is immutable"
		pointInT   = "spec.pointInTime is immutable"
		sanity     = "spec.sanityCheck is immutable"
	)
	cases := []e6Case{
		// A-16
		{"site removed", e6FG(withReader), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites = fg.Spec.Sites[:2] }), sites},
		{"site renamed", e6FG(withReader), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites[2].Name = "reader2" }), sites},
		// A-17
		{"zone", e6FG(nil), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites[0].Zone = "lion-dc3" }), "zone is immutable"},
		{"storageClassName", e6FG(nil), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites[0].Storage.StorageClassName = "fast" }), "storageClassName is immutable"},
		{"size", e6FG(nil), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites[0].Storage.Size = "20Gi" }), "size is immutable"},
		// A-23
		{"initFromBackup added", e6FG(nil), e6OnFG(e6WithInit), initFixed},
		{"initFromBackup removed", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.InitFromBackup = nil }), initFixed},
		{"initFromBackup backupRef", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Source.BackupRef.Name = "orders-weekly"
		}), initFixed},
		{"initFromBackup source backend", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Source = v1alpha1.RestoreSource{PVC: &v1alpha1.InitFromBackupPVCSource{ClaimName: "dumps"}}
		}), initFixed},
		{"initFromBackup s3 prefix", e6FG(e6WithS3Init), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Source.S3.Prefix = "orders-2"
		}), initFixed},
		{"initFromBackup s3 region removed", e6FG(e6WithS3Init), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Source.S3.Region = ""
		}), initFixed},
		{"initFromBackup decryption", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Decryption.PassphraseSecretKeyRef.Name = "new-key"
		}), initFixed},
		{"initFromBackup decryption removed", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.Decryption = nil
		}), initFixed},
		{"initFromBackup loadOptions threads", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.LoadOptions.Threads = 8
		}), initFixed},
		{"initFromBackup loadOptions skipBinlog", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.LoadOptions.SkipBinlog = new(false)
		}), initFixed},
		{"initFromBackup loadOptions excludeSchemas", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.LoadOptions.ExcludeSchemas = nil
		}), initFixed},
		{"initFromBackup stopTime", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.StopTime = metav1.Time{Time: time.Date(2026, 4, 15, 9, 30, 1, 0, time.UTC)}
		}), initFixed},
		{"initFromBackup allowPartial", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.AllowPartial = true
		}), initFixed},
		{"initFromBackup excludeGtids", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.ExcludeGtids = ""
		}), initFixed},
		{"initFromBackup archiveProfileName", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.ArchiveProfileName = "weekly-s3"
		}), initFixed},
		{"initFromBackup pointInTime removed", e6FG(e6WithInit), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime = nil
		}), initFixed},
		// A-24
		{"encryption disabled", e6FG(e6WithEncryption), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.EncryptionAtRest.Enabled = false }), encOff},
		{"encryption removed", e6FG(e6WithEncryption), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.EncryptionAtRest = nil }), encOff},
		{"keyring dataFileDir", e6FG(e6WithEncryption), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{DataFileDir: "/run/keyring-2"}
		}), keyring},
		{"keyring pluginDir", e6FG(e6WithEncryption), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{PluginDir: "/usr/lib/mysql/plugin"}
		}), keyring},
		{"keyring mysqldDir", e6FG(e6WithEncryption), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{MysqldDir: "/usr/local/sbin"}
		}), keyring},
		// #203, on the kind root (A-25)
		{"PITR profile encryption", e6FG(e6WithPITRProfile), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecretKeyRef.Name = "other-key"
		}), pitrEnc},
		// A-18 (R04: removal is refused, so it cannot be re-added with new values)
		{"provisioned size", e6FG(e6WithPVCProfile(e6Provisioned("fast", "10Gi"))), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC.Provisioned.Size = "20Gi"
		}), pvc},
		{"provisioned class", e6FG(e6WithPVCProfile(e6Provisioned("fast", "10Gi"))), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC.Provisioned.StorageClassName = new("slow")
		}), pvc},
		{"provisioned class removed", e6FG(e6WithPVCProfile(e6Provisioned("fast", "10Gi"))), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC.Provisioned.StorageClassName = nil
		}), pvc},
		{"provisioned class added", e6FG(e6WithPVCProfile(e6Provisioned("", "10Gi"))), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC.Provisioned.StorageClassName = new("fast")
		}), pvc},
		{"provisioned removed", e6FG(e6WithPVCProfile(e6Provisioned("fast", "10Gi"))), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC.Provisioned = nil
		}), pvcUnion},
		{"provisioned to claimName", e6FG(e6WithPVCProfile(e6Provisioned("fast", "10Gi"))), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC = &v1alpha1.PVCStorage{ClaimName: "backups"}
		}), pvc},
		{"claimName to provisioned", e6FG(e6WithPVCProfile(v1alpha1.PVCStorage{ClaimName: "backups"})), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			pvc := e6Provisioned("fast", "10Gi")
			fg.Spec.Backup.Profiles[0].Storage.PVC = &pvc
		}), pvc},
		// A-22, MysqlBackup
		{"backup failoverGroupRef", e6Backup(nil), e6OnBackup(func(b *v1alpha1.MysqlBackup) { b.Spec.FailoverGroupRef.Name = "tiger" }), groupRef},
		{"backup profileName", e6Backup(nil), e6OnBackup(func(b *v1alpha1.MysqlBackup) { b.Spec.ProfileName = "weekly" }), profile},
		{"backup sourceSite added", e6Backup(nil), e6OnBackup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "dc2" }), sourceSite},
		{"backup sourceSite changed", e6Backup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "dc1" }), e6OnBackup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "dc2" }), sourceSite},
		{"backup sourceSite removed", e6Backup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "dc1" }), e6OnBackup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "" }), sourceSite},
		// A-22, MysqlBackupVerification
		{"verification failoverGroupRef", e6Verification(nil), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.FailoverGroupRef.Name = "tiger" }), groupRef},
		{"verification profileName", e6Verification(nil), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.ProfileName = "weekly" }), profile},
		{"verification backupRef added", e6Verification(nil), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.BackupRef = &v1alpha1.LocalMysqlBackupRef{Name: "b1"}
		}), backupRef},
		{"verification backupRef changed", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.BackupRef = &v1alpha1.LocalMysqlBackupRef{Name: "b1"}
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.BackupRef.Name = "b2" }), backupRef},
		{"verification backupRef removed", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.BackupRef = &v1alpha1.LocalMysqlBackupRef{Name: "b1"}
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.BackupRef = nil }), backupRef},
		{"verification pointInTime added", e6Verification(nil), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime = e8VerificationPointInTime(v1alpha1.PointInTimeModeLatest)
		}), pointInT},
		{"verification pointInTime removed", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime = e8VerificationPointInTime(v1alpha1.PointInTimeModeLatest)
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.PointInTime = nil }), pointInT},
		{"verification pointInTime mode", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime = e8VerificationPointInTime(v1alpha1.PointInTimeModeLatest)
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime = e8VerificationPointInTime(v1alpha1.PointInTimeModeTimestamp)
		}), pointInT},
		{"verification stopTime", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime = e8VerificationPointInTime(v1alpha1.PointInTimeModeTimestamp)
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime.StopTime = &metav1.Time{Time: v.Spec.PointInTime.StopTime.Add(time.Second)}
		}), pointInT},
		{"verification excludeGtids", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime = e8VerificationPointInTime(v1alpha1.PointInTimeModeLatest)
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime.ExcludeGtids = "3e11fa47-71ca-11e1-9e33-c80aa9429562:5"
		}), pointInT},
		{"verification archiveProfileName", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime = e8VerificationPointInTime(v1alpha1.PointInTimeModeLatest)
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.PointInTime.ArchiveProfileName = "retained"
		}), pointInT},
		{"verification sanityCheck added", e6Verification(nil), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1"}
		}), sanity},
		{"verification sanityCheck removed", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1"}
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.SanityCheck = nil }), sanity},
		{"verification sanityCheck query", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1"}
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.SanityCheck.Query = "SELECT 2" }), sanity},
		{"verification sanityCheck minResult", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1", MinResult: new(int64(0))}
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.SanityCheck.MinResult = nil }), sanity},
		{"verification sanityCheck timeout", e6Verification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1"}
		}), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.SanityCheck.Timeout = e8Duration(2 * time.Minute) }), sanity},
	}
	e6Run(t, ns, cases)
}

// TestE6_AllowedUpdates: the updates the rules leave open are accepted.
func TestE6_AllowedUpdates(t *testing.T) {
	ns := createNamespace(t, "e6-allowed")
	for _, tc := range []struct {
		name   string
		build  func(ns, name string) client.Object
		change func(client.Object)
	}{
		{"site added", e6FG(nil), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader", ServerID: 103, Role: v1alpha1.SiteRoleReadOnly,
				Zone: "lion-reader", Storage: fg.Spec.Sites[0].Storage})
		})},
		{"sites reordered", e6FG(nil), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Sites[0], fg.Spec.Sites[1] = fg.Spec.Sites[1], fg.Spec.Sites[0]
		})},
		{"size respelled", e6FG(nil), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites[0].Storage.Size = "10240Mi" })},
		{"provisioned size respelled", e6FG(e6WithPVCProfile(e6Provisioned("fast", "10Gi"))), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC.Provisioned.Size = "10240Mi"
		})},
		{"claimName subPath", e6FG(e6WithPVCProfile(v1alpha1.PVCStorage{ClaimName: "backups"})), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Storage.PVC.SubPath = "orders"
		})},
		{"encryption enabled", e6FG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: "mysql-tls"} }),
			e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
			})},
		{"keyring changed while disabled", e6FG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{}
		}), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{PluginDir: "/usr/lib/mysql/plugin"}
		})},
		{"keyring default spelled out", e6FG(e6WithEncryption), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{DataFileDir: "/run/mysql-keyring", MysqldDir: "/usr/sbin", PluginDir: "/usr/lib64/mysql/plugin"}
		})},
		{"encryption coverage", e6FG(e6WithEncryption), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.EncryptionAtRest.Coverage = &v1alpha1.EncryptionCoverageSpec{SystemTablespace: new(false)}
		})},
		{"restoreInPlace added", e6FG(nil), e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "2026-01-01T00:00:00Z",
				Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "b"}}}
		})},
		{"backup deletionPolicy", e6Backup(nil), e6OnBackup(func(b *v1alpha1.MysqlBackup) { b.Spec.DeletionPolicy = v1alpha1.DeletionPolicyRetain })},
		{"verification keepOnFailure and TTL", e6Verification(nil), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.KeepOnFailure = new(false)
			v.Spec.TTLSecondsAfterFinished = new(int32(0))
		})},
		{"verification resources", e6Verification(nil), e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
			v.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := tc.build(ns, strings.ToLower(strings.ReplaceAll(tc.name, " ", "-")))
			if err := k8sClient.Create(ctx, obj); err != nil {
				t.Fatalf("create: %v", err)
			}
			fresh := e6Fresh(t, obj)
			tc.change(fresh)
			if err := k8sClient.Update(ctx, fresh); err != nil {
				t.Fatalf("refused: %v", err)
			}
		})
	}
}

// TestE6_TypedWritesKeepNonCanonicalValues is house rule T-1: an object
// written with non-canonical values (a Quantity spelled 1.5Gi or 1024Mi, a
// Duration 60s, a stopTime with a +05:00 offset, an explicit false or
// empty value the typed client drops) survives the operator's own typed
// finalizer Update and an annotation consumer's typed Update, and a real
// change to each field is still refused.
func TestE6_TypedWritesKeepNonCanonicalValues(t *testing.T) {
	ns := createNamespace(t, "e6-typed-writes")

	fgU := e4Unstructured(t, newTestFG(ns), "MysqlFailoverGroup")
	fgU.SetName("canon")
	sites, _, _ := unstructured.NestedSlice(fgU.Object, "spec", "sites")
	for i, size := range []string{"1.5Gi", "1024Mi"} {
		_ = unstructured.SetNestedField(sites[i].(map[string]any), size, "storage", "size")
	}
	if err := unstructured.SetNestedSlice(fgU.Object, sites, "spec", "sites"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(fgU.Object, map[string]any{
		"profiles": []any{map[string]any{
			"name":    "local",
			"storage": map[string]any{"type": "PVC", "pvc": map[string]any{"provisioned": map[string]any{"size": "1.5Gi"}, "subPath": ""}},
		}},
	}, "spec", "backup"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(fgU.Object, map[string]any{
		"source": map[string]any{"s3": map[string]any{"bucket": "b", "prefix": "", "region": "", "credentialsSecretName": "s3"}},
		"loadOptions": map[string]any{
			"excludeSchemas": []any{},
			"includeSchemas": []any{},
		},
		"pointInTime": map[string]any{
			"archiveProfileName": "local",
			"allowPartial":       false,
			"stopTime":           "2026-04-15T14:30:00+05:00",
			"excludeGtids":       "",
		},
	}, "spec", "initFromBackup"); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Create(ctx, fgU); err != nil {
		t.Fatalf("create the group with non-canonical values: %v", err)
	}

	mbvU := e4Unstructured(t, e8Verification(ns, "canon"), "MysqlBackupVerification")
	if err := unstructured.SetNestedField(mbvU.Object, map[string]any{"query": "SELECT 1", "timeout": "60s", "minResult": int64(0)}, "spec", "sanityCheck"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(mbvU.Object, map[string]any{"mode": "Timestamp", "stopTime": "2026-04-15T14:30:00+05:00", "excludeGtids": ""}, "spec", "pointInTime"); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Create(ctx, mbvU); err != nil {
		t.Fatalf("create the verification with non-canonical values: %v", err)
	}

	mbk := e6Backup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "dc1" })(ns, "canon")
	if err := k8sClient.Create(ctx, mbk); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		obj       client.Object
		finalizer string
		changes   []func(client.Object)
	}{
		{&v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "canon"}}, v1alpha1.FailoverGroupFinalizer, []func(client.Object){
			e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites[0].Storage.Size = "2Gi" }),
			e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Profiles[0].Storage.PVC.Provisioned.Size = "2Gi" }),
			e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.InitFromBackup.PointInTime.StopTime = metav1.Time{Time: fg.Spec.InitFromBackup.PointInTime.StopTime.Add(time.Second)}
			}),
			e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.InitFromBackup.PointInTime.AllowPartial = true }),
			e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.InitFromBackup.Source.S3.Prefix = "orders" }),
			e6OnFG(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.InitFromBackup.LoadOptions.ExcludeSchemas = []string{"scratch"}
			}),
		}},
		{&v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "canon"}}, v1alpha1.MysqlBackupVerificationFinalizer, []func(client.Object){
			e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.SanityCheck.Timeout = e8Duration(61 * time.Second) }),
			e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) { v.Spec.SanityCheck.MinResult = new(int64(1)) }),
			e6OnVerification(func(v *v1alpha1.MysqlBackupVerification) {
				v.Spec.PointInTime.StopTime = &metav1.Time{Time: v.Spec.PointInTime.StopTime.Add(-time.Second)}
			}),
		}},
		{&v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "canon"}}, v1alpha1.MysqlBackupFinalizer, []func(client.Object){
			e6OnBackup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "dc2" }),
		}},
	} {
		kind := typeName(tc.obj)
		// The finalizer add, as the reconcilers do it: a typed Update.
		typed := e6Fresh(t, tc.obj)
		controllerutil.AddFinalizer(typed, tc.finalizer)
		if err := k8sClient.Update(ctx, typed); err != nil {
			t.Fatalf("%s: the typed finalizer Update was refused: %v", kind, err)
		}
		// An annotation consumer: the annotation is set, then removed by a
		// typed Update of the object it was read with.
		annotated := e6Fresh(t, tc.obj)
		annotated.SetAnnotations(map[string]string{"bloodraven.shipstream.io/e6-request": "x"})
		if err := k8sClient.Update(ctx, annotated); err != nil {
			t.Fatalf("%s: a typed annotation Update was refused: %v", kind, err)
		}
		consumed := e6Fresh(t, tc.obj)
		consumed.SetAnnotations(nil)
		if err := k8sClient.Update(ctx, consumed); err != nil {
			t.Fatalf("%s: the annotation consumer's typed Update was refused: %v", kind, err)
		}
		if err := k8sClient.Status().Patch(ctx, e6Fresh(t, tc.obj), client.RawPatch(types.MergePatchType, e6StatusPatch(tc.obj))); err != nil {
			t.Fatalf("%s: a status write was refused: %v", kind, err)
		}
		for i, change := range tc.changes {
			changed := e6Fresh(t, tc.obj)
			change(changed)
			if err := k8sClient.Update(ctx, changed); err == nil {
				t.Fatalf("%s: change %d was accepted after the typed writes", kind, i)
			}
		}
	}
}

func typeName(o client.Object) string {
	switch o.(type) {
	case *v1alpha1.MysqlFailoverGroup:
		return "MysqlFailoverGroup"
	case *v1alpha1.MysqlBackup:
		return "MysqlBackup"
	default:
		return "MysqlBackupVerification"
	}
}

// TestE6_NameBounds is A-15 and R08: site names are DNS labels of at most
// 20 characters; a group's name is a DNS-1035 label and, with each site
// name, at most 41 characters; MysqlBackup and MysqlBackupVerification
// names are at most 63.
func TestE6_NameBounds(t *testing.T) {
	ns := createNamespace(t, "e6-names")
	fg := func(name string, mutate func(*v1alpha1.MysqlFailoverGroup)) error {
		o := newTestFG(ns)
		o.Name = name
		if mutate != nil {
			mutate(o)
		}
		return k8sClient.Create(ctx, o)
	}
	for _, name := range []string{"a.b", "1orders"} {
		e4WantInvalid(t, "group "+name, fg(name, nil), "metadata.name must be a DNS-1035 label")
	}
	// dc1 and dc2 are three characters, so 38 + 3 = 41.
	if err := fg("g"+strings.Repeat("x", 37), nil); err != nil {
		t.Fatalf("a 38-character group with 3-character sites refused: %v", err)
	}
	e4WantInvalid(t, "39-character group", fg("g"+strings.Repeat("x", 38), nil), "at most 41 characters")
	siteName := func(name string) func(*v1alpha1.MysqlFailoverGroup) {
		return func(o *v1alpha1.MysqlFailoverGroup) { o.Spec.Sites[1].Name = name }
	}
	if err := fg("long-site", siteName("s"+strings.Repeat("x", 19))); err != nil {
		t.Fatalf("a 20-character site refused: %v", err)
	}
	for _, s := range []string{"s" + strings.Repeat("x", 20), "-iad", "iad-", "IAD", "i.ad"} {
		e4WantInvalid(t, "site "+s, fg("bad-site", siteName(s)), "spec.sites[1].name")
	}
	e4WantInvalid(t, "sitePriorities item", fg("bad-priority", func(o *v1alpha1.MysqlFailoverGroup) {
		o.Spec.SplitBrainPolicy = &v1alpha1.SplitBrainPolicySpec{SitePriorities: []string{"-dc1"}}
	}), "sitePriorities")

	run := func(name string) []client.Object {
		return []client.Object{e6Backup(nil)(ns, name), e6Verification(nil)(ns, name)}
	}
	for _, o := range run("r" + strings.Repeat("x", 62)) {
		if err := k8sClient.Create(ctx, o); err != nil {
			t.Fatalf("%T with a 63-character name refused: %v", o, err)
		}
	}
	for _, o := range run("r" + strings.Repeat("x", 63)) {
		e4WantInvalid(t, typeName(o)+" with a 64-character name", k8sClient.Create(ctx, o), "metadata.name must be at most 63 characters")
	}
	e4WantInvalid(t, "backup sourceSite", k8sClient.Create(ctx, e6Backup(func(b *v1alpha1.MysqlBackup) { b.Spec.SourceSite = "-dc1" })(ns, "bad-source")), "spec.sourceSite")
}

// TestE6_SiteFieldShapes is A-17 and A-19 at creation: loadBalancerIP is
// an IPv4 address, required unless the site is ReadOnly, and a size is a
// positive quantity string.
func TestE6_SiteFieldShapes(t *testing.T) {
	ns := createNamespace(t, "e6-site-fields")
	fg := func(name string, mutate func(*v1alpha1.MysqlFailoverGroup)) error {
		o := newTestFG(ns)
		o.Name = name
		mutate(o)
		return k8sClient.Create(ctx, o)
	}
	for i, ip := range []string{"256.1.1.1", "2001:db8::1", "not-an-ip"} {
		e4WantInvalid(t, "loadBalancerIP "+ip, fg("bad-ip-"+string(rune('a'+i)), func(o *v1alpha1.MysqlFailoverGroup) { o.Spec.Sites[0].LoadBalancerIP = ip }), "loadBalancerIP")
	}
	e4WantInvalid(t, "candidate without loadBalancerIP", fg("no-ip", func(o *v1alpha1.MysqlFailoverGroup) { o.Spec.Sites[0].LoadBalancerIP = "" }),
		"taintNodeSelector and loadBalancerIP are required unless role is ReadOnly")
	for i, size := range []v1alpha1.StorageQuantity{"0", "-1Gi", "10 Gi"} {
		e4WantInvalid(t, "size "+string(size), fg("bad-size-"+string(rune('a'+i)), func(o *v1alpha1.MysqlFailoverGroup) { o.Spec.Sites[0].Storage.Size = size }), "size")
	}
	u := e4Unstructured(t, newTestFG(ns), "MysqlFailoverGroup")
	u.SetName("int-size")
	sites, _, _ := unstructured.NestedSlice(u.Object, "spec", "sites")
	_ = unstructured.SetNestedField(sites[0].(map[string]any), int64(10737418240), "storage", "size")
	_ = unstructured.SetNestedSlice(u.Object, sites, "spec", "sites")
	e4WantInvalid(t, "integer size", k8sClient.Create(ctx, u), "size")
}

// TestE6_BackupPVCUnion is A-18 at creation: exactly one of claimName and
// provisioned, and provisioned needs a positive size.
func TestE6_BackupPVCUnion(t *testing.T) {
	ns := createNamespace(t, "e6-pvc")
	create := func(name string, pvc v1alpha1.PVCStorage) error {
		return k8sClient.Create(ctx, e6FG(e6WithPVCProfile(pvc))(ns, name))
	}
	e4WantInvalid(t, "empty pvc", create("empty", v1alpha1.PVCStorage{}), "exactly one of claimName or provisioned must be set")
	both := e6Provisioned("fast", "10Gi")
	both.ClaimName = "backups"
	e4WantInvalid(t, "claimName and provisioned", create("both", both), "exactly one of claimName or provisioned must be set")
	e4WantInvalid(t, "zero provisioned size", create("zero", e6Provisioned("", "0")), "greater than 0")
	for name, pvc := range map[string]v1alpha1.PVCStorage{
		"claim":         {ClaimName: "backups", SubPath: "orders"},
		"default-class": e6Provisioned("", "50Gi"),
		"class":         e6Provisioned("fast", "1.5Ti"),
	} {
		if err := create(name, pvc); err != nil {
			t.Fatalf("%s refused: %v", name, err)
		}
	}
}

// TestE6_ConfirmTimestamp is A-26 and F-02: restoreInPlace.confirm is an
// RFC 3339 timestamp with an upper-case T and Z, an offset within
// -23:59..+23:59 and a digits-only fraction, and is kept as written.
func TestE6_ConfirmTimestamp(t *testing.T) {
	ns := createNamespace(t, "e6-confirm")
	create := func(name, confirm string) error {
		fg := newTestFG(ns)
		fg.Name = name
		fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: confirm,
			Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "b"}}}
		return k8sClient.Create(ctx, fg)
	}
	for i, bad := range []string{
		"2026-04-17t14:32:00Z", "2026-04-17T14:32:00z", "2026-04-17T14:32:00+25:00", "2026-04-17T14:32:00+05:99",
		"2026-04-17T14:32:00", "2026-04-17 14:32:00Z", "2026-04-17T14:32:00.Z", "2026-04-17T14:32:00.12aZ", "now",
	} {
		e4WantInvalid(t, "confirm "+bad, create("bad-"+string(rune('a'+i)), bad), "spec.restoreInPlace.confirm")
	}
	for i, good := range []string{"2026-04-17T14:32:00Z", "2026-04-17T14:32:00.123456Z", "2026-04-17T14:32:00+05:30", "2026-04-17T14:32:00-23:59"} {
		name := "good-" + string(rune('a'+i))
		if err := create(name, good); err != nil {
			t.Fatalf("confirm %q refused: %v", good, err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.RestoreInPlace.Confirm != good {
			t.Fatalf("confirm stored as %q, want %q as written", got.Spec.RestoreInPlace.Confirm, good)
		}
	}
}
