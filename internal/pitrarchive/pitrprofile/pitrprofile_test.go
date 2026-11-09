package pitrprofile

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

func testGroup() *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "orders", UID: "uid-1"}}
}

func TestDestinationFor(t *testing.T) {
	fg := testGroup()
	tests := []struct {
		name    string
		profile v1alpha1.BackupProfile
		want    string
		wantErr bool
	}{
		{
			name: "s3 aws",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{Bucket: "backups", Prefix: "/prod/", CredentialsSecretName: "c"}}},
			want: "s3://backups/prod/binlogs?endpoint=aws",
		},
		{
			name: "s3 endpoint",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{Bucket: "b", EndpointURL: "http://MinIO:80/", CredentialsSecretName: "c"}}},
			want: "s3://b/binlogs?endpoint=http%3A%2F%2Fminio",
		},
		{
			name: "pvc explicit claim",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{ClaimName: "shared", SubPath: "a/b"}}},
			want: "pvc://db/shared/a/b/binlogs",
		},
		{
			name: "pvc owned claim",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{}}},
			want: "pvc://db/mysql-orders-backup-pitr/binlogs",
		},
		{
			name: "pvc dot-dot",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStoragePVC, PVC: &v1alpha1.PVCStorage{SubPath: "../x"}}},
			wantErr: true,
		},
		{
			name:    "s3 nil",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3}},
			wantErr: true,
		},
		{
			name:    "pvc nil",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{Type: v1alpha1.BackupStoragePVC}},
			wantErr: true,
		},
		{
			name:    "unknown type",
			profile: v1alpha1.BackupProfile{Name: "pitr", Storage: v1alpha1.BackupStorage{Type: "GCS"}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := DestinationFor(fg, &tt.profile)
			if tt.wantErr {
				if !errors.Is(err, pitrarchive.ErrInvalidDestination) {
					t.Fatalf("DestinationFor = %+v, %v; want ErrInvalidDestination", d, err)
				}
				return
			}
			if err != nil || d.String() != tt.want {
				t.Fatalf("DestinationFor = %q, %v; want %q", d.String(), err, tt.want)
			}
		})
	}
	if _, err := DestinationFor(nil, &v1alpha1.BackupProfile{}); err == nil {
		t.Fatal("nil group accepted")
	}
}

func TestEncryptionIdentityFor(t *testing.T) {
	tests := []struct {
		name string
		p    *v1alpha1.BackupProfile
		want pitrarchive.EncryptionIdentity
	}{
		{"nil profile", nil, pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}},
		{"no encryption", &v1alpha1.BackupProfile{}, pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}},
		{"default algorithm", &v1alpha1.BackupProfile{Encryption: &v1alpha1.BackupEncryptionSpec{
			PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "k"}}},
			pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}},
		{"explicit algorithm", &v1alpha1.BackupProfile{Encryption: &v1alpha1.BackupEncryptionSpec{
			Algorithm: "AES-256-GCM", PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "k"}}},
			pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}},
	}
	for _, tt := range tests {
		got := EncryptionIdentityFor(tt.p)
		if got != tt.want {
			t.Errorf("%s: EncryptionIdentityFor = %+v, want %+v", tt.name, got, tt.want)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("%s: %v", tt.name, err)
		}
	}
}

func TestOwnedBackupPVCName(t *testing.T) {
	if got := OwnedBackupPVCName("orders", "pitr"); got != "mysql-orders-backup-pitr" {
		t.Fatalf("short name = %q", got)
	}
	// Golden for the truncated form (sha256 suffix); matches the
	// controller's former in-package implementation.
	long := OwnedBackupPVCName(strings.Repeat("a", 40), strings.Repeat("b", 40))
	// Cross-checked in Python: body name[:54] with the trailing '-' trimmed,
	// plus "-" + sha256(name)[:8].
	if long != "mysql-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-backup-9a686256" {
		t.Fatalf("long name = %q", long)
	}
	if got := OwnedBackupPVCName(strings.Repeat("a", 40), strings.Repeat("b", 40)); got != long {
		t.Fatal("not deterministic")
	}
}
