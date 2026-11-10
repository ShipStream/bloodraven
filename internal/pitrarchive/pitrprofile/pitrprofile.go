// Package pitrprofile builds pitrarchive values from v1alpha1 backup
// profiles. It is split from pitrarchive so that the sidecar, which never
// sees a profile, does not link the Kubernetes API types.
//
// Every caller that needs a profile's archive destination uses
// DestinationFor: the PITR sidecar renderer, the restore and verification
// builders and (#203) the bindings. The standby reconciler does not: its
// source is the standby spec's S3 storage, not a BackupProfile, so it calls
// pitrarchive.NewS3Destination directly.
package pitrprofile

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// DestinationFor returns the normalized archive destination of profile p in
// group fg (§3.1). For PVC the claim is the one ClaimFor resolves, and the
// namespace is fg's.
func DestinationFor(fg *v1alpha1.MysqlFailoverGroup, p *v1alpha1.BackupProfile) (pitrarchive.Destination, error) {
	if fg == nil || p == nil {
		return pitrarchive.Destination{}, fmt.Errorf("%w: nil group or profile", pitrarchive.ErrInvalidDestination)
	}
	switch p.Storage.Type {
	case v1alpha1.BackupStorageS3:
		s3 := p.Storage.S3
		if s3 == nil {
			return pitrarchive.Destination{}, fmt.Errorf("%w: profile %q type=S3 but storage.s3 is nil", pitrarchive.ErrInvalidDestination, p.Name)
		}
		d, err := pitrarchive.NewS3Destination(s3.Bucket, s3.Prefix, s3.EndpointURL)
		if err != nil {
			return pitrarchive.Destination{}, fmt.Errorf("profile %q: %w", p.Name, err)
		}
		return d, nil
	case v1alpha1.BackupStoragePVC:
		pvc := p.Storage.PVC
		if pvc == nil {
			return pitrarchive.Destination{}, fmt.Errorf("%w: profile %q type=PVC but storage.pvc is nil", pitrarchive.ErrInvalidDestination, p.Name)
		}
		claim, _, err := ClaimFor(fg.Name, p.Name, pvc)
		if err != nil {
			return pitrarchive.Destination{}, fmt.Errorf("%w: %w", pitrarchive.ErrInvalidDestination, err)
		}
		d, err := pitrarchive.NewPVCDestination(fg.Namespace, claim, pvc.SubPath)
		if err != nil {
			return pitrarchive.Destination{}, fmt.Errorf("profile %q: %w", p.Name, err)
		}
		return d, nil
	default:
		return pitrarchive.Destination{}, fmt.Errorf("%w: profile %q has unknown storage type %q", pitrarchive.ErrInvalidDestination, p.Name, p.Storage.Type)
	}
}

// EncryptionIdentityFor returns profile p's archive encryption identity
// (D-7): {None, ""} when encryption is not configured, else {Passphrase,
// algorithm (AES-256-GCM when unset)}. Unlike
// BackupEncryptionSpec.AlgorithmOrDefault, a nil spec never yields an
// algorithm (F30).
func EncryptionIdentityFor(p *v1alpha1.BackupProfile) pitrarchive.EncryptionIdentity {
	if !p.EncryptionEnabled() {
		return pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}
	}
	return pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: string(p.Encryption.AlgorithmOrDefault())}
}

// ClaimFor resolves the claim of PVC profile profileName in group fgName.
// It is the one rule every consumer uses, so the claim the operator
// provisions, the claim a backup Job writes and the claim the PITR archive
// mounts cannot disagree (A-18): pvc.claimName when it is set; else the
// operator-owned OwnedBackupPVCName when pvc.provisioned is set (owned is
// then true); else an error. Admission requires exactly one of the two, so
// the error is reached only by an object stored before that rule, and it
// names the fix instead of mounting a claim nobody provisions.
func ClaimFor(fgName, profileName string, pvc *v1alpha1.PVCStorage) (claim string, owned bool, err error) {
	switch {
	case pvc == nil:
		return "", false, fmt.Errorf("profile %q has no storage.pvc", profileName)
	case pvc.ClaimName != "" && pvc.Provisioned != nil:
		return "", false, fmt.Errorf("profile %q sets both storage.pvc.claimName and storage.pvc.provisioned; set exactly one", profileName)
	case pvc.ClaimName != "":
		return pvc.ClaimName, false, nil
	case pvc.Provisioned != nil:
		return OwnedBackupPVCName(fgName, profileName), true, nil
	default:
		return "", false, fmt.Errorf("profile %q sets neither storage.pvc.claimName nor storage.pvc.provisioned; set exactly one", profileName)
	}
}

// OwnedBackupPVCName is the name of the PVC the operator provisions for a
// PVC profile with pvc.provisioned: "mysql-<fg>-backup-<profile>", truncated to
// a DNS-1123 label. internal/controller's ownedBackupPVCName delegates here
// so the destination and the provisioned PVC can never disagree.
func OwnedBackupPVCName(fgName, profileName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-backup-%s", fgName, profileName))
}

// truncateDNS1123 matches internal/controller.truncateDNS1123: names over 63
// characters keep a 54-character body plus "-<8 hex of sha256(name)>", and
// a trailing non-alphanumeric is trimmed.
func truncateDNS1123(name string) string {
	if len(name) <= 63 {
		return trimDNS1123Tail(name)
	}
	const reserve = 9
	sum := sha256.Sum256([]byte(name))
	return trimDNS1123Tail(name[:63-reserve]) + "-" + hex.EncodeToString(sum[:])[:8]
}

func trimDNS1123Tail(s string) string {
	for len(s) > 0 {
		c := s[len(s)-1]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			return s
		}
		s = s[:len(s)-1]
	}
	return s
}
