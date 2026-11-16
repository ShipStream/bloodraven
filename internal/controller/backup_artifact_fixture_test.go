package controller

import v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"

// The fixtures below record a backup's status.artifactStorage as its
// producing Job's launch would: for S3 the bucket, endpoint and region of
// fgWithBackup's nightly-s3 profile, for PVC the claim the backup's
// profile owns.

const (
	testArtifactBucket   = "bloodraven-backups"
	testArtifactEndpoint = "https://minio.local"
	testArtifactRegion   = "us-east-1"
)

// testArtifactStorage is a recorded destination of type typ at location.
func testArtifactStorage(typ v1beta1.BackupStorageType, location string) *v1beta1.BackupArtifactStorage {
	st := &v1beta1.BackupArtifactStorage{Location: location}
	applyTestArtifactType(st, typ, "lion", "daily-local")
	if bucket, _ := splitS3Location(location); bucket != "" && typ == v1beta1.BackupStorageS3 {
		st.Bucket = bucket
	}
	return st
}

// testArtifactStorageWithKey is testArtifactStorage with the dump key the
// producing Job mounted.
func testArtifactStorageWithKey(typ v1beta1.BackupStorageType, location string, key *v1beta1.SecretKeyRef) *v1beta1.BackupArtifactStorage {
	st := testArtifactStorage(typ, location)
	st.PassphraseSecretKeyRef = key
	return st
}

func applyTestArtifactType(st *v1beta1.BackupArtifactStorage, typ v1beta1.BackupStorageType, group, profile string) {
	st.Type = typ
	switch typ {
	case v1beta1.BackupStorageS3:
		st.Bucket, st.EndpointURL, st.Region, st.ClaimName = testArtifactBucket, testArtifactEndpoint, testArtifactRegion, ""
	case v1beta1.BackupStoragePVC:
		st.Bucket, st.EndpointURL, st.Region = "", "", ""
		st.ClaimName = ownedBackupPVCName(group, profile)
	}
}

// ensureTestArtifact returns b's recorded destination, creating an S3 one.
func ensureTestArtifact(b *v1beta1.MysqlBackup) *v1beta1.BackupArtifactStorage {
	if b.Status.ArtifactStorage == nil {
		b.Status.ArtifactStorage = &v1beta1.BackupArtifactStorage{}
		applyTestArtifactType(b.Status.ArtifactStorage, v1beta1.BackupStorageS3, b.Spec.FailoverGroupRef.Name, b.Spec.ProfileName)
	}
	return b.Status.ArtifactStorage
}

// setTestArtifactLocation records location as b's artifact location. An
// encrypted S3 location (s3://bucket/key/) also records its bucket, as the
// producing Job's env does.
func setTestArtifactLocation(b *v1beta1.MysqlBackup, location string) {
	st := ensureTestArtifact(b)
	st.Location = location
	if bucket, _ := splitS3Location(location); bucket != "" && st.Type == v1beta1.BackupStorageS3 {
		st.Bucket = bucket
	}
}

// setTestArtifactType records typ as b's storage type, with that
// backend's destination fields.
func setTestArtifactType(b *v1beta1.MysqlBackup, typ v1beta1.BackupStorageType) {
	applyTestArtifactType(ensureTestArtifact(b), typ, b.Spec.FailoverGroupRef.Name, b.Spec.ProfileName)
}

// testArtifactLocation is b's recorded artifact location, "" without one.
func testArtifactLocation(b *v1beta1.MysqlBackup) string {
	if b.Status.ArtifactStorage == nil {
		return ""
	}
	return b.Status.ArtifactStorage.Location
}

// testArtifactType is b's recorded storage type, "" without one.
func testArtifactType(b *v1beta1.MysqlBackup) v1beta1.BackupStorageType {
	if b.Status.ArtifactStorage == nil {
		return ""
	}
	return b.Status.ArtifactStorage.Type
}
