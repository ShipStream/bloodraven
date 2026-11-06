// Package pitrarchive holds the identity contract of a PITR binlog archive
// (design WS-D §3.1-§3.3, D-1 to D-7, D-18, D-27, D-34, D-43):
//
//   - Destination: the normalized, injective location of an archive
//     (s3://… or pvc://…), its D-18 bounds, and ParseDestination;
//   - ArchiveIDDerivations: the versioned archive ID derivation (D-2);
//   - Descriptor: the write-once plaintext <keyPrefix>/archive.json, its
//     canonical JSON, and its keyCheck (built by internal/backupcrypto);
//   - VerifyDescriptor: the role-specific checks, returning *VerifyError
//     carrying a Token;
//   - Token: the one catalog of PITR refusal tokens (§7.1), and the
//     PITRArchiveReady reasons derived from them (D-27);
//   - the wire types shared by the sidecar and its clients: ArchiveStatus,
//     PromotionRecord and HistoryIntegrity.
//
// The package is pure and deliberately free of the Kubernetes API types, so
// the sidecar binary can import it without linking k8s.io/api. Building a
// Destination or EncryptionIdentity from a v1alpha1 profile lives in the
// subpackage pitrprofile.
package pitrarchive
