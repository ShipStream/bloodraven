package controller

// Shared contract of the PITR archive bindings (#203, IMPL-203 §3-§4): the
// metadata keys, condition, reasons, Events and log messages, and the pure
// helpers the binding engine (pitr_binding*.go), the renderer
// (pitr_render.go, pitr.go, reconciler.go) and the consumers (restore,
// verification, backup) share. The engine writes bindings; the renderer and
// the consumers only read them.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Metadata keys (§3.5).
const (
	// annotationPITRRender is on every site Deployment: the JSON
	// pitrRenderInputs.Canonical() of the PITR identity and access fields it
	// renders. It is the group hold's source (groupPITRHold).
	annotationPITRRender = "bloodraven.shipstream.io/pitr-render"
	// annotationPITRDestination is on every archive key snapshot: the
	// normalized destination (pitrarchive.Destination.String()).
	annotationPITRDestination = "bloodraven.shipstream.io/pitr-destination"
	// annotationPITRAccepted is on an accepted archive key snapshot: the
	// JSON pitrAcceptance (K8). Operator-owned; never edit it.
	annotationPITRAccepted = "bloodraven.shipstream.io/pitr-accepted"
	// labelPITRBindingID is on every archive key snapshot: the binding id
	// (pitrBindingID). It is the release selector, so a release also covers
	// snapshots whose entry was dropped or lost.
	labelPITRBindingID = "bloodraven.shipstream.io/pitr-binding-id"
	// labelFailoverGroupUID is on archive key and dump key snapshots: the
	// group's metadata.uid. Snapshots have no owner reference, so this label
	// scopes every listing to one group incarnation. Same key as the backup
	// Job annotation BackupJobFailoverGroupUIDAnnotation.
	labelFailoverGroupUID = "bloodraven.shipstream.io/failover-group-uid"
	// labelMysqlBackupUID is on a dump key snapshot: the MysqlBackup's uid.
	labelMysqlBackupUID = "bloodraven.shipstream.io/mysqlbackup-uid"
	// labelPITRKeySecret is on check, restore, verification and in-place
	// staging Jobs: the archive key snapshot the Job mounts. Release refuses
	// while a Job with it has not stopped.
	labelPITRKeySecret = "bloodraven.shipstream.io/pitr-key-secret"

	// ReleasePITRArchiveBindingAnnotation on a MysqlFailoverGroup releases
	// one binding: "<id>:confirm=<metadata.name>" (parsePITRBindingRelease).
	// Never commit it in GitOps.
	ReleasePITRArchiveBindingAnnotation = "bloodraven.shipstream.io/release-pitr-archive-binding"

	// appNamePITRKeySnapshot and appNameDumpKeySnapshot are the
	// app.kubernetes.io/name labels of the two snapshot kinds.
	appNamePITRKeySnapshot = "mysql-pitr-key"
	appNameDumpKeySnapshot = "mysql-dump-key"
	// appNameBackupJob is the app.kubernetes.io/name of backup, cleanup and
	// (Δ26) PITR archive check Jobs.
	appNameBackupJob = "mysql-backup"
	// resourcePITRArchiveCheck is the shipstream.io/resource label value of
	// a PITR archive check Job. Every Job list selecting
	// app.kubernetes.io/name=mysql-backup must exclude it (§4.9).
	resourcePITRArchiveCheck = "pitr-archive-check"

	// pitrKeySnapshotDataKey is the data key of both snapshot kinds.
	pitrKeySnapshotDataKey = "passphrase"
	// pitrKeyRevisionPlaintext is BLOODRAVEN_PITR_KEY_REVISION for a None
	// archive (Δ2, K4). An empty revision is a pre-#203 pod and never proves
	// anything.
	pitrKeyRevisionPlaintext = "plaintext"
)

// Domains, schema versions and bounds.
const (
	// pitrBindingIDDomain prefixes the destination in pitrBindingID.
	pitrBindingIDDomain = "bloodraven-pitr-binding-v1\x00"
	// pitrKeySnapshotNameDomain prefixes the keyed snapshot name hash (Δ4):
	// h10 = hex(HMAC-SHA256(digestKey, domain ‖ destination ‖ "\0" ‖
	// trimmedBytes))[:10].
	pitrKeySnapshotNameDomain = "bloodraven-pitr-key-v1\x00"
	// pitrRenderSchemaVersion is pitrRenderInputs.SchemaVersion.
	pitrRenderSchemaVersion = 1
	// pitrAcceptanceSchemaVersion is pitrAcceptance.SchemaVersion.
	pitrAcceptanceSchemaVersion = 1
	// pitrArchiveBindingsMax is the MaxItems of status.pitrArchiveBindings.
	pitrArchiveBindingsMax = 16
	// pitrBindingMessageMaxRefusals bounds the refusals one condition
	// message names (§4.5).
	pitrBindingMessageMaxRefusals = 8
)

// Timings (§4.3, §4.4, §4.7).
const (
	// pitrCheckJobActiveDeadlineSeconds is a check Job's
	// activeDeadlineSeconds.
	pitrCheckJobActiveDeadlineSeconds int64 = 120
	// pitrCheckPendingTimeout is how long a check Job's pod may stay
	// Pending before the Job is deleted and the entry reports it.
	pitrCheckPendingTimeout = 60 * time.Second
	// pitrCheckRetryAfter is the requeue after a check Job could not start
	// or failed without a result line.
	pitrCheckRetryAfter = time.Minute
	// pitrReleaseRefusedRequeue is the requeue while a release is refused
	// because the binding is in use.
	pitrReleaseRefusedRequeue = time.Minute
	// pitrVerificationBindingRequeue is a verification's requeue while its
	// read binding is not ready and not terminal.
	pitrVerificationBindingRequeue = 15 * time.Second
)

// conditionPITRArchiveBindingReady reports the group's PITR archive bindings
// (§4.10). It joins reconcilerOwnedConditions.
const conditionPITRArchiveBindingReady = "PITRArchiveBindingReady"

// PITRArchiveBindingReady condition reasons that are not binding reasons.
// The condition also uses every binding reason (string(reason)).
const (
	reasonPITRBindingLimitReached     = "BindingLimitReached"
	reasonPITREncryptionChangeRefused = "EncryptionChangeRefused"
	reasonPITRKeyChangeRefused        = "KeyChangeRefused"
	reasonPITRArchiveBindingNotReady  = string(pitrarchive.TokenArchiveBindingNotReady)
)

// Binding reasons (v1alpha1.PITRArchiveBinding.Reason). Pending:
// Authenticating, Establishing. Bound: Bound, KeyMaterialLost. Refused: the
// rest. WS-E moves them to api/v1alpha1 (A-69).
const (
	pitrBindingAuthenticating            v1alpha1.PITRArchiveBindingReason = "Authenticating"
	pitrBindingEstablishing              v1alpha1.PITRArchiveBindingReason = "Establishing"
	pitrBindingBound                     v1alpha1.PITRArchiveBindingReason = "Bound"
	pitrBindingKeyMaterialLost           v1alpha1.PITRArchiveBindingReason = "KeyMaterialLost"
	pitrBindingKeyMismatch               v1alpha1.PITRArchiveBindingReason = "KeyMismatch"
	pitrBindingArchiveForeign            v1alpha1.PITRArchiveBindingReason = "ArchiveForeign"
	pitrBindingArchiveCopy               v1alpha1.PITRArchiveBindingReason = "ArchiveCopy"
	pitrBindingArchiveLegacy             v1alpha1.PITRArchiveBindingReason = "ArchiveLegacy"
	pitrBindingArchiveDescriptorMissing  v1alpha1.PITRArchiveBindingReason = "ArchiveDescriptorMissing"
	pitrBindingArchiveNotFound           v1alpha1.PITRArchiveBindingReason = "ArchiveNotFound"
	pitrBindingArchiveEncryptionMismatch v1alpha1.PITRArchiveBindingReason = "ArchiveEncryptionMismatch"
	pitrBindingArchiveVersionUnsupported v1alpha1.PITRArchiveBindingReason = "ArchiveVersionUnsupported"
	pitrBindingArchiveListFailed         v1alpha1.PITRArchiveBindingReason = "ArchiveListFailed"
)

// Event reasons (§4.10).
const (
	eventReasonPITRArchiveBound                   = "PITRArchiveBound"
	eventReasonPITRArchiveBindingReleased         = "PITRArchiveBindingReleased"
	eventReasonPITRArchiveEncryptionChangeRefused = "PITRArchiveEncryptionChangeRefused"
	eventReasonPITRArchiveKeyChangeRefused        = "PITRArchiveKeyChangeRefused"
	eventReasonPITRArchiveBindingRefused          = "PITRArchiveBindingRefused"
	eventReasonPITRArchiveKeySourceRemoved        = "PITRArchiveKeySourceRemoved"
	eventReasonPITRArchiveKeySnapshotRecreated    = "PITRArchiveKeySnapshotRecreated"
	eventReasonPITRArchiveKeyMaterialLost         = "PITRArchiveKeyMaterialLost"
	eventReasonPITRArchiveKeySnapshotUnreadable   = "PITRArchiveKeySnapshotUnreadable"
	eventReasonPITRArchiveBindingReleaseRefused   = "PITRArchiveBindingReleaseRefused"
	eventReasonPITRArchiveBindingReleaseRejected  = "PITRArchiveBindingReleaseRejected"
	eventReasonPITRArchiveBindingLimitReached     = "PITRArchiveBindingLimitReached"
	eventReasonBackupScheduleRefused              = "BackupScheduleRefused"
	// eventReasonPITRArchiveNotStamped (Normal, on a MysqlBackup): the
	// backup runs without a PITR archive stamp (not a PITR baseline)
	// because the site Deployments do not agree on a rendered archive and
	// waiting would not make it one (F-B).
	eventReasonPITRArchiveNotStamped = "PITRArchiveNotStamped"
)

// MysqlBackup pend/fail reasons the backup reconciler adds (§4.7, §4.8).
const (
	backupReasonProfileReadOnly           = "ProfileReadOnly"
	backupReasonPITRArchiveRolloutPending = "PITRArchiveRolloutPending"
	backupReasonDumpKeySnapshotConflict   = "DumpKeySnapshotConflict"
	backupReasonPassphraseSecretMissing   = "PassphraseSecretMissing"
)

// Structured log messages and the binding-change causes (§4.10; documented
// in site/content/docs/8.observability/7.log-schema.md).
const (
	// logMsgPITRBindingChanged: INFO {fg, bindingID, destination, archiveID,
	// state, reason, keySecretName, cause}.
	logMsgPITRBindingChanged = "pitr archive binding changed"
	// logMsgPITRBindingReleased: INFO {fg, bindingID, destination}.
	logMsgPITRBindingReleased = "pitr archive binding released"
	// logMsgPITRRenderIgnored: WARN {fg, namespace, site, error}.
	logMsgPITRRenderIgnored = "pitr render annotation ignored"
	// logMsgPITRRenderDeferred: INFO {fg, namespace, sites}.
	logMsgPITRRenderDeferred = "pitr render deferred"

	pitrBindingCauseProof     = "proof"
	pitrBindingCauseCheck     = "check"
	pitrBindingCauseRecovery  = "recovery"
	pitrBindingCauseIntegrity = "integrity"
	pitrBindingCauseC3        = "c3"
	// pitrBindingCauseInput: a spec, Secret or snapshot input changed the
	// entry (a new target, a missing live key, a retry after an input
	// change, a candidate superseded by an identity change), not a check
	// Job or a sidecar report.
	pitrBindingCauseInput = "input"
)

// The `bloodraven pitr-archive-check` contract (§4.4). Exported because
// cmd/bloodraven formats what pitr_binding_check.go parses.
const (
	// PITRArchiveCheckLinePrefix starts the check Job's termination message:
	// "BLOODRAVEN_PITR_ARCHIVE_CHECK v=2 state=<state> reason=<token or
	// empty> archiveID=<id> sourceUID=<uid> sourceNamespace=<ns>
	// sourceName=<name> destinationDigest=<hex>". The destination is
	// carried as a fixed-size digest (PITRArchiveCheckDestinationDigest),
	// never verbatim: Kubernetes truncates a termination message beyond
	// 4096 bytes, and a long prefix or endpoint, escaped, would not fit.
	PITRArchiveCheckLinePrefix = "BLOODRAVEN_PITR_ARCHIVE_CHECK v=2"
	// PITRArchiveCheckLogMsg is the command's INFO log {destination, role,
	// state, reason, archiveID}.
	PITRArchiveCheckLogMsg = "pitr archive check"

	PITRArchiveCheckStateAbsent   = "Absent"
	PITRArchiveCheckStateVerified = "Verified"
	PITRArchiveCheckStateRefused  = "Refused"

	// PITRArchiveCheckRoleEnv selects the check's role: Writer for a write
	// target (own UID and destination expected; an empty prefix is Absent),
	// Reader for a read target (no expected UID; an empty prefix is
	// ArchiveNotFound).
	PITRArchiveCheckRoleEnv    = "BLOODRAVEN_PITR_CHECK_ROLE"
	PITRArchiveCheckRoleWriter = "Writer"
	PITRArchiveCheckRoleReader = "Reader"
)

// ---------------------------------------------------------------------------
// Binding identity and lookup.

// pitrBindingID is the binding id of a destination: "pab-" + the first 16
// hex digits of SHA-256(pitrBindingIDDomain ‖ dest.String()).
func pitrBindingID(dest pitrarchive.Destination) string {
	return pitrBindingIDFromString(dest.String())
}

// pitrBindingIDFromString is pitrBindingID of a normalized destination
// string (a binding's or an annotation's Destination).
func pitrBindingIDFromString(destination string) string {
	h := sha256.New()
	h.Write([]byte(pitrBindingIDDomain))
	h.Write([]byte(destination))
	return "pab-" + hex.EncodeToString(h.Sum(nil))[:16]
}

// findPITRBinding returns the group's binding for dest, or nil. The result
// points into fg.Status.PITRArchiveBindings.
func findPITRBinding(fg *v1alpha1.MysqlFailoverGroup, dest pitrarchive.Destination) *v1alpha1.PITRArchiveBinding {
	if fg == nil || dest.IsZero() {
		return nil
	}
	want := dest.String()
	for i := range fg.Status.PITRArchiveBindings {
		if fg.Status.PITRArchiveBindings[i].Destination == want {
			return &fg.Status.PITRArchiveBindings[i]
		}
	}
	return nil
}

// findPITRBindingByID returns the group's binding with id, or nil. The
// result points into fg.Status.PITRArchiveBindings.
func findPITRBindingByID(fg *v1alpha1.MysqlFailoverGroup, id string) *v1alpha1.PITRArchiveBinding {
	if fg == nil || id == "" {
		return nil
	}
	for i := range fg.Status.PITRArchiveBindings {
		if fg.Status.PITRArchiveBindings[i].ID == id {
			return &fg.Status.PITRArchiveBindings[i]
		}
	}
	return nil
}

// pitrKeyRevision is the key revision of an encryption identity, as the
// sidecar echoes it (BLOODRAVEN_PITR_KEY_REVISION): the snapshot name for
// Passphrase, pitrKeyRevisionPlaintext for None. It is "" for an incomplete
// identity (Passphrase without a snapshot, unknown mode), which never
// matches a report.
func pitrKeyRevision(enc v1alpha1.PITRArchiveBindingEncryption) string {
	switch enc.Mode {
	case v1alpha1.PITRArchiveEncryptionNone:
		return pitrKeyRevisionPlaintext
	case v1alpha1.PITRArchiveEncryptionPassphrase:
		return enc.KeySecretName
	default:
		return ""
	}
}

// pitrBindingReasonForRefusal maps a sidecar refusal (status.pitr.archive
// Reason) to a binding reason (§4.4 correlated refusal). ok is false for
// refusals that do not refuse the identity (ArchiveUnsafeHistory,
// ArchiveStoreUnsupported): the entry stays Pending.
func pitrBindingReasonForRefusal(r v1alpha1.PITRArchiveRefusalReason) (v1alpha1.PITRArchiveBindingReason, bool) {
	switch r {
	case "ArchiveKeyMismatch":
		return pitrBindingKeyMismatch, true
	case "ArchiveForeign":
		return pitrBindingArchiveForeign, true
	case "ArchiveCopy":
		return pitrBindingArchiveCopy, true
	case "ArchiveLegacy":
		return pitrBindingArchiveLegacy, true
	case "ArchiveDescriptorMissing":
		return pitrBindingArchiveDescriptorMissing, true
	case "ArchiveEncryptionMismatch":
		return pitrBindingArchiveEncryptionMismatch, true
	case "ArchiveVersionUnsupported":
		return pitrBindingArchiveVersionUnsupported, true
	default:
		return "", false
	}
}

// pitrBindingReasonForToken maps a check Job's refusal token to a binding
// reason (§4.4 "Reading the result"). ok is false for any other token.
func pitrBindingReasonForToken(t pitrarchive.Token) (v1alpha1.PITRArchiveBindingReason, bool) {
	switch t {
	case pitrarchive.TokenArchiveKeyMismatch:
		return pitrBindingKeyMismatch, true
	case pitrarchive.TokenArchiveForeign:
		return pitrBindingArchiveForeign, true
	case pitrarchive.TokenArchiveCopy:
		return pitrBindingArchiveCopy, true
	case pitrarchive.TokenArchiveLegacy:
		return pitrBindingArchiveLegacy, true
	case pitrarchive.TokenArchiveDescriptorMissing:
		return pitrBindingArchiveDescriptorMissing, true
	case pitrarchive.TokenArchiveNotFound:
		return pitrBindingArchiveNotFound, true
	case pitrarchive.TokenArchiveEncryptionMismatch:
		return pitrBindingArchiveEncryptionMismatch, true
	case pitrarchive.TokenArchiveVersionUnsupported:
		return pitrBindingArchiveVersionUnsupported, true
	case pitrarchive.TokenArchiveListFailed:
		return pitrBindingArchiveListFailed, true
	default:
		return "", false
	}
}

// pitrWriterEligible is K6: a Bound entry may be the write target only when
// this group created the archive at exactly this destination
// (source.uid == fg.UID and the archive ID is derived from fg.UID and the
// destination). It returns (false, "") for a nil or non-Bound entry (not
// applicable: §4.6 row 4 decides), and (false, ArchiveForeign|ArchiveCopy)
// for a Bound entry that fails the check. A Bound entry with no recorded
// source is treated as foreign; one whose destination does not parse as a
// copy (its ID cannot be shown derived for it).
func pitrWriterEligible(fg *v1alpha1.MysqlFailoverGroup, entry *v1alpha1.PITRArchiveBinding) (bool, v1alpha1.PITRArchiveBindingReason) {
	if fg == nil || entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound {
		return false, ""
	}
	if entry.Source == nil || fg.UID == "" || entry.Source.UID != fg.UID {
		return false, pitrBindingArchiveForeign
	}
	dest, err := pitrarchive.ParseDestination(entry.Destination)
	if err != nil {
		return false, pitrBindingArchiveCopy
	}
	if !pitrarchive.IsDerivedArchiveID(entry.ArchiveID, fg.UID, dest) {
		return false, pitrBindingArchiveCopy
	}
	return true, ""
}

// ---------------------------------------------------------------------------
// Render inputs and the group hold (§4.6).

// pitrRenderInputs is everything a site's PITR render depends on. Its
// Canonical() JSON is the bloodraven.shipstream.io/pitr-render annotation
// and the pitr.render spec-hash input. It holds no profile name (K7).
//
// Identity fields are held while the write binding is not Bound or not
// writer-eligible; access fields always follow the active profile when it
// resolves to Destination (K13). Fill Bucket, KeyPrefix and Claim from the
// pitrarchive.Destination (parsePITRRender checks they agree), StorageType
// with the profile's storage type ("S3"/"PVC"), SubPath with the profile's
// raw PVC subPath, EndpointURL with the raw S3 endpointURL (any spelling of
// the normalized endpoint), and PollInterval with
// spec.backup.pitr.archivePollInterval's Duration.String() ("" when unset).
type pitrRenderInputs struct {
	SchemaVersion int  `json:"schemaVersion"`
	Enabled       bool `json:"enabled"`

	// Identity.
	Destination    string `json:"destination"`
	ArchiveID      string `json:"archiveID"`
	StorageType    string `json:"storageType"`
	Bucket         string `json:"bucket"`
	KeyPrefix      string `json:"keyPrefix"`
	Claim          string `json:"claim"`
	SubPath        string `json:"subPath"`
	EncryptionMode string `json:"encryptionMode"`
	Algorithm      string `json:"algorithm"`
	KeySecretName  string `json:"keySecretName"`

	// Access.
	Region            string `json:"region"`
	EndpointURL       string `json:"endpointURL"`
	CredentialsSecret string `json:"credentialsSecret"`
	PollInterval      string `json:"pollInterval"`
}

// pitrRenderDisabled is the render of a group that archives nothing (§4.6
// row 1).
func pitrRenderDisabled() pitrRenderInputs {
	return pitrRenderInputs{SchemaVersion: pitrRenderSchemaVersion}
}

// normalized sets SchemaVersion and, for a disabled render, clears every
// other field: a disabled render has no identity, so its canonical form
// and hash never depend on leftovers.
func (p pitrRenderInputs) normalized() pitrRenderInputs {
	if !p.Enabled {
		return pitrRenderDisabled()
	}
	p.SchemaVersion = pitrRenderSchemaVersion
	return p
}

// Canonical returns the sorted-key JSON of p with every key present
// (normalized first). It is stable across Go versions and field order.
func (p pitrRenderInputs) Canonical() string {
	raw, err := json.Marshal(p.normalized())
	if err != nil {
		// A struct of strings, an int and a bool always marshals.
		panic(fmt.Sprintf("pitrRenderInputs: marshal: %v", err))
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		panic(fmt.Sprintf("pitrRenderInputs: unmarshal: %v", err))
	}
	// encoding/json writes map keys sorted.
	out, err := json.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("pitrRenderInputs: marshal map: %v", err))
	}
	return string(out)
}

// Identity returns p with the access fields (Region, EndpointURL,
// CredentialsSecret, PollInterval) zeroed.
func (p pitrRenderInputs) Identity() pitrRenderInputs {
	p.Region = ""
	p.EndpointURL = ""
	p.CredentialsSecret = ""
	p.PollInterval = ""
	return p
}

// keyRevision is BLOODRAVEN_PITR_KEY_REVISION for p: KeySecretName for
// Passphrase, pitrKeyRevisionPlaintext for None, "" when disabled.
func (p pitrRenderInputs) keyRevision() string {
	if !p.Enabled {
		return ""
	}
	return pitrKeyRevision(v1alpha1.PITRArchiveBindingEncryption{
		Mode:          v1alpha1.PITRArchiveEncryptionMode(p.EncryptionMode),
		KeySecretName: p.KeySecretName,
	})
}

// validate checks p's internal consistency: a disabled render carries
// nothing; an enabled one has a canonical destination, a valid archive ID, a
// storage type and location fields matching the destination, a valid
// encryption identity and a snapshot name iff Passphrase.
func (p pitrRenderInputs) validate() error {
	if p.SchemaVersion != pitrRenderSchemaVersion {
		return fmt.Errorf("schemaVersion %d, want %d", p.SchemaVersion, pitrRenderSchemaVersion)
	}
	if !p.Enabled {
		if p != pitrRenderDisabled() {
			return errors.New("a disabled render carries fields")
		}
		return nil
	}
	d, err := pitrarchive.ParseDestination(p.Destination)
	if err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	if !pitrarchive.ValidArchiveID(p.ArchiveID) {
		return fmt.Errorf("archiveID %q is not an archive ID", p.ArchiveID)
	}
	switch d.Kind {
	case pitrarchive.DestinationS3:
		if p.StorageType != string(v1alpha1.BackupStorageS3) {
			return fmt.Errorf("storageType %q for an s3 destination", p.StorageType)
		}
		if p.Bucket != d.Bucket || p.Claim != "" || p.SubPath != "" {
			return errors.New("bucket, claim or subPath disagree with the destination")
		}
	case pitrarchive.DestinationPVC:
		if p.StorageType != string(v1alpha1.BackupStoragePVC) {
			return fmt.Errorf("storageType %q for a pvc destination", p.StorageType)
		}
		if p.Claim != d.Claim || p.Bucket != "" || p.Region != "" || p.EndpointURL != "" || p.CredentialsSecret != "" {
			return errors.New("claim or S3 fields disagree with the destination")
		}
	default:
		return fmt.Errorf("destination kind %q", d.Kind)
	}
	if p.KeyPrefix != d.KeyPrefix {
		return fmt.Errorf("keyPrefix %q, destination's is %q", p.KeyPrefix, d.KeyPrefix)
	}
	enc := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionMode(p.EncryptionMode), Algorithm: p.Algorithm}
	if err := enc.Validate(); err != nil {
		return err
	}
	if (enc.Mode == pitrarchive.EncryptionPassphrase) != (p.KeySecretName != "") {
		return fmt.Errorf("keySecretName %q with mode %s", p.KeySecretName, enc.Mode)
	}
	return nil
}

// errPITRRenderAbsent is parsePITRRender's error for an absent annotation.
var errPITRRenderAbsent = errors.New("no pitr-render annotation")

// pitrRenderKeys is the exact key set of a schemaVersion 1 annotation.
var pitrRenderKeys = func() []string {
	var m map[string]json.RawMessage
	_ = json.Unmarshal([]byte(pitrRenderDisabled().Canonical()), &m)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}()

// parsePITRRender decodes a pitr-render annotation. It returns
// errPITRRenderAbsent for "", and an error for malformed JSON, an unknown
// schemaVersion, a missing or unknown key, or inputs that fail validate.
// Use it where the error is logged (WARN logMsgPITRRenderIgnored).
func parsePITRRender(annotation string) (pitrRenderInputs, error) {
	if annotation == "" {
		return pitrRenderInputs{}, errPITRRenderAbsent
	}
	var version struct {
		SchemaVersion *int `json:"schemaVersion"`
	}
	if err := json.Unmarshal([]byte(annotation), &version); err != nil {
		return pitrRenderInputs{}, fmt.Errorf("malformed pitr-render annotation: %w", err)
	}
	if version.SchemaVersion == nil || *version.SchemaVersion != pitrRenderSchemaVersion {
		return pitrRenderInputs{}, fmt.Errorf("pitr-render annotation has an unknown schemaVersion")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(annotation), &m); err != nil {
		return pitrRenderInputs{}, fmt.Errorf("malformed pitr-render annotation: %w", err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if !slices.Equal(keys, pitrRenderKeys) {
		return pitrRenderInputs{}, fmt.Errorf("pitr-render annotation keys %v, want %v", keys, pitrRenderKeys)
	}
	var p pitrRenderInputs
	if err := strictJSONUnmarshal([]byte(annotation), &p); err != nil {
		return pitrRenderInputs{}, fmt.Errorf("malformed pitr-render annotation: %w", err)
	}
	if err := p.validate(); err != nil {
		return pitrRenderInputs{}, fmt.Errorf("invalid pitr-render annotation: %w", err)
	}
	return p, nil
}

// decodePITRRender is parsePITRRender without the error: ok is false for an
// absent, malformed or unknown-version annotation (I-13: "no decodable
// annotation").
func decodePITRRender(annotation string) (pitrRenderInputs, bool) {
	p, err := parsePITRRender(annotation)
	return p, err == nil
}

// strictJSONUnmarshal decodes exactly one JSON value into v, refusing
// unknown fields and trailing data.
func strictJSONUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data after the JSON value")
	}
	return nil
}

// pitrHoldKind is what the group's site Deployments say about the render
// they run (§4.6, K14).
type pitrHoldKind int

const (
	// pitrHoldNone: the group has no site Deployment (no writer runs).
	pitrHoldNone pitrHoldKind = iota
	// pitrHoldUnannotated: site Deployments exist and none carries a
	// decodable pitr-render annotation (pre-#203 or mangled): §4.6 row 4d.
	pitrHoldUnannotated
	// pitrHeld: the hold H is a site Deployment's decoded annotation (it
	// may be a disabled render).
	pitrHeld
)

func (k pitrHoldKind) String() string {
	switch k {
	case pitrHoldNone:
		return "None"
	case pitrHoldUnannotated:
		return "Unannotated"
	case pitrHeld:
		return "Held"
	default:
		return fmt.Sprintf("pitrHoldKind(%d)", int(k))
	}
}

// pitrRenderAction is resolvePITRRender's verdict for the existing site
// Deployments.
type pitrRenderAction int

const (
	// pitrRenderApply: render the resolved inputs (and write the
	// annotation).
	pitrRenderApply pitrRenderAction = iota
	// pitrRenderDefer: do not update existing Deployments at all (§4.6 row
	// 4d); sites being created render as row 4a.
	pitrRenderDefer
)

func (a pitrRenderAction) String() string {
	switch a {
	case pitrRenderApply:
		return "Render"
	case pitrRenderDefer:
		return "Defer"
	default:
		return fmt.Sprintf("pitrRenderAction(%d)", int(a))
	}
}

// sitePITRRender is one site Deployment's pitr-render annotation.
type sitePITRRender struct {
	Site       string
	Deployment string
	// Inputs is valid iff Err is nil.
	Inputs pitrRenderInputs
	// Err is errPITRRenderAbsent, or why the annotation is not decodable.
	Err error
}

// isPITRSiteDeployment reports whether d is a MySQL site Deployment of fg
// (commonLabels: app.kubernetes.io/name=mysql, the group label and a site).
func isPITRSiteDeployment(fg *v1alpha1.MysqlFailoverGroup, d *appsv1.Deployment) bool {
	l := d.GetLabels()
	return l[labelAppName] == "mysql" && l[labelFailoverGroup] == fg.Name && l[labelSite] != ""
}

// sitePITRRenders returns the pitr-render annotations of fg's site
// Deployments among deploys, ordered: the active site (status.activeSite)
// first, then spec.sites order, then Deployments of sites no longer in the
// spec by site name. Deployments that are not fg's MySQL site Deployments
// are ignored; Deployments being deleted count (their pods still run a
// writer).
func sitePITRRenders(fg *v1alpha1.MysqlFailoverGroup, deploys []appsv1.Deployment) []sitePITRRender {
	rank := make(map[string]int, len(fg.Spec.Sites)+1)
	for i, s := range fg.Spec.Sites {
		rank[s.Name] = i + 1
	}
	if fg.Status.ActiveSite != "" {
		rank[fg.Status.ActiveSite] = 0
	}
	var out []sitePITRRender
	for i := range deploys {
		d := &deploys[i]
		if !isPITRSiteDeployment(fg, d) {
			continue
		}
		in, err := parsePITRRender(d.GetAnnotations()[annotationPITRRender])
		out = append(out, sitePITRRender{Site: d.Labels[labelSite], Deployment: d.Name, Inputs: in, Err: err})
	}
	sort.SliceStable(out, func(i, j int) bool {
		ri, iok := rank[out[i].Site]
		rj, jok := rank[out[j].Site]
		switch {
		case iok && jok && ri != rj:
			return ri < rj
		case iok != jok:
			return iok
		case out[i].Site != out[j].Site:
			return out[i].Site < out[j].Site
		default:
			return out[i].Deployment < out[j].Deployment
		}
	})
	return out
}

// groupPITRHold is the group hold H (§4.6, K14): pitrHoldNone when fg has no
// site Deployment among deploys; pitrHeld with the decodable annotation of
// the active site's Deployment, else of the first site (spec.sites order,
// then removed sites by name) that has one; pitrHoldUnannotated when site
// Deployments exist and none is decodable. Inputs are zero unless Held.
func groupPITRHold(fg *v1alpha1.MysqlFailoverGroup, deploys []appsv1.Deployment) (pitrRenderInputs, pitrHoldKind) {
	renders := sitePITRRenders(fg, deploys)
	if len(renders) == 0 {
		return pitrRenderInputs{}, pitrHoldNone
	}
	for _, r := range renders {
		if r.Err == nil {
			return r.Inputs, pitrHeld
		}
	}
	return pitrRenderInputs{}, pitrHoldUnannotated
}

// ---------------------------------------------------------------------------
// Acceptance annotation (K8, Δ34).

// pitrAcceptance is the bloodraven.shipstream.io/pitr-accepted annotation of
// an accepted archive key snapshot. KeyCheck is computed locally from the
// snapshot bytes and the other fields; it equals the descriptor's because
// the proof verified the descriptor with exactly these bytes and fields.
// It is a passphrase verifier and lives only on the Secret.
type pitrAcceptance struct {
	SchemaVersion int                                `json:"schemaVersion"`
	Destination   string                             `json:"destination"`
	ArchiveID     string                             `json:"archiveID"`
	Source        v1alpha1.PITRArchiveSource         `json:"source"`
	Mode          v1alpha1.PITRArchiveEncryptionMode `json:"mode"`
	Algorithm     v1alpha1.EncryptionAlgorithm       `json:"algorithm"`
	KeyCheck      string                             `json:"keyCheck"`
	AcceptedAt    time.Time                          `json:"acceptedAt"`
}

// Acceptance verification errors. verifyPITRAcceptance wraps exactly one.
var (
	// errPITRAcceptanceMissing: no pitr-accepted annotation (a candidate
	// snapshot, never accepted). Not an integrity failure for recovery,
	// but a Bound entry's snapshot must carry one.
	errPITRAcceptanceMissing = errors.New("snapshot has no acceptance annotation")
	// errPITRAcceptanceMalformed: the annotation does not decode or
	// validate (I-13: never used for recovery; Warning
	// PITRArchiveKeySnapshotUnreadable; never deleted).
	errPITRAcceptanceMalformed = errors.New("acceptance annotation is malformed")
	// errPITRAcceptanceMismatch: the annotation decodes but the snapshot
	// does not hold the accepted key (bytes or destination disagree): Δ34
	// KeyMaterialLost.
	errPITRAcceptanceMismatch = errors.New("snapshot does not hold the accepted key")
)

// keyCheckFields returns the fields a's keyCheck covers.
func (a pitrAcceptance) keyCheckFields() backupcrypto.KeyCheckFields {
	return backupcrypto.KeyCheckFields{
		ArchiveID:       a.ArchiveID,
		SourceUID:       string(a.Source.UID),
		SourceNamespace: a.Source.Namespace,
		SourceName:      a.Source.Name,
		Mode:            string(a.Mode),
		Algorithm:       string(a.Algorithm),
	}
}

// validate checks every field's form (not the key).
func (a pitrAcceptance) validate() error {
	if a.SchemaVersion != pitrAcceptanceSchemaVersion {
		return fmt.Errorf("schemaVersion %d, want %d", a.SchemaVersion, pitrAcceptanceSchemaVersion)
	}
	if _, err := pitrarchive.ParseDestination(a.Destination); err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	if !pitrarchive.ValidArchiveID(a.ArchiveID) {
		return fmt.Errorf("archiveID %q is not an archive ID", a.ArchiveID)
	}
	if a.Source.UID == "" || a.Source.Namespace == "" || a.Source.Name == "" {
		return errors.New("source is incomplete")
	}
	enc := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionMode(a.Mode), Algorithm: string(a.Algorithm)}
	if err := enc.Validate(); err != nil {
		return err
	}
	if enc.Mode != pitrarchive.EncryptionPassphrase {
		return fmt.Errorf("mode %s has no key snapshot", a.Mode)
	}
	if !backupcrypto.ValidKeyCheckEncoding(a.KeyCheck) {
		return errors.New("keyCheck is not a keyCheck")
	}
	if a.AcceptedAt.IsZero() {
		return errors.New("acceptedAt is missing")
	}
	return nil
}

// newPITRAcceptance builds the acceptance of a Bound Passphrase entry whose
// snapshot holds passphrase, computing the keyCheck locally. It refuses an
// entry that is not Bound, not Passphrase, has no source or archive ID, and
// an empty passphrase.
func newPITRAcceptance(entry *v1alpha1.PITRArchiveBinding, passphrase []byte, now time.Time) (pitrAcceptance, error) {
	if entry == nil || entry.State != v1alpha1.PITRArchiveBindingBound {
		return pitrAcceptance{}, errors.New("pitr acceptance: entry is not Bound")
	}
	if entry.Source == nil {
		return pitrAcceptance{}, errors.New("pitr acceptance: entry has no source")
	}
	a := pitrAcceptance{
		SchemaVersion: pitrAcceptanceSchemaVersion,
		Destination:   entry.Destination,
		ArchiveID:     entry.ArchiveID,
		Source:        *entry.Source,
		Mode:          entry.Encryption.Mode,
		Algorithm:     entry.Encryption.Algorithm,
		AcceptedAt:    now.UTC().Truncate(time.Second),
	}
	kc, err := backupcrypto.ArchiveKeyCheck(passphrase, a.keyCheckFields())
	if err != nil {
		return pitrAcceptance{}, fmt.Errorf("pitr acceptance: keyCheck: %w", err)
	}
	a.KeyCheck = kc
	if err := a.validate(); err != nil {
		return pitrAcceptance{}, fmt.Errorf("pitr acceptance: %w", err)
	}
	return a, nil
}

// encode returns a's annotation value (validated, fixed field order).
func (a pitrAcceptance) encode() (string, error) {
	if err := a.validate(); err != nil {
		return "", fmt.Errorf("pitr acceptance: %w", err)
	}
	b, err := json.Marshal(a)
	if err != nil {
		return "", fmt.Errorf("pitr acceptance: %w", err)
	}
	return string(b), nil
}

// decodePITRAcceptance decodes and validates an annotation value. Any
// failure wraps errPITRAcceptanceMalformed.
func decodePITRAcceptance(s string) (pitrAcceptance, error) {
	var a pitrAcceptance
	if err := strictJSONUnmarshal([]byte(s), &a); err != nil {
		return pitrAcceptance{}, fmt.Errorf("%w: %v", errPITRAcceptanceMalformed, err)
	}
	if err := a.validate(); err != nil {
		return pitrAcceptance{}, fmt.Errorf("%w: %v", errPITRAcceptanceMalformed, err)
	}
	return a, nil
}

// verifyPITRAcceptance checks an archive key snapshot against its own
// acceptance annotation (K8 recovery, Δ34 integrity): the annotation decodes,
// the pitr-destination annotation equals its destination, and
// data["passphrase"] verifies against its keyCheck. It returns the decoded
// acceptance with a nil error, or an error wrapping errPITRAcceptanceMissing,
// errPITRAcceptanceMalformed or errPITRAcceptanceMismatch (the acceptance is
// returned with Mismatch so a caller can name it). The group-UID label and
// the binding's destination are the caller's to check (it knows fg and the
// entry).
func verifyPITRAcceptance(secret *corev1.Secret) (pitrAcceptance, error) {
	if secret == nil {
		return pitrAcceptance{}, errPITRAcceptanceMissing
	}
	raw, ok := secret.Annotations[annotationPITRAccepted]
	if !ok {
		return pitrAcceptance{}, errPITRAcceptanceMissing
	}
	a, err := decodePITRAcceptance(raw)
	if err != nil {
		return pitrAcceptance{}, err
	}
	if got := secret.Annotations[annotationPITRDestination]; got != a.Destination {
		return a, fmt.Errorf("%w: Secret %s destination annotation %q, accepted for %q", errPITRAcceptanceMismatch, secret.Name, got, a.Destination)
	}
	key := secret.Data[pitrKeySnapshotDataKey]
	if !backupcrypto.VerifyArchiveKeyCheck(key, a.keyCheckFields(), a.KeyCheck) {
		return a, fmt.Errorf("%w: Secret %s key %q does not verify against archive %s", errPITRAcceptanceMismatch, secret.Name, pitrKeySnapshotDataKey, a.ArchiveID)
	}
	return a, nil
}

// ---------------------------------------------------------------------------
// Release annotation (Δ10).

// parsePITRBindingRelease parses a release annotation value
// "<id>:confirm=<fgName>". It returns the id, or an error for a malformed
// value or a confirm that is not fgName (Warning ReleaseRejected; the
// annotation is removed).
func parsePITRBindingRelease(value, fgName string) (string, error) {
	id, confirm, ok := strings.Cut(strings.TrimSpace(value), ":confirm=")
	if !ok {
		return "", fmt.Errorf("release value %q is not <id>:confirm=<group name>", value)
	}
	if !pitrBindingIDRE(id) {
		return "", fmt.Errorf("release id %q is not a binding id (pab- and 16 hex digits)", id)
	}
	if confirm != fgName {
		return "", fmt.Errorf("release confirm %q does not name this group %q", confirm, fgName)
	}
	return id, nil
}

// pitrBindingIDRE reports whether id has the binding id form.
func pitrBindingIDRE(id string) bool {
	if len(id) != len("pab-")+16 || !strings.HasPrefix(id, "pab-") {
		return false
	}
	for _, c := range id[len("pab-"):] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Read bindings for restore and verification (Δ22).

// errPITRBindingNotReady is a read binding that a restore or verification
// cannot use yet. Terminal errors fail the consumer with
// PITRArchiveBindingNotReady; the rest wait (restore Pending, verification
// requeue pitrVerificationBindingRequeue).
type errPITRBindingNotReady struct {
	// state is the entry's state, "" when there is no entry.
	state v1alpha1.PITRArchiveBindingState
	// reason is the entry's reason, or reasonPITRBindingLimitReached when no
	// entry could be made.
	reason string
	// message explains it (the entry's message when it has one).
	message string
	// terminal is pitrBindingReadTerminal(reason) for a Refused entry.
	terminal bool
}

// Error starts with the PITRArchiveBindingNotReady token, so a restore
// failure message classifies like every other PITR refusal.
func (e *errPITRBindingNotReady) Error() string {
	state := string(e.state)
	if state == "" {
		state = "absent"
	}
	msg := fmt.Sprintf("%s: archive binding is %s/%s", pitrarchive.TokenArchiveBindingNotReady, state, e.reason)
	if e.message != "" {
		msg += ": " + e.message
	}
	return msg
}

// pitrBindingReadTerminal is Δ22's split for a refused read binding: true
// (fail) for ArchiveNotFound, ArchiveDescriptorMissing, ArchiveLegacy and
// ArchiveVersionUnsupported, which no Secret, profile or RBAC edit fixes;
// false (wait) for everything else, including KeyMismatch,
// ArchiveEncryptionMismatch, ArchiveListFailed and BindingLimitReached.
// Verifications fail on BindingLimitReached too; they check it themselves.
func pitrBindingReadTerminal(reason string) bool {
	switch v1alpha1.PITRArchiveBindingReason(reason) {
	case pitrBindingArchiveNotFound, pitrBindingArchiveDescriptorMissing,
		pitrBindingArchiveLegacy, pitrBindingArchiveVersionUnsupported:
		return true
	default:
		return false
	}
}

// pitrBindingReadiness returns nil when entry is usable by a reader
// (Bound/Bound), else the errPITRBindingNotReady to report: nil entry →
// absent/Authenticating (waits; the engine creates it), Pending → waits,
// Bound/KeyMaterialLost → waits (its snapshot is missing or does not hold
// the key; a required mount would hang the Job), Refused → per
// pitrBindingReadTerminal.
func pitrBindingReadiness(entry *v1alpha1.PITRArchiveBinding) *errPITRBindingNotReady {
	if entry == nil {
		return &errPITRBindingNotReady{reason: string(pitrBindingAuthenticating), message: "no binding for the archive yet"}
	}
	e := &errPITRBindingNotReady{state: entry.State, reason: string(entry.Reason), message: entry.Message}
	switch entry.State {
	case v1alpha1.PITRArchiveBindingBound:
		if entry.Reason == pitrBindingKeyMaterialLost {
			return e
		}
		return nil
	case v1alpha1.PITRArchiveBindingRefused:
		e.terminal = pitrBindingReadTerminal(e.reason)
		return e
	default:
		return e
	}
}
