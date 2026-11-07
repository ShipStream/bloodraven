package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

const (
	pbcUID   = types.UID("6f1c2d3e-0000-4000-8000-000000000001")
	pbcOther = types.UID("6f1c2d3e-0000-4000-8000-000000000002")
)

func pbcS3Dest(t *testing.T, prefix string) pitrarchive.Destination {
	t.Helper()
	d, err := pitrarchive.NewS3Destination("archive-bucket", prefix, "https://s3.example.test")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func pbcPVCDest(t *testing.T) pitrarchive.Destination {
	t.Helper()
	d, err := pitrarchive.NewPVCDestination("db", "archive-claim", "pitr")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func pbcGroup() *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "db", UID: pbcUID},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{{Name: "iad", ServerID: 101}, {Name: "pdx", ServerID: 102}, {Name: "sfo", ServerID: 103}},
		},
	}
}

// pbcRender is a valid enabled S3 render with every field set.
func pbcRender(t *testing.T) pitrRenderInputs {
	t.Helper()
	d := pbcS3Dest(t, "p1")
	return pitrRenderInputs{
		SchemaVersion:     pitrRenderSchemaVersion,
		Enabled:           true,
		Destination:       d.String(),
		ArchiveID:         pitrarchive.CreateArchiveID(pbcUID, d),
		StorageType:       string(v1alpha1.BackupStorageS3),
		Bucket:            d.Bucket,
		KeyPrefix:         d.KeyPrefix,
		EncryptionMode:    string(v1alpha1.EncryptionModePassphrase),
		Algorithm:         pitrarchive.AlgorithmAES256GCM,
		KeySecretName:     "mysql-orders-pitr-key-0123456789",
		Region:            "us-east-1",
		EndpointURL:       "https://S3.example.test:443/",
		CredentialsSecret: "s3-creds",
		PollInterval:      "30s",
	}
}

func pbcDeploy(name, group, site, render string) appsv1.Deployment {
	d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "db",
		Labels: map[string]string{labelAppName: "mysql", v1alpha1.FailoverGroupLabel: group, v1alpha1.SiteLabel: site},
	}}
	if render != "" {
		d.Annotations = map[string]string{v1alpha1.PITRRenderAnnotation: render}
	}
	return d
}

// TestPITRBindingContractStrings pins every public string of the contract:
// they are documented (crd-reference, log schema, monitoring, doctor) and
// downstream tooling filters on them.
func TestPITRBindingContractStrings(t *testing.T) {
	cases := map[string]string{
		v1alpha1.PITRRenderAnnotation:                    "bloodraven.shipstream.io/pitr-render",
		v1alpha1.PITRDestinationAnnotation:               "bloodraven.shipstream.io/pitr-destination",
		v1alpha1.PITRAcceptedAnnotation:                  "bloodraven.shipstream.io/pitr-accepted",
		v1alpha1.PITRKeySecretUIDAnnotation:              "bloodraven.shipstream.io/pitr-key-secret-uid",
		v1alpha1.PITRBindingIDLabel:                      "bloodraven.shipstream.io/pitr-binding-id",
		v1alpha1.FailoverGroupUIDLabel:                   "bloodraven.shipstream.io/failover-group-uid",
		v1alpha1.MysqlBackupUIDLabel:                     "bloodraven.shipstream.io/mysqlbackup-uid",
		v1alpha1.PITRKeySecretLabel:                      "bloodraven.shipstream.io/pitr-key-secret",
		v1alpha1.ReleasePITRArchiveBindingAnnotation:     "bloodraven.shipstream.io/release-pitr-archive-binding",
		appNamePITRKeySnapshot:                           "mysql-pitr-key",
		appNameDumpKeySnapshot:                           "mysql-dump-key",
		appNameBackupJob:                                 "mysql-backup",
		resourcePITRArchiveCheck:                         "pitr-archive-check",
		pitrKeySnapshotDataKey:                           "passphrase",
		pitrKeyRevisionPlaintext:                         "plaintext",
		pitrBindingIDDomain:                              "bloodraven-pitr-binding-v1\x00",
		pitrKeySnapshotNameDomain:                        "bloodraven-pitr-key-v1\x00",
		v1alpha1.ConditionPITRArchiveBindingReady:        "PITRArchiveBindingReady",
		v1alpha1.ReasonBindingLimitReached:               "BindingLimitReached",
		v1alpha1.ReasonEncryptionChangeRefused:           "EncryptionChangeRefused",
		v1alpha1.ReasonKeyChangeRefused:                  "KeyChangeRefused",
		string(v1alpha1.PITRTokenArchiveBindingNotReady): "PITRArchiveBindingNotReady",
		v1alpha1.EventPITRArchiveBound:                   "PITRArchiveBound",
		v1alpha1.EventPITRArchiveBindingReleased:         "PITRArchiveBindingReleased",
		v1alpha1.EventPITRArchiveEncryptionChangeRefused: "PITRArchiveEncryptionChangeRefused",
		v1alpha1.EventPITRArchiveKeyChangeRefused:        "PITRArchiveKeyChangeRefused",
		v1alpha1.EventPITRArchiveBindingRefused:          "PITRArchiveBindingRefused",
		v1alpha1.EventPITRArchiveKeySourceRemoved:        "PITRArchiveKeySourceRemoved",
		v1alpha1.EventPITRArchiveKeySnapshotRecreated:    "PITRArchiveKeySnapshotRecreated",
		v1alpha1.EventPITRArchiveKeyMaterialLost:         "PITRArchiveKeyMaterialLost",
		v1alpha1.EventPITRArchiveKeySnapshotUnreadable:   "PITRArchiveKeySnapshotUnreadable",
		v1alpha1.EventPITRArchiveBindingReleaseRefused:   "PITRArchiveBindingReleaseRefused",
		v1alpha1.EventPITRArchiveBindingReleaseRejected:  "PITRArchiveBindingReleaseRejected",
		v1alpha1.EventPITRArchiveBindingLimitReached:     "PITRArchiveBindingLimitReached",
		v1alpha1.EventBackupScheduleRefused:              "BackupScheduleRefused",
		v1alpha1.ReasonProfileReadOnly:                   "ProfileReadOnly",
		v1alpha1.ReasonPITRArchiveRolloutPending:         "PITRArchiveRolloutPending",
		v1alpha1.ReasonDumpKeySnapshotConflict:           "DumpKeySnapshotConflict",
		v1alpha1.ReasonPassphraseSecretMissing:           "PassphraseSecretMissing",
		logMsgPITRBindingChanged:                         "pitr archive binding changed",
		logMsgPITRBindingReleased:                        "pitr archive binding released",
		logMsgPITRRenderIgnored:                          "pitr render annotation ignored",
		logMsgPITRRenderDeferred:                         "pitr render deferred",
		pitrBindingCauseProof:                            "proof",
		pitrBindingCauseCheck:                            "check",
		pitrBindingCauseRecovery:                         "recovery",
		pitrBindingCauseIntegrity:                        "integrity",
		pitrBindingCauseC3:                               "c3",
		PITRArchiveCheckLinePrefix:                       "BLOODRAVEN_PITR_ARCHIVE_CHECK v=2",
		PITRArchiveCheckLogMsg:                           "pitr archive check",
		PITRArchiveCheckStateAbsent:                      "Absent",
		PITRArchiveCheckStateVerified:                    "Verified",
		PITRArchiveCheckStateRefused:                     "Refused",
		PITRArchiveCheckRoleEnv:                          "BLOODRAVEN_PITR_CHECK_ROLE",
		PITRArchiveCheckRoleWriter:                       "Writer",
		PITRArchiveCheckRoleReader:                       "Reader",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("contract string %q, want %q", got, want)
		}
	}
	if v1alpha1.FailoverGroupUIDLabel != v1alpha1.BackupJobFailoverGroupUIDAnnotation {
		t.Errorf("FailoverGroupUIDLabel %q drifted from BackupJobFailoverGroupUIDAnnotation %q", v1alpha1.FailoverGroupUIDLabel, v1alpha1.BackupJobFailoverGroupUIDAnnotation)
	}
	if pitrArchiveBindingsMax != 16 || pitrBindingMessageMaxRefusals != 8 || pitrRenderSchemaVersion != 1 || pitrAcceptanceSchemaVersion != 1 {
		t.Errorf("bounds drifted")
	}
	if pitrCheckJobActiveDeadlineSeconds != 120 || pitrCheckPendingTimeout != time.Minute || pitrCheckRetryAfter != time.Minute ||
		pitrReleaseRefusedRequeue != time.Minute || pitrReadBindingRequeue != 15*time.Second {
		t.Errorf("timings drifted")
	}

	// Every binding reason constant is in the CRD enum, and vice versa.
	reasons := []v1alpha1.PITRArchiveBindingReason{
		v1alpha1.PITRArchiveBindingReasonAuthenticating, v1alpha1.PITRArchiveBindingReasonEstablishing, v1alpha1.PITRArchiveBindingReasonBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost,
		v1alpha1.PITRArchiveBindingReasonKeyMismatch, v1alpha1.PITRArchiveBindingReasonArchiveForeign, v1alpha1.PITRArchiveBindingReasonArchiveCopy, v1alpha1.PITRArchiveBindingReasonArchiveLegacy,
		v1alpha1.PITRArchiveBindingReasonArchiveDescriptorMissing, v1alpha1.PITRArchiveBindingReasonArchiveNotFound, v1alpha1.PITRArchiveBindingReasonArchiveEncryptionMismatch,
		v1alpha1.PITRArchiveBindingReasonArchiveVersionUnsupported, v1alpha1.PITRArchiveBindingReasonArchiveListFailed,
	}
	enum := "Authenticating;Establishing;Bound;KeyMaterialLost;KeyMismatch;ArchiveForeign;ArchiveCopy;ArchiveLegacy;ArchiveDescriptorMissing;ArchiveNotFound;ArchiveEncryptionMismatch;ArchiveVersionUnsupported;ArchiveListFailed"
	var names []string
	for _, r := range reasons {
		names = append(names, string(r))
	}
	if got := strings.Join(names, ";"); got != enum {
		t.Errorf("binding reasons %q, CRD enum %q", got, enum)
	}
}

func TestPITRBindingID(t *testing.T) {
	d1 := pbcS3Dest(t, "p1")
	d2 := pbcS3Dest(t, "p2")
	sum := sha256.Sum256([]byte("bloodraven-pitr-binding-v1\x00" + d1.String()))
	want := "pab-" + hex.EncodeToString(sum[:])[:16]
	if got := pitrBindingID(d1); got != want {
		t.Fatalf("pitrBindingID = %q, want %q", got, want)
	}
	if pitrBindingIDFromString(d1.String()) != want {
		t.Fatal("pitrBindingIDFromString disagrees with pitrBindingID")
	}
	if pitrBindingID(d2) == want {
		t.Fatal("two destinations share an id")
	}
	if !pitrBindingIDRE(want) {
		t.Fatalf("id %q fails the id form", want)
	}
	for _, bad := range []string{"", "pab-", "pab-0123456789abcdeF", "pab-0123456789abcde", "pab-0123456789abcdef0", "xab-0123456789abcdef"} {
		if pitrBindingIDRE(bad) {
			t.Errorf("pitrBindingIDRE(%q) = true", bad)
		}
	}
}

func TestPITRBindingFind(t *testing.T) {
	fg := pbcGroup()
	d1, d2 := pbcS3Dest(t, "p1"), pbcS3Dest(t, "p2")
	fg.Status.PITRArchiveBindings = []v1alpha1.PITRArchiveBinding{
		{ID: pitrBindingID(d1), Destination: d1.String()},
		{ID: pitrBindingID(d2), Destination: d2.String()},
	}
	got := findPITRBinding(fg, d2)
	if got == nil || got != &fg.Status.PITRArchiveBindings[1] {
		t.Fatalf("findPITRBinding(d2) = %v, want a pointer to entry 1", got)
	}
	if findBindingIn(fg.Status.PITRArchiveBindings, pitrBindingID(d1)) != &fg.Status.PITRArchiveBindings[0] {
		t.Fatal("findBindingIn(d1) is not entry 0")
	}
	if findPITRBinding(fg, pbcPVCDest(t)) != nil || findPITRBinding(fg, pitrarchive.Destination{}) != nil || findPITRBinding(nil, d1) != nil {
		t.Fatal("findPITRBinding found an absent destination")
	}
	if findBindingIn(fg.Status.PITRArchiveBindings, "") != nil || findBindingIn(fg.Status.PITRArchiveBindings, "pab-0000000000000000") != nil {
		t.Fatal("findBindingIn found an absent id")
	}
}

func TestPITRBindingKeyRevision(t *testing.T) {
	cases := []struct {
		enc  v1alpha1.PITRArchiveBindingEncryption
		want string
	}{
		{v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.EncryptionModeNone}, "plaintext"},
		{v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.EncryptionModePassphrase, KeySecretName: "snap"}, "snap"},
		{v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.EncryptionModePassphrase}, ""},
		{v1alpha1.PITRArchiveBindingEncryption{}, ""},
	}
	for _, c := range cases {
		if got := pitrKeyRevision(c.enc); got != c.want {
			t.Errorf("pitrKeyRevision(%+v) = %q, want %q", c.enc, got, c.want)
		}
	}
	r := pbcRender(t)
	if r.keyRevision() != r.KeySecretName {
		t.Errorf("render keyRevision = %q", r.keyRevision())
	}
	r.EncryptionMode, r.Algorithm, r.KeySecretName = string(v1alpha1.EncryptionModeNone), "", ""
	if r.keyRevision() != "plaintext" {
		t.Errorf("plaintext render keyRevision = %q", r.keyRevision())
	}
	if pitrRenderDisabled().keyRevision() != "" {
		t.Error("disabled render has a key revision")
	}
}

func TestPITRBindingReasonMappings(t *testing.T) {
	refusals := map[v1alpha1.PITRArchiveRefusalReason]v1alpha1.PITRArchiveBindingReason{
		"ArchiveKeyMismatch":        v1alpha1.PITRArchiveBindingReasonKeyMismatch,
		"ArchiveForeign":            v1alpha1.PITRArchiveBindingReasonArchiveForeign,
		"ArchiveCopy":               v1alpha1.PITRArchiveBindingReasonArchiveCopy,
		"ArchiveLegacy":             v1alpha1.PITRArchiveBindingReasonArchiveLegacy,
		"ArchiveDescriptorMissing":  v1alpha1.PITRArchiveBindingReasonArchiveDescriptorMissing,
		"ArchiveEncryptionMismatch": v1alpha1.PITRArchiveBindingReasonArchiveEncryptionMismatch,
		"ArchiveVersionUnsupported": v1alpha1.PITRArchiveBindingReasonArchiveVersionUnsupported,
		"ArchiveUnsafeHistory":      "",
		"ArchiveStoreUnsupported":   "",
		"":                          "",
	}
	for in, want := range refusals {
		got, ok := pitrBindingReasonForRefusal(in)
		if got != want || ok != (want != "") {
			t.Errorf("pitrBindingReasonForRefusal(%q) = %q,%v; want %q", in, got, ok, want)
		}
	}
	tokens := map[pitrarchive.Token]v1alpha1.PITRArchiveBindingReason{
		pitrarchive.TokenArchiveKeyMismatch:        v1alpha1.PITRArchiveBindingReasonKeyMismatch,
		pitrarchive.TokenArchiveForeign:            v1alpha1.PITRArchiveBindingReasonArchiveForeign,
		pitrarchive.TokenArchiveCopy:               v1alpha1.PITRArchiveBindingReasonArchiveCopy,
		pitrarchive.TokenArchiveLegacy:             v1alpha1.PITRArchiveBindingReasonArchiveLegacy,
		pitrarchive.TokenArchiveDescriptorMissing:  v1alpha1.PITRArchiveBindingReasonArchiveDescriptorMissing,
		pitrarchive.TokenArchiveNotFound:           v1alpha1.PITRArchiveBindingReasonArchiveNotFound,
		pitrarchive.TokenArchiveEncryptionMismatch: v1alpha1.PITRArchiveBindingReasonArchiveEncryptionMismatch,
		pitrarchive.TokenArchiveVersionUnsupported: v1alpha1.PITRArchiveBindingReasonArchiveVersionUnsupported,
		pitrarchive.TokenArchiveListFailed:         v1alpha1.PITRArchiveBindingReasonArchiveListFailed,
		pitrarchive.TokenArchiveUnsafeHistory:      "",
		pitrarchive.TokenArchiveIdentityMismatch:   "",
		"":                                         "",
	}
	for in, want := range tokens {
		got, ok := pitrBindingReasonForToken(in)
		if got != want || ok != (want != "") {
			t.Errorf("pitrBindingReasonForToken(%q) = %q,%v; want %q", in, got, ok, want)
		}
	}
}

func TestPITRWriterEligible(t *testing.T) {
	fg := pbcGroup()
	d := pbcS3Dest(t, "p1")
	copyDest := pbcS3Dest(t, "p1-copy")
	own := v1alpha1.PITRArchiveSource{Namespace: "db", Name: "orders", UID: pbcUID}
	foreign := v1alpha1.PITRArchiveSource{Namespace: "db", Name: "orders", UID: pbcOther}
	bound := func(mut func(*v1alpha1.PITRArchiveBinding)) *v1alpha1.PITRArchiveBinding {
		e := &v1alpha1.PITRArchiveBinding{
			ID: pitrBindingID(d), Destination: d.String(), ArchiveID: pitrarchive.CreateArchiveID(pbcUID, d),
			Source: own.DeepCopy(), State: v1alpha1.PITRArchiveBindingBound, Reason: v1alpha1.PITRArchiveBindingReasonBound,
		}
		if mut != nil {
			mut(e)
		}
		return e
	}
	cases := []struct {
		name   string
		entry  *v1alpha1.PITRArchiveBinding
		ok     bool
		reason v1alpha1.PITRArchiveBindingReason
	}{
		{"nil entry", nil, false, ""},
		{"pending", bound(func(e *v1alpha1.PITRArchiveBinding) {
			e.State, e.Reason = v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonEstablishing
		}), false, ""},
		{"refused", bound(func(e *v1alpha1.PITRArchiveBinding) {
			e.State, e.Reason = v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonKeyMismatch
		}), false, ""},
		{"own", bound(nil), true, ""},
		{"own, key material lost", bound(func(e *v1alpha1.PITRArchiveBinding) { e.Reason = v1alpha1.PITRArchiveBindingReasonKeyMaterialLost }), true, ""},
		{"foreign uid", bound(func(e *v1alpha1.PITRArchiveBinding) {
			e.Source = foreign.DeepCopy()
			e.ArchiveID = pitrarchive.CreateArchiveID(pbcOther, d)
		}), false, v1alpha1.PITRArchiveBindingReasonArchiveForeign},
		{"no source", bound(func(e *v1alpha1.PITRArchiveBinding) { e.Source = nil }), false, v1alpha1.PITRArchiveBindingReasonArchiveForeign},
		{"copy: id derived for another destination", bound(func(e *v1alpha1.PITRArchiveBinding) {
			e.ArchiveID = pitrarchive.CreateArchiveID(pbcUID, copyDest)
		}), false, v1alpha1.PITRArchiveBindingReasonArchiveCopy},
		{"empty archive id", bound(func(e *v1alpha1.PITRArchiveBinding) { e.ArchiveID = "" }), false, v1alpha1.PITRArchiveBindingReasonArchiveCopy},
		{"unparsable destination", bound(func(e *v1alpha1.PITRArchiveBinding) { e.Destination = "s3://not canonical" }), false, v1alpha1.PITRArchiveBindingReasonArchiveCopy},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, reason := pitrWriterEligible(fg, c.entry)
			if ok != c.ok || reason != c.reason {
				t.Fatalf("pitrWriterEligible = %v,%q; want %v,%q", ok, reason, c.ok, c.reason)
			}
		})
	}
	noUID := pbcGroup()
	noUID.UID = ""
	if ok, reason := pitrWriterEligible(noUID, bound(nil)); ok || reason != v1alpha1.PITRArchiveBindingReasonArchiveForeign {
		t.Fatalf("group without UID: %v,%q", ok, reason)
	}
}

func TestPITRRenderCanonical(t *testing.T) {
	r := pbcRender(t)
	c := r.Canonical()

	// Every key present and sorted.
	var m map[string]any
	if err := json.Unmarshal([]byte(c), &m); err != nil {
		t.Fatal(err)
	}
	if len(m) != reflect.TypeOf(pitrRenderInputs{}).NumField() {
		t.Fatalf("canonical has %d keys, struct has %d fields", len(m), reflect.TypeOf(pitrRenderInputs{}).NumField())
	}
	var keys []string
	dec := json.NewDecoder(strings.NewReader(c))
	if _, err := dec.Token(); err != nil {
		t.Fatal(err)
	}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k.(string))
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("keys not sorted: %v", keys)
	}

	// Stable: same input, same bytes; SchemaVersion is set by Canonical.
	r2 := r
	r2.SchemaVersion = 0
	if r2.Canonical() != c {
		t.Fatal("Canonical depends on the caller's SchemaVersion")
	}

	// Round trip.
	back, err := parsePITRRender(c)
	if err != nil {
		t.Fatalf("parse(Canonical) = %v", err)
	}
	if back != r {
		t.Fatalf("round trip:\n got %+v\nwant %+v", back, r)
	}

	// A disabled render has no identity: leftovers do not reach the hash.
	dis := r
	dis.Enabled = false
	if dis.Canonical() != pitrRenderDisabled().Canonical() {
		t.Fatal("disabled render with leftovers canonicalizes differently")
	}

	// Golden: the annotation format is a stored contract.
	golden := `{"algorithm":"","archiveID":"","bucket":"","claim":"","credentialsSecret":"","destination":"","enabled":false,"encryptionMode":"","endpointURL":"","keyPrefix":"","keySecretName":"","pollInterval":"","region":"","schemaVersion":1,"storageType":"","subPath":""}`
	if got := pitrRenderDisabled().Canonical(); got != golden {
		t.Fatalf("disabled canonical\n got %s\nwant %s", got, golden)
	}

	// A PVC render round-trips too.
	pd := pbcPVCDest(t)
	pvc := pitrRenderInputs{
		Enabled: true, Destination: pd.String(), ArchiveID: pitrarchive.CreateArchiveID(pbcUID, pd),
		StorageType: string(v1alpha1.BackupStoragePVC), Claim: pd.Claim, KeyPrefix: pd.KeyPrefix, SubPath: "pitr",
		EncryptionMode: string(v1alpha1.EncryptionModeNone), PollInterval: "1m0s",
	}
	back, err = parsePITRRender(pvc.Canonical())
	if err != nil {
		t.Fatalf("pvc parse: %v", err)
	}
	if back != pvc.normalized() {
		t.Fatalf("pvc round trip: %+v", back)
	}
}

func TestPITRRenderIdentity(t *testing.T) {
	access := map[string]bool{"Region": true, "EndpointURL": true, "CredentialsSecret": true, "PollInterval": true}
	full := pbcRender(t)
	id := full.Identity()
	fv, iv := reflect.ValueOf(full), reflect.ValueOf(id)
	for i := 0; i < fv.NumField(); i++ {
		name := fv.Type().Field(i).Name
		if access[name] {
			if !iv.Field(i).IsZero() {
				t.Errorf("Identity kept access field %s", name)
			}
			if fv.Field(i).IsZero() {
				t.Errorf("fixture leaves access field %s empty", name)
			}
			continue
		}
		if !reflect.DeepEqual(fv.Field(i).Interface(), iv.Field(i).Interface()) {
			t.Errorf("Identity changed identity field %s", name)
		}
	}
	// Different access fields, same identity.
	other := full
	other.Region, other.EndpointURL, other.CredentialsSecret, other.PollInterval = "eu-west-1", "https://s3.example.test", "other", "1m0s"
	if other.Identity() != id || other.Canonical() == full.Canonical() {
		t.Fatal("access-only change must keep Identity and change Canonical")
	}
}

func TestPITRRenderDecode(t *testing.T) {
	valid := pbcRender(t)
	setKey := func(k string, v any) string {
		var m map[string]any
		_ = json.Unmarshal([]byte(valid.Canonical()), &m)
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
		b, _ := json.Marshal(m)
		return string(b)
	}
	mutate := func(f func(*pitrRenderInputs)) string {
		p := valid
		f(&p)
		b, _ := json.Marshal(p)
		return string(b)
	}
	cases := []struct {
		name   string
		in     string
		ok     bool
		absent bool
	}{
		{"valid", valid.Canonical(), true, false},
		{"valid, unsorted field order", mutate(func(*pitrRenderInputs) {}), true, false},
		{"disabled", pitrRenderDisabled().Canonical(), true, false},
		{"absent", "", false, true},
		{"not json", "{", false, false},
		{"array", "[]", false, false},
		{"no schemaVersion", setKey("schemaVersion", nil), false, false},
		{"unknown schemaVersion", setKey("schemaVersion", 2), false, false},
		{"missing key", setKey("pollInterval", nil), false, false},
		{"unknown key", setKey("profileName", "a"), false, false},
		{"trailing data", valid.Canonical() + "{}", false, false},
		{"wrong type", setKey("enabled", "true"), false, false},
		{"disabled with fields", setKey("enabled", false), false, false},
		{"non-canonical destination", mutate(func(p *pitrRenderInputs) { p.Destination = "s3://archive-bucket/p1/binlogs" }), false, false},
		{"bad archive id", mutate(func(p *pitrRenderInputs) { p.ArchiveID = "ar1-x" }), false, false},
		{"storage type mismatch", mutate(func(p *pitrRenderInputs) { p.StorageType = "PVC" }), false, false},
		{"bucket mismatch", mutate(func(p *pitrRenderInputs) { p.Bucket = "other" }), false, false},
		{"key prefix mismatch", mutate(func(p *pitrRenderInputs) { p.KeyPrefix = "binlogs" }), false, false},
		{"claim on s3", mutate(func(p *pitrRenderInputs) { p.Claim = "c" }), false, false},
		{"passphrase without snapshot", mutate(func(p *pitrRenderInputs) { p.KeySecretName = "" }), false, false},
		{"none with snapshot", mutate(func(p *pitrRenderInputs) { p.EncryptionMode, p.Algorithm = "None", "" }), false, false},
		{"bad algorithm", mutate(func(p *pitrRenderInputs) { p.Algorithm = "ROT13" }), false, false},
		{"unknown mode", mutate(func(p *pitrRenderInputs) { p.EncryptionMode = "KMS" }), false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := parsePITRRender(c.in)
			if (err == nil) != c.ok {
				t.Fatalf("parsePITRRender err = %v, want ok=%v", err, c.ok)
			}
			if errors.Is(err, errPITRRenderAbsent) != c.absent {
				t.Fatalf("absent = %v, want %v (%v)", errors.Is(err, errPITRRenderAbsent), c.absent, err)
			}
			if !c.ok && p != (pitrRenderInputs{}) {
				t.Fatalf("failed parse returned inputs %+v", p)
			}
		})
	}
}

func TestPITRRenderGroupHold(t *testing.T) {
	r1 := pbcRender(t)
	r2 := pbcRender(t)
	d2 := pbcS3Dest(t, "p2")
	r2.Destination, r2.ArchiveID = d2.String(), pitrarchive.CreateArchiveID(pbcUID, d2)
	r2.KeyPrefix = d2.KeyPrefix
	dis := pitrRenderDisabled()

	cases := []struct {
		name    string
		active  string
		deploys []appsv1.Deployment
		kind    pitrHoldKind
		want    pitrRenderInputs
	}{
		{"no deployments", "", nil, pitrHoldNone, pitrRenderInputs{}},
		{"only foreign deployments", "", []appsv1.Deployment{
			pbcDeploy("mysql-other-iad", "other", "iad", r1.Canonical()),
			func() appsv1.Deployment {
				d := pbcDeploy("dragonfly-orders-iad", "orders", "iad", r1.Canonical())
				d.Labels[labelAppName] = "dragonfly"
				return d
			}(),
		}, pitrHoldNone, pitrRenderInputs{}},
		{"unannotated", "iad", []appsv1.Deployment{
			pbcDeploy("mysql-orders-iad", "orders", "iad", ""),
			pbcDeploy("mysql-orders-pdx", "orders", "pdx", "{garbage"),
		}, pitrHoldUnannotated, pitrRenderInputs{}},
		{"active site preferred", "pdx", []appsv1.Deployment{
			pbcDeploy("mysql-orders-iad", "orders", "iad", r1.Canonical()),
			pbcDeploy("mysql-orders-pdx", "orders", "pdx", r2.Canonical()),
		}, pitrHeld, r2},
		{"spec order when the active site has none", "sfo", []appsv1.Deployment{
			pbcDeploy("mysql-orders-sfo", "orders", "sfo", ""),
			pbcDeploy("mysql-orders-pdx", "orders", "pdx", r2.Canonical()),
			pbcDeploy("mysql-orders-iad", "orders", "iad", r1.Canonical()),
		}, pitrHeld, r1},
		{"undecodable skipped", "", []appsv1.Deployment{
			pbcDeploy("mysql-orders-iad", "orders", "iad", `{"schemaVersion":9}`),
			pbcDeploy("mysql-orders-pdx", "orders", "pdx", r2.Canonical()),
		}, pitrHeld, r2},
		{"removed site counts after spec sites", "", []appsv1.Deployment{
			pbcDeploy("mysql-orders-old", "orders", "old", r1.Canonical()),
			pbcDeploy("mysql-orders-pdx", "orders", "pdx", ""),
		}, pitrHeld, r1},
		{"held disabled render", "iad", []appsv1.Deployment{
			pbcDeploy("mysql-orders-iad", "orders", "iad", dis.Canonical()),
			pbcDeploy("mysql-orders-pdx", "orders", "pdx", r2.Canonical()),
		}, pitrHeld, dis},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fg := pbcGroup()
			fg.Status.ActiveSite = c.active
			got, kind := groupPITRHold(fg, c.deploys)
			if kind != c.kind || got != c.want {
				t.Fatalf("groupPITRHold = %v %+v; want %v %+v", kind, got, c.kind, c.want)
			}
		})
	}

	// sitePITRRenders reports every site Deployment with its error.
	fg := pbcGroup()
	fg.Status.ActiveSite = "pdx"
	renders := sitePITRRenders(fg, []appsv1.Deployment{
		pbcDeploy("mysql-orders-old", "orders", "old", ""),
		pbcDeploy("mysql-orders-iad", "orders", "iad", "{"),
		pbcDeploy("mysql-orders-pdx", "orders", "pdx", r1.Canonical()),
	})
	var sites []string
	for _, r := range renders {
		sites = append(sites, r.Site)
	}
	if strings.Join(sites, ",") != "pdx,iad,old" {
		t.Fatalf("order %v", sites)
	}
	if renders[0].Err != nil || !errors.Is(renders[2].Err, errPITRRenderAbsent) || renders[1].Err == nil || errors.Is(renders[1].Err, errPITRRenderAbsent) {
		t.Fatalf("errors %v %v %v", renders[0].Err, renders[1].Err, renders[2].Err)
	}
	if pitrHoldNone.String() != "None" || pitrHoldUnannotated.String() != "Unannotated" || pitrHeld.String() != "Held" ||
		pitrRenderApply.String() != "Render" || pitrRenderDefer.String() != "Defer" {
		t.Fatal("kind/action names drifted")
	}
}

// pbcAccepted returns a Bound entry, its snapshot Secret with a valid
// acceptance, and the descriptor a writer made with the same key.
func pbcAccepted(t *testing.T, key string) (*v1alpha1.PITRArchiveBinding, *corev1.Secret, *pitrarchive.Descriptor) {
	t.Helper()
	d := pbcS3Dest(t, "p1")
	src := pitrarchive.Source{Namespace: "db", Name: "orders", UID: pbcUID}
	desc, err := pitrarchive.NewDescriptor(src, d, pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}, []byte(key), "iad", time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	entry := &v1alpha1.PITRArchiveBinding{
		ID: pitrBindingID(d), Destination: d.String(), ArchiveID: desc.ArchiveID,
		Source: &v1alpha1.PITRArchiveSource{Namespace: "db", Name: "orders", UID: pbcUID},
		Encryption: v1alpha1.PITRArchiveBindingEncryption{
			Mode: v1alpha1.EncryptionModePassphrase, Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM,
			KeySecretName: "mysql-orders-pitr-key-0123456789",
		},
		State: v1alpha1.PITRArchiveBindingBound, Reason: v1alpha1.PITRArchiveBindingReasonBound,
	}
	a, err := newPITRAcceptance(entry, []byte(key), time.Unix(1_800_000_100, 5))
	if err != nil {
		t.Fatal(err)
	}
	enc, err := a.encode()
	if err != nil {
		t.Fatal(err)
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: entry.Encryption.KeySecretName, Namespace: "db",
			Annotations: map[string]string{v1alpha1.PITRAcceptedAnnotation: enc, v1alpha1.PITRDestinationAnnotation: d.String()},
		},
		Data: map[string][]byte{pitrKeySnapshotDataKey: []byte(key)},
	}
	return entry, sec, desc
}

func TestPITRAcceptance(t *testing.T) {
	const key = "correct horse battery staple"
	entry, sec, desc := pbcAccepted(t, key)

	a, err := verifyPITRAcceptance(sec)
	if err != nil {
		t.Fatalf("verify good snapshot: %v", err)
	}
	// The locally computed keyCheck equals the descriptor's (K8).
	if a.KeyCheck != desc.Encryption.KeyCheck {
		t.Fatalf("acceptance keyCheck %q != descriptor keyCheck %q", a.KeyCheck, desc.Encryption.KeyCheck)
	}
	if a.Destination != entry.Destination || a.ArchiveID != entry.ArchiveID || a.Source != *entry.Source ||
		a.Mode != entry.Encryption.Mode || a.Algorithm != entry.Encryption.Algorithm || !a.AcceptedAt.Equal(time.Unix(1_800_000_100, 0)) {
		t.Fatalf("acceptance fields %+v", a)
	}

	withAnn := func(f func(*corev1.Secret)) *corev1.Secret {
		s := sec.DeepCopy()
		f(s)
		return s
	}
	editAcceptance := func(f func(m map[string]any)) *corev1.Secret {
		return withAnn(func(s *corev1.Secret) {
			var m map[string]any
			_ = json.Unmarshal([]byte(s.Annotations[v1alpha1.PITRAcceptedAnnotation]), &m)
			f(m)
			b, _ := json.Marshal(m)
			s.Annotations[v1alpha1.PITRAcceptedAnnotation] = string(b)
		})
	}
	cases := []struct {
		name string
		sec  *corev1.Secret
		want error
	}{
		{"whitespace-padded bytes verify", withAnn(func(s *corev1.Secret) { s.Data[pitrKeySnapshotDataKey] = []byte("\n" + key + " \n") }), nil},
		{"nil secret", nil, errPITRAcceptanceMissing},
		{"no annotation", withAnn(func(s *corev1.Secret) { delete(s.Annotations, v1alpha1.PITRAcceptedAnnotation) }), errPITRAcceptanceMissing},
		{"not json", withAnn(func(s *corev1.Secret) { s.Annotations[v1alpha1.PITRAcceptedAnnotation] = "{" }), errPITRAcceptanceMalformed},
		{"unknown field", editAcceptance(func(m map[string]any) { m["extra"] = 1 }), errPITRAcceptanceMalformed},
		{"unknown version", editAcceptance(func(m map[string]any) { m["schemaVersion"] = 2 }), errPITRAcceptanceMalformed},
		{"bad keyCheck form", editAcceptance(func(m map[string]any) { m["keyCheck"] = "short" }), errPITRAcceptanceMalformed},
		{"mode None", editAcceptance(func(m map[string]any) { m["mode"], m["algorithm"] = "None", "" }), errPITRAcceptanceMalformed},
		{"no acceptedAt", editAcceptance(func(m map[string]any) { delete(m, "acceptedAt") }), errPITRAcceptanceMalformed},
		{"incomplete source", editAcceptance(func(m map[string]any) { m["source"] = map[string]any{"uid": string(pbcUID)} }), errPITRAcceptanceMalformed},
		{"wrong bytes", withAnn(func(s *corev1.Secret) { s.Data[pitrKeySnapshotDataKey] = []byte("rotated") }), errPITRAcceptanceMismatch},
		{"missing key", withAnn(func(s *corev1.Secret) { s.Data = nil }), errPITRAcceptanceMismatch},
		{"destination annotation differs", withAnn(func(s *corev1.Secret) {
			s.Annotations[v1alpha1.PITRDestinationAnnotation] = pbcS3Dest(t, "p2").String()
		}), errPITRAcceptanceMismatch},
		{"wrong fields: archive id edited", editAcceptance(func(m map[string]any) {
			m["archiveID"] = pitrarchive.CreateArchiveID(pbcOther, pbcS3Dest(t, "p1"))
		}), errPITRAcceptanceMismatch},
		{"wrong fields: source edited", editAcceptance(func(m map[string]any) {
			m["source"] = map[string]any{"namespace": "db", "name": "other", "uid": string(pbcUID)}
		}), errPITRAcceptanceMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := verifyPITRAcceptance(c.sec)
			if c.want == nil {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				return
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}

	// newPITRAcceptance refusals.
	refuse := []struct {
		name string
		mut  func(*v1alpha1.PITRArchiveBinding)
		key  string
	}{
		{"not bound", func(e *v1alpha1.PITRArchiveBinding) { e.State = v1alpha1.PITRArchiveBindingPending }, key},
		{"no source", func(e *v1alpha1.PITRArchiveBinding) { e.Source = nil }, key},
		{"plaintext", func(e *v1alpha1.PITRArchiveBinding) {
			e.Encryption = v1alpha1.PITRArchiveBindingEncryption{Mode: v1alpha1.EncryptionModeNone}
		}, key},
		{"no archive id", func(e *v1alpha1.PITRArchiveBinding) { e.ArchiveID = "" }, key},
		{"empty key", func(*v1alpha1.PITRArchiveBinding) {}, " \n"},
	}
	for _, c := range refuse {
		e := entry.DeepCopy()
		c.mut(e)
		if _, err := newPITRAcceptance(e, []byte(c.key), time.Now()); err == nil {
			t.Errorf("newPITRAcceptance(%s) succeeded", c.name)
		}
	}
	if _, err := newPITRAcceptance(nil, []byte(key), time.Now()); err == nil {
		t.Error("newPITRAcceptance(nil) succeeded")
	}
	if _, err := (pitrAcceptance{}).encode(); err == nil {
		t.Error("encode of an empty acceptance succeeded")
	}
}

func TestPITRBindingReleaseParse(t *testing.T) {
	id := pitrBindingID(pbcS3Dest(t, "p1"))
	cases := []struct {
		in   string
		ok   bool
		want string
	}{
		{id + ":confirm=orders", true, id},
		{" " + id + ":confirm=orders\n", true, id},
		{id + ":confirm=other", false, ""},
		{id + ":confirm=", false, ""},
		{id, false, ""},
		{id + "," + id + ":confirm=orders", false, ""},
		{"pab-XYZ:confirm=orders", false, ""},
		{"", false, ""},
	}
	for _, c := range cases {
		got, err := parsePITRBindingRelease(c.in, "orders")
		if (err == nil) != c.ok || got != c.want {
			t.Errorf("parsePITRBindingRelease(%q) = %q,%v; want %q ok=%v", c.in, got, err, c.want, c.ok)
		}
	}
}

func TestPITRBindingReadTerminal(t *testing.T) {
	cases := map[string]bool{
		string(v1alpha1.PITRArchiveBindingReasonArchiveNotFound):           true,
		string(v1alpha1.PITRArchiveBindingReasonArchiveDescriptorMissing):  true,
		string(v1alpha1.PITRArchiveBindingReasonArchiveLegacy):             true,
		string(v1alpha1.PITRArchiveBindingReasonArchiveVersionUnsupported): true,
		string(v1alpha1.PITRArchiveBindingReasonKeyMismatch):               false,
		string(v1alpha1.PITRArchiveBindingReasonArchiveEncryptionMismatch): false,
		string(v1alpha1.PITRArchiveBindingReasonArchiveListFailed):         false,
		string(v1alpha1.PITRArchiveBindingReasonArchiveForeign):            false,
		string(v1alpha1.PITRArchiveBindingReasonArchiveCopy):               false,
		string(v1alpha1.PITRArchiveBindingReasonAuthenticating):            false,
		string(v1alpha1.PITRArchiveBindingReasonEstablishing):              false,
		string(v1alpha1.PITRArchiveBindingReasonKeyMaterialLost):           false,
		v1alpha1.ReasonBindingLimitReached:                                 false,
		"":                                                                 false,
	}
	for reason, want := range cases {
		if got := pitrBindingReadTerminal(reason); got != want {
			t.Errorf("pitrBindingReadTerminal(%q) = %v, want %v", reason, got, want)
		}
	}

	entry := func(state v1alpha1.PITRArchiveBindingState, reason v1alpha1.PITRArchiveBindingReason) *v1alpha1.PITRArchiveBinding {
		return &v1alpha1.PITRArchiveBinding{State: state, Reason: reason, Message: "m"}
	}
	ready := []struct {
		name     string
		entry    *v1alpha1.PITRArchiveBinding
		ready    bool
		terminal bool
	}{
		{"absent", nil, false, false},
		{"bound", entry(v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound), true, false},
		{"key material lost", entry(v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost), false, false},
		{"authenticating", entry(v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating), false, false},
		{"key mismatch", entry(v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonKeyMismatch), false, false},
		{"not found", entry(v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveNotFound), false, true},
	}
	for _, c := range ready {
		err := pitrBindingReadiness(c.entry)
		if (err == nil) != c.ready {
			t.Errorf("%s: readiness = %v", c.name, err)
			continue
		}
		if err == nil {
			continue
		}
		if err.terminal != c.terminal {
			t.Errorf("%s: terminal = %v", c.name, err.terminal)
		}
		if !strings.HasPrefix(err.Error(), "PITRArchiveBindingNotReady: archive binding is ") {
			t.Errorf("%s: message %q does not start with the token", c.name, err.Error())
		}
	}
	e := &errPITRBindingNotReady{reason: v1alpha1.ReasonBindingLimitReached, message: "17th destination"}
	if got := e.Error(); got != "PITRArchiveBindingNotReady: archive binding is absent/BindingLimitReached: 17th destination" {
		t.Errorf("Error() = %q", got)
	}
}

func TestPITRBindingWatchMaps(t *testing.T) {
	r := &MysqlFailoverGroupReconciler{}
	v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "v", Namespace: "db"}}
	if got := r.mapVerificationToGroup(context.Background(), v); got != nil {
		t.Fatalf("map of an unbound verification = %v", got)
	}
	v.Spec.FailoverGroupRef.Name = "orders"
	if got := r.mapVerificationToGroup(context.Background(), v); len(got) != 1 || got[0].Namespace != "db" || got[0].Name != "orders" {
		t.Fatalf("map = %v", got)
	}
	if r.mapVerificationToGroup(context.Background(), &corev1.Secret{}) != nil {
		t.Fatal("non-verification objects must map to nothing")
	}

	snap := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "db", Labels: map[string]string{
		labelAppName: appNamePITRKeySnapshot, v1alpha1.FailoverGroupLabel: "orders",
	}}}
	if got := r.mapPITRKeySnapshotToGroup(context.Background(), snap); len(got) != 1 || got[0].Name != "orders" || got[0].Namespace != "db" {
		t.Fatalf("snapshot map = %v", got)
	}
	pred := pitrKeySnapshotPredicate()
	if !pred.Create(event.CreateEvent{Object: snap}) {
		t.Fatal("predicate refused a snapshot")
	}
	dump := snap.DeepCopy()
	dump.Labels[labelAppName] = appNameDumpKeySnapshot
	if r.mapPITRKeySnapshotToGroup(context.Background(), dump) != nil || pred.Create(event.CreateEvent{Object: dump}) {
		t.Fatal("dump snapshots must not map")
	}
	noGroup := snap.DeepCopy()
	delete(noGroup.Labels, v1alpha1.FailoverGroupLabel)
	if r.mapPITRKeySnapshotToGroup(context.Background(), noGroup) != nil {
		t.Fatal("a snapshot without a group label must not map")
	}
}
