package scenarios

import (
	"bytes"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func s77InitFailureStatus() *pgsidecar.ArchiverStatusResponse {
	ready := false
	return &pgsidecar.ArchiverStatusResponse{
		Enabled:        true,
		StoreReady:     &ready,
		Primary:        true,
		LastError:      "archive store init: list s3://b/p/binlogs/: dial tcp: connection refused",
		StorageType:    "S3",
		ManifestPrefix: "e2e/s77/run/binlogs",
		UploadFailures: 3,
		Archive: &pitrarchive.ArchiveStatus{
			EncryptionMode: pitrarchive.EncryptionPassphrase,
			State:          pitrarchive.StateAwaitingDescriptor,
		},
	}
}

func TestS77CheckStoreInitFailure(t *testing.T) {
	if err := s77CheckStoreInitFailure(s77InitFailureStatus(), "e2e/s77/run/binlogs/", true); err != nil {
		t.Fatalf("documented init failure refused: %v", err)
	}
	ready := true
	for name, edit := range map[string]func(*pgsidecar.ArchiverStatusResponse){
		"disabled":          func(s *pgsidecar.ArchiverStatusResponse) { s.Enabled = false },
		"storeReady absent": func(s *pgsidecar.ArchiverStatusResponse) { s.StoreReady = nil },
		"storeReady true":   func(s *pgsidecar.ArchiverStatusResponse) { s.StoreReady = &ready },
		"archiver error":    func(s *pgsidecar.ArchiverStatusResponse) { s.LastError = "s3 upload x: timeout" },
		"no error":          func(s *pgsidecar.ArchiverStatusResponse) { s.LastError = "" },
		"other prefix":      func(s *pgsidecar.ArchiverStatusResponse) { s.ManifestPrefix = "e2e/other/binlogs" },
		"wrong role":        func(s *pgsidecar.ArchiverStatusResponse) { s.Primary = false },
		"no attempt":        func(s *pgsidecar.ArchiverStatusResponse) { s.UploadFailures = 0 },
		"plaintext":         func(s *pgsidecar.ArchiverStatusResponse) { s.Archive.EncryptionMode = pitrarchive.EncryptionNone },
		"verified":          func(s *pgsidecar.ArchiverStatusResponse) { s.Archive.State = pitrarchive.StateVerified },
		"no archive block":  func(s *pgsidecar.ArchiverStatusResponse) { s.Archive = nil },
	} {
		st := s77InitFailureStatus()
		edit(st)
		if err := s77CheckStoreInitFailure(st, "e2e/s77/run/binlogs", true); err == nil {
			t.Errorf("%s: accepted %+v", name, st)
		}
	}
}

func TestS77PodsDiff(t *testing.T) {
	base := map[string]s77Pod{
		"iad": {Name: "a", UID: "1", Restarts: map[string]int32{"mysql": 0, "sidecar": 1}},
		"pdx": {Name: "b", UID: "2", Restarts: map[string]int32{"mysql": 0, "sidecar": 0}},
	}
	same := map[string]s77Pod{
		"iad": {Name: "a", UID: "1", Restarts: map[string]int32{"mysql": 0, "sidecar": 1}},
		"pdx": {Name: "b", UID: "2", Restarts: map[string]int32{"mysql": 0, "sidecar": 0}},
	}
	if d := s77PodsDiff(base, same); len(d) != 0 {
		t.Fatalf("unchanged pods differ: %v", d)
	}
	for name, tc := range map[string]struct {
		now  map[string]s77Pod
		want string
	}{
		"replaced": {map[string]s77Pod{"iad": same["iad"], "pdx": {Name: "c", UID: "3", Restarts: map[string]int32{"mysql": 0, "sidecar": 0}}}, "replaced"},
		"restart":  {map[string]s77Pod{"iad": {Name: "a", UID: "1", Restarts: map[string]int32{"mysql": 0, "sidecar": 2}}, "pdx": same["pdx"]}, "sidecar restarted"},
		"lost":     {map[string]s77Pod{"iad": same["iad"]}, "pdx has no pod"},
		"no status": {map[string]s77Pod{"iad": {Name: "a", UID: "1", Restarts: map[string]int32{"sidecar": 1}}, "pdx": same["pdx"]},
			"no mysql container status"},
	} {
		d := s77PodsDiff(base, tc.now)
		if len(d) != 1 || !strings.Contains(d[0], tc.want) {
			t.Errorf("%s: diff %v, want one entry containing %q", name, d, tc.want)
		}
	}
}

func TestS77CheckEncryptedBinlog(t *testing.T) {
	pass := []byte("0123456789abcdef0123456789abcdef")
	binlog := append(append([]byte{}, s77BinlogMagic...), bytes.Repeat([]byte{7}, 300)...)
	var enc bytes.Buffer
	if _, err := backupcrypto.Encrypt(&enc, bytes.NewReader(binlog), pass); err != nil {
		t.Fatal(err)
	}
	if err := s77CheckEncryptedBinlog(enc.Bytes(), pass); err != nil {
		t.Fatalf("encrypted binlog refused: %v", err)
	}
	if err := s77CheckEncryptedBinlog(binlog, pass); err == nil || !strings.Contains(err.Error(), "BRV1") {
		t.Errorf("plaintext binlog: %v", err)
	}
	if err := s77CheckEncryptedBinlog(enc.Bytes(), []byte("another passphrase of some length")); err == nil {
		t.Error("decrypted with the wrong passphrase")
	}
	var other bytes.Buffer
	if _, err := backupcrypto.Encrypt(&other, strings.NewReader(`{"files":[]}`), pass); err != nil {
		t.Fatal(err)
	}
	if err := s77CheckEncryptedBinlog(other.Bytes(), pass); err == nil || !strings.Contains(err.Error(), "not a MySQL binary log") {
		t.Errorf("encrypted manifest accepted as a binlog: %v", err)
	}
}

func TestS77InitFailedCondition(t *testing.T) {
	msg := "iad: archive store init: refused; pdx: archive store init: refused"
	mfg := &v1alpha1.MysqlFailoverGroup{Status: v1alpha1.MysqlFailoverGroupStatus{
		Conditions: []metav1.Condition{{Type: v1alpha1.ConditionPITRArchiveReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonArchiveStoreInitFailed, Message: msg}},
		PITR:       &v1alpha1.PITRStatus{Message: msg},
	}}
	if ok, why := s77InitFailedCondition(mfg, []string{"iad", "pdx"}); !ok {
		t.Fatalf("documented condition refused: %s", why)
	}
	if ok, _ := s77InitFailedCondition(mfg, []string{"iad", "pdx", "dc3"}); ok {
		t.Error("a site missing from the message was accepted")
	}
	mfg.Status.PITR.Message = "iad: archive store init: refused"
	if ok, _ := s77InitFailedCondition(mfg, []string{"iad", "pdx"}); ok {
		t.Error("status.pitr.message without pdx was accepted")
	}
	mfg.Status.PITR.Message = msg
	mfg.Status.Conditions[0].Reason = v1alpha1.ReasonArchiverError
	if ok, _ := s77InitFailedCondition(mfg, []string{"iad", "pdx"}); ok {
		t.Error("ArchiverError accepted")
	}
}
