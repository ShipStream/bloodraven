package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

const v2TestArchiveID = "ar1-abcdefghijklmnopqrstuvwxyz"

func TestManifestV2SaveStampsVersionAndRoundTrips(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	at := time.Date(2026, 11, 4, 20, 1, 2, 123456000, time.UTC)
	m := &Manifest{
		Version:   1, // save stamps the current version regardless
		ArchiveID: v2TestArchiveID,
		Site:      "a",
		Files: []ManifestEntry{{
			Name: "mysql-bin.000010", ID: "id1", RemotePath: "binlogs/a/mysql-bin.000010", Size: 10, ArchivedAt: at,
			GTIDCoverage: &BinlogGTIDCoverage{
				Complete: true, GTIDs: "u:1-10", ContentID: "id1",
				OriginalCommitMin: 1_700_000_000_000_001, OriginalCommitMax: 1_700_000_000_999_999,
				EmptyGTIDs: "u:4",
			},
		}},
		Promotions: []PromotionRecord{{ServerUUID: "u", ServerID: 101, GTIDExecuted: "u:1-3", ObservedAt: at}},
		HistoryIntegrity: &HistoryIntegrity{
			Violation: pitrarchive.ViolationBinlogRowImage, Detail: "binlog_row_image=MINIMAL", At: at,
		},
	}
	if err := saveManifest(ctx, store, "binlogs", m); err != nil {
		t.Fatalf("saveManifest: %v", err)
	}
	raw, _, _ := store.Get(ctx, manifestKey("binlogs", "a"))
	for _, want := range []string{`"version": 2`, `"archiveId": "` + v2TestArchiveID + `"`, `"originalCommitMin": 1700000000000001`,
		`"emptyGtids": "u:4"`, `"promotions"`, `"serverUuid": "u"`, `"serverId": 101`, `"historyIntegrity"`, `"violation": "BinlogRowImage"`} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Errorf("saved manifest lacks %s:\n%s", want, raw)
		}
	}
	got, err := loadManifest(ctx, store, "binlogs", "a")
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if got.Version != ManifestVersion || got.ArchiveID != v2TestArchiveID {
		t.Fatalf("loaded version/archiveId = %d/%q", got.Version, got.ArchiveID)
	}
	if c := got.Files[0].GTIDCoverage; c == nil || c.OriginalCommitMin != 1_700_000_000_000_001 || c.OriginalCommitMax != 1_700_000_000_999_999 || c.EmptyGTIDs != "u:4" {
		t.Fatalf("hints = %+v", got.Files[0].GTIDCoverage)
	}
	if len(got.Promotions) != 1 || got.Promotions[0] != m.Promotions[0] {
		t.Fatalf("promotions = %+v", got.Promotions)
	}
	if got.HistoryIntegrity == nil || *got.HistoryIntegrity != *m.HistoryIntegrity {
		t.Fatalf("historyIntegrity = %+v", got.HistoryIntegrity)
	}
}

func TestManifestV2AbsentIsEmptyV2(t *testing.T) {
	m, err := loadManifest(context.Background(), newMemStore(), "binlogs", "a")
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != ManifestVersion || m.ArchiveID != "" || m.Site != "a" {
		t.Fatalf("absent manifest = %+v", m)
	}
}

func TestManifestVersionRefusals(t *testing.T) {
	ctx := context.Background()
	tooMany := make([]PromotionRecord, MaxPromotionRecords+1)
	tooManyJSON, _ := json.Marshal(tooMany)
	tests := []struct {
		name  string
		doc   string
		want  error
		token pitrarchive.Token
	}{
		{"no version", `{"site":"a","files":[]}`, ErrManifestLegacy, pitrarchive.TokenArchiveLegacy},
		{"version 1", `{"version":1,"site":"a","files":[]}`, ErrManifestLegacy, pitrarchive.TokenArchiveLegacy},
		{"version 3", `{"version":3,"archiveId":"` + v2TestArchiveID + `","site":"a","files":[]}`, ErrManifestVersionUnsupported, pitrarchive.TokenArchiveVersionUnsupported},
		{"v2 without archiveId", `{"version":2,"site":"a","files":[]}`, ErrManifestInvalid, pitrarchive.TokenArchiveObjectInvalid},
		{"v2 too many promotions", `{"version":2,"archiveId":"` + v2TestArchiveID + `","site":"a","files":[],"promotions":` + string(tooManyJSON) + `}`, ErrManifestInvalid, pitrarchive.TokenArchiveObjectInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMemStore()
			if err := store.Put(ctx, manifestKey("binlogs", "a"), bytes.NewReader([]byte(tt.doc)), int64(len(tt.doc))); err != nil {
				t.Fatal(err)
			}
			_, err := loadManifest(ctx, store, "binlogs", "a")
			if !errors.Is(err, tt.want) {
				t.Fatalf("loadManifest err = %v, want %v", err, tt.want)
			}
			if tok, ok := pitrarchive.ParseToken(err.Error()); !ok || tok != tt.token {
				t.Fatalf("message %q does not start with token %s", err, tt.token)
			}
			if _, err := LoadManifest(ctx, store, "binlogs", "a"); !errors.Is(err, tt.want) {
				t.Fatalf("exported LoadManifest err = %v", err)
			}
		})
	}
}

func TestManifestV2SaveRefusesMissingArchiveID(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "a"})
	if !errors.Is(err, ErrManifestInvalid) {
		t.Fatalf("saveManifest without archiveId = %v, want ErrManifestInvalid", err)
	}
	if _, ok, _ := store.Get(ctx, manifestKey("binlogs", "a")); ok {
		t.Fatal("a refused save wrote the object")
	}
}

func TestManifestAppendPromotionRecordCap(t *testing.T) {
	var m Manifest
	for i := 0; i < MaxPromotionRecords+10; i++ {
		m.AppendPromotionRecord(PromotionRecord{ServerUUID: fmt.Sprintf("u%d", i), ServerID: 101})
	}
	if len(m.Promotions) != MaxPromotionRecords {
		t.Fatalf("kept %d records, want %d", len(m.Promotions), MaxPromotionRecords)
	}
	if m.Promotions[0].ServerUUID != "u10" || m.Promotions[MaxPromotionRecords-1].ServerUUID != fmt.Sprintf("u%d", MaxPromotionRecords+9) {
		t.Fatalf("cap must drop the oldest: first %q last %q", m.Promotions[0].ServerUUID, m.Promotions[MaxPromotionRecords-1].ServerUUID)
	}
	m.ArchiveID, m.Site = v2TestArchiveID, "a"
	if err := saveManifest(context.Background(), newMemStore(), "binlogs", &m); err != nil {
		t.Fatalf("save at the cap: %v", err)
	}
}

// TestArchiverStatusWireCompatibleWithClient pins that the sidecar's
// /archiver/status additions decode into the operator's client type.
func TestArchiverStatusWireCompatibleWithClient(t *testing.T) {
	at := time.Date(2026, 11, 4, 20, 1, 2, 123456000, time.UTC)
	s := Status{
		Enabled: true,
		Archive: &pitrarchive.ArchiveStatus{
			Destination: "s3://b/binlogs?endpoint=aws", ArchiveID: v2TestArchiveID,
			EncryptionMode: pitrarchive.EncryptionPassphrase, State: pitrarchive.StateRefused,
			Reason: pitrarchive.ReasonArchiveKeyMismatch, KeyRevision: "mysql-orders-pitr-key-0123456789",
		},
		DivergentRows:       2,
		QuarantinedRows:     3,
		PromotionRecordedAt: at,
		HistoryIntegrity:    &HistoryIntegrity{Violation: pitrarchive.ViolationCommitOrder, Detail: "replica_preserve_commit_order=0", At: at},
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var c internalmysql.ArchiverStatus
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.Archive == nil || *c.Archive != *s.Archive || c.DivergentRows != 2 || c.QuarantinedRows != 3 ||
		!c.PromotionRecordedAt.Equal(at) || c.HistoryIntegrity == nil || *c.HistoryIntegrity != *s.HistoryIntegrity {
		t.Fatalf("client decoded %+v from %s", c, b)
	}
	// Absent optional blocks stay absent on the wire.
	b, _ = json.Marshal(Status{Enabled: true})
	for _, key := range []string{`"archive"`, `"promotionRecordedAt"`, `"historyIntegrity"`} {
		if bytes.Contains(b, []byte(key)) {
			t.Errorf("empty status carries %s: %s", key, b)
		}
	}
}
