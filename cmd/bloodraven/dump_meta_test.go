package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

const dumpPrefix = "orders/nightly-1"

func dumpStamp(end bool) backupmeta.Stamp {
	start := backupmeta.NewMicroTime(time.Date(2026, 11, 4, 20, 0, 0, 123456000, time.UTC))
	s := backupmeta.Stamp{Kind: backupmeta.StampKind, Version: backupmeta.StampVersion,
		Backup: backupmeta.ObjectRef{Namespace: "ns", Name: "nightly-1", UID: "backup-uid"}, Source: backupmeta.ObjectRef{Namespace: "ns", Name: "orders", UID: "fg-uid"},
		PITRArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa", Site: "pdx", ServerID: 102, ServerClock: backupmeta.ServerClock{Start: start}, CreatedAt: start}
	if end {
		s.ServerClock.End = backupmeta.NewMicroTime(start.Add(90 * time.Second))
	}
	return s
}

type dumpObjects struct {
	stamp *backupmeta.Stamp
	dump  string // @.json
	done  string // @.done.json
}

func putDump(t *testing.T, store sidecar.ArchiveStore, o dumpObjects) {
	t.Helper()
	if o.stamp != nil {
		b, err := o.stamp.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		putObject(t, store, backupmeta.StampKey(dumpPrefix), b)
	}
	if o.dump != "" {
		putObject(t, store, dumpPrefix+"/@.json", []byte(o.dump))
	}
	if o.done != "" {
		putObject(t, store, dumpPrefix+"/@.done.json", []byte(o.done))
	}
}

func TestLoadDumpMeta(t *testing.T) {
	const sid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	full := dumpStamp(true)
	unfinished := dumpStamp(false)
	filtered := dumpStamp(true)
	filtered.DumpFilters.ExcludeSchemas = []string{"scratch"}
	goodDump := `{"version":"2.0.1","gtidExecuted":"` + sid + `:1-40,\n` + sid + `:42","gtidExecutedInconsistent":false}`
	goodDone := `{"end":"2026-11-04 20:01:30"}`
	tests := []struct {
		name   string
		objs   dumpObjects
		expect dumpExpectation
		want   pitrarchive.Token
	}{
		{name: "complete", objs: dumpObjects{&full, goodDump, goodDone}, expect: dumpExpectation{backupUID: "backup-uid", sourceUID: "fg-uid", gtidExecuted: sid + ":1-40"}},
		{name: "direct source, no expectations", objs: dumpObjects{&full, goodDump, goodDone}},
		{name: "stamp missing", objs: dumpObjects{nil, goodDump, goodDone}, want: pitrarchive.TokenBackupIdentityUnknown},
		{name: "backup uid mismatch", objs: dumpObjects{&full, goodDump, goodDone}, expect: dumpExpectation{backupUID: "other"}, want: pitrarchive.TokenBackupIdentityMismatch},
		{name: "source uid mismatch", objs: dumpObjects{&full, goodDump, goodDone}, expect: dumpExpectation{sourceUID: "other"}, want: pitrarchive.TokenBackupIdentityMismatch},
		{name: "status gtidExecuted not in @.json", objs: dumpObjects{&full, goodDump, goodDone}, expect: dumpExpectation{gtidExecuted: sid + ":1-41"}, want: pitrarchive.TokenBackupIdentityMismatch},
		{name: "inconsistent", objs: dumpObjects{&full, `{"gtidExecuted":"` + sid + `:1-40","gtidExecutedInconsistent":true}`, goodDone}, want: pitrarchive.TokenBackupInconsistent},
		{name: "filtered", objs: dumpObjects{&filtered, goodDump, goodDone}, want: pitrarchive.TokenBackupFiltered},
		{name: "no @.done.json", objs: dumpObjects{&full, goodDump, ""}, want: pitrarchive.TokenBackupIncomplete},
		{name: "stamp without serverClock.end", objs: dumpObjects{&unfinished, goodDump, goodDone}, want: pitrarchive.TokenBackupIncomplete},
		{name: "no @.json", objs: dumpObjects{&full, "", goodDone}, want: pitrarchive.TokenBackupIncomplete},
	}
	for _, tt := range tests {
		for _, encrypted := range []bool{false, true} {
			t.Run(tt.name+map[bool]string{true: " encrypted"}[encrypted], func(t *testing.T) {
				store := stampTestStore(t)
				if encrypted {
					store = sidecar.WrapWithEncryption(store, []byte("dump key"))
				}
				putDump(t, store, tt.objs)
				meta, err := loadDumpMeta(context.Background(), store, dumpPrefix, "s3://dumps/orders/nightly-1/", tt.expect)
				if tt.want != "" {
					if tok, ok := pitrarchive.ParseToken(errString(err)); !ok || tok != tt.want {
						t.Fatalf("err = %v, want %s", err, tt.want)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if meta.Version != 1 || meta.GTIDExecuted != sid+":1-40:42" || meta.End.String() != "2026-11-04T20:01:30.000000Z" ||
					!meta.ServerClock.End.Equal(full.ServerClock.End.Time) || meta.Stamp.ServerID != 102 || meta.Location != "s3://dumps/orders/nightly-1/" {
					t.Fatalf("meta = %+v", meta)
				}
				dir := t.TempDir()
				if err := writeDumpMeta(dir, meta); err != nil {
					t.Fatal(err)
				}
				back, err := readDumpMetaFile(filepath.Join(dir, dumpMetaJSONName))
				if err != nil || back.GTIDExecuted != meta.GTIDExecuted || !back.ServerClock.End.Equal(meta.ServerClock.End.Time) {
					t.Fatalf("dump.json round trip = %+v, %v", back, err)
				}
			})
		}
	}
	// An encrypted dump read without its key is not trusted as plaintext.
	store := stampTestStore(t)
	putDump(t, sidecar.WrapWithEncryption(store, []byte("dump key")), dumpObjects{&full, goodDump, goodDone})
	if _, err := loadDumpMeta(context.Background(), sidecar.WrapWithEncryption(store, []byte("wrong key")), dumpPrefix, "l", dumpExpectation{}); err == nil {
		t.Fatal("a wrong dump key was accepted")
	}
}

func TestParseDumpEnd(t *testing.T) {
	for in, want := range map[string]string{
		"2026-11-04 20:01:30":           "2026-11-04T20:01:30.000000Z",
		"2026-11-04 20:01:30.5":         "2026-11-04T20:01:30.500000Z",
		"2026-11-04T20:01:30.1234567Z":  "2026-11-04T20:01:30.123457Z",
		"2026-11-04T22:01:30.000001+02": "",
	} {
		got, err := parseDumpEnd(in)
		if want == "" {
			if err == nil {
				t.Errorf("%q accepted as %s", in, got)
			}
			continue
		}
		if err != nil || got.String() != want {
			t.Errorf("parseDumpEnd(%q) = %s, %v; want %s", in, got, err, want)
		}
	}
}

func TestDumpMetaStoreConfig(t *testing.T) {
	t.Setenv("BLOODRAVEN_S3_BUCKET", "dumps")
	cfg, key, err := dumpMetaStoreConfig("S3", "orders/nightly-1/")
	if err != nil || key != "orders/nightly-1" || cfg.S3 == nil || cfg.S3.Bucket != "dumps" || cfg.S3.DefaultRegion != "us-east-1" {
		t.Fatalf("S3 config %+v %q %v", cfg, key, err)
	}
	mount := t.TempDir()
	t.Setenv("BLOODRAVEN_PVC_MOUNT_PATH", mount)
	cfg, key, err = dumpMetaStoreConfig("PVC", "orders/nightly-1")
	if err != nil || key != "orders/nightly-1" || cfg.PVC == nil || cfg.PVC.MountPath != mount || cfg.PVC.Writable {
		t.Fatalf("PVC config %+v %q %v", cfg, key, err)
	}
	if _, err := dumpExpectationFromEnv(func(k string) string {
		if k == "BLOODRAVEN_DUMP_EXPECTED_GTID_EXECUTED" {
			return "bad"
		}
		return ""
	}); err == nil {
		t.Fatal("a malformed expected gtidExecuted was accepted")
	}
}

// dump.json's keys are the §4.2 contract pitr-download and WP-B2 read.
func TestDumpJSONShape(t *testing.T) {
	dir := t.TempDir()
	s := dumpStamp(true)
	if err := writeDumpMeta(dir, backupmeta.DumpMeta{Version: 1, Location: "l", GTIDExecuted: "", End: s.ServerClock.End, ServerClock: s.ServerClock, Stamp: s}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, dumpMetaJSONName))
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for k := range top {
		keys = append(keys, k)
	}
	for _, k := range []string{"version", "location", "gtidExecuted", "gtidExecutedInconsistent", "end", "serverClock", "stamp"} {
		if _, ok := top[k]; !ok {
			t.Errorf("dump.json lacks %q (has %s)", k, strings.Join(keys, ","))
		}
	}
}
