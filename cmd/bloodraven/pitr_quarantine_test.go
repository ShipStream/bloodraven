package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

const (
	quarantineTestUID = "00000000-0000-4000-8000-000000000a01"
	quarantineTestSID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
)

func quarantineTestDestination(t testing.TB) pitrarchive.Destination {
	t.Helper()
	d, err := pitrarchive.NewPVCDestination("ns", "archive", "")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// seedQuarantineDescriptor writes a plaintext descriptor for source on store.
func seedQuarantineDescriptor(t testing.TB, store sidecar.ArchiveStore, source pitrarchive.Source) string {
	t.Helper()
	d, err := pitrarchive.NewDescriptor(source, quarantineTestDestination(t), pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}, nil, "iad", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := d.Canonical()
	if err := store.Put(context.Background(), pitrarchive.DescriptorKey("binlogs"), bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	return d.ArchiveID
}

func setQuarantineEnv(t *testing.T, root string) {
	t.Helper()
	useTempDirMountVerifier(t)
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", root)
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "binlogs")
	t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "")
	t.Setenv("POD_NAMESPACE", "ns")
	t.Setenv("FAILOVER_GROUP", "orders")
	t.Setenv("BLOODRAVEN_FAILOVER_GROUP_UID", quarantineTestUID)
	t.Setenv("BLOODRAVEN_PITR_DESTINATION", quarantineTestDestination(t).String())
	t.Setenv("BLOODRAVEN_PITR_ENCRYPTION_MODE", "None")
}

func quarantineTestStore(t *testing.T, root string) sidecar.ArchiveStore {
	t.Helper()
	store, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: root, MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func putQuarantineManifest(t *testing.T, store sidecar.ArchiveStore, m sidecar.Manifest) {
	t.Helper()
	m.Version = sidecar.ManifestVersion
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), sidecar.ManifestKey("binlogs", m.Site), bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
}

func TestPITRQuarantineCommandGoesThroughTheWriterSession(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	setQuarantineEnv(t, root)
	store := quarantineTestStore(t, root)
	archiveID := seedQuarantineDescriptor(t, store, pitrarchive.Source{Namespace: "ns", Name: "orders", UID: quarantineTestUID})
	row := sidecar.ManifestEntry{Name: "mysql-bin.legacy"}
	at := time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC)
	putQuarantineManifest(t, store, sidecar.Manifest{ArchiveID: archiveID, Site: "iad", Files: []sidecar.ManifestEntry{row},
		Promotions:       []sidecar.PromotionRecord{{ServerUUID: "u", ServerID: 101, GTIDExecuted: "u:1-3", ObservedAt: at}},
		HistoryIntegrity: &sidecar.HistoryIntegrity{Violation: pitrarchive.ViolationBinlogFormat, Detail: "binlog_format=MIXED", At: at}})
	args := []string{"--site=iad", "--name=" + row.Name, "--id=legacy", "--reason=lost object; canonical backup verified", "--resume-gtids=" + quarantineTestSID + ":1-20", "--confirm"}
	if err := pitrQuarantine(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	m, err := sidecar.LoadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 0 || len(m.Quarantined) != 1 || m.ArchiveID != archiveID {
		t.Fatalf("quarantine: %+v, %v", m, err)
	}
	if len(m.Promotions) != 1 || m.HistoryIntegrity == nil {
		t.Fatalf("quarantine dropped the site's records or marker: %+v", m)
	}
	if err := pitrQuarantine(ctx, []string{"--site=iad", "--name=" + row.Name, "--id=legacy", "--undo", "--confirm"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if m, err := sidecar.LoadManifest(ctx, store, "binlogs", "iad"); err != nil || len(m.Files) != 1 || len(m.Quarantined) != 0 {
		t.Fatalf("undo: %+v, %v", m, err)
	}
}

func TestPITRQuarantineCommandRefusals(t *testing.T) {
	args := []string{"--site=iad", "--name=mysql-bin.legacy", "--id=legacy", "--reason=r", "--resume-gtids=" + quarantineTestSID + ":1-20", "--confirm"}
	tests := []struct {
		name  string
		setup func(t *testing.T, store sidecar.ArchiveStore)
		env   map[string]string
		want  string
	}{
		{name: "another group's archive", setup: func(t *testing.T, store sidecar.ArchiveStore) {
			id := seedQuarantineDescriptor(t, store, pitrarchive.Source{Namespace: "ns", Name: "other", UID: "00000000-0000-4000-8000-0000000000ff"})
			putQuarantineManifest(t, store, sidecar.Manifest{ArchiveID: id, Site: "iad", Files: []sidecar.ManifestEntry{{Name: "mysql-bin.legacy"}}})
		}, want: string(pitrarchive.TokenArchiveForeign)},
		{name: "no archive", setup: func(*testing.T, sidecar.ArchiveStore) {}, want: string(pitrarchive.TokenArchiveNotFound)},
		{name: "legacy prefix", setup: func(t *testing.T, store sidecar.ArchiveStore) {
			body := []byte(`{"version":1,"site":"iad","files":[{"name":"mysql-bin.legacy"}]}`)
			_ = store.Put(context.Background(), "binlogs/manifest-iad.json", bytes.NewReader(body), int64(len(body)))
		}, want: string(pitrarchive.TokenArchiveLegacy)},
		{name: "no writer identity", setup: func(*testing.T, sidecar.ArchiveStore) {}, env: map[string]string{"BLOODRAVEN_FAILOVER_GROUP_UID": ""}, want: "BLOODRAVEN_FAILOVER_GROUP_UID"},
		{name: "destination of another prefix", setup: func(*testing.T, sidecar.ArchiveStore) {}, env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": "pvc://ns/archive/other/binlogs"}, want: "key prefix"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			setQuarantineEnv(t, root)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			store := quarantineTestStore(t, root)
			tt.setup(t, store)
			before, _ := store.List(context.Background(), "")
			err := pitrQuarantine(context.Background(), args, io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("pitrQuarantine = %v, want %q", err, tt.want)
			}
			after, _ := store.List(context.Background(), "")
			if len(after) != len(before) {
				t.Fatalf("a refused quarantine wrote objects: %v -> %v", before, after)
			}
		})
	}
}

// Review F136: the no-archive refusal names no operation, so it reads
// right for --acknowledge-history as well as for a quarantine.
func TestPITRQuarantineNoArchiveIsOperationNeutral(t *testing.T) {
	for _, args := range [][]string{
		{"--site=iad", "--name=mysql-bin.legacy", "--id=legacy", "--reason=r", "--resume-gtids=" + quarantineTestSID + ":1-20", "--confirm"},
		{"--site=iad", "--acknowledge-history", "--reason=r", "--resume-gtids=" + quarantineTestSID + ":1-20", "--confirm"},
	} {
		root := t.TempDir()
		setQuarantineEnv(t, root)
		err := pitrQuarantine(context.Background(), args, io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), string(pitrarchive.TokenArchiveNotFound)) ||
			!strings.Contains(err.Error(), "nothing to update") || strings.Contains(err.Error(), "quarantine") {
			t.Fatalf("%v: err = %v", args, err)
		}
	}
}

// Review F135: --resume-gtids is recorded and checked, never a restore
// exclusion; the help must not promise one.
func TestPITRQuarantineResumeGTIDsHelp(t *testing.T) {
	var help bytes.Buffer
	if err := pitrQuarantine(context.Background(), []string{"--help"}, io.Discard, &help); err != nil {
		t.Fatal(err)
	}
	text := help.String()
	if strings.Contains(text, "exclusion") || !strings.Contains(text, "excludeGtids still decides") {
		t.Fatalf("resume-gtids help:\n%s", text)
	}
}
