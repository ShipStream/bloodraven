package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

const downloadPrefix = "orders/binlogs"

func downloadDest(t *testing.T) pitrarchive.Destination {
	t.Helper()
	d, err := pitrarchive.NewPVCDestination("ns", "backups", "orders")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func putObject(t *testing.T, store sidecar.ArchiveStore, key string, b []byte) {
	t.Helper()
	if err := store.Put(context.Background(), key, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
}

func putDescriptor(t *testing.T, store sidecar.ArchiveStore, dest pitrarchive.Destination) *pitrarchive.Descriptor {
	t.Helper()
	d, err := pitrarchive.NewDescriptor(pitrarchive.Source{Namespace: "ns", Name: "orders", UID: "orders-uid"}, dest,
		pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}, nil, "dc1", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	putObject(t, store, pitrarchive.DescriptorKey(downloadPrefix), b)
	return d
}

func putManifest(t *testing.T, store sidecar.ArchiveStore, m sidecar.Manifest) {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	putObject(t, store, sidecar.ManifestKey(downloadPrefix, m.Site), b)
}

// #206 series 1 (D-40, O2-22): pitr-download opens the archive as a reader
// and checks its identity; an archive that passes refuses
// PITRReplayUnsupported, and every identity failure refuses with its own
// token first.
func TestOpenPITRArchive(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, store sidecar.ArchiveStore, dest pitrarchive.Destination)
		enc   pitrarchive.EncryptionIdentity
		want  pitrarchive.Token
		text  string
	}{
		{name: "verified v2 archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			id := putDescriptor(t, s, d).ArchiveID
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: id, Site: "dc1"})
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: id, Site: "dc2"})
		}, want: pitrarchive.TokenReplayUnsupported, text: "2 site manifests [dc1 dc2]"},
		{name: "empty prefix", setup: func(*testing.T, sidecar.ArchiveStore, pitrarchive.Destination) {}, want: pitrarchive.TokenArchiveNotFound},
		{name: "legacy v1 manifests", setup: func(t *testing.T, s sidecar.ArchiveStore, _ pitrarchive.Destination) {
			putObject(t, s, sidecar.ManifestKey(downloadPrefix, "dc1"), []byte(`{"version":1,"site":"dc1","files":[]}`))
		}, want: pitrarchive.TokenArchiveLegacy},
		{name: "descriptor missing", setup: func(t *testing.T, s sidecar.ArchiveStore, _ pitrarchive.Destination) {
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa", Site: "dc1"})
		}, want: pitrarchive.TokenArchiveDescriptorMissing},
		{name: "encryption mismatch", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			putDescriptor(t, s, d)
		}, enc: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}, want: pitrarchive.TokenArchiveEncryptionMismatch},
		{name: "manifest of another archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			putDescriptor(t, s, d)
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: "ar1-bbbbbbbbbbbbbbbbbbbbbbbbbb", Site: "dc1"})
		}, want: pitrarchive.TokenArchiveIdentityMismatch},
		{name: "legacy manifest in a v2 archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			putDescriptor(t, s, d)
			putObject(t, s, sidecar.ManifestKey(downloadPrefix, "dc1"), []byte(`{"version":1,"site":"dc1","files":[]}`))
		}, want: pitrarchive.TokenArchiveLegacy},
		{name: "verified reuse refuses the whole archive", setup: func(t *testing.T, s sidecar.ArchiveStore, d pitrarchive.Destination) {
			id := putDescriptor(t, s, d).ArchiveID
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: id, Site: "dc1", Epoch: &sidecar.ArchiveEpoch{ResetReason: "GTID reuse verified"}})
		}, want: pitrarchive.TokenArchiveHistoryReset},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := stampTestStore(t)
			dest := downloadDest(t)
			tt.setup(t, store, dest)
			enc := tt.enc
			if enc.Mode == "" {
				enc.Mode = pitrarchive.EncryptionNone
			}
			err := openPITRArchive(context.Background(), store, pitrArchiveRequest{
				prefix: downloadPrefix,
				expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: dest, Encryption: enc},
			})
			if err == nil {
				t.Fatal("pitr-download must refuse in series 1")
			}
			if tok, ok := pitrarchive.ParseToken(err.Error()); !ok || tok != tt.want || !strings.Contains(err.Error(), tt.text) {
				t.Fatalf("err = %v, want token %s with %q", err, tt.want, tt.text)
			}
		})
	}
}

func TestPITRArchiveRequestFromEnv(t *testing.T) {
	dest, err := pitrarchive.NewS3Destination("b", "orders", "")
	if err != nil {
		t.Fatal(err)
	}
	env := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	base := map[string]string{
		"BLOODRAVEN_PITR_DESTINATION":     dest.String(),
		"BLOODRAVEN_PITR_MANIFEST_PREFIX": dest.KeyPrefix,
		"BLOODRAVEN_PITR_ENCRYPTION_MODE": "None",
	}
	req, err := pitrArchiveRequestFromEnv(env(base))
	if err != nil || req.prefix != "orders/binlogs" || req.expect.Destination != dest || req.expect.Role != pitrarchive.RoleReader {
		t.Fatalf("request %+v, err %v", req, err)
	}
	for name, mutate := range map[string]func(map[string]string){
		"prefix disagrees with the destination": func(m map[string]string) { m["BLOODRAVEN_PITR_MANIFEST_PREFIX"] = "/orders/binlogs" },
		"non-canonical destination":             func(m map[string]string) { m["BLOODRAVEN_PITR_DESTINATION"] = "s3://b/orders/binlogs" },
		"unknown mode":                          func(m map[string]string) { m["BLOODRAVEN_PITR_ENCRYPTION_MODE"] = "Kms" },
		"passphrase mode without a key":         func(m map[string]string) { m["BLOODRAVEN_PITR_ENCRYPTION_MODE"] = "Passphrase" },
		"key without passphrase mode":           func(m map[string]string) { m["BLOODRAVEN_PITR_PASSPHRASE_FILE"] = "/k" },
	} {
		kv := map[string]string{}
		for k, v := range base {
			kv[k] = v
		}
		mutate(kv)
		if _, err := pitrArchiveRequestFromEnv(env(kv)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	base["BLOODRAVEN_PITR_ENCRYPTION_MODE"], base["BLOODRAVEN_PITR_PASSPHRASE_FILE"] = "Passphrase", "/k"
	if req, err := pitrArchiveRequestFromEnv(env(base)); err != nil || req.expect.Encryption.Algorithm != pitrarchive.AlgorithmAES256GCM {
		t.Fatalf("passphrase request %+v, %v", req, err)
	}
}
