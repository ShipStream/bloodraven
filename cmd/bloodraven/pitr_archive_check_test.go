package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ktypes "k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

const checkGroupUID = "00000000-0000-4000-8000-000000000203"

var checkSource = pitrarchive.Source{Namespace: "ns", Name: "orders", UID: checkGroupUID}

// checkStoreWrap lets a case wrap the store the command builds.
type checkStoreWrap func(sidecar.ArchiveStore) sidecar.ArchiveStore

// listDeniedStore refuses every listing with an S3 403.
type listDeniedStore struct{ sidecar.ArchiveStore }

type checkForbidden struct{}

func (checkForbidden) Error() string       { return "api error AccessDenied: Access Denied" }
func (checkForbidden) HTTPStatusCode() int { return 403 }

func (s listDeniedStore) List(context.Context, string) ([]string, error) {
	return nil, checkForbidden{}
}

// flakyListStore fails the first n listings with a transport error.
type flakyListStore struct {
	sidecar.ArchiveStore
	n *int
}

func (s flakyListStore) List(ctx context.Context, prefix string) ([]string, error) {
	if *s.n > 0 {
		*s.n--
		return nil, errors.New("connection reset by peer")
	}
	return s.ArchiveStore.List(ctx, prefix)
}

func putCheckDescriptor(t *testing.T, store sidecar.ArchiveStore, src pitrarchive.Source, dest pitrarchive.Destination, enc pitrarchive.EncryptionIdentity, key []byte) *pitrarchive.Descriptor {
	t.Helper()
	d, err := pitrarchive.NewDescriptor(src, dest, enc, key, "dc1", time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC))
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

// pitr-archive-check reports every archive state as one verdict line, for
// both roles, on a PVC archive (and an S3 403 for ListFailed).
func TestPITRArchiveCheck(t *testing.T) {
	dest := downloadDest(t)
	none := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}
	enc := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}
	copyDest, err := pitrarchive.NewPVCDestination("ns", "copy", "orders")
	if err != nil {
		t.Fatal(err)
	}
	foreign := pitrarchive.Source{Namespace: "other-ns", Name: "billing", UID: "00000000-0000-4000-8000-0000000000ff"}
	v1 := []byte(`{"version":1,"site":"dc1","files":[]}`)

	tests := []struct {
		name      string
		role      string
		key       string // candidate key ("" = plaintext)
		seed      func(t *testing.T, s sidecar.ArchiveStore)
		wrap      checkStoreWrap
		wantState string
		wantToken pitrarchive.Token
		wantSrc   pitrarchive.Source
	}{
		{name: "writer: empty prefix is Absent", role: controller.PITRArchiveCheckRoleWriter, seed: func(*testing.T, sidecar.ArchiveStore) {},
			wantState: controller.PITRArchiveCheckStateAbsent},
		{name: "reader: empty prefix is NotFound", role: controller.PITRArchiveCheckRoleReader, seed: func(*testing.T, sidecar.ArchiveStore) {},
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveNotFound},
		{name: "writer: own plaintext archive verifies", role: controller.PITRArchiveCheckRoleWriter,
			seed:      func(t *testing.T, s sidecar.ArchiveStore) { putCheckDescriptor(t, s, checkSource, dest, none, nil) },
			wantState: controller.PITRArchiveCheckStateVerified, wantSrc: checkSource},
		{name: "reader: own encrypted archive verifies with the key", role: controller.PITRArchiveCheckRoleReader, key: "k1\n",
			seed: func(t *testing.T, s sidecar.ArchiveStore) {
				putCheckDescriptor(t, s, checkSource, dest, enc, []byte("k1"))
			},
			wantState: controller.PITRArchiveCheckStateVerified, wantSrc: checkSource},
		{name: "writer: another key is KeyMismatch", role: controller.PITRArchiveCheckRoleWriter, key: "k2",
			seed: func(t *testing.T, s sidecar.ArchiveStore) {
				putCheckDescriptor(t, s, checkSource, dest, enc, []byte("k1"))
			},
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveKeyMismatch, wantSrc: checkSource},
		{name: "reader: another key is KeyMismatch", role: controller.PITRArchiveCheckRoleReader, key: "k2",
			seed: func(t *testing.T, s sidecar.ArchiveStore) {
				putCheckDescriptor(t, s, checkSource, dest, enc, []byte("k1"))
			},
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveKeyMismatch, wantSrc: checkSource},
		{name: "plaintext candidate on an encrypted archive", role: controller.PITRArchiveCheckRoleWriter,
			seed: func(t *testing.T, s sidecar.ArchiveStore) {
				putCheckDescriptor(t, s, checkSource, dest, enc, []byte("k1"))
			},
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveEncryptionMismatch, wantSrc: checkSource},
		{name: "writer: foreign archive names its owner", role: controller.PITRArchiveCheckRoleWriter,
			seed:      func(t *testing.T, s sidecar.ArchiveStore) { putCheckDescriptor(t, s, foreign, dest, none, nil) },
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveForeign, wantSrc: foreign},
		{name: "reader: foreign archive verifies", role: controller.PITRArchiveCheckRoleReader,
			seed:      func(t *testing.T, s sidecar.ArchiveStore) { putCheckDescriptor(t, s, foreign, dest, none, nil) },
			wantState: controller.PITRArchiveCheckStateVerified, wantSrc: foreign},
		{name: "writer: a copy is ArchiveCopy", role: controller.PITRArchiveCheckRoleWriter,
			seed:      func(t *testing.T, s sidecar.ArchiveStore) { putCheckDescriptor(t, s, checkSource, copyDest, none, nil) },
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveCopy, wantSrc: checkSource},
		{name: "writer: legacy prefix", role: controller.PITRArchiveCheckRoleWriter,
			seed: func(t *testing.T, s sidecar.ArchiveStore) {
				putObject(t, s, sidecar.ManifestKey(downloadPrefix, "dc1"), v1)
			},
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveLegacy},
		{name: "reader: legacy prefix", role: controller.PITRArchiveCheckRoleReader,
			seed: func(t *testing.T, s sidecar.ArchiveStore) {
				putObject(t, s, sidecar.ManifestKey(downloadPrefix, "dc1"), v1)
			},
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveLegacy},
		{name: "descriptor missing", role: controller.PITRArchiveCheckRoleReader, seed: func(t *testing.T, s sidecar.ArchiveStore) {
			putManifest(t, s, sidecar.Manifest{Version: 2, ArchiveID: "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa", Site: "dc1"})
		}, wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveDescriptorMissing},
		{name: "unsupported descriptor version", role: controller.PITRArchiveCheckRoleWriter, seed: func(t *testing.T, s sidecar.ArchiveStore) {
			putObject(t, s, pitrarchive.DescriptorKey(downloadPrefix), []byte(`{"kind":"BloodravenPITRArchive","version":7}`))
		}, wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveVersionUnsupported},
		{name: "listing denied (403)", role: controller.PITRArchiveCheckRoleWriter, seed: func(*testing.T, sidecar.ArchiveStore) {},
			wrap:      func(s sidecar.ArchiveStore) sidecar.ArchiveStore { return listDeniedStore{s} },
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveListFailed},
		{name: "reader: listing denied (403)", role: controller.PITRArchiveCheckRoleReader, seed: func(*testing.T, sidecar.ArchiveStore) {},
			wrap:      func(s sidecar.ArchiveStore) sidecar.ArchiveStore { return listDeniedStore{s} },
			wantState: controller.PITRArchiveCheckStateRefused, wantToken: pitrarchive.TokenArchiveListFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			tt.seed(t, stampTestStoreAt(t, root))
			line, code, _ := runCheckForTest(t, root, tt.role, tt.key, tt.wrap, nil)
			if code != pitrCheckExitOK {
				t.Fatalf("exit %d", code)
			}
			res, err := controller.ParsePITRArchiveCheckLineForTest(line)
			if err != nil {
				t.Fatalf("line %q: %v", line, err)
			}
			if res.State != tt.wantState || res.Reason != tt.wantToken || res.DestinationDigest != controller.PITRArchiveCheckDestinationDigest(dest.String()) {
				t.Fatalf("verdict = %+v, want %s/%s", res, tt.wantState, tt.wantToken)
			}
			if res.Source.UID != tt.wantSrc.UID || res.Source.Namespace != tt.wantSrc.Namespace || res.Source.Name != tt.wantSrc.Name {
				t.Fatalf("source = %+v, want %+v", res.Source, tt.wantSrc)
			}
			if tt.wantSrc.UID != "" && !pitrarchive.ValidArchiveID(res.ArchiveID) {
				t.Fatalf("archiveID %q", res.ArchiveID)
			}
		})
	}
}

// A transient store error is retried five times; one that persists exits
// non-zero without a verdict line, so the controller never records an
// outage as a refusal. One that clears is a normal verdict.
func TestPITRArchiveCheckTransient(t *testing.T) {
	for _, tc := range []struct {
		failures int
		wantCode int
	}{{failures: 3, wantCode: pitrCheckExitOK}, {failures: 99, wantCode: pitrCheckExitTransient}} {
		root := t.TempDir()
		n := tc.failures
		wrap := func(s sidecar.ArchiveStore) sidecar.ArchiveStore { return flakyListStore{ArchiveStore: s, n: &n} }
		line, code, termLog := runCheckForTest(t, root, controller.PITRArchiveCheckRoleWriter, "", wrap, []time.Duration{0, 0, 0, 0, 0})
		if code != tc.wantCode {
			t.Fatalf("failures=%d: exit %d, want %d", tc.failures, code, tc.wantCode)
		}
		if tc.wantCode != pitrCheckExitOK {
			if line != "" || termLog != "" {
				t.Fatalf("a transient failure wrote a verdict: %q / %q", line, termLog)
			}
			if consumed := tc.failures - n; consumed != 6 {
				t.Fatalf("%d attempts, want 6 (one + five retries)", consumed)
			}
			continue
		}
		if !strings.HasPrefix(termLog, controller.PITRArchiveCheckLinePrefix+" state=Absent") {
			t.Fatalf("termination log %q", termLog)
		}
	}
}

// Configuration errors exit non-zero without a line.
func TestPITRArchiveCheckConfig(t *testing.T) {
	dest := downloadDest(t)
	base := map[string]string{
		"BLOODRAVEN_PITR_DESTINATION":     dest.String(),
		"BLOODRAVEN_PITR_MANIFEST_PREFIX": dest.KeyPrefix,
		"BLOODRAVEN_PITR_ENCRYPTION_MODE": "None",
		"BLOODRAVEN_PITR_CHECK_ROLE":      "Writer",
		"BLOODRAVEN_FAILOVER_GROUP_UID":   checkGroupUID,
	}
	for name, edit := range map[string]func(m map[string]string){
		"bad destination":      func(m map[string]string) { m["BLOODRAVEN_PITR_DESTINATION"] = "s3://nope" },
		"prefix disagrees":     func(m map[string]string) { m["BLOODRAVEN_PITR_MANIFEST_PREFIX"] = "other" },
		"unknown role":         func(m map[string]string) { m["BLOODRAVEN_PITR_CHECK_ROLE"] = "Auditor" },
		"writer without a UID": func(m map[string]string) { delete(m, "BLOODRAVEN_FAILOVER_GROUP_UID") },
		"passphrase without a key": func(m map[string]string) {
			m["BLOODRAVEN_PITR_ENCRYPTION_MODE"] = "Passphrase"
			m["BLOODRAVEN_PITR_ALGORITHM"] = "AES-256-GCM"
		},
		"None with an algorithm": func(m map[string]string) { m["BLOODRAVEN_PITR_ALGORITHM"] = "AES-256-GCM" },
		"plaintext with a key":   func(m map[string]string) { m["BLOODRAVEN_PITR_PASSPHRASE_FILE"] = "/x" },
	} {
		env := map[string]string{}
		for k, v := range base {
			env[k] = v
		}
		edit(env)
		if _, err := pitrArchiveCheckRequestFromEnv(func(k string) string { return env[k] }); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	req, err := pitrArchiveCheckRequestFromEnv(func(k string) string { return base[k] })
	if err != nil || req.expect.Role != pitrarchive.RoleWriter || req.expect.SourceUID != ktypes.UID(checkGroupUID) {
		t.Fatalf("base request = %+v, %v", req, err)
	}
}

// runCheckForTest runs the command core against the PVC archive at root
// and returns its stdout line, exit code and termination log.
func runCheckForTest(t *testing.T, root, role, key string, wrap checkStoreWrap, backoff []time.Duration) (string, int, string) {
	t.Helper()
	dest := downloadDest(t)
	t.Setenv("BLOODRAVEN_PITR_DESTINATION", dest.String())
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", dest.KeyPrefix)
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", root)
	t.Setenv(controller.PITRArchiveCheckRoleEnv, role)
	t.Setenv("BLOODRAVEN_FAILOVER_GROUP_UID", checkGroupUID)
	if key == "" {
		t.Setenv("BLOODRAVEN_PITR_ENCRYPTION_MODE", "None")
		t.Setenv("BLOODRAVEN_PITR_ALGORITHM", "")
		t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "")
	} else {
		file := filepath.Join(t.TempDir(), "passphrase")
		if err := os.WriteFile(file, []byte(key), 0o400); err != nil {
			t.Fatal(err)
		}
		t.Setenv("BLOODRAVEN_PITR_ENCRYPTION_MODE", "Passphrase")
		t.Setenv("BLOODRAVEN_PITR_ALGORITHM", pitrarchive.AlgorithmAES256GCM)
		t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", file)
	}
	termLog := filepath.Join(t.TempDir(), "termination-log")
	prevLog, prevBackoff := pitrArchiveCheckTerminationLogPath, pitrArchiveCheckBackoff
	pitrArchiveCheckTerminationLogPath = termLog
	if backoff != nil {
		pitrArchiveCheckBackoff = backoff
	}
	t.Cleanup(func() { pitrArchiveCheckTerminationLogPath, pitrArchiveCheckBackoff = prevLog, prevBackoff })
	newStore := func(ctx context.Context, cfg *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
		pvc := *cfg.PVC
		pvc.MountVerifier = sidecar.TempDirMountVerifierForTest
		c := *cfg
		c.PVC = &pvc
		s, err := sidecar.NewArchiveStore(ctx, &c)
		if err != nil || wrap == nil {
			return s, err
		}
		return wrap(s), nil
	}
	var out bytes.Buffer
	code := pitrArchiveCheck(context.Background(), os.Getenv, newStore, &out, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b, _ := os.ReadFile(termLog)
	return strings.TrimSpace(out.String()), code, strings.TrimSpace(string(b))
}
