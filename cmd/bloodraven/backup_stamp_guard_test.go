package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// fastStampSettle makes settleBackupStamp's retries immediate.
func fastStampSettle(t *testing.T) {
	t.Helper()
	prev := stampSettleBackoff
	stampSettleBackoff = time.Millisecond
	t.Cleanup(func() { stampSettleBackoff = prev })
}

func putRaw(t *testing.T, store sidecar.ArchiveStore, key string, body []byte) {
	t.Helper()
	if err := store.Put(context.Background(), key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
}

func canonical(t *testing.T, s backupmeta.Stamp) []byte {
	t.Helper()
	b, err := s.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// stampTestStoreAt is stampTestStore on root.
func stampTestStoreAt(t *testing.T, root string) sidecar.ArchiveStore {
	t.Helper()
	store, err := sidecar.NewArchiveStore(context.Background(), &sidecar.PITRConfig{StorageType: "PVC",
		PVC: &sidecar.PITRPVCConfig{MountPath: root, Writable: true, MountVerifier: sidecar.TempDirMountVerifierForTest}})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// Review F001: a stamp is created only on an empty dump location. Objects
// there without a stamp are another backup's (or an unstamped run's); a new
// stamp over them would name this backup's UID, and its abandoned cleanup
// would then delete them.
func TestWriteBackupStampRefusesUnstampedLocation(t *testing.T) {
	ctx := context.Background()
	plain := func(s sidecar.ArchiveStore) sidecar.ArchiveStore { return s }
	pvcWalk := func(_ sidecar.ArchiveStore, root string) objectLister { return pvcLocationLister{root: root} }
	tests := []struct {
		name    string
		wrap    func(sidecar.ArchiveStore) sidecar.ArchiveStore
		lister  func(sidecar.ArchiveStore, string) objectLister
		object  string
		refused bool
	}{
		{name: "unstamped dump object", wrap: plain, object: "lion/nightly-1/@.json", refused: true},
		{name: "unstamped object on a store without atomic create", wrap: func(s sidecar.ArchiveStore) sidecar.ArchiveStore { return noAtomicCreateStore{s} },
			object: "lion/nightly-1/@.json", refused: true},
		{name: "unstamped object, encrypted layout", wrap: func(s sidecar.ArchiveStore) sidecar.ArchiveStore {
			return sidecar.WrapWithEncryptionOptions(s, []byte("dump key"), false)
		}, object: "lion/nightly-1/chunk.zst", refused: true},
		{name: "a temporary file the PVC store hides from List", wrap: plain, lister: pvcWalk, object: "lion/nightly-1/sub/@.json.tmp", refused: true},
		{name: "a sibling location's object does not count", wrap: plain, object: "lion/nightly-10/@.json"},
		{name: "a sibling location's object, PVC walk", wrap: plain, lister: pvcWalk, object: "lion/nightly-10/@.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			mustWrite(t, filepath.Join(root, filepath.FromSlash(tt.object)))
			store := tt.wrap(stampTestStoreAt(t, root))
			var lister objectLister = store
			if tt.lister != nil {
				lister = tt.lister(store, root)
			}
			_, _, err := writeBackupStamp(ctx, store, lister, stampKey, startedStamp(stampStart))
			if !tt.refused {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, ErrBackupStampConflict) || !strings.Contains(err.Error(), tt.object) {
				t.Fatalf("err = %v, want a conflict naming %s", err, tt.object)
			}
			if _, statErr := os.Stat(filepath.Join(root, filepath.FromSlash(stampKey))); !os.IsNotExist(statErr) {
				t.Fatalf("a refused start step created the stamp: %v", statErr)
			}
		})
	}
}

// pagedFake is a paged lister that records how much it was asked to list.
type pagedFake struct {
	pages     [][]string
	err       error
	served    int
	listCalls int
}

func (f *pagedFake) ListPages(_ context.Context, _ string, fn func([]string) error) error {
	for _, p := range f.pages {
		f.served++
		if err := fn(p); err != nil {
			return err
		}
	}
	return f.err
}

func (f *pagedFake) List(context.Context, string) ([]string, error) {
	f.listCalls++
	var out []string
	for _, p := range f.pages {
		out = append(out, p...)
	}
	return out, f.err
}

// Review F119: the emptiness check stops at the first page that holds an
// object instead of listing the whole location; on a PVC it stops at the
// first file.
func TestFirstObjectUnderStopsEarly(t *testing.T) {
	ctx := context.Background()
	f := &pagedFake{pages: [][]string{{}, {"loc/a", "loc/b"}, {"loc/c"}}}
	first, found, err := firstObjectUnder(ctx, f, "loc/")
	if err != nil || !found || first != "loc/a" || f.served != 2 || f.listCalls != 0 {
		t.Fatalf("first=%q found=%t err=%v served=%d list=%d", first, found, err, f.served, f.listCalls)
	}
	if _, found, err := firstObjectUnder(ctx, &pagedFake{pages: [][]string{{}}}, "loc/"); found || err != nil {
		t.Fatalf("empty: found=%t err=%v", found, err)
	}
	boom := errors.New("boom")
	if _, _, err := firstObjectUnder(ctx, &pagedFake{err: boom}, "loc/"); !errors.Is(err, boom) {
		t.Fatalf("list error: %v", err)
	}

	root := t.TempDir()
	for i := 0; i < 50; i++ {
		mustWrite(t, filepath.Join(root, "loc", "sub", fmt.Sprintf("f%02d", i)))
	}
	calls := 0
	err = pvcLocationLister{root: root}.ListPages(ctx, "loc/", func(keys []string) error {
		calls++
		if len(keys) != 1 || !strings.HasPrefix(keys[0], "loc/sub/") {
			t.Fatalf("keys %v", keys)
		}
		return errStopListing
	})
	if !errors.Is(err, errStopListing) || calls != 1 {
		t.Fatalf("PVC walk: err=%v calls=%d", err, calls)
	}
	if first, found, err := firstObjectUnder(ctx, pvcLocationLister{root: root}, "missing/"); found || err != nil {
		t.Fatalf("missing location: %q %t %v", first, found, err)
	}
	if _, _, err := firstObjectUnder(ctx, pvcLocationLister{root: root}, "../escape/"); err == nil {
		t.Fatal("a location outside the mount was walked")
	}
}

// raceCreateStore's create finds the key taken by winner, as when a create
// whose first response was lost is retried, or another writer created it
// between the start step's read and its create.
type raceCreateStore struct {
	sidecar.ArchiveStore
	winner []byte
}

func (s raceCreateStore) PutIfAbsent(ctx context.Context, key string, _ io.Reader, _ int64) error {
	if err := s.ArchiveStore.Put(ctx, key, bytes.NewReader(s.winner), int64(len(s.winner))); err != nil {
		return err
	}
	return fmt.Errorf("create %s: %w", key, sidecar.ErrObjectExists)
}

// conflictCreateStore's create keeps reporting a concurrent conditional
// write (S3 409 ConditionalRequestConflict after every retry).
type conflictCreateStore struct {
	sidecar.ArchiveStore
	puts *int
}

func (s conflictCreateStore) PutIfAbsent(_ context.Context, key string, _ io.Reader, _ int64) error {
	return fmt.Errorf("create %s: %w", key, sidecar.ErrConditionalConflict)
}

func (s conflictCreateStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	*s.puts++
	return s.ArchiveStore.Put(ctx, key, r, size)
}

// Review F120 and F016: what the start step does when its create reports
// ErrObjectExists after the read found nothing, and when the store reports
// a concurrent conditional write.
func TestWriteBackupStampCreateRaces(t *testing.T) {
	ctx := context.Background()
	desired := startedStamp(stampStart)
	earlier := startedStamp(stampStart.Add(-time.Minute))
	foreign := startedStamp(stampStart)
	foreign.Backup.Namespace, foreign.Backup.UID = "other-ns", "other-uid"
	abandoned := stampOf("old", stampStart)
	for _, tt := range []struct {
		name       string
		winner     backupmeta.Stamp
		wantErr    error
		wantReused bool
		wantStamp  backupmeta.Stamp
	}{
		{name: "own create landed", winner: desired, wantStamp: desired},
		{name: "a retried pod's earlier stamp", winner: earlier, wantReused: true, wantStamp: earlier},
		{name: "another namespace's backup", winner: foreign, wantErr: ErrBackupStampConflict},
		{name: "an earlier incarnation created it concurrently", winner: abandoned, wantErr: ErrBackupStampConflict},
	} {
		t.Run("ObjectExists/"+tt.name, func(t *testing.T) {
			inner := stampTestStore(t)
			store := raceCreateStore{ArchiveStore: inner, winner: canonical(t, tt.winner)}
			got, reused, err := writeBackupStamp(ctx, store, inner, stampKey, desired)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil || reused != tt.wantReused || !sameStampBytes(got, tt.wantStamp) {
				t.Fatalf("got %+v reused=%t err=%v", got, reused, err)
			}
			// The winner's stamp is never overwritten.
			if !bytes.Equal(stampBytes(t, inner, stampKey), canonical(t, tt.winner)) {
				t.Fatal("the start step replaced the stamp that won the create")
			}
		})
	}
	t.Run("ConditionalConflict refuses without Put", func(t *testing.T) {
		inner := stampTestStore(t)
		puts := 0
		_, _, err := writeBackupStamp(ctx, conflictCreateStore{ArchiveStore: inner, puts: &puts}, inner, stampKey, desired)
		if !errors.Is(err, ErrBackupStampConflict) || !errors.Is(err, sidecar.ErrConditionalConflict) || puts != 0 {
			t.Fatalf("err = %v, puts %d", err, puts)
		}
		if _, ok, _ := inner.Get(ctx, stampKey); ok {
			t.Fatal("a conflicting create wrote the stamp")
		}
	})
}

// flakyStampStore injects the failures of review F017: read-backs that
// fail transiently, Puts whose response is lost after the write landed,
// Puts that fail before writing, and a foreign writer.
type flakyStampStore struct {
	sidecar.ArchiveStore
	mu              sync.Mutex
	getFailures     int    // fail this many Gets
	lostPutResponse int    // this many Puts write, then report an error
	failedPuts      int    // this many Puts fail without writing
	putInstead      []byte // Puts write these bytes instead (another writer)
	puts            int
}

var errFlaky = errors.New("connection reset by peer")

func (s *flakyStampStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	fail := s.getFailures > 0
	if fail {
		s.getFailures--
	}
	s.mu.Unlock()
	if fail {
		return nil, false, errFlaky
	}
	return s.ArchiveStore.Get(ctx, key)
}

func (s *flakyStampStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.puts++
	failBefore := s.failedPuts > 0
	if failBefore {
		s.failedPuts--
	}
	lost := !failBefore && s.lostPutResponse > 0
	if lost {
		s.lostPutResponse--
	}
	instead := s.putInstead
	s.mu.Unlock()
	if failBefore {
		return errFlaky
	}
	if instead != nil {
		r, size = bytes.NewReader(instead), int64(len(instead))
	}
	if err := s.ArchiveStore.Put(ctx, key, r, size); err != nil {
		return err
	}
	if lost {
		return errFlaky
	}
	return nil
}

// Review F017: the end step settles the stamp in-process. A transient
// read-back failure or a lost Put response no longer fails the pod (whose
// retries would all refuse the finished stamp); a Put that did not land is
// written again; another writer's stamp refuses without a second write.
func TestFinishBackupStampSettles(t *testing.T) {
	fastStampSettle(t)
	ctx := context.Background()
	end := backupmeta.NewMicroTime(stampStart.Add(time.Hour))
	foreign := startedStamp(stampStart)
	foreign.Backup.Namespace, foreign.Backup.UID = "other-ns", "other-uid"
	tests := []struct {
		name     string
		arm      func(*flakyStampStore)
		wantErr  error
		wantAny  bool // any error
		wantPuts int
		wantEnd  bool
	}{
		{name: "transient read-back failures", arm: func(s *flakyStampStore) { s.getFailures = 1 }, wantPuts: 1, wantEnd: true},
		{name: "lost Put response", arm: func(s *flakyStampStore) { s.lostPutResponse = 1 }, wantPuts: 1, wantEnd: true},
		{name: "Put failed before writing", arm: func(s *flakyStampStore) { s.failedPuts = 2 }, wantPuts: 3, wantEnd: true},
		{name: "read-back never works", arm: func(s *flakyStampStore) { s.getFailures = 1 << 20 }, wantAny: true},
		{name: "another writer's stamp", arm: func(s *flakyStampStore) { s.putInstead = canonical(t, foreign) }, wantErr: ErrBackupStampConflict, wantPuts: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := stampTestStore(t)
			if _, _, err := writeBackupStamp(ctx, inner, inner, stampKey, startedStamp(stampStart)); err != nil {
				t.Fatal(err)
			}
			store := &flakyStampStore{ArchiveStore: inner}
			// finishBackupStamp reads the stamp once before writing; the
			// injected Get failures start after that read.
			prev, err := loadOwnStamp(ctx, inner, stampKey, stampTemplate())
			if err != nil {
				t.Fatal(err)
			}
			next := prev
			next.ServerClock.End = end
			tt.arm(store)
			data := canonical(t, next)
			err = settleBackupStamp(ctx, store, stampKey, &prev, next, func() error {
				return store.Put(ctx, stampKey, bytes.NewReader(data), int64(len(data)))
			})
			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			case tt.wantAny:
				if err == nil || errors.Is(err, ErrBackupStampConflict) {
					t.Fatalf("err = %v, want a transient failure", err)
				}
			case err != nil:
				t.Fatal(err)
			}
			if tt.wantPuts != 0 && store.puts != tt.wantPuts {
				t.Fatalf("puts = %d, want %d", store.puts, tt.wantPuts)
			}
			got, perr := backupmeta.ParseStamp(stampBytes(t, inner, stampKey))
			if perr != nil {
				t.Fatal(perr)
			}
			if tt.wantEnd && !sameStampBytes(got, next) {
				t.Fatalf("stored %+v", got)
			}
		})
	}

	// The same through finishBackupStamp and, on a store without atomic
	// create, through the start step's Put fallback.
	t.Run("finishBackupStamp", func(t *testing.T) {
		inner := stampTestStore(t)
		if _, _, err := writeBackupStamp(ctx, inner, inner, stampKey, startedStamp(stampStart)); err != nil {
			t.Fatal(err)
		}
		store := &flakyStampStore{ArchiveStore: inner, lostPutResponse: 1}
		if _, err := finishBackupStamp(ctx, store, stampKey, stampTemplate(), end); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("start step Put fallback", func(t *testing.T) {
		inner := stampTestStore(t)
		store := &flakyStampStore{ArchiveStore: inner, lostPutResponse: 1}
		got, _, err := writeBackupStamp(ctx, store, inner, stampKey, startedStamp(stampStart))
		if err != nil || !sameStampBytes(got, startedStamp(stampStart)) || store.puts != 1 {
			t.Fatalf("got %+v err %v puts %d", got, err, store.puts)
		}
	})
}

// Review F016: the encrypted upload re-checks the stamp before it writes
// anything under the location.
func TestCheckBackupStampOwner(t *testing.T) {
	ctx := context.Background()
	foreign := startedStamp(stampStart)
	foreign.Backup.Namespace, foreign.Backup.UID = "other-ns", "other-uid"
	finished := startedStamp(stampStart)
	finished.ServerClock.End = backupmeta.NewMicroTime(stampStart.Add(time.Hour))
	for _, tt := range []struct {
		name   string
		stored *backupmeta.Stamp
		ok     bool
	}{
		{name: "own unfinished stamp", stored: ptrStamp(startedStamp(stampStart)), ok: true},
		{name: "another namespace's stamp", stored: &foreign},
		{name: "already finished", stored: &finished},
		{name: "missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := stampTestStore(t)
			if tt.stored != nil {
				putRaw(t, store, stampKey, canonical(t, *tt.stored))
			}
			err := checkBackupStampOwner(ctx, store, stampKey, stampTemplate())
			if tt.ok != (err == nil) || !tt.ok && !errors.Is(err, ErrBackupStampConflict) {
				t.Fatalf("err = %v, want ok=%t", err, tt.ok)
			}
		})
	}
}

// Review F139: a backup whose name ends with the stamp suffix is refused
// before it writes anything.
func TestBackupStampStoreConfigRefusesStampNamedLocation(t *testing.T) {
	t.Setenv("BLOODRAVEN_S3_BUCKET", "b")
	if _, _, err := backupStampStoreConfig("S3", "lion/nightly-1"+backupmeta.StampSuffix); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err = %v", err)
	}
}

// Review F015: the server_id guard refuses a Job that reached another
// server.
func TestCheckDumpServerID(t *testing.T) {
	tmpl := stampTemplate()
	if err := checkDumpServerID(tmpl, tmpl.ServerID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint32{0, tmpl.ServerID - 1, tmpl.ServerID + 1} {
		if err := checkDumpServerID(tmpl, id); !errors.Is(err, ErrBackupStampConflict) || !strings.Contains(err.Error(), fmt.Sprint(id)) {
			t.Fatalf("server_id %d: %v", id, err)
		}
	}
}

// testPKI is a CA and a server certificate it signed.
type testPKI struct {
	caPEM      []byte
	serverCert tls.Certificate
}

func newTestPKI(t *testing.T, serverUsage x509.ExtKeyUsage) testPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// The name does not match any host the client dials: VERIFY_CA checks
	// no host name.
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "mysql"},
		DNSNames: []string{"elsewhere.invalid"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{serverUsage}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return testPKI{
		caPEM:      pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		serverCert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key},
	}
}

// handshake runs a TLS handshake over loopback TCP between a server
// presenting cert and a client using cfg, and returns the client's error.
// (net.Pipe has no buffering, so a client that aborts mid-handshake can
// deadlock against the server's writes.)
func handshake(t *testing.T, cfg *tls.Config, cert tls.Certificate) error {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		_ = conn.(*tls.Conn).Handshake()
		_ = conn.Close()
	}()
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", ln.Addr().String(), cfg)
	if err == nil {
		_ = conn.Close()
	}
	<-done
	return err
}

// Review F015: the start step's MySQL session verifies the server's chain
// against the mounted CA (mysqlsh VERIFY_CA): a trusted chain passes
// whatever its host name, an untrusted chain or one not valid for server
// auth fails.
func TestVerifyCATLSConfig(t *testing.T) {
	trusted := newTestPKI(t, x509.ExtKeyUsageServerAuth)
	stranger := newTestPKI(t, x509.ExtKeyUsageServerAuth)
	clientOnly := newTestPKI(t, x509.ExtKeyUsageClientAuth)
	writeCA := func(b []byte) string {
		p := filepath.Join(t.TempDir(), "ca.crt")
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	cfg, err := verifyCATLSConfig(writeCA(trusted.caPEM))
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(t, cfg, trusted.serverCert); err != nil {
		t.Fatalf("trusted chain refused: %v", err)
	}
	if err := handshake(t, cfg, stranger.serverCert); err == nil || !strings.Contains(err.Error(), "verify MySQL server certificate") {
		t.Fatalf("untrusted chain: %v", err)
	}
	clientCfg, err := verifyCATLSConfig(writeCA(clientOnly.caPEM))
	if err != nil {
		t.Fatal(err)
	}
	if err := handshake(t, clientCfg, clientOnly.serverCert); err == nil {
		t.Fatal("a certificate without serverAuth was accepted")
	}
	if _, err := verifyCATLSConfig(writeCA([]byte("not a certificate"))); err == nil {
		t.Fatal("a CA file without certificates was accepted")
	}
	if _, err := verifyCATLSConfig(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("a missing CA file was accepted")
	}
	if err := cfg.VerifyConnection(tls.ConnectionState{}); err == nil {
		t.Fatal("a peer without certificates was accepted")
	}
}

// Review F015: the start step's MySQL configuration mirrors dump.py's.
func TestBackupStampMySQLConfig(t *testing.T) {
	ca := newTestPKI(t, x509.ExtKeyUsageServerAuth)
	creds := t.TempDir()
	mustWriteBytes := func(p string, b []byte) {
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	mustWriteBytes(filepath.Join(creds, "MYSQL_USER"), []byte("backup"))
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	mustWriteBytes(caFile, ca.caPEM)

	for _, tt := range []struct {
		name    string
		env     map[string]string
		wantErr string
		wantTLS bool
	}{
		{name: "plain", env: map[string]string{}},
		{name: "TLS with a CA", env: map[string]string{"BLOODRAVEN_TLS": "true", "BLOODRAVEN_TLS_CA_FILE": caFile}, wantTLS: true},
		{name: "TLS without a CA", env: map[string]string{"BLOODRAVEN_TLS": "1"}, wantErr: "BLOODRAVEN_TLS_CA_FILE"},
		{name: "TLS off", env: map[string]string{"BLOODRAVEN_TLS": "0", "BLOODRAVEN_TLS_CA_FILE": caFile}},
		{name: "no host", env: map[string]string{"BLOODRAVEN_MYSQL_HOST": ""}, wantErr: "BLOODRAVEN_MYSQL_HOST"},
		{name: "no credentials dir", env: map[string]string{"BLOODRAVEN_MYSQL_CREDS_DIR": ""}, wantErr: "BLOODRAVEN_MYSQL_CREDS_DIR"},
		{name: "no user file", env: map[string]string{"BLOODRAVEN_MYSQL_CREDS_DIR": t.TempDir()}, wantErr: "MYSQL_USER"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BLOODRAVEN_MYSQL_HOST", "mysql-lion-pdx:3306")
			t.Setenv("BLOODRAVEN_MYSQL_CREDS_DIR", creds)
			t.Setenv("BLOODRAVEN_TLS", "")
			t.Setenv("BLOODRAVEN_TLS_CA_FILE", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := backupStampMySQLConfig()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.User != "backup" || cfg.Passwd != "" || cfg.Addr != "mysql-lion-pdx:3306" || cfg.Net != "tcp" {
				t.Fatalf("cfg %+v", cfg)
			}
			if tt.wantTLS != (cfg.TLS != nil) || tt.wantTLS && cfg.TLS.VerifyConnection == nil {
				t.Fatalf("TLS config %+v, want TLS=%t", cfg.TLS, tt.wantTLS)
			}
		})
	}
}

// Review F015: the start step installs its own executable for the mysqlsh
// container's end step.
func TestInstallBackupStampTool(t *testing.T) {
	dir := t.TempDir()
	if err := installBackupStampTool(dir); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.Stat(self)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.Stat(filepath.Join(dir, backupStampToolName))
	if err != nil {
		t.Fatal(err)
	}
	if got.Size() != want.Size() || got.Mode().Perm() != 0o755 {
		t.Fatalf("installed %d bytes mode %v, want %d bytes 0755", got.Size(), got.Mode().Perm(), want.Size())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("temporary files left behind: %v", entries)
	}
	if err := installBackupStampTool(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("install into a missing directory succeeded")
	}
}
