package sidecar

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// probeSeed prepares one archive prefix for the parity table.
type probeSeed struct {
	name      string
	encrypted bool // the store encrypts with probeKey
	seed      func(t *testing.T, inner *memStore, store ArchiveStore)
	listErr   error
}

const probeKey = "archive key"

func probeSeeds() []probeSeed {
	other := pitrarchive.Source{Namespace: "ns", Name: "other", UID: "00000000-0000-4000-8000-0000000000ff"}
	copyDest, err := pitrarchive.NewPVCDestination("ns", "copy", "")
	if err != nil {
		panic(err)
	}
	none := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}
	put := func(t *testing.T, s ArchiveStore, key, body string) {
		t.Helper()
		if err := s.Put(context.Background(), key, strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatal(err)
		}
	}
	return []probeSeed{
		{name: "empty prefix", seed: func(*testing.T, *memStore, ArchiveStore) {}},
		{name: "own plaintext archive", seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			putDescriptor(t, s, testArchiveSource, testArchiveDestination, none, nil)
		}},
		{name: "own encrypted archive", encrypted: true, seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			putDescriptor(t, s, testArchiveSource, testArchiveDestination, passphraseIdentity(), []byte(probeKey))
		}},
		{name: "encrypted archive, another key", encrypted: true, seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			putDescriptor(t, s, testArchiveSource, testArchiveDestination, passphraseIdentity(), []byte("another key"))
		}},
		{name: "plaintext archive, encrypted consumer", encrypted: true, seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			putDescriptor(t, s, testArchiveSource, testArchiveDestination, none, nil)
		}},
		{name: "foreign archive", seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			putDescriptor(t, s, other, testArchiveDestination, none, nil)
		}},
		{name: "copied archive", seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			putDescriptor(t, s, testArchiveSource, copyDest, none, nil)
		}},
		{name: "unsupported descriptor version", seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			put(t, s, pitrarchive.DescriptorKey("binlogs"), `{"kind":"BloodravenPITRArchive","version":99}`)
		}},
		{name: "v2 manifests without a descriptor", seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			put(t, s, "binlogs/manifest-iad.json", `{"version":2,"archiveID":"`+testArchiveID+`","site":"iad","files":[]}`)
		}},
		{name: "v1 manifests", seed: func(t *testing.T, _ *memStore, s ArchiveStore) {
			put(t, s, "binlogs/manifest-iad.json", `{"version":1,"site":"iad","files":[]}`)
		}},
		{name: "list denied", listErr: forbiddenError{}},
		{name: "list EACCES", listErr: fs.ErrPermission},
	}
}

func (p probeSeed) store(t *testing.T) ArchiveStore {
	t.Helper()
	inner := newMemStore()
	var store ArchiveStore = inner
	if p.encrypted {
		store = WrapWithEncryption(inner, []byte(probeKey+"\n"))
	}
	p.seed(t, inner, store)
	if p.listErr != nil {
		return &faultStore{ArchiveStore: store, listErr: p.listErr}
	}
	return store
}

func (p probeSeed) seedOrNoop() probeSeed {
	if p.seed == nil {
		p.seed = func(*testing.T, *memStore, ArchiveStore) {}
	}
	return p
}

func tokenOrEmpty(err error) pitrarchive.Token {
	tok, _ := pitrarchive.TokenOf(err)
	return tok
}

// ProbeArchive with the reader role classifies every prefix the way
// OpenArchiveForRead does (pitr-download and the standby scan): the check
// Job's read verdict is the restore's.
func TestProbeArchiveReaderParity(t *testing.T) {
	for _, p := range probeSeeds() {
		p := p.seedOrNoop()
		t.Run(p.name, func(t *testing.T) {
			ctx := context.Background()
			enc := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}
			if p.encrypted {
				enc = passphraseIdentity()
			}
			expect := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: testArchiveDestination, Encryption: enc}
			store := p.store(t)
			probe, perr := ProbeArchive(ctx, store, "binlogs", expect)
			if perr != nil {
				t.Fatalf("probe error %v", perr)
			}
			_, rerr := OpenArchiveForRead(ctx, store, "binlogs", expect)
			want := tokenOrEmpty(rerr)
			switch {
			case rerr == nil:
				if probe.State != ArchiveProbeVerified || probe.Descriptor == nil {
					t.Fatalf("reader verified, probe = %+v", probe)
				}
			case want == "":
				t.Fatalf("OpenArchiveForRead returned a non-refusal %v", rerr)
			default:
				if probe.State != ArchiveProbeRefused || probe.Token != want {
					t.Fatalf("probe = %s/%s (%v), reader refused %s", probe.State, probe.Token, probe.Err, want)
				}
				if !strings.Contains(probe.Err.Error(), testArchiveDestination.String()) {
					t.Fatalf("refusal %q does not name the destination", probe.Err)
				}
			}
		})
	}
}

// ProbeArchive with the writer role agrees with a writer session's Open:
// an empty prefix is Absent (the writer would create the descriptor), and
// every refusal carries the same token.
func TestProbeArchiveWriterParity(t *testing.T) {
	for _, p := range probeSeeds() {
		p := p.seedOrNoop()
		if p.listErr != nil {
			continue // a writer session reports a list failure as transient; the probe's ListFailed is covered below
		}
		t.Run(p.name, func(t *testing.T) {
			ctx := context.Background()
			store := p.store(t)
			identity := testArchiveIdentity(store)
			expect := pitrarchive.Expectation{Role: pitrarchive.RoleWriter, SourceUID: identity.Source.UID,
				Destination: identity.Destination, Encryption: identity.Encryption}
			probe, perr := ProbeArchive(ctx, store, "binlogs", expect)
			if perr != nil {
				t.Fatalf("probe error %v", perr)
			}
			s, err := OpenArchiveForWrite(ctx, store, "binlogs", identity, "iad", nil)
			if s == nil {
				t.Fatalf("writer session not built: %v", err)
			}
			switch {
			case errors.Is(err, ErrDescriptorAbsent):
				if probe.State != ArchiveProbeAbsent {
					t.Fatalf("writer sees an empty prefix, probe = %+v", probe)
				}
				// The writer then establishes the archive; a second probe
				// verifies what it created.
				if err := s.EnsureCreated(ctx); err != nil {
					t.Fatal(err)
				}
				again, err := ProbeArchive(ctx, store, "binlogs", expect)
				if err != nil || again.State != ArchiveProbeVerified || again.Descriptor.ArchiveID != testArchiveID {
					t.Fatalf("probe after create = %+v, %v", again, err)
				}
			case err == nil:
				if probe.State != ArchiveProbeVerified || probe.Descriptor.ArchiveID != s.ArchiveID() {
					t.Fatalf("writer verified %s, probe = %+v", s.ArchiveID(), probe)
				}
			default:
				want := tokenOrEmpty(err)
				if want == "" || probe.State != ArchiveProbeRefused || probe.Token != want {
					t.Fatalf("probe = %s/%s, writer = %v", probe.State, probe.Token, err)
				}
			}
		})
	}
}

// A foreign refusal carries the parsed descriptor so the check line can
// name the archive's owner; a listing refused for permission is
// PITRArchiveListFailed for both roles; any other store failure is not a
// verdict.
func TestProbeArchiveDetails(t *testing.T) {
	ctx := context.Background()
	other := pitrarchive.Source{Namespace: "ns", Name: "other", UID: "00000000-0000-4000-8000-0000000000ff"}
	none := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}
	writer := pitrarchive.Expectation{Role: pitrarchive.RoleWriter, SourceUID: testArchiveSource.UID, Destination: testArchiveDestination, Encryption: none}
	reader := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: testArchiveDestination, Encryption: none}

	store := newMemStore()
	putDescriptor(t, store, other, testArchiveDestination, none, nil)
	p, err := ProbeArchive(ctx, store, "binlogs", writer)
	if err != nil || p.Token != pitrarchive.TokenArchiveForeign || p.Descriptor == nil || p.Descriptor.Source != other {
		t.Fatalf("foreign probe = %+v, %v", p, err)
	}
	// A reader with no expected UID verifies another group's archive.
	if p, err := ProbeArchive(ctx, store, "binlogs", reader); err != nil || p.State != ArchiveProbeVerified || p.Descriptor.Source != other {
		t.Fatalf("reader probe of a foreign archive = %+v, %v", p, err)
	}

	for _, e := range []pitrarchive.Expectation{writer, reader} {
		denied := &faultStore{ArchiveStore: newMemStore(), listErr: forbiddenError{}}
		p, err := ProbeArchive(ctx, denied, "binlogs", e)
		if err != nil || p.Token != pitrarchive.TokenArchiveListFailed || !strings.Contains(p.Err.Error(), "s3:ListBucket") {
			t.Fatalf("%s: denied list = %+v, %v", e.Role, p, err)
		}
		flaky := &faultStore{ArchiveStore: newMemStore(), listErr: errors.New("connection reset")}
		if p, err := ProbeArchive(ctx, flaky, "binlogs", e); err == nil || p.State != "" {
			t.Fatalf("%s: a transient list failure became a verdict: %+v", e.Role, p)
		}
	}

	// A writer probe without its identity, an unknown role or an invalid
	// encryption identity is a caller bug.
	for _, e := range []pitrarchive.Expectation{
		{Role: pitrarchive.RoleWriter, Destination: testArchiveDestination, Encryption: none},
		{Role: "Auditor", Destination: testArchiveDestination, Encryption: none},
		{Role: pitrarchive.RoleReader, Destination: testArchiveDestination, Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase}},
	} {
		if _, err := ProbeArchive(ctx, newMemStore(), "binlogs", e); !errors.Is(err, pitrarchive.ErrInvalidExpectation) {
			t.Fatalf("%+v: err = %v", e, err)
		}
	}
}

// pagingStore lists like S3: sorted pages of pageSize keys, counting the
// pages fetched.
type pagingStore struct {
	*memStore
	pageSize int
	pages    int
}

func (p *pagingStore) ListPages(ctx context.Context, prefix string, fn func(keys []string) error) error {
	keys, err := p.memStore.List(ctx, prefix)
	if err != nil {
		return err
	}
	sort.Strings(keys)
	for len(keys) > 0 {
		n := min(p.pageSize, len(keys))
		p.pages++
		if err := fn(keys[:n]); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
}

func (p *pagingStore) List(context.Context, string) ([]string, error) {
	panic("the probe materialized a whole listing")
}

// U52: a prefix with no descriptor and a large history settles from the
// manifests' own listing and, for a writer, the first object that counts:
// the probe never pages through the whole prefix.
func TestProbeArchiveDoesNotListWholePrefix(t *testing.T) {
	ctx := context.Background()
	inner := newMemStore()
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("binlogs/iad/mysql-bin.%06d", i)
		if err := inner.Put(ctx, key, strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
	}
	store := &pagingStore{memStore: inner, pageSize: 100}
	identity := testArchiveIdentity(store)
	writer := pitrarchive.Expectation{Role: pitrarchive.RoleWriter, SourceUID: identity.Source.UID, Destination: identity.Destination, Encryption: identity.Encryption}
	probe, err := ProbeArchive(ctx, store, "binlogs", writer)
	if err != nil || probe.State != ArchiveProbeRefused || probe.Token != pitrarchive.TokenArchiveLegacy {
		t.Fatalf("writer probe = %+v, %v", probe, err)
	}
	if store.pages > 2 {
		t.Fatalf("writer probe fetched %d pages", store.pages)
	}
	store.pages = 0
	reader := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: identity.Destination, Encryption: identity.Encryption}
	probe, err = ProbeArchive(ctx, store, "binlogs", reader)
	if err != nil || probe.State != ArchiveProbeRefused || probe.Token != pitrarchive.TokenArchiveNotFound {
		t.Fatalf("reader probe = %+v, %v", probe, err)
	}
	if store.pages > 1 {
		t.Fatalf("reader probe fetched %d pages", store.pages)
	}
	// A v2 manifest among the history is found through its own listing.
	if err := inner.Put(ctx, "binlogs/manifest-iad.json", strings.NewReader(`{"version":2,"archiveID":"`+testArchiveID+`","site":"iad","files":[]}`), 1); err != nil {
		t.Fatal(err)
	}
	store.pages = 0
	if probe, err = ProbeArchive(ctx, store, "binlogs", writer); err != nil || probe.Token != pitrarchive.TokenArchiveDescriptorMissing || store.pages > 1 {
		t.Fatalf("probe with a v2 manifest = %+v, %v (%d pages)", probe, err, store.pages)
	}
}
