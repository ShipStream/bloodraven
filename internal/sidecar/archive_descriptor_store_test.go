package sidecar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// selfConflictStore lands the descriptor create and then answers it with
// ErrObjectExists, as the S3 SDK does when its retry of a create whose
// first attempt landed (response lost) gets 412 for its own object.
type selfConflictStore struct {
	*memStore
	key string
}

func (s *selfConflictStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	err := s.memStore.PutIfAbsent(ctx, key, r, size)
	if err == nil && key == s.key {
		return fmt.Errorf("%w: precondition failed on retry", ErrObjectExists)
	}
	return err
}

// A descriptor create whose retry reports "exists" for the writer's own
// object verifies the archive and is logged as created (A4 i).
func TestEnsureCreatedSelfConflictOnRetry(t *testing.T) {
	ctx := context.Background()
	store := &selfConflictStore{memStore: newMemStore(), key: pitrarchive.DescriptorKey("binlogs")}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	s, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", logger)
	if s == nil {
		t.Fatalf("no session: %v", err)
	}
	if err := s.EnsureCreated(ctx); err != nil {
		t.Fatalf("EnsureCreated = %v", err)
	}
	if st := s.Status(); st.State != pitrarchive.StateVerified || st.ArchiveID != testArchiveID {
		t.Fatalf("status = %+v", st)
	}
	if !strings.Contains(logs.String(), `msg="archive descriptor created"`) {
		t.Fatalf("own create not logged as created:\n%s", logs.String())
	}

	// A sibling's descriptor (another site's bytes) is accepted but is not
	// this writer's create.
	sibling := newMemStore()
	if err := seedTestDescriptor(ctx, sibling, "binlogs"); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	s2, _ := OpenArchiveForWrite(ctx, sibling, "binlogs", testArchiveIdentity(sibling), "pdx", logger)
	if err := s2.EnsureCreated(ctx); err != nil {
		t.Fatalf("EnsureCreated over a sibling's descriptor = %v", err)
	}
	if strings.Contains(logs.String(), "archive descriptor created") {
		t.Fatalf("a sibling's descriptor was logged as created:\n%s", logs.String())
	}
}

// Crash leftovers of PVC writes (temp files of Put, PutIfAbsent, the CAS
// probe and GetFile) do not make a prefix non-empty: a writer still
// establishes the archive over them (A4 ii).
func TestPVCLeftoversDoNotBlockEstablishment(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	leftovers := []string{
		"binlogs/archive.json.123456.tmp",
		"binlogs/" + pitrarchive.CASProbeKeyPrefix + "0011223344556677.987654.tmp",
		"binlogs/manifest-iad.json.tmp",
		"binlogs/iad/mysql-bin.000001.part",
	}
	for _, rel := range leftovers {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store := &pvcStore{root: root}
	c, err := classifyArchivePrefix(ctx, store, store, "binlogs")
	if err != nil {
		t.Fatal(err)
	}
	if !c.empty() {
		t.Fatalf("leftovers classified as content: %+v", c)
	}
	s, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", nil)
	if s == nil {
		t.Fatalf("no session: %v", err)
	}
	if err := s.EnsureCreated(ctx); err != nil {
		t.Fatalf("EnsureCreated over leftovers = %v", err)
	}
	if st := s.Status(); st.State != pitrarchive.StateVerified {
		t.Fatalf("status = %+v", st)
	}

	// A real stray object still refuses.
	if err := os.WriteFile(filepath.Join(root, "binlogs/stray"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if c, err := classifyArchivePrefix(ctx, store, store, "binlogs"); err != nil || c.empty() {
		t.Fatalf("stray object classified empty: %+v, %v", c, err)
	}
}
