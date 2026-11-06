package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// s3Reply is one scripted S3 response.
type s3Reply struct {
	status  int
	code    string
	message string
}

// fakeConditionalS3 answers PUTs from a script (then fallback), records
// If-None-Match, and implements DELETE.
type fakeConditionalS3 struct {
	mu       sync.Mutex
	script   []s3Reply
	fallback func(key string, objects map[string][]byte) s3Reply
	objects  map[string][]byte
	puts     int
	deletes  []string
	headers  []string
}

func (f *fakeConditionalS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	switch r.Method {
	case http.MethodPut:
		f.puts++
		f.headers = append(f.headers, r.Header.Get("If-None-Match"))
		body, _ := io.ReadAll(r.Body)
		var reply s3Reply
		if len(f.script) > 0 {
			reply, f.script = f.script[0], f.script[1:]
		} else if f.fallback != nil {
			reply = f.fallback(key, f.objects)
		} else {
			reply = s3Reply{status: 200}
		}
		if reply.status == 200 {
			f.objects[key] = body
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(200)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(reply.status)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, reply.code, reply.message)
	case http.MethodDelete:
		f.deletes = append(f.deletes, key)
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// conformingS3 refuses a create of an existing key with 412.
func conformingS3(key string, objects map[string][]byte) s3Reply {
	if _, ok := objects[key]; ok {
		return s3Reply{status: 412, code: "PreconditionFailed", message: "At least one of the pre-conditions you specified did not hold"}
	}
	return s3Reply{status: 200}
}

func newFakeS3Store(t *testing.T, f *fakeConditionalS3) *s3Store {
	t.Helper()
	f.objects = map[string][]byte{}
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	prev := s3ConditionalBackoff
	s3ConditionalBackoff = time.Millisecond
	t.Cleanup(func() { s3ConditionalBackoff = prev })
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: server.Client(), RetryMaxAttempts: 1})
	return &s3Store{bucket: "bucket", client: client}
}

func TestS3PutIfAbsentClassifiesResponses(t *testing.T) {
	conflict := s3Reply{status: 409, code: "ConditionalRequestConflict", message: "A conflicting conditional operation is in progress"}
	tests := []struct {
		name      string
		script    []s3Reply
		want      error // nil, or a sentinel matched with errors.Is
		wantOther bool  // an ordinary error, none of the sentinels
		wantPuts  int
	}{
		{name: "created", script: []s3Reply{{status: 200}}, wantPuts: 1},
		{name: "412 exists", script: []s3Reply{{status: 412, code: "PreconditionFailed"}}, want: ErrObjectExists, wantPuts: 1},
		{name: "409 then created", script: []s3Reply{conflict, {status: 200}}, wantPuts: 2},
		{name: "409 then 412", script: []s3Reply{conflict, {status: 412, code: "PreconditionFailed"}}, want: ErrObjectExists, wantPuts: 2},
		{name: "409 four times", script: []s3Reply{conflict, conflict, conflict, conflict}, want: ErrConditionalConflict, wantPuts: 4},
		{name: "501", script: []s3Reply{{status: 501, code: "NotImplemented", message: "A header you provided implies functionality that is not implemented"}}, want: ErrConditionalWriteUnsupported, wantPuts: 1},
		{name: "400 naming the header", script: []s3Reply{{status: 400, code: "InvalidArgument", message: "If-None-Match is not supported"}}, want: ErrConditionalWriteUnsupported, wantPuts: 1},
		{name: "500", script: []s3Reply{{status: 500, code: "InternalError"}}, wantOther: true, wantPuts: 1},
		{name: "403", script: []s3Reply{{status: 403, code: "AccessDenied"}}, wantOther: true, wantPuts: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeConditionalS3{script: tt.script}
			store := newFakeS3Store(t, fake)
			err := store.PutIfAbsent(context.Background(), "binlogs/archive.json", strings.NewReader("{}"), 2)
			switch {
			case tt.wantOther:
				if err == nil || errors.Is(err, ErrObjectExists) || errors.Is(err, ErrConditionalConflict) || errors.Is(err, ErrConditionalWriteUnsupported) {
					t.Fatalf("err = %v, want an ordinary error", err)
				}
			case tt.want == nil:
				if err != nil {
					t.Fatalf("err = %v", err)
				}
			case !errors.Is(err, tt.want):
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if fake.puts != tt.wantPuts {
				t.Fatalf("PUT count = %d, want %d", fake.puts, tt.wantPuts)
			}
			for _, h := range fake.headers {
				if h != "*" {
					t.Fatalf("If-None-Match = %q, want *", h)
				}
			}
		})
	}
}

// Only an actual 412 on the second create certifies atomic create (G2-10).
func TestProbeAtomicCreateS3(t *testing.T) {
	conflict := s3Reply{status: 409, code: "ConditionalRequestConflict"}
	tests := []struct {
		name            string
		fake            *fakeConditionalS3
		wantCertified   bool
		wantUnsupported bool
		wantConflict    bool
	}{
		{name: "conforming store", fake: &fakeConditionalS3{fallback: conformingS3}, wantCertified: true},
		{name: "store ignores the header", fake: &fakeConditionalS3{}, wantUnsupported: true},
		{name: "store rejects the header", fake: &fakeConditionalS3{script: []s3Reply{{status: 501, code: "NotImplemented"}}}, wantUnsupported: true},
		{name: "409 on every retry of the second create is inconclusive",
			fake: &fakeConditionalS3{script: []s3Reply{{status: 200}, conflict, conflict, conflict, conflict}}, wantConflict: true},
		{name: "409 then 412 certifies",
			fake: &fakeConditionalS3{script: []s3Reply{{status: 200}, conflict, {status: 412, code: "PreconditionFailed"}}}, wantCertified: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeS3Store(t, tt.fake)
			err := ProbeAtomicCreate(context.Background(), store, "binlogs")
			token, refused := pitrarchive.TokenOf(err)
			switch {
			case tt.wantCertified && err != nil:
				t.Fatalf("probe = %v, want certified", err)
			case tt.wantUnsupported && (!refused || token != pitrarchive.TokenArchiveStoreUnsupported):
				t.Fatalf("probe = %v, want %s", err, pitrarchive.TokenArchiveStoreUnsupported)
			case tt.wantConflict && (refused || !errors.Is(err, ErrConditionalConflict)):
				t.Fatalf("probe = %v, want an inconclusive ErrConditionalConflict that is not a refusal", err)
			}
			if tt.wantCertified || tt.wantConflict {
				if len(tt.fake.deletes) != 1 || !strings.HasPrefix(tt.fake.deletes[0], "binlogs/"+pitrarchive.CASProbeKeyPrefix) {
					t.Fatalf("probe key not deleted: %v", tt.fake.deletes)
				}
			}
		})
	}
}

func TestPVCPutIfAbsentLinkRace(t *testing.T) {
	dir := t.TempDir()
	store, err := newPVCStore(testPVCConfig(dir), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const writers = 16
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := fmt.Sprintf("writer-%d", i)
			errs[i] = store.PutIfAbsent(ctx, "binlogs/archive.json", strings.NewReader(body), int64(len(body)))
		}()
	}
	wg.Wait()
	winner := -1
	for i, err := range errs {
		switch {
		case err == nil:
			if winner >= 0 {
				t.Fatalf("writers %d and %d both created the key", winner, i)
			}
			winner = i
		case !errors.Is(err, ErrObjectExists):
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	if winner < 0 {
		t.Fatal("no writer created the key")
	}
	got, ok, err := store.Get(ctx, "binlogs/archive.json")
	if err != nil || !ok || string(got) != fmt.Sprintf("writer-%d", winner) {
		t.Fatalf("stored %q (%v, %v), want the winner's bytes", got, ok, err)
	}
	keys, err := store.List(ctx, "binlogs/")
	if err != nil || len(keys) != 1 {
		t.Fatalf("List = %v, %v; temporary files must not be listed", keys, err)
	}
	if err := ProbeAtomicCreate(ctx, store, "binlogs"); err != nil {
		t.Fatalf("PVC probe = %v, want certified", err)
	}
	entries, _ := os.ReadDir(dir + "/binlogs")
	if len(entries) != 1 {
		t.Fatalf("probe left files behind: %v", entries)
	}
}

func TestMapLinkError(t *testing.T) {
	tests := []struct {
		errno syscall.Errno
		want  error
	}{
		{syscall.EEXIST, ErrObjectExists},
		{syscall.EPERM, ErrConditionalWriteUnsupported},
		{syscall.ENOTSUP, ErrConditionalWriteUnsupported},
		{syscall.EOPNOTSUPP, ErrConditionalWriteUnsupported},
		{syscall.EXDEV, ErrConditionalWriteUnsupported},
		{syscall.EIO, nil},
	}
	for _, tt := range tests {
		t.Run(tt.errno.Error(), func(t *testing.T) {
			err := mapLinkError(&os.LinkError{Op: "link", Old: "a", New: "b", Err: tt.errno})
			if tt.want == nil {
				if errors.Is(err, ErrObjectExists) || errors.Is(err, ErrConditionalWriteUnsupported) || !errors.Is(err, tt.errno) {
					t.Fatalf("mapLinkError(%v) = %v, want the errno unchanged", tt.errno, err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("mapLinkError(%v) = %v, want %v", tt.errno, err, tt.want)
			}
		})
	}
}

type plainOnlyStore struct{ ArchiveStore }

func TestPutIfAbsentWithoutAtomicCreateFailsClosed(t *testing.T) {
	store := plainOnlyStore{newMemStore()}
	if err := PutIfAbsent(context.Background(), store, "k", strings.NewReader("x"), 1); !errors.Is(err, ErrConditionalWriteUnsupported) {
		t.Fatalf("PutIfAbsent = %v, want ErrConditionalWriteUnsupported", err)
	}
	err := ProbeAtomicCreate(context.Background(), store, "binlogs")
	if token, ok := pitrarchive.TokenOf(err); !ok || token != pitrarchive.TokenArchiveStoreUnsupported {
		t.Fatalf("probe = %v, want %s", err, pitrarchive.TokenArchiveStoreUnsupported)
	}
}

func TestEncryptedPutIfAbsent(t *testing.T) {
	ctx := context.Background()
	inner := newMemStore()
	store := WrapWithEncryption(inner, []byte("archive key"))
	if err := PutIfAbsent(ctx, store, "stamp", strings.NewReader("first"), 5); err != nil {
		t.Fatal(err)
	}
	if err := PutIfAbsent(ctx, store, "stamp", strings.NewReader("second"), 6); !errors.Is(err, ErrObjectExists) {
		t.Fatalf("second create = %v, want ErrObjectExists", err)
	}
	got, ok, err := store.Get(ctx, "stamp")
	if err != nil || !ok || string(got) != "first" {
		t.Fatalf("Get = %q %v %v", got, ok, err)
	}
	raw, _, _ := inner.Get(ctx, "stamp")
	if bytes.Contains(raw, []byte("first")) {
		t.Fatal("encrypted store wrote plaintext")
	}
}
