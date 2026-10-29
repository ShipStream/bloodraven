package sidecar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

func TestRereview3S3ResetResumesBytesIncludingEncryption(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			plain := bytes.Repeat([]byte("binlog immutable content"), 10000)
			wire := plain
			if encrypted {
				var out bytes.Buffer
				if _, err := backupcrypto.Encrypt(&out, bytes.NewReader(plain), []byte("fixture-key")); err != nil {
					t.Fatal(err)
				}
				wire = out.Bytes()
			}
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := requests.Add(1)
				w.Header().Set("ETag", `"immutable"`)
				if call == 1 {
					w.Header().Set("Content-Length", strconv.Itoa(len(wire)))
					_, _ = w.Write(wire[:70000]) // close an incomplete response: real transport EOF
					return
				}
				if r.Header.Get("If-Match") != `"immutable"` {
					t.Error("resume was not pinned to the original object")
				}
				var offset int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &offset); err != nil || offset == 0 {
					t.Errorf("stream restarted rather than resumed: %s", r.Header.Get("Range"))
					w.WriteHeader(400)
					return
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(wire)-1, len(wire)))
				w.Header().Set("Content-Length", strconv.Itoa(len(wire)-offset))
				w.WriteHeader(206)
				_, _ = w.Write(wire[offset:])
			}))
			defer server.Close()
			backend := &s3Store{bucket: "fixture", client: s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: server.Client(), RetryMaxAttempts: 1})}
			var store ArchiveStore = backend
			if encrypted {
				store = WrapWithEncryption(store, []byte("fixture-key"))
			}
			r, err := store.Open(context.Background(), "binlog")
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil || !bytes.Equal(got, plain) {
				t.Fatalf("resume lost bytes/authentication: %d %v", len(got), err)
			}
			if requests.Load() != 2 {
				t.Fatalf("unexpected resume count: %d", requests.Load())
			}
		})
	}
}
func TestRereview3S3ResumeRejectsChangedObject(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("ETag", `"old"`)
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write([]byte("short"))
			return
		}
		w.Header().Set("ETag", `"new"`)
		w.Header().Set("Content-Range", "bytes 5-99/100")
		w.WriteHeader(206)
		_, _ = w.Write(bytes.Repeat([]byte("x"), 95))
	}))
	defer server.Close()
	backend := &s3Store{bucket: "fixture", client: s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: server.Client(), RetryMaxAttempts: 1})}
	r, err := backend.Open(context.Background(), "binlog")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	_ = r.Close()
	if err == nil || !strings.Contains(err.Error(), "object changed") {
		t.Fatalf("mixed bytes of different objects: %v", err)
	}
}

type round3RemoteReader struct {
	io.Reader
	closed chan struct{}
}

func (r *round3RemoteReader) Close() error {
	select {
	case <-r.closed:
	default:
		close(r.closed)
	}
	return nil
}

type round3RemoteStore struct {
	ArchiveStore
	first  *round3RemoteReader
	opened bool
}

func (s *round3RemoteStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if strings.HasSuffix(key, "mysql-bin.000001") && !s.opened {
		s.opened = true
		return s.first, nil
	}
	return s.ArchiveStore.Open(ctx, key)
}
func TestRereview3LegacyScanClosesStreamWithoutAnotherScan(t *testing.T) {
	a, base, _ := historyBudgetFixture(t, 1)
	r := &round3RemoteReader{Reader: io.LimitReader(historyZeroReader{}, 100<<20), closed: make(chan struct{})}
	a.store = &round3RemoteStore{ArchiveStore: base, first: r}
	defer func() {
		if a.historyTask != nil {
			a.historyTask.close()
		}
	}()
	a.scanAndArchive(context.Background())
	select {
	case <-r.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy verification parked a remote GET until the next scan")
	}
}
