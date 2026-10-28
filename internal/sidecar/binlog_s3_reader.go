package sidecar

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// A transport reset resumes ciphertext bytes under the same ETag, so the
// decrypter and plaintext hash retain their exact state. No file is copied.
type archiveS3Reader struct {
	store        *s3Store
	ctx          context.Context
	cancel       context.CancelFunc
	key, etag    string
	offset, size int64
	mu           sync.Mutex
	body         io.ReadCloser
	pending      error
}

func (s *s3Store) openResumable(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	response, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("s3 open %s: %w", key, err)
	}
	return &archiveS3Reader{store: s, ctx: ctx, cancel: cancel, key: key, etag: aws.ToString(response.ETag), size: aws.ToInt64(response.ContentLength), body: response.Body}, nil
}
func (r *archiveS3Reader) Close() error {
	r.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Close()
}
func (r *archiveS3Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for retries := 0; ; {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		err := r.pending
		r.pending = nil
		if err == nil {
			r.mu.Lock()
			body := r.body
			r.mu.Unlock()
			n, readErr := body.Read(p)
			r.offset += int64(n)
			if readErr == nil {
				return n, nil
			}
			if n > 0 {
				r.pending = readErr
				return n, nil
			}
			err = readErr
		}
		if err == io.EOF && r.offset == r.size {
			return 0, io.EOF
		}
		if r.etag == "" || r.size <= 0 || r.offset >= r.size {
			return 0, err
		}
		if retries == 3 {
			return 0, fmt.Errorf("s3 stream %s could not resume at byte %d: %w", r.key, r.offset, err)
		}
		retries++
		r.mu.Lock()
		_ = r.body.Close()
		r.mu.Unlock()
		response, openErr := r.store.client.GetObject(r.ctx, &s3.GetObjectInput{Bucket: aws.String(r.store.bucket), Key: aws.String(r.key), IfMatch: aws.String(r.etag), Range: aws.String(fmt.Sprintf("bytes=%d-", r.offset))})
		if openErr != nil {
			r.pending = openErr
			continue
		}
		if aws.ToString(response.ETag) != r.etag || !strings.HasPrefix(aws.ToString(response.ContentRange), fmt.Sprintf("bytes %d-", r.offset)) || !strings.HasSuffix(aws.ToString(response.ContentRange), fmt.Sprintf("/%d", r.size)) {
			_ = response.Body.Close()
			return 0, fmt.Errorf("s3 object changed or range not honored for %s", r.key)
		}
		r.mu.Lock()
		if r.ctx.Err() != nil {
			_ = response.Body.Close()
			r.mu.Unlock()
			return 0, r.ctx.Err()
		}
		r.body = response.Body
		r.mu.Unlock()
	}
}
