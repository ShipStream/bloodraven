package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// archiveStore abstracts the two storage backends (S3 and local PVC)
// behind a tiny upload/download/delete/list interface. The archiver
// and the future retention sweeper both program against this — so
// adding a third backend (GCS, Azure) later is localized to this file.
type archiveStore interface {
	// Put writes data under key. Overwrites are OK: the archiver treats
	// the write as idempotent by key.
	Put(ctx context.Context, key string, r io.Reader, size int64) error

	// Get reads the object at key into a new byte slice. Used for
	// manifest read-modify-write; binlog objects themselves are only
	// ever consumed by the restore Job, not by the sidecar.
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// Delete removes the object at key. Missing-key is a no-op (returns
	// nil). Used by retention cleanup.
	Delete(ctx context.Context, key string) error

	// PutFile is a file-path convenience wrapper that streams the file
	// at path to the object at key. For S3 it uses the managed uploader
	// (multipart if large); for PVC it is a sendfile-ish copy.
	PutFile(ctx context.Context, key, path string) error
}

// NewArchiveStore is the exported constructor used by cmd/sidecar/main.go.
// Delegates to newArchiveStore; keeping the lowercase version around
// for internal tests that want to stub it.
func NewArchiveStore(ctx context.Context, cfg *PITRConfig) (archiveStore, error) {
	return newArchiveStore(ctx, cfg)
}

// newArchiveStore constructs the backend matching cfg. It is called
// once at sidecar startup; the returned store is safe for concurrent
// use from the archiver goroutine and any future retention worker.
func newArchiveStore(ctx context.Context, cfg *PITRConfig) (archiveStore, error) {
	switch cfg.StorageType {
	case "S3":
		if cfg.S3 == nil {
			return nil, fmt.Errorf("archive store: S3 config is nil")
		}
		return newS3Store(ctx, cfg.S3)
	case "PVC":
		if cfg.PVC == nil {
			return nil, fmt.Errorf("archive store: PVC config is nil")
		}
		return newPVCStore(cfg.PVC)
	default:
		return nil, fmt.Errorf("archive store: unknown storage type %q", cfg.StorageType)
	}
}

// ----------------------------------------------------------------------
// S3 implementation
// ----------------------------------------------------------------------

type s3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func newS3Store(ctx context.Context, cfg *PITRS3Config) (*s3Store, error) {
	// Mirror the credential-file layout the backup/restore Python
	// scripts expect: read AWS_* from files under AWSCredsDir if
	// provided, otherwise fall through to the default SDK chain
	// (IRSA, env, instance profile).
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AWSCredsDir != "" {
		ak, _ := readTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_ACCESS_KEY_ID"))
		sk, _ := readTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_SECRET_ACCESS_KEY"))
		st, _ := readTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_SESSION_TOKEN"))
		if rg, _ := readTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_REGION")); cfg.Region == "" && rg != "" {
			loadOpts = append(loadOpts, awsconfig.WithRegion(rg))
		}
		if ak != "" && sk != "" {
			loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(ak, sk, st),
			))
		}
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){}
	if cfg.EndpointURL != "" {
		endpoint := cfg.EndpointURL
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			// Path-style is needed for most S3-compatible stores
			// (MinIO, Ceph) when a custom endpoint is set.
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOpts...)
	uploader := manager.NewUploader(client)

	return &s3Store{client: client, uploader: uploader, bucket: cfg.Bucket}, nil
}

func (s *s3Store) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("s3 upload %s: %w", key, err)
	}
	return nil
}

func (s *s3Store) PutFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	return s.Put(ctx, key, f, fi.Size())
}

func (s *s3Store) Get(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return nil, false, fmt.Errorf("s3 read %s: %w", key, err)
	}
	return buf.Bytes(), true, nil
}

func (s *s3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isS3NotFound(err) {
		return fmt.Errorf("s3 delete %s: %w", key, err)
	}
	return nil
}

// isS3NotFound returns true for the various flavours of "object/key
// does not exist" that the AWS SDK returns: NoSuchKey on GetObject,
// NotFound on HeadObject, 404 status on DeleteObject.
func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	code := apiErr.ErrorCode()
	return code == "NoSuchKey" || code == "NotFound" || code == "404"
}

// ----------------------------------------------------------------------
// PVC (local directory) implementation
// ----------------------------------------------------------------------

type pvcStore struct {
	root string
}

func newPVCStore(cfg *PITRPVCConfig) (*pvcStore, error) {
	if cfg.MountPath == "" {
		return nil, fmt.Errorf("pvc archive store: empty mount path")
	}
	// MountPath is the PVC mount root; binlog objects live under
	// <mount>/binlogs/. We lazily create the directory tree on each
	// Put, so no work needed here.
	return &pvcStore{root: cfg.MountPath}, nil
}

func (p *pvcStore) fullPath(key string) string {
	// Reject leading slashes and .. traversals to keep keys contained
	// within the mounted backup volume.
	clean := filepath.Clean("/" + strings.TrimLeft(key, "/"))
	return filepath.Join(p.root, clean)
}

func (p *pvcStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	dst := p.fullPath(key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	// Write to a tmp file first then rename, so readers never see a
	// partial manifest under concurrent access.
	tmp := dst + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, dst, err)
	}
	return nil
}

func (p *pvcStore) PutFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	return p.Put(ctx, key, f, fi.Size())
}

func (p *pvcStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	data, err := os.ReadFile(p.fullPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read %s: %w", key, err)
	}
	return data, true, nil
}

func (p *pvcStore) Delete(_ context.Context, key string) error {
	err := os.Remove(p.fullPath(key))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// readTrimFile reads a small file and returns its trimmed contents.
// Used for the AWS_* credential files and similar single-line files.
func readTrimFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
