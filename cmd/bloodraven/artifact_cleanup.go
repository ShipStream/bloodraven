package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// cleanupTerminationLogPath is where the verdict is written for the
// MysqlBackup finalizer to read from the pod status. Tests override it.
var cleanupTerminationLogPath = "/dev/termination-log"

// cleanupDeleteWorkers bounds concurrent DeleteObject calls. DeleteObjects
// (batch) is avoided because GCS's S3-compatible API does not implement it.
const cleanupDeleteWorkers = 16

// cleanupStore is the subset of sidecar.ArchiveStore that cleanup needs.
type cleanupStore interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// artifactCleanupRequest is the env contract of `bloodraven artifact-cleanup`:
//
//	BLOODRAVEN_BACKUP_NAME           MysqlBackup name; must be the location's last segment
//	BLOODRAVEN_STORAGE_TYPE          S3 | PVC
//	BLOODRAVEN_OUTPUT_URL            MysqlBackup.status.location
//	BLOODRAVEN_S3_BUCKET             bucket from status.artifactStorage (S3)
//	BLOODRAVEN_S3_ENDPOINT_OVERRIDE  endpoint from status.artifactStorage (S3, optional)
//	AWS_REGION                       region from status.artifactStorage (S3, optional)
//	BLOODRAVEN_AWS_CREDS_DIR         mounted credentialsSecret (S3)
//	BLOODRAVEN_PVC_MOUNT_PATH        mount of the claim from status.artifactStorage (PVC)
type artifactCleanupRequest struct {
	Name, StorageType, Location     string
	Bucket, Endpoint, Region, Creds string
	MountPath                       string
}

func artifactCleanupRequestFromEnv(getenv func(string) string) artifactCleanupRequest {
	return artifactCleanupRequest{
		Name:        getenv("BLOODRAVEN_BACKUP_NAME"),
		StorageType: strings.TrimSpace(getenv("BLOODRAVEN_STORAGE_TYPE")),
		Location:    getenv("BLOODRAVEN_OUTPUT_URL"),
		Bucket:      getenv("BLOODRAVEN_S3_BUCKET"),
		Endpoint:    getenv("BLOODRAVEN_S3_ENDPOINT_OVERRIDE"),
		Region:      getenv("AWS_REGION"),
		Creds:       getenv("BLOODRAVEN_AWS_CREDS_DIR"),
		MountPath:   getenv("BLOODRAVEN_PVC_MOUNT_PATH"),
	}
}

// cleanupError carries the process exit code for a failed run.
type cleanupError struct {
	code int
	err  error
}

func (e *cleanupError) Error() string { return e.err.Error() }

func rejected(format string, args ...any) *cleanupError {
	return &cleanupError{code: backupmeta.CleanupExitRejected, err: fmt.Errorf(format, args...)}
}

func transient(format string, args ...any) *cleanupError {
	return &cleanupError{code: backupmeta.CleanupExitTransient, err: fmt.Errorf(format, args...)}
}

// runArtifactCleanup is the entry point for `bloodraven artifact-cleanup`,
// the main container of the MysqlBackup finalizer's cleanup Job. It deletes
// exactly one backup artifact: every object under "<prefix>/<name>/" in S3,
// or the "<mount>/[<subPath>/]<name>" tree on a PVC.
//
// Exit codes: 0 done (including nothing found), 64 refused or misconfigured,
// 75 transient storage error (backupmeta.CleanupExit*). A crash exits 2. The verdict is written to the termination log:
// "BLOODRAVEN_CLEANUP_COMPLETE objects=<n>" or "BLOODRAVEN_CLEANUP_FAILED: <reason>".
func runArtifactCleanup(_ []string) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()
	os.Exit(artifactCleanup(ctx, artifactCleanupRequestFromEnv(os.Getenv), newCleanupStore, os.Stdout, logger))
}

func newCleanupStore(ctx context.Context, cfg *sidecar.PITRConfig) (cleanupStore, error) {
	return sidecar.NewArchiveStore(ctx, cfg)
}

func artifactCleanup(ctx context.Context, req artifactCleanupRequest, newStore func(context.Context, *sidecar.PITRConfig) (cleanupStore, error), out io.Writer, logger *slog.Logger) int {
	logger = logger.With("subcommand", "artifact-cleanup", "backup", req.Name, "storageType", req.StorageType, "location", req.Location)
	_, _ = fmt.Fprintf(out, "BLOODRAVEN_CLEANUP_START type=%s output=%s\n", req.StorageType, req.Location)

	objects, cerr := runCleanupRequest(ctx, req, newStore)
	if cerr != nil {
		verdict := backupmeta.CleanupFailedPrefix + " " + cerr.Error()
		logger.Error("artifact cleanup failed", "exitCode", cerr.code, "error", cerr.err)
		_, _ = fmt.Fprintln(out, verdict)
		writeCleanupTerminationLog(verdict)
		return cerr.code
	}
	verdict := backupmeta.FormatCleanupComplete(objects)
	logger.Info("artifact cleanup complete", "objects", objects)
	_, _ = fmt.Fprintln(out, verdict)
	writeCleanupTerminationLog(verdict)
	return 0
}

func writeCleanupTerminationLog(verdict string) {
	// Best effort: outside a pod the path may not exist, and the stdout
	// copy still feeds FallbackToLogsOnError.
	_ = os.WriteFile(cleanupTerminationLogPath, []byte(verdict+"\n"), 0o644)
}

func runCleanupRequest(ctx context.Context, req artifactCleanupRequest, newStore func(context.Context, *sidecar.PITRConfig) (cleanupStore, error)) (int, *cleanupError) {
	if req.Name == "" {
		return 0, rejected("BLOODRAVEN_BACKUP_NAME is required")
	}
	switch req.StorageType {
	case "S3":
		prefix, cerr := cleanupS3Prefix(req.Location, req.Bucket, req.Name)
		if cerr != nil {
			return 0, cerr
		}
		store, err := newStore(ctx, &sidecar.PITRConfig{StorageType: "S3", S3: &sidecar.PITRS3Config{
			Bucket: req.Bucket, Region: req.Region, EndpointURL: req.Endpoint, AWSCredsDir: req.Creds,
			// mysqlsh, which wrote the artifact, assumes us-east-1 when no
			// region is configured; the Go SDK refuses to sign without one.
			DefaultRegion: "us-east-1",
		}})
		if err != nil {
			return 0, rejected("init S3 store: %v", err)
		}
		return cleanupS3(ctx, store, prefix)
	case "PVC":
		rel, cerr := cleanupPVCPath(req.Location, req.MountPath, req.Name)
		if cerr != nil {
			return 0, cerr
		}
		return cleanupPVC(req.MountPath, rel)
	default:
		return 0, rejected("unknown storage type %q", req.StorageType)
	}
}

// cleanupS3Prefix turns a status.location into the object-key prefix that
// holds exactly this artifact. Plain dumps record a bucket-relative prefix;
// encrypted dumps record s3://<bucket>/<prefix>/. The result always ends in
// "/" so "x/backup-1" can never match "x/backup-10/...".
func cleanupS3Prefix(location, bucket, name string) (string, *cleanupError) {
	if bucket == "" {
		return "", rejected("S3 cleanup requires a bucket")
	}
	key := location
	if rest, ok := strings.CutPrefix(location, "s3://"); ok {
		locBucket, k, _ := strings.Cut(rest, "/")
		if locBucket != bucket {
			return "", rejected("refusing cross-bucket delete: location bucket %q, artifact bucket %q", locBucket, bucket)
		}
		key = k
	}
	if strings.HasPrefix(key, "/") {
		return "", rejected("S3 location %q has an absolute key", location)
	}
	key = strings.TrimRight(key, "/")
	if err := checkArtifactSegments(key, name); err != nil {
		return "", rejected("S3 location %q: %v", location, err)
	}
	return key + "/", nil
}

// cleanupPVCPath returns the mount-relative path of the artifact directory.
func cleanupPVCPath(location, mount, name string) (string, *cleanupError) {
	if mount == "" || !filepath.IsAbs(mount) {
		return "", rejected("PVC cleanup requires an absolute BLOODRAVEN_PVC_MOUNT_PATH")
	}
	if !filepath.IsAbs(location) {
		return "", rejected("PVC location %q is not absolute", location)
	}
	rel, err := filepath.Rel(filepath.Clean(mount), filepath.Clean(location))
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", rejected("refusing to delete %q: outside mount %q or the mount root", location, mount)
	}
	if err := checkArtifactSegments(filepath.ToSlash(rel), name); err != nil {
		return "", rejected("PVC location %q: %v", location, err)
	}
	return rel, nil
}

func checkArtifactSegments(key, name string) error {
	if key == "" {
		return errors.New("empty artifact path")
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("invalid path segment %q", seg)
		}
	}
	if base := path.Base(key); base != name {
		return fmt.Errorf("last path segment %q is not the backup name %q", base, name)
	}
	return nil
}

// pagedLister is implemented by the S3 store: it hands over one
// ListObjectsV2 page at a time, so a prefix with millions of objects is
// deleted without first holding every key in memory.
type pagedLister interface {
	ListPages(ctx context.Context, prefix string, fn func(keys []string) error) error
}

func cleanupS3(ctx context.Context, store cleanupStore, prefix string) (int, *cleanupError) {
	deleted := 0
	visit := func(keys []string) error {
		for _, key := range keys {
			if !strings.HasPrefix(key, prefix) {
				return rejected("store listed key %q outside prefix %q", key, prefix)
			}
		}
		n, err := deleteKeys(ctx, store, keys)
		deleted += n
		if err != nil {
			return transient("delete under %s: %v", prefix, err)
		}
		return nil
	}
	var err error
	if pl, ok := store.(pagedLister); ok {
		err = pl.ListPages(ctx, prefix, visit)
	} else {
		var keys []string
		if keys, err = store.List(ctx, prefix); err == nil {
			err = visit(keys)
		}
	}
	var cerr *cleanupError
	if errors.As(err, &cerr) {
		return deleted, cerr
	}
	if err != nil {
		return deleted, transient("list %s: %v", prefix, err)
	}
	remaining, err := store.List(ctx, prefix)
	if err != nil {
		return deleted, transient("re-list %s: %v", prefix, err)
	}
	if len(remaining) > 0 {
		return deleted, transient("%d objects remain under %s after delete", len(remaining), prefix)
	}
	return deleted, nil
}

// deleteKeys deletes keys with up to cleanupDeleteWorkers concurrent
// DeleteObject calls and returns how many succeeded and the first error.
func deleteKeys(ctx context.Context, store cleanupStore, keys []string) (int, error) {
	work := make(chan string)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		deleted  int
		firstErr error
	)
	for range min(cleanupDeleteWorkers, len(keys)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range work {
				err := store.Delete(ctx, key)
				mu.Lock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
				} else {
					deleted++
				}
				mu.Unlock()
			}
		}()
	}
	for _, key := range keys {
		work <- key
	}
	close(work)
	wg.Wait()
	return deleted, firstErr
}

// cleanupPVC removes rel through an os.Root anchored at the mount, so every
// path component is resolved inside the mount at the moment of use: a
// directory swapped for a symlink after validation cannot redirect the delete.
func cleanupPVC(mount, rel string) (int, *cleanupError) {
	root, err := os.OpenRoot(mount)
	if err != nil {
		return 0, transient("open mount %s: %v", mount, err)
	}
	defer func() { _ = root.Close() }()
	files := 0
	walkErr := fs.WalkDir(root.FS(), filepath.ToSlash(rel), func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files++
		}
		return nil
	})
	if errors.Is(walkErr, fs.ErrNotExist) {
		return 0, nil
	}
	if pathEscapes(walkErr) {
		return 0, rejected("refusing to delete %s: %v", rel, walkErr)
	}
	if walkErr != nil {
		return 0, transient("inspect %s: %v", rel, walkErr)
	}
	if err := root.RemoveAll(rel); err != nil {
		if pathEscapes(err) {
			return 0, rejected("refusing to delete %s: %v", rel, err)
		}
		return 0, transient("remove %s: %v", rel, err)
	}
	return files, nil
}

// pathEscapes reports os.Root's refusal to follow a path out of the mount.
// It is a refusal, not a storage error: retrying cannot make it succeed.
// os does not export the sentinel, so match its text.
func pathEscapes(err error) bool {
	return err != nil && strings.Contains(err.Error(), "path escapes from parent")
}
