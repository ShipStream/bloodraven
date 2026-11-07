package main

import (
	"bytes"
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

	"github.com/shipstream/bloodraven/internal/backupcrypto"
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
//	BLOODRAVEN_OUTPUT_URL            MysqlBackup.status.artifactStorage.location
//	BLOODRAVEN_S3_BUCKET             bucket from status.artifactStorage (S3)
//	BLOODRAVEN_S3_ENDPOINT_OVERRIDE  endpoint from status.artifactStorage (S3, optional)
//	AWS_REGION                       region from status.artifactStorage (S3, optional)
//	BLOODRAVEN_AWS_CREDS_DIR         the mounted s3.credentialsSecretName Secret (S3)
//	BLOODRAVEN_PVC_MOUNT_PATH        mount of the claim from status.artifactStorage (PVC)
//
// A backup that never succeeded is cleaned up in abandoned mode
// (BLOODRAVEN_CLEANUP_MODE=abandoned): BLOODRAVEN_OUTPUT_URL is then the
// planned destination from status.artifactStorage, and the artifact is
// deleted only when the stamp at its key names BLOODRAVEN_BACKUP_UID. An
// encrypted backup's stamp is BRV1, so the Job also mounts the passphrase
// (BLOODRAVEN_BACKUP_PASSPHRASE_FILE).
type artifactCleanupRequest struct {
	Name, StorageType, Location     string
	Bucket, Endpoint, Region, Creds string
	MountPath                       string
	Mode, UID, PassphraseFile       string
}

// cleanupModeAbandoned cleans up after a backup that never succeeded.
const cleanupModeAbandoned = "abandoned"

func artifactCleanupRequestFromEnv(getenv func(string) string) artifactCleanupRequest {
	return artifactCleanupRequest{
		Name:           getenv("BLOODRAVEN_BACKUP_NAME"),
		StorageType:    strings.TrimSpace(getenv("BLOODRAVEN_STORAGE_TYPE")),
		Location:       getenv("BLOODRAVEN_OUTPUT_URL"),
		Bucket:         getenv("BLOODRAVEN_S3_BUCKET"),
		Endpoint:       getenv("BLOODRAVEN_S3_ENDPOINT_OVERRIDE"),
		Region:         getenv("AWS_REGION"),
		Creds:          getenv("BLOODRAVEN_AWS_CREDS_DIR"),
		MountPath:      getenv("BLOODRAVEN_PVC_MOUNT_PATH"),
		Mode:           strings.TrimSpace(getenv("BLOODRAVEN_CLEANUP_MODE")),
		UID:            getenv("BLOODRAVEN_BACKUP_UID"),
		PassphraseFile: getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE"),
	}
}

// cleanupError carries the process exit code for a failed run.
type cleanupError struct {
	code int
	err  error
	// skipped marks an abandoned cleanup that proved nothing and deleted
	// nothing (exit 0, verdict backupmeta.CleanupSkippedPrefix).
	skipped bool
}

// skippedCleanup is an abandoned cleanup that deletes nothing because the
// location's objects cannot be shown to be the backup's.
func skippedCleanup(format string, args ...any) *cleanupError {
	return &cleanupError{code: 0, err: fmt.Errorf(format, args...), skipped: true}
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
	if cerr != nil && cerr.skipped {
		verdict := backupmeta.FormatCleanupSkipped(cerr.Error())
		logger.Warn("artifact cleanup skipped", "reason", cerr.err)
		_, _ = fmt.Fprintln(out, verdict)
		writeCleanupTerminationLog(verdict)
		return 0
	}
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
	switch req.Mode {
	case "":
	case cleanupModeAbandoned:
		return runAbandonedCleanup(ctx, req, newStore)
	default:
		return 0, rejected("unknown BLOODRAVEN_CLEANUP_MODE %q", req.Mode)
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
		n, cerr := cleanupS3(ctx, store, prefix)
		if cerr != nil {
			return n, cerr
		}
		// The backup stamp is a sibling object of the dump prefix (D-8).
		stamp := backupmeta.StampKey(prefix)
		if err := store.Delete(ctx, stamp); err != nil {
			return n, transient("delete backup stamp %s: %v", stamp, err)
		}
		return n, nil
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

// stampGetter is the read the abandoned mode needs on S3 to see whose stamp
// sits at the artifact's key.
type stampGetter interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
}

// runAbandonedCleanup removes what a backup that never succeeded left at
// its planned destination (review B1): its backup stamp, written before the
// dump, and any part of a dump (or, in the encrypted layout, a whole upload
// whose stamp could not be finished).
//
// Ownership is proven by the stamp alone. The stamp key is derived from the
// dump location (prefix plus backup name), so it is shared by every backup
// of that name under the prefix: earlier incarnations in this namespace and
// same-name backups in other namespaces. A backup's Job writes the stamp
// before anything else, only on a location that holds no object, and
// refuses a location that holds another backup's stamp, with one
// exception: the unfinished stamp of an earlier incarnation of the same
// namespace and name is replaced, and only once nothing remains under the
// location (backup-stamp's abandonedStampOf). So the objects under a
// location whose stamp names this backup's UID are this backup's. No stamp,
// or another backup's stamp, deletes nothing: the location may belong to a
// live backup in another namespace that shares the prefix, or to a later
// backup of the same name. The dump is deleted before the stamp, so an
// interrupted run keeps its proof.
//
// Deleting nothing is reported two ways. Another backup's stamp means this
// backup never wrote there (its Job writes the stamp first, and only on a
// location with no other stamp), and an empty location holds nothing of
// it: both are complete with zero objects, and the backup's key is no
// longer needed. No stamp over a location that still holds objects (the
// stamp was removed, say by a lifecycle rule) may be this backup's
// unproven dump: that is skipped (backupmeta.CleanupSkippedPrefix), so the
// finalizer keeps the key that decrypts it (U05).
func runAbandonedCleanup(ctx context.Context, req artifactCleanupRequest, newStore func(context.Context, *sidecar.PITRConfig) (cleanupStore, error)) (int, *cleanupError) {
	if req.UID == "" {
		return 0, rejected("abandoned cleanup requires BLOODRAVEN_BACKUP_UID")
	}
	var passphrase []byte
	if req.PassphraseFile != "" {
		p, err := backupcrypto.ReadPassphraseFile(req.PassphraseFile)
		if err != nil {
			return 0, rejected("read backup passphrase: %v", err)
		}
		passphrase = p
	}
	switch req.StorageType {
	case "S3":
		prefix, cerr := cleanupS3Prefix(req.Location, req.Bucket, req.Name)
		if cerr != nil {
			return 0, cerr
		}
		store, err := newStore(ctx, &sidecar.PITRConfig{StorageType: "S3", S3: &sidecar.PITRS3Config{
			Bucket: req.Bucket, Region: req.Region, EndpointURL: req.Endpoint, AWSCredsDir: req.Creds,
			DefaultRegion: "us-east-1",
		}})
		if err != nil {
			return 0, rejected("init S3 store: %v", err)
		}
		getter, ok := store.(stampGetter)
		if !ok {
			return 0, rejected("S3 store %T cannot read the backup stamp", store)
		}
		stampKey := backupmeta.StampKey(prefix)
		raw, found, err := getter.Get(ctx, stampKey)
		if err != nil {
			return 0, transient("read backup stamp %s: %v", stampKey, err)
		}
		if owned, cerr := abandonedStampOwned(raw, found, passphrase, req.UID, stampKey); cerr != nil || !owned {
			if cerr == nil && !found {
				cerr = abandonedUnstampedS3(ctx, store, prefix, stampKey)
			}
			return 0, cerr
		}
		n, cerr := cleanupS3(ctx, store, prefix)
		if cerr != nil {
			return n, cerr
		}
		if err := store.Delete(ctx, stampKey); err != nil {
			return n, transient("delete backup stamp %s: %v", stampKey, err)
		}
		return n, nil
	case "PVC":
		rel, cerr := cleanupPVCPath(req.Location, req.MountPath, req.Name)
		if cerr != nil {
			return 0, cerr
		}
		root, err := os.OpenRoot(req.MountPath)
		if err != nil {
			return 0, transient("open mount %s: %v", req.MountPath, err)
		}
		stampKey := backupmeta.StampKey(filepath.ToSlash(rel))
		raw, err := root.ReadFile(stampKey)
		_ = root.Close()
		found := err == nil
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case pathEscapes(err):
			return 0, rejected("refusing to read %s: %v", stampKey, err)
		case err != nil:
			return 0, transient("read backup stamp %s: %v", stampKey, err)
		}
		if owned, cerr := abandonedStampOwned(raw, found, passphrase, req.UID, stampKey); cerr != nil || !owned {
			if cerr == nil && !found {
				cerr = abandonedUnstampedPVC(req.MountPath, rel, stampKey)
			}
			return 0, cerr
		}
		return cleanupPVC(req.MountPath, rel)
	default:
		return 0, rejected("unknown storage type %q", req.StorageType)
	}
}

// abandonedUnstampedS3 is the verdict on an S3 location with no stamp: nil
// (complete, nothing deleted) when nothing is listed under prefix, else
// skipped.
func abandonedUnstampedS3(ctx context.Context, store cleanupStore, prefix, stampKey string) *cleanupError {
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return transient("list %s: %v", prefix, err)
	}
	if len(keys) == 0 {
		return nil
	}
	return skippedCleanup("no backup stamp at %s names this backup, but %d objects remain under %s; nothing was deleted", stampKey, len(keys), prefix)
}

// abandonedUnstampedPVC is abandonedUnstampedS3 for a PVC location.
func abandonedUnstampedPVC(mount, rel, stampKey string) *cleanupError {
	root, err := os.OpenRoot(mount)
	if err != nil {
		return transient("open mount %s: %v", mount, err)
	}
	defer func() { _ = root.Close() }()
	_, err = root.Stat(rel)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case pathEscapes(err):
		return rejected("refusing to read %s: %v", rel, err)
	case err != nil:
		return transient("stat %s: %v", rel, err)
	}
	return skippedCleanup("no backup stamp at %s names this backup, but %s still exists; nothing was deleted", stampKey, rel)
}

// abandonedStampOwned decodes the stamp read at key and reports whether it
// names uid. A missing stamp or another backup's stamp is not this
// backup's (false, nil); a stamp that cannot be read refuses, because
// whose it is cannot be told.
func abandonedStampOwned(raw []byte, found bool, passphrase []byte, uid, key string) (bool, *cleanupError) {
	if !found {
		return false, nil
	}
	if passphrase != nil {
		if !backupcrypto.LooksEncrypted(raw) {
			return false, rejected("backup stamp %s is not BRV1-encrypted, but the backup is", key)
		}
		var plain bytes.Buffer
		if _, err := backupcrypto.Decrypt(&plain, bytes.NewReader(raw), passphrase); err != nil {
			return false, rejected("decrypt backup stamp %s: %v", key, err)
		}
		raw = plain.Bytes()
	}
	stamp, err := backupmeta.ParseStamp(raw)
	if err != nil {
		return false, rejected("backup stamp %s: %v", key, err)
	}
	return string(stamp.Backup.UID) == uid, nil
}

// cleanupS3Prefix turns a status.artifactStorage.location into the object-key prefix that
// holds exactly this artifact. Plain dumps record a bucket-relative prefix;
// encrypted dumps record s3://<bucket>/<prefix>/. The result always ends in
// "/" so "x/backup-1" can never match "x/backup-10/...".
func cleanupS3Prefix(location, bucket, name string) (string, *cleanupError) {
	if bucket == "" {
		return "", rejected("S3 cleanup requires a bucket")
	}
	locBucket, key, inBucket := backupmeta.SplitS3Location(location)
	if inBucket && locBucket != bucket {
		return "", rejected("refusing cross-bucket delete: location bucket %q, artifact bucket %q", locBucket, bucket)
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

// cleanupPVC removes rel, then its backup stamp (a sibling file, D-8),
// through an os.Root anchored at the mount, so every path component is
// resolved inside the mount at the moment of use: a directory swapped for a
// symlink after validation cannot redirect the delete.
func cleanupPVC(mount, rel string) (int, *cleanupError) {
	root, err := os.OpenRoot(mount)
	if err != nil {
		return 0, transient("open mount %s: %v", mount, err)
	}
	defer func() { _ = root.Close() }()
	files, cerr := cleanupPVCTree(root, rel)
	if cerr != nil {
		return files, cerr
	}
	stamp := backupmeta.StampKey(filepath.ToSlash(rel))
	if err := root.Remove(stamp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		if pathEscapes(err) {
			return files, rejected("refusing to delete %s: %v", stamp, err)
		}
		return files, transient("remove backup stamp %s: %v", stamp, err)
	}
	return files, nil
}

func cleanupPVCTree(root *os.Root, rel string) (int, *cleanupError) {
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
