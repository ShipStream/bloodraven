package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

// `bloodraven backup-stamp` writes and finishes a dump's backup stamp (design
// D-8, D-52): the writer-stamped identity of one dump, stored at
// backupmeta.StampKey(<dump location>), a sibling of the dump directory.
//
// It runs twice per backup Job:
//
//   - As the `backup-stamp` init container, before the dump: it reads the
//     dumped server's clock and server_id over the same Service the dump
//     uses, checks the server_id against the one the operator pinned, and
//     writes the stamp with serverClock.start and no end. On a plaintext Job
//     it also installs its own binary into BLOODRAVEN_STAMP_TOOL_DIR, so the
//     mysqlsh container can run the end step.
//   - With --end=<µs UTC>, after mysqlsh exited successfully: dump.py (plain
//     Jobs, through the installed binary) or encrypt-upload (encrypted Jobs,
//     after the upload) adds serverClock.end, read on the dumped server once
//     the dump finished. Every other field must stay byte-identical.
//
// The stamp is BRV1-encrypted with the dump's passphrase when the dump is
// encrypted. Its key is derived from the dump location (the profile prefix
// plus the backup name), so it is unique to one backup name under one
// prefix, not to one MysqlBackup: an earlier incarnation of the same name,
// or a same-name backup in another namespace sharing the prefix, maps to the
// same key. The start step therefore creates a stamp only on an empty dump
// location (an unstamped object there is someone else's), refuses a stamp
// another backup owns, and replaces one exception, the unfinished stamp of
// an earlier incarnation of this namespace and name, only once nothing
// remains under the location (abandonedStampOf). A store without atomic
// create falls back to Put plus a read-back compare; a store that reports a
// concurrent conditional create refuses, because a second writer exists.
//
// Env:
//
//	BLOODRAVEN_BACKUP_STAMP            the stamp template pinned in the Job (JSON, no clock, no createdAt)
//	BLOODRAVEN_STORAGE_TYPE            S3 or PVC (the final destination)
//	BLOODRAVEN_OUTPUT_URL              the dump location (S3 key prefix, or the absolute PVC path)
//	BLOODRAVEN_BACKUP_PASSPHRASE_FILE  set when the dump is encrypted
//	BLOODRAVEN_MYSQL_HOST, BLOODRAVEN_MYSQL_CREDS_DIR, BLOODRAVEN_TLS, BLOODRAVEN_TLS_CA_FILE
//	                                   the dumped server (start only)
//	BLOODRAVEN_STAMP_TOOL_DIR          where to install this binary (start only, plaintext Jobs)
//	plus the S3/PVC storage env storageConfigFromEnv reads.
const (
	backupStampTemplateEnv = "BLOODRAVEN_BACKUP_STAMP"
	backupStampToolDirEnv  = "BLOODRAVEN_STAMP_TOOL_DIR"
	backupStampToolName    = "bloodraven"
)

// ErrBackupStampConflict reports a stamp at this backup's key that is not
// this backup's stamp, or that already records a finished dump.
var ErrBackupStampConflict = errors.New("backup stamp conflict")

func runBackupStamp(args []string) {
	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo).With("subcommand", "backup-stamp")
	fs := flag.NewFlagSet("backup-stamp", flag.ContinueOnError)
	endFlag := fs.String("end", "", "add serverClock.end (µs RFC3339 UTC) to an existing stamp")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()

	fail := func(msg string, err error) {
		logger.Error(msg, "error", err)
		_ = os.WriteFile("/dev/termination-log", []byte(compactTerminationMessage("backup stamp: "+err.Error())), 0644)
		os.Exit(2)
	}

	template, err := backupStampTemplateFromEnv()
	if err != nil {
		fail("read backup stamp template", err)
	}
	cfg, key, err := backupStampStoreConfig(os.Getenv("BLOODRAVEN_STORAGE_TYPE"), os.Getenv("BLOODRAVEN_OUTPUT_URL"))
	if err != nil {
		fail("resolve backup stamp storage", err)
	}
	// The raw store lists the dump location (object names are plaintext
	// even in an encrypted layout); the stamp itself goes through the
	// dump key.
	raw, err := newArchiveStore(ctx, cfg)
	if err != nil {
		fail("init backup stamp store", err)
	}
	store, err := wrapBackupStampStore(raw, os.Getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE"))
	if err != nil {
		fail("init backup stamp store", err)
	}
	logger = logger.With("key", key, "backup", template.Backup.Name, "site", template.Site)

	if *endFlag != "" {
		end, err := backupmeta.ParseMicroTime(*endFlag)
		if err != nil {
			fail("parse serverClock.end", err)
		}
		stamp, err := finishBackupStamp(ctx, store, key, template, end)
		if err != nil {
			fail("finish backup stamp", err)
		}
		logger.Info("backup stamp finished", "serverClockStart", stamp.ServerClock.Start.String(), "serverClockEnd", stamp.ServerClock.End.String())
		return
	}

	qctx, qcancel := context.WithTimeout(ctx, 2*time.Minute)
	start, serverID, err := readDumpServerClock(qctx)
	qcancel()
	if err != nil {
		fail("read dumped server clock", err)
	}
	if err := checkDumpServerID(template, serverID); err != nil {
		fail("check dumped server", err)
	}
	desired := template
	desired.ServerClock = backupmeta.ServerClock{Start: start}
	desired.CreatedAt = backupmeta.NewMicroTime(time.Now())
	stamp, reused, err := writeBackupStamp(ctx, store, dumpLocationLister(cfg, raw), key, desired)
	if err != nil {
		fail("write backup stamp", err)
	}
	if dir := os.Getenv(backupStampToolDirEnv); dir != "" {
		if err := installBackupStampTool(dir); err != nil {
			fail("install backup stamp tool", err)
		}
	}
	logger.Info("backup stamp written", "reused", reused, "serverID", stamp.ServerID,
		"pitrArchiveID", stamp.PITRArchiveID, "serverClockStart", stamp.ServerClock.Start.String())
}

// checkDumpServerID refuses a dump source whose server_id is not the one
// the operator pinned in the stamp template (F26, F41): the Job reached
// another server through the Service.
func checkDumpServerID(template backupmeta.Stamp, serverID uint32) error {
	if serverID != template.ServerID {
		return fmt.Errorf("%w: the dump source for site %s answered with server_id %d, the operator pinned %d; the Job reached the wrong server", ErrBackupStampConflict, template.Site, serverID, template.ServerID)
	}
	return nil
}

// wrapBackupStampStore encrypts the stamp with the dump key when the dump
// is encrypted (passphraseFile set), exactly as newArchiveStore would.
func wrapBackupStampStore(raw sidecar.ArchiveStore, passphraseFile string) (sidecar.ArchiveStore, error) {
	if passphraseFile == "" {
		return raw, nil
	}
	passphrase, err := backupcrypto.ReadPassphraseFile(passphraseFile)
	if err != nil {
		return nil, fmt.Errorf("read backup passphrase: %w", err)
	}
	return sidecar.WrapWithEncryption(raw, passphrase), nil
}

// backupStampStoreFromEnv opens the stamp's store from the Job's env, for
// a step that is not the backup-stamp subcommand itself (encrypt-upload).
func backupStampStoreFromEnv(ctx context.Context) (sidecar.ArchiveStore, string, backupmeta.Stamp, error) {
	template, err := backupStampTemplateFromEnv()
	if err != nil {
		return nil, "", backupmeta.Stamp{}, err
	}
	cfg, key, err := backupStampStoreConfig(os.Getenv("BLOODRAVEN_STORAGE_TYPE"), os.Getenv("BLOODRAVEN_OUTPUT_URL"))
	if err != nil {
		return nil, "", backupmeta.Stamp{}, err
	}
	cfg.PassphraseFile = os.Getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE")
	store, err := newArchiveStore(ctx, cfg)
	if err != nil {
		return nil, "", backupmeta.Stamp{}, err
	}
	return store, key, template, nil
}

// checkBackupStampOwnerFromEnv re-reads the stamp before the encrypted
// upload writes anything under the dump location (review F016): the
// location must still hold this backup's unfinished stamp. A same-name
// backup in another namespace sharing the prefix could otherwise have
// replaced it on a store without atomic create, and this Job's upload would
// overwrite that backup's objects.
func checkBackupStampOwnerFromEnv(ctx context.Context) error {
	store, key, template, err := backupStampStoreFromEnv(ctx)
	if err != nil {
		return err
	}
	return checkBackupStampOwner(ctx, store, key, template)
}

// checkBackupStampOwner requires the stamp at key to be template's
// (identity equal) and unfinished.
func checkBackupStampOwner(ctx context.Context, store sidecar.ArchiveStore, key string, template backupmeta.Stamp) error {
	prev, err := loadOwnStamp(ctx, store, key, template)
	if err != nil {
		return err
	}
	if prev.Complete() {
		return fmt.Errorf("%w: %s already records a finished dump (serverClock.end %s); a finished backup is never uploaded again", ErrBackupStampConflict, key, prev.ServerClock.End)
	}
	return nil
}

// finishBackupStampFromEnv runs the end step from the Job's env, for a
// final step that is not the backup-stamp subcommand itself
// (encrypt-upload).
func finishBackupStampFromEnv(ctx context.Context, end string) (backupmeta.Stamp, error) {
	at, err := parseStampEnd(end)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	store, key, template, err := backupStampStoreFromEnv(ctx)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	return finishBackupStamp(ctx, store, key, template, at)
}

// parseStampEnd parses the serverClock.end the dump reported.
func parseStampEnd(end string) (backupmeta.MicroTime, error) {
	if end == "" {
		return backupmeta.MicroTime{}, fmt.Errorf("the dump did not report serverClock.end")
	}
	at, err := backupmeta.ParseMicroTime(end)
	if err != nil {
		return backupmeta.MicroTime{}, fmt.Errorf("serverClock.end: %w", err)
	}
	return at, nil
}

// backupStampTemplateFromEnv parses the template the operator pinned in the
// Job. Unknown fields are refused: the operator that built the Job is this
// release.
func backupStampTemplateFromEnv() (backupmeta.Stamp, error) {
	raw := os.Getenv(backupStampTemplateEnv)
	if raw == "" {
		return backupmeta.Stamp{}, fmt.Errorf("%s is required", backupStampTemplateEnv)
	}
	var s backupmeta.Stamp
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return backupmeta.Stamp{}, fmt.Errorf("%s: %w", backupStampTemplateEnv, err)
	}
	return s, nil
}

// backupStampStoreConfig resolves the store and stamp key for a dump at
// outputURL. Both steps write, so a PVC store probes write access in the
// stamp's directory.
func backupStampStoreConfig(storageType, outputURL string) (*sidecar.PITRConfig, string, error) {
	cfg, keyPrefix, _, err := storageConfigFromEnv(storageType, outputURL)
	if err != nil {
		return nil, "", err
	}
	if keyPrefix == "" || keyPrefix == "." {
		return nil, "", fmt.Errorf("dump location %q has no key", outputURL)
	}
	if err := backupmeta.CheckDumpLocation(keyPrefix); err != nil {
		return nil, "", err
	}
	key := backupmeta.StampKey(keyPrefix)
	if cfg.S3 != nil {
		// mysqlsh, which writes the dump beside the stamp, assumes
		// us-east-1 when no region is configured; the Go SDK refuses to
		// sign without one (as in artifact-cleanup).
		cfg.S3.DefaultRegion = "us-east-1"
	}
	if cfg.PVC != nil {
		cfg.PVC.Writable = true
		if dir := path.Dir(key); dir != "." {
			cfg.PVC.WritePrefix = dir
		}
	}
	return cfg, key, nil
}

// stampIdentity is the stamp with its clock and creation time removed: what
// must agree between a retry of the same backup and the stamp it finds.
func stampIdentity(s backupmeta.Stamp) ([]byte, error) {
	s.ServerClock = backupmeta.ServerClock{}
	s.CreatedAt = backupmeta.MicroTime{}
	return s.Canonical()
}

func loadBackupStamp(ctx context.Context, store sidecar.ArchiveStore, key string) (backupmeta.Stamp, bool, error) {
	data, ok, err := store.Get(ctx, key)
	if err != nil {
		return backupmeta.Stamp{}, false, fmt.Errorf("read %s: %w", key, err)
	}
	if !ok {
		return backupmeta.Stamp{}, false, nil
	}
	s, err := backupmeta.ParseStamp(data)
	if err != nil {
		return backupmeta.Stamp{}, true, fmt.Errorf("%w: %s holds an unreadable stamp: %v", ErrBackupStampConflict, key, err)
	}
	return s, true, nil
}

// adoptExistingStamp decides what a start step does with a stamp already at
// its key. A retried pod of the same Job finds its earlier attempt's stamp:
// the identity agrees and the dump never finished, so the earlier stamp
// stands (its serverClock.start is the earlier, more conservative bound).
// Anything else refuses.
func adoptExistingStamp(key string, existing, desired backupmeta.Stamp) (backupmeta.Stamp, error) {
	a, err := stampIdentity(existing)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	b, err := stampIdentity(desired)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	if !bytes.Equal(a, b) {
		return backupmeta.Stamp{}, fmt.Errorf("%w: %s already holds a different stamp (%s); want %s", ErrBackupStampConflict, key, a, b)
	}
	if existing.Complete() {
		return backupmeta.Stamp{}, fmt.Errorf("%w: %s already records a finished dump (serverClock.end %s); a finished backup is never dumped again", ErrBackupStampConflict, key, existing.ServerClock.End)
	}
	return existing, nil
}

// abandonedStampOf reports whether existing is the stamp of an earlier
// MysqlBackup with desired's namespace and name (so the earlier object is
// gone: names are unique and its finalizer held the name until cleanup)
// whose dump never finished. A failed backup's stamp outlives it when its
// cleanup could not run; without this a re-created backup of the same name
// would refuse ErrBackupStampConflict on every attempt (review B1).
func abandonedStampOf(existing, desired backupmeta.Stamp) bool {
	return existing.Backup.Namespace == desired.Backup.Namespace &&
		existing.Backup.Name == desired.Backup.Name &&
		existing.Backup.UID != desired.Backup.UID &&
		!existing.Complete()
}

// errStopListing ends a listing once it found what it looked for.
var errStopListing = errors.New("stop listing")

// objectLister is what the start step needs to see whether anything is
// under a dump location. A lister that also implements pagedLister (the
// S3 store, pvcLocationLister) stops at the first page holding an object.
type objectLister interface {
	List(ctx context.Context, prefix string) ([]string, error)
}

// dumpLocationLister returns the lister the start step checks the dump
// location with: the raw store on S3 (paged, and it lists plaintext object
// names in an encrypted layout), or a direct walk of the location on a PVC
// that stops at the first entry.
func dumpLocationLister(cfg *sidecar.PITRConfig, raw sidecar.ArchiveStore) objectLister {
	if cfg.PVC != nil {
		return pvcLocationLister{root: cfg.PVC.MountPath}
	}
	return raw
}

// pvcLocationLister walks a PVC location in pages of one entry, so a large
// leftover dump is not walked in full just to find it is non-empty. Every
// non-directory entry counts, including the temporary files the PVC store
// hides from List: anything there is someone's.
type pvcLocationLister struct{ root string }

func (l pvcLocationLister) List(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	err := l.ListPages(ctx, prefix, func(keys []string) error {
		out = append(out, keys...)
		return nil
	})
	return out, err
}

func (l pvcLocationLister) ListPages(ctx context.Context, prefix string, fn func(keys []string) error) error {
	rel := strings.Trim(prefix, "/")
	if rel == "" || !filepath.IsLocal(filepath.FromSlash(rel)) {
		return fmt.Errorf("list %q: not a location under the PVC mount", prefix)
	}
	dir := filepath.Join(l.root, filepath.FromSlash(rel))
	err := filepath.WalkDir(dir, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if p == dir && errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		key, err := filepath.Rel(l.root, p)
		if err != nil {
			return err
		}
		return fn([]string{filepath.ToSlash(key)})
	})
	if err != nil && !errors.Is(err, errStopListing) {
		return fmt.Errorf("walk %s: %w", dir, err)
	}
	return err
}

// firstObjectUnder returns an object under prefix, if there is one,
// without listing the whole prefix when the lister pages (review F119).
func firstObjectUnder(ctx context.Context, l objectLister, prefix string) (string, bool, error) {
	if pl, ok := l.(pagedLister); ok {
		var first string
		err := pl.ListPages(ctx, prefix, func(keys []string) error {
			if len(keys) == 0 {
				return nil
			}
			first = keys[0]
			return errStopListing
		})
		switch {
		case errors.Is(err, errStopListing):
			return first, true, nil
		case err != nil:
			return "", false, err
		}
		return "", false, nil
	}
	keys, err := l.List(ctx, prefix)
	if err != nil || len(keys) == 0 {
		return "", false, err
	}
	return keys[0], true, nil
}

// dumpLocationOf is the dump location a stamp key belongs to.
func dumpLocationOf(key string) string {
	return strings.TrimSuffix(key, backupmeta.StampSuffix)
}

// checkDumpLocationEmpty refuses a dump location that holds any object
// before this backup's stamp is created there (review F001). The stamp is
// what proves whose objects a location holds, and abandoned cleanup deletes
// everything under a location whose stamp names its backup: a new stamp
// over another backup's unstamped objects would hand them to this backup
// and let its cleanup delete them.
func checkDumpLocationEmpty(ctx context.Context, objects objectLister, key, why string) error {
	location := dumpLocationOf(key)
	first, found, err := firstObjectUnder(ctx, objects, location+"/")
	if err != nil {
		return fmt.Errorf("list %s/: %w", location, err)
	}
	if found {
		return fmt.Errorf("%w: %s; objects remain under %s/ (first %s); they are not this backup's, so it does not dump there: delete them, or use another backup name or prefix, then retry",
			ErrBackupStampConflict, why, location, first)
	}
	return nil
}

// removeAbandonedStamp deletes an abandoned stamp (abandonedStampOf) so the
// caller can create its own with an atomic create. It refuses while any
// object remains under the dump location: the earlier backup may have
// written part of a dump, and the stamp is what proves whose objects those
// are, so it is kept until they are removed.
func removeAbandonedStamp(ctx context.Context, store sidecar.ArchiveStore, objects objectLister, key string, existing backupmeta.Stamp) error {
	why := fmt.Sprintf("%s holds the unfinished stamp of an earlier backup %s/%s (uid %s)", key, existing.Backup.Namespace, existing.Backup.Name, existing.Backup.UID)
	if err := checkDumpLocationEmpty(ctx, objects, key, why); err != nil {
		return err
	}
	if err := store.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete abandoned stamp %s: %w", key, err)
	}
	return nil
}

// writeBackupStamp creates the stamp at key on an empty dump location, or
// adopts a retry's earlier stamp, or replaces an abandoned stamp of an
// earlier same-name backup (abandonedStampOf), and confirms the stored
// bytes by reading them back. objects lists the dump location.
func writeBackupStamp(ctx context.Context, store sidecar.ArchiveStore, objects objectLister, key string, desired backupmeta.Stamp) (backupmeta.Stamp, bool, error) {
	if err := desired.Validate(); err != nil {
		return backupmeta.Stamp{}, false, err
	}
	if desired.Complete() {
		return backupmeta.Stamp{}, false, fmt.Errorf("%w: a new stamp cannot already carry serverClock.end", backupmeta.ErrStampInvalid)
	}
	existing, found, err := loadBackupStamp(ctx, store, key)
	if err != nil {
		return backupmeta.Stamp{}, false, err
	}
	if found {
		s, err := adoptExistingStamp(key, existing, desired)
		if err == nil || !abandonedStampOf(existing, desired) {
			return s, err == nil, err
		}
		if err := removeAbandonedStamp(ctx, store, objects, key, existing); err != nil {
			return backupmeta.Stamp{}, false, err
		}
	} else if err := checkDumpLocationEmpty(ctx, objects, key, fmt.Sprintf("there is no stamp at %s", key)); err != nil {
		return backupmeta.Stamp{}, false, err
	}
	data, err := desired.Canonical()
	if err != nil {
		return backupmeta.Stamp{}, false, err
	}
	switch err := sidecar.PutIfAbsent(ctx, store, key, bytes.NewReader(data), int64(len(data))); {
	case err == nil:
		if err := settleBackupStamp(ctx, store, key, nil, desired, nil); err != nil {
			return backupmeta.Stamp{}, false, err
		}
		return desired, false, nil
	case errors.Is(err, sidecar.ErrObjectExists):
		// A retried create whose first response was lost, or another
		// writer at this key: what is there decides.
		existing, found, loadErr := loadBackupStamp(ctx, store, key)
		if loadErr != nil {
			return backupmeta.Stamp{}, false, fmt.Errorf("create %s: %v; %w", key, err, loadErr)
		}
		if !found {
			return backupmeta.Stamp{}, false, fmt.Errorf("create %s: the store reports the key exists but it cannot be read", key)
		}
		if sameStampBytes(existing, desired) {
			return existing, false, nil
		}
		s, adoptErr := adoptExistingStamp(key, existing, desired)
		return s, adoptErr == nil, adoptErr
	case errors.Is(err, sidecar.ErrConditionalConflict):
		// The store kept reporting a concurrent conditional write to this
		// key. A stamp has one writer per backup, so another backup is
		// writing here; falling back to Put would overwrite its stamp
		// (review F016). A retried pod reads what landed and decides.
		return backupmeta.Stamp{}, false, fmt.Errorf("%w: create %s: another writer is creating the same stamp concurrently (%w); the dump location is shared with another backup", ErrBackupStampConflict, key, err)
	case errors.Is(err, sidecar.ErrConditionalWriteUnsupported):
		// No atomic create: Put plus the read-back. The location was empty
		// and held no stamp just above, so this cannot replace a stamp
		// that was there; a same-name backup of another namespace racing
		// this one is caught by the identity checks before the upload and
		// at the end step (D-8).
		if err := settleBackupStamp(ctx, store, key, nil, desired, func() error {
			return store.Put(ctx, key, bytes.NewReader(data), int64(len(data)))
		}); err != nil {
			return backupmeta.Stamp{}, false, err
		}
		return desired, false, nil
	default:
		// An ordinary failure may hide a create that landed: our own bytes
		// at the key mean it did.
		if got, found, loadErr := loadBackupStamp(ctx, store, key); loadErr == nil && found && sameStampBytes(got, desired) {
			return desired, false, nil
		}
		return backupmeta.Stamp{}, false, fmt.Errorf("create %s: %w", key, err)
	}
}

// loadOwnStamp reads the stamp at key and requires it to be template's
// (identity equal).
func loadOwnStamp(ctx context.Context, store sidecar.ArchiveStore, key string, template backupmeta.Stamp) (backupmeta.Stamp, error) {
	prev, found, err := loadBackupStamp(ctx, store, key)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	if !found {
		return backupmeta.Stamp{}, fmt.Errorf("%w: %s is missing; the backup-stamp init container writes it before the dump", ErrBackupStampConflict, key)
	}
	a, err := stampIdentity(prev)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	b, err := stampIdentity(template)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	if !bytes.Equal(a, b) {
		return backupmeta.Stamp{}, fmt.Errorf("%w: %s holds another backup's stamp (%s); want %s", ErrBackupStampConflict, key, a, b)
	}
	return prev, nil
}

// finishBackupStamp adds serverClock.end to the stamp at key. The stamp
// must be the one template describes, and every field but the end must be
// unchanged. Rewriting the same end again is a no-op.
func finishBackupStamp(ctx context.Context, store sidecar.ArchiveStore, key string, template backupmeta.Stamp, end backupmeta.MicroTime) (backupmeta.Stamp, error) {
	prev, err := loadOwnStamp(ctx, store, key, template)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	next := prev
	next.ServerClock.End = backupmeta.NewMicroTime(end.Time)
	if err := backupmeta.CheckStampEndRewrite(prev, next); err != nil {
		return backupmeta.Stamp{}, err
	}
	var write func() error
	if !sameStampBytes(prev, next) {
		data, err := next.Canonical()
		if err != nil {
			return backupmeta.Stamp{}, err
		}
		write = func() error { return store.Put(ctx, key, bytes.NewReader(data), int64(len(data))) }
	}
	if err := settleBackupStamp(ctx, store, key, &prev, next, write); err != nil {
		return backupmeta.Stamp{}, err
	}
	return next, nil
}

func sameStampBytes(a, b backupmeta.Stamp) bool {
	ab, errA := a.Canonical()
	bb, errB := b.Canonical()
	return errA == nil && errB == nil && bytes.Equal(ab, bb)
}

// stampSettleAttempts and stampSettleBackoff bound settleBackupStamp's
// retries: the n-th retry waits stampSettleBackoff<<(n-1), so about 7.5 s
// in all. Variables for tests.
var (
	stampSettleAttempts = 5
	stampSettleBackoff  = 500 * time.Millisecond
)

// settleBackupStamp makes key hold want and confirms it by reading it back
// (decrypting when the store encrypts). write, when non-nil, writes want;
// prev is what key held before (nil: nothing).
//
// A failed write or read-back is retried in-process (review F017): a write
// whose response was lost may have landed, and a read-back that fails
// transiently says nothing about what is stored. Exiting instead would fail
// the pod, and every retried pod's init container refuses a stamp that now
// records a finished dump. Each attempt reads what is stored first: want
// means done; prev (or nothing) means the write did not land, so it is
// written again; anything else is another writer's stamp and refuses
// without writing.
func settleBackupStamp(ctx context.Context, store sidecar.ArchiveStore, key string, prev *backupmeta.Stamp, want backupmeta.Stamp, write func() error) error {
	var writeErr error
	if write != nil {
		writeErr = write()
	}
	for attempt := 1; ; attempt++ {
		got, found, err := loadBackupStamp(ctx, store, key)
		var last error
		switch {
		case err != nil && errors.Is(err, ErrBackupStampConflict):
			return fmt.Errorf("read back: %w", err)
		case err != nil:
			last = fmt.Errorf("read back: %w", err)
		case found && sameStampBytes(got, want):
			return nil
		case !found && prev == nil, found && prev != nil && sameStampBytes(got, *prev):
			// The write did not land (or, with no write, the earlier one
			// is not visible yet).
			last = fmt.Errorf("read back %s: the stamp is still the earlier one after the write", key)
			if !found {
				last = fmt.Errorf("read back %s: the stamp is missing after the write", key)
			}
		default:
			gb, _ := got.Canonical()
			wb, _ := want.Canonical()
			if !found {
				gb = []byte("nothing")
			}
			return fmt.Errorf("%w: read back %s: stored %s, wrote %s", ErrBackupStampConflict, key, gb, wb)
		}
		if writeErr != nil {
			last = fmt.Errorf("write %s: %w; %w", key, writeErr, last)
		}
		if attempt >= stampSettleAttempts {
			return last
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w; %w", ctx.Err(), last)
		case <-time.After(stampSettleBackoff << (attempt - 1)):
		}
		// Write again only when the read proved it did not land.
		if write != nil && err == nil {
			writeErr = write()
		} else {
			writeErr = nil
		}
	}
}

// installBackupStampTool copies this executable into dir so a container of
// another image (mysqlsh) can run the end step.
func installBackupStampTool(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	src, err := os.Open(self)
	if err != nil {
		return err
	}
	defer src.Close()
	tmp, err := os.CreateTemp(dir, ".bloodraven-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o755); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, backupStampToolName))
}

// readDumpServerClock reads the dumped server's UTC clock at µs precision and
// its server_id over the Job's MySQL connection settings.
func readDumpServerClock(ctx context.Context) (backupmeta.MicroTime, uint32, error) {
	cfg, err := backupStampMySQLConfig()
	if err != nil {
		return backupmeta.MicroTime{}, 0, err
	}
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		return backupmeta.MicroTime{}, 0, err
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	var clock string
	var serverID uint32
	if err := db.QueryRowContext(ctx, serverClockQuery).Scan(&clock, &serverID); err != nil {
		return backupmeta.MicroTime{}, 0, fmt.Errorf("query %s: %w", cfg.Addr, err)
	}
	t, err := backupmeta.ParseMicroTime(clock)
	if err != nil {
		return backupmeta.MicroTime{}, 0, err
	}
	return t, serverID, nil
}

// serverClockQuery is the server's UTC clock in backupmeta.MicroTimeLayout,
// independent of the session time zone. dump.py runs the same expression
// for serverClock.end.
const serverClockQuery = "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ'), @@global.server_id"

// backupStampMySQLConfig mirrors dump.py's connection: the same host and
// credential files, and with BLOODRAVEN_TLS=1 a session that verifies the
// server certificate against the mounted CA (mysqlsh ssl-mode=VERIFY_CA).
func backupStampMySQLConfig() (*mysqldriver.Config, error) {
	host := os.Getenv("BLOODRAVEN_MYSQL_HOST")
	if host == "" {
		return nil, fmt.Errorf("BLOODRAVEN_MYSQL_HOST is required")
	}
	dir := os.Getenv("BLOODRAVEN_MYSQL_CREDS_DIR")
	if dir == "" {
		return nil, fmt.Errorf("BLOODRAVEN_MYSQL_CREDS_DIR is required")
	}
	// The derived credentials Secret holds the exact values; dump.py does
	// not trim them either.
	user, err := os.ReadFile(filepath.Join(dir, "MYSQL_USER"))
	if err != nil {
		return nil, fmt.Errorf("read MYSQL_USER: %w", err)
	}
	password, err := os.ReadFile(filepath.Join(dir, "MYSQL_PASSWORD"))
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read MYSQL_PASSWORD: %w", err)
	}
	cfg := mysqldriver.NewConfig()
	cfg.User = string(user)
	cfg.Passwd = string(password)
	cfg.Net = "tcp"
	cfg.Addr = host
	cfg.Timeout = 30 * time.Second
	cfg.ReadTimeout = 60 * time.Second
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("BLOODRAVEN_TLS"))); v == "1" || v == "true" {
		caFile := os.Getenv("BLOODRAVEN_TLS_CA_FILE")
		if caFile == "" {
			return nil, fmt.Errorf("BLOODRAVEN_TLS=1 requires BLOODRAVEN_TLS_CA_FILE")
		}
		tlsCfg, err := verifyCATLSConfig(caFile)
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlsCfg
	}
	return cfg, nil
}

// verifyCATLSConfig verifies the server's chain against caFile without a
// host name check, the semantics of mysqlsh's ssl-mode=VERIFY_CA.
func verifyCATLSConfig(caFile string) (*tls.Config, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("BLOODRAVEN_TLS_CA_FILE %q contains no valid certificates", caFile)
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // The chain is verified below; VERIFY_CA checks no host name.
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("verify MySQL server certificate: peer sent no certificates")
			}
			intermediates := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			if _, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}); err != nil {
				return fmt.Errorf("verify MySQL server certificate: %w", err)
			}
			return nil
		},
	}, nil
}
