package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

// runEncryptUpload is the entry point for `bloodraven encrypt-upload`.
// It is the main container of a backup Job whose profile has
// spec.backup.profiles[].encryption set: mysqlsh first dumps to a
// staging emptyDir (see the `mysqlsh-dump` init container), then this
// subcommand walks the staged directory, encrypts every file with
// AES-256-GCM, and uploads the ciphertext to the profile's storage
// backend (S3 or PVC).
//
// The terminal BLOODRAVEN_DUMP_COMPLETE sentinel is identical to what
// the unencrypted dump script emits, so the operator's log-tail
// reconcile path works unchanged. The "location" field intentionally
// points at the encrypted ciphertext prefix — that's where the
// artifact actually lives on disk.
//
// Env contract (inputs):
//
//	BLOODRAVEN_BACKUP_NAME             logical backup name (for logs)
//	BLOODRAVEN_SOURCE_DIR              local staging directory
//	BLOODRAVEN_STORAGE_TYPE            S3 | PVC
//	BLOODRAVEN_OUTPUT_URL              S3 prefix or PVC mount-relative path
//	BLOODRAVEN_ENCRYPTION_ALGORITHM    algorithm identifier (AES-256-GCM)
//	BLOODRAVEN_BACKUP_PASSPHRASE_FILE  path to a file containing the passphrase
//	BLOODRAVEN_S3_BUCKET               S3 bucket name (S3 storage only)
//	BLOODRAVEN_S3_ENDPOINT_OVERRIDE    S3 endpoint override (optional)
//	BLOODRAVEN_AWS_CREDS_DIR           directory with AWS_* files (S3 storage only)
//	BLOODRAVEN_PVC_MOUNT_PATH          PVC mount path (PVC storage only)
//	AWS_REGION                         AWS region (S3 storage only, optional)
//
// Env contract (outputs to stdout):
//
//	BLOODRAVEN_DUMP_COMPLETE location=<url> sizeBytes=<int> size=<human> \
//	                         gtidExecuted=... binlogFile=... binlogPos=...
//
// Exit codes:
//
//	0  on success
//	2  on any hard failure (bad config, upload/encrypt error)
func runEncryptUpload(args []string) {
	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	name := os.Getenv("BLOODRAVEN_BACKUP_NAME")
	srcDir := os.Getenv("BLOODRAVEN_SOURCE_DIR")
	storageType := os.Getenv("BLOODRAVEN_STORAGE_TYPE")
	outputURL := os.Getenv("BLOODRAVEN_OUTPUT_URL")
	passphraseFile := os.Getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE")
	algorithm := os.Getenv("BLOODRAVEN_ENCRYPTION_ALGORITHM")
	if algorithm == "" {
		algorithm = backupcrypto.AlgorithmAESGCMName
	}

	if name == "" || srcDir == "" || storageType == "" || outputURL == "" || passphraseFile == "" {
		baseLogger.Error("missing required env",
			"name", name != "", "src", srcDir != "",
			"storageType", storageType != "", "outputURL", outputURL != "",
			"passphrase", passphraseFile != "")
		os.Exit(2)
	}

	// Bind the backup identity into every structured log line so an
	// on-call engineer correlating Job logs against a specific backup
	// has one field to grep on (see AUDIT H7).
	logger := baseLogger.With(
		"subcommand", "encrypt-upload",
		"backup", name,
		"storage_type", storageType,
		"output", outputURL,
		"algorithm", algorithm,
	)

	passphrase, err := backupcrypto.ReadPassphraseFile(passphraseFile)
	if err != nil {
		logger.Error("read passphrase", "error", err)
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

	cfg, keyPrefix, locationDisplay, err := storageConfigFromEnv(storageType, outputURL)
	if err != nil {
		logger.Error("resolve storage config", "error", err)
		os.Exit(2)
	}

	store, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		logger.Error("init archive store", "error", err)
		os.Exit(2)
	}

	// Parse the dump's completion sentinel (written by backup_script.py
	// into a sidecar file) so we can forward the GTID/binlog coords.
	// The sentinel lives as BLOODRAVEN_DUMP_META.json alongside the
	// mysqlsh dump artifacts — backup_script.py writes it when it sees
	// BLOODRAVEN_STORAGE_TYPE=PVC-with-staging-path, which is what the
	// init container runs with in the encrypted flow.
	meta := readDumpMeta(filepath.Join(srcDir, dumpMetaFileName))

	printlnf("BLOODRAVEN_ENCRYPT_UPLOAD_START src=%s output=%s algorithm=%s",
		srcDir, outputURL, algorithm)

	runStart := time.Now()
	totalPlaintext, totalCiphertext, files, digests, err := encryptAndUpload(ctx, store, keyPrefix, srcDir, passphrase)
	if err != nil {
		logger.Error("encrypt-upload", "error", err)
		// Emit a sentinel the reconciler can parse into
		// bloodraven_backup_encrypt_failures_total (AUDIT M5).
		printlnf("BLOODRAVEN_ENCRYPT_METRICS stage=encrypt result=failure duration=%.3f bytes=%d files=%d",
			time.Since(runStart).Seconds(), totalPlaintext, files)
		os.Exit(2)
	}
	printlnf("BLOODRAVEN_ENCRYPT_METRICS stage=encrypt result=success duration=%.3f bytes=%d ciphertext=%d files=%d",
		time.Since(runStart).Seconds(), totalPlaintext, totalCiphertext, files)

	// Write the encryption manifest next to the objects so a reader
	// can discover that the prefix holds ciphertext even without the
	// MysqlBackup CR. The restore side falls back to this file when
	// the CR has been garbage-collected.
	manifestKey := path.Join(keyPrefix, encryptionManifestFileName)
	manifestBytes, err := json.MarshalIndent(EncryptionManifest{
		Version:   1,
		Algorithm: algorithm,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		FileCount: files,
		Files:     digests,
	}, "", "  ")
	if err != nil {
		logger.Error("marshal encryption manifest", "error", err)
		os.Exit(2)
	}
	if err := store.Put(ctx, manifestKey, newBytesReader(manifestBytes), int64(len(manifestBytes))); err != nil {
		logger.Error("upload encryption manifest", "error", err)
		os.Exit(2)
	}

	human := humanBytes(totalPlaintext)
	tokens := []string{
		"location=" + escapeToken(locationDisplay),
		fmt.Sprintf("sizeBytes=%d", totalPlaintext),
		"size=" + escapeToken(human),
		"gtidExecuted=" + escapeToken(meta.GTIDExecuted),
		"binlogFile=" + escapeToken(meta.BinlogFile),
		fmt.Sprintf("binlogPos=%d", meta.BinlogPos),
		fmt.Sprintf("ciphertextBytes=%d", totalCiphertext),
		fmt.Sprintf("files=%d", files),
		"encrypted=true",
		"algorithm=" + escapeToken(algorithm),
	}
	printlnf("BLOODRAVEN_DUMP_COMPLETE %s", strings.Join(tokens, " "))
}

// storageConfigFromEnv translates the BLOODRAVEN_STORAGE_* env vars into
// a sidecar.PITRConfig suitable for sidecar.NewArchiveStore, and
// returns the storage-relative key prefix to write under and the
// user-facing location string recorded on MysqlBackup.status.location.
//
// Re-using PITRConfig for dumps is fine — the struct is just the
// storage plumbing (bucket/endpoint/creds or mount path), not
// anything PITR-specific. Keeping one store interface for both paths
// is the whole reason ArchiveStore exists in `internal/sidecar`.
func storageConfigFromEnv(storageType, outputURL string) (*sidecar.PITRConfig, string, string, error) {
	cfg := &sidecar.PITRConfig{StorageType: storageType}
	switch storageType {
	case "S3":
		bucket := os.Getenv("BLOODRAVEN_S3_BUCKET")
		if bucket == "" {
			return nil, "", "", fmt.Errorf("BLOODRAVEN_S3_BUCKET is required for S3 storage")
		}
		cfg.S3 = &sidecar.PITRS3Config{
			Bucket:      bucket,
			Region:      os.Getenv("AWS_REGION"),
			EndpointURL: os.Getenv("BLOODRAVEN_S3_ENDPOINT_OVERRIDE"),
			AWSCredsDir: os.Getenv("BLOODRAVEN_AWS_CREDS_DIR"),
		}
		// outputURL is already the relative key prefix (e.g.
		// "orders/<backup>/"). Strip trailing slashes when
		// resolving the per-file key.
		prefix := strings.TrimSuffix(outputURL, "/")
		display := fmt.Sprintf("s3://%s/%s/", bucket, prefix)
		return cfg, prefix, display, nil
	case "PVC":
		mount := os.Getenv("BLOODRAVEN_PVC_MOUNT_PATH")
		if mount == "" {
			return nil, "", "", fmt.Errorf("BLOODRAVEN_PVC_MOUNT_PATH is required for PVC storage")
		}
		cfg.PVC = &sidecar.PITRPVCConfig{MountPath: mount}
		// outputURL is the absolute per-pod path of the target
		// directory — but the archive store API takes storage-
		// relative keys, so peel off the mount prefix.
		rel, err := filepath.Rel(mount, outputURL)
		if err != nil || strings.HasPrefix(rel, "..") {
			return nil, "", "", fmt.Errorf("outputURL %q is not under PVC mount %q", outputURL, mount)
		}
		prefix := strings.TrimSuffix(filepath.ToSlash(rel), "/")
		return cfg, prefix, outputURL, nil
	default:
		return nil, "", "", fmt.Errorf("unknown storage type %q", storageType)
	}
}

// encryptAndUpload walks srcDir recursively, encrypts every file with
// AES-256-GCM, and uploads the ciphertext to store at keyPrefix/<rel>.
// Returns (plaintext bytes, ciphertext bytes, file count, per-file
// plaintext sha256 map keyed by relative path). Symlinks are
// intentionally not followed — the staging emptyDir under mysqlsh's
// control shouldn't have any, and silently following them would widen
// the trust boundary.
func encryptAndUpload(ctx context.Context, store sidecar.ArchiveStore, keyPrefix, srcDir string, passphrase []byte) (int64, int64, int, map[string]string, error) {
	// Pre-collect so we can report a stable count and upload files in a
	// deterministic (lexical) order, useful for debugging.
	var relFiles []string
	err := filepath.Walk(srcDir, func(p string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		// Skip the dump metadata sidecar; encrypt-upload carries those
		// fields forward via the DUMP_COMPLETE sentinel instead.
		if info.Mode().IsRegular() && info.Name() == dumpMetaFileName {
			return nil
		}
		rel, relErr := filepath.Rel(srcDir, p)
		if relErr != nil {
			return relErr
		}
		relFiles = append(relFiles, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return 0, 0, 0, nil, fmt.Errorf("walk %s: %w", srcDir, err)
	}
	sort.Strings(relFiles)

	var totalPT, totalCT int64
	digests := make(map[string]string, len(relFiles))
	for _, rel := range relFiles {
		if err := ctx.Err(); err != nil {
			return totalPT, totalCT, 0, nil, err
		}
		srcPath := filepath.Join(srcDir, filepath.FromSlash(rel))
		fileStart := time.Now()
		var ct, pt int64
		var sha string
		// Per-file bounded retry with exponential backoff: transient
		// PVC disk-pressure or S3 hiccups shouldn't force a full
		// re-dump when the Job's backoffLimit kicks in. 3 attempts
		// total, starting at 500ms (AUDIT L6).
		retryErr := util.RetryWithBackoff(ctx, nil, 2, 500*time.Millisecond, func() error {
			var innerErr error
			ct, pt, sha, innerErr = encryptOneAndUpload(ctx, store, path.Join(keyPrefix, rel), srcPath, passphrase)
			return innerErr
		})
		if retryErr != nil {
			return totalPT, totalCT, 0, nil, fmt.Errorf("upload %s: %w", rel, retryErr)
		}
		totalPT += pt
		totalCT += ct
		digests[rel] = sha
		logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
		logger.Info("encrypted file uploaded",
			"rel", rel,
			"plaintext_bytes", pt,
			"ciphertext_bytes", ct,
			"sha256", sha,
			"duration_ms", time.Since(fileStart).Milliseconds(),
		)
	}
	return totalPT, totalCT, len(relFiles), digests, nil
}

// encryptOneAndUpload encrypts one file into a temp file under
// $TMPDIR, then Put-uploads the temp file to the archive store.
// Returns (ciphertext bytes, plaintext bytes, hex sha256 of plaintext).
// The temp file is always removed on exit. We do NOT encrypt into
// memory because dump chunks can be hundreds of megabytes and the Job
// pod typically runs with modest memory limits.
func encryptOneAndUpload(ctx context.Context, store sidecar.ArchiveStore, key, srcPath string, passphrase []byte) (int64, int64, string, error) {
	tmpFile, err := os.CreateTemp("", "bloodraven-enc-*")
	if err != nil {
		return 0, 0, "", fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	in, err := os.Open(srcPath)
	if err != nil {
		_ = tmpFile.Close()
		return 0, 0, "", fmt.Errorf("open %s: %w", srcPath, err)
	}
	defer in.Close()

	// Tee the plaintext through a sha256 hasher while encrypting, so
	// the manifest can carry a digest the restore side verifies
	// (AUDIT L1 — defence-in-depth for B1).
	hasher := sha256.New()
	tee := io.TeeReader(in, hasher)
	pt, err := backupcrypto.Encrypt(tmpFile, tee, passphrase)
	if err != nil {
		_ = tmpFile.Close()
		return 0, 0, "", fmt.Errorf("encrypt: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return 0, 0, "", fmt.Errorf("sync: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return 0, 0, "", fmt.Errorf("close: %w", err)
	}

	info, err := os.Stat(tmpPath)
	if err != nil {
		return 0, 0, "", fmt.Errorf("stat: %w", err)
	}
	ct := info.Size()
	sha := hex.EncodeToString(hasher.Sum(nil))

	if err := store.PutFile(ctx, key, tmpPath); err != nil {
		return ct, pt, sha, fmt.Errorf("put: %w", err)
	}
	return ct, pt, sha, nil
}

// runDecryptDownload is the entry point for `bloodraven decrypt-download`.
// It is used as an init container by encrypted-source restore Jobs:
// it downloads every object under the source prefix from storage,
// decrypts with the supplied passphrase, and writes the plaintext into
// a shared emptyDir (/staging) for the mysqlsh loadDump container to
// read. The restore main container then runs util.loadDump() against
// the local /staging path instead of talking to S3.
//
// Env contract (inputs):
//
//	BLOODRAVEN_TARGET_DIR              local directory to drop plaintext into
//	BLOODRAVEN_STORAGE_TYPE            S3 | PVC
//	BLOODRAVEN_SOURCE_PREFIX           S3 prefix or PVC-relative path to source objects
//	BLOODRAVEN_BACKUP_PASSPHRASE_FILE  path to a file containing the passphrase
//	BLOODRAVEN_S3_BUCKET               S3 bucket (S3 only)
//	BLOODRAVEN_S3_ENDPOINT_OVERRIDE    S3 endpoint override (optional)
//	BLOODRAVEN_AWS_CREDS_DIR           AWS_* directory (S3 only)
//	BLOODRAVEN_PVC_MOUNT_PATH          PVC mount path (PVC only)
//	AWS_REGION                         AWS region (S3 only, optional)
func runDecryptDownload(args []string) {
	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	targetDir := os.Getenv("BLOODRAVEN_TARGET_DIR")
	storageType := os.Getenv("BLOODRAVEN_STORAGE_TYPE")
	sourcePrefix := os.Getenv("BLOODRAVEN_SOURCE_PREFIX")
	passphraseFile := os.Getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE")
	// Default strict: an object whose first bytes don't match the BRV1
	// magic is rejected as tampering, not passed through as plaintext.
	// Set BLOODRAVEN_ALLOW_PLAINTEXT_FALLBACK=1 to opt back into the
	// legacy mixed-encryption behavior (see AUDIT B1).
	allowPlaintext := false
	if v := os.Getenv("BLOODRAVEN_ALLOW_PLAINTEXT_FALLBACK"); v == "1" || v == "true" {
		allowPlaintext = true
	}

	if targetDir == "" || storageType == "" || sourcePrefix == "" || passphraseFile == "" {
		baseLogger.Error("missing required env",
			"target", targetDir != "", "storageType", storageType != "",
			"prefix", sourcePrefix != "", "passphrase", passphraseFile != "")
		os.Exit(2)
	}

	logger := baseLogger.With(
		"subcommand", "decrypt-download",
		"storage_type", storageType,
		"prefix", sourcePrefix,
		"target", targetDir,
		"allow_plaintext_fallback", allowPlaintext,
	)

	passphrase, err := backupcrypto.ReadPassphraseFile(passphraseFile)
	if err != nil {
		logger.Error("read passphrase", "error", err)
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

	cfg, keyPrefix, _, err := storageConfigFromEnv(storageType, sourcePrefix)
	if err != nil {
		logger.Error("resolve storage config", "error", err)
		os.Exit(2)
	}
	store, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		logger.Error("init archive store", "error", err)
		os.Exit(2)
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		logger.Error("mkdir target", "target", targetDir, "error", err)
		os.Exit(2)
	}

	printlnf("BLOODRAVEN_DECRYPT_DOWNLOAD_START prefix=%s target=%s", sourcePrefix, targetDir)

	runStart := time.Now()
	// Load the encryption manifest up front so we can verify each
	// plaintext's sha256 after decryption (AUDIT L1). Manifest is
	// optional — older artifacts without it skip verification.
	manifest := loadEncryptionManifest(ctx, store, keyPrefix, logger)
	keys, err := store.List(ctx, keyPrefix+"/")
	if err != nil {
		// Some PVC backends stringify the prefix without trailing slash
		// when the entry is a directory. Retry bare.
		keys, err = store.List(ctx, keyPrefix)
		if err != nil {
			logger.Error("list source", "error", err)
			os.Exit(2)
		}
	}
	// Resolve the absolute staging directory once so we can verify
	// every descendant path stays under it (defence against crafted
	// storage keys with "../" segments; see AUDIT H1).
	absTarget, err := filepath.Abs(targetDir)
	if err != nil {
		logger.Error("abs target", "error", err)
		os.Exit(2)
	}

	downloaded := 0
	totalBytes := int64(0)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			logger.Error("context cancelled", "error", err)
			os.Exit(2)
		}
		rel := strings.TrimPrefix(key, keyPrefix)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" || strings.HasSuffix(key, "/") {
			// Directory placeholder on some S3-compat stores.
			continue
		}
		if rel == encryptionManifestFileName {
			// Skip the encryption marker; it's metadata about the
			// ciphertext set, not one of the dump files themselves.
			continue
		}
		dst, err := safeJoinUnderRoot(absTarget, rel)
		if err != nil {
			logger.Error("reject unsafe storage key", "key", key, "error", err)
			os.Exit(2)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			logger.Error("mkdir", "dir", filepath.Dir(dst), "error", err)
			os.Exit(2)
		}
		n, err := downloadAndDecrypt(ctx, store, key, dst, passphrase, allowPlaintext)
		if err != nil {
			logger.Error("decrypt", "key", key, "error", err)
			os.Exit(2)
		}
		// Verify plaintext sha256 against manifest (AUDIT L1). Missing
		// entries are skipped silently so artifacts written by older
		// encrypt-upload releases still restore.
		if manifest != nil {
			if expected, ok := manifest.Files[rel]; ok && expected != "" {
				actual, err := sha256File(dst)
				if err != nil {
					logger.Error("sha256", "key", key, "error", err)
					os.Exit(2)
				}
				if actual != expected {
					logger.Error("manifest sha256 mismatch", "key", key, "expected", expected, "actual", actual)
					_ = os.Remove(dst)
					os.Exit(2)
				}
			}
		}
		downloaded++
		totalBytes += n
		logger.Info("decrypted", "key", key, "bytes", n)
	}

	printlnf("BLOODRAVEN_DECRYPT_DOWNLOAD_COMPLETE files=%d bytes=%d", downloaded, totalBytes)
	printlnf("BLOODRAVEN_ENCRYPT_METRICS stage=decrypt result=success duration=%.3f bytes=%d files=%d",
		time.Since(runStart).Seconds(), totalBytes, downloaded)
}

// downloadAndDecrypt streams an encrypted object out of the archive
// store, decrypts it, and writes the plaintext to dst. Returns the
// plaintext byte count. When allowPlaintext is false and the object
// lacks the BRV1 magic header, the call fails with a tamper/downgrade
// error rather than silently passing bytes through.
func downloadAndDecrypt(ctx context.Context, store sidecar.ArchiveStore, key, dst string, passphrase []byte, allowPlaintext bool) (int64, error) {
	// Use a temp path in the same directory so the final rename is
	// atomic and the mysqlsh container never sees a half-written file.
	tmp := dst + ".part"
	if err := store.GetFile(ctx, key, tmp); err != nil {
		return 0, fmt.Errorf("get: %w", err)
	}
	defer os.Remove(tmp)

	in, err := os.Open(tmp)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, fmt.Errorf("create: %w", err)
	}
	n, decErr := backupcrypto.Decrypt(out, in, passphrase)
	if decErr != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		if errors.Is(decErr, backupcrypto.ErrMagicMismatch) {
			if !allowPlaintext {
				return 0, fmt.Errorf("ciphertext tamper/downgrade detected: object %q missing BRV1 magic (set BLOODRAVEN_ALLOW_PLAINTEXT_FALLBACK=1 to opt into legacy mixed-encryption)", key)
			}
			// Legacy upgrade window: object was never encrypted. Copy
			// plaintext verbatim so the caller gets the bytes
			// unchanged. Only reached when the operator explicitly
			// opted in.
			if _, err := in.Seek(0, io.SeekStart); err != nil {
				return 0, fmt.Errorf("rewind for plaintext passthrough: %w", err)
			}
			out, err = os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if err != nil {
				return 0, fmt.Errorf("create plaintext dst: %w", err)
			}
			n, copyErr := io.Copy(out, in)
			_ = out.Close()
			if copyErr != nil {
				_ = os.Remove(dst)
				return 0, fmt.Errorf("copy plaintext: %w", copyErr)
			}
			return n, nil
		}
		return 0, fmt.Errorf("decrypt: %w", decErr)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return 0, fmt.Errorf("sync: %w", err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(dst)
		return 0, fmt.Errorf("close: %w", err)
	}
	return n, nil
}

// safeJoinUnderRoot validates that rel, when joined under the absolute
// directory root, stays lexically inside root. Rejects absolute paths,
// parent-directory escapes, and NUL / control bytes that can confuse
// downstream tools. Used by the decrypt-download and pitr-download
// paths to defend against crafted storage keys (S3 object names may
// legally contain "../" segments; see AUDIT H1).
func safeJoinUnderRoot(root, rel string) (string, error) {
	if strings.ContainsRune(rel, 0x00) {
		return "", fmt.Errorf("path contains NUL byte: %q", rel)
	}
	for _, r := range rel {
		if r < 0x20 && r != '\t' {
			return "", fmt.Errorf("path contains control byte %U: %q", r, rel)
		}
	}
	// Normalize the storage-supplied path to OS separators and clean
	// it. Clean collapses "a/../b" into "b" so a trailing Rel check
	// catches any attempt to escape root even after normalization.
	cleaned := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("absolute path not allowed: %q", rel)
	}
	dst := filepath.Join(root, cleaned)
	resolved, err := filepath.Abs(dst)
	if err != nil {
		return "", fmt.Errorf("abs %q: %w", dst, err)
	}
	relBack, err := filepath.Rel(root, resolved)
	if err != nil {
		return "", fmt.Errorf("rel %q under %q: %w", resolved, root, err)
	}
	if relBack == ".." || strings.HasPrefix(relBack, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root %q: %q", root, rel)
	}
	return resolved, nil
}

// -----------------------------------------------------------------------
// Encryption manifest & dump-meta sidecar file
// -----------------------------------------------------------------------

// encryptionManifestFileName is the JSON marker file dropped alongside
// every encrypted object set. It lets the restore side confirm the
// encryption was intentional (not a wire protocol bug) and records the
// algorithm tag so future releases can reject incompatible artifacts
// ahead of reading the first object header.
const encryptionManifestFileName = "BLOODRAVEN_ENCRYPTION.json"

// EncryptionManifest is the on-disk JSON written to
// <prefix>/BLOODRAVEN_ENCRYPTION.json by runEncryptUpload. Its shape
// mirrors the relevant fields on MysqlBackup.status so the restore
// reconciler can re-derive them when the MysqlBackup CR has been
// garbage-collected.
type EncryptionManifest struct {
	Version   int    `json:"version"`
	Algorithm string `json:"algorithm"`
	CreatedAt string `json:"createdAt"`
	FileCount int    `json:"fileCount"`

	// Files carries a per-file plaintext sha256 digest map keyed by
	// the relative path inside the backup prefix. The restore side
	// verifies each plaintext after decryption against this map so
	// that even a ciphertext replacement that preserved valid BRV1
	// framing is caught (AUDIT L1 — defence-in-depth for B1).
	Files map[string]string `json:"files,omitempty"`
}

// dumpMetaFileName is written by backup_script.py in the encrypted
// flow. It carries the GTID/binlog coordinates captured during the
// mysqlsh dump so the uploader can forward them on the
// BLOODRAVEN_DUMP_COMPLETE sentinel (whose parser has lived in the
// reconciler forever).
const dumpMetaFileName = "BLOODRAVEN_DUMP_META.json"

// dumpMeta is the JSON shape backup_script.py writes into the
// staging directory when BLOODRAVEN_DUMP_META_FILE is set.
type dumpMeta struct {
	GTIDExecuted string `json:"gtidExecuted,omitempty"`
	BinlogFile   string `json:"binlogFile,omitempty"`
	BinlogPos    int64  `json:"binlogPos,omitempty"`
}

// loadEncryptionManifest best-effort fetches the
// BLOODRAVEN_ENCRYPTION.json manifest from the source prefix. Returns
// nil if the object doesn't exist or can't be parsed — L1 verification
// is strictly additive and an old artifact without the Files map is
// still a valid restore source (AUDIT L1).
func loadEncryptionManifest(ctx context.Context, store sidecar.ArchiveStore, keyPrefix string, logger *slog.Logger) *EncryptionManifest {
	key := path.Join(keyPrefix, encryptionManifestFileName)
	data, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		if err != nil {
			logger.Warn("could not fetch encryption manifest", "key", key, "error", err)
		}
		return nil
	}
	var m EncryptionManifest
	if err := json.Unmarshal(data, &m); err != nil {
		logger.Warn("could not parse encryption manifest", "key", key, "error", err)
		return nil
	}
	return &m
}

// sha256File hex-encodes the sha256 of the given file. Used by
// decrypt-download to verify plaintext against the manifest digest.
func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func readDumpMeta(path string) dumpMeta {
	var m dumpMeta
	data, err := os.ReadFile(path)
	if err != nil {
		return m
	}
	_ = json.Unmarshal(data, &m)
	return m
}

// -----------------------------------------------------------------------
// Small helpers
// -----------------------------------------------------------------------

// escapeToken mirrors backup_script.py's _escape_token: whitespace is
// replaced with underscores so the whitespace-splitting Go parser can
// round-trip multi-word values (e.g. "1.4 GiB").
func escapeToken(s string) string {
	if s == "" {
		return ""
	}
	return strings.ReplaceAll(s, " ", "_")
}

// humanBytes proxies to util.HumanBytes so the reconciler's .status.size
// and the encrypt-upload sentinel agree (AUDIT L8).
func humanBytes(n int64) string { return util.HumanBytes(n) }

// printlnf is a tiny stdout-flushing wrapper for the sentinel-style
// lines. We don't use slog for these: the operator's log-tail parser
// looks for literal BLOODRAVEN_* prefixes.
func printlnf(format string, args ...any) {
	fmt.Println(fmt.Sprintf(format, args...))
	_ = os.Stdout.Sync()
}

// newBytesReader wraps bytes in an io.Reader that returns a known
// length. Avoids a dependency on bytes.NewReader's specific type from
// the call site.
func newBytesReader(b []byte) io.Reader {
	return &byteSliceReader{b: b}
}

type byteSliceReader struct {
	b []byte
	i int
}

func (r *byteSliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
