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
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

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
// encrypted. Its key is unique to the backup and the Job is its only writer,
// so a store without atomic create falls back to Put plus a read-back
// compare.
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
	cfg.PassphraseFile = os.Getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE")
	store, err := newArchiveStore(ctx, cfg)
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
	if serverID != template.ServerID {
		fail("check dumped server", fmt.Errorf("%w: the dump source for site %s answered with server_id %d, the operator pinned %d; the Job reached the wrong server", ErrBackupStampConflict, template.Site, serverID, template.ServerID))
	}
	desired := template
	desired.ServerClock = backupmeta.ServerClock{Start: start}
	desired.CreatedAt = backupmeta.NewMicroTime(time.Now())
	stamp, reused, err := writeBackupStamp(ctx, store, key, desired)
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

// finishBackupStampFromEnv runs the end step from the Job's env, for a
// final step that is not the backup-stamp subcommand itself
// (encrypt-upload).
func finishBackupStampFromEnv(ctx context.Context, end string) (backupmeta.Stamp, error) {
	if end == "" {
		return backupmeta.Stamp{}, fmt.Errorf("the dump did not report serverClock.end")
	}
	at, err := backupmeta.ParseMicroTime(end)
	if err != nil {
		return backupmeta.Stamp{}, fmt.Errorf("serverClock.end: %w", err)
	}
	template, err := backupStampTemplateFromEnv()
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	cfg, key, err := backupStampStoreConfig(os.Getenv("BLOODRAVEN_STORAGE_TYPE"), os.Getenv("BLOODRAVEN_OUTPUT_URL"))
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	cfg.PassphraseFile = os.Getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE")
	store, err := newArchiveStore(ctx, cfg)
	if err != nil {
		return backupmeta.Stamp{}, err
	}
	return finishBackupStamp(ctx, store, key, template, at)
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

// writeBackupStamp creates the stamp at key, or adopts a retry's earlier
// stamp, and confirms the stored bytes by reading them back.
func writeBackupStamp(ctx context.Context, store sidecar.ArchiveStore, key string, desired backupmeta.Stamp) (backupmeta.Stamp, bool, error) {
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
		return s, err == nil, err
	}
	data, err := desired.Canonical()
	if err != nil {
		return backupmeta.Stamp{}, false, err
	}
	switch err := sidecar.PutIfAbsent(ctx, store, key, bytes.NewReader(data), int64(len(data))); {
	case err == nil:
	case errors.Is(err, sidecar.ErrObjectExists):
		// A retried create whose first response was lost, or another
		// writer at this backup's key: what is there decides.
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
	case errors.Is(err, sidecar.ErrConditionalWriteUnsupported), errors.Is(err, sidecar.ErrConditionalConflict):
		// The key is unique to this backup and the Job is its only writer,
		// so a plain Put plus the read-back below cannot replace another
		// writer's stamp (D-8).
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			return backupmeta.Stamp{}, false, fmt.Errorf("write %s: %w", key, err)
		}
	default:
		return backupmeta.Stamp{}, false, fmt.Errorf("create %s: %w", key, err)
	}
	if err := confirmBackupStamp(ctx, store, key, desired); err != nil {
		return backupmeta.Stamp{}, false, err
	}
	return desired, false, nil
}

// finishBackupStamp adds serverClock.end to the stamp at key. The stamp
// must be the one template describes, and every field but the end must be
// unchanged. Rewriting the same end again is a no-op.
func finishBackupStamp(ctx context.Context, store sidecar.ArchiveStore, key string, template backupmeta.Stamp, end backupmeta.MicroTime) (backupmeta.Stamp, error) {
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
	next := prev
	next.ServerClock.End = backupmeta.NewMicroTime(end.Time)
	if err := backupmeta.CheckStampEndRewrite(prev, next); err != nil {
		return backupmeta.Stamp{}, err
	}
	if !sameStampBytes(prev, next) {
		data, err := next.Canonical()
		if err != nil {
			return backupmeta.Stamp{}, err
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			return backupmeta.Stamp{}, fmt.Errorf("write %s: %w", key, err)
		}
	}
	if err := confirmBackupStamp(ctx, store, key, next); err != nil {
		return backupmeta.Stamp{}, err
	}
	return next, nil
}

func sameStampBytes(a, b backupmeta.Stamp) bool {
	ab, errA := a.Canonical()
	bb, errB := b.Canonical()
	return errA == nil && errB == nil && bytes.Equal(ab, bb)
}

// confirmBackupStamp reads key back (decrypting when the store encrypts)
// and requires exactly want.
func confirmBackupStamp(ctx context.Context, store sidecar.ArchiveStore, key string, want backupmeta.Stamp) error {
	got, found, err := loadBackupStamp(ctx, store, key)
	if err != nil {
		return fmt.Errorf("read back: %w", err)
	}
	if !found {
		return fmt.Errorf("read back %s: the stamp is missing after the write", key)
	}
	if !sameStampBytes(got, want) {
		gb, _ := got.Canonical()
		wb, _ := want.Canonical()
		return fmt.Errorf("%w: read back %s: stored %s, wrote %s", ErrBackupStampConflict, key, gb, wb)
	}
	return nil
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
