package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

// `bloodraven dump-meta` is the first init container of every restore and
// verification Job with pointInTime (design §4.2). It reads the dump's
// @.json, @.done.json and its backup stamp, checks them, and writes
// backupmeta.DumpMeta to <BLOODRAVEN_DUMP_META_OUTPUT_DIR>/dump.json for
// pitr-download. It uses decrypt-download's storage wiring; when the dump
// is encrypted every object is BRV1 and there is no plaintext fallback.
//
// Env (see contracts.md "WP-B2 assumptions"):
//
//	BLOODRAVEN_DUMP_META_OUTPUT_DIR        where dump.json goes (/pitr-dump-meta)
//	BLOODRAVEN_STORAGE_TYPE                S3 or PVC
//	BLOODRAVEN_SOURCE_PREFIX               the dump location relative to the store
//	BLOODRAVEN_S3_BUCKET, BLOODRAVEN_S3_ENDPOINT_OVERRIDE, AWS_REGION, BLOODRAVEN_AWS_CREDS_DIR   (S3)
//	BLOODRAVEN_PVC_MOUNT_PATH              (PVC)
//	BLOODRAVEN_BACKUP_PASSPHRASE_FILE      set iff the dump is encrypted
//	BLOODRAVEN_DUMP_EXPECTED_BACKUP_UID    mysqlBackupRef: the MysqlBackup's UID
//	BLOODRAVEN_DUMP_EXPECTED_SOURCE_UID    mysqlBackupRef: status.failoverGroupUID, when set
//	BLOODRAVEN_DUMP_EXPECTED_GTID_EXECUTED mysqlBackupRef: status.gtidExecuted (⊆ @.json)
const dumpMetaJSONName = "dump.json"

func runDumpMeta(args []string) {
	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo).With("subcommand", "dump-meta")
	fail := func(msg string, err error) {
		_ = os.WriteFile("/dev/termination-log", []byte(compactTerminationMessage(err.Error())), 0644)
		logger.Error(msg, "error", err)
		os.Exit(2)
	}
	outDir := os.Getenv("BLOODRAVEN_DUMP_META_OUTPUT_DIR")
	storageType := os.Getenv("BLOODRAVEN_STORAGE_TYPE")
	sourcePrefix := os.Getenv("BLOODRAVEN_SOURCE_PREFIX")
	if outDir == "" || storageType == "" || sourcePrefix == "" {
		fail("missing required env", fmt.Errorf("BLOODRAVEN_DUMP_META_OUTPUT_DIR, BLOODRAVEN_STORAGE_TYPE and BLOODRAVEN_SOURCE_PREFIX are required"))
	}
	expect, err := dumpExpectationFromEnv(os.Getenv)
	if err != nil {
		fail("invalid dump-meta config", err)
	}
	cfg, keyPrefix, err := dumpMetaStoreConfig(storageType, sourcePrefix)
	if err != nil {
		fail("resolve dump storage", err)
	}
	cfg.PassphraseFile = os.Getenv("BLOODRAVEN_BACKUP_PASSPHRASE_FILE")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()
	store, err := newArchiveStore(ctx, cfg)
	if err != nil {
		fail("init dump store", err)
	}
	meta, err := loadDumpMeta(ctx, store, keyPrefix, sourcePrefix, expect)
	if err != nil {
		// A refusal (token first) or a store read error. Not "dump
		// metadata", which is the success record.
		fail("read dump metadata", err)
	}
	if err := writeDumpMeta(outDir, meta); err != nil {
		fail("write dump metadata", err)
	}
	logger.Info("dump metadata", "location", meta.Location, "backupUID", string(meta.Stamp.Backup.UID), "sourceUID", string(meta.Stamp.Source.UID),
		"pitrArchiveID", meta.Stamp.PITRArchiveID, "site", meta.Stamp.Site, "dumpEnd", meta.End.String())
}

// dumpExpectation is what the restore's mysqlBackupRef pins.
type dumpExpectation struct {
	backupUID, sourceUID types.UID
	gtidExecuted         string
}

func dumpExpectationFromEnv(getenv func(string) string) (dumpExpectation, error) {
	e := dumpExpectation{
		backupUID:    types.UID(getenv("BLOODRAVEN_DUMP_EXPECTED_BACKUP_UID")),
		sourceUID:    types.UID(getenv("BLOODRAVEN_DUMP_EXPECTED_SOURCE_UID")),
		gtidExecuted: getenv("BLOODRAVEN_DUMP_EXPECTED_GTID_EXECUTED"),
	}
	if _, err := pitrplan.ParseSet(e.gtidExecuted); err != nil {
		return e, fmt.Errorf("BLOODRAVEN_DUMP_EXPECTED_GTID_EXECUTED: %v", err)
	}
	return e, nil
}

// dumpMetaStoreConfig is decrypt-download's read-only wiring. S3 needs a
// region to sign; mysqlsh assumes us-east-1 when none is configured.
func dumpMetaStoreConfig(storageType, sourcePrefix string) (*sidecar.PITRConfig, string, error) {
	cfg, keyPrefix, _, err := decryptDownloadStoreConfig(storageType, sourcePrefix)
	if err != nil {
		return nil, "", err
	}
	keyPrefix = strings.Trim(keyPrefix, "/")
	if keyPrefix == "" || keyPrefix == "." {
		return nil, "", fmt.Errorf("dump location %q has no key", sourcePrefix)
	}
	if cfg.S3 != nil {
		cfg.S3.DefaultRegion = "us-east-1"
	}
	return cfg, keyPrefix, nil
}

// mysqlshDumpJSON is the part of mysqlsh's @.json dump-meta reads.
type mysqlshDumpJSON struct {
	GTIDExecuted             *string `json:"gtidExecuted"`
	GTIDExecutedInconsistent bool    `json:"gtidExecutedInconsistent"`
}

// mysqlshDoneJSON is the part of @.done.json dump-meta reads.
type mysqlshDoneJSON struct {
	End string `json:"end"`
}

// loadDumpMeta reads and checks the dump's metadata (§4.2), refusing in
// the design's order: IdentityUnknown, IdentityMismatch, Inconsistent,
// Filtered, Incomplete.
func loadDumpMeta(ctx context.Context, store sidecar.ArchiveStore, keyPrefix, location string, expect dumpExpectation) (backupmeta.DumpMeta, error) {
	var meta backupmeta.DumpMeta
	stampKey := backupmeta.StampKey(keyPrefix)
	raw, ok, err := store.Get(ctx, stampKey)
	if err != nil {
		return meta, fmt.Errorf("read backup stamp %s: %w", stampKey, err)
	}
	if !ok {
		return meta, fmt.Errorf("%s: the dump at %s has no backup stamp (%s); it was taken by a release without dump identity, or by hand; take a new backup", pitrarchive.TokenBackupIdentityUnknown, location, stampKey)
	}
	stamp, err := backupmeta.ParseStamp(raw)
	if err != nil {
		return meta, fmt.Errorf("%s: the dump at %s has an unreadable backup stamp: %v; take a new backup", pitrarchive.TokenBackupIdentityUnknown, location, err)
	}
	if expect.backupUID != "" && stamp.Backup.UID != expect.backupUID {
		return meta, fmt.Errorf("%s: the dump at %s was written by MysqlBackup %s/%s (uid %s), not the referenced MysqlBackup (uid %s)", pitrarchive.TokenBackupIdentityMismatch,
			location, stamp.Backup.Namespace, stamp.Backup.Name, stamp.Backup.UID, expect.backupUID)
	}
	if expect.sourceUID != "" && stamp.Source.UID != expect.sourceUID {
		return meta, fmt.Errorf("%s: the dump at %s was taken from MysqlFailoverGroup %s/%s (uid %s), not the backup's recorded group (uid %s)", pitrarchive.TokenBackupIdentityMismatch,
			location, stamp.Source.Namespace, stamp.Source.Name, stamp.Source.UID, expect.sourceUID)
	}
	dumpKey := path.Join(keyPrefix, "@.json")
	raw, ok, err = store.Get(ctx, dumpKey)
	if err != nil {
		return meta, fmt.Errorf("read %s: %w", dumpKey, err)
	}
	if !ok {
		return meta, fmt.Errorf("%s: the dump at %s has no @.json", pitrarchive.TokenBackupIncomplete, location)
	}
	var dj mysqlshDumpJSON
	if err := json.Unmarshal(raw, &dj); err != nil || dj.GTIDExecuted == nil {
		return meta, fmt.Errorf("%s: the dump's @.json at %s has no gtidExecuted (%v)", pitrarchive.TokenBackupInconsistent, location, err)
	}
	b, err := pitrplan.ParseSet(*dj.GTIDExecuted)
	if err != nil {
		return meta, fmt.Errorf("%s: the dump's @.json gtidExecuted %q: %v", pitrarchive.TokenBackupInconsistent, *dj.GTIDExecuted, err)
	}
	if want := pitrplan.MustParseSet(expect.gtidExecuted); !b.Contains(want) {
		return meta, fmt.Errorf("%s: the referenced MysqlBackup recorded gtidExecuted %s, which the dump at %s (@.json %s) does not contain; the location holds another dump",
			pitrarchive.TokenBackupIdentityMismatch, want, location, b)
	}
	if dj.GTIDExecutedInconsistent {
		return meta, fmt.Errorf("%s: the dump at %s is marked gtidExecutedInconsistent; PITR needs a consistent dump", pitrarchive.TokenBackupInconsistent, location)
	}
	if !stamp.DumpFilters.Empty() {
		return meta, fmt.Errorf("%s: the dump at %s excludes schemas (includeSchemas %v, excludeSchemas %v); PITR needs a full dump", pitrarchive.TokenBackupFiltered,
			location, stamp.DumpFilters.IncludeSchemas, stamp.DumpFilters.ExcludeSchemas)
	}
	doneKey := path.Join(keyPrefix, "@.done.json")
	raw, ok, err = store.Get(ctx, doneKey)
	if err != nil {
		return meta, fmt.Errorf("read %s: %w", doneKey, err)
	}
	if !ok {
		return meta, fmt.Errorf("%s: the dump at %s has no @.done.json; the dump did not finish", pitrarchive.TokenBackupIncomplete, location)
	}
	var done mysqlshDoneJSON
	if err := json.Unmarshal(raw, &done); err != nil {
		return meta, fmt.Errorf("%s: the dump's @.done.json at %s: %v", pitrarchive.TokenBackupIncomplete, location, err)
	}
	end, err := parseDumpEnd(done.End)
	if err != nil {
		return meta, fmt.Errorf("%s: the dump's @.done.json at %s: %v", pitrarchive.TokenBackupIncomplete, location, err)
	}
	if !stamp.Complete() {
		return meta, fmt.Errorf("%s: the backup stamp of the dump at %s has no serverClock.end; the dump did not finish", pitrarchive.TokenBackupIncomplete, location)
	}
	return backupmeta.DumpMeta{Version: backupmeta.DumpMetaVersion, Location: location, GTIDExecuted: b.String(), End: end,
		ServerClock: stamp.ServerClock, Stamp: stamp}, nil
}

// parseDumpEnd parses @.done.json's end. The dump container runs with
// TZ=UTC (D-8), so a zoneless value is UTC; mysqlsh writes
// "YYYY-MM-DD HH:MM:SS" with an optional fraction.
func parseDumpEnd(s string) (backupmeta.MicroTime, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{"2006-01-02 15:04:05.999999", time.RFC3339Nano, "2006-01-02T15:04:05.999999"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			if t.Nanosecond()%int(time.Microsecond) != 0 {
				// Round up: the dump ended no earlier than this.
				t = t.Truncate(time.Microsecond).Add(time.Microsecond)
			}
			return backupmeta.NewMicroTime(t), nil
		}
	}
	return backupmeta.MicroTime{}, fmt.Errorf("end %q is not a timestamp", s)
}

// writeDumpMeta writes dump.json atomically.
func writeDumpMeta(dir string, meta backupmeta.DumpMeta) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, dumpMetaJSONName+".part")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, dumpMetaJSONName))
}
