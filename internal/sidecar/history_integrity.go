package sidecar

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// historyIntegrityProber checks the settings under which this server's
// binlogs can certify history (D-34). It returns the first violation, or
// "" when the server is clean.
type historyIntegrityProber interface {
	HistoryIntegrityViolation(ctx context.Context) (pitrarchive.HistoryViolation, string, error)
}

var _ historyIntegrityProber = (*LiveMysql)(nil)

// HistoryIntegrityViolation runs the D-34 checks in a fixed order:
// replication applier filters (per channel and global), the binlog's
// Binlog_Do_DB/Binlog_Ignore_DB, binlog_format=ROW, binlog_row_image=FULL
// and replica_preserve_commit_order=ON.
func (m *LiveMysql) HistoryIntegrityViolation(ctx context.Context) (pitrarchive.HistoryViolation, string, error) {
	for _, table := range []string{"replication_applier_filters", "replication_applier_global_filters"} {
		var n int64
		if err := m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema."+table).Scan(&n); err != nil {
			return "", "", fmt.Errorf("query %s: %w", table, err)
		}
		if n > 0 {
			return pitrarchive.ViolationReplicationFilter, fmt.Sprintf("performance_schema.%s has %d rows", table, n), nil
		}
	}
	doDB, ignoreDB, err := m.binlogFilters(ctx)
	if err != nil {
		return "", "", err
	}
	if doDB != "" || ignoreDB != "" {
		return pitrarchive.ViolationBinlogFilter, fmt.Sprintf("Binlog_Do_DB=%q Binlog_Ignore_DB=%q", doDB, ignoreDB), nil
	}
	var format, rowImage, commitOrder sql.NullString
	if err := m.db.QueryRowContext(ctx,
		"SELECT @@global.binlog_format, @@global.binlog_row_image, @@global.replica_preserve_commit_order").
		Scan(&format, &rowImage, &commitOrder); err != nil {
		return "", "", fmt.Errorf("query binlog settings: %w", err)
	}
	return historySettingsViolation(format, rowImage, commitOrder)
}

// historySettingsViolation checks the three server variables D-34 owns.
func historySettingsViolation(format, rowImage, commitOrder sql.NullString) (pitrarchive.HistoryViolation, string, error) {
	if !format.Valid || !strings.EqualFold(strings.TrimSpace(format.String), "ROW") {
		return pitrarchive.ViolationBinlogFormat, "binlog_format=" + format.String, nil
	}
	if !rowImage.Valid || !strings.EqualFold(strings.TrimSpace(rowImage.String), "FULL") {
		return pitrarchive.ViolationBinlogRowImage, "binlog_row_image=" + rowImage.String, nil
	}
	if !mysqlBool(commitOrder) {
		return pitrarchive.ViolationCommitOrder, "replica_preserve_commit_order=" + commitOrder.String, nil
	}
	return "", "", nil
}

// binlogFilters reads Binlog_Do_DB and Binlog_Ignore_DB from SHOW BINARY
// LOG STATUS, by column name.
func (m *LiveMysql) binlogFilters(ctx context.Context) (string, string, error) {
	rows, err := m.db.QueryContext(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		return "", "", fmt.Errorf("show binary log status: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", "", err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", "", err
		}
		return "", "", fmt.Errorf("show binary log status returned no row; binary logging is off")
	}
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return "", "", err
	}
	var doDB, ignoreDB string
	found := 0
	for i, c := range cols {
		switch c {
		case "Binlog_Do_DB":
			doDB, found = values[i].String, found+1
		case "Binlog_Ignore_DB":
			ignoreDB, found = values[i].String, found+1
		}
	}
	if found != 2 {
		return "", "", fmt.Errorf("show binary log status has no Binlog_Do_DB/Binlog_Ignore_DB columns")
	}
	return doDB, ignoreDB, rows.Err()
}

// unsafeHistoryError is the LastError of a marked site (§3.7).
func unsafeHistoryError(site string, marker *HistoryIntegrity) error {
	return fmt.Errorf("%s: %s %s %s; this site's binlogs cannot certify history; start a new archive prefix and take a full backup",
		pitrarchive.TokenArchiveUnsafeHistory, site, marker.Violation, marker.Detail)
}

// checkHistoryIntegrity runs at every scan on every site (D-34), before
// the archive session is established, so a violation is seen whatever the
// session state (awaiting the descriptor, refused, or failing). A marker
// already in this site's manifest stands, whatever the server says now. A
// new violation is held as the pending marker (in memory and, when
// cfg.StateDir is set, in a file that outlives a container restart) and is
// persisted in the site's own manifest at the first verified session. It
// returns nil only for a clean, unmarked site; any error keeps the site
// from uploading.
//
// A manifest that cannot be read on a verified session blocks uploads, but
// the MySQL probe still runs: a violation that comes and goes during a
// storage outage is held as the pending marker, not missed.
func (a *BinlogArchiver) checkHistoryIntegrity(ctx context.Context) error {
	var manifestErr error
	if a.session != nil && a.session.Verified() {
		if _, err := a.loadManifest(ctx); err != nil {
			manifestErr = wrapKeepToken("history integrity: load manifest", err)
		}
	}
	marker := a.currentMarker()
	if marker == nil {
		if a.pendingMarkerErr != nil {
			return joinIntegrity(manifestErr, fmt.Errorf("history integrity: %w", a.pendingMarkerErr))
		}
		prober, ok := a.mysql.(historyIntegrityProber)
		if !ok {
			return joinIntegrity(manifestErr, fmt.Errorf("history integrity: the MySQL probe cannot check replication filters and binlog settings"))
		}
		violation, detail, err := prober.HistoryIntegrityViolation(ctx)
		if err != nil {
			return joinIntegrity(manifestErr, fmt.Errorf("history integrity: %w", err))
		}
		if violation == "" {
			return manifestErr
		}
		marker = &HistoryIntegrity{Violation: violation, Detail: detail, At: a.now().UTC().Truncate(time.Microsecond)}
		a.pendingMarker = marker
		a.pendingMarkerSaved = false
	}
	// A pending marker whose file write failed is retried every scan
	// until it is durable or persisted in the manifest.
	if a.pendingMarker != nil && !a.pendingMarkerSaved {
		if err := a.savePendingMarker(a.pendingMarker); err != nil {
			a.logger.Warn("persist history integrity marker", "error", err)
		} else {
			a.pendingMarkerSaved = true
		}
	}
	if !a.unsafeLogged {
		a.unsafeLogged = true
		a.logger.Warn("archive history unsafe", "violation", marker.Violation, "detail", marker.Detail)
	}
	if a.pendingMarker != nil && manifestErr == nil && a.session != nil && a.session.Verified() {
		if err := a.persistMarker(ctx); err != nil {
			a.logger.Warn("persist history integrity marker", "error", err)
		}
	}
	a.mu.Lock()
	a.historyIntegrity = marker
	a.mu.Unlock()
	// The unsafe-history refusal leads: its remedy (a new prefix) stands
	// whatever else fails.
	if manifestErr != nil {
		return joinKeepToken(unsafeHistoryError(a.site, marker), manifestErr)
	}
	return unsafeHistoryError(a.site, marker)
}

// joinIntegrity adds a probe-side error to a manifest read error.
func joinIntegrity(manifestErr, err error) error {
	if manifestErr == nil {
		return err
	}
	return joinKeepToken(manifestErr, err)
}

// pendingMarkerFile is the pending marker's file name under cfg.StateDir.
const pendingMarkerFile = "history-integrity-pending.json"

func (a *BinlogArchiver) pendingMarkerPath() string {
	if a.cfg == nil || a.cfg.StateDir == "" {
		return ""
	}
	return filepath.Join(a.cfg.StateDir, pendingMarkerFile)
}

// loadPendingMarker restores a pending marker a previous container wrote.
// An unreadable or invalid file is kept as pendingMarkerErr, which blocks
// uploads: it may hold a violation, so it is never silently dropped.
func (a *BinlogArchiver) loadPendingMarker() {
	path := a.pendingMarkerPath()
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		a.pendingMarkerErr = fmt.Errorf("read pending marker %s: %w", path, err)
		return
	}
	var m HistoryIntegrity
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		a.pendingMarkerErr = fmt.Errorf("decode pending marker %s: %w", path, err)
		return
	}
	if !slices.Contains(pitrarchive.HistoryViolations, m.Violation) {
		a.pendingMarkerErr = fmt.Errorf("pending marker %s has unknown violation %q", path, m.Violation)
		return
	}
	a.pendingMarker = &m
	a.pendingMarkerSaved = true
	a.mu.Lock()
	a.historyIntegrity = &m
	a.mu.Unlock()
}

// savePendingMarker writes the pending marker durably (temp file, fsync,
// rename, directory fsync). A no-op without cfg.StateDir.
func (a *BinlogArchiver) savePendingMarker(m *HistoryIntegrity) error {
	path := a.pendingMarkerPath()
	if path == "" {
		return nil
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, pendingMarkerFile+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(dir)
}

// clearPendingMarker removes the file once the marker is in a manifest.
func (a *BinlogArchiver) clearPendingMarker() {
	path := a.pendingMarkerPath()
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		a.logger.Warn("remove pending history integrity marker", "path", path, "error", err)
	}
}

// currentMarker is the persisted marker, else a pending one.
func (a *BinlogArchiver) currentMarker() *HistoryIntegrity {
	if a.persistedMarker != nil {
		return a.persistedMarker
	}
	return a.pendingMarker
}

// persistMarker writes the pending marker unless the manifest already has
// one; the first marker stands (set once, never cleared).
func (a *BinlogArchiver) persistMarker(ctx context.Context) error {
	m, err := a.loadManifest(ctx)
	if err != nil {
		return err
	}
	if m.HistoryIntegrity == nil {
		marker := *a.pendingMarker
		m.HistoryIntegrity = &marker
		if err := a.saveManifest(ctx, m); err != nil {
			return err
		}
	}
	a.persistedMarker = m.HistoryIntegrity
	a.pendingMarker = nil
	a.clearPendingMarker()
	return nil
}
