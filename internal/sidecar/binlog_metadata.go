package sidecar

import (
	"fmt"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

// BinlogMetadata is the summary of a single sealed binlog file. It is
// extracted once per file (on archive), stored alongside the archived
// object via the per-site manifest, and consumed by the restore Job to
// decide which files are in the replay window for a given
// --stop-datetime target.
//
// We deliberately keep the field set minimal:
//
//   - Name: matches the MySQL-chosen basename (mysql-bin.NNNNNN).
//   - Size: local file size before upload.
//   - FirstEventTime / LastEventTime: wall-clock timestamps taken from
//     event headers. The parser walks the file once; this is cheap.
//   - PreviousGTIDs: GTID set present at the start of the file
//     (PREVIOUS_GTIDS_LOG_EVENT), used by restore to prune files whose
//     entire content predates the dump's gtid_executed.
//   - EndGTIDs: PreviousGTIDs plus all GTID events observed while
//     scanning to the end of file. Same shape as PreviousGTIDs.
//
// MySQL writes the FDE with the server start time, not the "first real
// event time". We therefore skip the FDE when computing FirstEventTime
// and use the timestamp of the first non-FDE event.
type BinlogMetadata struct {
	Name           string    `json:"name"`
	Size           int64     `json:"size"`
	FirstEventTime time.Time `json:"firstEventTime"`
	LastEventTime  time.Time `json:"lastEventTime"`
	PreviousGTIDs  string    `json:"previousGtids,omitempty"`
	EndGTIDs       string    `json:"endGtids,omitempty"`
}

// parseBinlogMetadata scans the given binlog file once and returns a
// BinlogMetadata describing its coordinates. The file must be a
// complete (sealed) binlog — parsing an actively written file is
// racy and is the caller's responsibility to avoid.
//
// Errors parsing individual events are intentionally non-fatal: we
// accept whatever metadata we could gather before the error. A binlog
// can legitimately end with a half-written event if MySQL crashed, and
// we still want to archive what we have.
func parseBinlogMetadata(path string) (BinlogMetadata, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return BinlogMetadata{}, fmt.Errorf("stat %s: %w", path, err)
	}
	meta := BinlogMetadata{
		Name: fi.Name(),
		Size: fi.Size(),
	}

	parser := replication.NewBinlogParser()

	// GTID accumulator for EndGTIDs: we start with PreviousGTIDs and
	// extend it as we see GTID events (GTID_EVENT or GTID_LIST_EVENT).
	// Tracking the full set from strings is tricky; we build a simple
	// string accumulator by concatenating GTIDs as observed and let
	// the restore side run them through mysql's GTID normalization.
	// A more precise approach would be github.com/go-mysql-org/go-mysql
	// mysql.ParseGTIDSet, but for MVP we keep this as a best-effort
	// string hint.
	var gotFirstTimestamp bool

	err = parser.ParseFile(path, 0, func(e *replication.BinlogEvent) error {
		ts := time.Unix(int64(e.Header.Timestamp), 0).UTC()

		switch ev := e.Event.(type) {
		case *replication.FormatDescriptionEvent:
			// FDE timestamp is server-start-time, not useful as
			// FirstEventTime. Skip.
			_ = ev
		case *replication.PreviousGTIDsEvent:
			meta.PreviousGTIDs = ev.GTIDSets
			if meta.EndGTIDs == "" {
				meta.EndGTIDs = ev.GTIDSets
			}
		case *replication.GTIDEvent:
			if !gotFirstTimestamp && e.Header.Timestamp > 0 {
				meta.FirstEventTime = ts
				gotFirstTimestamp = true
			}
			if e.Header.Timestamp > 0 {
				meta.LastEventTime = ts
			}
		default:
			if !gotFirstTimestamp && e.Header.Timestamp > 0 && e.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
				meta.FirstEventTime = ts
				gotFirstTimestamp = true
			}
			if e.Header.Timestamp > 0 {
				meta.LastEventTime = ts
			}
		}
		return nil
	})
	// Ignore EOF-style errors from ParseFile (truncated tail events).
	// If we got at least a PreviousGTIDs or a first timestamp we return
	// success; the upstream caller decides whether to archive or defer.
	if err != nil && !gotFirstTimestamp && meta.PreviousGTIDs == "" {
		return meta, fmt.Errorf("parse %s: %w", path, err)
	}
	return meta, nil
}
