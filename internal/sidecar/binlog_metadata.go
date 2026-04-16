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
//     (PREVIOUS_GTIDS_LOG_EVENT). This is the complete GTID state that
//     was already applied BEFORE this file was opened; it's enough for
//     restore to cheaply prune files whose entire contents predate the
//     dump's gtid_executed. A full per-file GTID range would require
//     accumulating GTID events and merging sets, which the MVP restore
//     path doesn't need — timestamp filtering plus server-side GTID
//     dedup at replay time is sufficient.
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
	var gotFirstTimestamp bool

	err = parser.ParseFile(path, 0, func(e *replication.BinlogEvent) error {
		ts := time.Unix(int64(e.Header.Timestamp), 0).UTC()

		if pg, ok := e.Event.(*replication.PreviousGTIDsEvent); ok {
			meta.PreviousGTIDs = pg.GTIDSets
			return nil
		}
		// FDE timestamp is server-start-time, not a real event time;
		// skip it so FirstEventTime tracks the first actual event.
		if e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			return nil
		}
		if e.Header.Timestamp > 0 {
			if !gotFirstTimestamp {
				meta.FirstEventTime = ts
				gotFirstTimestamp = true
			}
			meta.LastEventTime = ts
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
