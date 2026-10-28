package sidecar

import (
	"fmt"
	"os"
	"time"
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
//     was already applied BEFORE this file was opened, not the file's
//     own transactions.
//   - GTIDCoverage: complete transaction coverage for retention, or an
//     explicit unknown result when parsing cannot establish it.
//
// MySQL writes the FDE with the server start time, not the "first real
// event time". We therefore skip the FDE when computing FirstEventTime
// and use the timestamp of the first non-FDE event.
type BinlogMetadata struct {
	GTIDCoverage   *BinlogGTIDCoverage `json:"gtidCoverage,omitempty"`
	Name           string              `json:"name"`
	Size           int64               `json:"size"`
	FirstEventTime time.Time           `json:"firstEventTime"`
	LastEventTime  time.Time           `json:"lastEventTime"`
	PreviousGTIDs  string              `json:"previousGtids,omitempty"`
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

	f, err := os.Open(path)
	if err != nil {
		return meta, err
	}
	defer f.Close()
	evidence, parseErr := InspectBinlog(f, time.Time{})
	evidence.Metadata.Name = fi.Name()
	if parseErr != nil && evidence.Metadata.FirstEventTime.IsZero() && evidence.Metadata.PreviousGTIDs == "" {
		return evidence.Metadata, parseErr
	}
	return evidence.Metadata, nil
}
