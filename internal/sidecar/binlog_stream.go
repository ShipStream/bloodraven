package sidecar

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// BinlogEvidence separates replay observations from stricter retention coverage.
// Evidence is usable only when InspectBinlog reaches an authenticated EOF.
type BinlogEvidence struct {
	Metadata        BinlogMetadata
	Digest          string
	GTIDs           internalmysql.GTIDSet
	StopReached     bool
	BeforeStopGTIDs internalmysql.GTIDSet
}
type binlogCountingReader struct {
	io.Reader
	n int64
}

func (r *binlogCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += int64(n)
	return n, err
}

// InspectBinlog parses a whole plaintext stream, including unknown valid events.
// Retention may remain incomplete for valid XA, LOAD DATA or checksum-off files.
func InspectBinlog(r io.Reader, stop time.Time) (BinlogEvidence, error) {
	return inspectBinlog(r, stop, nil)
}
func inspectBinlog(r io.Reader, stop time.Time, observe func(*replication.BinlogEvent)) (BinlogEvidence, error) {
	h := sha256.New()
	count := &binlogCountingReader{Reader: io.TeeReader(r, h)}
	result := BinlogEvidence{GTIDs: make(internalmysql.GTIDSet)}
	var magic [4]byte
	if _, err := io.ReadFull(count, magic[:]); err != nil {
		return result, err
	}
	if !bytes.Equal(magic[:], replication.BinLogFileHeader) {
		return result, fmt.Errorf("invalid binlog magic")
	}
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	coverage := newBinlogCoverageCollector()
	first := false
	previous := false
	anonymous := false
	beforeStop := newBinlogCoverageCollector()
	err := parser.ParseReader(count, func(event *replication.BinlogEvent) error {
		if observe != nil {
			observe(event)
		}
		coverage.observe(event)
		if !stop.IsZero() && int64(event.Header.Timestamp) >= stop.Unix() {
			result.StopReached = true
		}
		if !result.StopReached {
			beforeStop.observe(event)
		}
		if pg, ok := event.Event.(*replication.PreviousGTIDsEvent); ok {
			previous = true
			result.Metadata.PreviousGTIDs = pg.GTIDSets
		}
		if event.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT && event.Header.EventType != replication.PREVIOUS_GTIDS_EVENT && event.Header.Timestamp > 0 {
			ts := time.Unix(int64(event.Header.Timestamp), 0).UTC()
			if !first {
				result.Metadata.FirstEventTime = ts
				first = true
			}
			result.Metadata.LastEventTime = ts
		}
		if event.Header.EventType == replication.ANONYMOUS_GTID_EVENT {
			anonymous = true
		}
		if event.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			if format, ok := event.Event.(*replication.FormatDescriptionEvent); ok && format.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32 && !coverage.valid {
				return fmt.Errorf("invalid format checksum")
			}
		}
		return nil
	})
	_, drainErr := io.Copy(io.Discard, count)
	if err == nil {
		err = drainErr
	}
	if err == nil && coverage.bytes != count.n {
		err = fmt.Errorf("truncated binlog: parsed %d of %d bytes", coverage.bytes, count.n)
	}
	if err == nil && (!coverage.format || !previous) {
		err = fmt.Errorf("incomplete binlog header")
	}
	if err == nil && anonymous {
		err = fmt.Errorf("anonymous GTID cannot establish replay order")
	}
	result.GTIDs = UnionBinlogGTIDs(coverage.gtids)
	result.BeforeStopGTIDs = UnionBinlogGTIDs(beforeStop.gtids)
	result.Metadata.Size = count.n
	result.Metadata.GTIDCoverage = coverage.result(count.n, err)
	// Observations remain useful for early divergence diagnostics, never deletion.
	if !result.Metadata.GTIDCoverage.Complete {
		result.Metadata.GTIDCoverage.GTIDs = result.GTIDs.String()
	}
	result.Digest = hex.EncodeToString(h.Sum(nil))
	return result, err
}
