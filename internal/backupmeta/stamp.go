package backupmeta

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Backup stamp (design D-8, §4.2).
//
// The backup Job's `backup-stamp` init container writes the stamp to
// StampKey(location) before the dump starts, and the Job's final step
// rewrites it with ServerClock.End once mysqlsh has exited successfully. It
// is a sibling of the dump directory, not inside it, because mysqlsh refuses
// a non-empty output directory (F33). It is BRV1-encrypted with the dump key
// when the dump is encrypted. `dump-meta` reads it, with @.json and
// @.done.json, and writes DumpMeta to /pitr-dump-meta/dump.json.
const (
	StampKind    = "BloodravenBackupStamp"
	StampVersion = 1
	// StampSuffix is appended to the dump location to form the stamp key.
	StampSuffix = ".bloodraven-backup.json"

	// DumpMetaVersion is the version of /pitr-dump-meta/dump.json.
	DumpMetaVersion = 1
)

// StampKey returns the stamp's object key (S3) or relative path (PVC) for a
// dump written under location: "<location without trailing slashes>" +
// StampSuffix, e.g. "backups/nightly-1" -> "backups/nightly-1.bloodraven-backup.json".
func StampKey(location string) string {
	return strings.TrimRight(location, "/") + StampSuffix
}

// ObjectRef names a Kubernetes object by namespace, name and UID.
type ObjectRef struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

func (r ObjectRef) complete() bool {
	return r.Namespace != "" && r.Name != "" && r.UID != ""
}

// DumpFilters records the schema filters the dump ran with (F42, D-45).
// Both lists marshal as [] when empty, so the canonical bytes do not depend
// on nil versus empty.
type DumpFilters struct {
	IncludeSchemas []string `json:"includeSchemas"`
	ExcludeSchemas []string `json:"excludeSchemas"`
}

// Empty reports whether the dump ran without schema filters. A non-empty
// filter refuses PITR (PITRBackupFiltered).
func (f DumpFilters) Empty() bool {
	return len(f.IncludeSchemas) == 0 && len(f.ExcludeSchemas) == 0
}

// ServerClock is the dumped server's own clock (`SELECT NOW(6)`, UTC) around
// the dump (D-52). Start is read just before the dump; End is read on the
// same server after mysqlsh exits successfully. A zero End means the dump
// did not finish (PITRBackupIncomplete).
type ServerClock struct {
	Start MicroTime `json:"start"`
	End   MicroTime `json:"end"`
}

// Stamp is the writer-stamped identity of one dump.
type Stamp struct {
	Kind    string `json:"kind"`
	Version int    `json:"version"`
	// Backup is the MysqlBackup that owns the dump.
	Backup ObjectRef `json:"backup"`
	// Source is the MysqlFailoverGroup the dump was taken from.
	Source ObjectRef `json:"source"`
	// PITRArchiveID is the source group's active PITR archive ID when the
	// backup was created (pitrarchive.ArchiveIDDerivations[0]); "" when the
	// group had PITR disabled.
	PITRArchiveID string `json:"pitrArchiveID"`
	// Site and ServerID identify the dumped server (F26, F41).
	Site        string      `json:"site"`
	ServerID    uint32      `json:"serverID"`
	DumpFilters DumpFilters `json:"dumpFilters"`
	ServerClock ServerClock `json:"serverClock"`
	CreatedAt   MicroTime   `json:"createdAt"`
}

// Stamp validation errors. They are wrapped with detail; match with
// errors.Is.
var (
	ErrStampInvalid         = errors.New("backup stamp is invalid")
	ErrStampRewriteConflict = errors.New("backup stamp rewrite changes fields other than serverClock.end")
)

// Validate checks the fields every stamp must carry, whether or not the
// dump has finished. It does not require ServerClock.End.
func (s Stamp) Validate() error {
	var problems []string
	if s.Kind != StampKind {
		problems = append(problems, fmt.Sprintf("kind %q, want %q", s.Kind, StampKind))
	}
	if s.Version != StampVersion {
		problems = append(problems, fmt.Sprintf("version %d, want %d", s.Version, StampVersion))
	}
	if !s.Backup.complete() {
		problems = append(problems, "backup {namespace,name,uid} incomplete")
	}
	if !s.Source.complete() {
		problems = append(problems, "source {namespace,name,uid} incomplete")
	}
	if s.PITRArchiveID != "" && !pitrarchive.ValidArchiveID(s.PITRArchiveID) {
		problems = append(problems, fmt.Sprintf("pitrArchiveID %q is malformed", s.PITRArchiveID))
	}
	if s.Site == "" {
		problems = append(problems, "site is empty")
	}
	if s.ServerID == 0 {
		problems = append(problems, "serverID is 0")
	}
	if s.ServerClock.Start.IsZero() {
		problems = append(problems, "serverClock.start is missing")
	}
	if !s.ServerClock.End.IsZero() && s.ServerClock.End.Before(s.ServerClock.Start.Time) {
		problems = append(problems, "serverClock.end is before serverClock.start")
	}
	if s.CreatedAt.IsZero() {
		problems = append(problems, "createdAt is missing")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrStampInvalid, strings.Join(problems, "; "))
	}
	return nil
}

// Complete reports whether the dump finished (ServerClock.End is set).
func (s Stamp) Complete() bool { return !s.ServerClock.End.IsZero() }

// Canonical returns the stamp's canonical JSON: fixed field order, nil
// filter lists as [], times in MicroTimeLayout. Two stamps are the same
// stamp iff their canonical bytes are equal.
func (s Stamp) Canonical() ([]byte, error) {
	c := s
	c.DumpFilters.IncludeSchemas = nonNil(c.DumpFilters.IncludeSchemas)
	c.DumpFilters.ExcludeSchemas = nonNil(c.DumpFilters.ExcludeSchemas)
	c.ServerClock.Start = NewMicroTime(c.ServerClock.Start.Time)
	c.ServerClock.End = NewMicroTime(c.ServerClock.End.Time)
	c.CreatedAt = NewMicroTime(c.CreatedAt.Time)
	return json.Marshal(c)
}

// ParseStamp decodes and validates a stamp. Unknown fields are refused: the
// stamp is version 1 and its writer is this release.
func ParseStamp(b []byte) (Stamp, error) {
	var s Stamp
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return Stamp{}, fmt.Errorf("%w: decode: %v", ErrStampInvalid, err)
	}
	if err := s.Validate(); err != nil {
		return Stamp{}, err
	}
	return s, nil
}

// CheckStampEndRewrite checks that next may replace prev at the stamp key:
// next sets ServerClock.End, and every other field is byte-identical
// (D-8). A retry that rewrites the same End is accepted; a different End
// over an existing one is refused.
func CheckStampEndRewrite(prev, next Stamp) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if !next.Complete() {
		return fmt.Errorf("%w: the rewrite does not set serverClock.end", ErrStampRewriteConflict)
	}
	if prev.Complete() && !prev.ServerClock.End.Equal(next.ServerClock.End.Time) {
		return fmt.Errorf("%w: serverClock.end is already %s", ErrStampRewriteConflict, prev.ServerClock.End)
	}
	a, b := prev, next
	a.ServerClock.End, b.ServerClock.End = MicroTime{}, MicroTime{}
	ab, err := a.Canonical()
	if err != nil {
		return err
	}
	bb, err := b.Canonical()
	if err != nil {
		return err
	}
	if !bytes.Equal(ab, bb) {
		return fmt.Errorf("%w: %s != %s", ErrStampRewriteConflict, ab, bb)
	}
	return nil
}

// DumpMeta is /pitr-dump-meta/dump.json, written by `dump-meta` and read by
// `pitr-download` (§4.2). GTIDExecuted is B, the dump's @.json
// gtidExecuted. End is @.done.json's end (UTC; the dump container runs with
// TZ=UTC). ServerClock is copied from the stamp (D-52).
type DumpMeta struct {
	Version                  int         `json:"version"`
	Location                 string      `json:"location"`
	GTIDExecuted             string      `json:"gtidExecuted"`
	GTIDExecutedInconsistent bool        `json:"gtidExecutedInconsistent"`
	End                      MicroTime   `json:"end"`
	ServerClock              ServerClock `json:"serverClock"`
	Stamp                    Stamp       `json:"stamp"`
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
