package sidecar

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// The tail drain (#264). A writable site archives every sealed binlog. When
// its server turns read-only (a planned failover, a fence, a restart behind
// a new primary) the last files of its writable era may not be archived yet:
// the file the fence's FLUSH BINARY LOGS sealed, or files a busy scan had
// not reached. A reclone wipes them. So a read-only site keeps archiving,
// but only its writable-era tail:
//
//	j = the newest sealed file that holds an origin transaction
//	    (binlog_origin.go)
//
// and it uploads every sealed file up to and including j that is not in its
// manifest, through the same scanFiles a writable site uses. Each of those
// files was sealed before j's first origin transaction was written, so a
// scan of that writable era was entitled to upload it; that includes
// replica-era files sealed at the promotion that a short writable era left
// behind. Nothing after j is uploaded: those are pure replica copies. A file
// that mixes the writable era with later replica copies is uploaded whole.
//
// The rule keeps no memory of the demotion: it is decided from the local
// files, the role probe and the manifest alone, so a sidecar restart in the
// middle of a drain resumes it. What is remembered in memory (per-file
// verdicts, the settled prefix) is bound to each file's identity
// (stableBinlogFile) and only saves work.

// tailFile is what the drain remembers about one sealed binlog, while the
// file is unchanged.
type tailFile struct {
	info os.FileInfo
	// previous is the file's Previous_gtids header.
	previous     string
	havePrevious bool
	// origin is whether the file holds an origin transaction;
	// originUnfinished until it was examined.
	origin originVerdict
	// archived: the file needs no upload. A row of this site's manifest
	// (live, retired or quarantined) matched it, or a drain settled it.
	archived bool
	// id is the file's content identity, once a drain needed it.
	id string
}

// tailDrain is the drain's memory, owned by the scan goroutine.
type tailDrain struct {
	files map[string]*tailFile
	// through is the newest file of a settled prefix: it and every sealed
	// file before it need no upload. The search for j stops there.
	through     string
	throughInfo os.FileInfo
	// walk is an origin walk the budget cut short, of walkName.
	walk     *originWalk
	walkName string
	// blockedReason is the last refusal logged, so a standing one is
	// logged once.
	blockedReason string
	// scanned are the files this scan has stat'ed (tailStat): within one
	// scan a file has one identity, from its first stat through its
	// examination to the upload list.
	scanned map[string]struct{}
	// headerReads, walks and hashes count the file headers read, the origin
	// walks started and the files hashed for their content identity, for
	// the tests that pin a file is examined once. budget replaces
	// newOriginBudget in tests.
	headerReads int
	walks       int
	hashes      int
	budget      func() *originBudget
}

// uploadFailure is a pass of uploads that failed, as /archiver/status
// reports it: the archiver error and the backlog. It stands until a scan
// settles what it was about (see tailOutcome).
type uploadFailure struct {
	msg     string
	backlog int64
}

// tailOutcome is what a read-only scan's drain means for the archiver's
// error and backlog. A failing upload is reported exactly as on a writable
// site: from the scan that failed until a scan that settles it, with no gap
// while a later scan is still working (the operator's polls would take a gap
// for a recovery, and the backlog alert could never fire).
type tailOutcome int

const (
	// tailQuiet: the scan decided that this site has nothing to upload (no
	// tail, or all of it archived) or may not upload (a gate refused, by
	// design not an error). A standby with nothing to report: no error, no
	// backlog, and an earlier upload failure is over.
	tailQuiet tailOutcome = iota
	// tailUndecided: the scan could not decide (the index or a file could
	// not be read, the examination is unfinished, the role moved on). It
	// reports nothing of its own; an upload failure that stands is still
	// the truth about its files and stays reported.
	tailUndecided
	// tailUploads: an upload pass ran, or its manifest could not be loaded.
	// The drain set the error and the backlog itself.
	tailUploads
)

// tailUpload is set while a read-only site's drain runs scanFiles. It binds
// that pass to what the drain decided on: the role observation the gates
// were checked against (episodeCurrent refuses manifest saves once the
// timeline moved on) and the files as the drain examined them: each file's
// identity is the one its first stat of this scan found (tailStat), which
// the examination ran on (archivePinned uploads no other bytes under their
// names).
type tailUpload struct {
	obs   RoleObservation
	files map[string]os.FileInfo
}

// tailScan is one scan's result for the tail block of /archiver/status.
type tailScan struct {
	// indexRead: entries is the binlog index this scan read.
	indexRead bool
	entries   []string
	state     string
	pending   int64
	reason    string
}

func tailDrained(entries []string) tailScan {
	return tailScan{indexRead: true, entries: entries, state: internalmysql.ArchiverTailDrained}
}

func tailPending(entries []string, pending int64) tailScan {
	return tailScan{indexRead: true, entries: entries, state: internalmysql.ArchiverTailPending, pending: pending}
}

func tailBlocked(entries []string, pending int64, reason string) tailScan {
	return tailScan{indexRead: true, entries: entries, state: internalmysql.ArchiverTailBlocked, pending: pending, reason: reason}
}

// tailBlockedEarly is the result of a scan that ended before it read the
// binlog index: Blocked, with the index read now for the file names.
func (a *BinlogArchiver) tailBlockedEarly(reason string) tailScan {
	t := tailScan{state: internalmysql.ArchiverTailBlocked, reason: reason}
	if entries, err := readBinlogIndex(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex)); err == nil {
		t.indexRead, t.entries = true, entries
	}
	return t
}

// publishTail sets the tail block from one scan: the state and the file
// names come from the same index read and change together under the status
// mutex. The names say which index the state is about: Drained means that
// every sealed file of the index in which ActiveFile was the active one is
// archived, or is not this site's to archive. A rotation after that read is
// not in it (the scan does not read the index again), so a reader that waits
// for a rotation compares ActiveFile with the file that rotation opened. A
// scan that could not read the index reports no file names (and is never
// Drained), and publishes nothing before any scan read it.
func (a *BinlogArchiver) publishTail(t tailScan) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := &internalmysql.ArchiverTailStatus{State: t.state, PendingFiles: t.pending, Reason: t.reason}
	if !t.indexRead {
		if a.tailStatus == nil {
			return
		}
	} else if n := len(t.entries); n > 0 {
		st.ActiveFile = filepath.Base(t.entries[n-1])
		if n > 1 {
			st.NewestSealedFile = filepath.Base(t.entries[n-2])
		}
	}
	a.tailStatus = st
}

// errTailEnded: the role sampler saw the server leave the read-only role
// and incarnation a tail drain was decided on, while the drain was writing.
var errTailEnded = errors.New("the server stopped being read-only, or changed incarnation, while its binlog tail was being archived; the write waits for the next scan")

// tailCurrent refuses a drain's manifest save once the timeline no longer
// shows the read-only server the drain's gates were checked against: a
// writable or failed observation, or an incarnation change.
func (t *roleTimeline) tailCurrent(obs RoleObservation) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != roleReadOnly || t.readOnly == nil || t.readOnly.ServerUUID == "" || len(t.resets) > 0 || !sameIncarnation(obs, *t.readOnly) {
		return errTailEnded
	}
	return nil
}

// readOnlyIdentity returns the latest role observation when it is read-only
// (readOnly) and reported the server identity (identified).
func (t *roleTimeline) readOnlyIdentity() (obs RoleObservation, readOnly, identified bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != roleReadOnly || t.readOnly == nil {
		return RoleObservation{}, false, false
	}
	if t.readOnly.ServerUUID == "" {
		return RoleObservation{}, true, false
	}
	return *t.readOnly, true, true
}

// check refuses the upload of a sealed binlog that is not the file the
// drain examined (a reclone or a reset restarts the numbering, so the name
// may now be a file of another incarnation).
func (u *tailUpload) check(name string, opened os.FileInfo) error {
	if want := u.files[name]; want == nil || !stableBinlogFile(want, opened) {
		return fmt.Errorf("binlog %s was replaced after its index was read; it is not this server's sealed tail", name)
	}
	return nil
}

// drainTail is a scan's work on a read-only site: it archives the
// writable-era tail (see the top of this file) and returns the tail block.
//
// A read-only site with nothing to drain reports what it always did:
// backlog 0 and no error. Nothing that only decides whether there is a tail
// (the index, the file headers, the origin walk, the gates) is reported as
// an archiver error: it blocks the tail with a reason and is logged once.
// Errors of the uploads themselves (the manifest, the store, a file) are
// archiver errors, as on a writable site, and as sticky (tailOutcome).
//
// The scan's incarnation-reset result (recordResets) is not a gate here. A
// write error is reported by the scan before it gets here. What is left is
// errStartCheckPending: the session is not verified, or this process has no
// observation that reported the server identity. The drain refuses both
// before it reads the manifest (the session gate, readOnlyIdentity), and
// what the start-up check compares, the site's latest record with the
// server, tailRecordGate compares for the very observation the drain
// uploads under.
func (a *BinlogArchiver) drainTail(ctx context.Context) tailScan {
	result, outcome := a.drainTailFiles(ctx)
	switch outcome {
	case tailQuiet:
		a.uploadFailure = nil
		a.setBacklog(0)
		a.clearScanError()
	case tailUndecided:
		if f := a.uploadFailure; f != nil {
			a.setBacklog(f.backlog)
			a.setError(f.msg)
		} else {
			a.setBacklog(0)
			a.clearScanError()
		}
	}
	if result.state != internalmysql.ArchiverTailBlocked || outcome == tailUploads {
		a.tail.blockedReason = ""
	} else if result.reason != a.tail.blockedReason {
		a.tail.blockedReason = result.reason
		a.logger.Warn("binlog tail not archived while read-only", "reason", result.reason, "pendingFiles", result.pending)
	}
	return result
}

// noteUploadFailure remembers a failed pass of uploads as it is reported
// now (LastError and the backlog), for the read-only scans that cannot
// decide (tailUndecided).
func (a *BinlogArchiver) noteUploadFailure() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.uploadFailure = &uploadFailure{msg: a.lastError, backlog: a.backlogFiles}
	return a.lastError
}

// drainTailFiles does drainTail's work. Nothing here clears the archiver's
// error or backlog before the scan knows its outcome.
func (a *BinlogArchiver) drainTailFiles(ctx context.Context) (tailScan, tailOutcome) {
	a.tail.scanned = make(map[string]struct{})
	entries, err := readBinlogIndex(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex))
	if err != nil {
		return tailScan{state: internalmysql.ArchiverTailBlocked, reason: fmt.Sprintf("read index: %v", err)}, tailUndecided
	}
	if len(entries) < 2 {
		return tailDrained(entries), tailQuiet
	}
	sealed := entries[:len(entries)-1]
	a.pruneTailFiles(sealed)
	obs, readOnly, identified := a.roles.readOnlyIdentity()
	if !readOnly {
		// The role sampler saw the server change since this scan's probe.
		return tailBlocked(entries, 0, "the server stopped being read-only during this scan; the next scan decides what it archives"), tailUndecided
	}
	if !identified {
		return tailBlocked(entries, 0, "the tail cannot be told from replica copies: "+errRoleProbeUnsupported.Error()), tailQuiet
	}
	j, stop, done, err := a.findTail(entries, obs, a.tailBudget())
	if err != nil {
		return tailBlocked(entries, 0, err.Error()), tailUndecided
	}
	if !done {
		return tailPending(entries, 0), tailUndecided
	}
	if j < 0 {
		return tailDrained(entries), tailQuiet
	}
	if a.session == nil || !a.session.Verified() {
		return tailBlocked(entries, 0, "the archive session is not verified yet (the archive descriptor is awaited); the tail through "+filepath.Base(sealed[j])+" waits for it"), tailQuiet
	}
	manifest, err := a.loadManifest(ctx)
	if err != nil {
		a.incUploadFailures()
		a.setError(wrapKeepToken("load manifest", err).Error())
		a.logger.Warn("load manifest", "error", err)
		return tailBlocked(entries, 0, a.noteUploadFailure()), tailUploads
	}
	retired := a.retiredBinlogIdentities(manifest)
	// A budget of its own: a slow manifest read must not starve it.
	unarchived, unconfirmed, done, err := a.unarchivedTail(manifest, retired, sealed, stop, j, a.tailBudget())
	if err != nil {
		return tailBlocked(entries, 0, err.Error()), tailUndecided
	}
	if !done {
		return tailPending(entries, 0), tailUndecided
	}
	last := filepath.Base(sealed[j])
	if len(unarchived) == 0 {
		a.tail.through, a.tail.throughInfo = last, a.tail.files[last].info
		return tailDrained(entries), tailQuiet
	}
	pending := int64(len(unarchived))
	// The gates of a read-only upload. The session is verified and the
	// scan returned before this on a history-integrity error.
	if err := tailRecordGate(manifest, obs); err != nil {
		return tailBlocked(entries, pending, err.Error()), tailQuiet
	}
	// The upload is bound to the files as this scan examined them: tailStat
	// returns each file's identity from its first stat of the scan, and
	// refuses a file that changed since.
	upload := &tailUpload{obs: obs, files: make(map[string]os.FileInfo, j+1)}
	for _, entry := range sealed[:j+1] {
		name := filepath.Base(entry)
		f, err := a.tailStat(name)
		if err != nil {
			return tailBlocked(entries, pending, fmt.Sprintf("stat %s: %v", name, err)), tailUndecided
		}
		upload.files[name] = f.info
	}
	// Nothing is uploaded for a server the role sampler has since seen
	// writable, failed or of another incarnation: the save would be refused
	// (episodeCurrent), after the object was written.
	if err := a.roles.tailCurrent(obs); err != nil {
		return tailBlocked(entries, pending, err.Error()), tailUndecided
	}
	a.tailUpload = upload
	archived, _, hadError := a.scanFiles(ctx, manifest, sealed[:j+1], retired)
	a.tailUpload = nil

	pending = 0
	for _, names := range [][]string{unarchived, unconfirmed} {
		for _, name := range names {
			if a.scanSettled[name] {
				a.tail.files[name].archived = true
			} else {
				pending++
			}
		}
	}
	a.setBacklog(pending)
	if archived > 0 {
		now := time.Now().UTC()
		a.mu.Lock()
		a.filesArchiv += int64(archived)
		a.lastUploadAt = now
		a.mu.Unlock()
		// Index order is name order (readBinlogIndex).
		a.logger.Info("archived binlog tail while read-only", "count", archived, "through", slices.Max(a.scanUploaded))
	}
	if hadError {
		// scanFiles reported the file's error and kept it as LastError.
		return tailBlocked(entries, pending, a.noteUploadFailure()), tailUploads
	}
	a.uploadFailure = nil
	a.clearScanError()
	if pending > 0 {
		return tailPending(entries, pending), tailUploads
	}
	a.tail.through, a.tail.throughInfo = last, a.tail.files[last].info
	return tailDrained(entries), tailUploads
}

// tailBudget bounds one phase of a scan's examination of local files.
func (a *BinlogArchiver) tailBudget() *originBudget {
	if a.tail.budget != nil {
		return a.tail.budget()
	}
	return newOriginBudget()
}

// pruneTailFiles forgets the files that left the index.
func (a *BinlogArchiver) pruneTailFiles(sealed []string) {
	if len(a.tail.files) == 0 {
		return
	}
	names := make(map[string]struct{}, len(sealed))
	for _, entry := range sealed {
		names[filepath.Base(entry)] = struct{}{}
	}
	for name := range a.tail.files {
		if _, ok := names[name]; !ok {
			delete(a.tail.files, name)
		}
	}
}

// errTailFileChanged: a sealed binlog is no longer the file an earlier step
// of the same scan saw under its name.
var errTailFileChanged = errors.New("the binlog was replaced during this scan (a reclone or a reset restarts the numbering); the next scan examines it again")

// tailStat stats a sealed binlog and returns what is remembered about it,
// starting over when the file changed since an earlier scan. Within one
// scan a file has one identity, the one its first stat of the scan found:
// the examination and the upload list are bound to it, and a file that
// changed since is an error, never examined as one file and uploaded as
// another.
func (a *BinlogArchiver) tailStat(name string) (*tailFile, error) {
	info, err := os.Stat(filepath.Join(a.cfg.BinlogDir, name))
	if err != nil {
		return nil, err
	}
	f := a.tail.files[name]
	if _, seen := a.tail.scanned[name]; seen && f != nil {
		if !stableBinlogFile(f.info, info) {
			return nil, errTailFileChanged
		}
		return f, nil
	}
	if a.tail.scanned != nil {
		a.tail.scanned[name] = struct{}{}
	}
	if f == nil || !stableBinlogFile(f.info, info) {
		f = &tailFile{info: info}
		if a.tail.files == nil {
			a.tail.files = make(map[string]*tailFile)
		}
		a.tail.files[name] = f
		if a.tail.walkName == name {
			a.tail.walk, a.tail.walkName = nil, ""
		}
	}
	return f, nil
}

// tailPrevious returns a sealed binlog's Previous_gtids header, read once.
func (a *BinlogArchiver) tailPrevious(name string, f *tailFile) (string, error) {
	if !f.havePrevious {
		a.tail.headerReads++
		header, err := localBinlogHeader(filepath.Join(a.cfg.BinlogDir, name))
		if err != nil {
			return "", fmt.Errorf("read the header of %s: %w", name, err)
		}
		f.previous, f.havePrevious = header.previous, true
	}
	return f.previous, nil
}

// findTail searches the sealed files, newest first, for j: the newest one
// that holds an origin transaction of the server obs. It stops at the
// settled prefix (stop is its last file's index, -1 without one). j is -1
// when no file above the prefix holds one. done is false when the budget
// ran out; the verdicts found so far are kept for the next scan.
func (a *BinlogArchiver) findTail(entries []string, obs RoleObservation, budget *originBudget) (j, stop int, done bool, err error) {
	sealed := entries[:len(entries)-1]
	for i := len(sealed) - 1; i >= 0; i-- {
		name := filepath.Base(sealed[i])
		f, err := a.tailStat(name)
		if err != nil {
			return -1, -1, false, fmt.Errorf("stat %s: %w", name, err)
		}
		if name == a.tail.through && stableBinlogFile(a.tail.throughInfo, f.info) {
			return -1, i, true, nil
		}
		if f.origin == originUnfinished {
			if budget.spent() {
				return -1, -1, false, nil
			}
			verdict, err := a.examineOrigin(entries, i, f, obs, budget)
			if err != nil {
				return -1, -1, false, err
			}
			if verdict == originUnfinished {
				return -1, -1, false, nil
			}
			f.origin = verdict
		}
		if f.origin == originFound {
			return i, a.settledPrefix(sealed[:i]), true, nil
		}
	}
	return -1, -1, true, nil
}

// settledPrefix returns the index in sealed of the settled prefix's last
// file, or -1 when sealed does not hold it unchanged.
func (a *BinlogArchiver) settledPrefix(sealed []string) int {
	for k := len(sealed) - 1; k >= 0 && a.tail.through != ""; k-- {
		name := filepath.Base(sealed[k])
		if name != a.tail.through {
			continue
		}
		if f, err := a.tailStat(name); err == nil && stableBinlogFile(a.tail.throughInfo, f.info) {
			return k
		}
		break
	}
	return -1
}

// examineOrigin decides whether sealed file i holds an origin transaction.
// The GTIDs a sealed file logged are exactly the next file's Previous_gtids
// minus its own, so a file that logged none under this server's server_uuid
// (plain or tagged) holds no transaction the server originated, and its body
// is not read: a long-time replica examines headers only. Otherwise the
// event headers are walked for a GTID event with the writer's server_id.
//
// A transaction the server committed under another source's GTID (an
// explicit gtid_next) is not found by this filter; a file whose only origin
// transactions are of that kind is not part of the tail.
func (a *BinlogArchiver) examineOrigin(entries []string, i int, f *tailFile, obs RoleObservation, budget *originBudget) (originVerdict, error) {
	name := filepath.Base(entries[i])
	if a.tail.walk == nil || a.tail.walkName != name {
		previous, err := a.tailPrevious(name, f)
		if err != nil {
			return originUnfinished, err
		}
		// The next file may be the active one, whose header is not cached:
		// only a sealed file's identity is stable.
		nextName := filepath.Base(entries[i+1])
		var next string
		if i+1 < len(entries)-1 {
			nf, err := a.tailStat(nextName)
			if err != nil {
				return originUnfinished, fmt.Errorf("stat %s: %w", nextName, err)
			}
			if next, err = a.tailPrevious(nextName, nf); err != nil {
				return originUnfinished, err
			}
		} else {
			a.tail.headerReads++
			header, err := localBinlogHeader(filepath.Join(a.cfg.BinlogDir, nextName))
			if err != nil {
				return originUnfinished, fmt.Errorf("read the header of %s: %w", nextName, err)
			}
			next = header.previous
		}
		before, err := internalmysql.ParseGTIDSet(previous)
		if err != nil {
			return originUnfinished, fmt.Errorf("Previous_gtids of %s: %w", name, err)
		}
		after, err := internalmysql.ParseGTIDSet(next)
		if err != nil {
			return originUnfinished, fmt.Errorf("Previous_gtids of %s: %w", nextName, err)
		}
		// A next file that does not continue this one is no proof of what
		// this one logged: walk it.
		if after.Contains(before) && !hasSourceUUID(after.Subtract(before), obs.ServerUUID) {
			return originNone, nil
		}
		a.tail.walk, a.tail.walkName = &originWalk{}, name
		a.tail.walks++
	}
	file, err := os.Open(filepath.Join(a.cfg.BinlogDir, name))
	if err != nil {
		a.tail.walk, a.tail.walkName = nil, ""
		return originUnfinished, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !stableBinlogFile(f.info, opened) {
		a.tail.walk, a.tail.walkName = nil, ""
		return originUnfinished, fmt.Errorf("binlog %s changed while it was examined", name)
	}
	verdict, err := a.tail.walk.Walk(file, opened.Size(), budget)
	if err != nil {
		a.tail.walk, a.tail.walkName = nil, ""
		return originUnfinished, fmt.Errorf("examine %s: %w", name, err)
	}
	if verdict != originUnfinished {
		a.tail.walk, a.tail.walkName = nil, ""
	}
	return verdict, nil
}

// hasSourceUUID reports whether set holds a transaction of the server uuid,
// under its plain or a tagged source identifier.
func hasSourceUUID(set internalmysql.GTIDSet, uuid string) bool {
	for sid, intervals := range set {
		source, _, _ := strings.Cut(sid, ":")
		if len(intervals) > 0 && strings.EqualFold(source, uuid) {
			return true
		}
	}
	return false
}

// unarchivedTail returns the sealed files in (stop, j] that this site's
// manifest does not hold. A file is held when a live or quarantined row has
// its name, size and Previous_gtids (the check the scan uses for old files).
// Two matches are confirmed by content identity, the only thing that tells
// a reused name from the file a row was made of:
//
//   - j, the file that holds the tail's last transactions;
//   - a file that only a retirement record matches. A record keeps the
//     name and Previous_gtids, and compaction keeps it for as long as a
//     local file of that name exists. After a reclone the numbering starts
//     again and the first Previous_gtids are empty again, so the record of
//     an earlier incarnation's file matches the new file of that name.
//
// A file a retirement record matches is hashed once per process, within the
// budget (the scan's identity cache serves when the scan identified the
// file already). While another file is known unarchived the hash is left to
// the upload pass, which identifies every file without a live row itself:
// those files are returned as unconfirmed, and the upload does not wait for
// them. done is false when the budget ran out.
func (a *BinlogArchiver) unarchivedTail(m *Manifest, retired map[binlogIdentity]struct{}, sealed []string, stop, j int, budget *originBudget) (unarchived, unconfirmed []string, done bool, err error) {
	rows := make(map[string][]ManifestEntry)
	for _, row := range m.Files {
		rows[row.Name] = append(rows[row.Name], row)
	}
	for _, q := range m.Quarantined {
		rows[q.Entry.Name] = append(rows[q.Entry.Name], q.Entry)
	}
	records := make(map[string][]string)
	for _, p := range m.Pruned {
		records[p.Name] = append(records[p.Name], p.PreviousGTIDs)
	}
	// sameContent: a row or a retirement record of the name has the file's
	// content identity.
	sameContent := func(name string, f *tailFile) (bool, error) {
		id, err := a.tailContentID(name, f)
		if err != nil {
			return false, err
		}
		return a.candidateSeen(rows[name], id) || a.retiredIdentity(retired, name, id), nil
	}
	var byRecord []string
	for i := stop + 1; i <= j; i++ {
		name := filepath.Base(sealed[i])
		f, err := a.tailStat(name)
		if err != nil {
			return nil, nil, false, fmt.Errorf("stat %s: %w", name, err)
		}
		if f.archived {
			continue
		}
		if len(rows[name]) == 0 && len(records[name]) == 0 {
			unarchived = append(unarchived, name)
			continue
		}
		if !f.havePrevious && budget.spent() {
			return nil, nil, false, nil
		}
		previous, err := a.tailPrevious(name, f)
		if err != nil {
			return nil, nil, false, err
		}
		byRow, record := false, false
		for _, row := range rows[name] {
			byRow = byRow || (row.Size == f.info.Size() && row.PreviousGTIDs == previous)
		}
		for _, p := range records[name] {
			record = record || p == previous
		}
		switch {
		case !byRow && !record:
			unarchived = append(unarchived, name)
		case i == j:
			held, err := sameContent(name, f)
			if err != nil {
				return nil, nil, false, err
			}
			if held {
				f.archived = true
			} else {
				unarchived = append(unarchived, name)
			}
		case byRow:
			f.archived = true
		default:
			byRecord = append(byRecord, name)
		}
	}
	for _, name := range byRecord {
		f := a.tail.files[name]
		if !a.tailContentKnown(name, f) {
			if len(unarchived) > 0 {
				unconfirmed = append(unconfirmed, name)
				continue
			}
			if budget.spent() {
				return nil, nil, false, nil
			}
			budget.bytes -= f.info.Size()
		}
		held, err := sameContent(name, f)
		if err != nil {
			return nil, nil, false, err
		}
		if held {
			f.archived = true
		} else {
			unarchived = append(unarchived, name)
		}
	}
	return unarchived, unconfirmed, true, nil
}

// tailContentKnown reports whether a sealed binlog's content identity is at
// hand without reading the file: the drain or the scan identified this very
// file before.
func (a *BinlogArchiver) tailContentKnown(name string, f *tailFile) bool {
	if f.id != "" {
		return true
	}
	cached, ok := a.localIDs[name]
	return ok && stableBinlogFile(cached.info, f.info)
}

// tailContentID is the content identity of a sealed binlog as the drain saw
// it. It is kept with the file, so a file is hashed once; the scan's
// identity cache is read when it holds this very file, and never written:
// its entries stand for files already reconciled with the manifest.
func (a *BinlogArchiver) tailContentID(name string, f *tailFile) (string, error) {
	if f.id != "" {
		return f.id, nil
	}
	if cached, ok := a.localIDs[name]; ok && stableBinlogFile(cached.info, f.info) {
		f.id = cached.id
		return f.id, nil
	}
	a.tail.hashes++
	id, err := a.stableContentID(name, f)
	if err != nil {
		return "", err
	}
	f.id = id
	return id, nil
}

// stableContentID hashes a sealed binlog for its content identity and
// refuses a file that changed while it was hashed.
func (a *BinlogArchiver) stableContentID(name string, f *tailFile) (string, error) {
	path := filepath.Join(a.cfg.BinlogDir, name)
	id, err := a.fileContentID(path)
	if err != nil {
		return "", fmt.Errorf("identify %s: %w", name, err)
	}
	after, err := os.Stat(path)
	if err != nil || !stableBinlogFile(f.info, after) {
		return "", fmt.Errorf("binlog %s changed while it was identified", name)
	}
	return id, nil
}

// tailRecordGate allows a read-only upload only under a confirmed promotion
// record of the very server incarnation that is being read: the latest
// record of this site's manifest (records are appended in time order) must
// be a promotion record with obs's server_uuid and server_id whose
// gtid_executed the server still contains. A site whose latest record is an
// incarnation reset (it was recloned or reset since it last wrote), or that
// has no record (it never wrote), uploads nothing while read-only: its
// files reach the archive only under the promotion record of a later
// writable episode, as before.
func tailRecordGate(m *Manifest, obs RoleObservation) error {
	n := len(m.Promotions)
	if n == 0 {
		return errors.New("this site's manifest holds no promotion record; a read-only site archives only the tail of a recorded writable episode")
	}
	latest := m.Promotions[n-1]
	at := latest.ObservedAt.UTC().Format(time.RFC3339Nano)
	if latest.Kind != pitrarchive.RecordPromotion {
		return fmt.Errorf("this site's latest record (%q, observed at %s) is not a promotion record: the server changed incarnation since it last wrote, and a read-only site archives only the tail of its latest recorded writable episode", latest.Kind, at)
	}
	if latest.ServerUUID != obs.ServerUUID || latest.ServerID != obs.ServerID {
		return fmt.Errorf("this site's latest promotion record (observed at %s) is of server %s (server_id %d), not of this server %s (server_id %d)",
			at, latest.ServerUUID, latest.ServerID, obs.ServerUUID, obs.ServerID)
	}
	if !sameIncarnation(RoleObservation{ServerUUID: latest.ServerUUID, ServerID: latest.ServerID, GTIDExecuted: latest.GTIDExecuted}, obs) {
		return fmt.Errorf("the server's gtid_executed no longer contains that of this site's latest promotion record (observed at %s): it is another incarnation", at)
	}
	return nil
}
