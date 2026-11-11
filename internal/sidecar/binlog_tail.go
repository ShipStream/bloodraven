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
	// headerReads and walks count the file headers read and the origin
	// walks started, for the tests that pin a file is examined once.
	// budget replaces newOriginBudget in tests.
	headerReads int
	walks       int
	budget      func() *originBudget
}

// tailUpload is set while a read-only site's drain runs scanFiles. It binds
// that pass to what the drain decided on: the role observation the gates
// were checked against (episodeCurrent refuses manifest saves once the
// timeline moved on) and the files as they were when the index was read
// (archivePinned uploads no other bytes under their names).
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
// mutex. A scan that could not read the index keeps the names of the last
// one that did (never as Drained), and publishes nothing before any scan
// read it.
func (a *BinlogArchiver) publishTail(t tailScan) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := &internalmysql.ArchiverTailStatus{State: t.state, PendingFiles: t.pending, Reason: t.reason}
	if !t.indexRead {
		if a.tailStatus == nil {
			return
		}
		st.ActiveFile, st.NewestSealedFile = a.tailStatus.ActiveFile, a.tailStatus.NewestSealedFile
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
// and reported the server identity.
func (t *roleTimeline) readOnlyIdentity() (RoleObservation, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != roleReadOnly || t.readOnly == nil || t.readOnly.ServerUUID == "" {
		return RoleObservation{}, false
	}
	return *t.readOnly, true
}

// check refuses the upload of a sealed binlog that was replaced between the
// drain's index read and the open (a reclone or a reset restarts the
// numbering, so the name may now be a file of another incarnation).
func (u *tailUpload) check(name string, opened os.FileInfo) error {
	if want := u.files[name]; want == nil || !stableBinlogFile(want, opened) {
		return fmt.Errorf("binlog %s was replaced after its index was read; it is not this server's sealed tail", name)
	}
	return nil
}

// drainTail is a scan's work on a read-only site: it archives the
// writable-era tail (see the top of this file) and returns the tail block.
// resetErr is recordResets' result.
//
// A read-only site with nothing to drain reports what it always did:
// backlog 0 and no error. Nothing that only decides whether there is a tail
// (the index, the file headers, the origin walk, the gates) is reported as
// an archiver error: it blocks the tail with a reason and is logged once.
// Errors of the uploads themselves (the manifest, the store, a file) are
// archiver errors, as on a writable site.
func (a *BinlogArchiver) drainTail(ctx context.Context, resetErr error) tailScan {
	result, reported := a.drainTailFiles(ctx, resetErr)
	if result.state != internalmysql.ArchiverTailBlocked || reported {
		a.tail.blockedReason = ""
	} else if result.reason != a.tail.blockedReason {
		a.tail.blockedReason = result.reason
		a.logger.Warn("binlog tail not archived while read-only", "reason", result.reason, "pendingFiles", result.pending)
	}
	return result
}

// drainTailFiles does drainTail's work. reported: the result's error is
// already the archiver's LastError (and logged).
func (a *BinlogArchiver) drainTailFiles(ctx context.Context, resetErr error) (result tailScan, reported bool) {
	// Until uploads start this is a standby with nothing to report.
	a.setBacklog(0)
	a.clearScanError()
	entries, err := readBinlogIndex(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex))
	if err != nil {
		return tailScan{state: internalmysql.ArchiverTailBlocked, reason: fmt.Sprintf("read index: %v", err)}, false
	}
	if len(entries) < 2 {
		return tailDrained(entries), false
	}
	sealed := entries[:len(entries)-1]
	a.pruneTailFiles(sealed)
	obs, ok := a.roles.readOnlyIdentity()
	if !ok {
		return tailBlocked(entries, 0, "the tail cannot be told from replica copies: "+errRoleProbeUnsupported.Error()), false
	}
	j, stop, done, err := a.findTail(entries, obs, a.tailBudget())
	if err != nil {
		return tailBlocked(entries, 0, err.Error()), false
	}
	if !done {
		return tailPending(entries, 0), false
	}
	if j < 0 {
		return tailDrained(entries), false
	}
	if a.session == nil || !a.session.Verified() {
		return tailBlocked(entries, 0, "the archive session is not verified yet (the archive descriptor is awaited); the tail through "+filepath.Base(sealed[j])+" waits for it"), false
	}
	manifest, err := a.loadManifest(ctx)
	if err != nil {
		a.incUploadFailures()
		msg := wrapKeepToken("load manifest", err).Error()
		a.setError(msg)
		a.logger.Warn("load manifest", "error", err)
		return tailBlocked(entries, 0, msg), true
	}
	retired := a.retiredBinlogIdentities(manifest)
	// A budget of its own: a slow manifest read must not starve it.
	unarchived, done, err := a.unarchivedTail(manifest, retired, sealed, stop, j, a.tailBudget())
	if err != nil {
		return tailBlocked(entries, 0, err.Error()), false
	}
	if !done {
		return tailPending(entries, 0), false
	}
	last := filepath.Base(sealed[j])
	if len(unarchived) == 0 {
		a.tail.through, a.tail.throughInfo = last, a.tail.files[last].info
		return tailDrained(entries), false
	}
	pending := int64(len(unarchived))
	// The gates of a read-only upload. The session is verified and the
	// scan returned before this on a history-integrity error.
	if resetErr != nil {
		return tailBlocked(entries, pending, resetErr.Error()), false
	}
	if err := tailRecordGate(manifest, obs); err != nil {
		return tailBlocked(entries, pending, err.Error()), false
	}
	upload := &tailUpload{obs: obs, files: make(map[string]os.FileInfo, j+1)}
	for _, entry := range sealed[:j+1] {
		name := filepath.Base(entry)
		f, err := a.tailStat(name)
		if err != nil {
			return tailBlocked(entries, pending, fmt.Sprintf("stat %s: %v", name, err)), false
		}
		upload.files[name] = f.info
	}
	if err := a.roles.tailCurrent(obs); err != nil {
		return tailBlocked(entries, pending, err.Error()), false
	}
	a.tailUpload = upload
	archived, _, hadError := a.scanFiles(ctx, manifest, sealed[:j+1], retired)
	a.tailUpload = nil

	pending = 0
	for _, name := range unarchived {
		if a.scanSettled[name] {
			a.tail.files[name].archived = true
		} else {
			pending++
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
		a.mu.Lock()
		msg := a.lastError
		a.mu.Unlock()
		return tailBlocked(entries, pending, msg), true
	}
	a.clearScanError()
	if pending > 0 {
		return tailPending(entries, pending), false
	}
	a.tail.through, a.tail.throughInfo = last, a.tail.files[last].info
	return tailDrained(entries), false
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

// tailStat stats a sealed binlog and returns what is remembered about it,
// starting over when the file changed.
func (a *BinlogArchiver) tailStat(name string) (*tailFile, error) {
	info, err := os.Stat(filepath.Join(a.cfg.BinlogDir, name))
	if err != nil {
		return nil, err
	}
	f := a.tail.files[name]
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
// its name, size and Previous_gtids (the check the scan uses for old files),
// or a retirement record its name and Previous_gtids. For j, the file that
// holds the tail's last transactions, a matching live row is confirmed by
// content identity, so a reused name never passes for it. done is false
// when the budget ran out.
func (a *BinlogArchiver) unarchivedTail(m *Manifest, retired map[binlogIdentity]struct{}, sealed []string, stop, j int, budget *originBudget) (unarchived []string, done bool, err error) {
	rows := make(map[string][]ManifestEntry)
	for _, row := range m.Files {
		rows[row.Name] = append(rows[row.Name], row)
	}
	for _, q := range m.Quarantined {
		rows[q.Entry.Name] = append(rows[q.Entry.Name], q.Entry)
	}
	pruned := make(map[string][]string)
	for _, p := range m.Pruned {
		pruned[p.Name] = append(pruned[p.Name], p.PreviousGTIDs)
	}
	for i := stop + 1; i <= j; i++ {
		name := filepath.Base(sealed[i])
		f, err := a.tailStat(name)
		if err != nil {
			return nil, false, fmt.Errorf("stat %s: %w", name, err)
		}
		if f.archived {
			continue
		}
		if len(rows[name]) == 0 && len(pruned[name]) == 0 {
			unarchived = append(unarchived, name)
			continue
		}
		if !f.havePrevious && budget.spent() {
			return nil, false, nil
		}
		previous, err := a.tailPrevious(name, f)
		if err != nil {
			return nil, false, err
		}
		held := false
		for _, row := range rows[name] {
			held = held || (row.Size == f.info.Size() && row.PreviousGTIDs == previous)
		}
		for _, p := range pruned[name] {
			held = held || p == previous
		}
		if held && i == j {
			id, err := a.stableContentID(name, f)
			if err != nil {
				return nil, false, err
			}
			held = a.candidateSeen(rows[name], id) || a.retiredIdentity(retired, name, id)
		}
		if !held {
			unarchived = append(unarchived, name)
			continue
		}
		f.archived = true
	}
	return unarchived, true, nil
}

// stableContentID is the content identity of a sealed binlog that did not
// change while it was hashed. It does not touch the scan's identity cache,
// whose entries stand for files already reconciled with the manifest.
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
