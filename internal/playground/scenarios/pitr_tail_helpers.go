package scenarios

// Helpers for the archived binary log tail (#264) in the cross-site PITR
// scenarios.
//
// A site's sidecar archives, while the site is read-only, the tail of the
// site's last writable era: the sealed binary logs up to the last one that
// holds a transaction the server originated. Three things seal a tail no
// scenario flush sealed:
//
//   - a planned failover rotates the fenced primary's binary log, so the era
//     it wrote since its last flush is a sealed file its sidecar uploads
//     (pitrPlannedFailover asserts that with a pitrTailWatch);
//   - a primary that died and comes back starts a new binary log, which
//     seals the one it was writing (s70 asserts its upload the same way);
//   - a reclone rotates its recipient's binary log and waits, up to two
//     minutes, until the recipient's sidecar reports the tail archived
//     before CLONE wipes it; when that is not confirmed the group gets a
//     Warning Event PITRArchiveTailNotArchived (pitrRecloneSite asserts the
//     confirmation and the Event's absence).

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brmysql "github.com/shipstream/bloodraven/internal/mysql"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

const (
	// pitrTailWaitTimeout bounds the wait for a read-only site's sidecar to
	// archive a tail once it is sealed: one scan (the sidecar scans on the
	// rotation itself, else on its periodic pass) and the upload of one
	// small file. It is a flushing fence's upload wait.
	pitrTailWaitTimeout = 3 * time.Minute
	// pitrPreRecloneTailWait mirrors the operator's
	// preRecloneArchiveTailTimeout (internal/controller/
	// reclone_archive_tail.go): with PITR enabled a reclone starts only
	// once its recipient's tail is archived, or after this long. A reclone
	// wait adds it to its clone budget. TestOperatorLogMirrors pins it.
	pitrPreRecloneTailWait = 2 * time.Minute
	// pitrPreRecloneLogWait bounds the wait, after a reclone completed, for
	// the operator's line about its archive-tail wait to reach the log
	// tailer. The line is written before the clone starts.
	pitrPreRecloneLogWait = 30 * time.Second
)

// pitrTailArchived is the predicate of a tail wait. sealed is the binary
// log the server was writing before the rotation (the operator's, or a
// restart's). The tail is archived once the sidecar reports it Drained from
// a scan that saw the rotation (its active file is no longer sealed): a
// Drained from before the rotation says nothing about the file the rotation
// sealed. With wantRow the site's manifest must also hold a new row for
// sealed (rowArchived): the tail held transactions the site originated, so
// it had to be uploaded.
func pitrTailArchived(st *pgsidecar.ArchiverStatusResponse, sealed string, wantRow, rowArchived bool) (bool, string) {
	if st == nil {
		return false, "no archiver status"
	}
	summary := fmt.Sprintf("row for %s archived=%v; %s", sealed, rowArchived, archiverSummary(st))
	t := st.Tail
	switch {
	case t == nil:
		return false, "the sidecar reports no tail block (it has not read the binlog index yet, or predates the field): " + summary
	case t.ActiveFile == "" || t.ActiveFile == sealed:
		return false, "the sidecar's last scan did not see " + sealed + " sealed (it was still the active binary log, or the scan reported none): " + summary
	case t.State != brmysql.ArchiverTailDrained:
		return false, "the tail is " + t.State + ": " + summary
	case wantRow && !rowArchived:
		return false, "the tail is Drained but the manifest holds no new row for " + sealed + ": " + summary
	}
	return true, summary
}

// pitrTailWatch follows one site's binary log tail across the rotation that
// seals it: it records, before the rotation, the file the server is writing
// and the site's manifest rows, and later waits until the sidecar archived
// that file's era. Its fields are exported for the stash (a watch can span
// steps).
type pitrTailWatch struct {
	Site string
	// Key is the bucket key of the site's manifest; Before its rows when
	// the watch started.
	Key    string
	Before []pitrManifestRow `json:",omitempty"`
	// File is the binary log the server was writing when the watch
	// started: the tail's last file once something seals it.
	File string
}

// pitrStartTailWatch starts a watch on site, which must be the active
// primary: call it before the step that ends its writable era (the planned
// failover that fences it, or its loss).
func pitrStartTailWatch(ctx context.Context, env *runner.Env, site string) (*pitrTailWatch, error) {
	probe, err := pgsidecar.Open(ctx, env.Kube, env.Namespace, env.FG, site)
	if err != nil {
		return nil, fmt.Errorf("open sidecar %s: %w", site, err)
	}
	st, err := probe.ArchiverStatus(ctx)
	probe.Close()
	if err != nil {
		return nil, fmt.Errorf("read archiver status on %s before its tail is sealed: %w", site, err)
	}
	if st.ManifestPrefix == "" {
		return nil, fmt.Errorf("archiver on %s reports no manifest prefix: %s", site, archiverSummary(st))
	}
	manifestSite := st.Site
	if manifestSite == "" {
		manifestSite = site
	}
	w := &pitrTailWatch{Site: site, Key: pitrManifestKey(st.ManifestPrefix, manifestSite)}
	if w.Before, err = pitrReadManifestRows(ctx, env, w.Key); err != nil {
		return nil, err
	}
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return nil, fmt.Errorf("open %s for its active binlog: %w", site, err)
	}
	defer c.Close()
	if w.File, err = pitrCurrentBinlog(ctx, c.DB); err != nil {
		return nil, fmt.Errorf("read the active binlog on %s: %w", site, err)
	}
	return w, nil
}

// pitrStashTailWatch and pitrFetchTailWatch keep a watch across steps.
func pitrStashTailWatch(ctx context.Context, env *runner.Env, key string, w *pitrTailWatch) error {
	body, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return ctxStash(ctx, env, "tailWatch:"+key, string(body))
}

func pitrFetchTailWatch(env *runner.Env, key string) (*pitrTailWatch, error) {
	raw := ctxFetch(env, "tailWatch:"+key)
	if raw == "" {
		return nil, fmt.Errorf("tail watch %q was not recorded", key)
	}
	var w pitrTailWatch
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return nil, fmt.Errorf("decode tail watch %q: %w", key, err)
	}
	return &w, nil
}

// wait waits until the site's sidecar archived the tail (pitrTailArchived).
// since is when the step that seals the tail was requested; a timeout
// quotes the operator's failed-rotation line when it logged one for the
// site after it.
func (w *pitrTailWatch) wait(ctx context.Context, env *runner.Env, wantRow bool, since time.Time) error {
	waitCtx, cancel := context.WithTimeout(ctx, pitrTailWaitTimeout)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var last string
	for {
		done := false
		rows, err := pitrReadManifestRows(waitCtx, env, w.Key)
		if err != nil {
			last = err.Error()
		} else if probe, err := pgsidecar.Open(waitCtx, env.Kube, env.Namespace, env.FG, w.Site); err != nil {
			last = err.Error()
		} else {
			st, err := probe.ArchiverStatus(waitCtx)
			probe.Close()
			if err != nil {
				last = err.Error()
			} else {
				done, last = pitrTailArchived(st, w.File, wantRow, pitrSealedFileArchived(w.Before, rows, w.File))
			}
		}
		if done {
			env.Capture.Note(fmt.Sprintf("binlog tail of %s archived once %s was sealed (manifest row required=%v): %s", w.Site, w.File, wantRow, last))
			return nil
		}
		select {
		case <-waitCtx.Done():
			err := fmt.Errorf("site %s did not archive its binlog tail (through %s) within %s: %w (last: %s)", w.Site, w.File, pitrTailWaitTimeout, waitCtx.Err(), last)
			if tail, lerr := env.Logs("operator"); lerr == nil {
				if line := pitrRotationFailureLine(logLinesSince(tail, since.Add(-logClockSlack)), w.Site, logOutgoingBinlogRotated, logOutgoingBinlogRotationFailed); line != "" {
					err = fmt.Errorf("%w; the operator logged: %s", err, line)
				}
			}
			return err
		case <-tick.C:
		}
	}
}

// pitrRotationFailureLine returns the operator's WARN line of a failed
// rotation (done/failed messages) of site among lines, or "".
func pitrRotationFailureLine(lines []pglogs.Match, site, done, failed string) string {
	for _, m := range lines {
		if rec, ok := parseBinlogRotation(m.Line, done, failed); ok && rec.Failed && rec.Site == site {
			return rec.Line
		}
	}
	return ""
}

// pitrPreRecloneTail is what the operator logged about one reclone's
// archive-tail wait.
type pitrPreRecloneTail struct {
	// Archived: the operator confirmed the recipient's tail archived
	// before it started the clone, after Waited.
	Archived bool
	Waited   time.Duration
	// NotArchived is the operator's WARN line when it started the clone
	// without that confirmation, else "".
	NotArchived string
}

// pitrPreRecloneTailOutcome reads the outcome of site's pre-reclone
// archive-tail wait from the operator's lines (JSON or slog text).
func pitrPreRecloneTailOutcome(lines []pglogs.Match, site string) pitrPreRecloneTail {
	var out pitrPreRecloneTail
	for _, m := range lines {
		if !strings.Contains(m.Line, logPreRecloneTailArchived) && !strings.Contains(m.Line, logPreRecloneTailNotArchived) {
			continue
		}
		fields, ok := pglogs.Fields(m.Line)
		if !ok || fields["site"] != site {
			continue
		}
		switch fields["msg"] {
		case logPreRecloneTailArchived:
			out.Archived = true
			if ms, err := strconv.ParseInt(fields["waitedMs"], 10, 64); err == nil {
				out.Waited = time.Duration(ms) * time.Millisecond
			}
		case logPreRecloneTailNotArchived:
			if out.NotArchived == "" {
				out.NotArchived = m.Line
			}
		}
	}
	return out
}

// pitrNewTailNotArchived finds a PITRArchiveTailNotArchived Event for site
// that is not in before (keyed by Event UID, with its count): the operator
// recloned site although its binlog tail was not confirmed archived.
func pitrNewTailNotArchived(before map[string]int32, events []corev1.Event, group, site string) (corev1.Event, bool) {
	for _, ev := range events {
		if ev.InvolvedObject.Kind != "MysqlFailoverGroup" || ev.InvolvedObject.Name != group || ev.Reason != v1alpha1.EventPITRArchiveTailNotArchived {
			continue
		}
		// "site <name> is being recloned although …" (the operator's
		// emitPITRArchiveTailNotArchived).
		if !strings.Contains(ev.Message, "site "+site+" ") {
			continue
		}
		if n, seen := before[string(ev.UID)]; seen && n >= ev.Count {
			continue
		}
		return ev, true
	}
	return corev1.Event{}, false
}

// pitrRecloneTailCheck is one look at a running reclone's archive-tail
// wait: an error as soon as the operator announced, by Event or log line,
// that it clones site without the tail confirmed archived.
func pitrRecloneTailCheck(before map[string]int32, events []corev1.Event, lines []pglogs.Match, group, site string) (pitrPreRecloneTail, error) {
	if ev, found := pitrNewTailNotArchived(before, events, group, site); found {
		return pitrPreRecloneTail{}, fmt.Errorf("the operator recloned %s without its binlog tail confirmed archived (%s Event): %s", site, ev.Reason, ev.Message)
	}
	out := pitrPreRecloneTailOutcome(lines, site)
	if out.NotArchived != "" {
		return out, fmt.Errorf("the operator recloned %s without its binlog tail confirmed archived; it logged: %s", site, out.NotArchived)
	}
	return out, nil
}

// pitrAssertRecloneTailArchived is the check after a reclone of site
// completed with PITR enabled: no PITRArchiveTailNotArchived Event for it
// since before, and the operator's "pre-reclone archive tail archived" line
// (the clone started only after the recipient's sidecar reported its tail
// Drained from a scan that saw the operator's rotation). operatorLog reads
// the operator's lines since the reclone was requested; logWait bounds the
// wait for the line to arrive.
func pitrAssertRecloneTailArchived(ctx context.Context, env *runner.Env, operatorLog logSource, before map[string]int32, site string, logWait time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, logWait)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		_, events, err := pitrRecloneEventSnapshot(waitCtx, env)
		if err != nil {
			return fmt.Errorf("list events after the reclone of %s: %w", site, err)
		}
		out, err := pitrRecloneTailCheck(before, events, operatorLog(), env.FG, site)
		if err != nil {
			return err
		}
		if out.Archived {
			env.Capture.Note(fmt.Sprintf("reclone of %s: the operator confirmed its binlog tail archived before the clone (waited %s); no %s Event", site, out.Waited, v1alpha1.EventPITRArchiveTailNotArchived))
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("the operator did not log %q for site %s since the reclone was requested: with PITR enabled a reclone confirms its recipient's binlog tail archived before the clone",
				logPreRecloneTailArchived, site)
		case <-tick.C:
		}
	}
}
