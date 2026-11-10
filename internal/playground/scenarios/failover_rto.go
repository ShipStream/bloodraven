package scenarios

// Measurement of an emergency failover under write load (#229): how long
// writes are down as a client of the new primary sees it, and what the
// promotion's FLUSH BINARY LOGS costs of that. Used by
// 01-clean-primary-kill.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// The operator's log messages for its best-effort binary log rotations
// (#229, #264) and for the pre-reclone archive-tail wait. They are in the
// log schema (site/content/docs/8.observability/7.log-schema.md); the
// operator does not export them, so TestOperatorLogMirrors pins each to the
// controller's source.
const (
	logPromotionBinlogRotated        = "promotion binlog rotated"
	logPromotionBinlogRotationFailed = "promotion binlog rotation failed"
	logOutgoingBinlogRotated         = "outgoing primary binlog rotated"
	logOutgoingBinlogRotationFailed  = "outgoing primary binlog rotation failed"
	logPreRecloneTailArchived        = "pre-reclone archive tail archived"
	logPreRecloneTailNotArchived     = "pre-reclone archive tail not archived"
)

// logClockSlack is subtracted from a runner-clock instant before it filters
// log lines, which carry the node's (kubelet's) timestamps.
const logClockSlack = 2 * time.Second

// logLinesSince returns the tailer's buffered lines at or after since.
func logLinesSince(t *pglogs.Tailer, since time.Time) []pglogs.Match {
	return linesSince(t.Snapshot(), since)
}

func linesSince(lines []pglogs.Match, since time.Time) []pglogs.Match {
	var out []pglogs.Match
	for _, m := range lines {
		if !since.IsZero() && m.Time.Before(since) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// logSource reads a component's log lines of interest as they are now.
type logSource func() []pglogs.Match

// operatorLogSince is the operator tailer's lines at or after since.
func operatorLogSince(t *pglogs.Tailer, since time.Time) logSource {
	return func() []pglogs.Match { return logLinesSince(t, since) }
}

// binlogRotationLog is one rotation line of the operator's log: the INFO
// line of an acknowledged FLUSH BINARY LOGS or the WARN line of one that
// failed or timed out.
type binlogRotationLog struct {
	Site string
	// Duration is the line's durationMs; DurationKnown is false when the
	// field is missing or not a whole number of milliseconds.
	Duration      time.Duration
	DurationKnown bool
	Failed        bool
	TimedOut      bool
	Error         string
	Line          string
}

// parseBinlogRotation reads line as the rotation whose INFO message is done
// and whose WARN message is failed, in the operator's JSON format or slog
// text. ok is false for any other line.
func parseBinlogRotation(line, done, failed string) (binlogRotationLog, bool) {
	// Most lines are not rotations: skip the parse.
	if !strings.Contains(line, done) && !strings.Contains(line, failed) {
		return binlogRotationLog{}, false
	}
	fields, ok := pglogs.Fields(line)
	if !ok || (fields["msg"] != done && fields["msg"] != failed) {
		return binlogRotationLog{}, false
	}
	rec := binlogRotationLog{Site: fields["site"], Failed: fields["msg"] == failed, Error: fields["error"], Line: line}
	rec.TimedOut = fields["timedOut"] == "true"
	if ms, err := strconv.ParseInt(fields["durationMs"], 10, 64); err == nil && ms >= 0 {
		rec.Duration, rec.DurationKnown = time.Duration(ms)*time.Millisecond, true
	}
	return rec, true
}

// rotationPollInterval is how often waitBinlogRotation rereads the log.
const rotationPollInterval = 500 * time.Millisecond

// errNoRotationLine: the operator has not logged the rotation yet.
var errNoRotationLine = errors.New("no rotation line yet")

// assessBinlogRotation judges the rotation (done/failed messages) the
// operator logged for site among lines: it must have been acknowledged
// (the INFO line, and no WARN line) in less than bound. It returns the
// acknowledged rotation, errNoRotationLine while neither line is there,
// and otherwise an error that prints the offending line.
func assessBinlogRotation(lines []pglogs.Match, site, done, failed string, bound time.Duration) (binlogRotationLog, error) {
	var acked *binlogRotationLog
	for _, m := range lines {
		rec, ok := parseBinlogRotation(m.Line, done, failed)
		if !ok || rec.Site != site {
			continue
		}
		if rec.Failed {
			return rec, fmt.Errorf("the operator's FLUSH BINARY LOGS on %s was not acknowledged (timedOut=%v error=%q); the operator logged: %s",
				site, rec.TimedOut, rec.Error, rec.Line)
		}
		if acked == nil {
			r := rec
			acked = &r
		}
	}
	if acked == nil {
		return binlogRotationLog{}, errNoRotationLine
	}
	if !acked.DurationKnown {
		return *acked, fmt.Errorf("the operator's %q line for %s carries no usable durationMs: %s", done, site, acked.Line)
	}
	if acked.Duration >= bound {
		return *acked, fmt.Errorf("the operator's FLUSH BINARY LOGS on %s took %s, not under its %s bound: %s", site, acked.Duration, bound, acked.Line)
	}
	return *acked, nil
}

// waitBinlogRotation polls the operator's log until assessBinlogRotation has
// a verdict on site's rotation, for at most timeout.
func waitBinlogRotation(ctx context.Context, lines logSource, site, done, failed string, bound, timeout time.Duration) (binlogRotationLog, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(rotationPollInterval)
	defer tick.Stop()
	for {
		rec, err := assessBinlogRotation(lines(), site, done, failed, bound)
		if !errors.Is(err, errNoRotationLine) {
			return rec, err
		}
		select {
		case <-waitCtx.Done():
			return binlogRotationLog{}, fmt.Errorf("the operator logged neither %q nor %q for site %s within %s of the failover", done, failed, site, timeout)
		case <-tick.C:
		}
	}
}

// writeProbeTarget is what a writeProbe needs of the site it probes.
type writeProbeTarget interface {
	// ReadOnly reports whether the server refuses application writes
	// (read_only or super_read_only).
	ReadOnly(ctx context.Context) (bool, error)
	// Write commits one row.
	Write(ctx context.Context) error
}

// writeProbe measures when a site starts accepting writes. Every interval
// it reads the site's read-only flags and, once both are off, commits one
// row; it records when that first write was acknowledged and stops. It
// never sends a write to a read-only server: its account is root, which
// read_only alone does not stop, and a write on a replica would be an
// errant transaction.
//
// It holds its own connection, opened before the fault, so the measurement
// does not include connection setup.
type writeProbe struct {
	site   string
	target writeProbeTarget

	mu       sync.Mutex
	firstAck time.Time
	readOnly int
	failed   int
	lastErr  string

	cancel   context.CancelFunc
	done     chan struct{}
	closer   func()
	stopOnce sync.Once
}

// writeProbeAttemptTimeout bounds one read or write of the probe, so a hung
// connection costs one attempt, not the measurement.
const writeProbeAttemptTimeout = 3 * time.Second

// runWriteProbe starts the probe loop against target.
func runWriteProbe(site string, target writeProbeTarget, interval time.Duration, closer func()) *writeProbe {
	runCtx, cancel := context.WithCancel(context.Background())
	p := &writeProbe{site: site, target: target, cancel: cancel, done: make(chan struct{}), closer: closer}
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if p.attempt(runCtx) {
				return
			}
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return p
}

// attempt is one probe round; true once the first write was acknowledged.
func (p *writeProbe) attempt(ctx context.Context) bool {
	attemptCtx, cancel := context.WithTimeout(ctx, writeProbeAttemptTimeout)
	defer cancel()
	readOnly, err := p.target.ReadOnly(attemptCtx)
	if err == nil && !readOnly {
		err = p.target.Write(attemptCtx)
	}
	acked := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case err != nil:
		p.failed++
		p.lastErr = err.Error()
		return false
	case readOnly:
		p.readOnly++
		return false
	}
	p.firstAck = acked
	return true
}

// firstAckAt is when the site acknowledged the probe's first write (the
// runner's clock); ok is false while it has not.
func (p *writeProbe) firstAckAt() (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstAck, !p.firstAck.IsZero()
}

func (p *writeProbe) summary() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := fmt.Sprintf("write probe on %s: read-only rounds=%d failed rounds=%d acked=%v", p.site, p.readOnly, p.failed, !p.firstAck.IsZero())
	if p.lastErr != "" {
		s += fmt.Sprintf(" lastErr=%q", p.lastErr)
	}
	return s
}

// stop ends the probe and closes its connection. Safe to call twice, and on
// a probe that was never started.
func (p *writeProbe) stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		p.cancel()
		<-p.done
		if p.closer != nil {
			p.closer()
		}
	})
}

// sqlWriteProbeTarget probes one site over SQL, inserting into table.
type sqlWriteProbeTarget struct {
	client *pgmysql.SiteClient
	table  string
}

func (t sqlWriteProbeTarget) ReadOnly(ctx context.Context) (bool, error) {
	ro, err := t.client.ScalarInt(ctx, "SELECT @@global.read_only OR @@global.super_read_only")
	return ro != 0, err
}

func (t sqlWriteProbeTarget) Write(ctx context.Context) error {
	_, err := t.client.Exec(ctx, "INSERT INTO "+t.table+" () VALUES ()")
	return err
}

// writeProbeTableDDL creates the probe's table; it is run on the primary and
// replicates to the site the probe watches.
func writeProbeTableDDL(db, table string) string {
	return "CREATE DATABASE IF NOT EXISTS " + quoteSQLIdentifier(db) + "; " +
		"CREATE TABLE IF NOT EXISTS " + quoteSQLIdentifier(db) + "." + quoteSQLIdentifier(table) +
		" (id BIGINT AUTO_INCREMENT PRIMARY KEY, written_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
}

// startWriteProbe starts a probe on site, which must be a read-only replica
// that already has db.table (writeProbeTableDDL, replicated).
func startWriteProbe(ctx context.Context, env *runner.Env, site, db, table string, interval time.Duration) (*writeProbe, error) {
	client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return nil, fmt.Errorf("open write-probe connection to %s: %w", site, err)
	}
	target := sqlWriteProbeTarget{client: client, table: quoteSQLIdentifier(db) + "." + quoteSQLIdentifier(table)}
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := client.ScalarInt(checkCtx, "SELECT COUNT(*) FROM "+target.table); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("write-probe table %s on %s: %w", target.table, site, err)
	}
	readOnly, err := target.ReadOnly(checkCtx)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("read the read-only flags of %s: %w", site, err)
	}
	if !readOnly {
		_ = client.Close()
		return nil, fmt.Errorf("site %s is writable; the write probe measures a read-only site becoming the primary", site)
	}
	return runWriteProbe(site, target, interval, func() { _ = client.Close() }), nil
}

// failoverRTO is one measured emergency failover under write load.
type failoverRTO struct {
	Old, New string
	// FaultAt is when the scenario injected the fault; LastOldAck when the
	// old primary last acknowledged a write of the load; FirstNewAck when
	// the new primary first acknowledged a write. All on the runner's clock.
	FaultAt, LastOldAck, FirstNewAck time.Time
	// LoadAcked is how many writes the old primary acknowledged.
	LoadAcked int
	// Rotation is the promotion's FLUSH BINARY LOGS on the new primary, and
	// RotationBound the operator's bound on it.
	Rotation      binlogRotationLog
	RotationBound time.Duration
}

// rto is the time from the fault to the first acknowledged write on the new
// primary: the write outage a client of the new primary sees, measured at
// its SQL port (DNS and Service switching are not in it).
func (r failoverRTO) rto() time.Duration { return r.FirstNewAck.Sub(r.FaultAt) }

// outage is the time between the last write the old primary acknowledged
// and the first the new primary did.
func (r failoverRTO) outage() time.Duration { return r.FirstNewAck.Sub(r.LastOldAck) }

// check fails a measurement that is incomplete or outside budget, the
// failover budget the scenario already had.
func (r failoverRTO) check(budget time.Duration) error {
	switch {
	case r.LoadAcked == 0 || r.LastOldAck.IsZero():
		return fmt.Errorf("the load on %s acknowledged no write before the fault; the failover was not under load", r.Old)
	case r.FirstNewAck.IsZero():
		return fmt.Errorf("the new primary %s acknowledged no write", r.New)
	case !r.FirstNewAck.After(r.FaultAt):
		return fmt.Errorf("site %s acknowledged a write at %s, before the fault at %s: it was not a read-only replica",
			r.New, r.FirstNewAck.Format(time.RFC3339Nano), r.FaultAt.Format(time.RFC3339Nano))
	case r.rto() > budget:
		return fmt.Errorf("the first write on the new primary %s came %s after the fault, over the %s failover budget", r.New, roundMs(r.rto()), budget)
	}
	return nil
}

// String is the measurement as the scenario reports it.
func (r failoverRTO) String() string {
	rto := r.rto()
	return fmt.Sprintf("failover under load %s->%s: rto=%s (fault to first acknowledged write on %s), write outage=%s (last ack on %s to first ack on %s), "+
		"promotion FLUSH BINARY LOGS=%s, load acked=%d before the outage",
		r.Old, r.New, roundMs(rto), r.New, roundMs(r.outage()), r.Old, r.New, r.rotationReport(rto), r.LoadAcked)
}

// rotationReport words the promotion's rotation: its duration, bound and
// share of the RTO, or that it was not acknowledged.
func (r failoverRTO) rotationReport(rto time.Duration) string {
	switch {
	case r.Rotation.Failed:
		return fmt.Sprintf("not acknowledged after %dms (timedOut=%v, bound %s)", r.Rotation.Duration.Milliseconds(), r.Rotation.TimedOut, r.RotationBound)
	case !r.Rotation.DurationKnown:
		return fmt.Sprintf("not measured (bound %s)", r.RotationBound)
	}
	share := 0.0
	if rto > 0 {
		share = 100 * float64(r.Rotation.Duration) / float64(rto)
	}
	return fmt.Sprintf("%dms (bound %s, %.2f%% of rto)", r.Rotation.Duration.Milliseconds(), r.RotationBound, share)
}

func roundMs(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
