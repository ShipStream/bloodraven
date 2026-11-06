package dst

import (
	"context"
	"errors"
	"strings"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// simSession is the simulated mysql.Session: one connection pinned to the
// server incarnation the site had at pin time.
//
// Every call checks, under the cluster lock, that the site still runs that
// incarnation; once a crash, restart or pod replacement has moved it, every
// call fails with errSessionLost and the session never redials, however the
// site's address resolves now. That is the property the planned failover's
// proofs rest on (§15.1 "Pinned sim sessions"): a session that proved
// something about a server can only change that same server.
//
// Pooled simChecker calls follow the model's other rules unchanged: they
// reach whichever server the site runs now.
//
// A transport failure on the connection (a statement that timed out,
// applied or not, or a call that found the server gone or unreachable)
// breaks it, as go-sql-driver/mysql discards a connection after a read
// timeout: every later call fails with errSessionLost (K079). A semantic
// MySQL rejection leaves the connection usable.
type simSession struct {
	m      *simChecker // pinned copy
	closed bool
	broken bool
}

var _ mysql.Session = (*simSession)(nil)

// errSessionClosed mirrors database/sql's sql.ErrConnDone text.
var errSessionClosed = errors.New("sql: connection is already closed")

func (s *simSession) open() error {
	if s.closed {
		return errSessionClosed
	}
	if s.broken {
		return errSessionLost
	}
	return nil
}

// note records a transport failure (see simSession) and returns err.
func (s *simSession) note(err error) error {
	if transportErr(err) {
		s.broken = true
	}
	return err
}

// transportErr reports whether err broke the connection rather than being a
// MySQL statement error.
func transportErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errNotApplied) || errors.Is(err, errAmbiguous) || errors.Is(err, errSessionLost) ||
		errors.Is(err, errOperatorDead) || strings.HasPrefix(err.Error(), "dial tcp ")
}

func (s *simSession) StopReplica(ctx context.Context) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.note(s.m.StopReplica(ctx))
}

func (s *simSession) ResetReplicaAll(ctx context.Context) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.note(s.m.ResetReplicaAll(ctx))
}

func (s *simSession) GetGtidExecuted(ctx context.Context) (string, error) {
	if err := s.open(); err != nil {
		return "", err
	}
	v, err := s.m.GetGtidExecuted(ctx)
	return v, s.note(err)
}

func (s *simSession) SetSuperReadOnly(ctx context.Context, on bool) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.note(s.m.SetSuperReadOnly(ctx, on))
}

func (s *simSession) SetReadOnly(ctx context.Context, on bool) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.note(s.m.SetReadOnly(ctx, on))
}

func (s *simSession) CheckReadOnly(ctx context.Context) (bool, error) {
	if err := s.open(); err != nil {
		return false, err
	}
	v, err := s.m.CheckReadOnly(ctx)
	return v, s.note(err)
}

func (s *simSession) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	if err := s.open(); err != nil {
		return false, err
	}
	v, err := s.m.CheckSuperReadOnly(ctx)
	return v, s.note(err)
}

func (s *simSession) ShowReplicaStatus(ctx context.Context) (*mysql.ReplicaStatus, error) {
	if err := s.open(); err != nil {
		return nil, err
	}
	v, err := s.m.ShowReplicaStatus(ctx)
	return v, s.note(err)
}

func (s *simSession) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	if err := s.open(); err != nil {
		return mysql.GTIDSnapshot{}, err
	}
	v, err := s.m.ReadGTIDSnapshot(ctx)
	return v, s.note(err)
}

func (s *simSession) StartReplica(ctx context.Context) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.note(s.m.StartReplica(ctx))
}

func (s *simSession) ChangeReplicationSource(ctx context.Context, opts mysql.ReplicationSourceOpts) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.note(s.m.ChangeReplicationSource(ctx, opts))
}

// Close releases the connection. It does not change server state.
func (s *simSession) Close() error {
	s.closed = true
	return nil
}
