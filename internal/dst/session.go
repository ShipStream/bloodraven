package dst

import (
	"context"
	"errors"

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
type simSession struct {
	m      *simChecker // pinned copy
	closed bool
}

var _ mysql.Session = (*simSession)(nil)

// errSessionClosed mirrors database/sql's sql.ErrConnDone text.
var errSessionClosed = errors.New("sql: connection is already closed")

func (s *simSession) open() error {
	if s.closed {
		return errSessionClosed
	}
	return nil
}

func (s *simSession) StopReplica(ctx context.Context) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.m.StopReplica(ctx)
}

func (s *simSession) ResetReplicaAll(ctx context.Context) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.m.ResetReplicaAll(ctx)
}

func (s *simSession) GetGtidExecuted(ctx context.Context) (string, error) {
	if err := s.open(); err != nil {
		return "", err
	}
	return s.m.GetGtidExecuted(ctx)
}

func (s *simSession) SetSuperReadOnly(ctx context.Context, on bool) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.m.SetSuperReadOnly(ctx, on)
}

func (s *simSession) SetReadOnly(ctx context.Context, on bool) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.m.SetReadOnly(ctx, on)
}

func (s *simSession) CheckReadOnly(ctx context.Context) (bool, error) {
	if err := s.open(); err != nil {
		return false, err
	}
	return s.m.CheckReadOnly(ctx)
}

func (s *simSession) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	if err := s.open(); err != nil {
		return false, err
	}
	return s.m.CheckSuperReadOnly(ctx)
}

func (s *simSession) ShowReplicaStatus(ctx context.Context) (*mysql.ReplicaStatus, error) {
	if err := s.open(); err != nil {
		return nil, err
	}
	return s.m.ShowReplicaStatus(ctx)
}

func (s *simSession) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	if err := s.open(); err != nil {
		return mysql.GTIDSnapshot{}, err
	}
	return s.m.ReadGTIDSnapshot(ctx)
}

func (s *simSession) StartReplica(ctx context.Context) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.m.StartReplica(ctx)
}

func (s *simSession) ChangeReplicationSource(ctx context.Context, opts mysql.ReplicationSourceOpts) error {
	if err := s.open(); err != nil {
		return err
	}
	return s.m.ChangeReplicationSource(ctx, opts)
}

// Close releases the connection. It does not change server state.
func (s *simSession) Close() error {
	s.closed = true
	return nil
}
