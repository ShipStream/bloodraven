package sidecar

import (
	"context"
	"io"
	"os"
)

const coverageTestSID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

type retentionRound2Store struct {
	ArchiveStore
	reads map[string]int
	deny  string
}

func (s *retentionRound2Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.reads[key]++
	if key == s.deny {
		return nil, os.ErrPermission
	}
	return s.ArchiveStore.Open(ctx, key)
}
