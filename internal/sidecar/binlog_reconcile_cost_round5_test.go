package sidecar

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestReview5ReconciliationIndexCost(t *testing.T) {
	for _, n := range []int{1000, 10000, 40000} {
		for _, reverse := range []bool{false, true} {
			m := &Manifest{Site: "iad"}
			for i := 0; i < n; i++ {
				gno := i + 1
				if reverse {
					gno = n - i
				}
				id := fmt.Sprintf("%064x", gno)
				m.Files = append(m.Files, ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", gno), ID: id, GTIDCoverage: &BinlogGTIDCoverage{ContentID: id, GTIDs: fmt.Sprintf("%s:%d", coverageTestSID, gno)}})
			}
			a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, newMemStore(), slog.New(slog.NewTextHandler(io.Discard, nil)))
			at := time.Now()
			w := a.chooseArchiveReuseWork(m, false)
			t.Logf("rows=%d reverse=%v elapsed=%v work=%v", n, reverse, time.Since(at), w != nil)
		}
	}
}
