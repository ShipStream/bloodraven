package sidecar

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	brmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// archiverStatusFrom serves body at /archiver/status and decodes it with
// the probe.
func archiverStatusFrom(t *testing.T, body []byte) *ArchiverStatusResponse {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/archiver/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	p := &Probe{Site: "iad", cli: srv.Client(), base: srv.URL}
	st, err := p.ArchiverStatus(context.Background())
	if err != nil {
		t.Fatalf("ArchiverStatus: %v", err)
	}
	return st
}

// TestArchiverStatusDecodesTail pins the probe's mirror of the `tail` block
// (#264) to the operator's wire type: what internal/mysql.ArchiverStatus
// marshals, the probe decodes field for field.
func TestArchiverStatusDecodesTail(t *testing.T) {
	for _, tail := range []brmysql.ArchiverTailStatus{
		{State: brmysql.ArchiverTailDrained, ActiveFile: "mysql-bin.000008", NewestSealedFile: "mysql-bin.000007"},
		{State: brmysql.ArchiverTailPending, ActiveFile: "mysql-bin.000008", NewestSealedFile: "mysql-bin.000007", PendingFiles: 2},
		{State: brmysql.ArchiverTailBlocked, ActiveFile: "mysql-bin.000002", PendingFiles: 1, Reason: "this site's manifest holds no promotion record"},
	} {
		t.Run(tail.State, func(t *testing.T) {
			wire := brmysql.ArchiverStatus{Enabled: true, Site: "iad", ManifestPrefix: "e2e/x/binlogs", BacklogFiles: tail.PendingFiles, Tail: &tail}
			body, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			st := archiverStatusFrom(t, body)
			if !st.Enabled || st.Site != "iad" || st.ManifestPrefix != "e2e/x/binlogs" || st.BacklogFiles != tail.PendingFiles {
				t.Errorf("status %+v does not match the wire %s", st, body)
			}
			if st.Tail == nil {
				t.Fatalf("tail not decoded from %s", body)
			}
			got := brmysql.ArchiverTailStatus{State: st.Tail.State, ActiveFile: st.Tail.ActiveFile, NewestSealedFile: st.Tail.NewestSealedFile,
				PendingFiles: st.Tail.PendingFiles, Reason: st.Tail.Reason}
			if got != tail {
				t.Errorf("tail %+v, want %+v (wire %s)", got, tail, body)
			}
		})
	}
}

// TestArchiverStatusWithoutTail: a sidecar that has not read its binlog
// index yet, or predates the field, reports no tail; the probe keeps that
// apart from an empty one.
func TestArchiverStatusWithoutTail(t *testing.T) {
	body, err := json.Marshal(brmysql.ArchiverStatus{Enabled: true, Site: "iad"})
	if err != nil {
		t.Fatal(err)
	}
	if st := archiverStatusFrom(t, body); st.Tail != nil {
		t.Errorf("tail=%+v, want nil", st.Tail)
	}
}

// TestArchiverTailMirrorFields pins the mirror's JSON keys to the wire
// type's: a renamed key on either side would decode as a zero value.
func TestArchiverTailMirrorFields(t *testing.T) {
	keys := func(v any) map[string]bool {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for k := range m {
			out[k] = true
		}
		return out
	}
	wire := keys(brmysql.ArchiverTailStatus{State: "s", ActiveFile: "a", NewestSealedFile: "n", PendingFiles: 1, Reason: "r"})
	mirror := keys(ArchiverTailResponse{State: "s", ActiveFile: "a", NewestSealedFile: "n", PendingFiles: 1, Reason: "r"})
	for k := range wire {
		if !mirror[k] {
			t.Errorf("the wire type's tail field %q is missing from the probe's mirror", k)
		}
	}
	for k := range mirror {
		if !wire[k] {
			t.Errorf("the probe's mirror has tail field %q the wire type lacks", k)
		}
	}
}
