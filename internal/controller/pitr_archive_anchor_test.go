package controller

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// Re-review 3 N1 (GPT, Opus): a predecessor that never managed an
// uncached read hands over an unanchored tracker. Ownership changing is
// not an authoritative read: the replacement must not overwrite the
// persisted False with Unknown, recover after one clean round, or fire a
// duplicate Warning.
func TestPITRArchiveUnanchoredHandoffStaysUnanchored(t *testing.T) {
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	persistedFalse := &metav1.Condition{Type: conditionPITRArchiveReady, Status: cFalse, Reason: reasonPITRArchiveStoreInitFailed,
		Message: "iad: archive store init: boom", ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(t0.Add(-time.Hour))}
	clean := pitrRound{iad: pitrClean(), pdx: pitrClean()}
	for _, reader := range []string{"reader still failing", "reader recovered before replacement"} {
		for _, first := range []string{"missed", "clean"} {
			t.Run(reader+"/first round "+first, func(t *testing.T) {
				fg := pitrArchiveFG(persistedFalse)
				fg.UID = "uid-1"
				nn := FailoverGroupNamespacedName(fg)
				r, c, rec, _ := pitrArchiveRunner(t, fg, nil)
				clients := make([]*internalmysql.SidecarClient, 2)

				r.apiReader = failingReader{}
				delete(r.managers, nn)
				a := r.newPITRArchiverPoller(context.Background(), fg, nn, clients, nil)
				r.managers[nn] = &managedTopology{archiver: a}
				observeAndWrite(t, r, a, nn, t0, pitrRound{iad: pitrMiss(), pdx: pitrClean()}.results())
				if cond, _ := pitrCondition(t, c, nn); cond.Status != cFalse {
					t.Fatalf("predecessor overwrote the persisted False: %+v", cond)
				}
				if a.isAnchored() {
					t.Fatal("predecessor anchored without an uncached read")
				}

				prev := r.managers[nn]
				done := make(chan struct{})
				close(done)
				prev.archiverDone = done
				delete(r.managers, nn)
				if reader == "reader recovered before replacement" {
					r.apiReader = nil
				}
				b := r.newPITRArchiverPoller(context.Background(), fg, nn, clients, prev)
				r.managers[nn] = &managedTopology{archiver: b}

				round := clean
				if first == "missed" {
					round = pitrRound{iad: pitrMiss(), pdx: pitrClean()}
				}
				observeAndWrite(t, r, b, nn, t0.Add(30*time.Second), round.results())
				if cond, _ := pitrCondition(t, c, nn); cond.Status != cFalse || cond.Reason != reasonPITRArchiveStoreInitFailed {
					t.Fatalf("after the replacement's first round: %+v, want the persisted False kept", cond)
				}
				if first == "missed" {
					observeAndWrite(t, r, b, nn, t0.Add(60*time.Second), pitrRound{iad: pitrInitFailed("boom"), pdx: pitrClean()}.results())
					if cond, _ := pitrCondition(t, c, nn); cond.Status != cFalse {
						t.Fatalf("after the init failure is seen again: %+v, want False", cond)
					}
				} else {
					observeAndWrite(t, r, b, nn, t0.Add(60*time.Second), clean.results())
					cond, _ := pitrCondition(t, c, nn)
					wantSecond := cTrue
					if reader == "reader still failing" {
						wantSecond = cFalse // still unanchored: left as persisted
					}
					if cond.Status != wantSecond {
						t.Fatalf("after two clean rounds: %+v, want %s", cond, wantSecond)
					}
				}
				w, n := countPITREvents(drainEventsPITR(rec))
				wantNormals := 0
				if first == "clean" && reader == "reader recovered before replacement" {
					wantNormals = 1
				}
				if w != 0 || n != wantNormals {
					t.Fatalf("events: %d Warning, %d Normal; want 0 Warning, %d Normal", w, n, wantNormals)
				}
			})
		}
	}
}

// Re-review 3 N2 (Opus): a failing uncached read is logged once per
// episode, and its recovery once.
func TestPITRArchiveAnchorReadFailureLoggedOncePerEpisode(t *testing.T) {
	fg := pitrArchiveFG(nil)
	fg.UID = "uid-1"
	nn := FailoverGroupNamespacedName(fg)
	r, _, _, _ := pitrArchiveRunner(t, fg, nil)
	counts := map[string]int{}
	var mu sync.Mutex
	r.logger = slog.New(&pitrLogCounter{counts: counts, mu: &mu})
	r.apiReader = failingReader{}
	delete(r.managers, nn)
	p := r.newPITRArchiverPoller(context.Background(), fg, nn, make([]*internalmysql.SidecarClient, 2), nil)
	r.managers[nn] = &managedTopology{archiver: p}
	t0 := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		observeAndWrite(t, r, p, nn, t0.Add(time.Duration(i)*30*time.Second), pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())
	}
	r.apiReader = nil
	for i := 3; i < 5; i++ {
		observeAndWrite(t, r, p, nn, t0.Add(time.Duration(i)*30*time.Second), pitrRound{iad: pitrClean(), pdx: pitrClean()}.results())
	}
	mu.Lock()
	defer mu.Unlock()
	if got := counts["PITR archive condition read failed; leaving the condition as persisted"]; got != 1 {
		t.Fatalf("read-failed logged %d times, want 1", got)
	}
	if got := counts["PITR archive condition read recovered"]; got != 1 {
		t.Fatalf("read-recovered logged %d times, want 1", got)
	}
}

type pitrLogCounter struct {
	counts map[string]int
	mu     *sync.Mutex
}

func (h *pitrLogCounter) Enabled(context.Context, slog.Level) bool { return true }
func (h *pitrLogCounter) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.counts[r.Message]++
	h.mu.Unlock()
	return nil
}
func (h *pitrLogCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *pitrLogCounter) WithGroup(string) slog.Handler      { return h }
