package scenarios

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// ackedWriter commits one row per transaction to one site and records
// every id MySQL acknowledged. It is the scenario-sized version of the
// playground ledger: a failover that loses an acknowledged write shows up
// as an acked id missing from the surviving primary, which a row count or
// a GTID comparison against a snapshot taken before the fault can miss.
//
// It owns its own port-forwarded connection so a hung or dying site
// cannot starve the scenario's cached clients. Writes stop being acked
// when the site dies or is fenced; that is expected, and the errors are
// counted rather than returned.
type ackedWriter struct {
	site  string
	table string

	mu      sync.Mutex
	acked   []int64
	failed  int
	lastErr string

	cancel   context.CancelFunc
	done     chan struct{}
	client   *pgmysql.SiteClient
	stopOnce sync.Once
}

// startAckedWriter creates db.table on site and starts writing a row
// every interval until stop is called.
func startAckedWriter(ctx context.Context, env *runner.Env, site, db, table string, interval time.Duration) (*ackedWriter, error) {
	client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return nil, fmt.Errorf("open writer connection to %s: %w", site, err)
	}
	qualified := quoteSQLIdentifier(db) + "." + quoteSQLIdentifier(table)
	ddl := "CREATE DATABASE IF NOT EXISTS " + quoteSQLIdentifier(db) + "; " +
		"CREATE TABLE IF NOT EXISTS " + qualified + " (id BIGINT PRIMARY KEY, written_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
	ddlCtx, cancelDDL := context.WithTimeout(ctx, 10*time.Second)
	_, err = client.Exec(ddlCtx, ddl)
	cancelDDL()
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("create %s on %s: %w", qualified, site, err)
	}
	// Ids continue past any rows a previous run left behind.
	var next int64
	maxCtx, cancelMax := context.WithTimeout(ctx, 10*time.Second)
	next, err = client.ScalarInt(maxCtx, "SELECT COALESCE(MAX(id), 0) FROM "+qualified)
	cancelMax()
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("read max id from %s on %s: %w", qualified, site, err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	w := &ackedWriter{site: site, table: qualified, cancel: cancel, done: make(chan struct{}), client: client}
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			next++
			writeCtx, cancelWrite := context.WithTimeout(runCtx, 3*time.Second)
			_, err := client.Exec(writeCtx, "INSERT INTO "+qualified+" (id) VALUES (?)", next)
			cancelWrite()
			w.mu.Lock()
			if err == nil {
				w.acked = append(w.acked, next)
			} else {
				w.failed++
				w.lastErr = err.Error()
			}
			w.mu.Unlock()
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return w, nil
}

// stop ends the writer and closes its connection. Safe to call twice.
func (w *ackedWriter) stop() {
	if w == nil {
		return
	}
	w.stopOnce.Do(func() {
		w.cancel()
		<-w.done
		_ = w.client.Close()
	})
}

// ackedIDs returns a copy of the acknowledged ids.
func (w *ackedWriter) ackedIDs() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]int64(nil), w.acked...)
}

func (w *ackedWriter) summary() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := fmt.Sprintf("writer on %s: acked=%d failed=%d", w.site, len(w.acked), w.failed)
	if w.lastErr != "" {
		s += fmt.Sprintf(" lastErr=%q", w.lastErr)
	}
	return s
}

// missingAckedIDs returns the acknowledged ids absent from table on site.
func missingAckedIDs(ctx context.Context, env *runner.Env, site, table string, acked []int64) ([]int64, error) {
	if len(acked) == 0 {
		return nil, nil
	}
	client, err := env.MySQL(site)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", site, err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rows, err := client.DB.QueryContext(queryCtx, "SELECT id FROM "+table)
	if err != nil {
		return nil, fmt.Errorf("read %s on %s: %w", table, site, err)
	}
	defer func() { _ = rows.Close() }()
	present := make(map[int64]bool, len(acked))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		present[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var missing []int64
	for _, id := range acked {
		if !present[id] {
			missing = append(missing, id)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing, nil
}

// formatIDs renders ids compactly for notes and errors.
func formatIDs(ids []int64) string {
	const limit = 20
	parts := make([]string, 0, limit+1)
	for i, id := range ids {
		if i == limit {
			parts = append(parts, fmt.Sprintf("... (%d more)", len(ids)-limit))
			break
		}
		parts = append(parts, fmt.Sprintf("%d", id))
	}
	return "[" + strings.Join(parts, " ") + "]"
}
