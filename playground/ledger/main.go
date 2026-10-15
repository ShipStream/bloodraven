// Ledger is a chaos-testing workload. Every write is a uniquely keyed INSERT
// inside a transaction that also reads @@hostname, so the writer knows which
// MySQL server acknowledged it. Acks are kept in memory and served over HTTP;
// `ledger audit` compares them against every site to find acknowledged writes
// that were lost, writes accepted by a server that was not the primary, and
// errant GTIDs.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

type ack struct {
	Writer int    `json:"w"`
	Seq    int64  `json:"s"`
	Host   string `json:"h"`
	At     int64  `json:"t"` // unix ms when COMMIT returned
}

type failure struct {
	Writer    int    `json:"w"`
	Seq       int64  `json:"s"`
	Ambiguous bool   `json:"ambiguous"` // error at COMMIT: may have committed
	Err       string `json:"err"`
	At        int64  `json:"t"`
}

type state struct {
	mu       sync.Mutex
	run      string
	paused   bool
	acks     []ack
	failures []failure
	started  time.Time
}

var st = &state{}

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "audit" {
		os.Exit(audit(os.Args[2:]))
	}
	dsn := env("MYSQL_DSN", "root:playground-root-pw@tcp(127.0.0.1:3306)/")
	workers, _ := strconv.Atoi(env("WORKERS", "4"))
	interval, _ := time.ParseDuration(env("INTERVAL", "50ms"))
	st.run = env("RUN_ID", fmt.Sprintf("r%d", time.Now().Unix()))
	st.started = time.Now()

	ensureSchema(dsn)
	for w := 0; w < workers; w++ {
		go writer(dsn, w, interval)
	}
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/acks", handleAcks)
	http.HandleFunc("/failures", handleFailures)
	http.HandleFunc("/pause", func(w http.ResponseWriter, _ *http.Request) { setPaused(true) })
	http.HandleFunc("/resume", func(w http.ResponseWriter, _ *http.Request) { setPaused(false) })
	log.Printf("ledger run=%s workers=%d interval=%s", st.run, workers, interval)
	log.Fatal(http.ListenAndServe(env("LISTEN_ADDR", ":8095"), nil))
}

func setPaused(p bool) {
	st.mu.Lock()
	st.paused = p
	st.mu.Unlock()
}

func ensureSchema(dsn string) {
	for {
		db, err := sql.Open("mysql", dsn+"?timeout=3s&readTimeout=5s&writeTimeout=5s")
		if err == nil {
			_, err = db.Exec("CREATE DATABASE IF NOT EXISTS ledger")
			if err == nil {
				_, err = db.Exec(`CREATE TABLE IF NOT EXISTS ledger.events (
					run VARCHAR(32) NOT NULL, writer INT NOT NULL, seq BIGINT NOT NULL,
					host VARCHAR(128) NOT NULL, at DATETIME(6) NOT NULL,
					PRIMARY KEY (run, writer, seq)) ENGINE=InnoDB`)
			}
			db.Close()
		}
		if err == nil {
			return
		}
		log.Printf("schema: %v", err)
		time.Sleep(2 * time.Second)
	}
}

// writer keeps one long-lived connection, like an application pool would, so
// a fenced or demoted server that still accepts commits on established
// sessions shows up as acks from the wrong host.
func writer(dsn string, w int, interval time.Duration) {
	db, err := sql.Open("mysql", dsn+"ledger?timeout=2s&readTimeout=4s&writeTimeout=4s")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var seq int64
	for {
		st.mu.Lock()
		paused := st.paused
		st.mu.Unlock()
		if paused {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		seq++
		host, ambiguous, err := writeOne(db, w, seq)
		now := time.Now().UnixMilli()
		st.mu.Lock()
		if err == nil {
			st.acks = append(st.acks, ack{Writer: w, Seq: seq, Host: host, At: now})
		} else {
			st.failures = append(st.failures, failure{Writer: w, Seq: seq, Ambiguous: ambiguous, Err: err.Error(), At: now})
		}
		st.mu.Unlock()
		if err != nil {
			time.Sleep(200*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond)
			continue
		}
		time.Sleep(interval)
	}
}

func writeOne(db *sql.DB, w int, seq int64) (host string, ambiguous bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO events (run, writer, seq, host, at) VALUES (?, ?, ?, @@hostname, NOW(6))", st.run, w, seq); err != nil {
		tx.Rollback()
		return "", false, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT @@hostname").Scan(&host); err != nil {
		tx.Rollback()
		return "", false, err
	}
	if err = tx.Commit(); err != nil {
		var me *mysql.MySQLError
		// A server-side rejection of COMMIT is definite; a transport error is not.
		return host, !errors.As(err, &me), err
	}
	return host, false, nil
}

func handleStats(w http.ResponseWriter, _ *http.Request) {
	st.mu.Lock()
	defer st.mu.Unlock()
	byHost := map[string]int{}
	for _, a := range st.acks {
		byHost[a.Host]++
	}
	amb := 0
	for _, f := range st.failures {
		if f.Ambiguous {
			amb++
		}
	}
	last := ""
	if n := len(st.failures); n > 0 {
		last = st.failures[n-1].Err
	}
	json.NewEncoder(w).Encode(map[string]any{
		"run": st.run, "acks": len(st.acks), "acksByHost": byHost,
		"failures": len(st.failures), "ambiguous": amb, "lastError": last,
		"uptime": time.Since(st.started).Round(time.Second).String(),
	})
}

func handleAcks(w http.ResponseWriter, _ *http.Request) {
	st.mu.Lock()
	out := append([]ack(nil), st.acks...)
	run := st.run
	st.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]any{"run": run, "acks": out})
}

func handleFailures(w http.ResponseWriter, _ *http.Request) {
	st.mu.Lock()
	out := append([]failure(nil), st.failures...)
	st.mu.Unlock()
	json.NewEncoder(w).Encode(out)
}

// audit: ledger audit <ledger-url> <site=host:port>...
func audit(args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ledger audit <ledger-url> site=host:port ...")
		return 2
	}
	base := strings.TrimRight(args[0], "/")
	// Quiesce writers so sites are compared at one point in the stream; the
	// in-flight write finishes within its 5s context.
	if r, err := http.Get(base + "/pause"); err == nil {
		r.Body.Close()
		defer http.Get(base + "/resume")
	}
	time.Sleep(time.Duration(envInt("AUDIT_SETTLE_SECONDS", 7)) * time.Second)
	resp, err := http.Get(base + "/acks")
	if err != nil {
		fmt.Println("fetch acks:", err)
		return 2
	}
	var payload struct {
		Run  string `json:"run"`
		Acks []ack  `json:"acks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		fmt.Println("decode acks:", err)
		return 2
	}
	resp.Body.Close()
	user := env("AUDIT_USER", "root")
	pass := env("AUDIT_PASSWORD", "playground-root-pw")

	type siteInfo struct {
		name, addr, host, gtid string
		ro, sro                bool
		rows                   map[[2]int64]string
		err                    error
	}
	var sites []*siteInfo
	for _, s := range args[1:] {
		name, addr, _ := strings.Cut(s, "=")
		si := &siteInfo{name: name, addr: addr, rows: map[[2]int64]string{}}
		sites = append(sites, si)
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/?timeout=3s&readTimeout=10s", user, pass, addr))
		if err != nil {
			si.err = err
			continue
		}
		if err := db.QueryRow("SELECT @@hostname, @@global.gtid_executed, @@global.read_only, @@global.super_read_only").Scan(&si.host, &si.gtid, &si.ro, &si.sro); err != nil {
			si.err = err
			db.Close()
			continue
		}
		si.gtid = strings.ReplaceAll(si.gtid, "\n", "")
		rows, err := db.Query("SELECT writer, seq, host FROM ledger.events WHERE run = ?", payload.Run)
		if err != nil {
			si.err = err
			db.Close()
			continue
		}
		for rows.Next() {
			var wr, sq int64
			var h string
			rows.Scan(&wr, &sq, &h)
			si.rows[[2]int64{wr, sq}] = h
		}
		rows.Close()
		db.Close()
	}

	ackHosts := map[string]int{}
	for _, a := range payload.Acks {
		ackHosts[a.Host]++
	}
	fmt.Printf("run=%s acks=%d acksByHost=%v\n", payload.Run, len(payload.Acks), ackHosts)
	bad := 0
	var writable []*siteInfo
	for _, si := range sites {
		if si.err != nil {
			fmt.Printf("site %-7s ERROR %v\n", si.name, si.err)
			continue
		}
		missing, wrongHost := 0, 0
		var missingSample []string
		for _, a := range payload.Acks {
			h, ok := si.rows[[2]int64{int64(a.Writer), a.Seq}]
			if !ok {
				missing++
				if len(missingSample) < 5 {
					missingSample = append(missingSample, fmt.Sprintf("w%d/s%d@%s", a.Writer, a.Seq, a.Host))
				}
			} else if h != a.Host {
				wrongHost++
			}
		}
		if !si.ro {
			writable = append(writable, si)
		}
		fmt.Printf("site %-7s host=%s ro=%v sro=%v rows=%d ackedMissing=%d hostMismatch=%d sample=%v\n",
			si.name, si.host, si.ro, si.sro, len(si.rows), missing, wrongHost, missingSample)
	}
	if len(writable) != 1 {
		fmt.Printf("INVARIANT writable sites = %d (want 1)\n", len(writable))
		bad++
	}
	if len(writable) == 1 {
		p := writable[0]
		for _, si := range sites {
			if si == p || si.err != nil {
				continue
			}
			var subset bool
			db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/?timeout=3s", user, pass, p.addr))
			if err == nil {
				err = db.QueryRow("SELECT GTID_SUBSET(?, ?)", si.gtid, p.gtid).Scan(&subset)
				if !subset && err == nil {
					var extra string
					db.QueryRow("SELECT GTID_SUBTRACT(?, ?)", si.gtid, p.gtid).Scan(&extra)
					fmt.Printf("INVARIANT %s has errant GTIDs vs primary %s: %s\n", si.name, p.name, extra)
					bad++
				}
				db.Close()
			}
		}
		// Rows on the primary acked by a host other than the primary's own
		// hostname were accepted by an earlier primary; fine. Rows on any
		// site that the primary lacks are divergence.
		for _, si := range sites {
			if si == p || si.err != nil {
				continue
			}
			extra := 0
			hosts := map[string]int{}
			for k, h := range si.rows {
				if _, ok := p.rows[k]; !ok {
					extra++
					hosts[h]++
				}
			}
			if extra > 0 {
				fmt.Printf("INVARIANT %s has %d rows the primary lacks (by accepting host %v)\n", si.name, extra, hosts)
				bad++
			}
		}
	}
	// Acks by host over time: overlapping windows from two hosts are dual writes.
	type span struct{ first, last int64 }
	spans := map[string]*span{}
	for _, a := range payload.Acks {
		s := spans[a.Host]
		if s == nil {
			s = &span{a.At, a.At}
			spans[a.Host] = s
		}
		if a.At < s.first {
			s.first = a.At
		}
		if a.At > s.last {
			s.last = a.At
		}
	}
	var hosts []string
	for h := range spans {
		hosts = append(hosts, h)
	}
	sort.Slice(hosts, func(i, j int) bool { return spans[hosts[i]].first < spans[hosts[j]].first })
	for _, h := range hosts {
		s := spans[h]
		fmt.Printf("acks from %s: %s .. %s\n", h, time.UnixMilli(s.first).UTC().Format("15:04:05.000"), time.UnixMilli(s.last).UTC().Format("15:04:05.000"))
	}
	if bad > 0 {
		return 1
	}
	return 0
}
