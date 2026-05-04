// Counter-app is a minimal web app that increments a counter stored in MySQL.
// It demonstrates that state persists across Bloodraven failovers.
package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed index.html
var staticFS embed.FS

var (
	mu    sync.RWMutex
	db    *sql.DB
	dbErr error = fmt.Errorf("connecting to MySQL...")

	// rdb is set when REDIS_HOST is configured; nil otherwise.
	// The Dragonfly counter is opt-in: a counter-app pod still
	// works when Bloodraven is deployed without spec.dragonfly.
	rdb *redisClient
)

func getDB() (*sql.DB, error) {
	mu.RLock()
	defer mu.RUnlock()
	return db, dbErr
}

func main() {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:playground-root-pw@tcp(127.0.0.1:3306)/counter_db?parseTime=true&timeout=5s"
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8090"
	}

	// Connect to MySQL in the background so the HTTP server starts immediately.
	go connectLoop(dsn)

	// Optional Dragonfly counter for demonstrating session/cache continuity
	// across Bloodraven failovers. When REDIS_HOST is unset, /api/counter
	// returns cacheValue=0 with cacheError="not configured" and increments
	// only touch MySQL.
	if host := os.Getenv("REDIS_HOST"); host != "" {
		port := os.Getenv("REDIS_PORT")
		if port == "" {
			port = "6379"
		}
		rdb = newRedisClient(host + ":" + port)
		log.Printf("dragonfly counter enabled at %s:%s", host, port)
	}

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/healthz", handleHealthz)
	http.HandleFunc("/api/counter", handleCounter)
	http.HandleFunc("/api/increment", handleIncrement)

	log.Printf("counter-app listening on %s (MySQL connecting in background)", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func connectLoop(dsn string) {
	for {
		conn, err := sql.Open("mysql", dsn)
		if err == nil {
			err = conn.Ping()
		}
		if err != nil {
			mu.Lock()
			dbErr = fmt.Errorf("MySQL unavailable: %v", err)
			mu.Unlock()
			log.Printf("waiting for mysql: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		conn.SetMaxOpenConns(5)
		conn.SetConnMaxLifetime(30 * time.Second)

		if err := migrate(conn); err != nil {
			log.Printf("migration failed (will retry): %v", err)
			conn.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		mu.Lock()
		db = conn
		dbErr = nil
		mu.Unlock()
		log.Printf("MySQL connected and migrated")
		return
	}
}

func migrate(conn *sql.DB) error {
	_, err := conn.Exec(`CREATE DATABASE IF NOT EXISTS counter_db`)
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS counter_db.counters (
		id INT PRIMARY KEY DEFAULT 1,
		value BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	_, err = conn.Exec(`INSERT IGNORE INTO counter_db.counters (id, value) VALUES (1, 0)`)
	return err
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, _ := staticFS.ReadFile("index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	conn, err := getDB()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if err := conn.Ping(); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

type counterResponse struct {
	Value     int64  `json:"value"`
	UpdatedAt string `json:"updatedAt"`
	DBHost    string `json:"dbHost"`
	ReadOnly  bool   `json:"readOnly"`

	// Dragonfly counter — independent of the MySQL value. Survives
	// planned failover when sessions are preserved; resets to 0 if the
	// active site changes via emergency failover or if Dragonfly was
	// never configured.
	CacheValue int64  `json:"cacheValue"`
	CacheHost  string `json:"cacheHost,omitempty"`
	CacheError string `json:"cacheError,omitempty"`
}

func handleCounter(w http.ResponseWriter, _ *http.Request) {
	conn, err := getDB()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	var resp counterResponse
	err = conn.QueryRow(`SELECT value, updated_at FROM counter_db.counters WHERE id = 1`).
		Scan(&resp.Value, &resp.UpdatedAt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if the DB is read-only
	var readOnly string
	if err := conn.QueryRow(`SELECT @@global.read_only`).Scan(&readOnly); err == nil {
		resp.ReadOnly = readOnly == "1"
	}
	var host string
	if err := conn.QueryRow(`SELECT @@hostname`).Scan(&host); err == nil {
		resp.DBHost = host
	}

	// Dragonfly counter (best-effort; surface error rather than 500-ing).
	if rdb != nil {
		resp.CacheHost = rdb.host()
		v, gerr := rdb.get("counter:cache")
		if gerr != nil {
			resp.CacheError = gerr.Error()
		} else {
			resp.CacheValue = v
		}
	} else {
		resp.CacheError = "not configured"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleIncrement(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	conn, err := getDB()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	_, err = conn.Exec(`UPDATE counter_db.counters SET value = value + 1 WHERE id = 1`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Mirror the increment into Dragonfly. Best-effort: a failure here
	// must not roll back the MySQL increment (cache is not the source
	// of truth) — handleCounter will surface the error to the UI.
	if rdb != nil {
		if _, ierr := rdb.incr("counter:cache"); ierr != nil {
			log.Printf("dragonfly INCR failed: %v", ierr)
		}
	}

	handleCounter(w, r)
}
