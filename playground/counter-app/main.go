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
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//go:embed index.html
var staticFS embed.FS

var db *sql.DB

func main() {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:playground-root-pw@tcp(127.0.0.1:3306)/counter_db?parseTime=true&timeout=5s"
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8090"
	}

	var err error
	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			break
		}
		log.Printf("waiting for mysql (%d/30): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("could not connect to mysql after 60s: %v", err)
	}
	db.SetMaxOpenConns(5)
	db.SetConnMaxLifetime(30 * time.Second)

	if err := migrate(); err != nil {
		log.Fatalf("migration failed: %v", err)
	}
	log.Printf("counter-app ready, listening on %s", addr)

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/healthz", handleHealthz)
	http.HandleFunc("/api/counter", handleCounter)
	http.HandleFunc("/api/increment", handleIncrement)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func migrate() error {
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS counter_db`)
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS counter_db.counters (
		id INT PRIMARY KEY DEFAULT 1,
		value BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	_, err = db.Exec(`INSERT IGNORE INTO counter_db.counters (id, value) VALUES (1, 0)`)
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
	if err := db.Ping(); err != nil {
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
}

func handleCounter(w http.ResponseWriter, _ *http.Request) {
	var resp counterResponse
	err := db.QueryRow(`SELECT value, updated_at FROM counter_db.counters WHERE id = 1`).
		Scan(&resp.Value, &resp.UpdatedAt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if the DB is read-only
	var readOnly string
	if err := db.QueryRow(`SELECT @@global.read_only`).Scan(&readOnly); err == nil {
		resp.ReadOnly = readOnly == "1"
	}
	var host string
	if err := db.QueryRow(`SELECT @@hostname`).Scan(&host); err == nil {
		resp.DBHost = host
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleIncrement(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	_, err := db.Exec(`UPDATE counter_db.counters SET value = value + 1 WHERE id = 1`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	handleCounter(w, r)
}
