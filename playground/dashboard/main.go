// Dashboard is a tiny HTTP server that serves the playground dashboard HTML
// and proxies the Bloodraven operator's websocket/status endpoints so the
// browser can reach them without CORS issues.
package main

import (
	"embed"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
)

//go:embed index.html
var staticFS embed.FS

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8091"
	}

	// The operator's auxiliary service inside the cluster.
	operatorURL := os.Getenv("OPERATOR_URL")
	if operatorURL == "" {
		operatorURL = "http://bloodraven.bloodraven-playground.svc.cluster.local:8082"
	}

	// Counter app service inside the cluster.
	counterURL := os.Getenv("COUNTER_URL")
	if counterURL == "" {
		counterURL = "http://counter-app.bloodraven-playground.svc.cluster.local:8090"
	}

	// DNS webhook API service inside the cluster.
	dnsWebhookURL := os.Getenv("DNS_WEBHOOK_URL")
	if dnsWebhookURL == "" {
		dnsWebhookURL = "http://dns-webhook.bloodraven-playground.svc.cluster.local:8889"
	}

	opTarget, err := url.Parse(operatorURL)
	if err != nil {
		log.Fatalf("invalid OPERATOR_URL %q: %v", operatorURL, err)
	}
	counterTarget, err := url.Parse(counterURL)
	if err != nil {
		log.Fatalf("invalid COUNTER_URL %q: %v", counterURL, err)
	}
	dnsTarget, err := url.Parse(dnsWebhookURL)
	if err != nil {
		log.Fatalf("invalid DNS_WEBHOOK_URL %q: %v", dnsWebhookURL, err)
	}

	opProxy := httputil.NewSingleHostReverseProxy(opTarget)
	counterProxy := httputil.NewSingleHostReverseProxy(counterTarget)
	dnsProxy := httputil.NewSingleHostReverseProxy(dnsTarget)

	mux := http.NewServeMux()

	// Serve the dashboard HTML
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/index.html" {
			http.NotFound(w, r)
			return
		}
		data, err := staticFS.ReadFile("index.html")
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	// Proxy operator endpoints
	mux.HandleFunc("/api/operator/status", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/status"
		opProxy.ServeHTTP(w, r)
	})
	mux.HandleFunc("/ws/status", func(w http.ResponseWriter, r *http.Request) {
		opProxy.ServeHTTP(w, r)
	})

	// Proxy counter app endpoints
	mux.HandleFunc("/api/counter/", func(w http.ResponseWriter, r *http.Request) {
		counterProxy.ServeHTTP(w, r)
	})

	// Proxy dns-webhook API endpoints
	mux.HandleFunc("/api/dns", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/api/records"
		dnsProxy.ServeHTTP(w, r)
	})
	mux.HandleFunc("/api/dns/events", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/api/events"
		dnsProxy.ServeHTTP(w, r)
	})

	// Healthz
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})

	log.Printf("dashboard listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

