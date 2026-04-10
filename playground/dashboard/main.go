// Dashboard is a tiny HTTP server that serves the playground dashboard HTML
// and proxies the Bloodraven operator's websocket/status endpoints so the
// browser can reach them without CORS issues.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"
)

//go:embed index.html
var staticFS embed.FS

// k8sConfig holds pre-loaded in-cluster configuration for querying the K8s API.
type k8sConfig struct {
	client    *http.Client
	apiServer string
	namespace string
	tokenPath string
}

func newK8sConfig() (*k8sConfig, error) {
	const (
		tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
		caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
		nsPath    = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	)

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA cert")
	}

	ns, err := os.ReadFile(nsPath)
	if err != nil {
		return nil, fmt.Errorf("read namespace: %w", err)
	}

	return &k8sConfig{
		client: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: pool,
				},
			},
		},
		apiServer: "https://kubernetes.default.svc",
		namespace: string(ns),
		tokenPath: tokenPath,
	}, nil
}

// dnsEndpointList is the minimal K8s API list response for DNSEndpoint CRs.
type dnsEndpointList struct {
	Items []struct {
		Spec struct {
			Endpoints []dnsRecord `json:"endpoints"`
		} `json:"spec"`
	} `json:"items"`
}

// dnsRecord matches the dns-webhook Endpoint format.
type dnsRecord struct {
	DNSName    string   `json:"dnsName"`
	Targets    []string `json:"targets"`
	RecordType string   `json:"recordType"`
	RecordTTL  int      `json:"recordTTL,omitempty"`
}

func (k *k8sConfig) listDNSEndpoints() ([]dnsRecord, error) {
	token, err := os.ReadFile(k.tokenPath)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}

	u := fmt.Sprintf("%s/apis/externaldns.k8s.io/v1alpha1/namespaces/%s/dnsendpoints",
		k.apiServer, k.namespace)

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+string(token))
	req.Header.Set("Accept", "application/json")

	resp, err := k.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("k8s API request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("k8s API returned %d: %s", resp.StatusCode, body)
	}

	var list dnsEndpointList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	var records []dnsRecord
	for _, item := range list.Items {
		records = append(records, item.Spec.Endpoints...)
	}
	return records, nil
}

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

	// K8s API direct read of DNSEndpoint CRs (only works in-cluster)
	k8sCfg, err := newK8sConfig()
	if err != nil {
		log.Printf("WARN: K8s API client not available (running outside cluster?): %v", err)
	} else {
		mux.HandleFunc("/api/dns/k8s", func(w http.ResponseWriter, r *http.Request) {
			records, err := k8sCfg.listDNSEndpoints()
			if err != nil {
				log.Printf("ERROR: list DNSEndpoints: %v", err)
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(records)
		})
	}

	// Healthz
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})

	log.Printf("dashboard listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
