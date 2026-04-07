package main

import "github.com/prometheus/client_golang/prometheus"

var (
	pollLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mysql_watcher_poll_latency_seconds",
		Help:    "Latency of MySQL poll operations.",
		Buckets: prometheus.DefBuckets,
	}, []string{"dc"})

	stateTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mysql_watcher_state_transitions_total",
		Help: "Number of state transitions per DC.",
	}, []string{"dc", "from", "to"})

	taintOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mysql_watcher_taint_operations_total",
		Help: "Number of taint/untaint operations.",
	}, []string{"dc", "action"})

	wsClientCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mysql_watcher_websocket_clients",
		Help: "Number of connected websocket clients.",
	})

	dnsFlipCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mysql_watcher_dns_flips_total",
		Help: "Number of DNS flips per target DC.",
	}, []string{"dc"})
)

func init() {
	prometheus.MustRegister(pollLatency, stateTransitions, taintOperations, wsClientCount, dnsFlipCount)
}
