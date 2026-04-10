package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	PollLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bloodraven_poll_latency_seconds",
		Help:    "Latency of MySQL poll operations.",
		Buckets: prometheus.DefBuckets,
	}, []string{"site"})

	StateTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_state_transitions_total",
		Help: "Number of state transitions per site.",
	}, []string{"site", "from", "to"})

	TaintOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_taint_operations_total",
		Help: "Number of taint/untaint operations.",
	}, []string{"site", "action"})

	WSClientCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bloodraven_websocket_clients",
		Help: "Number of connected websocket clients.",
	})

	DNSFlipCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_dns_flips_total",
		Help: "Number of DNS flips per target site.",
	}, []string{"site"})

	ReplicationLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_replication_lag_seconds",
		Help: "Replication lag in seconds. Only set for the replica site; -1 if lag is NULL (not replicating).",
	}, []string{"site"})

	ReplicationRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_replication_running",
		Help: "Whether a replication thread is running (1=yes, 0=no). Thread label is 'io' or 'sql'.",
	}, []string{"site", "thread"})

	SiteState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_site_state",
		Help: "Current site state as a state-set (1 for current state, 0 for others). State label is 'writable', 'read-only', 'unreachable', or 'unknown'.",
	}, []string{"site", "state"})

	BackupLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_last_success_timestamp_seconds",
		Help: "Unix timestamp of the last successful backup.",
	}, []string{"failover_group"})

	BackupLastAttemptTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_last_attempt_timestamp_seconds",
		Help: "Unix timestamp of the last backup attempt (success or failure).",
	}, []string{"failover_group"})

	BackupDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bloodraven_backup_duration_seconds",
		Help:    "Duration of completed backup operations in seconds.",
		Buckets: []float64{30, 60, 120, 300, 600, 1200, 1800, 3600, 7200},
	}, []string{"failover_group", "method"})

	BackupTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_backup_total",
		Help: "Total number of completed backup attempts, labeled by result ('success' or 'failure').",
	}, []string{"failover_group", "result"})
)

// AllStates is the set of possible site states, used to emit the full state-set.
var AllStates = []string{"writable", "read-only", "unreachable", "unknown"}

// Register registers all metrics with the given registerer.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(PollLatency, StateTransitions, TaintOperations, WSClientCount, DNSFlipCount,
		ReplicationLag, ReplicationRunning, SiteState,
		BackupLastSuccessTimestamp, BackupLastAttemptTimestamp, BackupDurationSeconds, BackupTotal)
}
