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

	// --- Backup metrics ------------------------------------------------
	//
	// These five metrics are emitted exactly-once per terminal backup
	// observation by MysqlBackupReconciler.emitTerminalMetrics. The
	// reconciler short-circuits status patches via a semantic-equality
	// check on the computed next status, which guarantees idempotent
	// emission across reconciles of the same terminal CR.

	// BackupRunsTotal counts terminal backup attempts by result label
	// ("success" or "failure"). Labels are (group, profile, result).
	BackupRunsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_backup_runs_total",
		Help: "Total number of terminal MysqlBackup runs labelled by result (success|failure).",
	}, []string{"group", "profile", "result"})

	// BackupDurationSeconds is the wall-clock duration from Job
	// StartTime to CompletionTime, observed once per terminal run.
	BackupDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "bloodraven_backup_duration_seconds",
		Help: "Wall-clock duration of a MysqlBackup Job from StartTime to CompletionTime.",
		// Buckets cover ~15 seconds up to ~8 hours.
		Buckets: []float64{
			15, 30, 60, 120, 300, 600, 900, 1800,
			3600, 7200, 14400, 28800,
		},
	}, []string{"group", "profile"})

	// BackupLastSuccessTimestamp is the Unix timestamp of the most
	// recent successful backup per (group, profile). Useful for the
	// classic "backup is stale" alert.
	BackupLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_last_success_timestamp_seconds",
		Help: "Unix timestamp of the last successful MysqlBackup completion per (group, profile).",
	}, []string{"group", "profile"})

	// BackupLastAttemptTimestamp is the Unix timestamp of the most
	// recent terminal backup attempt regardless of result.
	BackupLastAttemptTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_last_attempt_timestamp_seconds",
		Help: "Unix timestamp of the last terminal MysqlBackup attempt per (group, profile) regardless of result.",
	}, []string{"group", "profile"})

	// BackupLastSizeBytes is the size of the most recent successful
	// backup, in bytes.
	BackupLastSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_last_size_bytes",
		Help: "Size in bytes of the last successful MysqlBackup artifact per (group, profile).",
	}, []string{"group", "profile"})
)

// AllStates is the set of possible site states, used to emit the full state-set.
var AllStates = []string{"writable", "read-only", "unreachable", "unknown"}

// Register registers all metrics with the given registerer.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(PollLatency, StateTransitions, TaintOperations, WSClientCount, DNSFlipCount,
		ReplicationLag, ReplicationRunning, SiteState,
		BackupRunsTotal, BackupDurationSeconds,
		BackupLastSuccessTimestamp, BackupLastAttemptTimestamp, BackupLastSizeBytes)
}
