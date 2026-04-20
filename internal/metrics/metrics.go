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
		Name: "bloodraven_websocket_connected_clients",
		Help: "Number of currently connected WebSocket clients.",
	})

	DNSFlipCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_dns_flips_total",
		Help: "Number of DNS flips per target site.",
	}, []string{"site"})

	FailoversTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_failovers_total",
		Help: "Total number of failovers executed. Incremented after successful MySQL promotion.",
	}, []string{"target_site"})

	SplitBrainAutoResolveTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_split_brain_auto_resolve_total",
		Help: "Total number of split-brain incidents auto-resolved by spec.splitBrainPolicy.preferSite. The label is the preferred (winning) site.",
	}, []string{"prefer_site"})

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

	// --- Recovery metrics -----------------------------------------------

	// DivergentTransactions is the number of divergent transactions on a
	// site pending recovery after an emergency failover. 0 when healthy.
	DivergentTransactions = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_divergent_transactions",
		Help: "Number of divergent transactions on a site pending recovery. 0 when healthy.",
	}, []string{"site"})

	// RecloneOperations counts admin-triggered reclone operations per site.
	RecloneOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_reclone_operations_total",
		Help: "Total number of admin-triggered reclone operations per site.",
	}, []string{"site"})

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

	// --- Backup verification metrics ----------------------------------
	//
	// Verification is periodic restore-of-the-latest-backup into a
	// throwaway MySQL instance. The headline signal is
	// BackupVerifiedTimestamp: users alert on
	// `time() - bloodraven_backup_verified_timestamp_seconds > <SLO>`
	// to catch "backup ran but is not actually restorable."

	// BackupVerifiedTimestamp is the Unix timestamp of the most
	// recent Succeeded MysqlBackupVerification per (group, profile).
	// This is the gauge wishlist item #8 explicitly names; dashboards
	// and alerts should anchor freshness checks on it.
	BackupVerifiedTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_verified_timestamp_seconds",
		Help: "Unix timestamp of the last Succeeded MysqlBackupVerification per (group, profile).",
	}, []string{"group", "profile"})

	// BackupVerificationLastAttemptTimestamp is the Unix timestamp of
	// the most recent terminal verification attempt regardless of
	// result. Lets operators distinguish "verification never ran" from
	// "verification ran but failed".
	BackupVerificationLastAttemptTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_verification_last_attempt_timestamp_seconds",
		Help: "Unix timestamp of the last terminal MysqlBackupVerification attempt per (group, profile) regardless of result.",
	}, []string{"group", "profile"})

	// BackupVerificationRunsTotal counts terminal verification
	// attempts labelled by result ("success" or "failure"). Labels are
	// (group, profile, result).
	BackupVerificationRunsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_backup_verification_runs_total",
		Help: "Total number of terminal MysqlBackupVerification runs labelled by result (success|failure).",
	}, []string{"group", "profile", "result"})

	// BackupVerificationDurationSeconds is the wall-clock duration of
	// a verification run from StartTime to CompletionTime. Buckets
	// mirror the backup duration histogram (15s → 8h).
	BackupVerificationDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "bloodraven_backup_verification_duration_seconds",
		Help: "Wall-clock duration of a MysqlBackupVerification run from StartTime to CompletionTime.",
		Buckets: []float64{
			15, 30, 60, 120, 300, 600, 900, 1800,
			3600, 7200, 14400, 28800,
		},
	}, []string{"group", "profile"})

	// BackupVerificationReplayLagSeconds is the difference between the
	// verification's CompletionTime and the timestamp of the last binlog
	// event the PITR replay caught up to. Measures PITR archive freshness
	// at verification time: a high value means the archived binlog stream
	// lags the live primary and a real PITR restore would recover less
	// than operators expect. Emitted only for Succeeded runs whose
	// spec.pointInTime.mode is not "none".
	BackupVerificationReplayLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_backup_verification_replay_lag_seconds",
		Help: "Difference in seconds between the verification completion time and the timestamp of the last replayed binlog event.",
	}, []string{"group", "profile"})

	// --- PITR archiver metrics ----------------------------------------
	//
	// These three gauges mirror per-site sidecar archiver state that the
	// operator polls via /archiver/status. They use Gauge (not Counter)
	// because the operator doesn't observe individual increments — it
	// reports the sidecar's current cumulative value. Labels are
	// (namespace, group, site) so multi-cluster Prometheus scrapes can
	// disambiguate.

	// ArchiverUploadFailures is the cumulative count of failed archive
	// attempts reported by the sidecar since its last start. Resets on
	// sidecar restart; dashboards should use `increase()` / `rate()` to
	// handle resets gracefully.
	ArchiverUploadFailures = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_archiver_upload_failures",
		Help: "Cumulative PITR archiver upload failures reported by the per-site sidecar. Monotonic except across sidecar restarts.",
	}, []string{"namespace", "group", "site"})

	// ArchiverLastUploadTimestamp is the Unix timestamp of the most
	// recent successful binlog archive, per site. 0 when the sidecar
	// has not archived anything since start.
	ArchiverLastUploadTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_archiver_last_upload_timestamp_seconds",
		Help: "Unix timestamp of the last successful PITR binlog archive per site.",
	}, []string{"namespace", "group", "site"})

	// ArchiverBacklogFiles is the count of sealed binlogs not yet
	// present in the manifest at the end of the last scan. >0 means
	// the archiver is behind and RPO is drifting.
	ArchiverBacklogFiles = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_archiver_backlog_files",
		Help: "Sealed binlogs that have not been uploaded yet at the end of the last archiver scan.",
	}, []string{"namespace", "group", "site"})
)

// AllStates is the set of possible site states, used to emit the full state-set.
var AllStates = []string{"writable", "read-only", "unreachable", "unknown"}

// Register registers all metrics with the given registerer.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(PollLatency, StateTransitions, TaintOperations, WSClientCount, DNSFlipCount, FailoversTotal,
		SplitBrainAutoResolveTotal,
		ReplicationLag, ReplicationRunning, SiteState, DivergentTransactions, RecloneOperations,
		BackupRunsTotal, BackupDurationSeconds,
		BackupLastSuccessTimestamp, BackupLastAttemptTimestamp, BackupLastSizeBytes,
		BackupVerifiedTimestamp, BackupVerificationLastAttemptTimestamp,
		BackupVerificationRunsTotal, BackupVerificationDurationSeconds,
		BackupVerificationReplayLagSeconds,
		ArchiverUploadFailures, ArchiverLastUploadTimestamp, ArchiverBacklogFiles)
}
