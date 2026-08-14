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

	// PlannedFailoversTotal counts admin-triggered (graceful)
	// switchovers separately from the automatic failover counter above,
	// so existing dashboards and alerts keyed on bloodraven_failovers_total
	// keep their meaning. The "result" label is one of:
	//   success         — promotion completed, target is writable
	//   rejected        — validation failed (unknown site, cooldown, ...)
	//   failed_timeout  — lag gate timed out; source was unfenced
	//   failed_other    — any other mid-flight failure
	PlannedFailoversTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_planned_failovers_total",
		Help: "Total number of admin-triggered (graceful) planned failovers, labelled by result.",
	}, []string{"target_site", "result"})

	// PlannedFailoverDurationSeconds is the end-to-end wall-clock
	// duration of a planned failover, measured from
	// status.plannedFailover.startTime to completionTime. Buckets span
	// typical switchover times (1s) to worst-case lag timeouts (300s).
	PlannedFailoverDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bloodraven_planned_failover_duration_seconds",
		Help:    "Wall-clock duration of an admin-triggered planned failover from acceptance to terminal phase.",
		Buckets: []float64{1, 2, 5, 10, 30, 60, 120, 300},
	}, []string{"target_site"})

	// PlannedFailoverLagWaitSeconds is the time spent in the
	// WaitingForLag phase alone: how long the target took to catch up
	// before it was promoted. Populated on every terminal run,
	// including rollbacks, so a high distribution here is the canary
	// for "planned switchover would have timed out".
	PlannedFailoverLagWaitSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bloodraven_planned_failover_lag_wait_seconds",
		Help:    "Time spent waiting for the target GTID to cover the fenced source GTID during a planned failover.",
		Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300},
	}, []string{"target_site"})

	SplitBrainAutoResolveTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_split_brain_auto_resolve_total",
		Help: "Total successful losing-site fences while auto-resolving split brain by spec.splitBrainPolicy.sitePriorities. The label is the preferred (winning) site.",
	}, []string{"prefer_site"})

	// PrimaryReassertTotal counts restorations of writability on the last
	// failover target after it was found fenced with no writable site
	// anywhere (typically the target's own sidecar re-fenced it with a
	// stale lease right after a promotion). A steadily increasing counter
	// means something keeps fencing the primary — check the sidecars'
	// connectivity to the operator's auxiliary Service.
	PrimaryReassertTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_primary_reassert_total",
		Help: "Total number of times the operator restored writability on the promoted primary after finding it fenced with no writable site remaining.",
	}, []string{"site"})

	ReplicationLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_replication_lag_seconds",
		Help: "Replication lag in seconds on a follower site. -1 if lag is NULL (not replicating). role is spec.sites[].role (primary-candidate, dr-only, or read-only).",
	}, []string{"namespace", "group", "site", "role"})

	ReplicationRunning = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_replication_running",
		Help: "Whether a replication thread is running (1=yes, 0=no). Thread label is 'io' or 'sql'. role is spec.sites[].role.",
	}, []string{"namespace", "group", "site", "role", "thread"})

	ReplicationSourceState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_replication_source_state",
		Help: "Direct-primary replication source convergence as a state-set.",
	}, []string{"namespace", "group", "site", "state"})

	SiteState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_site_state",
		Help: "Current site state as a state-set (1 for current state, 0 for others). State is 'writable', 'read-only', 'unreachable', or 'unknown'. role is spec.sites[].role.",
	}, []string{"namespace", "group", "site", "role", "state"})

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

	// --- Restore metrics ----------------------------------------------
	//
	// Restore metrics are emitted once per successful restore Job. Labels
	// are bounded to (namespace, group, restore_kind, target_site), where
	// restore_kind is "init_from_backup" or "in_place".

	RestoreDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "bloodraven_restore_duration_seconds",
		Help: "Data-plane duration of a successful restore Job from Job StartTime to terminal success observation.",
		Buckets: []float64{
			15, 30, 60, 120, 300, 600, 900, 1800,
			3600, 7200, 14400, 28800,
		},
	}, []string{"namespace", "group", "restore_kind", "target_site"})

	RestoreLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_restore_last_success_timestamp_seconds",
		Help: "Unix timestamp of the last successful restore Job observation per restore target.",
	}, []string{"namespace", "group", "restore_kind", "target_site"})

	RestoreLastSourceSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_restore_last_source_size_bytes",
		Help: "Source backup artifact size in bytes for the most recent successful restore when known.",
	}, []string{"namespace", "group", "restore_kind", "target_site"})

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

	// --- HTTP RED metrics (aux server + sidecar) ---------------------
	//
	// Label set kept narrow (handler, method, status) to bound
	// cardinality — status is the string form of the class ("2xx",
	// "4xx", ...) rather than the exact code so the series count stays
	// manageable across dozens of cluster installs. AUDIT M6.
	HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_http_requests_total",
		Help: "Total HTTP requests served by the operator aux and sidecar mux handlers.",
	}, []string{"server", "handler", "method", "status"})

	HTTPRequestDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bloodraven_http_request_duration_seconds",
		Help:    "End-to-end server-side latency of HTTP requests served by the operator aux and sidecar mux handlers.",
		Buckets: prometheus.DefBuckets,
	}, []string{"server", "handler", "method"})

	// --- Backup encryption data-plane metrics ------------------------
	//
	// Emitted by the cmd/bloodraven encrypt-upload / decrypt-download
	// subcommands via a BLOODRAVEN_ENCRYPT_METRICS sentinel the
	// reconciler parses. See AUDIT M5. Labels are (group, profile) to
	// match the rest of the backup metric family; stage is one of
	// encrypt|decrypt|upload|download.
	BackupEncryptDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bloodraven_backup_encrypt_duration_seconds",
		Help:    "Wall-clock duration of the encrypt-upload data plane per (group, profile).",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600},
	}, []string{"group", "profile"})

	BackupDecryptDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bloodraven_backup_decrypt_duration_seconds",
		Help:    "Wall-clock duration of the decrypt-download data plane per (group, profile).",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600},
	}, []string{"group", "profile"})

	BackupEncryptBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_backup_encrypt_bytes_total",
		Help: "Plaintext bytes processed by the encrypt-upload data plane per (group, profile).",
	}, []string{"group", "profile"})

	BackupEncryptFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_backup_encrypt_failures_total",
		Help: "Total failures in the encrypt/decrypt data plane labelled by (group, profile, stage).",
	}, []string{"group", "profile", "stage"})

	// DragonflySiteUp is 1 when the operator's most recent poll
	// successfully completed INFO replication against a site's
	// Dragonfly instance, 0 otherwise.
	DragonflySiteUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloodraven_dragonfly_site_up",
		Help: "Dragonfly site reachability (1=reachable, 0=unreachable) per (group, site).",
	}, []string{"group", "site"})

	// DragonflyPromotionsTotal counts Dragonfly promotion attempts and
	// their outcome. Result labels: "success", "failed", "skipped".
	DragonflyPromotionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_dragonfly_promotions_total",
		Help: "Number of Dragonfly promotion attempts labelled by result (success|failed|skipped).",
	}, []string{"group", "target_site", "result"})

	// DragonflyManagerPanicsTotal counts panics recovered in the
	// DragonflyManager polling loop. A non-zero value means a Tick
	// would have killed the goroutine without the recovery guard;
	// alert and inspect the stack trace logged alongside.
	DragonflyManagerPanicsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bloodraven_dragonfly_manager_panics_total",
		Help: "Number of panics recovered in DragonflyManager.Tick (per group).",
	}, []string{"namespace", "name"})
)

// AllStates is the set of possible site states, used to emit the full state-set.
var AllStates = []string{"writable", "read-only", "unreachable", "unknown"}

// AllSourceStates is the bounded source-convergence state set.
var AllSourceStates = []string{"converged", "pending", "blocked"}

// --- encryption at rest ---------------------------------------------

// KeyringPhase reports the per-site keyring lifecycle phase as a set of
// one-hot gauges. Alert on bloodraven_keyring_phase{phase="sealed"} == 0
// for a site that should be protected: any other phase means the site is
// running with a writable keyring or failed to escrow one.
var KeyringPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bloodraven_keyring_phase",
	Help: "1 for the site's current keyring phase, 0 for the others.",
}, []string{"namespace", "group", "site", "phase"})

// AllKeyringPhases is the bounded keyring phase set, used to zero out
// stale series when a site transitions.
var AllKeyringPhases = []string{"pending", "unsealed", "escrowed", "sealed", "failed"}

// KeyringEscrowVersion is the escrow Secret version a site is currently
// sealed against. Monotonic per site; a jump means a rotation or a
// clone re-wrapped the keyring.
var KeyringEscrowVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bloodraven_keyring_escrow_version",
	Help: "Current keyring escrow Secret version per site.",
}, []string{"namespace", "group", "site"})

// KeyringEscrowPushesTotal counts sidecar escrow pushes by outcome.
// Sustained failures mean a site cannot be sealed and is therefore
// running unprotected.
var KeyringEscrowPushesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bloodraven_keyring_escrow_pushes_total",
	Help: "Keyring escrow pushes from the sidecar to the operator, by outcome.",
}, []string{"group", "site", "outcome"})

// KeyringRotationsTotal counts master-key rotations by outcome.
var KeyringRotationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "bloodraven_keyring_rotations_total",
	Help: "InnoDB master key rotations attempted by the sidecar, by outcome.",
}, []string{"group", "site", "outcome"})

// EncryptionCoverageGaps counts observed coverage shortfalls per site:
// user tablespaces still reporting ENCRYPTION='N'. Non-zero on a cluster
// adopted from unencrypted data whose tables were never rebuilt.
var EncryptionCoverageGaps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bloodraven_encryption_unencrypted_tablespaces",
	Help: "User tablespaces still reporting ENCRYPTION='N' on a site.",
}, []string{"namespace", "group", "site"})

// EncryptionCoverageFlag reports individual coverage booleans observed
// on the live instance (1 = encrypted). The `aspect` label is one of
// system_tablespace, redo_log, undo_log, binlog, keyring_read_only.
var EncryptionCoverageFlag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bloodraven_encryption_coverage",
	Help: "Observed data-at-rest encryption coverage per aspect (1 = on).",
}, []string{"namespace", "group", "site", "aspect"})

// SiteIdentity is the shared identity of a site-scoped gauge series.
// Extra labels (role, state, thread) are not included so callers can
// DeletePartialMatch without knowing the last emitted role.
func SiteIdentity(namespace, group, site string) prometheus.Labels {
	return prometheus.Labels{"namespace": namespace, "group": group, "site": site}
}

// DeleteSiteState removes every bloodraven_site_state series for a site.
func DeleteSiteState(namespace, group, site string) {
	SiteState.DeletePartialMatch(SiteIdentity(namespace, group, site))
}

// DeleteReplicationGauges removes lag and thread-running series for a site.
func DeleteReplicationGauges(namespace, group, site string) {
	id := SiteIdentity(namespace, group, site)
	ReplicationLag.DeletePartialMatch(id)
	ReplicationRunning.DeletePartialMatch(id)
}

// DeleteSiteGauges removes every site-scoped gauge series for a site
// (state, lag, replication threads). Used when a site or group goes away.
func DeleteSiteGauges(namespace, group, site string) {
	DeleteSiteState(namespace, group, site)
	DeleteReplicationGauges(namespace, group, site)
}

// DeleteKeyringSiteMetrics removes every gauge series for a site that is
// no longer present in the failover-group spec.
func DeleteKeyringSiteMetrics(namespace, group, site string) {
	labels := prometheus.Labels{"namespace": namespace, "group": group, "site": site}
	KeyringPhase.DeletePartialMatch(labels)
	KeyringEscrowVersion.DeletePartialMatch(labels)
	EncryptionCoverageGaps.DeletePartialMatch(labels)
	EncryptionCoverageFlag.DeletePartialMatch(labels)
}

// Register registers all metrics with the given registerer.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(PollLatency, StateTransitions, TaintOperations, WSClientCount, DNSFlipCount, FailoversTotal,
		PlannedFailoversTotal, PlannedFailoverDurationSeconds, PlannedFailoverLagWaitSeconds,
		SplitBrainAutoResolveTotal, PrimaryReassertTotal,
		ReplicationLag, ReplicationRunning, ReplicationSourceState, SiteState, DivergentTransactions, RecloneOperations,
		BackupRunsTotal, BackupDurationSeconds,
		BackupLastSuccessTimestamp, BackupLastAttemptTimestamp, BackupLastSizeBytes,
		BackupVerifiedTimestamp, BackupVerificationLastAttemptTimestamp,
		BackupVerificationRunsTotal, BackupVerificationDurationSeconds,
		BackupVerificationReplayLagSeconds,
		RestoreDurationSeconds, RestoreLastSuccessTimestamp, RestoreLastSourceSizeBytes,
		ArchiverUploadFailures, ArchiverLastUploadTimestamp, ArchiverBacklogFiles,
		HTTPRequestsTotal, HTTPRequestDurationSeconds,
		BackupEncryptDurationSeconds, BackupDecryptDurationSeconds,
		BackupEncryptBytesTotal, BackupEncryptFailuresTotal,
		DragonflySiteUp, DragonflyPromotionsTotal, DragonflyManagerPanicsTotal,
		KeyringPhase, KeyringEscrowVersion, KeyringEscrowPushesTotal, KeyringRotationsTotal,
		EncryptionCoverageGaps, EncryptionCoverageFlag)
}

// StatusClass returns "2xx", "3xx", "4xx", "5xx" for an HTTP status
// code. Narrowing to a class keeps the label cardinality bounded for
// Prometheus; the exact code remains available in access logs via the
// auxLoggingMiddleware in cmd/bloodraven/main.go (AUDIT M4/M6).
func StatusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}
