package scenarios

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario75FullRestoreSessionsSpecRemoval())
}

const (
	s75ID          = "75-full-restore-three-site-sessions-spec-removal"
	s75MarkerDB    = "chaos_s75_marker"
	s75Profile     = "pvc-e2e-encrypted"
	s75AppUser     = "s75app"
	s75AppPassword = "s75-app-pw"
	s75ClientPod   = "s75-app-sessions"
	// s75DropMarker is the restore Job's log line printed right before it
	// drops the marker schema.
	s75DropMarker = "BLOODRAVEN_DROP_SCHEMA schema=" + s75MarkerDB
	// s75GuardWindow is how long the scenario watches the guards after
	// spec.restoreInPlace is removed while the Job is held in its DROP.
	s75GuardWindow = 40 * time.Second
)

// scenario75FullRestoreSessionsSpecRemoval is the three-site full-instance
// in-place restore PR #197 named as its missing live coverage (#214).
//
// The backup is written to an encrypted profile on an operator-provisioned
// PVC, so the restore Job decrypts from a read-only claim into a staging
// directory that is mounted read-only too. Marker row 1 is in the backup,
// row 2 is written after it and reaches every peer.
//
// An application account without CONNECTION_ADMIN holds one session
// through the -primary Service and one through the primary's per-site
// Service from a pod in the cluster, and probes new connections through
// both every two seconds. The Fencing step must end both sessions and
// refuse new ones while the restore runs.
//
// The window for removing the spec is made deterministic, not timed: an
// operator-account session (which the fence keeps, by contract) holds a
// metadata lock on the marker table, so the Job blocks in its DROP of the
// marker schema. With the Job held there, the scenario removes
// spec.restoreInPlace and checks that every guard stays up, then releases
// the lock. The run must end Succeeded, re-applying the same confirm must
// start nothing, and the reclone queue must replace the data on both
// peers (the other candidate and the ReadOnly reader).
func scenario75FullRestoreSessionsSpecRemoval() runner.Scenario {
	return runner.Scenario{
		ID:    s75ID,
		Title: "Three-site full-instance restore: application sessions fenced, spec removed mid-Restoring, peers recloned",
		Hypothesis: "A full-instance in-place restore from an encrypted backup on a PVC decrypts from a read-only claim and " +
			"succeeds. Its Fencing step ends application sessions held through the -primary and the per-site Service and " +
			"refuses new ones (offline_mode, RestoreOfflineFenceApplied) while the restore runs. Removing " +
			"spec.restoreInPlace while the Job runs emits RestoreInPlaceSpecRemoved and keeps every guard (fence, " +
			"offline_mode, peer hold, no failover) until the Job finishes, after which the run ends Succeeded and " +
			"offline_mode returns to OFF. Re-applying the same confirm starts nothing. Every peer is recloned " +
			"(RestoreRecloneSucceeded, RestoreReclonesDrained) and holds the backup's row and not the later one.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#75-full-instance-restore-sessions-spec-removal",
		Timeout:           30 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          s75Precheck,
		Steps: []runner.Step{
			s75ConfigureProfile(),
			s75SeedMarkerAndAppUser(),
			s75CreateBackup(),
			s75WriteAfterBackup(),
			s75OpenAppSessions(),
			s75HoldDropAndRequestRestore(),
			s75ObserveFenceWhileJobHeld(),
			s75RemoveSpecMidRestoring(),
			s75ObserveSucceededAndRelease(),
			s75ReapplySameConfirm(),
			s75WaitPeersRecloned(),
			s75VerifyGroupAndPeers(),
		},
		Cleanup:        s75Cleanup,
		CleanupTimeout: 25 * time.Minute,
	}
}

func s75Precheck(ctx context.Context, env *runner.Env) error {
	if err := assertReplicationRunningPrecheck(ctx, env); err != nil {
		return err
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	if mfg.Spec.RestoreInPlace != nil {
		return fmt.Errorf("precheck: spec.restoreInPlace already set; run ./playground/reset-mysql.sh")
	}
	if mfg.Status.RestoreReclones != nil {
		return fmt.Errorf("precheck: status.restoreReclones is set (%s); run ./playground/reset-mysql.sh", mfg.Status.RestoreReclones.Phase)
	}
	if mfg.Status.RestoreOfflineFence != nil {
		return fmt.Errorf("precheck: status.restoreOfflineFence is set (site %s); run ./playground/reset-mysql.sh", mfg.Status.RestoreOfflineFence.Site)
	}
	if len(mfg.Spec.Sites) != 3 {
		return fmt.Errorf("precheck: the scenario needs the three-site playground, the group has %d sites", len(mfg.Spec.Sites))
	}
	if _, err := findReadOnlyReaderSite(mfg); err != nil {
		return fmt.Errorf("precheck: %w", err)
	}
	return nil
}

func s75PVCName(env *runner.Env) string {
	return pitrprofile.OwnedBackupPVCName(env.FG, s75Profile)
}

func s75ConfigureProfile() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "configure an encrypted backup profile on an operator-provisioned PVC",
		Do: func(ctx context.Context, env *runner.Env) error {
			existed, err := env.Kube.PVCExists(ctx, env.Namespace, s75PVCName(env))
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "s75PVCExisted", strconv.FormatBool(existed)); err != nil {
				return err
			}
			secret, err := ensureBackupPassphraseSecret(ctx, env)
			if err != nil {
				return err
			}
			if err := patchBackupSpec(ctx, env, pvcBackupProfileSpec(s75Profile, "playground-fast", "1Gi", secret)); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			if err := waitForPVCBackupProfile(waitCtx, env, s75Profile); err != nil {
				return err
			}
			return s75BindBackupClaim(ctx, env)
		},
	}
}

const s75BinderPod = "s75-backup-claim-binder"

// s75BindBackupClaim binds the profile's claim on the active primary's
// node. The playground's local-path provisioner creates a volume with a
// helper pod that does not tolerate the operator's NoExecute db-readonly
// taint, so a claim first consumed by a backup Job scheduled onto a
// read-only site's node is never provisioned. The primary's node is not
// tainted; once the claim is bound there, the backup and restore Jobs
// follow the volume's node affinity.
func s75BindBackupClaim(ctx context.Context, env *runner.Env) error {
	claim := s75PVCName(env)
	pvcs := env.Kube.Kubernetes.CoreV1().PersistentVolumeClaims(env.Namespace)
	pods := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		pvc, err := pvcs.Get(ctx, claim, metav1.GetOptions{})
		switch {
		case err == nil && pvc.Status.Phase == corev1.ClaimBound:
			env.Capture.Note(fmt.Sprintf("backup claim %s bound to %s (%s)", claim, pvc.Spec.VolumeName, pvc.Annotations["volume.kubernetes.io/selected-node"]))
			if derr := pods.Delete(ctx, s75BinderPod, metav1.DeleteOptions{}); derr != nil && !apierrors.IsNotFound(derr) {
				return fmt.Errorf("delete %s: %w", s75BinderPod, derr)
			}
			return nil
		case err == nil:
			if _, gerr := pods.Get(ctx, s75BinderPod, metav1.GetOptions{}); apierrors.IsNotFound(gerr) {
				mfg, merr := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if merr != nil {
					return merr
				}
				primaryPod, perr := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, mfg.Status.ActiveSite)
				if perr != nil {
					return perr
				}
				deadlineSeconds := int64(600)
				binder := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: s75BinderPod, Namespace: env.Namespace, Labels: map[string]string{
						"playground.bloodraven.shipstream.io/scenario":       s75ID,
						"playground.bloodraven.shipstream.io/created-by-e2e": "true",
					}},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyNever,
						ActiveDeadlineSeconds: &deadlineSeconds,
						NodeSelector:          map[string]string{corev1.LabelHostname: primaryPod.Spec.NodeName},
						Containers: []corev1.Container{{
							Name: "binder", Image: mfg.Spec.Image, ImagePullPolicy: corev1.PullIfNotPresent,
							Command:      []string{"sleep", "600"},
							VolumeMounts: []corev1.VolumeMount{{Name: "claim", MountPath: "/claim"}},
						}},
						Volumes: []corev1.Volume{{Name: "claim", VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
						}}},
					},
				}
				if _, cerr := pods.Create(ctx, binder, metav1.CreateOptions{}); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
					return fmt.Errorf("create %s: %w", s75BinderPod, cerr)
				}
			}
		case !apierrors.IsNotFound(err):
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("backup claim %s not bound within 3m", claim)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// s75Peers returns the sites other than active, in spec order.
func s75Peers(mfg *v1alpha1.MysqlFailoverGroup, active string) []string {
	var peers []string
	for _, s := range mfg.Spec.Sites {
		if s.Name != active {
			peers = append(peers, s.Name)
		}
	}
	return peers
}

func s75SeedMarkerAndAppUser() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "create the marker schema with row 1 and a non-admin application account",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, _, err := activeAndReplica(ctx, env)
			if err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			for _, q := range []string{
				"CREATE DATABASE IF NOT EXISTS " + s75MarkerDB,
				"CREATE TABLE IF NOT EXISTS " + s75MarkerDB + ".marker (id INT PRIMARY KEY, note VARCHAR(64))",
				fmt.Sprintf("INSERT INTO %s.marker (id, note) VALUES (1, 'in-backup') ON DUPLICATE KEY UPDATE note=VALUES(note)", s75MarkerDB),
				fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'", s75AppUser, s75AppPassword),
				fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON %s.* TO '%s'@'%%'", s75MarkerDB, s75AppUser),
			} {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("seed stmt %q: %w", q, err)
				}
			}
			if err := s75RequireNonAdminAppUser(ctx, primary); err != nil {
				return err
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			for _, peer := range s75Peers(mfg, active) {
				if err := waitForReplicaGTID(ctx, env, active, peer); err != nil {
					return fmt.Errorf("peer %s did not receive the seed: %w", peer, err)
				}
			}
			env.Capture.Note(fmt.Sprintf("seeded %s.marker row 1 and account %s on %s", s75MarkerDB, s75AppUser, active))
			return nil
		},
	}
}

// s75RequireNonAdminAppUser fails when the application account holds a
// privilege that exempts it from offline_mode: such a session would not
// be fenced, and the scenario's session assertions would prove nothing.
func s75RequireNonAdminAppUser(ctx context.Context, c *pgmysql.SiteClient) error {
	rows, err := c.DB.QueryContext(ctx, fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%'", s75AppUser))
	if err != nil {
		return fmt.Errorf("show grants for %s: %w", s75AppUser, err)
	}
	defer rows.Close()
	var grants []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return err
		}
		grants = append(grants, g)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, g := range grants {
		u := strings.ToUpper(g)
		if strings.Contains(u, "CONNECTION_ADMIN") || strings.Contains(u, "SUPER") || strings.Contains(u, "ALL PRIVILEGES ON *.*") {
			return fmt.Errorf("application account %s must not be exempt from offline_mode, has: %s", s75AppUser, g)
		}
	}
	return nil
}

func s75CreateBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "back up to the encrypted PVC profile and wait Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			name := "s75-backup-" + backupRunStamp(env)
			if err := ctxStash(ctx, env, "backupName", name); err != nil {
				return err
			}
			if err := createMysqlBackupForProfile(ctx, env, name, s75ID, s75Profile); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			b, err := waitForBackupPhase(waitCtx, env, name, v1alpha1.BackupPhaseSucceeded)
			if err != nil {
				return err
			}
			env.Capture.Note("backup succeeded: " + backupStatusSummary(b))
			if st := b.Status.ArtifactStorage; st == nil || st.Type != v1alpha1.BackupStoragePVC {
				return fmt.Errorf("backup status.artifactStorage=%+v, want a PVC artifact", b.Status.ArtifactStorage)
			}
			if b.Status.Encryption == nil || b.Status.Encryption.Mode != v1alpha1.EncryptionModePassphrase {
				return fmt.Errorf("backup status.encryption=%+v, want mode %s", b.Status.Encryption, v1alpha1.EncryptionModePassphrase)
			}
			return nil
		},
	}
}

func s75WriteAfterBackup() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "write marker row 2 after the backup and wait for every peer",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			if _, err := primary.Exec(ctx, fmt.Sprintf("INSERT INTO %s.marker (id, note) VALUES (2, 'after-backup')", s75MarkerDB)); err != nil {
				return fmt.Errorf("write post-backup marker: %w", err)
			}
			for _, peer := range s75Peers(mfg, active) {
				if err := waitForReplicaGTID(ctx, env, active, peer); err != nil {
					return fmt.Errorf("peer %s did not receive row 2: %w", peer, err)
				}
				if err := s75RequireMarkerRows(ctx, env, peer, []int{1, 2}); err != nil {
					return err
				}
			}
			return ctxStash(ctx, env, "activeAtRestore", active)
		},
	}
}

// s75RequireMarkerRows checks that site holds exactly the marker rows want.
func s75RequireMarkerRows(ctx context.Context, env *runner.Env, site string, want []int) error {
	c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site, env.Creds)
	if err != nil {
		return fmt.Errorf("open %s: %w", site, err)
	}
	defer c.Close()
	rows, err := c.DB.QueryContext(ctx, fmt.Sprintf("SELECT id FROM %s.marker ORDER BY id", s75MarkerDB))
	if err != nil {
		return fmt.Errorf("read %s.marker on %s: %w", s75MarkerDB, site, err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return err
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("%s.marker on %s holds rows %v, want %v", s75MarkerDB, site, got, want)
	}
	return nil
}

func s75PrimaryServiceName(env *runner.Env) string {
	return fmt.Sprintf("mysql-%s-primary", env.FG)
}

func s75ServiceHost(env *runner.Env, service string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", service, env.Namespace)
}

// s75ClientScript holds one application session through each Service and
// probes a new connection through each every two seconds. Every line is
// "<unix seconds> <kind> via=<primary|site> rc=<exit code> out=<output>".
const s75ClientScript = `set -u
oneline() { printf '%s' "$1" | tr '\n' ' '; }
hold() {
  local via=$1 host=$2 out rc
  echo "$(date +%s) S75_HOLD_START via=$via rc=0 out="
  out=$(mysql --protocol=TCP -h "$host" -u "$APP_USER" --connect-timeout=5 -N -B -e "SELECT SLEEP(86400) AS s75_hold_$via" 2>&1)
  rc=$?
  echo "$(date +%s) S75_HOLD_END via=$via rc=$rc out=$(oneline "$out")"
}
probe() {
  local via=$1 host=$2 out rc
  out=$(mysql --protocol=TCP -h "$host" -u "$APP_USER" --connect-timeout=3 -N -B -e "SELECT 'probe-ok'" 2>&1)
  rc=$?
  echo "$(date +%s) S75_PROBE via=$via rc=$rc out=$(oneline "$out")"
}
hold primary "$PRIMARY_HOST" &
hold site "$SITE_HOST" &
while true; do
  probe primary "$PRIMARY_HOST"
  probe site "$SITE_HOST"
  sleep 2
done
`

// s75ClientRecord is one line of the session client's log.
type s75ClientRecord struct {
	At   time.Time
	Kind string
	Via  string
	RC   int
	Out  string
}

// parseS75ClientLog decodes the session client's log. Lines that are not
// records (for example a mysql client warning on stderr) are skipped.
func parseS75ClientLog(data []byte) []s75ClientRecord {
	var out []s75ClientRecord
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.SplitN(strings.TrimSpace(line), " ", 5)
		if len(fields) < 4 || !strings.HasPrefix(fields[1], "S75_") {
			continue
		}
		sec, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			continue
		}
		via, ok := strings.CutPrefix(fields[2], "via=")
		if !ok {
			continue
		}
		rcText, ok := strings.CutPrefix(fields[3], "rc=")
		if !ok {
			continue
		}
		rc, err := strconv.Atoi(rcText)
		if err != nil {
			continue
		}
		rec := s75ClientRecord{At: time.Unix(sec, 0), Kind: fields[1], Via: via, RC: rc}
		if len(fields) == 5 {
			rec.Out = strings.TrimSpace(strings.TrimPrefix(fields[4], "out="))
		}
		out = append(out, rec)
	}
	return out
}

// s75ProbesSince returns the probe records through via at or after t.
func s75ProbesSince(records []s75ClientRecord, via string, t time.Time) []s75ClientRecord {
	var out []s75ClientRecord
	for _, r := range records {
		if r.Kind == "S75_PROBE" && r.Via == via && !r.At.Before(t) {
			out = append(out, r)
		}
	}
	return out
}

// s75HoldEnd returns the S75_HOLD_END record of via, if the session ended.
func s75HoldEnd(records []s75ClientRecord, via string) (s75ClientRecord, bool) {
	for _, r := range records {
		if r.Kind == "S75_HOLD_END" && r.Via == via {
			return r, true
		}
	}
	return s75ClientRecord{}, false
}

func s75ClientLog(ctx context.Context, env *runner.Env) ([]s75ClientRecord, []byte, error) {
	data, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).GetLogs(s75ClientPod, &corev1.PodLogOptions{Container: "client"}).DoRaw(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s log: %w", s75ClientPod, err)
	}
	return parseS75ClientLog(data), data, nil
}

func s75ClientPodSpec(env *runner.Env, image, primaryHost, siteHost string) *corev1.Pod {
	deadline := int64(3600)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s75ClientPod,
			Namespace: env.Namespace,
			Labels: map[string]string{
				"playground.bloodraven.shipstream.io/scenario":       s75ID,
				"playground.bloodraven.shipstream.io/created-by-e2e": "true",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:         corev1.RestartPolicyNever,
			ActiveDeadlineSeconds: &deadline,
			Tolerations:           []corev1.Toleration{{Key: v1alpha1.ReadOnlyTaintKey(env.Namespace, env.FG), Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{{
				Name:            "client",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"bash", "-c", s75ClientScript},
				Env: []corev1.EnvVar{
					{Name: "APP_USER", Value: s75AppUser},
					{Name: "MYSQL_PWD", Value: s75AppPassword},
					{Name: "PRIMARY_HOST", Value: primaryHost},
					{Name: "SITE_HOST", Value: siteHost},
				},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("10m"),
					corev1.ResourceMemory: resource.MustParse("32Mi"),
				}},
			}},
		},
	}
}

// s75CountHoldSessions counts the client's held sessions on site per via.
func s75CountHoldSessions(ctx context.Context, c *pgmysql.SiteClient) (map[string]int64, error) {
	counts := map[string]int64{}
	for _, via := range []string{"primary", "site"} {
		n, err := c.ScalarInt(ctx, "SELECT COUNT(*) FROM performance_schema.processlist WHERE USER = ? AND INFO LIKE ?",
			s75AppUser, "%s75_hold_"+via+"%")
		if err != nil {
			return nil, fmt.Errorf("count %s sessions via %s: %w", s75AppUser, via, err)
		}
		counts[via] = n
	}
	return counts, nil
}

func s75OpenAppSessions() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "hold application sessions through the -primary and the per-site Service",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := ctxFetch(env, "activeAtRestore")
			siteService := pgkube.MysqlDeploymentName(env.FG, active)
			if err := ctxStash(ctx, env, "s75SiteService", siteService); err != nil {
				return err
			}
			if err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).Delete(ctx, s75ClientPod, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete leftover %s: %w", s75ClientPod, err)
			}
			if err := s75WaitPodGone(ctx, env, 90*time.Second); err != nil {
				return err
			}
			pod := s75ClientPodSpec(env, mfg.Spec.Image, s75ServiceHost(env, s75PrimaryServiceName(env)), s75ServiceHost(env, siteService))
			if _, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("create %s: %w", s75ClientPod, err)
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			deadline := time.Now().Add(3 * time.Minute)
			var last string
			for {
				counts, cerr := s75CountHoldSessions(ctx, primary)
				records, _, lerr := s75ClientLog(ctx, env)
				switch {
				case cerr != nil:
					last = cerr.Error()
				case lerr != nil:
					last = lerr.Error()
				default:
					okPrimary := s75LastProbeOK(records, "primary")
					okSite := s75LastProbeOK(records, "site")
					last = fmt.Sprintf("held sessions on %s: %v; last probe ok primary=%v site=%v", active, counts, okPrimary, okSite)
					if counts["primary"] == 1 && counts["site"] == 1 && okPrimary && okSite {
						env.Capture.Note("application sessions open: " + last)
						return nil
					}
					for _, via := range []string{"primary", "site"} {
						if end, ended := s75HoldEnd(records, via); ended {
							return fmt.Errorf("the session through the %s Service ended before the restore: rc=%d %s", via, end.RC, end.Out)
						}
					}
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("application sessions did not open within 3m (last: %s)", last)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}
		},
	}
}

func s75LastProbeOK(records []s75ClientRecord, via string) bool {
	probes := s75ProbesSince(records, via, time.Time{})
	if len(probes) == 0 {
		return false
	}
	last := probes[len(probes)-1]
	return last.RC == 0 && strings.Contains(last.Out, "probe-ok")
}

func s75WaitPodGone(ctx context.Context, env *runner.Env, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).Get(ctx, s75ClientPod, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pod %s still present after %s", s75ClientPod, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// s75Holder is the operator-account session that holds a metadata lock
// on the marker table, so the restore Job blocks in its DROP.
type s75Holder struct {
	client *pgmysql.SiteClient
	conn   *sql.Conn
}

var (
	s75HoldersMu sync.Mutex
	s75Holders   = map[*runner.Env]*s75Holder{}
)

// s75ReleaseHolder ends the lock holder's transaction and closes it. It
// reports whether a holder was open.
func s75ReleaseHolder(ctx context.Context, env *runner.Env) (bool, error) {
	s75HoldersMu.Lock()
	h := s75Holders[env]
	delete(s75Holders, env)
	s75HoldersMu.Unlock()
	if h == nil {
		return false, nil
	}
	_, cerr := h.conn.ExecContext(ctx, "COMMIT")
	_ = h.conn.Close()
	_ = h.client.Close()
	return true, cerr
}

func s75HoldDropAndRequestRestore() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "hold a metadata lock on the marker table as the operator account, then request the full-instance restore",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeAtRestore")
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != active {
				return fmt.Errorf("status.activeSite moved %q -> %q before the restore", active, mfg.Status.ActiveSite)
			}
			if err := clearRetainedInPlaceRestoreJobs(ctx, env, "s75-retained-restore-job", active); err != nil {
				env.Capture.Note("pre-attempt retained restore-job sweep: " + err.Error())
			}
			if peer, perr := PeerOf(mfg, active); perr == nil {
				if err := ctxStash(ctx, env, "s75CandidatePeer", peer); err != nil {
					return err
				}
				if err := stashMetricCounter(ctx, env, "s75FailoversToPeerBefore", "bloodraven_failovers_total", map[string]string{"target_site": peer}); err != nil {
					return err
				}
			}

			client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open lock holder on %s: %w", active, err)
			}
			conn, err := client.DB.Conn(ctx)
			if err != nil {
				_ = client.Close()
				return fmt.Errorf("pin lock holder connection: %w", err)
			}
			holder := &s75Holder{client: client, conn: conn}
			s75HoldersMu.Lock()
			s75Holders[env] = holder
			s75HoldersMu.Unlock()
			var prior int64
			if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.offline_mode").Scan(&prior); err != nil {
				return fmt.Errorf("read offline_mode before the restore: %w", err)
			}
			if prior != 0 {
				return fmt.Errorf("offline_mode is already ON on %s before the restore", active)
			}
			if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
				return fmt.Errorf("begin lock holder transaction: %w", err)
			}
			var n int64
			if err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s.marker", s75MarkerDB)).Scan(&n); err != nil {
				return fmt.Errorf("take the metadata lock on %s.marker: %w", s75MarkerDB, err)
			}

			// The sessions must still be up right before the request.
			counts, err := s75CountHoldSessions(ctx, client)
			if err != nil {
				return err
			}
			if counts["primary"] != 1 || counts["site"] != 1 {
				return fmt.Errorf("application sessions on %s before the restore: %v, want one per Service", active, counts)
			}

			confirm := time.Now().UTC().Truncate(time.Second)
			if rip := mfg.Status.RestoreInPlace; rip != nil && rip.ConfirmTokenUsed != "" {
				if used, perr := time.Parse(time.RFC3339, rip.ConfirmTokenUsed); perr == nil && !confirm.After(used) {
					confirm = used.Add(time.Second)
				}
			}
			confirmText := confirm.Format(time.RFC3339)
			requested := time.Now()
			if err := ctxStash(ctx, env, "s75Confirm", confirmText); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "s75RequestedAt", requested.Format(time.RFC3339Nano)); err != nil {
				return err
			}
			if _, err := patchRestoreInPlace(ctx, env, s75RestoreSpec(env, confirmText)); err != nil {
				return fmt.Errorf("patch full-instance restoreInPlace: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("holding a metadata lock on %s.marker (%d rows) as the operator account; requested the restore of %s with confirm=%s",
				s75MarkerDB, n, active, confirmText))
			return nil
		},
	}
}

func s75RestoreSpec(env *runner.Env, confirm string) v1alpha1.RestoreInPlaceSpec {
	return v1alpha1.RestoreInPlaceSpec{
		Confirm: confirm,
		Source:  v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: ctxFetch(env, "backupName")}},
	}
}

// s75SameRunStart reports whether rip is the status of the run this
// scenario started (same startTime, to the second the API keeps).
func s75SameRunStart(env *runner.Env, rip *v1alpha1.RestoreInPlaceStatus) bool {
	want := s75StashedTime(env, "s75RunStart")
	return rip.StartTime != nil && !want.IsZero() && rip.StartTime.Time.Equal(want)
}

func s75StashedTime(env *runner.Env, key string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, ctxFetch(env, key))
	if err != nil {
		return time.Time{}
	}
	return t
}

// s75RestoreJobReadsReadOnlyPVC checks the restore Job's wiring for an
// encrypted backup on claim: the decrypt-download init container reads
// the claim through a read-only volume and mount, the loader reads the
// decrypted staging directory read-only, nothing mounts the claim
// read-write, and the load keeps no progress file (#197: mysqlsh wrote
// it beside the dump, which failed on read-only sources).
func s75RestoreJobReadsReadOnlyPVC(job *batchv1.Job, claim string) error {
	spec := job.Spec.Template.Spec
	claimVolumes := map[string]bool{}
	for _, v := range spec.Volumes {
		if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != claim {
			continue
		}
		if !v.PersistentVolumeClaim.ReadOnly {
			return fmt.Errorf("volume %s mounts claim %s without readOnly", v.Name, claim)
		}
		claimVolumes[v.Name] = true
	}
	if len(claimVolumes) == 0 {
		return fmt.Errorf("no volume of the restore Job mounts claim %s", claim)
	}
	all := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
	for _, c := range all {
		for _, m := range c.VolumeMounts {
			if claimVolumes[m.Name] && !m.ReadOnly {
				return fmt.Errorf("container %s mounts claim volume %s read-write", c.Name, m.Name)
			}
		}
	}
	var decrypt *corev1.Container
	for i := range spec.InitContainers {
		if spec.InitContainers[i].Name == "decrypt-download" {
			decrypt = &spec.InitContainers[i]
		}
	}
	if decrypt == nil {
		return fmt.Errorf("the restore Job has no decrypt-download init container")
	}
	if !slices.ContainsFunc(decrypt.VolumeMounts, func(m corev1.VolumeMount) bool { return claimVolumes[m.Name] }) {
		return fmt.Errorf("decrypt-download does not mount claim %s", claim)
	}
	if len(spec.Containers) == 0 {
		return fmt.Errorf("the restore Job has no containers")
	}
	for _, c := range spec.Containers {
		staged := false
		for _, m := range c.VolumeMounts {
			if m.Name == "restore-decrypted" {
				if !m.ReadOnly {
					return fmt.Errorf("container %s mounts the decrypted staging directory read-write", c.Name)
				}
				staged = true
			}
		}
		if !staged {
			return fmt.Errorf("container %s does not read the decrypted staging directory", c.Name)
		}
		progress, found := "", false
		for _, e := range c.Env {
			if e.Name == "BLOODRAVEN_LOAD_PROGRESS_FILE" {
				progress, found = e.Value, true
			}
		}
		if !found || progress != "" {
			return fmt.Errorf("container %s: BLOODRAVEN_LOAD_PROGRESS_FILE found=%v value=%q, want set and empty", c.Name, found, progress)
		}
	}
	return nil
}

// s75WaitDropBlocked waits until the restore Job logged the DROP of the
// marker schema and the primary shows that DROP waiting for a metadata
// lock: the Job is running, past its offline_mode check, and held.
func s75WaitDropBlocked(ctx context.Context, env *runner.Env, primary *pgmysql.SiteClient, jobName string) error {
	deadline := time.Now().Add(5 * time.Minute)
	var last string
	for {
		logged, err := s62JobLogContains(ctx, env, jobName, s75DropMarker)
		if err != nil {
			last = err.Error()
		} else {
			waiting, qerr := primary.ScalarInt(ctx,
				"SELECT COUNT(*) FROM performance_schema.processlist WHERE INFO LIKE ? AND STATE LIKE 'Waiting for%metadata lock'",
				"DROP DATABASE%"+s75MarkerDB+"%")
			if qerr != nil {
				last = qerr.Error()
			} else {
				last = fmt.Sprintf("job logged %q: %v; DROP waiting on a metadata lock: %d", s75DropMarker, logged, waiting)
				if logged && waiting == 1 {
					env.Capture.Note(last)
					return nil
				}
			}
		}
		job, jerr := env.Kube.Kubernetes.BatchV1().Jobs(env.Namespace).Get(ctx, jobName, metav1.GetOptions{})
		if jerr == nil && (job.Status.Succeeded > 0 || job.Status.Failed > 0) {
			captureJobPodLogs(ctx, env, jobName, "s75-restore-job")
			return fmt.Errorf("restore Job %s finished (succeeded=%d failed=%d) before it blocked in the DROP of %s; the lock holder did not hold (last: %s)",
				jobName, job.Status.Succeeded, job.Status.Failed, s75MarkerDB, last)
		}
		if time.Now().After(deadline) {
			captureJobPodLogs(ctx, env, jobName, "s75-restore-job")
			return fmt.Errorf("restore Job %s did not block in the DROP of %s within 5m (last: %s)", jobName, s75MarkerDB, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func s75ObserveFenceWhileJobHeld() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "Fencing ends the sessions and refuses new ones; the Job reads the read-only PVC and blocks in its DROP",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeAtRestore")
			requested := s75StashedTime(env, "s75RequestedAt")
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
			// A status left by an earlier run stays until this request is
			// accepted; wait for the run of this request first.
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "restoreInPlace run of this request", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				rip := mfg.Status.RestoreInPlace
				if rip == nil {
					return false, "no status", nil
				}
				return rip.RunConfirm == ctxFetch(env, "s75Confirm"), fmt.Sprintf("phase=%s runConfirm=%s message=%q", rip.Phase, rip.RunConfirm, rip.Message), nil
			})
			if err != nil {
				return err
			}
			rip, err := waitRestoreInPlaceActive(waitCtx, env, nil)
			if err != nil {
				return fmt.Errorf("restore Job did not become active: %w", err)
			}
			if rip.TargetSite != active {
				return fmt.Errorf("restore targetSite=%q, want the active site %q", rip.TargetSite, active)
			}
			if err := ctxStash(ctx, env, "restoreJobName", rip.JobName); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "s75RunID", rip.RunID); err != nil {
				return err
			}
			if rip.StartTime == nil {
				return fmt.Errorf("status.restoreInPlace of the running restore has no startTime")
			}
			if err := ctxStash(ctx, env, "s75RunStart", rip.StartTime.UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			if err := s75WaitDropBlocked(ctx, env, primary, rip.JobName); err != nil {
				return err
			}
			held := time.Now()

			job, err := env.Kube.Kubernetes.BatchV1().Jobs(env.Namespace).Get(ctx, rip.JobName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get restore Job %s: %w", rip.JobName, err)
			}
			if err := ctxStash(ctx, env, "s75JobUID", string(job.UID)); err != nil {
				return err
			}
			if err := s75RestoreJobReadsReadOnlyPVC(job, s75PVCName(env)); err != nil {
				return fmt.Errorf("restore Job %s: %w", rip.JobName, err)
			}
			env.Capture.Note(fmt.Sprintf("restore Job %s decrypts from read-only claim %s and loads a read-only staging directory with no progress file", rip.JobName, s75PVCName(env)))

			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			fence := mfg.Status.RestoreOfflineFence
			switch {
			case fence == nil:
				return fmt.Errorf("status.restoreOfflineFence is not set while the restore Job runs")
			case fence.Site != active:
				return fmt.Errorf("status.restoreOfflineFence.site=%q, want %q", fence.Site, active)
			case fence.RunID != rip.RunID:
				return fmt.Errorf("status.restoreOfflineFence.runID=%q, want the run's %q", fence.RunID, rip.RunID)
			case fence.PriorOfflineMode == nil || *fence.PriorOfflineMode:
				return fmt.Errorf("status.restoreOfflineFence.priorOfflineMode=%v, want recorded false", fence.PriorOfflineMode)
			case !fence.SessionsEvicted:
				return fmt.Errorf("status.restoreOfflineFence.sessionsEvicted=false while the Job runs")
			}
			if _, ok, err := findMFGEvent(ctx, env, requested, v1alpha1.EventRestoreOfflineFenceApplied, active); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("no %s event for %s since the request", v1alpha1.EventRestoreOfflineFenceApplied, active)
			}
			if err := s75RequireOfflineMode(ctx, primary, true); err != nil {
				return err
			}
			counts, err := s75CountHoldSessions(ctx, primary)
			if err != nil {
				return err
			}
			if counts["primary"] != 0 || counts["site"] != 0 {
				return fmt.Errorf("application sessions survived the fence on %s: %v", active, counts)
			}
			if err := s75RequireSessionsEnded(ctx, env, requested); err != nil {
				return err
			}
			if err := s75RequireProbesRefused(ctx, env, held, 2); err != nil {
				return err
			}
			if err := s75RequirePrimaryServiceEmpty(ctx, env); err != nil {
				return err
			}
			if err := s75RequirePeersHeld(ctx, env, mfg, active); err != nil {
				return err
			}
			return ctxStash(ctx, env, "s75HeldAt", held.Format(time.RFC3339Nano))
		},
	}
}

func s75RequireOfflineMode(ctx context.Context, c *pgmysql.SiteClient, want bool) error {
	on, err := c.ScalarInt(ctx, "SELECT @@GLOBAL.offline_mode")
	if err != nil {
		return fmt.Errorf("read offline_mode on %s: %w", c.Site, err)
	}
	if (on == 1) != want {
		return fmt.Errorf("offline_mode on %s is %d, want %v", c.Site, on, want)
	}
	return nil
}

// s75RequireSessionsEnded checks that both held sessions ended with an
// error after the restore was requested.
func s75RequireSessionsEnded(ctx context.Context, env *runner.Env, requested time.Time) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		records, _, err := s75ClientLog(ctx, env)
		if err != nil {
			return err
		}
		var missing []string
		for _, via := range []string{"primary", "site"} {
			end, ok := s75HoldEnd(records, via)
			if !ok {
				missing = append(missing, via)
				continue
			}
			if end.RC == 0 {
				return fmt.Errorf("the session through the %s Service ended without an error: %s", via, end.Out)
			}
			if end.At.Before(requested.Add(-time.Second)) {
				return fmt.Errorf("the session through the %s Service ended at %s, before the restore was requested at %s: %s",
					via, end.At.Format(time.RFC3339), requested.Format(time.RFC3339), end.Out)
			}
			env.Capture.Note(fmt.Sprintf("session through the %s Service ended at %s: rc=%d %s", via, end.At.Format(time.RFC3339), end.RC, end.Out))
		}
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the client has not seen its sessions through %v end, although the server shows none", missing)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// s75RequireProbesRefused waits for at least n new-connection probes per
// Service at or after since and requires every one to fail: through the
// per-site Service with ER_SERVER_OFFLINE_MODE (3032), through -primary
// with any error (the Service has no endpoint while the primary is fenced).
func s75RequireProbesRefused(ctx context.Context, env *runner.Env, since time.Time, n int) error {
	deadline := time.Now().Add(time.Minute)
	for {
		records, _, err := s75ClientLog(ctx, env)
		if err != nil {
			return err
		}
		primary := s75ProbesSince(records, "primary", since)
		site := s75ProbesSince(records, "site", since)
		for _, p := range primary {
			if p.RC == 0 {
				return fmt.Errorf("a new application connection through -primary succeeded during the restore at %s: %s", p.At.Format(time.RFC3339), p.Out)
			}
		}
		for _, p := range site {
			if p.RC == 0 || !strings.Contains(p.Out, "3032") {
				return fmt.Errorf("a new application connection through the per-site Service at %s was not refused by offline_mode: rc=%d %s",
					p.At.Format(time.RFC3339), p.RC, p.Out)
			}
		}
		if len(primary) >= n && len(site) >= n {
			env.Capture.Note(fmt.Sprintf("new application connections refused since %s: -primary %d probe(s) (last: %s), per-site %d probe(s) (last: %s)",
				since.Format(time.RFC3339), len(primary), primary[len(primary)-1].Out, len(site), site[len(site)-1].Out))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d -primary and %d per-site probes since %s", len(primary), len(site), since.Format(time.RFC3339))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func s75RequirePrimaryServiceEmpty(ctx context.Context, env *runner.Env) error {
	state, err := env.Kube.ServiceEndpointState(ctx, env.Namespace, s75PrimaryServiceName(env))
	if err != nil {
		return fmt.Errorf("read %s EndpointSlices: %w", s75PrimaryServiceName(env), err)
	}
	if ready := state.ReadyPodNames("mysql"); len(ready) > 0 {
		return fmt.Errorf("%s still has ready endpoints %v while the primary is fenced", s75PrimaryServiceName(env), ready)
	}
	return nil
}

// s75RequirePeersHeld checks the reclone queue armed for every peer, the
// ReadOnly reader last, and every peer's replication receiver stopped.
func s75RequirePeersHeld(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup, active string) error {
	q := mfg.Status.RestoreReclones
	if q == nil {
		return fmt.Errorf("status.restoreReclones is not set while the restore Job runs")
	}
	if q.Phase != v1alpha1.RestoreReclonesArmed {
		return fmt.Errorf("status.restoreReclones.phase=%s, want %s while the restore runs", q.Phase, v1alpha1.RestoreReclonesArmed)
	}
	peers := s75Peers(mfg, active)
	got := slices.Clone(q.Pending)
	want := slices.Clone(peers)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		return fmt.Errorf("status.restoreReclones.pending=%v, want every peer %v", q.Pending, peers)
	}
	reader, err := findReadOnlyReaderSite(mfg)
	if err != nil {
		return err
	}
	if q.Pending[len(q.Pending)-1] != reader {
		return fmt.Errorf("status.restoreReclones.pending=%v, want the ReadOnly site %s last", q.Pending, reader)
	}
	for _, peer := range peers {
		c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, peer, env.Creds)
		if err != nil {
			return fmt.Errorf("open peer %s: %w", peer, err)
		}
		svc, err := c.ScalarString(ctx, "SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1")
		_ = c.Close()
		if err != nil {
			return fmt.Errorf("peer %s receiver state: %w", peer, err)
		}
		if svc != "OFF" {
			return fmt.Errorf("peer %s replication receiver is %s while the restore runs, want OFF", peer, svc)
		}
	}
	return nil
}

func s75RemoveSpecMidRestoring() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "remove spec.restoreInPlace while the Job is held; every guard stays up",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeAtRestore")
			jobName := ctxFetch(env, "restoreJobName")
			runID := ctxFetch(env, "s75RunID")
			removed := time.Now()
			if err := removeRestoreInPlaceSpec(ctx, env); err != nil {
				return fmt.Errorf("remove spec.restoreInPlace: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			ev, err := waitForMFGEvent(waitCtx, env, removed, v1alpha1.EventRestoreInPlaceSpecRemoved, jobName)
			if err != nil {
				return err
			}
			env.Capture.Note("spec removal event: " + ev.Message)
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			deadline := time.Now().Add(s75GuardWindow)
			samples := 0
			for {
				if err := s75CheckGuardsHeld(ctx, env, primary, active, jobName, runID, removed); err != nil {
					return fmt.Errorf("after spec removal (sample %d): %w", samples+1, err)
				}
				samples++
				if time.Now().After(deadline) {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(4 * time.Second):
				}
			}
			if err := s75RequireProbesRefused(ctx, env, removed, 4); err != nil {
				return err
			}
			for _, reason := range []string{v1alpha1.EventRestoreInPlaceCanceled, v1alpha1.EventRestoreInPlaceJobLost, v1alpha1.EventRestoreOfflineFenceReleased} {
				if ev, ok, err := findMFGEvent(ctx, env, removed, reason, ""); err != nil {
					return err
				} else if ok {
					return fmt.Errorf("unexpected %s event after the spec was removed mid-Job: %s", reason, ev.Message)
				}
			}
			env.Capture.Note(fmt.Sprintf("guards held across %d samples over %s after the spec was removed", samples, s75GuardWindow))
			return ctxStash(ctx, env, "s75SpecRemovedAt", removed.Format(time.RFC3339Nano))
		},
	}
}

// s75CheckGuardsHeld is one sample of the guards a running Job keeps
// after its spec is removed.
func s75CheckGuardsHeld(ctx context.Context, env *runner.Env, primary *pgmysql.SiteClient, active, jobName, runID string, removed time.Time) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	if mfg.Spec.RestoreInPlace != nil {
		return fmt.Errorf("spec.restoreInPlace is set again")
	}
	rip := mfg.Status.RestoreInPlace
	switch {
	case rip == nil:
		return fmt.Errorf("status.restoreInPlace was cleared")
	case rip.Phase != v1alpha1.RestoreInPlaceRestoring:
		return fmt.Errorf("status.restoreInPlace.phase=%s message=%q, want Restoring while the Job runs", rip.Phase, rip.Message)
	case rip.JobName != jobName || rip.RunID != runID:
		return fmt.Errorf("status.restoreInPlace job=%q runID=%q, want %q %q", rip.JobName, rip.RunID, jobName, runID)
	case !strings.Contains(rip.Message, "spec.restoreInPlace was removed"):
		return fmt.Errorf("status.restoreInPlace.message=%q does not say the spec was removed", rip.Message)
	}
	if mfg.Status.ActiveSite != active {
		return fmt.Errorf("status.activeSite moved %q -> %q", active, mfg.Status.ActiveSite)
	}
	if f := mfg.Status.RestoreOfflineFence; f == nil || f.RunID != runID {
		return fmt.Errorf("status.restoreOfflineFence=%+v, want the run's fence held", f)
	}
	if q := mfg.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
		return fmt.Errorf("status.restoreReclones=%+v, want Armed", q)
	}
	if err := s75RequireOfflineMode(ctx, primary, true); err != nil {
		return err
	}
	if err := s75RequirePrimaryServiceEmpty(ctx, env); err != nil {
		return err
	}
	if err := s75RequirePeersHeld(ctx, env, mfg, active); err != nil {
		return err
	}
	job, err := env.Kube.Kubernetes.BatchV1().Jobs(env.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get restore Job %s: %w", jobName, err)
	}
	if string(job.UID) != ctxFetch(env, "s75JobUID") {
		return fmt.Errorf("restore Job %s was replaced (uid %s, want %s)", jobName, job.UID, ctxFetch(env, "s75JobUID"))
	}
	if job.Status.Succeeded > 0 || job.Status.Failed > 0 {
		return fmt.Errorf("restore Job %s finished while the lock was held (succeeded=%d failed=%d)", jobName, job.Status.Succeeded, job.Status.Failed)
	}
	if peer := ctxFetch(env, "s75CandidatePeer"); peer != "" {
		before, berr := fetchStashedFloat(env, "s75FailoversToPeerBefore")
		cur, cerr := metricCounter(ctx, env, "bloodraven_failovers_total", map[string]string{"target_site": peer})
		if berr == nil && cerr == nil && cur > before {
			return fmt.Errorf("a failover to %s fired during the restore: failovers_total %g -> %g", peer, before, cur)
		}
	}
	return nil
}

func s75ObserveSucceededAndRelease() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "release the lock: the run ends Succeeded, the fence is released and the primary holds row 1 only",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeAtRestore")
			confirm := ctxFetch(env, "s75Confirm")
			if open, err := s75ReleaseHolder(ctx, env); err != nil {
				return fmt.Errorf("release the lock holder: %w", err)
			} else if !open {
				return fmt.Errorf("the lock holder was not open")
			}
			released := time.Now()
			waitCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
			defer cancel()
			rip, err := waitRestoreInPlacePhase(waitCtx, env, v1alpha1.RestoreInPlaceSucceeded)
			captureJobPodLogs(ctx, env, ctxFetch(env, "restoreJobName"), "s75-restore-job")
			if err != nil {
				return fmt.Errorf("restore did not end Succeeded: %w", err)
			}
			// runID is cleared once the run is terminal; the run is
			// identified by its startTime and Job.
			if rip.ConfirmTokenUsed != confirm || rip.RunID != "" || !s75SameRunStart(env, rip) || rip.JobName != ctxFetch(env, "restoreJobName") {
				return fmt.Errorf("terminal status confirmTokenUsed=%q runID=%q startTime=%v job=%q, want %q, no runID, startTime %s, job %q",
					rip.ConfirmTokenUsed, rip.RunID, rip.StartTime, rip.JobName, confirm, ctxFetch(env, "s75RunStart"), ctxFetch(env, "restoreJobName"))
			}
			job, err := env.Kube.Kubernetes.BatchV1().Jobs(env.Namespace).Get(ctx, rip.JobName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get restore Job %s: %w", rip.JobName, err)
			}
			if job.Status.Succeeded == 0 || string(job.UID) != ctxFetch(env, "s75JobUID") {
				return fmt.Errorf("restore Job %s uid=%s succeeded=%d, want the held Job %s succeeded", rip.JobName, job.UID, job.Status.Succeeded, ctxFetch(env, "s75JobUID"))
			}
			env.Capture.Note(fmt.Sprintf("restore Succeeded: confirmTokenUsed=%s message=%q", rip.ConfirmTokenUsed, rip.Message))

			// The fence is released once the run is terminal.
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace, "restoreOfflineFence released", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				f := mfg.Status.RestoreOfflineFence
				return f == nil, fmt.Sprintf("restoreOfflineFence=%+v", f), nil
			})
			if err != nil {
				return err
			}
			ev, err := waitForMFGEvent(waitCtx, env, released, v1alpha1.EventRestoreOfflineFenceReleased, active+" is OFF")
			if err != nil {
				return err
			}
			env.Capture.Note("fence release event: " + ev.Message)
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open active %s: %w", active, err)
			}
			defer primary.Close()
			if err := s75RequireOfflineMode(ctx, primary, false); err != nil {
				return err
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != active {
				return fmt.Errorf("status.activeSite moved %q -> %q during the restore", active, mfg.Status.ActiveSite)
			}
			if err := s75RequireMarkerRows(ctx, env, active, []int{1}); err != nil {
				return fmt.Errorf("restored primary: %w", err)
			}
			if err := s75RequireProbesAccepted(ctx, env, released); err != nil {
				return err
			}
			return nil
		},
	}
}

// s75RequireProbesAccepted waits for a new application connection to
// succeed through both Services after since.
func s75RequireProbesAccepted(ctx context.Context, env *runner.Env, since time.Time) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		records, _, err := s75ClientLog(ctx, env)
		if err != nil {
			return err
		}
		ok := map[string]bool{}
		for _, via := range []string{"primary", "site"} {
			for _, p := range s75ProbesSince(records, via, since) {
				if p.RC == 0 && strings.Contains(p.Out, "probe-ok") {
					ok[via] = true
				}
			}
		}
		if ok["primary"] && ok["site"] {
			env.Capture.Note("new application connections succeed again through -primary and the per-site Service")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("application connections not accepted again within 2m of the release: %v", ok)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func s75ReapplySameConfirm() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "re-applying the same confirm starts nothing",
		Do: func(ctx context.Context, env *runner.Env) error {
			confirm := ctxFetch(env, "s75Confirm")
			jobName := ctxFetch(env, "restoreJobName")
			before, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if before.Status.RestoreInPlace == nil || before.Status.RestoreInPlace.CompletionTime == nil {
				return fmt.Errorf("terminal status has no completionTime: %+v", before.Status.RestoreInPlace)
			}
			completion := before.Status.RestoreInPlace.CompletionTime.Time
			reapplied := time.Now()
			if _, err := patchRestoreInPlace(ctx, env, s75RestoreSpec(env, confirm)); err != nil {
				return fmt.Errorf("re-apply spec.restoreInPlace: %w", err)
			}
			deadline := time.Now().Add(45 * time.Second)
			for {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				rip := mfg.Status.RestoreInPlace
				switch {
				case rip == nil:
					return fmt.Errorf("status.restoreInPlace was cleared after the re-apply")
				case rip.Phase != v1alpha1.RestoreInPlaceSucceeded || rip.RunID != "" || rip.RunConfirm != "" || !s75SameRunStart(env, rip) || rip.ConfirmTokenUsed != confirm:
					return fmt.Errorf("re-applying confirm=%s started a run: phase=%s runID=%q runConfirm=%q startTime=%v confirmTokenUsed=%s message=%q",
						confirm, rip.Phase, rip.RunID, rip.RunConfirm, rip.StartTime, rip.ConfirmTokenUsed, rip.Message)
				case rip.CompletionTime == nil || !rip.CompletionTime.Time.Equal(completion):
					return fmt.Errorf("terminal status rewritten after the re-apply: completionTime=%v, was %s", rip.CompletionTime, completion)
				}
				if mfg.Status.RestoreOfflineFence != nil {
					return fmt.Errorf("the re-apply recorded an offline_mode fence: %+v", mfg.Status.RestoreOfflineFence)
				}
				if q := mfg.Status.RestoreReclones; q != nil && q.ConfirmToken != confirm {
					return fmt.Errorf("the re-apply armed a new reclone queue: %+v", q)
				}
				job, err := env.Kube.Kubernetes.BatchV1().Jobs(env.Namespace).Get(ctx, jobName, metav1.GetOptions{})
				if err == nil && string(job.UID) != ctxFetch(env, "s75JobUID") {
					return fmt.Errorf("a new restore Job %s (uid %s) was created after the re-apply", jobName, job.UID)
				} else if err != nil && !apierrors.IsNotFound(err) {
					return err
				}
				for _, reason := range []string{v1alpha1.EventRestoreInPlaceStarted, v1alpha1.EventRestoreOfflineFenceApplied, v1alpha1.EventRestoreInPlaceJobCreated} {
					if ev, ok, err := findMFGEvent(ctx, env, reapplied, reason, ""); err != nil {
						return err
					} else if ok {
						return fmt.Errorf("re-applying the used confirm emitted %s: %s", reason, ev.Message)
					}
				}
				if time.Now().After(deadline) {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(3 * time.Second):
				}
			}
			env.Capture.Note("re-applied confirm=" + confirm + "; the run stayed Succeeded and nothing started for 45s")
			return nil
		},
	}
}

// s75AllMFGEvents scans every event of the namespace (not only the most
// recent 200) for one matching matchMFGEvent.
func s75AllMFGEvents(ctx context.Context, env *runner.Env, notBefore time.Time, reason, snippet string) ([]corev1.Event, error) {
	events, err := env.Kube.RecentEvents(ctx, env.Namespace, 0)
	if err != nil {
		return nil, err
	}
	var out []corev1.Event
	for _, ev := range events {
		if matchMFGEvent(ev, env.FG, notBefore, reason, snippet) {
			out = append(out, ev)
		}
	}
	return out, nil
}

func s75WaitPeersRecloned() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "the reclone queue drains every peer",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeAtRestore")
			confirm := ctxFetch(env, "s75Confirm")
			requested := s75StashedTime(env, "s75RequestedAt")
			waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "status.restoreReclones drained", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				q := mfg.Status.RestoreReclones
				if q == nil {
					return true, "drained", nil
				}
				if q.Phase == v1alpha1.RestoreReclonesHeld {
					return false, "", fmt.Errorf("the reclone queue of a succeeded restore is Held: %+v", q)
				}
				if q.ConfirmToken != confirm {
					return false, "", fmt.Errorf("status.restoreReclones belongs to confirm %s, want %s", q.ConfirmToken, confirm)
				}
				return false, fmt.Sprintf("phase=%s pending=%v requested=%q failures=%d", q.Phase, q.Pending, q.Requested, len(q.Failures)), nil
			})
			if err != nil {
				return err
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			for _, peer := range s75Peers(mfg, active) {
				evs, err := s75AllMFGEvents(ctx, env, requested, v1alpha1.EventRestoreRecloneSucceeded, "recloned "+peer+" ")
				if err != nil {
					return err
				}
				if len(evs) == 0 {
					return fmt.Errorf("no %s event for %s", v1alpha1.EventRestoreRecloneSucceeded, peer)
				}
				env.Capture.Note("reclone event: " + evs[0].Message)
			}
			evs, err := s75AllMFGEvents(ctx, env, requested, v1alpha1.EventRestoreReclonesDrained, "confirm="+confirm)
			if err != nil {
				return err
			}
			if len(evs) == 0 {
				return fmt.Errorf("no %s event for confirm=%s", v1alpha1.EventRestoreReclonesDrained, confirm)
			}
			if failed, err := s75AllMFGEvents(ctx, env, requested, v1alpha1.EventRestoreRecloneFailed, ""); err == nil && len(failed) > 0 {
				env.Capture.Note(fmt.Sprintf("%d %s event(s) before the queue drained (retried): %s", len(failed), v1alpha1.EventRestoreRecloneFailed, failed[0].Message))
			}
			return nil
		},
	}
}

func s75VerifyGroupAndPeers() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the group is Ready with one writable site, and every peer replicates and holds row 1 only",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "activeAtRestore")
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "Ready with one writable site and every peer replicating", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
				ready := conditionTrueForGeneration(mfg.Status.Conditions, v1alpha1.ConditionReady, mfg.Generation)
				var writable []string
				var notReplicating []string
				for _, s := range mfg.Status.Sites {
					if s.State == v1alpha1.SiteStateWritable {
						writable = append(writable, s.Name)
						continue
					}
					if !s.Replication.Running() {
						notReplicating = append(notReplicating, fmt.Sprintf("%s(%s)", s.Name, s.Replication))
					}
				}
				msg := fmt.Sprintf("ready=%v writable=%v notReplicating=%v", ready, writable, notReplicating)
				return ready && len(writable) == 1 && writable[0] == active && len(notReplicating) == 0 && len(mfg.Status.Sites) == len(mfg.Spec.Sites), msg, nil
			})
			if err != nil {
				return err
			}
			for _, peer := range s75Peers(mfg, active) {
				if err := waitForReplicaGTID(ctx, env, active, peer); err != nil {
					return err
				}
				c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, peer, env.Creds)
				if err != nil {
					return fmt.Errorf("open peer %s: %w", peer, err)
				}
				rs, err := c.ShowReplicaStatus(ctx)
				_ = c.Close()
				if err != nil {
					return fmt.Errorf("peer %s replica status: %w", peer, err)
				}
				if !rs.IORunning || !rs.SQLRunning {
					return fmt.Errorf("peer %s replication io=%v sql=%v (io error %q, sql error %q)", peer, rs.IORunning, rs.SQLRunning, rs.LastIOError, rs.LastSQLError)
				}
				if err := s75RequireMarkerRows(ctx, env, peer, []int{1}); err != nil {
					return fmt.Errorf("recloned peer: %w", err)
				}
			}
			if peer := ctxFetch(env, "s75CandidatePeer"); peer != "" {
				before, berr := fetchStashedFloat(env, "s75FailoversToPeerBefore")
				cur, cerr := metricCounter(ctx, env, "bloodraven_failovers_total", map[string]string{"target_site": peer})
				if berr == nil && cerr == nil && cur > before {
					return fmt.Errorf("a failover to %s fired during the scenario: failovers_total %g -> %g", peer, before, cur)
				}
			}
			env.Capture.Note(fmt.Sprintf("group Ready on %s; peers %v replicate and hold row 1 only", active, s75Peers(mfg, active)))
			return nil
		},
	}
}

func s75Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	if _, err := s75ReleaseHolder(ctx, env); err != nil {
		env.Capture.Note("cleanup: release lock holder: " + err.Error())
	}
	for _, pod := range []string{s75ClientPod, s75BinderPod} {
		if err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).Delete(ctx, pod, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete %s: %w", pod, err))
		}
	}
	// Restore state is the scenario's to undo only once it requested its
	// restore (the confirm is stashed before the spec is patched). Cleanup
	// also runs after a failed precheck, which refuses a group that already
	// has a restore or a reclone queue: those are not the scenario's.
	confirm := ctxFetch(env, "s75Confirm")
	if confirm == "" {
		return s75CleanupBackupArtifacts(ctx, env, errs)
	}
	if err := removeRestoreInPlaceSpec(ctx, env); err != nil {
		errs = append(errs, fmt.Errorf("remove restoreInPlace spec: %w", err))
	}
	// A run interrupted by a failure finishes on its own once the lock is
	// released (a running Job is never interrupted); wait for it before
	// touching the Job or the queue.
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	_, werr := env.Wait.UntilCR(waitCtx, env.Namespace, "restoreInPlace terminal and fence released", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		rip := mfg.Status.RestoreInPlace
		terminal := rip == nil || restoreInPlaceTerminal(rip.Phase)
		phase := ""
		if rip != nil {
			phase = string(rip.Phase)
		}
		return terminal && mfg.Status.RestoreOfflineFence == nil, fmt.Sprintf("phase=%q fence=%v", phase, mfg.Status.RestoreOfflineFence != nil), nil
	})
	cancel()
	if werr != nil {
		errs = append(errs, werr)
	}
	waitCtx, cancel = context.WithTimeout(ctx, 12*time.Minute)
	_, qerr := env.Wait.UntilCR(waitCtx, env.Namespace, "reclone queue drained or held", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		q := mfg.Status.RestoreReclones
		if q == nil {
			return true, "none", nil
		}
		return q.Phase == v1alpha1.RestoreReclonesHeld, fmt.Sprintf("phase=%s pending=%v", q.Phase, q.Pending), nil
	})
	cancel()
	if qerr != nil {
		errs = append(errs, qerr)
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		errs = append(errs, err)
	} else {
		if q := mfg.Status.RestoreReclones; q != nil && q.ConfirmToken == confirm && (q.Phase == v1alpha1.RestoreReclonesHeld || werr != nil) {
			// A failed run held the peers. run-all resets before the next
			// destructive scenario; release the hold so a single run
			// leaves a replicating group behind.
			if perr := env.Kube.PatchMFGStatusNamed(ctx, env.Namespace, env.FG,
				[]pgkube.JSONPatchOp{{Op: "remove", Path: "/status/restoreReclones"}}); perr != nil {
				errs = append(errs, fmt.Errorf("remove status.restoreReclones: %w", perr))
			}
		}
		if werr == nil {
			var sites []string
			for _, s := range mfg.Spec.Sites {
				sites = append(sites, s.Name)
			}
			if serr := clearRetainedInPlaceRestoreJobs(ctx, env, "", sites...); serr != nil {
				errs = append(errs, serr)
			}
		}
		if active := mfg.Status.ActiveSite; active != "" {
			if primary, oerr := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds); oerr == nil {
				for _, q := range []string{
					"DROP DATABASE IF EXISTS " + s75MarkerDB,
					fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", s75AppUser),
				} {
					if _, derr := primary.Exec(ctx, q); derr != nil {
						env.Capture.Note(fmt.Sprintf("cleanup: %s: %v", q, derr))
					}
				}
				_ = primary.Close()
			} else {
				env.Capture.Note("cleanup: open primary: " + oerr.Error())
			}
		}
	}
	return s75CleanupBackupArtifacts(ctx, env, errs)
}

// s75CleanupBackupArtifacts deletes what the scenario created for its
// backup (the backup CRs, its passphrase Secret and claim) and restores the
// original spec.backup, then reports every error collected.
func s75CleanupBackupArtifacts(ctx context.Context, env *runner.Env, errs []error) error {
	if err := deleteBackupCRs(ctx, env, ctxFetch(env, "backupName"), ""); err != nil {
		errs = append(errs, err)
	}
	if err := restoreOriginalBackupSpec(ctx, env); err != nil {
		errs = append(errs, err)
	}
	if err := deleteBackupPassphraseSecret(ctx, env); err != nil {
		errs = append(errs, err)
	}
	if ctxFetch(env, "s75PVCExisted") == "false" {
		if err := env.Kube.DeletePVC(ctx, env.Namespace, s75PVCName(env)); err != nil {
			errs = append(errs, fmt.Errorf("delete backup claim %s: %w", s75PVCName(env), err))
		} else {
			env.Capture.Note("deleted backup claim " + s75PVCName(env))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("s75 cleanup: %w", errors.Join(errs...))
	}
	return nil
}
