package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

const (
	backupE2EBucket       = "bloodraven-backup-e2e"
	backupE2EProfile      = "rustfs-e2e"
	backupE2EEndpoint     = "http://rustfs.bloodraven-playground.svc.cluster.local:9000"
	backupE2ECredsSecret  = "dragonfly-s3-credentials"
	backupOriginalSpecKey = "backupOriginalSpecJSON"
	backupOriginalHadKey  = "backupOriginalHadSpec"
)

func backupRunStamp(env *runner.Env) string {
	start := env.StartTime.UTC()
	if start.IsZero() {
		start = time.Now().UTC()
	}
	return strings.ToLower(start.Format("20060102T150405Z"))
}

func backupProfileSpec(prefix string, pitr bool) v1alpha1.BackupSpec {
	consistent := true
	spec := v1alpha1.BackupSpec{
		Image:                  v1alpha1.DefaultBackupImage,
		MaxLagSecondsForSource: 60,
		ActiveDeadlineSeconds:  1800,
		BackoffLimit:           0,
		Profiles: []v1alpha1.BackupProfile{{
			Name: backupE2EProfile,
			RetentionPolicy: &v1alpha1.RetentionPolicy{
				Count:         2,
				MaxAgeDays:    1,
				MinKeep:       1,
				MaxFailedKeep: 2,
			},
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3,
				S3: &v1alpha1.S3Storage{
					Bucket:            backupE2EBucket,
					Prefix:            prefix,
					EndpointURL:       backupE2EEndpoint,
					CredentialsSecret: backupE2ECredsSecret,
					Region:            "us-east-1",
				},
			},
			Dump: &v1alpha1.DumpOptions{
				Threads:       2,
				BytesPerChunk: "64M",
				Compression:   "zstd",
				Consistent:    &consistent,
			},
		}},
	}
	if pitr {
		spec.PITR = &v1alpha1.PITRSpec{
			Enabled:             true,
			ProfileName:         backupE2EProfile,
			MaxBinlogSize:       "1M",
			ArchivePollInterval: &metav1.Duration{Duration: 2 * time.Second},
		}
	}
	return spec
}

func patchBackupSpec(ctx context.Context, env *runner.Env, spec v1alpha1.BackupSpec) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("read MFG before backup spec patch: %w", err)
	}
	hadOriginal := mfg.Spec.Backup != nil
	if err := ctxStash(ctx, env, backupOriginalHadKey, strconv.FormatBool(hadOriginal)); err != nil {
		return err
	}
	if hadOriginal {
		body, err := json.Marshal(mfg.Spec.Backup.DeepCopy())
		if err != nil {
			return fmt.Errorf("marshal original backup spec: %w", err)
		}
		if err := ctxStash(ctx, env, backupOriginalSpecKey, string(body)); err != nil {
			return err
		}
	}
	op := "add"
	if hadOriginal {
		op = "replace"
	}
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{
		Op:    op,
		Path:  "/spec/backup",
		Value: spec,
	}}); err != nil {
		return fmt.Errorf("patch backup spec: %w", err)
	}
	env.Capture.Note(fmt.Sprintf("patched spec.backup: hadOriginal=%v profile=%s bucket=%s", hadOriginal, backupE2EProfile, backupE2EBucket))
	return nil
}

func restoreOriginalBackupSpec(ctx context.Context, env *runner.Env) error {
	rawHad := ctxFetch(env, backupOriginalHadKey)
	if rawHad == "" {
		return nil
	}
	hadOriginal, err := strconv.ParseBool(rawHad)
	if err != nil {
		return fmt.Errorf("parse original backup spec marker %q: %w", rawHad, err)
	}
	if !hadOriginal {
		if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{Op: "remove", Path: "/spec/backup"}}); err != nil {
			return fmt.Errorf("remove scenario backup spec: %w", err)
		}
		env.Capture.Note("restored spec.backup by removing scenario-added config")
		return nil
	}
	var original v1alpha1.BackupSpec
	if err := json.Unmarshal([]byte(ctxFetch(env, backupOriginalSpecKey)), &original); err != nil {
		return fmt.Errorf("unmarshal original backup spec: %w", err)
	}
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{
		Op:    "replace",
		Path:  "/spec/backup",
		Value: original,
	}}); err != nil {
		return fmt.Errorf("restore original backup spec: %w", err)
	}
	env.Capture.Note("restored original spec.backup")
	return nil
}

func waitForBackupProfile(ctx context.Context, env *runner.Env, prefix string, pitr bool) error {
	_, err := env.Wait.UntilCR(ctx, env.Namespace, "backup profile observed", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		if mfg.Spec.Backup == nil {
			return false, "spec.backup is nil", nil
		}
		var found *v1alpha1.BackupProfile
		for i := range mfg.Spec.Backup.Profiles {
			if mfg.Spec.Backup.Profiles[i].Name == backupE2EProfile {
				found = &mfg.Spec.Backup.Profiles[i]
				break
			}
		}
		if found == nil || found.Storage.S3 == nil {
			return false, "rustfs-e2e profile not observed", nil
		}
		s3 := found.Storage.S3
		matches := found.Storage.Type == v1alpha1.BackupStorageS3 && s3.Bucket == backupE2EBucket && s3.Prefix == prefix && s3.EndpointURL == backupE2EEndpoint
		pitrOK := !pitr || (mfg.Spec.Backup.PITR != nil && mfg.Spec.Backup.PITR.Enabled && mfg.Spec.Backup.PITR.ProfileName == backupE2EProfile)
		ready := conditionTrue(mfg.Status.Conditions, "Ready")
		activeOK := mfg.Status.ActiveSite != "" && mfg.Status.UpdatePhase == ""
		return matches && pitrOK && ready && activeOK, fmt.Sprintf("profile storage=%s bucket=%q prefix=%q endpoint=%q pitrOK=%v ready=%v active=%q updatePhase=%q", found.Storage.Type, s3.Bucket, s3.Prefix, s3.EndpointURL, pitrOK, ready, mfg.Status.ActiveSite, mfg.Status.UpdatePhase), nil
	})
	return err
}

func createMysqlBackup(ctx context.Context, env *runner.Env, name, scenarioID string) error {
	b := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: env.Namespace,
			Labels: map[string]string{
				"shipstream.io/failover-group":                  env.FG,
				"shipstream.io/backup-profile":                  backupE2EProfile,
				"chaos.playground.bloodraven.io/scenario":       scenarioID,
				"chaos.playground.bloodraven.io/created-by-e2e": "true",
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: env.FG},
			ProfileName:      backupE2EProfile,
			TriggeredBy:      "chaos/" + scenarioID,
		},
	}
	if err := env.Kube.Controller.Create(ctx, b); err != nil {
		return fmt.Errorf("create MysqlBackup %s: %w", name, err)
	}
	env.Capture.Note(fmt.Sprintf("created MysqlBackup %s", name))
	return nil
}

func createMysqlBackupVerification(ctx context.Context, env *runner.Env, name, backupName, scenarioID string, pit *v1alpha1.PointInTimeVerificationSpec, query string, minRows int64) error {
	keep := true
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: env.Namespace,
			Labels: map[string]string{
				"shipstream.io/failover-group":                  env.FG,
				"shipstream.io/backup-profile":                  backupE2EProfile,
				"chaos.playground.bloodraven.io/scenario":       scenarioID,
				"chaos.playground.bloodraven.io/created-by-e2e": "true",
			},
		},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef:        v1alpha1.LocalGroupRef{Name: env.FG},
			ProfileName:             backupE2EProfile,
			BackupRef:               &corev1.LocalObjectReference{Name: backupName},
			KeepOnFailure:           &keep,
			TTLSecondsAfterFinished: 0,
			TriggeredBy:             "chaos/" + scenarioID,
			PointInTime:             pit,
			SanityCheck: &v1alpha1.SanityCheckSpec{
				Query: query,
				Expect: &v1alpha1.SanityCheckExpectation{
					MinRows:            minRows,
					MaxDurationSeconds: 30,
				},
			},
		},
	}
	if err := env.Kube.Controller.Create(ctx, v); err != nil {
		return fmt.Errorf("create MysqlBackupVerification %s: %w", name, err)
	}
	env.Capture.Note(fmt.Sprintf("created MysqlBackupVerification %s pinnedTo=%s", name, backupName))
	return nil
}

func getMysqlBackup(ctx context.Context, env *runner.Env, name string) (*v1alpha1.MysqlBackup, error) {
	b := &v1alpha1.MysqlBackup{}
	if err := env.Kube.Controller.Get(ctx, ctrlclient.ObjectKey{Namespace: env.Namespace, Name: name}, b); err != nil {
		return nil, err
	}
	return b, nil
}

func getMysqlBackupVerification(ctx context.Context, env *runner.Env, name string) (*v1alpha1.MysqlBackupVerification, error) {
	v := &v1alpha1.MysqlBackupVerification{}
	if err := env.Kube.Controller.Get(ctx, ctrlclient.ObjectKey{Namespace: env.Namespace, Name: name}, v); err != nil {
		return nil, err
	}
	return v, nil
}

func waitForBackupPhase(ctx context.Context, env *runner.Env, name string, want v1alpha1.BackupPhase) (*v1alpha1.MysqlBackup, error) {
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	progress := time.NewTicker(10 * time.Second)
	defer progress.Stop()
	var last string
	for {
		b, err := getMysqlBackup(ctx, env, name)
		if err != nil {
			last = err.Error()
		} else {
			last = backupStatusSummary(b)
			if b.Status.Phase == want {
				return b, nil
			}
			if b.Status.Phase == v1alpha1.BackupPhaseFailed {
				return b, fmt.Errorf("MysqlBackup %s failed: %s", name, last)
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for MysqlBackup %s phase %s timed out after %s: %s", name, want, time.Since(start).Round(time.Second), last)
		case <-progress.C:
			env.Logger.Info("wait", "what", "MysqlBackup "+name, "last", last, "elapsed", time.Since(start).Round(time.Second))
		case <-tick.C:
		}
	}
}

func waitForVerificationPhase(ctx context.Context, env *runner.Env, name string, want v1alpha1.VerificationPhase) (*v1alpha1.MysqlBackupVerification, error) {
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	progress := time.NewTicker(10 * time.Second)
	defer progress.Stop()
	var last string
	for {
		v, err := getMysqlBackupVerification(ctx, env, name)
		if err != nil {
			last = err.Error()
		} else {
			last = verificationStatusSummary(v)
			if v.Status.Phase == want {
				return v, nil
			}
			if v.Status.Phase == v1alpha1.VerificationPhaseFailed {
				captureVerificationJobLogs(ctx, env, v)
				return v, fmt.Errorf("MysqlBackupVerification %s failed: %s", name, last)
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for MysqlBackupVerification %s phase %s timed out after %s: %s", name, want, time.Since(start).Round(time.Second), last)
		case <-progress.C:
			env.Logger.Info("wait", "what", "MysqlBackupVerification "+name, "last", last, "elapsed", time.Since(start).Round(time.Second))
		case <-tick.C:
		}
	}
}

func captureVerificationJobLogs(ctx context.Context, env *runner.Env, v *v1alpha1.MysqlBackupVerification) {
	if v.Status.JobName == "" {
		env.Capture.Note("verification failed before status.jobName was set")
		return
	}
	pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + v.Status.JobName})
	if err != nil {
		env.Capture.Note("list verification pods failed: " + err.Error())
		return
	}
	if len(pods.Items) == 0 {
		env.Capture.Note("no verification pods found for job " + v.Status.JobName)
		return
	}
	for _, pod := range pods.Items {
		containers := append([]corev1.Container{}, pod.Spec.InitContainers...)
		containers = append(containers, pod.Spec.Containers...)
		for _, c := range containers {
			req := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: c.Name, Previous: false})
			r, err := req.Stream(ctx)
			if err != nil {
				env.Capture.Note(fmt.Sprintf("read verification pod log %s/%s failed: %v", pod.Name, c.Name, err))
				continue
			}
			body, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				env.Capture.Note(fmt.Sprintf("read verification pod log stream %s/%s failed: %v", pod.Name, c.Name, err))
				continue
			}
			name := fmt.Sprintf("verification-%s-%s.log", pod.Name, c.Name)
			if err := env.Capture.WriteFile(name, body); err != nil {
				env.Capture.Note(fmt.Sprintf("write verification pod log %s failed: %v", name, err))
			}
		}
	}
}

func deleteBackupCRs(ctx context.Context, env *runner.Env, backupName, verificationName string) error {
	var errs []error
	if verificationName != "" {
		if err := deleteAndWait(ctx, env, &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: verificationName, Namespace: env.Namespace}}, "MysqlBackupVerification", verificationName); err != nil {
			errs = append(errs, err)
		}
	}
	if backupName != "" {
		if err := deleteAndWait(ctx, env, &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: env.Namespace}}, "MysqlBackup", backupName); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("delete backup CRs: %v", errs)
	}
	return nil
}

func deleteAndWait(ctx context.Context, env *runner.Env, obj ctrlclient.Object, kind, name string) error {
	if err := env.Kube.Controller.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete %s %s: %w", kind, name, err)
	}
	if kind == "MysqlBackup" {
		// See comment below: the playground mysqlsh image may not support
		// util.rmdump, so the finalizer cleanup job can never complete. Remove
		// the test CR finalizer immediately after deletion is requested rather
		// than consuming the cleanup budget and failing an otherwise-successful
		// backup/restore scenario.
		if err := forceRemoveFinalizers(ctx, env, obj, kind, name); err != nil {
			return fmt.Errorf("force finalizer removal for %s %s: %w", kind, name, err)
		}
		return waitForDeleted(ctx, env, obj, kind, name, 30*time.Second)
	}
	deleteTimeout := 90 * time.Second
	waitCtx, cancel := context.WithTimeout(ctx, deleteTimeout)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		fresh := obj.DeepCopyObject().(ctrlclient.Object)
		if err := env.Kube.Controller.Get(waitCtx, types.NamespacedName{Namespace: env.Namespace, Name: name}, fresh); apierrors.IsNotFound(err) {
			env.Capture.Note(fmt.Sprintf("deleted %s %s", kind, name))
			return nil
		} else if err != nil {
			return fmt.Errorf("get %s %s after delete: %w", kind, name, err)
		}
		select {
		case <-waitCtx.Done():
			if kind == "MysqlBackup" {
				if err := forceRemoveFinalizers(ctx, env, fresh, kind, name); err != nil {
					return fmt.Errorf("wait for %s %s deletion: %w; force finalizer removal failed: %v", kind, name, waitCtx.Err(), err)
				}
				return waitForDeleted(ctx, env, fresh, kind, name, 30*time.Second)
			}
			return fmt.Errorf("wait for %s %s deletion: %w", kind, name, waitCtx.Err())
		case <-tick.C:
		}
	}
}

func forceRemoveFinalizers(ctx context.Context, env *runner.Env, obj ctrlclient.Object, kind, name string) error {
	fresh := obj.DeepCopyObject().(ctrlclient.Object)
	if err := env.Kube.Controller.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: name}, fresh); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if len(fresh.GetFinalizers()) == 0 {
		return nil
	}
	fresh.SetFinalizers(nil)
	if err := env.Kube.Controller.Update(ctx, fresh); err != nil {
		return err
	}
	env.Capture.Note(fmt.Sprintf("force-removed finalizers from %s %s after cleanup timeout", kind, name))
	return nil
}

func waitForDeleted(ctx context.Context, env *runner.Env, obj ctrlclient.Object, kind, name string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		fresh := obj.DeepCopyObject().(ctrlclient.Object)
		if err := env.Kube.Controller.Get(waitCtx, types.NamespacedName{Namespace: env.Namespace, Name: name}, fresh); apierrors.IsNotFound(err) {
			env.Capture.Note(fmt.Sprintf("deleted %s %s after force finalizer removal", kind, name))
			return nil
		} else if err != nil {
			return fmt.Errorf("get %s %s after force finalizer removal: %w", kind, name, err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for %s %s deletion after force finalizer removal: %w", kind, name, waitCtx.Err())
		case <-tick.C:
		}
	}
}

func dropMarkerSchemaAndReplicate(ctx context.Context, env *runner.Env, dbName string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	mfg, err := waitForS04CleanupBaseline(waitCtx, env)
	if err != nil {
		return err
	}
	writable := mfg.Status.ActiveSite
	replica, err := PeerOf(mfg, writable)
	if err != nil {
		return fmt.Errorf("cleanup: resolve replica for %s: %w", writable, err)
	}
	primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, writable, env.Creds)
	if err != nil {
		return fmt.Errorf("cleanup: open %s: %w", writable, err)
	}
	defer primary.Close()
	if _, err := primary.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)); err != nil {
		return fmt.Errorf("cleanup: drop database %s on %s: %w", dbName, writable, err)
	}
	gtid, err := primary.GtidExecuted(ctx)
	if err != nil {
		return fmt.Errorf("cleanup: read post-drop gtid on %s: %w", writable, err)
	}
	replicaClient, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, replica, env.Creds)
	if err != nil {
		return fmt.Errorf("cleanup: open replica %s: %w", replica, err)
	}
	defer replicaClient.Close()
	if rc, err := replicaClient.ScalarInt(waitCtx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", gtid); err != nil {
		return fmt.Errorf("cleanup: wait for schema drop on replica %s: %w", replica, err)
	} else if rc != 0 {
		return fmt.Errorf("cleanup: replica %s did not apply schema drop gtid within 30s (rc=%d gtid=%q)", replica, rc, gtid)
	}
	env.Capture.Note(fmt.Sprintf("cleanup: dropped %s on %s and replicated to %s", dbName, writable, replica))
	return nil
}

func activeAndReplica(ctx context.Context, env *runner.Env) (string, string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "active site available", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		active := mfg.Status.ActiveSite
		if active == "" {
			return false, "activeSite is empty", nil
		}
		if _, err := PeerOf(mfg, active); err != nil {
			return false, "activeSite peer not available: " + err.Error(), nil
		}
		return true, "activeSite=" + active, nil
	})
	if err != nil {
		return "", "", err
	}
	active := mfg.Status.ActiveSite
	if active == "" {
		return "", "", fmt.Errorf("no active site in MFG status")
	}
	replica, err := PeerOf(mfg, active)
	if err != nil {
		return "", "", err
	}
	return active, replica, nil
}

func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func waitForReplicaGTID(ctx context.Context, env *runner.Env, primarySite, replicaSite string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var last string
	for {
		primary, err := pgmysql.Open(waitCtx, env.Kube, env.Namespace, env.FG, primarySite, env.Creds)
		if err != nil {
			last = fmt.Sprintf("open primary mysql: %v", err)
		} else {
			gtid, err := primary.GtidExecuted(waitCtx)
			_ = primary.Close()
			if err != nil {
				last = fmt.Sprintf("read primary gtid_executed: %v", err)
			} else {
				replica, err := pgmysql.Open(waitCtx, env.Kube, env.Namespace, env.FG, replicaSite, env.Creds)
				if err != nil {
					last = fmt.Sprintf("open replica mysql: %v", err)
				} else {
					rc, err := replica.ScalarInt(waitCtx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", gtid)
					_ = replica.Close()
					if err != nil {
						last = fmt.Sprintf("WAIT_FOR_EXECUTED_GTID_SET on replica %s: %v", replicaSite, err)
					} else if rc != 0 {
						last = fmt.Sprintf("replica %s did not catch up within 30s (rc=%d gtid=%q)", replicaSite, rc, gtid)
					} else {
						env.Capture.Note(fmt.Sprintf("replica %s caught up to %s gtid=%q", replicaSite, primarySite, gtid))
						return nil
					}
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for replica %s to catch up to %s: %w (last: %s)", replicaSite, primarySite, waitCtx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

func originalPITRExpected(env *runner.Env) (enabled bool, profile string, err error) {
	rawHad := ctxFetch(env, backupOriginalHadKey)
	if rawHad == "" {
		return false, "", nil
	}
	hadOriginal, err := strconv.ParseBool(rawHad)
	if err != nil {
		return false, "", fmt.Errorf("parse original backup spec marker %q: %w", rawHad, err)
	}
	if !hadOriginal {
		return false, "", nil
	}
	var original v1alpha1.BackupSpec
	if err := json.Unmarshal([]byte(ctxFetch(env, backupOriginalSpecKey)), &original); err != nil {
		return false, "", fmt.Errorf("unmarshal original backup spec for PITR expectation: %w", err)
	}
	if original.PITR == nil || !original.PITR.Enabled {
		return false, "", nil
	}
	return true, original.PITR.ProfileName, nil
}

func backupStatusSummary(b *v1alpha1.MysqlBackup) string {
	return fmt.Sprintf("name=%s phase=%s message=%q job=%q source=%q location=%q storage=%s sizeBytes=%d gtid=%q binlog=%s:%d conditions=%s",
		b.Name, b.Status.Phase, b.Status.Message, b.Status.JobName, b.Status.SourceSite, b.Status.Location, b.Status.StorageType, b.Status.SizeBytes, b.Status.GtidExecuted, b.Status.BinlogFile, b.Status.BinlogPos, conditionsSummary(b.Status.Conditions))
}

func verificationStatusSummary(v *v1alpha1.MysqlBackupVerification) string {
	backupRef := "<nil>"
	if v.Status.BackupRef != nil {
		backupRef = fmt.Sprintf("%s/%s", v.Status.BackupRef.Name, v.Status.BackupRef.UID)
	}
	return fmt.Sprintf("name=%s phase=%s message=%q backupRef=%s job=%q pod=%q pvc=%q replay=%v sanity=%v conditions=%s",
		v.Name, v.Status.Phase, v.Status.Message, backupRef, v.Status.JobName, v.Status.PodName, v.Status.PVCName, v.Status.ReplayedThroughBinlog, v.Status.SanityCheck, conditionsSummary(v.Status.Conditions))
}

func conditionsSummary(conds []metav1.Condition) string {
	parts := make([]string, 0, len(conds))
	for _, c := range conds {
		parts = append(parts, fmt.Sprintf("%s=%s/%s", c.Type, c.Status, c.Reason))
	}
	return strings.Join(parts, ",")
}

func conditionTrue(conds []metav1.Condition, typ string) bool {
	for _, c := range conds {
		if c.Type == typ && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func archiverSummary(st *pgsidecar.ArchiverStatusResponse) string {
	if st == nil {
		return "<nil>"
	}
	return fmt.Sprintf("enabled=%v primary=%v site=%s storage=%s prefix=%s lastScan=%s files=%d manifestFiles=%d backlog=%d newest=%s lastUpload=%s failures=%d lastError=%q",
		st.Enabled, st.Primary, st.Site, st.StorageType, st.ManifestPrefix, st.LastScanAt.Format(time.RFC3339Nano), st.FilesArchived, st.ManifestFileCount, st.BacklogFiles, st.NewestArchivedTime.Format(time.RFC3339Nano), st.LastUploadAt.Format(time.RFC3339Nano), st.UploadFailures, st.LastError)
}
