package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// MysqlStandbyClusterReconciler reconciles MysqlStandbyCluster CRs.
//
// Phase 1 (this file): discovers source backup artifacts (dumps + PITR binlog
// manifests) by scanning the configured object-store prefix. Populates
// status.discovered and stamps BucketReadable / SourceConfigKnown conditions.
// No MySQL contact, no activation, no verification Job creation.
//
// Future phases: Phase 2 adds continuous restore verification (Restorable
// condition); Phase 3 adds the one-shot dr-activate promotion path.
type MysqlStandbyClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// newStoreFunc is the factory for ArchiveStore instances. Overridable
	// in tests to inject a fake store without network access.
	newStoreFunc func(ctx context.Context, cfg *sidecar.PITRConfig) (sidecar.ArchiveStore, error)
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlstandbyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlstandbyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlstandbyclusters/finalizers,verbs=update

// standbyDefaultDiscoveryInterval is used when spec.freshness is nil or
// spec.freshness.discoveryInterval is nil. Must match the CRD default.
const standbyDefaultDiscoveryInterval = 5 * time.Minute

// standbyAtJSONSuffix is the key suffix that mysqlsh writes for its dump
// metadata. Every dump directory contains exactly one "@.json" file.
const standbyAtJSONSuffix = "@.json"

// standbyDefaultScanTimeout bounds one object-store discovery scan. The timeout
// prevents a stalled S3-compatible endpoint from tying up a controller worker
// indefinitely while still leaving normal paginated scans room to complete.
const standbyDefaultScanTimeout = 30 * time.Second

// Reconcile is the main reconciliation loop.
//
// Phase 1 reconcile order:
//  1. Validate spec.
//  2. Resolve storage backend → ArchiveStore.
//  3. Scan the bucket prefix.
//  4. Stamp status.discovered.
//  5. Set BucketReadable condition.
//  6. Set SourceConfigKnown condition.
//  7. Requeue after discoveryInterval.
func (r *MysqlStandbyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("mysqlstandbycluster", req.NamespacedName)

	var sc v1alpha1.MysqlStandbyCluster
	if err := r.Get(ctx, req.NamespacedName, &sc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion: no finalizer logic needed in Phase 1.
	if !sc.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	interval := standbyDefaultDiscoveryInterval
	if sc.Spec.Freshness != nil && sc.Spec.Freshness.DiscoveryInterval != nil {
		interval = sc.Spec.Freshness.DiscoveryInterval.Duration
	}

	// ---------------------------------------------------------------
	// Step 1: Validate spec
	// ---------------------------------------------------------------
	if sc.Spec.Transport != v1alpha1.StandbyTransportObjectStore {
		// The CRD XValidation already enforces this at admission; defend
		// in-depth for objects that pre-date the webhook or were created
		// by older clients.
		msg := fmt.Sprintf("transport %q is not supported; only ObjectStore is implemented", sc.Spec.Transport)
		return r.stampConditionsAndRequeue(ctx, &sc, standbyConditionPair{
			bucketReadable: metav1.ConditionFalse,
			bucketReason:   "ConfigError",
			bucketMsg:      msg,
			sourceKnown:    metav1.ConditionFalse,
			sourceReason:   "ConfigError",
			sourceMsg:      "transport not ObjectStore",
		}, interval, nil)
	}

	// ---------------------------------------------------------------
	// Step 2: Resolve storage backend
	// ---------------------------------------------------------------
	storeCfg, storePrefix, credsDir, configErr := r.buildStoreCfg(ctx, &sc)
	if credsDir != "" {
		// Safety-net defer: keeps the tempdir clean even on unexpected
		// panics. The on-success path removes it immediately after newStore
		// returns (R-3).
		defer func() { _ = os.RemoveAll(credsDir) }()
	}
	if configErr != nil {
		logger.Error(configErr, "cannot resolve storage backend")
		r.Recorder.Eventf(&sc, corev1.EventTypeWarning, "ConfigError",
			"cannot resolve storage backend: %v", configErr)
		return r.stampConditionsAndRequeue(ctx, &sc, standbyConditionPair{
			bucketReadable: metav1.ConditionFalse,
			bucketReason:   "ConfigError",
			bucketMsg:      configErr.Error(),
			sourceKnown:    metav1.ConditionFalse,
			sourceReason:   "ConfigError",
			sourceMsg:      "storage backend unresolvable",
		}, interval, nil)
	}

	newStore := r.newStoreFunc
	if newStore == nil {
		newStore = sidecar.NewArchiveStore
	}

	store, err := newStore(ctx, storeCfg)
	// R-3: remove the credentials tempdir immediately after store construction
	// (success or failure) — the SDK reads the files at init and holds them in
	// memory. The defer above is the safety net; this is the primary cleanup.
	if credsDir != "" {
		_ = os.RemoveAll(credsDir)
		credsDir = "" // prevent double-removal by the defer
	}
	if err != nil {
		logger.Error(err, "failed to construct archive store")
		r.Recorder.Eventf(&sc, corev1.EventTypeWarning, "AuthFailed",
			"failed to construct archive store: %v", err)
		return r.stampConditionsAndRequeue(ctx, &sc, standbyConditionPair{
			bucketReadable: metav1.ConditionFalse,
			bucketReason:   "AuthFailed",
			bucketMsg:      fmt.Sprintf("failed to construct archive store: %v", err),
			sourceKnown:    metav1.ConditionFalse,
			sourceReason:   "AuthFailed",
			sourceMsg:      "store construction failed",
		}, interval, nil)
	}

	// ---------------------------------------------------------------
	// Step 3: Scan the bucket prefix
	// ---------------------------------------------------------------
	scanCtx, cancelScan := context.WithTimeout(ctx, standbyScanTimeout(interval))
	discovered, scanErr := r.scanBucket(scanCtx, logger, &sc, store, storePrefix)
	cancelScan()
	if scanErr != nil {
		// scanErr here means a fatal List failure (not a metadata parse error).
		logger.Error(scanErr, "bucket list failed", "prefix", storePrefix)
		r.Recorder.Eventf(&sc, corev1.EventTypeWarning, "ListFailed",
			"failed to list bucket at prefix %q: %v", storePrefix, scanErr)

		// R-2: surface staleness on the BucketReadable=False condition; preserve
		// status.discovered so kubectl get continues to show last-known-good values.
		bucketMsg := fmt.Sprintf("list prefix %q: %v", storePrefix, scanErr)
		if sc.Status.Discovered != nil && sc.Status.Discovered.LastScanAt != nil {
			bucketMsg = fmt.Sprintf("bucket unreadable since %s; last successful scan at %s; reason=ListFailed: %v",
				metav1.Now().UTC().Format(time.RFC3339),
				sc.Status.Discovered.LastScanAt.UTC().Format(time.RFC3339),
				scanErr)
		}
		return r.stampConditionsAndRequeue(ctx, &sc, standbyConditionPair{
			bucketReadable: metav1.ConditionFalse,
			bucketReason:   "ListFailed",
			bucketMsg:      bucketMsg,
			sourceKnown:    metav1.ConditionFalse,
			sourceReason:   "ListFailed",
			sourceMsg:      "bucket not readable",
		}, interval, nil)
	}

	// scanBucket may return a MetadataUnreadable error (R-6) embedded in
	// discovered.MetadataErr. Handle that here before stamping conditions.
	if discovered.MetadataErr != nil {
		logger.Error(discovered.MetadataErr, "dump @.json unreadable", "dump", discovered.DumpName)
		r.Recorder.Eventf(&sc, corev1.EventTypeWarning, "MetadataParseFailed",
			"failed to parse @.json for dump %q: %v", discovered.DumpName, discovered.MetadataErr)
		// Log a partial-scan line so operators can correlate.
		logger.Info("partial scan completed",
			"prefix", storePrefix,
			"dumpName", discovered.DumpName,
			"manifestCount", discovered.ManifestCount)
		return r.stampConditionsAndRequeue(ctx, &sc, standbyConditionPair{
			bucketReadable: metav1.ConditionTrue,
			bucketReason:   "ListSucceeded",
			bucketMsg:      "",
			sourceKnown:    metav1.ConditionFalse,
			sourceReason:   "MetadataUnreadable",
			sourceMsg: fmt.Sprintf("dump %q found but @.json is unreadable: %v",
				discovered.DumpName, discovered.MetadataErr),
		}, interval, nil)
	}

	// ---------------------------------------------------------------
	// Steps 4–7: Stamp status, set conditions, requeue
	// ---------------------------------------------------------------
	sourceReason := "DumpFound"
	sourceMsg := ""
	sourceKnown := metav1.ConditionTrue
	if discovered.DumpName == "" {
		sourceKnown = metav1.ConditionFalse
		sourceReason = "NoDumpFound"
		sourceMsg = fmt.Sprintf("no dump found under prefix %q (no @.json objects)", storePrefix)
	} else if discovered.ManifestCount == 0 {
		sourceKnown = metav1.ConditionFalse
		sourceReason = "NoBinlogManifests"
		sourceMsg = fmt.Sprintf("dump %q found but no binlog manifests under %q/%s/",
			discovered.DumpName, storePrefix, pitrBinlogSubprefix)
	}

	return r.stampConditionsAndRequeue(ctx, &sc, standbyConditionPair{
		bucketReadable: metav1.ConditionTrue,
		bucketReason:   "ListSucceeded",
		bucketMsg:      "",
		sourceKnown:    sourceKnown,
		sourceReason:   sourceReason,
		sourceMsg:      sourceMsg,
	}, interval, discovered)
}

// SetNewStoreFunc overrides the ArchiveStore factory. Used by tests to inject a
// fake store without network access; production code never calls this.
func (r *MysqlStandbyClusterReconciler) SetNewStoreFunc(
	fn func(ctx context.Context, cfg *sidecar.PITRConfig) (sidecar.ArchiveStore, error),
) {
	r.newStoreFunc = fn
}

// SetupWithManager registers the reconciler with the manager.
func (r *MysqlStandbyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlStandbyCluster{}).
		Complete(r)
}

// ----------------------------------------------------------------------
// Storage backend resolution
// ----------------------------------------------------------------------

// buildStoreCfg converts a MysqlStandbyCluster's source.storage into a
// PITRConfig (which NewArchiveStore consumes) and returns:
//   - PITRConfig for constructing the store
//   - the logical prefix to scan
//   - the path to a temporary credentials directory (empty when not needed)
//   - any configuration error
//
// For S3 it reads AWS credentials from the referenced Kubernetes Secret and
// writes them to a temporary directory that the sidecar SDK reads at store
// construction time. The caller must os.RemoveAll the returned dir after the
// store has been constructed.
//
// For PVC it returns a ConfigError: the operator pod does not mount the source
// cluster's backup PVC, which is a cross-cluster constraint. Use S3-compatible
// storage for production DR.
func (r *MysqlStandbyClusterReconciler) buildStoreCfg(ctx context.Context, sc *v1alpha1.MysqlStandbyCluster) (*sidecar.PITRConfig, string, string, error) {
	storage := sc.Spec.Source.Storage
	switch storage.Type {
	case v1alpha1.BackupStorageS3:
		if storage.S3 == nil {
			return nil, "", "", fmt.Errorf("source.storage.type=S3 but source.storage.s3 is not set")
		}
		s3Cfg, prefix, credsDir, err := r.buildS3PitrCfg(ctx, sc.Namespace, storage.S3)
		if err != nil {
			return nil, "", credsDir, err
		}
		cfg := &sidecar.PITRConfig{
			StorageType: "S3",
			S3:          s3Cfg,
		}
		if sc.Spec.Source.Decryption != nil && sc.Spec.Source.Decryption.PassphraseSecret.Name != "" {
			passphraseFile, dir, err := r.resolvePassphraseToFile(ctx, sc.Namespace, sc.Spec.Source.Decryption.PassphraseSecret, credsDir)
			if err != nil {
				return nil, "", dir, err
			}
			credsDir = dir
			cfg.PassphraseFile = passphraseFile
		}
		return cfg, prefix, credsDir, nil

	case v1alpha1.BackupStoragePVC:
		return nil, "", "", fmt.Errorf("PVC storage is not supported by the standby reconciler: " +
			"the operator pod does not mount the source cluster backup PVC; " +
			"use S3-compatible object storage for cross-cluster DR")

	default:
		return nil, "", "", fmt.Errorf("unknown storage type %q", storage.Type)
	}
}

// buildS3PitrCfg resolves S3 credentials from the referenced Kubernetes
// Secret and returns (PITRS3Config, scanPrefix, tempCredsDir, error).
// The scan prefix is s3.Prefix (the top-level prefix under which dumps and the
// binlogs/ subdir both live). tempCredsDir is non-empty when credentials were
// written to disk; the caller must remove it after the store is constructed.
func (r *MysqlStandbyClusterReconciler) buildS3PitrCfg(ctx context.Context, namespace string, s3 *v1alpha1.S3Storage) (*sidecar.PITRS3Config, string, string, error) {
	cfg := &sidecar.PITRS3Config{
		Bucket:      s3.Bucket,
		Region:      s3.Region,
		EndpointURL: s3.EndpointURL,
	}

	var credsDir string
	if s3.CredentialsSecret != "" {
		var err error
		credsDir, err = r.resolveS3CredsToDir(ctx, namespace, s3.CredentialsSecret)
		if err != nil {
			return nil, "", credsDir, fmt.Errorf("resolve S3 credentials from secret %q: %w", s3.CredentialsSecret, err)
		}
		cfg.AWSCredsDir = credsDir
	}

	return cfg, standbyNormalizePrefix(s3.Prefix), credsDir, nil
}

func (r *MysqlStandbyClusterReconciler) resolvePassphraseToFile(ctx context.Context, namespace string, ref v1alpha1.PassphraseSecretRef, dir string) (string, string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", dir, fmt.Errorf("Secret %q not found", ref.Name)
		}
		return "", dir, fmt.Errorf("get Secret %q: %w", ref.Name, err)
	}

	key := ref.PassphraseSecretKeyOrDefault()
	passphrase := strings.TrimSpace(string(secret.Data[key]))
	if passphrase == "" {
		return "", dir, fmt.Errorf("Secret %q key %q is missing or empty", ref.Name, key)
	}

	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "bloodraven-standby-creds-*")
		if err != nil {
			return "", "", fmt.Errorf("create passphrase tempdir: %w", err)
		}
	}
	passphraseFile := filepath.Join(dir, backupPassphraseFileName)
	if err := os.WriteFile(passphraseFile, []byte(passphrase), 0o600); err != nil {
		return "", dir, fmt.Errorf("write passphrase file: %w", err)
	}
	return passphraseFile, dir, nil
}

// resolveS3CredsToDir fetches the named Secret and writes its AWS credential
// keys into a temporary directory in the format sidecar.newS3Store expects
// (one plain-text file per key, matching the backup Job mount layout).
//
// Expected Secret keys:
//   - AWS_ACCESS_KEY_ID (required)
//   - AWS_SECRET_ACCESS_KEY (required)
//   - AWS_SESSION_TOKEN (optional)
//   - AWS_REGION (optional)
//
// Returns the path to the temp dir. The caller must remove it after the
// ArchiveStore has been constructed (the SDK reads the files once, at init).
func (r *MysqlStandbyClusterReconciler) resolveS3CredsToDir(ctx context.Context, namespace, secretName string) (string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("Secret %q not found", secretName)
		}
		return "", fmt.Errorf("get Secret %q: %w", secretName, err)
	}

	ak := strings.TrimSpace(string(secret.Data["AWS_ACCESS_KEY_ID"]))
	sk := strings.TrimSpace(string(secret.Data["AWS_SECRET_ACCESS_KEY"]))
	if ak == "" || sk == "" {
		return "", fmt.Errorf("Secret %q is missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY", secretName)
	}

	st := strings.TrimSpace(string(secret.Data["AWS_SESSION_TOKEN"]))
	rg := strings.TrimSpace(string(secret.Data["AWS_REGION"]))

	dir, err := standbyCreds(ak, sk, st, rg)
	if err != nil {
		return "", fmt.Errorf("create S3 creds tempdir: %w", err)
	}
	return dir, nil
}

// standbyCreds writes AWS credentials to a temporary directory in the same
// layout that sidecar.newS3Store expects (one file per env-var key). Returns
// the tempdir path. The caller must os.RemoveAll the returned dir when done.
func standbyCreds(accessKey, secretKey, sessionToken, region string) (string, error) {
	dir, err := os.MkdirTemp("", "bloodraven-standby-creds-*")
	if err != nil {
		return "", err
	}
	files := map[string]string{
		"AWS_ACCESS_KEY_ID":     accessKey,
		"AWS_SECRET_ACCESS_KEY": secretKey,
	}
	if sessionToken != "" {
		files["AWS_SESSION_TOKEN"] = sessionToken
	}
	if region != "" {
		files["AWS_REGION"] = region
	}
	for name, val := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(val), 0o600); err != nil {
			_ = os.RemoveAll(dir)
			return "", fmt.Errorf("write %s: %w", name, err)
		}
	}
	return dir, nil
}

// ----------------------------------------------------------------------
// Bucket scanning
// ----------------------------------------------------------------------

// standbyScanResult holds the parsed output of a single bucket scan.
// MetadataErr is non-nil when readDumpAtJSON failed (R-6); in that case
// DumpName is set (we found the directory) but the metadata fields are empty.
type standbyScanResult struct {
	DumpName                 string
	DumpLocation             string
	DumpCompletionTime       *metav1.Time
	DumpGtidExecuted         string
	DumpSizeBytes            int64
	OldestArchivedBinlogTime *metav1.Time
	NewestArchivedBinlogTime *metav1.Time
	ManifestCount            int32
	ArchivedBinlogCount      int32
	ArchivedBinlogBytes      int64
	// MetadataErr is set when the chosen dump's @.json could not be parsed.
	// The caller stamps SourceConfigKnown=False / MetadataUnreadable in that case.
	MetadataErr error
}

// scanBucket lists the bucket under prefix, finds the newest dump and reads
// binlog manifests. Returns a zero-value standbyScanResult on a clean-but-empty
// bucket (not an error).
//
// logger and sc are used to emit a Warning event (R-4) when LoadManifest fails
// for a specific manifest key.
func (r *MysqlStandbyClusterReconciler) scanBucket(
	ctx context.Context,
	logger interface{ Info(string, ...interface{}) },
	sc *v1alpha1.MysqlStandbyCluster,
	store sidecar.ArchiveStore,
	prefix string,
) (*standbyScanResult, error) {
	listPrefix := standbyListPrefix(prefix)
	keys, err := store.List(ctx, listPrefix)
	if err != nil {
		return nil, fmt.Errorf("list prefix %q: %w", listPrefix, err)
	}

	result := &standbyScanResult{}

	// -------------------------------------------------------------------
	// R-1: Find the newest dump directory by timestamp from @.json, not
	// lexicographic order.
	//
	// Layout: <prefix>/<backupName>/@.json
	// Backup names come from GenerateName with random base36 suffixes —
	// they do NOT sort by time. We:
	//   1. Collect all @.json-bearing directories.
	//   2. Read @.json for each candidate and pick the one with the largest
	//      "end" timestamp.
	//   3. Log if the timestamp-newest differs from the lex-newest.
	// -------------------------------------------------------------------
	var dumpDirs []string
	for _, key := range keys {
		if !strings.HasSuffix(key, "/"+standbyAtJSONSuffix) {
			continue
		}
		// dir is "<prefix>/<backupName>".
		dir := strings.TrimSuffix(key, "/"+standbyAtJSONSuffix)
		rel, ok := standbyRelUnderPrefix(dir, prefix)
		if !ok {
			continue
		}
		// Reject nested directories: a valid backup dir is exactly one
		// level deep under the prefix (no slash in rel).
		if rel != "" && !strings.Contains(rel, "/") {
			dumpDirs = append(dumpDirs, dir)
		}
	}

	if len(dumpDirs) > 0 {
		sort.Strings(dumpDirs)
		lexNewest := dumpDirs[len(dumpDirs)-1]

		// Read @.json for each candidate; pick the one with the largest end time.
		var bestDir string
		var bestEnd time.Time
		var bestMeta *standbyDumpMeta
		var bestMetaErr error
		for _, dir := range dumpDirs {
			atKey := dir + "/" + standbyAtJSONSuffix
			meta, metaErr := r.readDumpAtJSON(ctx, store, atKey)
			if metaErr != nil {
				// Record the error for the lex-newest; other candidates may still
				// be readable.
				if dir == lexNewest {
					bestMetaErr = metaErr
					bestDir = dir
					// Continue — a later candidate with a parseable @.json wins.
				}
				continue
			}
			var endTime time.Time
			if meta.DumpCompletionTime != nil {
				endTime = meta.DumpCompletionTime.Time
			}
			if bestDir == "" || endTime.After(bestEnd) {
				bestDir = dir
				bestEnd = endTime
				bestMeta = meta
				bestMetaErr = nil
			}
		}

		// If every candidate failed to parse, fall back to lex-newest with warning.
		if bestDir == "" {
			bestDir = lexNewest
			bestMetaErr = fmt.Errorf("all %d candidates had unreadable @.json", len(dumpDirs))
		}

		result.DumpName = path.Base(bestDir)
		result.DumpLocation = bestDir + "/"

		if bestMetaErr != nil {
			// R-6: propagate parse error so caller can stamp MetadataUnreadable.
			result.MetadataErr = bestMetaErr
		} else if bestMeta != nil {
			// Log if the timestamp-best differs from the lex-newest candidate.
			if bestDir != lexNewest {
				logger.Info("dump selection: timestamp-newest differs from lex-newest",
					"selectedDump", result.DumpName,
					"lexNewest", path.Base(lexNewest),
					"selectedEndTime", bestEnd)
			}
			result.DumpCompletionTime = bestMeta.DumpCompletionTime
			result.DumpGtidExecuted = bestMeta.DumpGtidExecuted
			result.DumpSizeBytes = bestMeta.DumpSizeBytes
		}
	}

	// -------------------------------------------------------------------
	// Read per-site binlog manifests under <prefix>/binlogs/.
	// Layout: <prefix>/binlogs/manifest-<site>.json
	// sidecar.ManifestKeyPrefix("<prefix>/binlogs") → "<prefix>/binlogs/manifest-"
	// -------------------------------------------------------------------
	binlogPrefix := path.Join(prefix, pitrBinlogSubprefix)
	manifestPrefix := sidecar.ManifestKeyPrefix(binlogPrefix)

	for _, key := range keys {
		if !strings.HasPrefix(key, manifestPrefix) || !strings.HasSuffix(key, ".json") {
			continue
		}

		site := sidecar.SiteFromManifestKey(key)
		if site == "" {
			continue
		}
		m, err := sidecar.LoadManifest(ctx, store, binlogPrefix, site)
		if err != nil {
			// R-4: do NOT increment ManifestCount for a failed LoadManifest.
			// Emit a Warning event so corruption is visible in the event stream.
			r.Recorder.Eventf(sc, corev1.EventTypeWarning, "ManifestParseFailed",
				"failed to parse binlog manifest %q: %v", key, err)
			continue
		}
		// R-4: only count after successful LoadManifest.
		result.ManifestCount++

		for _, f := range m.Files {
			result.ArchivedBinlogCount++
			result.ArchivedBinlogBytes += f.Size
			if !f.FirstEventTime.IsZero() {
				t := metav1.NewTime(f.FirstEventTime)
				if result.OldestArchivedBinlogTime == nil ||
					f.FirstEventTime.Before(result.OldestArchivedBinlogTime.Time) {
					result.OldestArchivedBinlogTime = &t
				}
			}
			if !f.LastEventTime.IsZero() {
				t := metav1.NewTime(f.LastEventTime)
				if result.NewestArchivedBinlogTime == nil ||
					f.LastEventTime.After(result.NewestArchivedBinlogTime.Time) {
					result.NewestArchivedBinlogTime = &t
				}
			}
		}
	}

	return result, nil
}

func standbyScanTimeout(interval time.Duration) time.Duration {
	if interval > 0 && interval < standbyDefaultScanTimeout {
		return interval
	}
	return standbyDefaultScanTimeout
}

func standbyNormalizePrefix(prefix string) string {
	return strings.Trim(prefix, "/")
}

func standbyListPrefix(prefix string) string {
	prefix = standbyNormalizePrefix(prefix)
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

func standbyRelUnderPrefix(key, prefix string) (string, bool) {
	prefix = standbyNormalizePrefix(prefix)
	if prefix == "" {
		return strings.TrimPrefix(key, "/"), true
	}
	boundary := prefix + "/"
	if !strings.HasPrefix(key, boundary) {
		return "", false
	}
	return strings.TrimPrefix(key, boundary), true
}

// mysqlshAtJSON is the raw JSON shape mysqlsh writes to the dump's @.json file.
// Unknown fields are silently ignored; this is a subset of what mysqlsh writes.
type mysqlshAtJSON struct {
	// End is the dump completion timestamp in RFC3339 format.
	End string `json:"end"`
	// GtidExecuted is the GTID set captured at dump time.
	GtidExecuted string `json:"gtidExecuted"`
	// TotalBytes is the total size of the dump in bytes.
	TotalBytes int64 `json:"totalBytes"`
	// TotalDataBytes is an alternative size field used in older mysqlsh versions.
	TotalDataBytes int64 `json:"totalDataBytes"`
}

// standbyDumpMeta is the parsed output of readDumpAtJSON.
// Fields are nil/zero when @.json is missing or partially readable.
type standbyDumpMeta struct {
	DumpCompletionTime *metav1.Time
	DumpGtidExecuted   string
	DumpSizeBytes      int64
}

// readDumpAtJSON fetches and parses the @.json file at key from the store.
// A missing file returns an empty standbyDumpMeta (not an error).
// A malformed file returns the parse error — the caller must handle it (R-6).
func (r *MysqlStandbyClusterReconciler) readDumpAtJSON(ctx context.Context, store sidecar.ArchiveStore, key string) (*standbyDumpMeta, error) {
	data, ok, err := store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	if !ok {
		return &standbyDumpMeta{}, nil
	}
	var raw mysqlshAtJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		// R-6: propagate the parse error; do not silently return empty metadata.
		return nil, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	out := &standbyDumpMeta{
		DumpGtidExecuted: raw.GtidExecuted,
	}
	if raw.TotalBytes > 0 {
		out.DumpSizeBytes = raw.TotalBytes
	} else if raw.TotalDataBytes > 0 {
		out.DumpSizeBytes = raw.TotalDataBytes
	}
	if raw.End != "" {
		if t, err := time.Parse(time.RFC3339, raw.End); err == nil {
			mt := metav1.NewTime(t)
			out.DumpCompletionTime = &mt
		}
	}
	return out, nil
}

// ----------------------------------------------------------------------
// Status patching
// ----------------------------------------------------------------------

// standbyConditionPair bundles the two conditions Phase 1 manages.
type standbyConditionPair struct {
	bucketReadable metav1.ConditionStatus
	bucketReason   string
	bucketMsg      string
	sourceKnown    metav1.ConditionStatus
	sourceReason   string
	sourceMsg      string
}

// stampConditionsAndRequeue applies the conditions and discovered status to the
// CR, then returns a ctrl.Result that requeues after interval.
//
// Idempotency: setCondition preserves LastTransitionTime when status is
// unchanged, so this call is safe to repeat with the same inputs.
func (r *MysqlStandbyClusterReconciler) stampConditionsAndRequeue(
	ctx context.Context,
	sc *v1alpha1.MysqlStandbyCluster,
	cp standbyConditionPair,
	interval time.Duration,
	discovered *standbyScanResult,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	now := metav1.Now()
	patch := client.MergeFrom(sc.DeepCopy())

	// R-2: when stamping BucketReadable=False, preserve Status.Discovered
	// (do not nil it). The condition Message already carries the staleness
	// context from the caller. No special action needed here — we only
	// update Status.Discovered when discovered != nil.

	setCondition(&sc.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.StandbyConditionBucketReadable,
		Status:             cp.bucketReadable,
		ObservedGeneration: sc.Generation,
		LastTransitionTime: now,
		Reason:             cp.bucketReason,
		Message:            cp.bucketMsg,
	})
	setCondition(&sc.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.StandbyConditionSourceConfigKnown,
		Status:             cp.sourceKnown,
		ObservedGeneration: sc.Generation,
		LastTransitionTime: now,
		Reason:             cp.sourceReason,
		Message:            cp.sourceMsg,
	})
	sc.Status.ObservedGeneration = sc.Generation

	if discovered != nil {
		// R-5: LastScanAt is the wall-clock time of the most recent successful
		// bucket scan. BucketScanned events remain suppressed unless substantive
		// discovered content changes.
		//
		// Algorithm:
		//   1. Build newDiscovered WITHOUT LastScanAt.
		//   2. Build a copy of the existing Discovered WITHOUT its LastScanAt.
		//   3. Deep-equal compare the two copies.
		//   4. Always set LastScanAt = now and replace sc.Status.Discovered.
		//   5. Only if they differ: emit the BucketScanned event.
		newDiscovered := &v1alpha1.StandbyDiscovered{
			DumpName:                 discovered.DumpName,
			DumpLocation:             discovered.DumpLocation,
			DumpCompletionTime:       discovered.DumpCompletionTime,
			DumpGtidExecuted:         discovered.DumpGtidExecuted,
			DumpSizeBytes:            discovered.DumpSizeBytes,
			OldestArchivedBinlogTime: discovered.OldestArchivedBinlogTime,
			NewestArchivedBinlogTime: discovered.NewestArchivedBinlogTime,
			ManifestCount:            discovered.ManifestCount,
			ArchivedBinlogCount:      discovered.ArchivedBinlogCount,
			ArchivedBinlogBytes:      discovered.ArchivedBinlogBytes,
			// LastScanAt intentionally left nil for comparison purposes.
		}

		// Determine whether the substantive fields changed.
		changed := true
		if sc.Status.Discovered != nil {
			prevNoScan := sc.Status.Discovered.DeepCopy()
			prevNoScan.LastScanAt = nil
			prevNoScan.Message = ""
			changed = !equality.Semantic.DeepEqual(prevNoScan, newDiscovered)
		}

		newDiscovered.LastScanAt = &now
		sc.Status.Discovered = newDiscovered
		if changed {
			// R-5 (BucketScanned event): emit only on a state transition
			// (DumpName changed, ArchivedBinlogCount changed, or BucketReadable
			// changed from False to True).
			r.Recorder.Eventf(sc, corev1.EventTypeNormal, "BucketScanned",
				"scanned prefix: dump=%q manifests=%d binlogs=%d",
				discovered.DumpName, discovered.ManifestCount, discovered.ArchivedBinlogCount)
		}
	}

	if err := r.Status().Patch(ctx, sc, patch); err != nil {
		logger.Error(err, "failed to patch standby status")
		return ctrl.Result{RequeueAfter: interval}, fmt.Errorf("patch status: %w", err)
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}
