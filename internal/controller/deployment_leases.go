package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DeploymentLeaseView is the public, secret-free lease representation.
type DeploymentLeaseView struct {
	Kind               string    `json:"kind"`
	OperationID        string    `json:"operationId"`
	Instance           string    `json:"instance"`
	ExpiresAt          time.Time `json:"expiresAt"`
	TopologyGeneration int64     `json:"topologyGeneration"`
}

type DeploymentLeaseResult struct {
	DeploymentLeaseView
	Token  string `json:"token,omitempty"`
	Status int    `json:"-"`
}

type DeploymentLeaseError struct {
	Status             int                  `json:"-"`
	Code               string               `json:"error"`
	Reason             string               `json:"reason,omitempty"`
	Holder             *DeploymentLeaseView `json:"holder,omitempty"`
	RetryAfterSeconds  int                  `json:"retryAfterSeconds,omitempty"`
	TopologyGeneration int64                `json:"topologyGeneration,omitempty"`
}

func (e *DeploymentLeaseError) Error() string { return e.Code + ": " + e.Reason }

// deploymentLeaseRecordSchemaVersion is deploymentLeaseRecord.SchemaVersion
// (A-80). A record with another version is refused (errLeaseRecordVersion).
const deploymentLeaseRecordSchemaVersion = 1

type deploymentLeaseRecord struct {
	SchemaVersion int `json:"schemaVersion"`
	DeploymentLeaseView
	Namespace      string    `json:"namespace"`
	ServiceAccount string    `json:"serviceAccount"`
	TokenHash      string    `json:"tokenHash"`
	CreatedAt      time.Time `json:"createdAt"`
	State          string    `json:"state"`
	Reason         string    `json:"reason,omitempty"`
}

// DeploymentLeaseManager must be shared by the leader's HTTP API and controllers.
// Reader must bypass the informer cache. The mutex serializes admission and API
// writes, not MySQL actions; Kubernetes resource versions fence competing writers.
type DeploymentLeaseManager struct {
	client   client.Client
	reader   client.Reader
	recorder record.EventRecorder
	mu       sync.Mutex
	now      func() time.Time
	planned  map[types.NamespacedName]string
	// pending is separate from admission: emergency topology changes never wait
	// for a lease read/write or the admission mutex.
	pending         sync.Map
	epoch           atomic.Uint64
	requireTopology atomic.Bool
	observed        sync.Map
	confirmed       sync.Map
	wake            chan struct{}
	// plannedClaimHeld reports whether a group's planned-failover claim slot
	// is held (a run, a provisional grant, or an automation hold). Nil means
	// no runner: only status is consulted.
	plannedClaimHeld atomic.Pointer[func(types.NamespacedName) bool]
	// plannedHoldHeld reports whether that slot holds an automation hold
	// (rather than a run's claim). Nil means no runner.
	plannedHoldHeld atomic.Pointer[func(types.NamespacedName) bool]
	// refusals reports a lease record of an unknown schemaVersion. Nil
	// means no runner.
	refusals atomic.Pointer[leaseRefusalReporter]
}

// The operations that take BeginPlanned's admission reservation. A refusal
// while another one holds the reservation carries the holder's name as
// its reason, so each name is the unstableReason the operation reports.
const (
	deploymentOperationPlannedFailover = v1alpha1.UnstableReasonPlannedFailover
	deploymentOperationRestoreInPlace  = v1alpha1.UnstableReasonRestoreInPlace
	deploymentOperationOrderedUpdate   = v1alpha1.UnstableReasonOrderedUpdate
)

func NewDeploymentLeaseManager(c client.Client, reader client.Reader, recorder record.EventRecorder) *DeploymentLeaseManager {
	if reader == nil {
		reader = c
	}
	return &DeploymentLeaseManager{client: c, reader: reader, recorder: recorder, now: time.Now, planned: make(map[types.NamespacedName]string), wake: make(chan struct{}, 1)}
}

// SetClock is a test seam. Configure before serving requests.
func (m *DeploymentLeaseManager) SetClock(now func() time.Time) { m.now = now }

// SetPlannedClaimHeld injects the runner's planned-failover claim check, so
// admission and stability also exclude a provisional grant and a hold that
// status does not show yet.
func (m *DeploymentLeaseManager) SetPlannedClaimHeld(held func(types.NamespacedName) bool) {
	if held == nil {
		m.plannedClaimHeld.Store(nil)
		return
	}
	m.plannedClaimHeld.Store(&held)
}

// SetPlannedHoldHeld injects the runner's check that the claim slot holds
// an automation hold, which UnstableReason reports as
// AutomaticFailoverHeld rather than PlannedFailover.
func (m *DeploymentLeaseManager) SetPlannedHoldHeld(hold func(types.NamespacedName) bool) {
	if hold == nil {
		m.plannedHoldHeld.Store(nil)
		return
	}
	m.plannedHoldHeld.Store(&hold)
}

// claimHeld reports the injected planned-failover claim check for fg.
func (m *DeploymentLeaseManager) claimHeld(fg *v1alpha1.MysqlFailoverGroup) bool {
	held := m.plannedClaimHeld.Load()
	return held != nil && (*held)(client.ObjectKeyFromObject(fg))
}

// holdClaimed reports the injected automation-hold check for fg.
func (m *DeploymentLeaseManager) holdClaimed(fg *v1alpha1.MysqlFailoverGroup) bool {
	hold := m.plannedHoldHeld.Load()
	return hold != nil && (*hold)(client.ObjectKeyFromObject(fg))
}

// UnstableReason is DeploymentUnstableReason plus the planned-failover
// claim slot and the durable automation hold. A run's claim, held before
// status shows the run, is "PlannedFailover". A standing automation hold,
// loaded into the slot or only durable (before the slot has been hydrated
// from it), is "AutomaticFailoverHeld": unlike a run it does not end by
// itself.
func (m *DeploymentLeaseManager) UnstableReason(fg *v1alpha1.MysqlFailoverGroup) string {
	reason := DeploymentUnstableReason(fg)
	switch reason {
	case v1alpha1.UnstableReasonDeploymentRevocationPending, v1alpha1.UnstableReasonInitializing, v1alpha1.UnstableReasonNoPrimary, v1alpha1.UnstableReasonPlannedFailover:
		return reason
	}
	holdClaimed := m.holdClaimed(fg)
	if m.claimHeld(fg) && !holdClaimed {
		return v1alpha1.UnstableReasonPlannedFailover
	}
	if holdClaimed || PlannedHoldFrom(fg) != nil {
		return v1alpha1.UnstableReasonAutomaticFailoverHeld
	}
	return reason
}

func deploymentLeaseName(group, kind, operation string) string {
	name := "bloodraven-deploy-" + group + "-" + kind
	if operation != "" {
		h := sha256.Sum256([]byte(operation))
		name += "-" + hex.EncodeToString(h[:16])
	}
	if len(name) > 63 {
		h := sha256.Sum256([]byte(name))
		name = strings.TrimRight(name[:38], "-.") + "-" + hex.EncodeToString(h[:12])
	}
	return name
}

func leaseTokenHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

func (m *DeploymentLeaseManager) read(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind, operation string) (*coordinationv1.Lease, *deploymentLeaseRecord, error) {
	l := &coordinationv1.Lease{}
	name := deploymentLeaseName(fg.Name, kind, operation)
	err := m.reader.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, l)
	if apierrors.IsNotFound(err) {
		// A refusal of a record this operator could not read ends with
		// the Lease that carried it.
		if report := m.refusals.Load(); report != nil {
			(*report).clear(client.ObjectKeyFromObject(fg), leaseRefusalKey(name))
		}
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if len(l.OwnerReferences) != 1 || l.OwnerReferences[0].UID != fg.UID {
		return nil, nil, fmt.Errorf("deployment Lease belongs to another group UID")
	}
	r, err := m.decodeRecord(fg, l)
	if err != nil {
		return nil, nil, err
	}
	return l, r, nil
}

// decodeRecord reads l's lease record. A record with a schemaVersion this
// operator does not know is refused, and reported as a group refusal
// (Degraded=True/MetadataVersionUnsupported and a Warning Event); a
// readable one clears that refusal.
func (m *DeploymentLeaseManager) decodeRecord(fg *v1alpha1.MysqlFailoverGroup, l *coordinationv1.Lease) (*deploymentLeaseRecord, error) {
	raw := l.Annotations[v1alpha1.DeploymentLeaseAnnotation]
	key := leaseRefusalKey(l.Name)
	var r deploymentLeaseRecord
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil, fmt.Errorf("invalid deployment Lease: %w", err)
	}
	if r.SchemaVersion != deploymentLeaseRecordSchemaVersion {
		msg := fmt.Sprintf("deployment Lease %s has a %s record with schemaVersion %d, which this operator does not know (it reads %d); "+
			"lease requests for the group are refused until the Lease is deleted or an operator release that reads it runs",
			l.Name, v1alpha1.DeploymentLeaseAnnotation, r.SchemaVersion, deploymentLeaseRecordSchemaVersion)
		if report := m.refusals.Load(); report != nil {
			(*report).set(fg, key, v1alpha1.ReasonMetadataVersionUnsupported, msg)
		}
		return nil, errLeaseRecordVersion{msg: msg}
	}
	if report := m.refusals.Load(); report != nil {
		(*report).clear(client.ObjectKeyFromObject(fg), key)
	}
	return &r, nil
}

// leaseRefusalPrefix prefixes the group refusal key of a Lease whose
// record this operator cannot read.
const leaseRefusalPrefix = "lease/"

func leaseRefusalKey(leaseName string) string { return leaseRefusalPrefix + leaseName }

// pruneLeaseRefusals clears fg's lease refusals whose Lease is gone. Only
// a successful decode of the same Lease clears a refusal otherwise, and a
// deleted Lease is never decoded again: Grant refuses while Degraded
// reports the refusal, before it reads any Lease, and housekeeping lists
// only the hold Leases that still exist. Read uncached, as every Lease
// read here is.
func (m *DeploymentLeaseManager) pruneLeaseRefusals(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	report := m.refusals.Load()
	if report == nil || (*report).keys == nil {
		return nil
	}
	nn := client.ObjectKeyFromObject(fg)
	for _, key := range (*report).keys(nn, leaseRefusalPrefix) {
		name := strings.TrimPrefix(key, leaseRefusalPrefix)
		err := m.reader.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &coordinationv1.Lease{})
		if apierrors.IsNotFound(err) {
			(*report).clear(nn, key)
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// errLeaseRecordVersion is a lease record with an unknown schemaVersion.
type errLeaseRecordVersion struct{ msg string }

func (e errLeaseRecordVersion) Error() string { return e.msg }

// leaseRefusalReporter is the runner's group refusal registry, as the
// lease manager uses it.
type leaseRefusalReporter struct {
	set   func(fg *v1alpha1.MysqlFailoverGroup, key, reason, message string)
	clear func(nn types.NamespacedName, key string)
	keys  func(nn types.NamespacedName, prefix string) []string
}

// SetGroupRefusals injects the runner's group refusal registry
// (TopologyManagerRunner.SetGroupRefusal and ClearGroupRefusal), which
// reports a lease record of an unknown schemaVersion. Nil reports nothing.
func (m *DeploymentLeaseManager) SetGroupRefusals(r *TopologyManagerRunner) {
	if r == nil {
		m.refusals.Store(nil)
		return
	}
	m.refusals.Store(&leaseRefusalReporter{set: r.SetGroupRefusal, clear: r.ClearGroupRefusal, keys: r.GroupRefusalKeys})
}

func (m *DeploymentLeaseManager) write(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, l *coordinationv1.Lease, r *deploymentLeaseRecord, operation string) error {
	create := l == nil
	if create {
		l = &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: deploymentLeaseName(fg.Name, r.Kind, operation), Namespace: fg.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "MysqlFailoverGroup", Name: fg.Name, UID: fg.UID}}}}
	}
	r.SchemaVersion = deploymentLeaseRecordSchemaVersion
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if l.Annotations == nil {
		l.Annotations = make(map[string]string)
	}
	l.Annotations[v1alpha1.DeploymentLeaseAnnotation] = string(b)
	if l.Labels == nil {
		l.Labels = make(map[string]string)
	}
	l.Labels[v1alpha1.DeploymentKindLabel] = r.Kind
	l.Labels[v1alpha1.DeploymentStateLabel] = r.State
	l.Spec.HolderIdentity = &r.OperationID
	now := metav1.NewMicroTime(m.now())
	l.Spec.RenewTime = &now
	// Kubernetes requires a positive duration. The record's state and exact
	// ExpiresAt, not this rounded projection, govern admission and fencing.
	ttl := int32(max(1, r.ExpiresAt.Sub(now.Time).Seconds()))
	l.Spec.LeaseDurationSeconds = &ttl
	if create {
		return m.client.Create(ctx, l)
	}
	return m.client.Update(ctx, l)
}

func (m *DeploymentLeaseManager) archive(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, r *deploymentLeaseRecord) error {
	// Revocation tombstones intentionally live until the owning group is deleted:
	// expiring them would let a delayed client reuse a revoked operation ID.
	l, _, err := m.read(ctx, fg, r.Kind, r.OperationID)
	if err != nil {
		return err
	}
	return m.write(ctx, fg, l, r, r.OperationID)
}

// current expires/revokes durably before allowing the slot to be reused.
func (m *DeploymentLeaseManager) current(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind string, operation ...string) (*coordinationv1.Lease, *deploymentLeaseRecord, error) {
	id := ""
	if kind == "failover-hold" && len(operation) > 0 {
		id = operation[0]
	}
	l, r, err := m.read(ctx, fg, kind, id)
	if err != nil || r == nil {
		return l, r, err
	}
	if r.State == "active" {
		reason := ""
		if m.now().After(r.ExpiresAt) {
			r.State = "expired"
		} else if _, pending := fg.Annotations[v1alpha1.RevokeDeploymentLeasesAnnotation]; pending {
			reason = v1alpha1.DeploymentLeaseRevokedByOperator
		} else if r.TopologyGeneration != fg.Status.TopologyGeneration {
			reason = v1alpha1.DeploymentLeaseRevokedTopologyChanged
		}
		if reason != "" {
			r.State, r.Reason = "revoked", reason
		}
		if r.State != "active" {
			if err := m.write(ctx, fg, l, r, id); err != nil {
				return nil, nil, err
			}
			if r.State == "revoked" {
				m.revocation(fg, r)
			} else {
				metrics.DeployLeaseExpirationsTotal.WithLabelValues(fg.Name, kind).Inc()
			}
		}
	}
	active, age := 0.0, 0.0
	if r.State == "active" {
		active, age = 1, max(0, m.now().Sub(r.CreatedAt).Seconds())
	}
	if kind == "migration" {
		metrics.DeployLeasesActive.WithLabelValues(fg.Name, kind).Set(active)
		metrics.DeployLeaseAgeSeconds.WithLabelValues(fg.Name, kind).Set(age)
	}
	return l, r, nil
}

func (m *DeploymentLeaseManager) currentHolds(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) ([]DeploymentLeaseView, error) {
	var leases coordinationv1.LeaseList
	if err := m.reader.List(ctx, &leases, client.InNamespace(fg.Namespace), client.MatchingLabels{
		v1alpha1.DeploymentKindLabel:  "failover-hold",
		v1alpha1.DeploymentStateLabel: "active",
	}); err != nil {
		return nil, err
	}
	views := make([]DeploymentLeaseView, 0, len(leases.Items))
	oldestAge := 0.0
	for i := range leases.Items {
		l := &leases.Items[i]
		if len(l.OwnerReferences) != 1 || l.OwnerReferences[0].UID != fg.UID {
			continue
		}
		record, err := m.decodeRecord(fg, l)
		if err != nil {
			return nil, err
		}
		_, r, err := m.current(ctx, fg, "failover-hold", record.OperationID)
		if err != nil {
			return nil, err
		}
		if r != nil && r.State == "active" {
			views = append(views, r.DeploymentLeaseView)
			oldestAge = max(oldestAge, m.now().Sub(r.CreatedAt).Seconds())
		}
	}
	sort.Slice(views, func(i, j int) bool {
		if views[i].ExpiresAt.Equal(views[j].ExpiresAt) {
			return views[i].OperationID < views[j].OperationID
		}
		return views[i].ExpiresAt.Before(views[j].ExpiresAt)
	})
	metrics.DeployLeasesActive.WithLabelValues(fg.Name, "failover-hold").Set(float64(len(views)))
	metrics.DeployLeaseAgeSeconds.WithLabelValues(fg.Name, "failover-hold").Set(oldestAge)
	return views, nil
}

func (m *DeploymentLeaseManager) revocation(fg *v1alpha1.MysqlFailoverGroup, r *deploymentLeaseRecord) {
	metrics.DeployLeaseRevocationsTotal.WithLabelValues(fg.Name, r.Kind, r.Reason).Inc()
	if m.recorder != nil {
		m.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDeploymentLeaseRevoked, "%s lease for operation %s instance %s revoked: %s", r.Kind, r.OperationID, r.Instance, r.Reason)
	}
}

// DeploymentUnstableReason mirrors the companion API stability contract.
func DeploymentUnstableReason(fg *v1alpha1.MysqlFailoverGroup) string {
	if _, pending := fg.Annotations[v1alpha1.RevokeDeploymentLeasesAnnotation]; pending {
		return v1alpha1.UnstableReasonDeploymentRevocationPending
	}
	if fg.Status.ActiveSite == "" {
		if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionReady); c != nil && c.Reason == v1alpha1.ReasonInitializing {
			return v1alpha1.UnstableReasonInitializing
		}
		return v1alpha1.UnstableReasonNoPrimary
	}
	if plannedFailoverActive(fg) {
		return v1alpha1.UnstableReasonPlannedFailover
	}
	if fg.Status.UpdatePhase != "" {
		return v1alpha1.UnstableReasonOrderedUpdate
	}
	if restoreInFlight(fg) || (fg.Status.Restore != nil && fg.Status.Restore.Phase != "" && fg.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded && fg.Status.Restore.Phase != v1alpha1.BackupPhaseFailed) {
		return v1alpha1.UnstableReasonRestore
	}
	if d := fg.Status.Dragonfly; d != nil && d.Upgrade != nil && d.Upgrade.Phase != "" && d.Upgrade.Phase != v1alpha1.DragonflyUpgradePhaseSucceeded && d.Upgrade.Phase != v1alpha1.DragonflyUpgradePhaseFailed {
		return v1alpha1.UnstableReasonDragonflyUpgrade
	}
	if d := fg.Status.Dragonfly; d != nil {
		if reason, ok := dragonflyUnstableReasons[d.Phase]; ok {
			return reason
		}
	}
	if s := fg.Status.RestoreInPlace; s != nil && s.Phase != v1alpha1.RestoreInPlaceSucceeded && s.Phase != v1alpha1.RestoreInPlaceFailed && s.Phase != "" {
		return v1alpha1.UnstableReasonRestoreInPlace
	}
	auditOnly := divergenceIsReplicaAuditOnly(fg)
	for _, name := range []string{v1alpha1.ConditionDegraded, v1alpha1.ConditionRecoveryPending, v1alpha1.ConditionBootstrapping} {
		if c := apimeta.FindStatusCondition(fg.Status.Conditions, name); c != nil && c.Status == metav1.ConditionTrue {
			if name == v1alpha1.ConditionDegraded && c.Reason == v1alpha1.ReasonReplicationLagging {
				continue
			}
			// A replica-audit divergence report is an integrity warning
			// about a site that keeps replicating; like lag it does not
			// make the group unstable for deployments. Anything it could
			// hide is still checked below (Ready, per-site replication and
			// source convergence). A returning old primary's report still
			// gates, as it always has.
			if c.Reason == v1alpha1.ReasonDivergentTransactions && (name == v1alpha1.ConditionDegraded || auditOnly) {
				continue
			}
			// RecoveryPending summarizes one site. When it names a
			// replica's divergence but another site's recovery is what
			// gates, report that site's state instead.
			if name == v1alpha1.ConditionRecoveryPending && c.Reason == v1alpha1.ReasonDivergentTransactions {
				if reason := gatingRecoveryState(fg); reason != "" {
					return reason
				}
			}
			if c.Reason != "" {
				return c.Reason
			}
			return name
		}
	}
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionReady); c == nil || c.Status != metav1.ConditionTrue {
		return v1alpha1.UnstableReasonNotReady
	}
	for _, configured := range fg.Spec.Sites {
		found := false
		for _, s := range fg.Status.Sites {
			if s.Name == configured.Name {
				found = true
				break
			}
		}
		if !found {
			return v1alpha1.UnstableReasonSiteNotObserved
		}
	}
	for _, s := range fg.Status.Sites {
		if s.Name == fg.Status.ActiveSite && s.State != v1alpha1.SiteStateWritable {
			return v1alpha1.UnstableReasonNoPrimary
		}
		if s.Name != fg.Status.ActiveSite && s.State != v1alpha1.SiteStateReadOnly {
			return v1alpha1.UnstableReasonSiteNotReady
		}
		if s.Name != fg.Status.ActiveSite && restoreReclonePending(fg, s.Name) {
			// Held out of failover and replication until its
			// post-restore reclone.
			return v1alpha1.UnstableReasonRestoreReclonePending
		}
		if s.RecoveryState != "" && !(s.RecoveryState == v1alpha1.RecoveryStateBlocked && s.DivergenceDetectedBy == divergenceDetectedByAudit) {
			return string(s.RecoveryState)
		}
		if reason, ok := sourceConvergenceUnstableReasons[s.SourceConvergenceState]; ok {
			return reason
		}
		if s.Name != fg.Status.ActiveSite && !s.Replication.Running() {
			return v1alpha1.UnstableReasonReplicationNotRunning
		}
	}
	return ""
}

// dragonflyUnstableReasons maps each Dragonfly phase that makes the group
// unstable to its unstableReason. Disabled and Ready do not.
var dragonflyUnstableReasons = map[v1alpha1.DragonflyPhase]string{
	v1alpha1.DragonflyPhaseReconciling:            v1alpha1.UnstableReasonDragonflyReconciling,
	v1alpha1.DragonflyPhaseConfiguringReplication: v1alpha1.UnstableReasonDragonflyConfiguringReplication,
	v1alpha1.DragonflyPhaseDegraded:               v1alpha1.UnstableReasonDragonflyDegraded,
	v1alpha1.DragonflyPhasePromoting:              v1alpha1.UnstableReasonDragonflyPromoting,
}

// sourceConvergenceUnstableReasons maps each source-convergence state that
// makes the group unstable to its unstableReason. Converged does not.
var sourceConvergenceUnstableReasons = map[v1alpha1.SourceConvergenceState]string{
	v1alpha1.SourceConvergencePending: v1alpha1.UnstableReasonSourceConvergencePending,
	v1alpha1.SourceConvergenceBlocked: v1alpha1.UnstableReasonSourceConvergenceBlocked,
}

// divergenceIsReplicaAuditOnly reports whether every site in recovery
// carries a replica-audit divergence report, so RecoveryPending describes
// nothing but those. A report restored without provenance (an older CRD
// pruned the field) counts as an old primary's.
func divergenceIsReplicaAuditOnly(fg *v1alpha1.MysqlFailoverGroup) bool {
	for _, s := range fg.Status.Sites {
		if s.RecoveryState == "" {
			continue
		}
		if s.RecoveryState != v1alpha1.RecoveryStateBlocked || s.DivergenceDetectedBy != divergenceDetectedByAudit {
			return false
		}
	}
	return true
}

// gatingRecoveryState returns the recovery state of the first site whose
// recovery gates the deployment API (anything but a replica-audit
// divergence report), as a reason: "DivergentTransactions" for an old
// primary's block (or one without provenance), otherwise the state.
func gatingRecoveryState(fg *v1alpha1.MysqlFailoverGroup) string {
	for _, s := range fg.Status.Sites {
		switch {
		case s.RecoveryState == "":
		case s.RecoveryState == v1alpha1.RecoveryStateBlocked && s.DivergenceDetectedBy == divergenceDetectedByAudit:
		case s.RecoveryState == v1alpha1.RecoveryStateBlocked:
			return v1alpha1.ReasonDivergentTransactions
		default:
			return string(s.RecoveryState)
		}
	}
	return ""
}

func (m *DeploymentLeaseManager) fresh(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (*v1alpha1.MysqlFailoverGroup, error) {
	fresh := &v1alpha1.MysqlFailoverGroup{}
	if err := m.reader.Get(ctx, client.ObjectKeyFromObject(fg), fresh); err != nil {
		return nil, err
	}
	if fresh.UID != fg.UID {
		return nil, fmt.Errorf("failover group was replaced")
	}
	return fresh, nil
}

func (m *DeploymentLeaseManager) gate(fg *v1alpha1.MysqlFailoverGroup) error {
	nn := client.ObjectKeyFromObject(fg)
	if _, pending := fg.Annotations[v1alpha1.RevokeDeploymentLeasesAnnotation]; pending {
		return &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: v1alpha1.UnstableReasonDeploymentRevocationPending, RetryAfterSeconds: 1}
	}
	if err := m.topologyGate(fg); err != nil {
		return err
	}
	if reason := m.planned[nn]; reason != "" {
		return &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: reason, RetryAfterSeconds: 1}
	}
	return nil
}

func (m *DeploymentLeaseManager) topologyGate(fg *v1alpha1.MysqlFailoverGroup) error {
	nn := client.ObjectKeyFromObject(fg)
	if _, pending := m.pending.Load(nn); pending {
		return &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: v1alpha1.UnstableReasonTopologyPersistencePending, RetryAfterSeconds: 1}
	}
	if uid, observed := m.observed.Load(nn); m.requireTopology.Load() && (!observed || uid != fg.UID) {
		return &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: v1alpha1.UnstableReasonTopologyNotObserved, RetryAfterSeconds: 1}
	}
	return nil
}

func sameDeploymentHolder(r *deploymentLeaseRecord, operation, instance, namespace, sa string) bool {
	return r.OperationID == operation && r.Instance == instance && r.Namespace == namespace && r.ServiceAccount == sa
}

func validDeploymentKind(kind string) bool { return kind == "migration" || kind == "failover-hold" }

func (m *DeploymentLeaseManager) Grant(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind, operationID, instance, namespace, serviceAccount string, ttl int) (DeploymentLeaseResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validDeploymentKind(kind) || operationID == "" || instance == "" || namespace == "" || serviceAccount == "" {
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 400, Code: "invalid_request"}
	}
	fg, err := m.fresh(ctx, fg)
	if err != nil {
		return DeploymentLeaseResult{}, err
	}
	if err := m.gate(fg); err != nil {
		metrics.DeployLeaseGrantsTotal.WithLabelValues(fg.Name, kind, "unstable").Inc()
		return DeploymentLeaseResult{}, err
	}
	if reason := m.UnstableReason(fg); reason != "" {
		metrics.DeployLeaseGrantsTotal.WithLabelValues(fg.Name, kind, "unstable").Inc()
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: reason, RetryAfterSeconds: 1}
	}
	if kind == "failover-hold" {
		_, migration, err := m.current(ctx, fg, "migration")
		if err != nil {
			return DeploymentLeaseResult{}, err
		}
		if migration == nil || migration.State != "active" || !sameDeploymentHolder(migration, operationID, instance, namespace, serviceAccount) {
			metrics.DeployLeaseGrantsTotal.WithLabelValues(fg.Name, kind, "unauthorized").Inc()
			return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 403, Code: "forbidden"}
		}
	}
	l, old, err := m.current(ctx, fg, kind, operationID)
	if err != nil {
		return DeploymentLeaseResult{}, err
	}
	status := 201
	created := m.now()
	if old != nil {
		if old.State == "active" {
			if !sameDeploymentHolder(old, operationID, instance, namespace, serviceAccount) {
				metrics.DeployLeaseGrantsTotal.WithLabelValues(fg.Name, kind, "conflict").Inc()
				return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 409, Code: "held", Holder: &old.DeploymentLeaseView}
			}
			status, created = 200, old.CreatedAt
		} else if old.OperationID != operationID && old.State == "revoked" {
			// Only revocations need to outlive the slot. Archiving released or
			// expired records would leak one Lease per historical operation ID
			// without gating anything: the tombstone check below rejects
			// "revoked" only.
			if err := m.archive(ctx, fg, old); err != nil {
				return DeploymentLeaseResult{}, err
			}
		}
	}
	// A revoked operation may never silently acquire a new fencing epoch.
	_, tombstone, err := m.read(ctx, fg, kind, operationID)
	if err != nil {
		return DeploymentLeaseResult{}, err
	}
	if old != nil && old.OperationID == operationID && old.State == "revoked" {
		tombstone = old
	}
	if tombstone != nil && tombstone.State == "revoked" {
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 409, Code: "revoked", Reason: tombstone.Reason, TopologyGeneration: fg.Status.TopologyGeneration}
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return DeploymentLeaseResult{}, err
	}
	token := hex.EncodeToString(secret)
	r := &deploymentLeaseRecord{DeploymentLeaseView: DeploymentLeaseView{Kind: kind, OperationID: operationID, Instance: instance, ExpiresAt: m.now().Add(time.Duration(max(5, min(120, ttl))) * time.Second), TopologyGeneration: fg.Status.TopologyGeneration}, Namespace: namespace, ServiceAccount: serviceAccount, TokenHash: leaseTokenHash(token), CreatedAt: created, State: "active"}
	leaseOperation := ""
	if kind == "failover-hold" {
		leaseOperation = operationID
	}
	if err := m.write(ctx, fg, l, r, leaseOperation); err != nil {
		return DeploymentLeaseResult{}, err
	}
	if kind == "failover-hold" {
		if _, err := m.currentHolds(ctx, fg); err != nil {
			return DeploymentLeaseResult{}, err
		}
	}
	// An emergency may have started while the API-server write was in flight.
	if err := m.gate(fg); err != nil {
		metrics.DeployLeaseGrantsTotal.WithLabelValues(fg.Name, kind, "unstable").Inc()
		return DeploymentLeaseResult{}, err
	}
	if err := m.validateWrite(ctx, fg, kind, operationID, true); err != nil {
		return DeploymentLeaseResult{}, err
	}
	metrics.DeployLeaseGrantsTotal.WithLabelValues(fg.Name, kind, "granted").Inc()
	if kind == "migration" {
		metrics.DeployLeasesActive.WithLabelValues(fg.Name, kind).Set(1)
	}
	return DeploymentLeaseResult{DeploymentLeaseView: r.DeploymentLeaseView, Token: token, Status: status}, nil
}

func (m *DeploymentLeaseManager) Renew(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind, operationID, instance, namespace, serviceAccount, token string, ttl int) (DeploymentLeaseResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mutate(ctx, fg, kind, operationID, instance, namespace, serviceAccount, token, ttl, false)
}

func (m *DeploymentLeaseManager) Release(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind, operationID, instance, namespace, serviceAccount, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.mutate(ctx, fg, kind, operationID, instance, namespace, serviceAccount, token, 0, true)
	return err
}

func (m *DeploymentLeaseManager) mutate(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind, operationID, instance, namespace, serviceAccount, token string, ttl int, release bool) (DeploymentLeaseResult, error) {
	if !validDeploymentKind(kind) {
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 400, Code: "invalid_request"}
	}
	fg, err := m.fresh(ctx, fg)
	if err != nil {
		return DeploymentLeaseResult{}, err
	}
	l, r, err := m.current(ctx, fg, kind, operationID)
	if err != nil {
		return DeploymentLeaseResult{}, err
	}
	if r == nil || r.OperationID != operationID {
		l, r, err = m.read(ctx, fg, kind, operationID)
		if err != nil {
			return DeploymentLeaseResult{}, err
		}
	}
	if r == nil {
		if release {
			return DeploymentLeaseResult{Status: 204}, nil
		}
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 404, Code: "not_found"}
	}
	if !sameDeploymentHolder(r, operationID, instance, namespace, serviceAccount) || subtle.ConstantTimeCompare([]byte(r.TokenHash), []byte(leaseTokenHash(token))) != 1 {
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 403, Code: "token_mismatch"}
	}
	if r.State == "revoked" {
		if release {
			return DeploymentLeaseResult{Status: 204}, nil
		}
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 409, Code: "revoked", Reason: r.Reason, TopologyGeneration: fg.Status.TopologyGeneration}
	}
	if r.State != "active" {
		if release {
			return DeploymentLeaseResult{Status: 204}, nil
		}
		return DeploymentLeaseResult{}, &DeploymentLeaseError{Status: 404, Code: "not_found"}
	}
	if release {
		r.State = "released"
	} else {
		if err := m.gate(fg); err != nil {
			return DeploymentLeaseResult{}, err
		}
		r.ExpiresAt = m.now().Add(time.Duration(max(5, min(120, ttl))) * time.Second)
	}
	if err := m.write(ctx, fg, l, r, ""); err != nil {
		return DeploymentLeaseResult{}, err
	}
	if release {
		if kind == "failover-hold" {
			if _, err := m.currentHolds(ctx, fg); err != nil {
				return DeploymentLeaseResult{}, err
			}
		} else {
			metrics.DeployLeasesActive.WithLabelValues(fg.Name, kind).Set(0)
		}
		return DeploymentLeaseResult{Status: 204}, nil
	}
	if err := m.gate(fg); err != nil {
		return DeploymentLeaseResult{}, err
	}
	if err := m.validateWrite(ctx, fg, kind, operationID, false); err != nil {
		return DeploymentLeaseResult{}, err
	}
	return DeploymentLeaseResult{DeploymentLeaseView: r.DeploymentLeaseView, Status: 200}, nil
}

// Reconciler-owned status writes do not take the admission mutex.
func (m *DeploymentLeaseManager) validateWrite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind, operation string, grant bool) error {
	fresh, err := m.fresh(ctx, fg)
	if err != nil {
		return err
	}
	if _, pending := fresh.Annotations[v1alpha1.RevokeDeploymentLeasesAnnotation]; pending {
		if _, _, err := m.current(ctx, fresh, kind, operation); err != nil {
			return err
		}
		return &DeploymentLeaseError{Status: 409, Code: "revoked", Reason: v1alpha1.DeploymentLeaseRevokedByOperator, TopologyGeneration: fresh.Status.TopologyGeneration}
	}
	if fresh.Status.TopologyGeneration != fg.Status.TopologyGeneration {
		if _, _, err := m.current(ctx, fresh, kind, operation); err != nil {
			return err
		}
		return &DeploymentLeaseError{Status: 409, Code: "revoked", Reason: v1alpha1.DeploymentLeaseRevokedTopologyChanged, TopologyGeneration: fresh.Status.TopologyGeneration}
	}
	if grant {
		if reason := m.UnstableReason(fresh); reason != "" {
			return &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: reason, RetryAfterSeconds: 1}
		}
	}
	return m.gate(fresh)
}

func (m *DeploymentLeaseManager) List(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) ([]DeploymentLeaseView, error) {
	_, views, err := m.Snapshot(ctx, fg)
	return views, err
}

// Snapshot returns a freshly read group and its leases from one observation.
// HTTP callers must use this group, not combine List with a separate group read.
// An in-memory operation reservation returns 423 until status can describe it.
func (m *DeploymentLeaseManager) Snapshot(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (*v1alpha1.MysqlFailoverGroup, []DeploymentLeaseView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for attempt := 0; attempt < 3; attempt++ {
		fresh, err := m.fresh(ctx, fg)
		if err != nil {
			return nil, nil, err
		}
		if err := m.topologyGate(fresh); err != nil {
			return nil, nil, err
		}
		if m.UnstableReason(fresh) == "" {
			if err := m.gate(fresh); err != nil {
				return nil, nil, err
			}
		}
		views, err := m.currentHolds(ctx, fresh)
		if err != nil {
			return nil, nil, err
		}
		_, r, err := m.current(ctx, fresh, "migration")
		if err != nil {
			return nil, nil, err
		}
		if r != nil && r.State == "active" {
			views = append([]DeploymentLeaseView{r.DeploymentLeaseView}, views...)
		}
		last, err := m.fresh(ctx, fresh)
		if err != nil {
			return nil, nil, err
		}
		if err := m.topologyGate(last); err != nil {
			return nil, nil, err
		}
		if last.ResourceVersion == fresh.ResourceVersion {
			return fresh, views, nil
		}
	}
	return nil, nil, &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: v1alpha1.UnstableReasonTopologyObservationChanged, RetryAfterSeconds: 1}
}

func (m *DeploymentLeaseManager) Hold(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (*DeploymentLeaseView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fg, err := m.fresh(ctx, fg)
	if err != nil {
		return nil, err
	}
	return m.hold(ctx, fg)
}

func (m *DeploymentLeaseManager) hold(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (*DeploymentLeaseView, error) {
	holds, err := m.currentHolds(ctx, fg)
	if err != nil {
		return nil, err
	}
	if len(holds) == 0 {
		return nil, nil
	}
	return &holds[0], nil
}

// BeginPlanned atomically checks holds and reserves admission. The caller must
// persist its operation phase before EndPlanned, and never retain the mutex.
func (m *DeploymentLeaseManager) BeginPlanned(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, operation string) (*DeploymentLeaseView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fg, err := m.fresh(ctx, fg)
	if err != nil {
		return nil, err
	}
	if _, pending := fg.Annotations[v1alpha1.RevokeDeploymentLeasesAnnotation]; pending {
		return nil, m.gate(fg)
	}
	hold, err := m.hold(ctx, fg)
	if err != nil || hold != nil {
		return hold, err
	}
	// Another operation is refused while a planned-failover run owns the
	// sites, holds a (possibly provisional) claim, or a hold stands: the
	// durable hold counts on its own, so a hold is refused before the
	// claim slot has been hydrated from it (after a restart).
	if (operation != deploymentOperationPlannedFailover && (plannedFailoverOwnsSites(fg) || m.claimHeld(fg) || PlannedHoldFrom(fg) != nil)) ||
		(operation != deploymentOperationRestoreInPlace && inPlaceRestoreInFlight(fg)) ||
		(operation != deploymentOperationOrderedUpdate && fg.Status.UpdatePhase != "") {
		return nil, &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: v1alpha1.UnstableReasonConcurrentOperation, RetryAfterSeconds: 1}
	}
	nn := client.ObjectKeyFromObject(fg)
	if _, pending := m.pending.Load(nn); pending {
		return nil, &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: v1alpha1.UnstableReasonTopologyPersistencePending, RetryAfterSeconds: 1}
	}
	if m.planned[nn] != "" {
		return nil, &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: m.planned[nn]}
	}
	m.planned[nn] = operation
	return nil, nil
}

func (m *DeploymentLeaseManager) EndPlanned(fg *v1alpha1.MysqlFailoverGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.planned, client.ObjectKeyFromObject(fg))
}

// TopologyPending closes admission without consulting leases or taking locks.
func (m *DeploymentLeaseManager) TopologyPending(nn types.NamespacedName) uint64 {
	epoch := m.epoch.Add(1)
	for {
		old, loaded := m.pending.LoadOrStore(nn, epoch)
		if !loaded || old.(uint64) >= epoch || m.pending.CompareAndSwap(nn, old, epoch) {
			break
		}
	}
	return epoch
}

// TopologyPersisted revokes old generations before reopening admission.
func (m *DeploymentLeaseManager) TopologyPersisted(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, epoch uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fg, err := m.fresh(ctx, fg)
	if err != nil {
		return err
	}
	if err := m.pruneLeaseRefusals(ctx, fg); err != nil {
		return err
	}
	if _, _, err := m.current(ctx, fg, "migration"); err != nil {
		return err
	}
	if _, err := m.currentHolds(ctx, fg); err != nil {
		return err
	}
	if m.pending.CompareAndDelete(client.ObjectKeyFromObject(fg), epoch) {
		m.observed.Store(client.ObjectKeyFromObject(fg), fg.UID)
	}
	return nil
}

func (m *DeploymentLeaseManager) Revoke(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, reason string) error {
	if reason != v1alpha1.DeploymentLeaseRevokedTopologyChanged && reason != v1alpha1.DeploymentLeaseRevokedByOperator {
		return fmt.Errorf("invalid revocation reason")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fg, err := m.fresh(ctx, fg)
	if err != nil {
		return err
	}
	return m.revoke(ctx, fg, reason)
}

// reconcileRevocation runs before operation-specific reconcile early returns.
// Admission stays closed until every revocation and annotation removal persist.
func (m *DeploymentLeaseManager) reconcileRevocation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fresh, err := m.fresh(ctx, fg)
	if err != nil {
		return err
	}
	request, pending := fresh.Annotations[v1alpha1.RevokeDeploymentLeasesAnnotation]
	if !pending {
		// The informer can still carry a completed request, including after
		// an annotation removal committed but its response was lost.
		*fg = *fresh
		return nil
	}
	if err := m.revoke(ctx, fresh, v1alpha1.DeploymentLeaseRevokedByOperator); err != nil {
		return err
	}
	fresh, err = m.fresh(ctx, fresh)
	if err != nil {
		return err
	}
	current, pending := fresh.Annotations[v1alpha1.RevokeDeploymentLeasesAnnotation]
	if pending {
		if current != request {
			return fmt.Errorf("deployment lease revocation request changed")
		}
		patch := client.MergeFromWithOptions(fresh.DeepCopy(), client.MergeFromWithOptimisticLock{})
		delete(fresh.Annotations, v1alpha1.RevokeDeploymentLeasesAnnotation)
		if err := m.client.Patch(ctx, fresh, patch); err != nil {
			return err
		}
	}
	*fg = *fresh
	return nil
}

// Caller holds m.mu and supplies an uncached group observation.
func (m *DeploymentLeaseManager) revoke(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, reason string) error {
	holds, err := m.currentHolds(ctx, fg)
	if err != nil {
		return err
	}
	for _, view := range append(holds, DeploymentLeaseView{Kind: "migration"}) {
		kind := view.Kind
		l, r, err := m.current(ctx, fg, kind, view.OperationID)
		if err != nil {
			return err
		}
		if r == nil || r.State != "active" {
			continue
		}
		r.State, r.Reason = "revoked", reason
		if err := m.write(ctx, fg, l, r, ""); err != nil {
			return err
		}
		m.revocation(fg, r)
		metrics.DeployLeasesActive.WithLabelValues(fg.Name, kind).Set(0)
	}
	return nil
}

type deploymentTopologyConfirmation struct {
	fg    *v1alpha1.MysqlFailoverGroup
	epoch uint64
}

func (m *DeploymentLeaseManager) confirmTopology(fg *v1alpha1.MysqlFailoverGroup, epoch uint64) {
	nn := client.ObjectKeyFromObject(fg)
	next := &deploymentTopologyConfirmation{fg: fg.DeepCopy(), epoch: epoch}
	for {
		old, loaded := m.confirmed.LoadOrStore(nn, next)
		if !loaded {
			break
		}
		previous := old.(*deploymentTopologyConfirmation)
		if previous.epoch > epoch {
			break
		}
		if m.confirmed.CompareAndSwap(nn, old, next) {
			break
		}
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// Lease housekeeping is isolated from the topology polling loop: a stalled
// Lease API request must not delay the next emergency failover decision.
func (m *DeploymentLeaseManager) run(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-m.wake:
		}
		m.confirmed.Range(func(key, value any) bool {
			confirmation := value.(*deploymentTopologyConfirmation)
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := m.TopologyPersisted(probeCtx, confirmation.fg, confirmation.epoch)
			cancel()
			if apierrors.IsNotFound(err) {
				m.confirmed.CompareAndDelete(key, value)
				m.observed.Delete(key)
				for _, kind := range []string{"migration", "failover-hold"} {
					metrics.DeployLeasesActive.DeleteLabelValues(confirmation.fg.Name, kind)
					metrics.DeployLeaseAgeSeconds.DeleteLabelValues(confirmation.fg.Name, kind)
				}
				return ctx.Err() == nil
			}
			if err != nil && ctx.Err() == nil {
				logger.Warn("deployment lease housekeeping failed", "group", confirmation.fg.Name, "error", err)
			}
			return ctx.Err() == nil
		})
	}
}
