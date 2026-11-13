package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// status.rollout.reason values.
const (
	rolloutReasonWaitingForSlot       = string(v1alpha1.RolloutReasonWaitingForSlot)
	rolloutReasonHalted               = string(v1alpha1.RolloutReasonHalted)
	rolloutReasonGroupNotStable       = string(v1alpha1.RolloutReasonGroupNotStable)
	rolloutReasonNotStarted           = string(v1alpha1.RolloutReasonNotStarted)
	rolloutReasonAdmitted             = string(v1alpha1.RolloutReasonAdmitted)
	rolloutReasonAdmittedByAnnotation = string(v1alpha1.RolloutReasonAdmittedByAnnotation)
	rolloutReasonStarted              = string(v1alpha1.RolloutReasonStarted)
	rolloutReasonStallTimeout         = string(v1alpha1.RolloutReasonStallTimeout)
	rolloutReasonUpdateStalled        = v1alpha1.ReasonOrderedUpdateStalled
)

// RolloutPacerConfig tunes the pacer. Zero values take the defaults.
type RolloutPacerConfig struct {
	// Interval is how often the pacer looks at the fleet.
	Interval time.Duration
	// Soak is how long the slot stays empty after a group completed its
	// roll, before the next group is admitted. Negative means none.
	Soak time.Duration
	// StallAfter is how long a started roll may take before it is recorded
	// as stalled. An ordered update that stops on a pod that is not Ready
	// is recorded at once.
	StallAfter time.Duration
	// StartGrace is how long an admitted group has to start its roll
	// before the admission is withdrawn and the next group is tried.
	StartGrace time.Duration
}

const (
	defaultRolloutPacerInterval = 5 * time.Second
	defaultRolloutSoak          = time.Minute
	defaultRolloutStallAfter    = 30 * time.Minute
	defaultRolloutStartGrace    = 5 * time.Minute
)

func (c RolloutPacerConfig) withDefaults() RolloutPacerConfig {
	if c.Interval <= 0 {
		c.Interval = defaultRolloutPacerInterval
	}
	// A negative soak means none; it stays negative so that defaulting
	// twice does not turn it into the default.
	if c.Soak == 0 {
		c.Soak = defaultRolloutSoak
	}
	if c.StallAfter <= 0 {
		c.StallAfter = defaultRolloutStallAfter
	}
	if c.StartGrace <= 0 {
		c.StartGrace = defaultRolloutStartGrace
	}
	return c
}

// rolloutGroupView is one group as the planner sees it.
type rolloutGroupView struct {
	Key    types.NamespacedName
	Record *v1alpha1.RolloutStatus
	// State is the group's workloads; StateErr is set instead when they
	// could not be read. A group that cannot be read is never admitted,
	// never completed and never deferred.
	State    rolloutGroupState
	StateErr string
	// Unstable is why the group is not stable ("" when it is): the
	// deployment API's unstableReason, including a planned-failover hold.
	Unstable string
	// UpdateStalled is the message of Updating=True/OrderedUpdateStalled.
	UpdateStalled string
	// Admit and ReleaseSlot are the administrator's annotations.
	Admit, ReleaseSlot bool
}

// rolloutAction is one status write the planner asks for.
type rolloutAction struct {
	Key types.NamespacedName
	// From is the record the decision was taken on; the write is skipped
	// when the stored record is another one by then.
	From *v1alpha1.RolloutStatus
	// To is the new record; nil clears it.
	To *v1alpha1.RolloutStatus
	// Event, when set, is emitted after the write.
	EventType, EventReason, EventMessage string
}

// rolloutFleet is the planner's summary of the fleet after its decisions.
type rolloutFleet struct {
	Pending, Admitted, Rolling, Stalled int
	// Phases is each group's phase after the decisions ("" for none).
	Phases map[types.NamespacedName]v1alpha1.RolloutPhase
}

func rolloutTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

// planRollout is the pacer's whole decision, as a pure function of the
// fleet: views in, status writes out. One group holds the slot at a time.
//
//   - An unfinished admission (for any renderer) holds the slot. After an
//     operator upgrade it is retargeted to the running renderer, so a
//     second upgrade mid-rollout continues with the same group, and a
//     stalled one stays stalled.
//   - An admission that has not started (no workload written) is withdrawn
//     when the group no longer needs it, or after StartGrace: something
//     else holds the group, and the fleet moves on. One that has started is
//     never withdrawn; it completes when every workload runs the release's
//     template, its rollout finished and the group is stable.
//   - With the slot free and the soak elapsed, the first stable group in
//     the queue is admitted: deferred groups last, then by namespace and
//     name.
func planRollout(now time.Time, identity string, cfg RolloutPacerConfig, views []rolloutGroupView) ([]rolloutAction, rolloutFleet) {
	cfg = cfg.withDefaults()
	fleet := rolloutFleet{Phases: make(map[types.NamespacedName]v1alpha1.RolloutPhase, len(views))}
	var actions []rolloutAction
	emit := func(v rolloutGroupView, to *v1alpha1.RolloutStatus, eventType, reason, msg string) {
		if equality.Semantic.DeepEqual(v.Record, to) {
			return
		}
		actions = append(actions, rolloutAction{Key: v.Key, From: v.Record, To: to, EventType: eventType, EventReason: reason, EventMessage: msg})
	}
	count := func(key types.NamespacedName, rec *v1alpha1.RolloutStatus) {
		if rec == nil {
			return
		}
		fleet.Phases[key] = rec.Phase
		switch rec.Phase {
		case v1alpha1.RolloutPhasePending:
			fleet.Pending++
		case v1alpha1.RolloutPhaseAdmitted:
			fleet.Admitted++
		case v1alpha1.RolloutPhaseRolling:
			fleet.Rolling++
		case v1alpha1.RolloutPhaseStalled:
			fleet.Stalled++
		}
	}

	sorted := append([]rolloutGroupView(nil), views...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Key.String() < sorted[j].Key.String() })

	var holder *rolloutGroupView
	holderStalled := false
	var lastCompleted time.Time
	var waiting []rolloutGroupView

	// Pass 1: the groups that hold an admission.
	for i := range sorted {
		v := sorted[i]
		if !v.Record.InFlight() {
			if r := v.Record; r != nil && r.CompletedTime != nil && r.StartedTime != nil && r.CompletedTime.Time.After(lastCompleted) {
				lastCompleted = r.CompletedTime.Time
			}
			waiting = append(waiting, v)
			continue
		}
		rec := v.Record.DeepCopy()
		rec.Renderer = identity
		eventType, eventReason, eventMsg := "", "", ""
		switch {
		case v.StateErr != "":
			// Unknown is not "done": the admission stands.
		case rec.StartedTime == nil && len(v.State.Pending) == 0:
			// Nothing to roll any more (the release was rolled back, or
			// the spec moved): the slot was not used.
			emit(v, nil, "", "", "")
			continue
		case rec.StartedTime == nil && !v.Admit && now.Sub(rec.AdmittedTime.Time) >= cfg.StartGrace:
			why := v.Unstable
			if why == "" {
				why = "no workload was changed"
			}
			deferred := &v1alpha1.RolloutStatus{
				Renderer:     identity,
				Phase:        v1alpha1.RolloutPhasePending,
				Reason:       rolloutReasonNotStarted,
				Message:      fmt.Sprintf("admitted at %s but the roll did not start within %s (%s); the group keeps its pods and waits for the slot again", rec.AdmittedTime.UTC().Format(time.RFC3339), cfg.StartGrace, why),
				DeferredTime: rolloutTime(now),
			}
			emit(v, deferred, corev1.EventTypeWarning, v1alpha1.EventRolloutDeferred, deferred.Message)
			count(v.Key, deferred)
			continue
		case rec.StartedTime != nil && len(v.State.Pending) == 0 && len(v.State.Incomplete) == 0 && v.Unstable == "":
			rec.CompletedTime = rolloutTime(now)
			rec.StalledTime = nil
			rec.Phase = v1alpha1.RolloutPhaseComplete
			rec.Reason, rec.Message = "", ""
			emit(v, rec, corev1.EventTypeNormal, v1alpha1.EventRolloutCompleted,
				fmt.Sprintf("every workload runs the pod template of renderer %s and the group is stable", identity))
			lastCompleted = now
			count(v.Key, rec)
			continue
		case rec.StartedTime != nil && rec.StalledTime == nil && (v.UpdateStalled != "" || now.Sub(rec.StartedTime.Time) >= cfg.StallAfter):
			rec.StalledTime = rolloutTime(now)
			rec.Phase = v1alpha1.RolloutPhaseStalled
			if v.UpdateStalled != "" {
				rec.Reason, rec.Message = rolloutReasonUpdateStalled, v.UpdateStalled
			} else {
				rec.Reason = rolloutReasonStallTimeout
				rec.Message = fmt.Sprintf("the roll started at %s and has not completed within %s: %s",
					rec.StartedTime.UTC().Format(time.RFC3339), cfg.StallAfter, rolloutOutstanding(v))
			}
			eventType, eventReason = corev1.EventTypeWarning, v1alpha1.EventRolloutStalled
			eventMsg = rec.Message + "; no other group is rolled until this one completes"
		}
		emit(v, rec, eventType, eventReason, eventMsg)
		count(v.Key, rec)
		if !v.ReleaseSlot && holder == nil {
			holder = &sorted[i]
			holderStalled = rec.StalledTime != nil
		}
	}

	// Pass 2: the groups that wait.
	type candidate struct {
		view     rolloutGroupView
		eligible bool
	}
	var candidates []candidate
	for _, v := range waiting {
		if v.StateErr == "" && len(v.State.Pending) == 0 {
			if v.Record != nil && v.Record.Phase == v1alpha1.RolloutPhasePending {
				emit(v, nil, "", "", "")
			} else {
				count(v.Key, v.Record)
			}
			continue
		}
		if v.StateErr != "" && v.Record == nil {
			// Nothing known and nothing recorded: not in the queue yet.
			continue
		}
		if v.Admit && v.StateErr == "" {
			rec := &v1alpha1.RolloutStatus{
				Renderer:     identity,
				Phase:        v1alpha1.RolloutPhaseAdmitted,
				Reason:       rolloutReasonAdmittedByAnnotation,
				Message:      fmt.Sprintf("admitted by the %s annotation, outside the queue", v1alpha1.RolloutAdmitAnnotation),
				AdmittedTime: rolloutTime(now),
			}
			emit(v, rec, corev1.EventTypeNormal, v1alpha1.EventRolloutAdmitted,
				fmt.Sprintf("admitted to roll the pod template of renderer %s by the %s annotation", identity, v1alpha1.RolloutAdmitAnnotation))
			count(v.Key, rec)
			if !v.ReleaseSlot && holder == nil {
				vv := v
				holder = &vv
			}
			continue
		}
		candidates = append(candidates, candidate{view: v, eligible: v.StateErr == "" && v.Unstable == ""})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i].view.Record, candidates[j].view.Record
		var at, bt time.Time
		if a != nil && a.DeferredTime != nil {
			at = a.DeferredTime.Time
		}
		if b != nil && b.DeferredTime != nil {
			bt = b.DeferredTime.Time
		}
		return at.Before(bt)
	})
	soakUntil := lastCompleted
	if cfg.Soak > 0 {
		soakUntil = lastCompleted.Add(cfg.Soak)
	}
	slotFree := holder == nil && !now.Before(soakUntil)
	for _, c := range candidates {
		v := c.view
		if slotFree && c.eligible {
			rec := &v1alpha1.RolloutStatus{
				Renderer:     identity,
				Phase:        v1alpha1.RolloutPhaseAdmitted,
				Reason:       rolloutReasonAdmitted,
				Message:      "the group holds the rollout slot",
				AdmittedTime: rolloutTime(now),
			}
			emit(v, rec, corev1.EventTypeNormal, v1alpha1.EventRolloutAdmitted,
				fmt.Sprintf("admitted to roll the pod template of renderer %s", identity))
			count(v.Key, rec)
			slotFree = false
			vv := v
			holder = &vv
			continue
		}
		rec := &v1alpha1.RolloutStatus{Renderer: identity, Phase: v1alpha1.RolloutPhasePending}
		if v.Record != nil {
			rec.DeferredTime = v.Record.DeferredTime
		}
		switch {
		case v.StateErr != "":
			rec.Reason = rolloutReasonGroupNotStable
			rec.Message = "the group's workloads could not be read: " + v.StateErr
		case v.Unstable != "":
			rec.Reason = rolloutReasonGroupNotStable
			rec.Message = fmt.Sprintf("the group is not stable (%s); it is admitted once it is, and does not hold up other groups", v.Unstable)
		case holder != nil && holderStalled:
			rec.Reason = rolloutReasonHalted
			rec.Message = fmt.Sprintf("the rollout is halted: the roll of %s stalled; this group keeps its pods", holder.Key)
		case holder != nil:
			rec.Reason = rolloutReasonWaitingForSlot
			rec.Message = fmt.Sprintf("%s holds the rollout slot", holder.Key)
		default:
			rec.Reason = rolloutReasonWaitingForSlot
			rec.Message = fmt.Sprintf("the slot is free again at %s (soak after the last completed roll)", soakUntil.UTC().Format(time.RFC3339))
		}
		// A deferral's own message stays until something else is to say.
		if r := v.Record; r != nil && r.Phase == v1alpha1.RolloutPhasePending && r.Reason == rolloutReasonNotStarted &&
			r.Renderer == identity && rec.Reason == rolloutReasonGroupNotStable {
			rec.Reason, rec.Message = r.Reason, r.Message
		}
		emit(v, rec, "", "", "")
		count(v.Key, rec)
	}
	return actions, fleet
}

// rolloutOutstanding says what a started roll still waits for.
func rolloutOutstanding(v rolloutGroupView) string {
	var parts []string
	if len(v.State.Pending) > 0 {
		parts = append(parts, "not rolled yet: "+strings.Join(v.State.Pending, ", "))
	}
	if len(v.State.Incomplete) > 0 {
		parts = append(parts, "rollout not finished: "+strings.Join(v.State.Incomplete, ", "))
	}
	if v.Unstable != "" {
		parts = append(parts, "group not stable: "+v.Unstable)
	}
	if v.StateErr != "" {
		parts = append(parts, "workloads unreadable: "+v.StateErr)
	}
	if len(parts) == 0 {
		return "nothing outstanding"
	}
	return strings.Join(parts, "; ")
}

// rolloutStateSource classifies a group's workloads for the pacer; the
// group reconciler implements it.
type rolloutStateSource interface {
	RolloutGroupState(ctx context.Context, reader client.Reader, fg *v1alpha1.MysqlFailoverGroup) (rolloutGroupState, error)
}

// RolloutPacer admits failover groups, one at a time, to roll a pod change
// that an operator release causes, and records each group's place in
// status.rollout. It runs on the leader only. Everything it knows is read
// from the groups and their workloads on every pass, so an operator restart
// (the upgrade that starts a rollout always is one) loses nothing.
type RolloutPacer struct {
	client   client.Client
	reader   client.Reader
	recorder record.EventRecorder
	logger   *slog.Logger
	states   rolloutStateSource
	// unstable is the group stability check (DeploymentLeaseManager's
	// UnstableReason, which also knows a held planned-failover claim).
	unstable func(*v1alpha1.MysqlFailoverGroup) string
	cfg      RolloutPacerConfig
	now      func() time.Time

	mu sync.Mutex
	// notStored remembers the groups whose record the CRD pruned, so the
	// Event is emitted once.
	notStored map[types.NamespacedName]bool
	// reported is the label sets last written to the per-group gauge.
	reported map[types.NamespacedName]v1alpha1.RolloutPhase
}

// NewRolloutPacer builds the pacer. reader must be uncached: admission is
// decided on it.
func NewRolloutPacer(c client.Client, reader client.Reader, recorder record.EventRecorder, logger *slog.Logger,
	states rolloutStateSource, unstable func(*v1alpha1.MysqlFailoverGroup) string, cfg RolloutPacerConfig) *RolloutPacer {
	if unstable == nil {
		unstable = DeploymentUnstableReason
	}
	return &RolloutPacer{
		client: c, reader: reader, recorder: recorder, logger: logger, states: states, unstable: unstable,
		cfg: cfg.withDefaults(), now: time.Now,
		notStored: make(map[types.NamespacedName]bool),
		reported:  make(map[types.NamespacedName]v1alpha1.RolloutPhase),
	}
}

// SetClock replaces the pacer's clock (tests).
func (p *RolloutPacer) SetClock(now func() time.Time) { p.now = now }

// NeedLeaderElection implements manager.LeaderElectionRunnable: two pacers
// would admit two groups.
func (p *RolloutPacer) NeedLeaderElection() bool { return true }

// Start implements manager.Runnable.
func (p *RolloutPacer) Start(ctx context.Context) error {
	p.logger.Info("paced rollout: pacer started", "renderer", rendererIdentity(),
		"soak", p.cfg.Soak.String(), "stallAfter", p.cfg.StallAfter.String(), "startGrace", p.cfg.StartGrace.String())
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	for {
		if err := p.Tick(ctx); err != nil && ctx.Err() == nil {
			p.logger.Warn("paced rollout: pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Tick runs one pass: read the fleet, decide, write.
func (p *RolloutPacer) Tick(ctx context.Context) error {
	var groups v1alpha1.MysqlFailoverGroupList
	if err := p.reader.List(ctx, &groups); err != nil {
		return fmt.Errorf("list MysqlFailoverGroups: %w", err)
	}
	views := make([]rolloutGroupView, 0, len(groups.Items))
	for i := range groups.Items {
		fg := &groups.Items[i]
		if !fg.DeletionTimestamp.IsZero() {
			continue
		}
		views = append(views, p.view(ctx, fg))
	}
	actions, fleet := planRollout(p.now(), rendererIdentity(), p.cfg, views)
	var firstErr error
	for _, a := range actions {
		if err := p.apply(ctx, a); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.report(fleet)
	return firstErr
}

// view reads one group for the planner. A group that holds an admission is
// read uncached, workloads included: its completion must not be decided on
// a cached Deployment status from before the template write.
func (p *RolloutPacer) view(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) rolloutGroupView {
	v := rolloutGroupView{
		Key:      types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name},
		Record:   fg.Status.Rollout.DeepCopy(),
		Unstable: p.unstable(fg),
	}
	// Only "true" arms an annotation: "false" must not admit a group.
	v.Admit = fg.Annotations[v1alpha1.RolloutAdmitAnnotation] == "true"
	v.ReleaseSlot = fg.Annotations[v1alpha1.RolloutReleaseSlotAnnotation] == "true"
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionUpdating); c != nil &&
		c.Status == metav1.ConditionTrue && c.Reason == v1alpha1.ReasonOrderedUpdateStalled {
		v.UpdateStalled = c.Message
	}
	reader := client.Reader(p.client)
	if v.Record.InFlight() {
		reader = p.reader
	}
	state, err := p.states.RolloutGroupState(ctx, reader, fg)
	if err != nil {
		v.StateErr = err.Error()
	}
	v.State = state
	return v
}

// apply writes one decision, conditional on the record it was taken on.
func (p *RolloutPacer) apply(ctx context.Context, a rolloutAction) error {
	var fresh v1alpha1.MysqlFailoverGroup
	if err := p.reader.Get(ctx, a.Key, &fresh); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !equality.Semantic.DeepEqual(fresh.Status.Rollout, a.From) {
		// The write path latched a start, or another pass wrote: decide
		// again on the next pass.
		return nil
	}
	if a.To != nil && a.To.InFlight() && !a.From.InFlight() {
		// An admission is decided on what the workloads are now.
		state, err := p.states.RolloutGroupState(ctx, p.reader, &fresh)
		if err != nil || len(state.Pending) == 0 {
			return nil
		}
	}
	fresh.Status.Rollout = a.To.DeepCopy()
	if err := p.client.Status().Update(ctx, &fresh); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("write status.rollout of %s: %w", a.Key, err)
	}
	if a.To != nil && fresh.Status.Rollout == nil {
		p.noteNotStored(&fresh)
		return nil
	}
	p.mu.Lock()
	delete(p.notStored, a.Key)
	p.mu.Unlock()
	if a.EventReason == "" {
		return nil
	}
	p.logger.Info("paced rollout: "+a.EventReason, "fg", a.Key, "renderer", rendererIdentity(), "detail", a.EventMessage)
	p.recordEvent(&fresh, a)
	switch a.EventReason {
	case v1alpha1.EventRolloutAdmitted:
		metrics.RolloutAdmissionsTotal.WithLabelValues(a.To.Reason).Inc()
	case v1alpha1.EventRolloutDeferred:
		metrics.RolloutDeferralsTotal.Inc()
	case v1alpha1.EventRolloutStalled:
		metrics.RolloutStallsTotal.WithLabelValues(a.To.Reason).Inc()
	case v1alpha1.EventRolloutCompleted:
		metrics.RolloutCompletionsTotal.Inc()
	}
	return nil
}

// recordEvent emits a decision's Event. Each reason is spelled with its one
// Event type here, so the Event reference (internal/apiregistry) can pair
// them; the planner sets the same type on the action.
func (p *RolloutPacer) recordEvent(fg *v1alpha1.MysqlFailoverGroup, a rolloutAction) {
	if p.recorder == nil {
		return
	}
	switch a.EventReason {
	case v1alpha1.EventRolloutAdmitted:
		p.recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventRolloutAdmitted, "%s", a.EventMessage)
	case v1alpha1.EventRolloutDeferred:
		p.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventRolloutDeferred, "%s", a.EventMessage)
	case v1alpha1.EventRolloutStalled:
		p.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventRolloutStalled, "%s", a.EventMessage)
	case v1alpha1.EventRolloutCompleted:
		p.recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventRolloutCompleted, "%s", a.EventMessage)
	}
}

// noteNotStored reports, once per group, that the CRD pruned the record.
func (p *RolloutPacer) noteNotStored(fg *v1alpha1.MysqlFailoverGroup) {
	key := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	p.mu.Lock()
	seen := p.notStored[key]
	p.notStored[key] = true
	p.mu.Unlock()
	if seen {
		return
	}
	msg := "status.rollout was not stored: the installed MysqlFailoverGroup CRD is older than the operator. " +
		"No group is rolled to this release's pod template until the release's CRDs are applied"
	p.logger.Error("paced rollout: record not stored", "fg", key, "remedy", "apply the CRDs of this release (helm does not upgrade the crds/ of a chart)")
	if p.recorder != nil {
		p.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventRolloutRecordNotStored, "%s", msg)
	}
}

// report publishes the fleet gauges and each group's phase.
func (p *RolloutPacer) report(fleet rolloutFleet) {
	metrics.RolloutGroups.WithLabelValues("pending").Set(float64(fleet.Pending))
	metrics.RolloutGroups.WithLabelValues("admitted").Set(float64(fleet.Admitted))
	metrics.RolloutGroups.WithLabelValues("rolling").Set(float64(fleet.Rolling))
	metrics.RolloutGroups.WithLabelValues("stalled").Set(float64(fleet.Stalled))
	p.mu.Lock()
	defer p.mu.Unlock()
	metrics.RolloutRecordNotStored.Set(float64(len(p.notStored)))
	for key, was := range p.reported {
		if now := fleet.Phases[key]; now != was {
			metrics.RolloutGroupPhase.DeleteLabelValues(key.Namespace, key.Name, strings.ToLower(string(was)))
			delete(p.reported, key)
		}
	}
	for key, phase := range fleet.Phases {
		if phase == "" || phase == v1alpha1.RolloutPhaseComplete {
			continue
		}
		metrics.RolloutGroupPhase.WithLabelValues(key.Namespace, key.Name, strings.ToLower(string(phase))).Set(1)
		p.reported[key] = phase
	}
}
