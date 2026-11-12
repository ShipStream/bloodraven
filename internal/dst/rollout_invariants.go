package dst

import (
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// Invariants of the paced rollout (README "Paced rollouts"). They are
// judged where the fact is made: on every committed write of a rollout
// record or a Deployment (the interceptor calls observeRecord and
// checkTemplateWrite with the stored object before and after), on every
// ApplyUpdate of an ordered update, after every step, and at the end.

// reasonAdmittedByAnnotation is status.rollout.reason of an admission made
// by the rollout-admit annotation.
const reasonAdmittedByAnnotation = "AdmittedByAnnotation"

// admits reports whether rec admits its group to roll the running
// renderer's templates.
func (s *rolloutSim) admits(rec *v1alpha1.RolloutStatus) bool {
	return rec.InFlight() && rec.Renderer == s.identity
}

// aged reports whether a spec hash or a renderer name is one a release
// left behind (release).
func aged(v string) bool { return strings.HasPrefix(v, "release-") }

// observeRecord judges one committed change of a group's status.rollout.
// Callers hold s.mu.
func (s *rolloutSim) observeRecord(g *rolloutGroupSim, was, now *v1alpha1.RolloutStatus) {
	if equality.Semantic.DeepEqual(was, now) {
		return
	}
	name := g.spec.Name
	note := func(kind string) {
		s.res.Transitions[kind]++
		s.logf("record %s: %s -> %s", name, recordString(was), recordString(now))
	}
	switch {
	case !was.InFlight() && now.InFlight():
		// An admission.
		byAnnotation := now.Reason == reasonAdmittedByAnnotation
		if byAnnotation {
			note("admitByAnnotation")
			if !g.admitAnno {
				s.violate("AdmittedByAnnotationWithoutIt", fmt.Sprintf("%s was admitted with reason %s but carries no %s annotation",
					name, now.Reason, v1alpha1.RolloutAdmitAnnotation))
			}
		} else {
			note("admit")
			// Invariants 1 and 3: the slot is free, or its holder is
			// explicitly released.
			for _, o := range s.groups {
				if o == g {
					continue
				}
				if rec := o.rec; rec.InFlight() && !o.releaseAnno {
					s.violate("SecondAdmission", fmt.Sprintf("%s was admitted while %s holds the slot (%s)", name, o.spec.Name, recordString(rec)))
				}
			}
			if soak := time.Duration(s.plan.SoakSteps) * rolloutStep; soak > 0 {
				for _, o := range s.groups {
					rec := o.rec
					if o != g && rec != nil && rec.CompletedTime != nil && rec.StartedTime != nil && s.clk.Now().Before(rec.CompletedTime.Add(soak)) {
						s.violate("AdmittedDuringSoak", fmt.Sprintf("%s was admitted at %s, within %s of the roll %s completed at %s",
							name, s.clk.Now().UTC().Format(time.TimeOnly), soak, o.spec.Name, rec.CompletedTime.UTC().Format(time.TimeOnly)))
					}
				}
			}
		}
		if now.Renderer != s.identity {
			s.violate("AdmittedForAnotherRenderer", fmt.Sprintf("%s: %s", name, recordString(now)))
		}
		// Invariant 4: no group is rolled twice for one release.
		if g.completedEpoch == s.epoch {
			s.violate("AdmittedTwice", fmt.Sprintf("%s completed its roll of release %d and was admitted again", name, s.epoch))
		}
		if now.StartedTime != nil || now.StalledTime != nil {
			s.violate("AdmissionBornStarted", fmt.Sprintf("%s: %s", name, recordString(now)))
		}
		g.manual = byAnnotation || g.releaseAnno
	case was.InFlight() && now.InFlight():
		if !was.AdmittedTime.Equal(now.AdmittedTime) {
			s.violate("AdmissionRewritten", fmt.Sprintf("%s: %s -> %s", name, recordString(was), recordString(now)))
		}
		switch {
		case was.StartedTime != nil && (now.StartedTime == nil || !was.StartedTime.Equal(now.StartedTime)):
			// Invariant 3: the latch holds until the roll completes.
			s.violate("StartLatchLost", fmt.Sprintf("%s: %s -> %s", name, recordString(was), recordString(now)))
		case was.StartedTime == nil && now.StartedTime != nil:
			note("start")
			if now.Renderer != s.identity {
				s.violate("StartedForAnotherRenderer", fmt.Sprintf("%s: %s", name, recordString(now)))
			}
		}
		switch {
		case was.StalledTime != nil && (now.StalledTime == nil || !was.StalledTime.Equal(now.StalledTime)):
			// Invariant 3: a recorded stall is never cleared before completion.
			s.violate("StallCleared", fmt.Sprintf("%s: %s -> %s", name, recordString(was), recordString(now)))
		case was.StalledTime == nil && now.StalledTime != nil:
			note("stall:" + now.Reason)
			if now.StartedTime == nil {
				s.violate("StalledWithoutStart", fmt.Sprintf("%s: %s", name, recordString(now)))
			}
		}
		if was.Renderer != now.Renderer {
			note("retarget")
			if now.Renderer != s.identity {
				s.violate("RetargetedAway", fmt.Sprintf("%s: %s -> %s", name, recordString(was), recordString(now)))
			}
			if was.StartedTime != nil {
				s.seen["retargetStarted"] = struct{}{}
			}
		}
	case was.InFlight() && !now.InFlight():
		if now != nil && now.CompletedTime != nil {
			note("complete")
			if was.StartedTime == nil || now.StartedTime == nil {
				s.violate("CompletedWithoutStart", fmt.Sprintf("%s: %s -> %s", name, recordString(was), recordString(now)))
			}
			// Invariant 4: a completed group runs the current renderer's
			// template in every workload, and every pod of it is Ready.
			if why := s.notRolled(g); why != "" {
				s.violate("CompletedNotRolled", fmt.Sprintf("%s was recorded Complete but %s", name, why))
			}
			g.completedEpoch = s.epoch
			g.licensedStep = s.step
		} else {
			// Invariant 3: only an admission that never started is withdrawn.
			if was.StartedTime != nil {
				s.violate("StartedAdmissionWithdrawn", fmt.Sprintf("%s: %s -> %s", name, recordString(was), recordString(now)))
			}
			if now == nil {
				note("withdraw")
			} else {
				note("defer")
			}
		}
		g.manual = false
	default:
		// Neither holds an admission: the queue's bookkeeping.
		if was != nil && was.CompletedTime != nil && (now == nil || now.CompletedTime == nil) {
			s.res.Transitions["requeue"]++
		}
	}
}

func recordPhase(r *v1alpha1.RolloutStatus) string {
	if r == nil {
		return "-"
	}
	return string(r.Phase)
}

func recordString(r *v1alpha1.RolloutStatus) string {
	if r == nil {
		return "<none>"
	}
	out := fmt.Sprintf("%s/%s", r.Phase, r.Reason)
	if aged(r.Renderer) {
		out += " renderer=" + r.Renderer
	}
	for _, f := range []struct {
		label string
		at    *metav1.Time
	}{{"admitted", r.AdmittedTime}, {"started", r.StartedTime}, {"stalled", r.StalledTime}, {"completed", r.CompletedTime}, {"deferred", r.DeferredTime}} {
		if f.at != nil {
			out += " " + f.label + "=" + f.at.UTC().Format(time.TimeOnly)
		}
	}
	return out
}

// notRolled says why g is not rolled to the running renderer, or "".
func (s *rolloutSim) notRolled(g *rolloutGroupSim) string {
	for _, site := range g.spec.Sites {
		d, err := s.deployment(g, site)
		if err != nil {
			return fmt.Sprintf("the Deployment of %s cannot be read: %v", site, err)
		}
		if why := s.deploymentNotRolled(d); why != "" {
			return fmt.Sprintf("%s %s", site, why)
		}
		if pod := g.pods[site]; pod == nil || !pod.ready {
			return fmt.Sprintf("the pod of %s is not Ready", site)
		}
	}
	return ""
}

func (s *rolloutSim) deploymentNotRolled(d *appsv1.Deployment) string {
	switch {
	case d.Annotations[v1alpha1.RenderedByAnnotation] != s.identity:
		return fmt.Sprintf("was rendered by %q", d.Annotations[v1alpha1.RenderedByAnnotation])
	case aged(d.Annotations[v1alpha1.SpecHashAnnotation]):
		return fmt.Sprintf("carries the running renderer's name over the spec hash %q of an earlier release", d.Annotations[v1alpha1.SpecHashAnnotation])
	case aged(d.Spec.Template.Annotations[v1alpha1.SpecHashAnnotation]):
		return fmt.Sprintf("has a pod template with the spec hash %q of an earlier release", d.Spec.Template.Annotations[v1alpha1.SpecHashAnnotation])
	}
	return ""
}

// checkTemplateWrite judges a committed change of a site Deployment's pod
// template (invariant 2). Callers hold s.mu.
func (s *rolloutSim) checkTemplateWrite(g *rolloutGroupSim, site string, old, now *appsv1.Deployment) {
	name := g.spec.Name + "/" + site
	rec := g.rec
	if old.Annotations[v1alpha1.RenderedByAnnotation] != s.identity {
		// A release-driven change: the group is admitted for the running
		// renderer and the start of its roll was latched before this write.
		switch {
		case !s.admits(rec):
			s.violate("TemplateWrittenOutsideSlot", fmt.Sprintf("the pod template of %s (rendered by %q) was changed while the group's record is %s",
				name, old.Annotations[v1alpha1.RenderedByAnnotation], recordString(rec)))
		case rec.StartedTime == nil:
			s.violate("TemplateWrittenBeforeLatch", fmt.Sprintf("the pod template of %s was changed before status.rollout.startedTime was set (%s)",
				name, recordString(rec)))
		}
	} else {
		// Nothing in a trial changes a group's spec, so the running
		// renderer never has a reason to rewrite its own template.
		s.violate("TemplateRewritten", fmt.Sprintf("the pod template of %s, which the running renderer wrote, was changed again", name))
	}
	if why := s.deploymentNotRolled(now); why != "" {
		s.violate("TemplateWriteNotCurrent", fmt.Sprintf("%s was written and %s", name, why))
	}
	g.writes[site]++
	if g.writes[site] > 1 {
		// Invariant 4: no workload is rolled twice for one release.
		s.violate("RolledTwice", fmt.Sprintf("the pod template of %s was changed %d times for release %d", name, g.writes[site], s.epoch))
	}
}

// checkRestamp judges a committed write of a site Deployment that left its
// pod template alone: the renderer annotation may move to the running
// renderer only on a workload whose template it would not change, and no
// trial has such a workload after a release.
func (s *rolloutSim) checkRestamp(g *rolloutGroupSim, site string, old, now *appsv1.Deployment) {
	if old.Annotations[v1alpha1.RenderedByAnnotation] != s.identity && now.Annotations[v1alpha1.RenderedByAnnotation] == s.identity {
		s.violate("RestampedPendingWorkload", fmt.Sprintf("%s/%s (rendered by %q, spec hash %q) was stamped with the running renderer without a template write",
			g.spec.Name, site, old.Annotations[v1alpha1.RenderedByAnnotation], old.Annotations[v1alpha1.SpecHashAnnotation]))
	}
}

// noteApply judges the ordered updater's ApplyUpdate of a site (invariant
// 2: the updater of a group that is not admitted never gets this far, the
// drift check holds its sites).
func (s *rolloutSim) noteApply(g *rolloutGroupSim, site string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return
	}
	rec := g.rec
	s.logf("ordered update of %s applies %s (%s)", g.spec.Name, site, recordString(rec))
	if !s.admits(rec) {
		s.violate("UpdaterRanOutsideSlot", fmt.Sprintf("the ordered update of %s applied %s while the group's record is %s",
			g.spec.Name, site, recordString(rec)))
	}
}

// checkHeldDrift judges the drift check's result (invariant 2): a group
// that is not admitted has no drift, so no ordered update starts for it.
func (s *rolloutSim) checkHeldDrift(g *rolloutGroupSim, fg *v1alpha1.MysqlFailoverGroup) {
	if drift := g.tm.SpecDriftSites(); len(drift) > 0 && !s.admits(fg.Status.Rollout) {
		s.violate("DriftOutsideSlot", fmt.Sprintf("the drift check of %s reported %v while the group's record is %s",
			g.spec.Name, drift, recordString(fg.Status.Rollout)))
	}
}

// checkStep runs after every step.
func (s *rolloutSim) checkStep(p int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Invariant 1: at most one group holds an unfinished admission that
	// the administrator's annotations did not make or release.
	var holders []string
	for _, g := range s.groups {
		rec := g.rec
		if rec.Started() && s.admits(rec) {
			g.licensedStep = p
		}
		if rec.InFlight() && !g.manual {
			holders = append(holders, g.spec.Name+" ("+recordString(rec)+")")
		}
	}
	if len(holders) > 1 {
		s.violate("TwoSlotHolders", strings.Join(holders, "; "))
	}
	if s.capture {
		// The fleet as the step left it, when it changed.
		var b strings.Builder
		for _, g := range s.groups {
			fg := s.group(g)
			var writable []string
			for _, t := range g.cluster.Truth() {
				if !t.Crashed && !t.ReadOnly && !t.SuperReadOnly {
					writable = append(writable, t.Name)
				}
			}
			fmt.Fprintf(&b, " %s[%s active=%q writable=%v unstable=%q]", g.spec.Name, recordPhase(fg.Status.Rollout), fg.Status.ActiveSite,
				writable, controller.DeploymentUnstableReason(fg))
		}
		if state := b.String(); state != s.lastState {
			s.lastState = state
			s.logf("fleet:%s", state)
		}
	}
	// Invariant 2: the ordered updater changes MySQL only for a group
	// whose roll has started.
	for _, g := range s.groups {
		for _, e := range g.cluster.PollEvents() {
			if e.Actor != ActorUpdate || e.Outcome == "failed" || e.Outcome == "unreachable" {
				continue
			}
			switch e.Kind {
			case EvSetSuperRO, EvSetRO, EvPromote, EvStopReplica, EvResetReplica, EvChangeSource, EvStartReplica, EvKillConns:
			default:
				continue
			}
			if e.Kind == EvSetRO && strings.Contains(e.Detail, "false") {
				s.res.Handoffs++
			}
			if g.licensedStep != p {
				s.violate("UpdaterTouchedMySQLOutsideSlot", fmt.Sprintf("the ordered update of %s issued %s %s on %s while the group's record is %s",
					g.spec.Name, e.Kind, e.Detail, e.Site, recordString(g.rec)))
			}
		}
	}
}

// needsHuman reports whether g ended in the documented needs-human state of
// a group with no primary (README "Paced rollouts", Inherent): every site
// answers read-only without a fence, and neither the group's status nor
// its failover record names a site the operator could make writable again.
// A Recreate roll restarts the primary, which boots read_only=ON, and the
// status then names no active site until the operator has re-asserted it;
// an operator that restarts in between has no authority left on record.
// The failover trials excuse the same state (checkWedge: no history).
func (s *rolloutSim) needsHuman(g *rolloutGroupSim, fg *v1alpha1.MysqlFailoverGroup) bool {
	for _, t := range g.cluster.Truth() {
		if t.Crashed || !t.ReadOnly || t.SuperReadOnly {
			return false
		}
	}
	rec, _, err := controller.EffectiveFailoverRecord(fg, s.clk.Now())
	return err == nil && rec.LastFailoverTarget == "" && fg.Status.ActiveSite == ""
}

// checkEnd judges the end state (invariants 4 and 5): the faults healed
// with a settle window to spare, so every group has one primary and has
// completed its roll. A group in the needs-human state is excused, and so
// are the groups that wait behind it while it holds the slot.
func (s *rolloutSim) checkEnd() []Violation {
	var out []Violation
	last := s.step
	add := func(invariant, detail string) {
		out = append(out, Violation{Invariant: invariant, Poll: last, Detail: detail})
	}
	fgs := map[*rolloutGroupSim]*v1alpha1.MysqlFailoverGroup{}
	halted := false
	for _, g := range s.groups {
		fg := s.group(g)
		fgs[g] = fg
		if s.needsHuman(g, fg) {
			s.res.NeedsHuman = append(s.res.NeedsHuman, g.spec.Name)
			s.seen["needsHuman"] = struct{}{}
			if fg.Status.Rollout.InFlight() {
				halted = true
				s.seen["haltedBehindNeedsHuman"] = struct{}{}
			}
		}
	}
	for _, g := range s.groups {
		fg := fgs[g]
		rec := fg.Status.Rollout
		if s.needsHuman(g, fg) {
			continue
		}
		if writable := g.writableSites(); writable != 1 {
			add("GroupWithoutOnePrimary", fmt.Sprintf("%s ended with %d writable sites: %+v", g.spec.Name, writable, g.cluster.Truth()))
		}
		if !g.everPending {
			continue
		}
		done := rec != nil && rec.CompletedTime != nil && rec.Renderer == s.identity
		if halted && !done {
			continue
		}
		if why := s.notRolled(g); why != "" {
			add("RolloutNotConverged", fmt.Sprintf("%s: %s; record %s; unstable=%q", g.spec.Name, why, recordString(rec), controller.DeploymentUnstableReason(fg)))
			continue
		}
		if !done {
			add("RolloutNotConverged", fmt.Sprintf("%s runs the release in every pod but its record is %s; unstable=%q",
				g.spec.Name, recordString(rec), controller.DeploymentUnstableReason(fg)))
			continue
		}
		s.res.Completed++
	}
	return out
}
