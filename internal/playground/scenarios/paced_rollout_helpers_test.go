package scenarios

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// Scenario 73 is in the release profile; scenario 74 needs a cluster
// installed at the previous release, so no profile selects it and the e2e
// upgrade job names it. Both clean up after themselves.
func TestPacedRolloutScenariosRegisteredAndProfiled(t *testing.T) {
	all := runner.DefaultRegistry.List()

	s, ok := runner.DefaultRegistry.Get(s73ID)
	if !ok {
		t.Fatalf("scenario %s is not registered", s73ID)
	}
	if s.Quarantine != "" || s.Timeout <= 0 || s.Cleanup == nil || s.Precheck == nil || s.CleanupTimeout <= runner.DefaultCleanupTimeout {
		t.Fatalf("scenario %s: quarantine=%q timeout=%s cleanup=%v precheck=%v cleanupTimeout=%s",
			s73ID, s.Quarantine, s.Timeout, s.Cleanup != nil, s.Precheck != nil, s.CleanupTimeout)
	}
	if !inventoryContains(runner.SelectForProfile(all, runner.ProfileRelease), s73ID) {
		t.Errorf("scenario %s must be in the release profile", s73ID)
	}
	if inventoryContains(runner.SelectForProfile(all, runner.ProfileSmoke), s73ID) {
		t.Errorf("scenario %s must not be in the smoke profile", s73ID)
	}

	s, ok = runner.DefaultRegistry.Get(s74ID)
	if !ok {
		t.Fatalf("scenario %s is not registered", s74ID)
	}
	if s.Quarantine == "" || s.Cleanup == nil || s.Precheck == nil {
		t.Fatalf("scenario %s: quarantine=%q cleanup=%v precheck=%v", s74ID, s.Quarantine, s.Cleanup != nil, s.Precheck != nil)
	}
	for _, p := range []runner.Profile{runner.ProfileSmoke, runner.ProfileRelease, runner.ProfileFull} {
		if inventoryContains(runner.SelectForProfile(all, p), s74ID) {
			t.Errorf("scenario %s must not be in the %s profile", s74ID, p)
		}
	}
}

// The auxiliary group manifest parses strictly into the API type, is small,
// carries the label cleanup finds it by, and sorts before the playground
// group in the pacer's queue.
func TestPacedAuxGroupManifest(t *testing.T) {
	seenIDs := map[int64]bool{}
	seenHosts := map[string]bool{}
	for i, name := range pacedAuxGroupNames {
		fg, err := pacedAuxGroupObject("bloodraven-playground", name, "zone-iad", i)
		if err != nil {
			t.Fatal(err)
		}
		if fg.Name != name || fg.Namespace != "bloodraven-playground" || fg.Labels[pacedAuxLabel] != "true" {
			t.Errorf("%s: metadata %+v", name, fg.ObjectMeta)
		}
		if !(name < "playground") {
			t.Errorf("%s must sort before the playground group: the pacer's queue is by name", name)
		}
		if len(fg.Spec.Sites) != 2 {
			t.Fatalf("%s: %d sites, want 2", name, len(fg.Spec.Sites))
		}
		if fg.Spec.Sidecar.Image != "" {
			t.Errorf("%s pins spec.sidecar.image; it must follow the operator's default", name)
		}
		if fg.Spec.Dragonfly != nil {
			t.Errorf("%s enables Dragonfly", name)
		}
		if seenHosts[fg.Spec.DNS.Hostname] {
			t.Errorf("%s: DNS hostname %s repeats", name, fg.Spec.DNS.Hostname)
		}
		seenHosts[fg.Spec.DNS.Hostname] = true
		for _, site := range fg.Spec.Sites {
			if site.Zone != "zone-iad" || !site.IsPromotable() || site.LoadBalancerIP == "" {
				t.Errorf("%s site %s: zone=%q promotable=%v lb=%q", name, site.Name, site.Zone, site.IsPromotable(), site.LoadBalancerIP)
			}
			// The selector the API requires names a label of the scenario's
			// own, which no node carries: an auxiliary group taints nothing.
			if len(site.TaintNodeSelector) != 1 || site.TaintNodeSelector[pacedAuxTaintLabel] != name+"-"+site.Name {
				t.Errorf("%s site %s: taintNodeSelector %v", name, site.Name, site.TaintNodeSelector)
			}
			if seenIDs[site.ServerID] {
				t.Errorf("%s site %s: serverID %d repeats", name, site.Name, site.ServerID)
			}
			seenIDs[site.ServerID] = true
			mem := site.Resources.Requests[corev1.ResourceMemory]
			if mem.Value() > 256<<20 {
				t.Errorf("%s site %s requests %s of memory; keep the auxiliary groups small", name, site.Name, mem.String())
			}
		}
	}
}

func TestS73ImageWithSuffix(t *testing.T) {
	for in, want := range map[string]string{
		"bloodraven-sidecar:playground":                          "bloodraven-sidecar:playground-next",
		"localhost/bloodraven-sidecar:playground":                "localhost/bloodraven-sidecar:playground-next",
		"registry.local:5000/bloodraven-sidecar":                 "registry.local:5000/bloodraven-sidecar:latest-next",
		"registry.local:5000/bloodraven-sidecar:v1":              "registry.local:5000/bloodraven-sidecar:v1-next",
		"ghcr.io/shipstream/bloodraven-sidecar:1.8.0@sha256:abc": "ghcr.io/shipstream/bloodraven-sidecar:1.8.0-next",
	} {
		if got := s73ImageWithSuffix(in, pacedNextTagSuffix); got != want {
			t.Errorf("s73ImageWithSuffix(%q) = %q, want %q", in, got, want)
		}
	}
}

// The values from before the scenario are recorded once and restored
// exactly, including a variable that was not set.
func TestPacedOperatorEnvRecordAndRestore(t *testing.T) {
	c := &corev1.Container{Name: pacedOperatorContainer, Env: []corev1.EnvVar{
		{Name: pacedEnvSidecarImage, Value: "bloodraven-sidecar:playground"},
		{Name: pacedEnvSoak, Value: "1m"},
		{Name: "UNRELATED", Value: "kept"},
	}}
	str := func(s string) *string { return &s }

	stored, err := pacedRecordOriginalEnv("", c, []string{pacedEnvSidecarImage, pacedEnvSoak, pacedEnvStallAfter})
	if err != nil {
		t.Fatal(err)
	}
	pacedApplyEnv(c, map[string]*string{pacedEnvSidecarImage: str("bad"), pacedEnvSoak: str("10s"), pacedEnvStallAfter: str("45s")})

	// A second change (the good release) must not overwrite the originals
	// with the bad release's values.
	stored, err = pacedRecordOriginalEnv(stored, c, []string{pacedEnvSidecarImage, pacedEnvStallAfter})
	if err != nil {
		t.Fatal(err)
	}
	pacedApplyEnv(c, map[string]*string{pacedEnvSidecarImage: str("good"), pacedEnvStallAfter: str("30m")})

	originals := map[string]*string{}
	if err := json.Unmarshal([]byte(stored), &originals); err != nil {
		t.Fatal(err)
	}
	pacedApplyEnv(c, originals)
	if v, ok := pacedEnvValue(c, pacedEnvSidecarImage); !ok || v != "bloodraven-sidecar:playground" {
		t.Errorf("sidecar image restored to %q (set=%v)", v, ok)
	}
	if v, ok := pacedEnvValue(c, pacedEnvSoak); !ok || v != "1m" {
		t.Errorf("soak restored to %q (set=%v)", v, ok)
	}
	if v, ok := pacedEnvValue(c, pacedEnvStallAfter); ok {
		t.Errorf("stallAfter was not set before the scenario and must be removed, got %q", v)
	}
	if v, _ := pacedEnvValue(c, "UNRELATED"); v != "kept" {
		t.Errorf("an unrelated variable changed: %q", v)
	}
	if len(c.Env) != 3 {
		t.Errorf("env has %d entries after the restore, want 3: %+v", len(c.Env), c.Env)
	}
}

func TestPacedDeploymentRolledOut(t *testing.T) {
	one := int32(1)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Generation: 4}, Spec: appsv1.DeploymentSpec{Replicas: &one}}
	dep.Status = appsv1.DeploymentStatus{ObservedGeneration: 4, UpdatedReplicas: 1, Replicas: 1, AvailableReplicas: 1}
	if !pacedDeploymentRolledOut(dep) {
		t.Error("a rolled-out deployment reads as not rolled out")
	}
	for name, st := range map[string]appsv1.DeploymentStatus{
		"stale generation":        {ObservedGeneration: 3, UpdatedReplicas: 1, Replicas: 1, AvailableReplicas: 1},
		"old pod still counted":   {ObservedGeneration: 4, UpdatedReplicas: 1, Replicas: 2, AvailableReplicas: 1},
		"new pod not available":   {ObservedGeneration: 4, UpdatedReplicas: 1, Replicas: 1, AvailableReplicas: 0},
		"new pod not created":     {ObservedGeneration: 4, UpdatedReplicas: 0, Replicas: 0, AvailableReplicas: 0},
		"old pod, not yet rolled": {ObservedGeneration: 4, UpdatedReplicas: 0, Replicas: 1, AvailableReplicas: 1},
	} {
		dep.Status = st
		if pacedDeploymentRolledOut(dep) {
			t.Errorf("%s: reads as rolled out", name)
		}
	}
}

// A spare node is one the playground group never taints: outside its
// zones and its taintNodeSelector labels, schedulable and untainted.
func TestPacedPickSpareNode(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{
		{Name: "iad", Zone: "zone-iad", TaintNodeSelector: map[string]string{"bloodraven.shipstream.io/failover-group.playground": "true", "bloodraven.shipstream.io/site.playground": "iad"}},
		{Name: "pdx", Zone: "zone-pdx", TaintNodeSelector: map[string]string{"bloodraven.shipstream.io/failover-group.playground": "true", "bloodraven.shipstream.io/site.playground": "pdx"}},
	}}}
	ready := corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}
	node := func(name string, labels map[string]string, taints ...corev1.Taint) corev1.Node {
		return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Spec: corev1.NodeSpec{Taints: taints}, Status: ready}
	}
	site := func(name, s string) corev1.Node {
		return node(name, map[string]string{pacedZoneLabel: "zone-" + s, "bloodraven.shipstream.io/failover-group.playground": "true", "bloodraven.shipstream.io/site.playground": s})
	}
	controlPlane := node("control-plane", nil, corev1.Taint{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule})

	// The CI cluster of the release profile: three workers, all sites.
	if n := pacedPickSpareNode([]corev1.Node{controlPlane, site("w1", "iad"), site("w2", "pdx")}, fg); n != nil {
		t.Errorf("picked %s on a cluster with no spare node", n.Name)
	}
	// A fourth worker.
	if n := pacedPickSpareNode([]corev1.Node{controlPlane, site("w1", "iad"), site("w2", "pdx"), node("w4", nil)}, fg); n == nil || n.Name != "w4" {
		t.Errorf("picked %v, want w4", n)
	}
	// A node in one of the group's zones is not spare even without the
	// group's labels: the zone selector would put a site's pod there.
	if n := pacedPickSpareNode([]corev1.Node{node("w5", map[string]string{pacedZoneLabel: "zone-pdx"})}, fg); n != nil {
		t.Errorf("picked %s, which is in a zone of the group", n.Name)
	}
	// Not Ready, or cordoned.
	notReady := node("w6", nil)
	notReady.Status = corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}}
	cordoned := node("w7", nil)
	cordoned.Spec.Unschedulable = true
	if n := pacedPickSpareNode([]corev1.Node{notReady, cordoned}, fg); n != nil {
		t.Errorf("picked %s, which cannot take pods", n.Name)
	}
	// A node that already has a zone is preferred: nothing to label.
	if n := pacedPickSpareNode([]corev1.Node{node("w4", nil), node("w8", map[string]string{pacedZoneLabel: "zone-other"})}, fg); n == nil || n.Name != "w8" {
		t.Errorf("picked %v, want w8", n)
	}
}

func TestPacedImageOnNodes(t *testing.T) {
	withImages := func(zone string, n int, names ...string) corev1.Node {
		node := corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{pacedZoneLabel: zone}}}
		for i := 0; i < n; i++ {
			node.Status.Images = append(node.Status.Images, corev1.ContainerImage{Names: []string{"docker.io/library/filler:" + strings.Repeat("x", i+1)}})
		}
		node.Status.Images = append(node.Status.Images, corev1.ContainerImage{Names: names})
		return node
	}
	const next = "bloodraven-sidecar:playground-next"
	if present, known := pacedImageOnNodes([]corev1.Node{withImages("zone-iad", 3, "docker.io/library/"+next)}, "zone-iad", next); !present || !known {
		t.Errorf("an image the node lists: present=%v known=%v", present, known)
	}
	if present, known := pacedImageOnNodes([]corev1.Node{withImages("zone-iad", 3, "docker.io/library/bloodraven-sidecar:playground")}, "zone-iad", next); present || !known {
		t.Errorf("a missing image on a short list: present=%v known=%v, want a known absence", present, known)
	}
	// A list at the kubelet's limit may be truncated.
	if present, known := pacedImageOnNodes([]corev1.Node{withImages("zone-iad", 60)}, "zone-iad", next); present || known {
		t.Errorf("a missing image on a truncated list: present=%v known=%v, want unknown", present, known)
	}
	// Another zone's node says nothing.
	if _, known := pacedImageOnNodes([]corev1.Node{withImages("zone-pdx", 3, "docker.io/library/"+next)}, "zone-iad", next); known {
		t.Error("no node in the zone must read as unknown")
	}
}

// --- invariants ------------------------------------------------------------

func pacedTestTime(sec int) time.Time {
	return time.Date(2026, 11, 12, 10, 0, sec, 0, time.UTC)
}

func pacedTestRecord(phase v1alpha1.RolloutPhase, admitted, completed int) *v1alpha1.RolloutStatus {
	r := &v1alpha1.RolloutStatus{Phase: phase, Renderer: "new"}
	if admitted >= 0 {
		t := metav1.NewTime(pacedTestTime(admitted))
		r.AdmittedTime, r.StartedTime = &t, &t
	}
	if completed >= 0 {
		t := metav1.NewTime(pacedTestTime(completed))
		r.CompletedTime = &t
	}
	return r
}

// pacedTestGroup is a two-site group on pods "<gen>-one"/"<gen>-two".
func pacedTestGroup(name, gen string, rollout *v1alpha1.RolloutStatus) pacedGroupSample {
	by, hash, image := "", "hash-old", "sidecar:old"
	if gen != "old" {
		by, hash, image = "new", "hash-new", "sidecar:new"
	}
	return pacedGroupSample{
		Group: name, ActiveSite: "one", Ready: true, Rollout: rollout,
		Pods: map[string][]pacedPod{
			"one": {{UID: name + "-" + gen + "-one", Ready: true}},
			"two": {{UID: name + "-" + gen + "-two", Ready: true}},
		},
		Workloads: map[string]pacedWorkload{
			"one": {RenderedBy: by, SpecHash: hash, SidecarImage: image},
			"two": {RenderedBy: by, SpecHash: hash, SidecarImage: image},
		},
	}
}

func pacedTestSample(sec int, groups ...pacedGroupSample) pacedSample {
	s := pacedSample{At: pacedTestTime(sec), Groups: map[string]pacedGroupSample{}}
	for _, g := range groups {
		s.Groups[g.Group] = g
	}
	return s
}

// A paced rollout of two groups, as the samples of a correct operator show
// it: a rolls and completes, then b is admitted, rolls and completes.
func pacedTestGoodRun() (base pacedSample, samples []pacedSample, final pacedSample) {
	pending := &v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhasePending, Reason: "WaitingForSlot"}
	base = pacedTestSample(0, pacedTestGroup("a", "old", nil), pacedTestGroup("b", "old", nil))
	samples = []pacedSample{
		base,
		pacedTestSample(5, pacedTestGroup("a", "old", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 4, -1)), pacedTestGroup("b", "old", pending)),
		pacedTestSample(30, pacedTestGroup("a", "new", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 4, -1)), pacedTestGroup("b", "old", pending)),
		pacedTestSample(40, pacedTestGroup("a", "new", pacedTestRecord(v1alpha1.RolloutPhaseComplete, 4, 38)), pacedTestGroup("b", "old", pending)),
		pacedTestSample(55, pacedTestGroup("a", "new", pacedTestRecord(v1alpha1.RolloutPhaseComplete, 4, 38)), pacedTestGroup("b", "old", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 50, -1))),
		pacedTestSample(90, pacedTestGroup("a", "new", pacedTestRecord(v1alpha1.RolloutPhaseComplete, 4, 38)), pacedTestGroup("b", "new", pacedTestRecord(v1alpha1.RolloutPhaseComplete, 50, 88))),
	}
	return base, samples, samples[len(samples)-1]
}

func TestPacedInvariantsAcceptAPacedRollout(t *testing.T) {
	base, samples, final := pacedTestGoodRun()
	groups := []string{"a", "b"}
	if err := pacedCheckOneAtATime(samples); err != nil {
		t.Error(err)
	}
	if err := pacedCheckHeldGroupsKeepPods(base, samples, groups); err != nil {
		t.Error(err)
	}
	order, err := pacedAdmissionOrder(samples, groups)
	if err != nil || strings.Join(order, ",") != "a,b" {
		t.Fatalf("admission order = %v, %v", order, err)
	}
	if err := pacedCheckSequential(base, samples, final, order); err != nil {
		t.Error(err)
	}
	for _, g := range groups {
		if renderer, err := pacedCheckRolled(base.Groups[g], final.Groups[g]); err != nil || renderer != "new" {
			t.Errorf("group %s: renderer=%q err=%v", g, renderer, err)
		}
	}
	if lines := pacedTimeline(samples, groups); len(lines) < 6 {
		t.Errorf("timeline has %d lines, want one per change:\n%s", len(lines), strings.Join(lines, "\n"))
	}
}

// What an unpaced release (every group at once, as before v1.8.0) looks
// like in the samples must fail each invariant that names it.
func TestPacedInvariantsRejectAnUnpacedRollout(t *testing.T) {
	base, good, final := pacedTestGoodRun()
	groups := []string{"a", "b"}

	// Two groups admitted in one sample.
	both := append([]pacedSample{}, good...)
	both[2] = pacedTestSample(30, pacedTestGroup("a", "new", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 4, -1)), pacedTestGroup("b", "old", pacedTestRecord(v1alpha1.RolloutPhaseAdmitted, 20, -1)))
	if err := pacedCheckOneAtATime(both); err == nil || !strings.Contains(err.Error(), "more than one group") {
		t.Errorf("two admissions in one sample: %v", err)
	}

	// A group that is not admitted loses a pod.
	rolledWhilePending := append([]pacedSample{}, good...)
	rolledWhilePending[2] = pacedTestSample(30, pacedTestGroup("a", "new", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 4, -1)),
		pacedTestGroup("b", "new", &v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhasePending}))
	if err := pacedCheckHeldGroupsKeepPods(base, rolledWhilePending, groups); err == nil || !strings.Contains(err.Error(), "group b was not admitted") {
		t.Errorf("a pending group rolled: %v", err)
	}

	// A group with no record at all loses a pod (pacing switched off).
	noRecord := []pacedSample{base, pacedTestSample(10, pacedTestGroup("a", "new", nil), pacedTestGroup("b", "new", nil))}
	if err := pacedCheckHeldGroupsKeepPods(base, noRecord, groups); err == nil {
		t.Error("groups rolled without any admission and the invariant held")
	}
	if _, err := pacedAdmissionOrder(noRecord, groups); err == nil {
		t.Error("groups that were never admitted have an admission order")
	}

	// A held group whose container restarted.
	restarted := append([]pacedSample{}, good...)
	g := pacedTestGroup("b", "old", &v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhasePending})
	g.Pods["two"] = []pacedPod{{UID: "b-old-two", Restarts: 1, Ready: true}}
	restarted[1] = pacedTestSample(5, pacedTestGroup("a", "old", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 4, -1)), g)
	if err := pacedCheckHeldGroupsKeepPods(base, restarted, groups); err == nil || !strings.Contains(err.Error(), "restarted containers") {
		t.Errorf("a held group's container restarted: %v", err)
	}

	// b admitted before a completed, by the final records.
	early := final
	early.Groups = map[string]pacedGroupSample{
		"a": final.Groups["a"],
		"b": pacedTestGroup("b", "new", pacedTestRecord(v1alpha1.RolloutPhaseComplete, 20, 88)),
	}
	if err := pacedCheckSequential(base, good, early, []string{"a", "b"}); err == nil || !strings.Contains(err.Error(), "before a completed") {
		t.Errorf("b admitted before a completed: %v", err)
	}

	// b's pods changed in a sample where a shows no completedTime.
	overlap := append([]pacedSample{}, good...)
	overlap[2] = pacedTestSample(30, pacedTestGroup("a", "new", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 4, -1)), pacedTestGroup("b", "new", pacedTestRecord(v1alpha1.RolloutPhaseRolling, 20, -1)))
	if err := pacedCheckSequential(base, overlap, final, []string{"a", "b"}); err == nil || !strings.Contains(err.Error(), "had not completed its roll") {
		t.Errorf("b rolled while a was rolling: %v", err)
	}

	// The claimed order contradicts admittedTime.
	if err := pacedCheckSequential(base, good, final, []string{"b", "a"}); err == nil {
		t.Error("the order b,a was accepted although a was admitted first")
	}

	// A group that never completed.
	unfinished := final
	unfinished.Groups = map[string]pacedGroupSample{"a": final.Groups["a"], "b": pacedTestGroup("b", "old", pacedTestRecord(v1alpha1.RolloutPhaseStalled, 50, -1))}
	if err := pacedCheckSequential(base, good, unfinished, []string{"a", "b"}); err == nil || !strings.Contains(err.Error(), "did not complete") {
		t.Errorf("an unfinished group: %v", err)
	}
}

func TestPacedCheckRolledRejectsAPartialRoll(t *testing.T) {
	base := pacedTestGroup("a", "old", nil)
	for name, mutate := range map[string]func(*pacedGroupSample){
		"a pod that was not replaced": func(g *pacedGroupSample) { g.Pods["two"] = base.Pods["two"] },
		"a pod that is not Ready":     func(g *pacedGroupSample) { g.Pods["two"] = []pacedPod{{UID: "a-new-two"}} },
		"two pods on one site":        func(g *pacedGroupSample) { g.Pods["two"] = append(g.Pods["two"], pacedPod{UID: "extra", Ready: true}) },
		"no renderer annotation":      func(g *pacedGroupSample) { w := g.Workloads["two"]; w.RenderedBy = ""; g.Workloads["two"] = w },
		"two renderers in one group":  func(g *pacedGroupSample) { w := g.Workloads["two"]; w.RenderedBy = "other"; g.Workloads["two"] = w },
		"an unchanged spec hash":      func(g *pacedGroupSample) { w := g.Workloads["two"]; w.SpecHash = "hash-old"; g.Workloads["two"] = w },
	} {
		final := pacedTestGroup("a", "new", nil)
		mutate(&final)
		if _, err := pacedCheckRolled(base, final); err == nil {
			t.Errorf("%s: read as fully rolled", name)
		}
	}
	// A restamp only: same pods, same hash, the renderer annotation moved.
	restamped := pacedTestGroup("a", "old", nil)
	for site, w := range restamped.Workloads {
		w.RenderedBy = "new"
		restamped.Workloads[site] = w
	}
	if _, err := pacedCheckRolled(base, restamped); err == nil {
		t.Error("a restamped group read as rolled")
	}
}

// The halt check of scenario 73: what a halted rollout must look like, and
// each way it can be wrong.
func TestS73HaltProblems(t *testing.T) {
	const bad = "sidecar:bad"
	started := pacedTestRecord(v1alpha1.RolloutPhaseStalled, 4, -1)
	stalledAt := metav1.NewTime(pacedTestTime(50))
	started.StalledTime = &stalledAt
	run := &s73Run{
		groups: []string{"a", "b"}, first: "a", second: "b", bad: bad, started: started,
		base: pacedTestSample(0, pacedTestGroup("a", "old", nil), pacedTestGroup("b", "old", nil), pacedTestGroup("playground", "old", nil)),
	}
	halted := func() pacedSample {
		a := pacedTestGroup("a", "old", started.DeepCopy())
		// The follower (site two) was rolled to the bad release.
		a.Pods["two"] = []pacedPod{{UID: "a-bad-two", Ready: false}}
		a.Workloads["two"] = pacedWorkload{RenderedBy: "bad-renderer", SpecHash: "hash-bad", SidecarImage: bad}
		b := pacedTestGroup("b", "old", &v1alpha1.RolloutStatus{Phase: v1alpha1.RolloutPhasePending, Reason: "RolloutHalted"})
		return pacedTestSample(60, a, b, pacedTestGroup("playground", "old", nil))
	}
	if problems := s73HaltProblemsIn(run, "playground", halted()); len(problems) != 0 {
		t.Fatalf("a halted rollout reads as wrong: %v", problems)
	}

	for name, tc := range map[string]struct {
		mutate func(*pacedSample)
		want   string
	}{
		"the primary was rolled": {func(s *pacedSample) {
			g := s.Groups["a"]
			g.Pods["one"] = []pacedPod{{UID: "a-bad-one"}}
			s.Groups["a"] = g
		}, "a primary was rolled"},
		"the primary Deployment was written": {func(s *pacedSample) {
			g := s.Groups["a"]
			g.Workloads["one"] = pacedWorkload{RenderedBy: "bad-renderer", SpecHash: "hash-bad", SidecarImage: bad}
			s.Groups["a"] = g
		}, "a primary Deployment was written"},
		"the group was handed over": {func(s *pacedSample) {
			g := s.Groups["a"]
			g.ActiveSite = "two"
			s.Groups["a"] = g
		}, "a was handed over"},
		"the first group is not stalled": {func(s *pacedSample) {
			g := s.Groups["a"]
			g.Rollout = pacedTestRecord(v1alpha1.RolloutPhaseRolling, 4, -1)
			s.Groups["a"] = g
		}, "a is not Stalled"},
		"the latch moved": {func(s *pacedSample) {
			g := s.Groups["a"]
			moved := metav1.NewTime(pacedTestTime(59))
			g.Rollout.StartedTime = &moved
			s.Groups["a"] = g
		}, "startedTime moved"},
		"the second group was admitted": {func(s *pacedSample) {
			g := s.Groups["b"]
			g.Rollout = pacedTestRecord(v1alpha1.RolloutPhaseAdmitted, 55, -1)
			s.Groups["b"] = g
		}, "b is not Pending/RolloutHalted"},
		"the second group lost a pod": {func(s *pacedSample) {
			g := s.Groups["b"]
			g.Pods["two"] = []pacedPod{{UID: "b-bad-two"}}
			s.Groups["b"] = g
		}, "b did not keep its pods"},
		"the second group's Deployment was written": {func(s *pacedSample) {
			g := s.Groups["b"]
			g.Workloads["two"] = pacedWorkload{RenderedBy: "bad-renderer", SpecHash: "hash-bad", SidecarImage: bad}
			s.Groups["b"] = g
		}, "deployment of site two changed"},
		"the pinned group was rolled": {func(s *pacedSample) {
			g := s.Groups["playground"]
			g.Pods["one"] = []pacedPod{{UID: "playground-bad-one"}}
			s.Groups["playground"] = g
		}, "pinned group playground did not keep its pods"},
		"a pod runs Ready on the bad release": {func(s *pacedSample) {
			g := s.Groups["a"]
			g.Pods["two"] = []pacedPod{{UID: "a-bad-two", Ready: true}}
			s.Groups["a"] = g
		}, "is Ready on an image that cannot be pulled"},
		"no follower was rolled": {func(s *pacedSample) {
			g := s.Groups["a"]
			g.Workloads["two"] = pacedWorkload{RenderedBy: "", SpecHash: "hash-old", SidecarImage: "sidecar:old"}
			s.Groups["a"] = g
		}, "no follower Deployment carries the bad release"},
	} {
		s := halted()
		tc.mutate(&s)
		problems := strings.Join(s73HaltProblemsIn(run, "playground", s), "; ")
		if !strings.Contains(problems, tc.want) {
			t.Errorf("%s: problems = %q, want %q", name, problems, tc.want)
		}
	}
}
