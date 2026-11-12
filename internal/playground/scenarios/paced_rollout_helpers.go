package scenarios

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	sigsyaml "sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// Helpers of the paced-rollout scenarios (proposals/23-paced-rollouts.md):
// 73 (a bad build halts the rollout at the first group) and 74 (an upgrade
// from the previous release is paced).
//
// Both need more than one failover group, and the playground has one. They
// create small auxiliary groups for their duration (two sites, no Dragonfly)
// and remove them in cleanup. A promotable site must have a
// taintNodeSelector; theirs names a label no node carries
// (pacedAuxTaintLabel), so an auxiliary group taints nothing.
//
// Where an auxiliary group may run is decided by the playground group's
// db-readonly taint: the operator taints the nodes of every site of the
// playground group that is not writable (NoExecute), and a pod of another
// group does not tolerate that taint, on purpose. So an auxiliary pod can
// only live on a node the playground group does not taint:
//
//   - the node of the playground group's active site, for as long as that
//     group does not fail over (scenario 73 pins the playground group's
//     sidecar image, so the release-driven change does not roll it);
//   - a spare node that carries none of the playground group's labels
//     (scenario 74, where the playground group rolls and hands off too).
//
// Both sites of an auxiliary group therefore share one zone and one node.

const (
	// pacedAuxLabel marks the auxiliary groups, so cleanup finds what an
	// abandoned run left behind.
	pacedAuxLabel = "playground.bloodraven.shipstream.io/paced-rollout-aux"

	// pacedAuxTaintLabel is the key of the auxiliary sites'
	// taintNodeSelector. No node is ever given this label: the API requires
	// the selector, and selecting no node is how an auxiliary group stays
	// out of the node taints (the tainter lists the nodes a selector
	// matches and taints those).
	pacedAuxTaintLabel = "playground.bloodraven.shipstream.io/paced-rollout-aux-site"

	// pacedOriginalEnvAnnotation keeps, on the operator Deployment, the
	// environment values a scenario replaced. Cleanup restores from it, so
	// a later run can undo what a killed run changed.
	pacedOriginalEnvAnnotation = "playground.bloodraven.shipstream.io/paced-rollout-original-env"

	// pacedPinAnnotation marks a spec.sidecar.image pin the scenario put on
	// the playground group; cleanup removes only a pin it finds marked.
	pacedPinAnnotation = "playground.bloodraven.shipstream.io/paced-rollout-pinned-sidecar"

	// pacedSpareZoneAnnotation marks a node whose zone label the scenario
	// set; cleanup removes only such a label.
	pacedSpareZoneAnnotation = "playground.bloodraven.shipstream.io/paced-rollout-zone"
	pacedSpareZone           = "zone-paced-aux"
	pacedZoneLabel           = "topology.kubernetes.io/zone"

	pacedOperatorDeployment = "bloodraven"
	pacedOperatorContainer  = "operator"

	pacedEnvSidecarImage = "BLOODRAVEN_SIDECAR_IMAGE"
	pacedEnvPacing       = "BLOODRAVEN_ROLLOUT_PACING"
	pacedEnvSoak         = "BLOODRAVEN_ROLLOUT_SOAK"
	pacedEnvStallAfter   = "BLOODRAVEN_ROLLOUT_STALL_AFTER"

	// pacedNextTagSuffix names the second tag of the sidecar image that
	// setup.sh, rebuild.sh and `make e2e-images` load into the cluster
	// (bloodraven-sidecar:playground-next): the same build under another
	// reference, which the operator sees as a new default sidecar image.
	pacedNextTagSuffix = "-next"
)

// pacedAuxGroupNames are the auxiliary groups, in the order the pacer admits
// them: its queue is by namespace and name, and both sort before the
// playground group ("paced-..." < "playground").
var pacedAuxGroupNames = []string{"paced-a", "paced-b"}

// pacedAuxGroupManifest renders one auxiliary group. index keeps server
// ids, DNS names and load-balancer addresses apart.
func pacedAuxGroupManifest(namespace, name, zone string, index int) string {
	return fmt.Sprintf(`apiVersion: shipstream.io/v1alpha1
kind: MysqlFailoverGroup
metadata:
  name: %[2]s
  namespace: %[1]s
  labels:
    %[5]s: "true"
spec:
  image: mysql:9.7
  credentials:
    operatorSecretName: mysql-credentials
  pollInterval: 2s
  failureThreshold: 3
  recoveryThreshold: 2
  failoverCooldown: 30s
  splitBrainPolicy:
    sitePriorities: [one, two]
  dns:
    hostname: %[2]s-db.example.local
    ttlSeconds: 10
  sites:
    - name: one
      serverID: %[6]d
      role: PrimaryCandidate
      zone: %[3]s
      taintNodeSelector:
        %[8]s: %[2]s-one
      loadBalancerIP: "10.96.%[4]d.10"
      storage:
        storageClassName: playground-fast
        size: 1Gi
      resources:
        requests:
          cpu: 50m
          memory: 256Mi
        limits:
          cpu: 500m
          memory: 768Mi
    - name: two
      serverID: %[7]d
      role: PrimaryCandidate
      zone: %[3]s
      taintNodeSelector:
        %[8]s: %[2]s-two
      loadBalancerIP: "10.96.%[4]d.20"
      storage:
        storageClassName: playground-fast
        size: 1Gi
      resources:
        requests:
          cpu: 50m
          memory: 256Mi
        limits:
          cpu: 500m
          memory: 768Mi
  sidecar:
    leaseTimeout: 20s
    peerCheckInterval: 5s
  replication:
    maxLagSeconds: 30
`, namespace, name, zone, 110+index, pacedAuxLabel, 201+10*index, 202+10*index, pacedAuxTaintLabel)
}

// pacedAuxGroupObject parses pacedAuxGroupManifest strictly, so a field the
// API does not know fails here and not as a silently dropped key.
func pacedAuxGroupObject(namespace, name, zone string, index int) (*v1alpha1.MysqlFailoverGroup, error) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	if err := sigsyaml.UnmarshalStrict([]byte(pacedAuxGroupManifest(namespace, name, zone, index)), fg); err != nil {
		return nil, fmt.Errorf("parse auxiliary group %s: %w", name, err)
	}
	return fg, nil
}

// PacedRolloutAuxGroups returns the auxiliary failover groups the
// paced-rollout scenarios create, for zone. Exported for the envtest that
// submits them to the API server: a CRD rule that refuses them would
// otherwise only show on a cluster.
func PacedRolloutAuxGroups(namespace, zone string) ([]*v1alpha1.MysqlFailoverGroup, error) {
	groups := make([]*v1alpha1.MysqlFailoverGroup, 0, len(pacedAuxGroupNames))
	for i, name := range pacedAuxGroupNames {
		fg, err := pacedAuxGroupObject(namespace, name, zone, i)
		if err != nil {
			return nil, err
		}
		groups = append(groups, fg)
	}
	return groups, nil
}

// pacedCreateAuxGroups creates the auxiliary groups in zone.
func pacedCreateAuxGroups(ctx context.Context, env *runner.Env, zone string) error {
	groups, err := PacedRolloutAuxGroups(env.Namespace, zone)
	if err != nil {
		return err
	}
	for _, fg := range groups {
		if err := env.Kube.Controller.Create(ctx, fg); err != nil {
			return fmt.Errorf("create auxiliary group %s: %w", fg.Name, err)
		}
	}
	return nil
}

// pacedWaitGroupsHealthy waits until every named group passes the shared
// baseline (Ready, an active site, every site observed and replicating) for
// stableFor.
func pacedWaitGroupsHealthy(ctx context.Context, env *runner.Env, groups []string, stableFor time.Duration) error {
	var healthySince time.Time
	var last string
	started := time.Now()
	lastLog := started
	for {
		var bad []string
		for _, g := range groups {
			if err := CheckBaseline(ctx, env.Kube, env.Namespace, g); err != nil {
				bad = append(bad, fmt.Sprintf("%s: %v", g, err))
			}
		}
		if len(bad) == 0 {
			if healthySince.IsZero() {
				healthySince = time.Now()
			}
			if time.Since(healthySince) >= stableFor {
				return nil
			}
			last = fmt.Sprintf("healthy for %s of %s", time.Since(healthySince).Round(time.Second), stableFor)
		} else {
			healthySince = time.Time{}
			last = strings.Join(bad, "; ")
		}
		if time.Since(lastLog) >= 10*time.Second {
			lastLog = time.Now()
			env.Logger.Info("wait", "what", "groups healthy", "groups", strings.Join(groups, ","),
				"last", last, "elapsed", time.Since(started).Round(time.Second))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("groups %v not healthy after %s: %s", groups, time.Since(started).Round(time.Second), last)
		case <-time.After(time.Second):
		}
	}
}

// pacedListAuxGroups returns the names of the auxiliary groups present.
func pacedListAuxGroups(ctx context.Context, env *runner.Env) ([]string, error) {
	var list v1alpha1.MysqlFailoverGroupList
	if err := env.Kube.Controller.List(ctx, &list, client.InNamespace(env.Namespace), client.MatchingLabels{pacedAuxLabel: "true"}); err != nil {
		return nil, fmt.Errorf("list auxiliary groups: %w", err)
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].Name)
	}
	sort.Strings(names)
	return names, nil
}

// pacedDeleteAuxGroups deletes every auxiliary group and waits until the
// groups, their pods and their volume claims are gone. The operator runs
// the groups' finalizer, so it must be up; a group still there after the
// wait is reported with the command that removes it by hand.
func pacedDeleteAuxGroups(ctx context.Context, env *runner.Env) error {
	names, err := pacedListAuxGroups(ctx, env)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}
	for _, name := range names {
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: env.Namespace, Name: name}}
		if err := env.Kube.Controller.Delete(ctx, fg, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete auxiliary group %s: %w", name, err)
		}
	}
	last := ""
	for {
		var left []string
		for _, name := range names {
			if _, err := env.Kube.GetMFGNamed(ctx, env.Namespace, name); err == nil {
				left = append(left, "group "+name)
				continue
			}
			pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{
				LabelSelector: "app.kubernetes.io/name=mysql,app.kubernetes.io/instance=" + name})
			if err != nil || len(pods.Items) > 0 {
				left = append(left, "pods of "+name)
			}
			pvcs, err := env.Kube.Kubernetes.CoreV1().PersistentVolumeClaims(env.Namespace).List(ctx, metav1.ListOptions{
				LabelSelector: "app.kubernetes.io/instance=" + name})
			if err != nil || len(pvcs.Items) > 0 {
				left = append(left, "volume claims of "+name)
			}
		}
		if len(left) == 0 {
			return nil
		}
		last = strings.Join(left, ", ")
		select {
		case <-ctx.Done():
			return fmt.Errorf("auxiliary groups not removed (%s still present): kubectl -n %s delete mysqlfailovergroup -l %s=true, and if the operator is down also kubectl -n %s patch mysqlfailovergroup <name> --type merge -p '{\"metadata\":{\"finalizers\":null}}'",
				last, env.Namespace, pacedAuxLabel, env.Namespace)
		case <-time.After(2 * time.Second):
		}
	}
}

// pacedSiteZone is the zone of site in fg's spec.
func pacedSiteZone(fg *v1alpha1.MysqlFailoverGroup, site string) string {
	for _, s := range fg.Spec.Sites {
		if s.Name == site {
			return s.Zone
		}
	}
	return ""
}

// pacedNodeUsable reports whether pods without tolerations can run on node.
func pacedNodeUsable(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, t := range node.Spec.Taints {
		if t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute {
			return false
		}
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// pacedPickSpareNode chooses a node the fg group never taints: one that
// carries none of the group's taintNodeSelector labels and accepts pods
// now. It prefers a node that already has a zone label.
func pacedPickSpareNode(nodes []corev1.Node, fg *v1alpha1.MysqlFailoverGroup) *corev1.Node {
	groupLabels := map[string]bool{}
	for _, s := range fg.Spec.Sites {
		for k := range s.TaintNodeSelector {
			groupLabels[k] = true
		}
	}
	zones := map[string]bool{}
	for _, s := range fg.Spec.Sites {
		zones[s.Zone] = true
	}
	var pick *corev1.Node
	for i := range nodes {
		n := &nodes[i]
		if !pacedNodeUsable(n) || zones[n.Labels[pacedZoneLabel]] {
			continue
		}
		selected := false
		for k := range groupLabels {
			if _, ok := n.Labels[k]; ok {
				selected = true
			}
		}
		if selected {
			continue
		}
		if pick == nil || (pick.Labels[pacedZoneLabel] == "" && n.Labels[pacedZoneLabel] != "") {
			pick = n
		}
	}
	return pick
}

// pacedEnsureSpareZone returns the zone of a spare node (see
// pacedPickSpareNode), labelling the node with pacedSpareZone when it has
// no zone yet.
func pacedEnsureSpareZone(ctx context.Context, env *runner.Env) (string, error) {
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return "", err
	}
	nodes, err := env.Kube.ListNodes(ctx)
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	node := pacedPickSpareNode(nodes.Items, fg)
	if node == nil {
		return "", fmt.Errorf("no spare node: this scenario rolls the %s group too, whose db-readonly taint evicts other groups' pods from its sites' nodes, so the auxiliary groups need a schedulable node outside that group "+
			"(k3d: the server node or `--agents 4`; kind: a fourth worker, see .github/kind/e2e-upgrade.yaml)", env.FG)
	}
	if zone := node.Labels[pacedZoneLabel]; zone != "" {
		return zone, nil
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q},"annotations":{%q:"true"}}}`, pacedZoneLabel, pacedSpareZone, pacedSpareZoneAnnotation)
	if _, err := env.Kube.Kubernetes.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return "", fmt.Errorf("label spare node %s: %w", node.Name, err)
	}
	env.Capture.Note(fmt.Sprintf("spare node %s labelled %s=%s for the auxiliary groups", node.Name, pacedZoneLabel, pacedSpareZone))
	return pacedSpareZone, nil
}

// pacedRemoveSpareZone removes the zone label from the nodes the scenario
// labelled.
func pacedRemoveSpareZone(ctx context.Context, env *runner.Env) error {
	nodes, err := env.Kube.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	var errs []error
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if n.Annotations[pacedSpareZoneAnnotation] == "" {
			continue
		}
		patch := fmt.Sprintf(`{"metadata":{"labels":{%q:null},"annotations":{%q:null}}}`, pacedZoneLabel, pacedSpareZoneAnnotation)
		if _, err := env.Kube.Kubernetes.CoreV1().Nodes().Patch(ctx, n.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			errs = append(errs, fmt.Errorf("unlabel spare node %s: %w", n.Name, err))
		}
	}
	return errors.Join(errs...)
}

// pacedImageOnNodes reports whether every node of zone lists image among
// its images. known is false when a node's list may be truncated (the
// kubelet reports at most 50 images, largest first), in which case a
// missing image proves nothing.
func pacedImageOnNodes(nodes []corev1.Node, zone, image string) (present, known bool) {
	matches := func(name string) bool {
		return name == image || strings.HasSuffix(name, "/"+image)
	}
	present, known = true, true
	seen := false
	for i := range nodes {
		n := &nodes[i]
		if n.Labels[pacedZoneLabel] != zone {
			continue
		}
		seen = true
		found := false
		for _, img := range n.Status.Images {
			for _, name := range img.Names {
				found = found || matches(name)
			}
		}
		if !found {
			present = false
			if len(n.Status.Images) >= 50 {
				known = false
			}
		}
	}
	if !seen {
		return false, false
	}
	return present, known
}

// --- operator Deployment environment -------------------------------------

// pacedOperatorContainerOf returns the operator container of dep.
func pacedOperatorContainerOf(dep *appsv1.Deployment) (*corev1.Container, error) {
	for i := range dep.Spec.Template.Spec.Containers {
		if dep.Spec.Template.Spec.Containers[i].Name == pacedOperatorContainer {
			return &dep.Spec.Template.Spec.Containers[i], nil
		}
	}
	return nil, fmt.Errorf("deployment %s has no container %q", dep.Name, pacedOperatorContainer)
}

// pacedEnvValue returns the literal value of name in c, and whether it is
// set.
func pacedEnvValue(c *corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// pacedApplyEnv sets (or, for a nil value, removes) variables on c.
func pacedApplyEnv(c *corev1.Container, set map[string]*string) {
	for name, value := range set {
		idx := -1
		for i := range c.Env {
			if c.Env[i].Name == name {
				idx = i
			}
		}
		switch {
		case value == nil && idx >= 0:
			c.Env = append(c.Env[:idx], c.Env[idx+1:]...)
		case value != nil && idx >= 0:
			c.Env[idx].Value, c.Env[idx].ValueFrom = *value, nil
		case value != nil:
			c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: *value})
		}
	}
}

// pacedRecordOriginalEnv merges into the stored originals (the JSON of
// pacedOriginalEnvAnnotation, "" for none) the current value of each
// variable about to change. A variable already recorded keeps its first
// value: that is the one from before the scenario.
func pacedRecordOriginalEnv(stored string, c *corev1.Container, names []string) (string, error) {
	originals := map[string]*string{}
	if stored != "" {
		if err := json.Unmarshal([]byte(stored), &originals); err != nil {
			return "", fmt.Errorf("parse %s: %w", pacedOriginalEnvAnnotation, err)
		}
	}
	for _, name := range names {
		if _, recorded := originals[name]; recorded {
			continue
		}
		if v, ok := pacedEnvValue(c, name); ok {
			value := v
			originals[name] = &value
		} else {
			originals[name] = nil
		}
	}
	body, err := json.Marshal(originals)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// pacedOperatorSidecarDefault reads the operator's default sidecar image
// and whether pacing is on.
func pacedOperatorSidecarDefault(ctx context.Context, env *runner.Env) (image string, pacing bool, err error) {
	dep, err := env.Kube.GetDeployment(ctx, env.Namespace, pacedOperatorDeployment)
	if err != nil {
		return "", false, err
	}
	c, err := pacedOperatorContainerOf(dep)
	if err != nil {
		return "", false, err
	}
	image, _ = pacedEnvValue(c, pacedEnvSidecarImage)
	v, _ := pacedEnvValue(c, pacedEnvPacing)
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "false", "0", "off", "no":
		pacing = false
	default:
		pacing = true
	}
	return image, pacing, nil
}

// pacedSetOperatorEnv changes environment variables of the operator
// container, remembering the values from before the scenario on the
// Deployment, and waits until one pod of the new template is available and
// the old one is gone (the chart's strategy is Recreate).
func pacedSetOperatorEnv(ctx context.Context, env *runner.Env, set map[string]string) error {
	names := make([]string, 0, len(set))
	values := make(map[string]*string, len(set))
	for name, value := range set {
		v := value
		names = append(names, name)
		values[name] = &v
	}
	sort.Strings(names)
	deployments := env.Kube.Kubernetes.AppsV1().Deployments(env.Namespace)
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		dep, err := deployments.Get(ctx, pacedOperatorDeployment, metav1.GetOptions{})
		if err != nil {
			return err
		}
		c, err := pacedOperatorContainerOf(dep)
		if err != nil {
			return err
		}
		stored, err := pacedRecordOriginalEnv(dep.Annotations[pacedOriginalEnvAnnotation], c, names)
		if err != nil {
			return err
		}
		if dep.Annotations == nil {
			dep.Annotations = map[string]string{}
		}
		dep.Annotations[pacedOriginalEnvAnnotation] = stored
		pacedApplyEnv(c, values)
		_, err = deployments.Update(ctx, dep, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("set operator environment %v: %w", names, err)
	}
	return pacedWaitOperatorRolledOut(ctx, env)
}

// pacedRestoreOperatorEnv puts back the operator environment a scenario
// replaced, from the annotation, and removes the annotation. It reports
// whether there was anything to restore.
func pacedRestoreOperatorEnv(ctx context.Context, env *runner.Env) (bool, error) {
	deployments := env.Kube.Kubernetes.AppsV1().Deployments(env.Namespace)
	restored := false
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		dep, err := deployments.Get(ctx, pacedOperatorDeployment, metav1.GetOptions{})
		if err != nil {
			return err
		}
		stored := dep.Annotations[pacedOriginalEnvAnnotation]
		if stored == "" {
			restored = false
			return nil
		}
		originals := map[string]*string{}
		if err := json.Unmarshal([]byte(stored), &originals); err != nil {
			return fmt.Errorf("parse %s: %w", pacedOriginalEnvAnnotation, err)
		}
		c, err := pacedOperatorContainerOf(dep)
		if err != nil {
			return err
		}
		pacedApplyEnv(c, originals)
		delete(dep.Annotations, pacedOriginalEnvAnnotation)
		if _, err := deployments.Update(ctx, dep, metav1.UpdateOptions{}); err != nil {
			return err
		}
		restored = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("restore operator environment: %w (./playground/setup.sh renders the Deployment again)", err)
	}
	if !restored {
		return false, nil
	}
	return true, pacedWaitOperatorRolledOut(ctx, env)
}

// pacedDeploymentRolledOut reports whether dep runs exactly its desired
// pods on its current template, all available.
func pacedDeploymentRolledOut(dep *appsv1.Deployment) bool {
	want := int32(1)
	if dep.Spec.Replicas != nil {
		want = *dep.Spec.Replicas
	}
	st := dep.Status
	return st.ObservedGeneration >= dep.Generation && st.UpdatedReplicas == want && st.Replicas == want && st.AvailableReplicas == want
}

func pacedWaitOperatorRolledOut(ctx context.Context, env *runner.Env) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last string
	for {
		dep, err := env.Kube.GetDeployment(waitCtx, env.Namespace, pacedOperatorDeployment)
		if err == nil && pacedDeploymentRolledOut(dep) {
			return nil
		}
		if err != nil {
			last = err.Error()
		} else {
			last = fmt.Sprintf("generation=%d observed=%d updated=%d replicas=%d available=%d",
				dep.Generation, dep.Status.ObservedGeneration, dep.Status.UpdatedReplicas, dep.Status.Replicas, dep.Status.AvailableReplicas)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("operator deployment did not roll out: %s", last)
		case <-time.After(time.Second):
		}
	}
}

// --- playground group sidecar pin ----------------------------------------

// pacedPinSidecar pins fg's spec.sidecar.image to image, the operator's
// current default, when the group has no pin. The effective image does not
// change, so neither does the group's spec hash: the pin restarts nothing.
// It keeps a change of the default from rolling the group.
func pacedPinSidecar(ctx context.Context, env *runner.Env, image string) error {
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	if strings.TrimSpace(fg.Spec.Sidecar.Image) != "" {
		if fg.Annotations[pacedPinAnnotation] == "" && fg.Spec.Sidecar.Image != image {
			return fmt.Errorf("group %s pins spec.sidecar.image=%q, not the operator default %q: this scenario does not run over a foreign pin", env.FG, fg.Spec.Sidecar.Image, image)
		}
		return nil
	}
	if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, pacedPinAnnotation, "true"); err != nil {
		return err
	}
	return env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{
		{Op: "add", Path: "/spec/sidecar/image", Value: image},
	})
}

// pacedUnpinSidecar removes a pin pacedPinSidecar set.
func pacedUnpinSidecar(ctx context.Context, env *runner.Env) error {
	fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return err
	}
	if fg.Annotations[pacedPinAnnotation] == "" {
		return nil
	}
	if strings.TrimSpace(fg.Spec.Sidecar.Image) != "" {
		if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{
			{Op: "remove", Path: "/spec/sidecar/image"},
		}); err != nil {
			return fmt.Errorf("remove the sidecar pin: %w", err)
		}
	}
	return env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, pacedPinAnnotation, "")
}

// --- observation -----------------------------------------------------------

// pacedPod is one MySQL pod as a sample sees it.
type pacedPod struct {
	UID      string
	Restarts int32
	Ready    bool
}

// pacedWorkload is one site Deployment as a sample sees it.
type pacedWorkload struct {
	RenderedBy   string
	SpecHash     string
	SidecarImage string
}

// pacedGroupSample is one group at one moment. The pods are read before
// the group, so a record that does not admit the group proves that no
// paced roll had touched the pods listed.
type pacedGroupSample struct {
	Group      string
	ActiveSite string
	Ready      bool
	Rollout    *v1alpha1.RolloutStatus
	// Pods and Workloads are by site name.
	Pods      map[string][]pacedPod
	Workloads map[string]pacedWorkload
}

// pacedSample is every observed group at one moment.
type pacedSample struct {
	At     time.Time
	Groups map[string]pacedGroupSample
}

// pacedReadGroup reads one group for a sample.
func pacedReadGroup(ctx context.Context, k *pgkube.Client, namespace, group string) (pacedGroupSample, error) {
	out := pacedGroupSample{Group: group, Pods: map[string][]pacedPod{}, Workloads: map[string]pacedWorkload{}}
	// The spec names the sites; it does not change during a scenario.
	spec, err := k.GetMFGNamed(ctx, namespace, group)
	if err != nil {
		return out, err
	}
	for _, site := range spec.Spec.Sites {
		pods, err := k.ListMysqlPods(ctx, namespace, group, site.Name)
		if err != nil {
			return out, fmt.Errorf("list pods of %s/%s: %w", group, site.Name, err)
		}
		var list []pacedPod
		for i := range pods.Items {
			p := &pods.Items[i]
			pp := pacedPod{UID: string(p.UID)}
			for _, cs := range p.Status.ContainerStatuses {
				pp.Restarts += cs.RestartCount
			}
			for _, c := range p.Status.Conditions {
				if c.Type == corev1.PodReady {
					pp.Ready = c.Status == corev1.ConditionTrue
				}
			}
			list = append(list, pp)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].UID < list[j].UID })
		out.Pods[site.Name] = list

		dep, err := k.GetDeployment(ctx, namespace, pgkube.MysqlDeploymentName(group, site.Name))
		if err != nil {
			return out, fmt.Errorf("get deployment of %s/%s: %w", group, site.Name, err)
		}
		w := pacedWorkload{
			RenderedBy: dep.Annotations[v1alpha1.RenderedByAnnotation],
			SpecHash:   dep.Annotations[v1alpha1.SpecHashAnnotation],
		}
		for _, c := range dep.Spec.Template.Spec.Containers {
			if c.Name == "sidecar" {
				w.SidecarImage = c.Image
			}
		}
		out.Workloads[site.Name] = w
	}
	// The record last: see pacedGroupSample.
	fg, err := k.GetMFGNamed(ctx, namespace, group)
	if err != nil {
		return out, err
	}
	out.ActiveSite = fg.Status.ActiveSite
	out.Ready = pgkube.ReadyCondition(fg) == "True"
	out.Rollout = fg.Status.Rollout.DeepCopy()
	return out, nil
}

// pacedReadGroups reads every group into one sample.
func pacedReadGroups(ctx context.Context, k *pgkube.Client, namespace string, groups []string) (pacedSample, error) {
	s := pacedSample{At: time.Now(), Groups: make(map[string]pacedGroupSample, len(groups))}
	for _, g := range groups {
		gs, err := pacedReadGroup(ctx, k, namespace, g)
		if err != nil {
			return s, err
		}
		s.Groups[g] = gs
	}
	return s, nil
}

// pacedObserver samples the groups once a second in the background.
type pacedObserver struct {
	mu      sync.Mutex
	samples []pacedSample
	errors  int
	lastErr error
	cancel  context.CancelFunc
	done    chan struct{}
}

// pacedStartObserver starts sampling; Stop ends it.
func pacedStartObserver(k *pgkube.Client, namespace string, groups []string) *pacedObserver {
	ctx, cancel := context.WithCancel(context.Background())
	o := &pacedObserver{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(o.done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
			s, err := pacedReadGroups(readCtx, k, namespace, groups)
			readCancel()
			o.mu.Lock()
			if err != nil {
				if ctx.Err() == nil {
					o.errors++
					o.lastErr = err
				}
			} else {
				o.samples = append(o.samples, s)
			}
			o.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return o
}

// Stop ends the sampling and returns what was sampled.
func (o *pacedObserver) Stop() []pacedSample {
	o.cancel()
	<-o.done
	return o.Samples()
}

// Samples returns a copy of the samples so far.
func (o *pacedObserver) Samples() []pacedSample {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]pacedSample(nil), o.samples...)
}

// Failures returns how many samples could not be read, and the last error.
func (o *pacedObserver) Failures() (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.errors, o.lastErr
}

// --- invariants over samples ----------------------------------------------

func pacedRolloutString(r *v1alpha1.RolloutStatus) string {
	if r == nil {
		return "none"
	}
	s := string(r.Phase)
	if r.Reason != "" {
		s += "/" + r.Reason
	}
	stamp := func(label string, t *metav1.Time) {
		if t != nil {
			s += fmt.Sprintf(" %s=%s", label, t.UTC().Format("15:04:05"))
		}
	}
	stamp("admitted", r.AdmittedTime)
	stamp("started", r.StartedTime)
	stamp("stalled", r.StalledTime)
	stamp("completed", r.CompletedTime)
	if r.Renderer != "" {
		s += " renderer=" + r.Renderer
	}
	return s
}

func pacedPodUIDs(pods map[string][]pacedPod) string {
	sites := make([]string, 0, len(pods))
	for site := range pods {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	var parts []string
	for _, site := range sites {
		var uids []string
		for _, p := range pods[site] {
			uids = append(uids, p.UID)
		}
		parts = append(parts, site+"="+strings.Join(uids, "+"))
	}
	return strings.Join(parts, " ")
}

// pacedPodsUnchanged reports how now differs from base: a pod that was
// replaced, is missing or was added, or a container that restarted. "" when
// the group still runs exactly the pods of base.
func pacedPodsUnchanged(base, now map[string][]pacedPod) string {
	sites := make([]string, 0, len(base))
	for site := range base {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	for _, site := range sites {
		b, n := base[site], now[site]
		if len(b) != len(n) {
			return fmt.Sprintf("site %s has %d pod(s), had %d", site, len(n), len(b))
		}
		for i := range b {
			if b[i].UID != n[i].UID {
				return fmt.Sprintf("site %s pod %s was replaced by %s", site, b[i].UID, n[i].UID)
			}
			if n[i].Restarts != b[i].Restarts {
				return fmt.Sprintf("site %s pod %s restarted containers (%d restarts, had %d)", site, n[i].UID, n[i].Restarts, b[i].Restarts)
			}
		}
	}
	return ""
}

// pacedWorkloadsUnchanged reports how the site Deployments differ from
// base, "" when their renderer, hash and sidecar image are the same.
func pacedWorkloadsUnchanged(base, now map[string]pacedWorkload) string {
	sites := make([]string, 0, len(base))
	for site := range base {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	for _, site := range sites {
		if base[site] != now[site] {
			return fmt.Sprintf("deployment of site %s changed: %+v, was %+v", site, now[site], base[site])
		}
	}
	return ""
}

// pacedCheckOneAtATime verifies that no sample shows more than one group
// with an unfinished admission.
func pacedCheckOneAtATime(samples []pacedSample) error {
	for _, s := range samples {
		var holders []string
		for name, g := range s.Groups {
			if g.Rollout.InFlight() {
				holders = append(holders, name)
			}
		}
		if len(holders) > 1 {
			sort.Strings(holders)
			return fmt.Errorf("at %s more than one group held an unfinished admission: %v", s.At.UTC().Format(time.RFC3339), holders)
		}
	}
	return nil
}

// pacedCheckHeldGroupsKeepPods verifies that in every sample a group whose
// record does not show an admission (none, or Pending) still runs exactly
// the pods and the site Deployments of base. groups limits the check; a
// group that was admitted and completed is past it.
func pacedCheckHeldGroupsKeepPods(base pacedSample, samples []pacedSample, groups []string) error {
	for _, s := range samples {
		for _, name := range groups {
			g, ok := s.Groups[name]
			if !ok {
				continue
			}
			if g.Rollout != nil && g.Rollout.AdmittedTime != nil {
				continue
			}
			if diff := pacedPodsUnchanged(base.Groups[name].Pods, g.Pods); diff != "" {
				return fmt.Errorf("group %s was not admitted (status.rollout: %s) at %s, but its pods changed: %s",
					name, pacedRolloutString(g.Rollout), s.At.UTC().Format(time.RFC3339), diff)
			}
			if diff := pacedWorkloadsUnchanged(base.Groups[name].Workloads, g.Workloads); diff != "" {
				return fmt.Errorf("group %s was not admitted (status.rollout: %s) at %s, but %s",
					name, pacedRolloutString(g.Rollout), s.At.UTC().Format(time.RFC3339), diff)
			}
		}
	}
	return nil
}

// pacedAdmissionOrder returns the groups in the order of their first
// sampled admission. A group that was never seen admitted is an error.
func pacedAdmissionOrder(samples []pacedSample, groups []string) ([]string, error) {
	first := map[string]time.Time{}
	for _, s := range samples {
		for _, name := range groups {
			if r := s.Groups[name].Rollout; r != nil && r.AdmittedTime != nil {
				if _, seen := first[name]; !seen {
					first[name] = s.At
				}
			}
		}
	}
	var order []string
	for _, name := range groups {
		if _, ok := first[name]; !ok {
			return nil, fmt.Errorf("group %s was never seen admitted", name)
		}
		order = append(order, name)
	}
	sort.SliceStable(order, func(i, j int) bool { return first[order[i]].Before(first[order[j]]) })
	return order, nil
}

// pacedCheckSequential verifies, on the final records, that the groups
// rolled one after the other in order: each has an admission and a
// completion, a group was admitted no earlier than its predecessor
// completed, and admittedTime sorts the groups the same way. It also
// verifies on the samples that a group's pods stayed those of base until
// every predecessor showed a completedTime.
func pacedCheckSequential(base pacedSample, samples []pacedSample, final pacedSample, order []string) error {
	for i, name := range order {
		r := final.Groups[name].Rollout
		if r == nil || r.AdmittedTime == nil || r.CompletedTime == nil {
			return fmt.Errorf("group %s did not complete its roll: status.rollout is %s", name, pacedRolloutString(r))
		}
		if r.CompletedTime.Time.Before(r.AdmittedTime.Time) {
			return fmt.Errorf("group %s: completedTime is before admittedTime (%s)", name, pacedRolloutString(r))
		}
		if i == 0 {
			continue
		}
		prev := final.Groups[order[i-1]].Rollout
		if r.AdmittedTime.Time.Before(prev.AdmittedTime.Time) {
			return fmt.Errorf("admission order %v does not match admittedTime: %s was admitted at %s, before %s at %s",
				order, name, r.AdmittedTime.UTC().Format(time.RFC3339), order[i-1], prev.AdmittedTime.UTC().Format(time.RFC3339))
		}
		if r.AdmittedTime.Time.Before(prev.CompletedTime.Time) {
			return fmt.Errorf("group %s was admitted at %s, before %s completed at %s",
				name, r.AdmittedTime.UTC().Format(time.RFC3339), order[i-1], prev.CompletedTime.UTC().Format(time.RFC3339))
		}
	}
	for _, s := range samples {
		for i, name := range order {
			g, ok := s.Groups[name]
			if !ok || pacedPodsUnchanged(base.Groups[name].Pods, g.Pods) == "" {
				continue
			}
			for _, before := range order[:i] {
				if r := s.Groups[before].Rollout; r == nil || r.CompletedTime == nil {
					return fmt.Errorf("at %s the pods of %s had changed (%s) while %s had not completed its roll (status.rollout: %s)",
						s.At.UTC().Format(time.RFC3339), name, pacedPodsUnchanged(base.Groups[name].Pods, g.Pods), before, pacedRolloutString(r))
				}
			}
		}
	}
	return nil
}

// pacedCheckRolled verifies that final shows group fully rolled from base:
// every MySQL pod replaced, every site Deployment written by one renderer
// that is not the one of base, with another spec hash.
func pacedCheckRolled(base, final pacedGroupSample) (renderer string, err error) {
	sites := make([]string, 0, len(base.Workloads))
	for site := range base.Workloads {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	for _, site := range sites {
		b, f := base.Workloads[site], final.Workloads[site]
		if f.RenderedBy == "" {
			return "", fmt.Errorf("group %s site %s: the Deployment has no %s annotation", base.Group, site, v1alpha1.RenderedByAnnotation)
		}
		if f.RenderedBy == b.RenderedBy {
			return "", fmt.Errorf("group %s site %s: the Deployment is still rendered by %q", base.Group, site, b.RenderedBy)
		}
		if renderer == "" {
			renderer = f.RenderedBy
		}
		if f.RenderedBy != renderer {
			return "", fmt.Errorf("group %s: site Deployments carry different renderers (%q and %q)", base.Group, renderer, f.RenderedBy)
		}
		if f.SpecHash == "" || f.SpecHash == b.SpecHash {
			return "", fmt.Errorf("group %s site %s: the spec hash did not change (%q)", base.Group, site, f.SpecHash)
		}
		old := map[string]bool{}
		for _, p := range base.Pods[site] {
			old[p.UID] = true
		}
		now := final.Pods[site]
		if len(now) != 1 {
			return "", fmt.Errorf("group %s site %s: %d pods, want 1", base.Group, site, len(now))
		}
		if old[now[0].UID] {
			return "", fmt.Errorf("group %s site %s: pod %s was not replaced", base.Group, site, now[0].UID)
		}
		if !now[0].Ready {
			return "", fmt.Errorf("group %s site %s: pod %s is not Ready", base.Group, site, now[0].UID)
		}
	}
	return renderer, nil
}

// pacedTimeline renders the samples as one line per change of any group's
// record, pods or site Deployments.
func pacedTimeline(samples []pacedSample, groups []string) []string {
	var lines []string
	last := map[string]string{}
	for _, s := range samples {
		for _, name := range groups {
			g, ok := s.Groups[name]
			if !ok {
				continue
			}
			sites := make([]string, 0, len(g.Workloads))
			for site := range g.Workloads {
				sites = append(sites, site)
			}
			sort.Strings(sites)
			var work []string
			for _, site := range sites {
				w := g.Workloads[site]
				work = append(work, fmt.Sprintf("%s{by=%s sidecar=%s}", site, emptyAsNone(w.RenderedBy), w.SidecarImage))
			}
			state := fmt.Sprintf("active=%s ready=%v rollout=[%s] pods=[%s] deployments=[%s]",
				g.ActiveSite, g.Ready, pacedRolloutString(g.Rollout), pacedPodUIDs(g.Pods), strings.Join(work, " "))
			if last[name] == state {
				continue
			}
			last[name] = state
			lines = append(lines, fmt.Sprintf("%s %s: %s", s.At.UTC().Format("15:04:05"), name, state))
		}
	}
	return lines
}

// pacedDumpForensics writes the auxiliary groups and the sampled timeline
// into the capture directory. The runner's own capture covers the
// playground group only.
func pacedDumpForensics(ctx context.Context, env *runner.Env, groups []string, samples []pacedSample) {
	var b strings.Builder
	for _, name := range groups {
		fg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, name)
		if err != nil {
			fmt.Fprintf(&b, "# %s: %v\n---\n", name, err)
			continue
		}
		fg.ManagedFields = nil
		body, err := sigsyaml.Marshal(fg)
		if err != nil {
			fmt.Fprintf(&b, "# %s: %v\n---\n", name, err)
			continue
		}
		b.Write(body)
		b.WriteString("---\n")
	}
	if err := env.Capture.WriteFile("paced-rollout-groups.yaml", []byte(b.String())); err != nil {
		env.Logger.Warn("write paced-rollout-groups.yaml failed", "err", err)
	}
	timeline := strings.Join(pacedTimeline(samples, groups), "\n") + "\n"
	if err := env.Capture.WriteFile("paced-rollout-timeline.txt", []byte(timeline)); err != nil {
		env.Logger.Warn("write paced-rollout-timeline.txt failed", "err", err)
	}
	if dep, err := env.Kube.GetDeployment(ctx, env.Namespace, pacedOperatorDeployment); err == nil {
		dep.ManagedFields = nil
		if body, err := sigsyaml.Marshal(dep); err == nil {
			_ = env.Capture.WriteFile("operator-deployment.yaml", body)
		}
	}
	// The operator restarts during these scenarios, so the runner's log
	// tail follows a pod that is gone. Keep the log of the one that runs.
	if pods, err := operatorPodNames(ctx, env); err == nil {
		for _, pod := range pods {
			if body, err := env.Kube.PodLogTailLines(ctx, env.Namespace, pod, pacedOperatorContainer, 4000); err == nil {
				_ = env.Capture.WriteFile("operator-current-"+pod+".log", body)
			}
		}
	}
	// The pods of the auxiliary groups: why one is not Ready.
	var pods strings.Builder
	for _, name := range groups {
		list, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/name=mysql,app.kubernetes.io/instance=" + name})
		if err != nil {
			fmt.Fprintf(&pods, "%s: %v\n", name, err)
			continue
		}
		for i := range list.Items {
			p := &list.Items[i]
			fmt.Fprintf(&pods, "%s pod=%s uid=%s node=%s phase=%s reason=%q\n", name, p.Name, p.UID, p.Spec.NodeName, p.Status.Phase, p.Status.Reason)
			for _, cs := range append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...) {
				state := "running"
				switch {
				case cs.State.Waiting != nil:
					state = fmt.Sprintf("waiting(%s: %s)", cs.State.Waiting.Reason, cs.State.Waiting.Message)
				case cs.State.Terminated != nil:
					state = fmt.Sprintf("terminated(%s, exit %d)", cs.State.Terminated.Reason, cs.State.Terminated.ExitCode)
				}
				fmt.Fprintf(&pods, "    %s image=%s ready=%v restarts=%d %s\n", cs.Name, cs.Image, cs.Ready, cs.RestartCount, state)
			}
		}
	}
	_ = env.Capture.WriteFile("paced-rollout-pods.txt", []byte(pods.String()))
}

// pacedWaitRollout polls group until cond holds for its rollout record.
func pacedWaitRollout(ctx context.Context, env *runner.Env, group, what string, timeout time.Duration,
	cond func(fg *v1alpha1.MysqlFailoverGroup) bool) (*v1alpha1.MysqlFailoverGroup, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started := time.Now()
	lastLog := started
	var last string
	for {
		fg, err := env.Kube.GetMFGNamed(waitCtx, env.Namespace, group)
		if err == nil {
			if cond(fg) {
				return fg, nil
			}
			last = fmt.Sprintf("status.rollout: %s; updatePhase=%q active=%s ready=%s",
				pacedRolloutString(fg.Status.Rollout), fg.Status.UpdatePhase, fg.Status.ActiveSite, pgkube.ReadyCondition(fg))
		} else {
			last = err.Error()
		}
		if time.Since(lastLog) >= 10*time.Second {
			lastLog = time.Now()
			env.Logger.Info("wait", "what", what, "group", group, "last", last, "elapsed", time.Since(started).Round(time.Second))
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("timed out after %s waiting for %s on group %s: %s", time.Since(started).Round(time.Second), what, group, last)
		case <-time.After(time.Second):
		}
	}
}
