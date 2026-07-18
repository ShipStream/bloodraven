package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const s40MarkerTable = "bloodraven_reader_e2e.marker"

type s40RunState struct {
	reader         string
	active         string
	donorHost      string
	marker         string
	originalPVC    string
	replacement    string
	originalPod    string
	replacementPod string
}

type s40ContinuousObservation struct {
	mu                     sync.Mutex
	err                    error
	sawClientEndpointEmpty bool
	sawInternalUnreadyPod  bool
}

func init() {
	runner.Register(scenario40ReaderDataLoss())
}

func scenario40ReaderDataLoss() runner.Scenario {
	state := &s40RunState{}
	return runner.Scenario{
		ID:    "40-reader-data-loss-reclone",
		Title: "Read-only reader data loss auto-clones without degrading the group",
		Hypothesis: "Replacing the dedicated reader PVC removes its client endpoint without changing group Ready or activeSite; " +
			"the operator auto-clones from the confirmed active primary and restores direct, lag-bounded replication.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#40-reader-data-loss-and-auto-clone",
		Timeout:           10 * time.Minute,
		ResetBeforeRunAll: true,
		Precheck:          s40Precheck(state),
		Steps: []runner.Step{
			s40SeedAndReplicateMarker(state),
			s40ReplaceReaderDataAndObserve(state),
		},
	}
}

func s40Precheck(state *s40RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s40RunState{}
		if err := AssertHealthyBaseline(ctx, env); err != nil {
			return err
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return err
		}
		for i := range mfg.Spec.Sites {
			if mfg.Spec.Sites[i].IsReadOnlyReader() {
				if state.reader != "" {
					return fmt.Errorf("precheck requires exactly one read-only role site, found at least %q and %q", state.reader, mfg.Spec.Sites[i].Name)
				}
				state.reader = mfg.Spec.Sites[i].Name
			}
		}
		if state.reader == "" {
			return fmt.Errorf("precheck requires exactly one read-only role site")
		}
		state.active = mfg.Status.ActiveSite
		if site := mfg.Spec.SiteByName(state.active); site == nil || !site.IsPromotable() {
			return fmt.Errorf("active site %q is missing or non-promotable", state.active)
		}
		state.donorHost = playgroundInternalSiteHost(env.FG, state.active, env.Namespace)

		readerStatus := statusSiteByName(mfg, state.reader)
		if readerStatus == nil {
			return fmt.Errorf("reader %q missing from status", state.reader)
		}
		if err := assertReaderServingStatus(mfg, readerStatus, state.donorHost); err != nil {
			return fmt.Errorf("reader status precheck: %w", err)
		}
		pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, state.reader)
		if err != nil {
			return err
		}
		node, err := env.Kube.GetNode(ctx, pod.Spec.NodeName)
		if err != nil {
			return fmt.Errorf("get reader node %s: %w", pod.Spec.NodeName, err)
		}
		if got := node.Labels["topology.kubernetes.io/zone"]; got != "zone-reader" {
			return fmt.Errorf("reader pod %s scheduled in zone %q, want zone-reader", pod.Name, got)
		}
		if got := node.Labels["shipstream.io/site.playground"]; got != state.reader {
			return fmt.Errorf("reader pod node %s site label=%q, want %q", node.Name, got, state.reader)
		}
		for _, site := range mfg.Spec.Sites {
			if site.Name == state.reader {
				continue
			}
			peerPod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, site.Name)
			if err == nil && peerPod.Spec.NodeName == pod.Spec.NodeName {
				return fmt.Errorf("reader pod shares node %s with site %s; dedicated reader worker required", pod.Spec.NodeName, site.Name)
			}
		}
		if err := s40AssertServiceTopology(ctx, env, state.reader, pod.Name, true); err != nil {
			return fmt.Errorf("reader service precheck: %w", err)
		}
		return nil
	}
}

func s40SeedAndReplicateMarker(state *s40RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "write a marker on the active primary and confirm it reaches the reader",
		Do: func(ctx context.Context, env *runner.Env) error {
			state.marker = fmt.Sprintf("reader-loss-%d", time.Now().UnixNano())
			if err := seedMarkerRow(ctx, env, state.active, s40MarkerTable, state.marker); err != nil {
				return err
			}
			return waitForMarkerOnSite(ctx, env, state.reader, s40MarkerTable, state.marker, 60*time.Second)
		},
	}
}

func s40ReplaceReaderDataAndObserve(state *s40RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "replace reader PVC; continuously assert Ready and endpoint safety through auto-clone",
		Do: func(ctx context.Context, env *runner.Env) error {
			operatorLogs, err := env.Logs("operator")
			if err != nil {
				return fmt.Errorf("open operator log tailer: %w", err)
			}
			pvcName := pgkube.MysqlPVCName(env.FG, state.reader)
			state.originalPVC, err = env.Kube.PVCUID(ctx, env.Namespace, pvcName)
			if err != nil || state.originalPVC == "" {
				return fmt.Errorf("read original reader PVC UID: uid=%q err=%v", state.originalPVC, err)
			}
			oldPod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, state.reader)
			if err != nil {
				return err
			}
			state.originalPod = string(oldPod.UID)

			observeCtx, stopObserve := context.WithCancel(ctx)
			observation := &s40ContinuousObservation{}
			observeDone := make(chan struct{})
			go func() {
				defer close(observeDone)
				s40ObserveContinuously(observeCtx, env, state, observation)
			}()
			stopAndCheck := func() error {
				stopObserve()
				<-observeDone
				return observation.result()
			}

			if err := env.Chaos.WipeSiteData(ctx, state.reader, env.FG); err != nil {
				_ = stopAndCheck()
				return fmt.Errorf("wipe reader data: %w", err)
			}
			state.replacement, err = env.Kube.PVCUID(ctx, env.Namespace, pvcName)
			if err != nil || state.replacement == "" || state.replacement == state.originalPVC {
				_ = stopAndCheck()
				return fmt.Errorf("reader PVC was not replaced: original=%q replacement=%q err=%v", state.originalPVC, state.replacement, err)
			}
			if err := env.Chaos.Revert(ctx); err != nil {
				_ = stopAndCheck()
				return fmt.Errorf("restore reader deployment scale: %w", err)
			}

			logCtx, cancelLog := context.WithTimeout(ctx, 3*time.Minute)
			_, err = env.Wait.UntilLog(logCtx, operatorLogs, env.StartTime,
				"reader auto-clone starts from the captured active primary internal Service",
				pglogs.Structured("starting bootstrap", map[string]string{
					"source":    "auto-clone",
					"donor":     state.active,
					"recipient": state.reader,
					"donorHost": state.donorHost,
				}),
			)
			cancelLog()
			if err != nil {
				_ = stopAndCheck()
				return err
			}

			waitCtx, cancelWait := context.WithTimeout(ctx, 6*time.Minute)
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "reader returns with direct healthy replication",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.ActiveSite != state.active {
						return false, fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite), fmt.Errorf("active site changed during reader loss: %q -> %q", state.active, mfg.Status.ActiveSite)
					}
					status := statusSiteByName(mfg, state.reader)
					if status == nil {
						return false, "reader status missing", nil
					}
					err := assertReaderServingStatus(mfg, status, state.donorHost)
					return err == nil, fmt.Sprintf("state=%s replicating=%v source=%q convergence=%s lag=%v", status.State, status.Replicating, status.SourceHost, status.SourceConvergenceState, formatLag(status.SecondsBehindSource)), nil
				})
			cancelWait()
			if err != nil {
				_ = stopAndCheck()
				return err
			}
			if mfg.Status.ActiveSite != state.active {
				_ = stopAndCheck()
				return fmt.Errorf("active site changed during reader recovery: %q -> %q", state.active, mfg.Status.ActiveSite)
			}

			if err := s40WaitForLiveReader(ctx, env, state, 90*time.Second); err != nil {
				_ = stopAndCheck()
				return err
			}
			newPod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, state.reader)
			if err != nil {
				_ = stopAndCheck()
				return err
			}
			state.replacementPod = string(newPod.UID)
			if state.replacementPod == state.originalPod {
				_ = stopAndCheck()
				return fmt.Errorf("reader pod UID did not change after PVC replacement: %s", state.originalPod)
			}
			if err := s40WaitForServiceTopology(ctx, env, state.reader, newPod.Name, 60*time.Second); err != nil {
				_ = stopAndCheck()
				return err
			}
			if err := stopAndCheck(); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("reader recovered: active=%s source=%s pvc=%s->%s pod=%s->%s", state.active, state.donorHost, state.originalPVC, state.replacement, state.originalPod, state.replacementPod))
			return nil
		},
	}
}

func s40ObserveContinuously(ctx context.Context, env *runner.Env, state *s40RunState, observation *s40ContinuousObservation) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s40ObserveOnce(ctx, env, state, observation)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func s40ObserveOnce(ctx context.Context, env *runner.Env, state *s40RunState, observation *s40ContinuousObservation) {
	mfg, mfgErr := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	s40ObserveReadError(ctx, observation, "read MFG", mfgErr)
	readerServingHealthy := false
	if mfgErr == nil {
		if pgkube.ReadyCondition(mfg) != "True" {
			observation.fail(fmt.Errorf("group Ready changed to %q while reader was unavailable/recovering", pgkube.ReadyCondition(mfg)))
		}
		if mfg.Status.ActiveSite != state.active {
			observation.fail(fmt.Errorf("active site changed while reader was unavailable/recovering: %q -> %q", state.active, mfg.Status.ActiveSite))
		}
		if status := statusSiteByName(mfg, state.reader); status != nil {
			readerServingHealthy = assertReaderServingStatus(mfg, status, state.donorHost) == nil
		}
	}

	clientState, clientErr := env.Kube.ServiceEndpointState(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, state.reader))
	s40ObserveReadError(ctx, observation, "read reader client EndpointSlices", clientErr)
	if clientErr == nil {
		ready := clientState.ReadyPodNames("mysql")
		if len(ready) == 0 {
			// Empty EndpointSlice results are expected while scale-to-zero or
			// endpoint turnover is in progress. The successful list, rather than
			// an ignored NotFound/read error, is the explicit absence signal.
			observation.markClientEmpty()
		} else if mfgErr == nil && !readerServingHealthy {
			observation.fail(fmt.Errorf("reader client Service published ready pods %v while reader status was unhealthy", ready))
		}
	}

	pods, podErr := env.Kube.ListMysqlPods(ctx, env.Namespace, env.FG, state.reader)
	s40ObserveReadError(ctx, observation, "list reader pods", podErr)
	if podErr != nil {
		return
	}
	// A successful empty pod list is the expected scale-to-zero transition.
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) == state.originalPod || s40PodReady(pod) {
			continue
		}
		if clientErr == nil && len(clientState.ReadyPodNames("mysql")) != 0 {
			observation.fail(fmt.Errorf("reader client Service published a ready endpoint while replacement pod %s was not ready", pod.Name))
		}
		internal, internalErr := env.Kube.ServiceEndpointState(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, state.reader)+"-internal")
		s40ObserveReadError(ctx, observation, "read reader internal EndpointSlices", internalErr)
		if internalErr != nil {
			continue
		}
		for _, endpoint := range internal.Endpoints {
			if endpoint.PodName == pod.Name {
				observation.markInternalUnready()
			}
		}
	}
}

func s40ObserveReadError(ctx context.Context, observation *s40ContinuousObservation, operation string, err error) {
	if err == nil {
		return
	}
	// Shutdown may cancel an in-flight API call after the scenario has
	// explicitly stopped the observer. A context-shaped API error while the
	// observer context is still live remains a real observation failure.
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return
	}
	observation.fail(fmt.Errorf("continuous observer %s: %w", operation, err))
}

func (o *s40ContinuousObservation) fail(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err == nil {
		o.err = err
	}
}

func (o *s40ContinuousObservation) markClientEmpty() {
	o.mu.Lock()
	o.sawClientEndpointEmpty = true
	o.mu.Unlock()
}

func (o *s40ContinuousObservation) markInternalUnready() {
	o.mu.Lock()
	o.sawInternalUnreadyPod = true
	o.mu.Unlock()
}

func (o *s40ContinuousObservation) result() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	if !o.sawClientEndpointEmpty {
		return fmt.Errorf("reader client Service was never observed without a serving endpoint during recovery")
	}
	if !o.sawInternalUnreadyPod {
		return fmt.Errorf("reader internal Service never published the replacement pod while it was unready")
	}
	return nil
}

func s40WaitForLiveReader(ctx context.Context, env *runner.Env, state *s40RunState, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, state.reader, env.Creds)
		if err == nil {
			rs, statusErr := client.ShowReplicaStatus(ctx)
			markerCount, markerErr := client.ScalarInt(ctx, "SELECT COUNT(*) FROM "+s40MarkerTable+" WHERE marker=?", state.marker)
			_ = client.Close()
			last = fmt.Sprintf("configured=%v io=%v sql=%v source=%q lag=%v marker=%d statusErr=%v markerErr=%v", rs.Configured, rs.IORunning, rs.SQLRunning, rs.SourceHost, formatLag(rs.SecondsBehindSrc), markerCount, statusErr, markerErr)
			if statusErr == nil && markerErr == nil && rs.Configured && rs.IORunning && rs.SQLRunning && canonicalMySQLHost(rs.SourceHost) == canonicalMySQLHost(state.donorHost) && rs.SecondsBehindSrc != nil && markerCount == 1 {
				return nil
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("reader live replication did not converge within %s (last: %s)", timeout, last)
}

func s40AssertServiceTopology(ctx context.Context, env *runner.Env, reader, expectedPod string, requireClientEndpoint bool) error {
	clientName := pgkube.MysqlDeploymentName(env.FG, reader)
	clientService, err := env.Kube.Kubernetes.CoreV1().Services(env.Namespace).Get(ctx, clientName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get client Service: %w", err)
	}
	if len(clientService.Spec.Ports) != 1 || clientService.Spec.Ports[0].Name != "mysql" || clientService.Spec.Ports[0].Port != 3306 {
		return fmt.Errorf("client Service ports=%v, want only mysql:3306", clientService.Spec.Ports)
	}
	clientEndpoints, err := env.Kube.ServiceEndpointState(ctx, env.Namespace, clientName)
	if err != nil {
		return err
	}
	if _, exposed := clientEndpoints.Ports["sidecar"]; exposed {
		return fmt.Errorf("client Service EndpointSlice exposes sidecar port")
	}
	if requireClientEndpoint {
		ready := clientEndpoints.ReadyPodNames("mysql")
		if len(ready) != 1 || ready[0] != expectedPod {
			return fmt.Errorf("client Service ready pods=%v, want exactly %s", ready, expectedPod)
		}
	}

	internalName := clientName + "-internal"
	internalService, err := env.Kube.Kubernetes.CoreV1().Services(env.Namespace).Get(ctx, internalName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get internal Service: %w", err)
	}
	if internalService.Spec.Type != corev1.ServiceTypeClusterIP || !internalService.Spec.PublishNotReadyAddresses {
		return fmt.Errorf("internal Service type=%s publishNotReadyAddresses=%v, want ClusterIP/true", internalService.Spec.Type, internalService.Spec.PublishNotReadyAddresses)
	}
	internalPorts := map[string]int32{}
	for _, port := range internalService.Spec.Ports {
		internalPorts[port.Name] = port.Port
	}
	if internalPorts["mysql"] != 3306 || internalPorts["sidecar"] != 8080 || len(internalPorts) != 2 {
		return fmt.Errorf("internal Service ports=%v, want mysql:3306 and sidecar:8080", internalPorts)
	}
	if _, gated := internalService.Spec.Selector["shipstream.io/healthy"]; gated {
		return fmt.Errorf("internal Service must not select shipstream.io/healthy")
	}
	return nil
}

func s40WaitForServiceTopology(ctx context.Context, env *runner.Env, reader, expectedPod string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if err := s40AssertServiceTopology(ctx, env, reader, expectedPod, true); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("reader Service topology did not converge within %s: %w", timeout, last)
}

func s40PodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
