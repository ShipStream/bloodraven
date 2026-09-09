package scenarios

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed deploy_client.py
var deployClientScript string

type deployScenarioState struct {
	name, operation string
	podCreated      bool
	objects         []client.Object
	grant           deployClientRecord
	expiry          time.Time
	leaseTTL        time.Duration
}

type deployClientRecord struct {
	At     time.Time `json:"at"`
	Action string    `json:"action"`
	Kind   string    `json:"kind"`
	Status int       `json:"status"`
	Body   struct {
		ExpiresAt          time.Time `json:"expiresAt"`
		TopologyGeneration int64     `json:"topologyGeneration"`
		Error              string    `json:"error"`
		Reason             string    `json:"reason"`
	} `json:"body"`
}

func init() {
	for _, mode := range []string{"expiry", "crash", "emergency", "renew"} {
		runner.Register(deploymentCoordinationScenario(mode))
	}
}

func deploymentCoordinationScenario(mode string) runner.Scenario {
	ids := map[string]string{
		"expiry": "53-deployment-hold-expiry", "crash": "54-deployment-hold-owner-kill",
		"emergency": "55-deployment-emergency-revocation", "renew": "56-deployment-live-renewal-fenced",
	}
	hypotheses := map[string]string{
		"expiry":    "An active failover-hold defers planned failover with DeploymentHold; expiry alone permits completion.",
		"crash":     "Killing a renewing hold owner does not strand planned failover: the durable hold expires and the plan completes.",
		"emergency": "Emergency primary loss bypasses live migration and hold leases, revokes both, and emits DeploymentLeaseRevoked.",
		"renew":     "A live migration client renewing every five seconds sees 409 revoked after a planned topology change and stops.",
	}
	state := &deployScenarioState{}
	s := runner.Scenario{
		ID: ids[mode], Title: ids[mode], Hypothesis: hypotheses[mode], Risk: "medium",
		DocLink: "playground/chaos-scenarios.md#deployment-coordination", Timeout: 8 * time.Minute,
		Precheck: func(ctx context.Context, env *runner.Env) error {
			*state = deployScenarioState{}
			if err := AssertHealthyBaseline(ctx, env); err != nil {
				return err
			}
			dep, err := env.Kube.GetDeployment(ctx, env.Namespace, "bloodraven")
			if err != nil {
				return err
			}
			enabled := false
			for _, container := range dep.Spec.Template.Spec.Containers {
				for _, variable := range container.Env {
					if variable.Name == "BLOODRAVEN_DEPLOY_API_ENABLED" && variable.Value == "true" {
						enabled = true
					}
				}
			}
			if !enabled {
				return fmt.Errorf("deploy API not enabled; prepare with BLOODRAVEN_SETUP_DEPLOY_API=1 ./playground/setup.sh")
			}
			_, err = env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, "mysql-playground-tls", metav1.GetOptions{})
			return err
		},
		Cleanup: state.cleanup,
		Steps: []runner.Step{{Phase: runner.PhaseInject, Name: "create authorized projected-token TLS client", Do: func(ctx context.Context, env *runner.Env) error {
			return state.create(ctx, env, mode)
		}}},
	}
	if mode == "emergency" {
		s.Steps = append(s.Steps, injectScaleZero(), observeFailover(), verifyFailoverMetric())
	} else {
		s.Steps = append(s.Steps, injectPlannedFailoverAnnotation())
		if mode != "renew" {
			s.Steps = append(s.Steps, runner.Step{Phase: runner.PhaseVerify, Name: "DeploymentHold persists until expiry", Do: func(ctx context.Context, env *runner.Env) error {
				return state.observeHold(ctx, env, mode == "crash")
			}})
		}
		s.Steps = append(s.Steps, observePlannedFailoverSucceeded(), verifyTransactionsLostZero())
	}
	s.Steps = append(s.Steps, runner.Step{Phase: runner.PhaseVerify, Name: "verify topology and lease fencing", Do: func(ctx context.Context, env *runner.Env) error {
		if mode == "emergency" || mode == "renew" {
			if err := state.waitClient(ctx, env, func(records []deployClientRecord) bool {
				wanted := 1
				if mode == "emergency" {
					wanted = 2
				}
				revoked := map[string]bool{}
				expires := map[string]time.Time{}
				stopped := false
				for _, r := range records {
					if r.Status == 200 || r.Status == 201 {
						expires[r.Kind] = r.Body.ExpiresAt
					}
					if r.Action == "renew" && r.Status == 409 && r.Body.Error == "revoked" &&
						!r.At.IsZero() && r.At.Before(expires[r.Kind]) &&
						(r.Body.Reason == "topology_changed" || r.Body.Reason == "operator_revoked") &&
						r.Body.TopologyGeneration > state.grant.Body.TopologyGeneration {
						revoked[r.Kind] = true
					}
					stopped = stopped || r.Action == "stopped"
				}
				return len(revoked) == wanted && stopped
			}); err != nil {
				return err
			}
		}
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if mode == "emergency" || mode == "renew" {
			if _, err := waitForMFGEvent(waitCtx, env, env.StartTime, "DeploymentLeaseRevoked", ""); err != nil {
				return err
			}
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return err
		}
		if mfg.Status.TopologyGeneration <= state.grant.Body.TopologyGeneration {
			return fmt.Errorf("topologyGeneration=%d must exceed grant generation=%d", mfg.Status.TopologyGeneration, state.grant.Body.TopologyGeneration)
		}
		return nil
	}})
	return s
}

func (s *deployScenarioState) create(ctx context.Context, env *runner.Env, mode string) error {
	s.leaseTTL = 30 * time.Second
	if mode == "emergency" {
		s.leaseTTL = 120 * time.Second
	}
	s.operation = uuid.NewString()
	s.name = "chaos-deploy-" + s.operation[:8]
	meta := metav1.ObjectMeta{Name: s.name, Namespace: env.Namespace}
	ca, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, "mysql-playground-tls", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if len(ca.Data["ca.crt"]) == 0 {
		return fmt.Errorf("playground TLS Secret has no ca.crt")
	}
	mdb := &v1alpha1.MysqlDatabase{ObjectMeta: meta, Spec: v1alpha1.MysqlDatabaseSpec{
		GroupRef: v1alpha1.LocalGroupRef{Name: env.FG}, DatabaseName: strings.ReplaceAll(s.name, "-", "_"),
		Owner: v1alpha1.MysqlDatabaseOwner{SecretName: s.name}, DeletionPolicy: v1alpha1.MysqlDatabaseDelete,
		DeploymentClients: []v1alpha1.DeploymentClient{{Namespace: env.Namespace, ServiceAccount: s.name}},
	}}
	for _, obj := range []client.Object{
		&corev1.ServiceAccount{ObjectMeta: meta},
		&corev1.Secret{ObjectMeta: meta, Data: map[string][]byte{"username": []byte(strings.ReplaceAll(s.name, "-", "_")), "password": []byte(uuid.NewString())}},
		&corev1.ConfigMap{ObjectMeta: meta, Data: map[string]string{"ca.crt": string(ca.Data["ca.crt"])}}, mdb,
	} {
		if err := env.Kube.Controller.Create(ctx, obj); err != nil {
			return err
		}
		s.objects = append(s.objects, obj)
	}
	// Wait for actual reconciliation before granting access or changing topology.
	if err := deployPoll(ctx, 2*time.Minute, func() (bool, error) {
		var db v1alpha1.MysqlDatabase
		if err := env.Kube.Controller.Get(ctx, client.ObjectKeyFromObject(mdb), &db); err != nil {
			return false, err
		}
		if db.Status.Phase == v1alpha1.MysqlDatabasePhaseFailed {
			return false, fmt.Errorf("deployment fixture failed: %s", db.Status.Message)
		}
		return db.Status.Phase == v1alpha1.MysqlDatabasePhaseReady && db.Status.ObservedGeneration == db.Generation, nil
	}); err != nil {
		return err
	}
	pod := deploymentClientPod(meta, s.operation, env.FG, mode)
	if err := env.Kube.Controller.Create(ctx, pod); err != nil {
		return err
	}
	s.podCreated = true
	return s.waitClient(ctx, env, func(records []deployClientRecord) bool {
		granted, renewed := map[string]bool{}, map[string]bool{}
		for _, r := range records {
			if r.Action == "grant" && r.Status == 201 {
				granted[r.Kind] = r.Body.ExpiresAt.After(r.At) && !r.At.IsZero()
				if r.Kind == "migration" {
					s.grant = r
				}
			}
			if r.Action == "renew" && r.Status == 200 {
				renewed[r.Kind] = true
			}
			if r.Kind == "failover-hold" && (r.Status == 200 || r.Status == 201) {
				s.expiry = r.Body.ExpiresAt
			}
		}
		if mode == "renew" {
			return granted["migration"] && renewed["migration"]
		}
		return granted["migration"] && granted["failover-hold"] && (mode == "expiry" || (renewed["migration"] && renewed["failover-hold"]))
	})
}

func deploymentClientPod(meta metav1.ObjectMeta, operation, group, mode string) *corev1.Pod {
	no, seconds, deadline := false, int64(3600), int64(600)
	meta.Labels = map[string]string{"bloodraven.shipstream.io/deploy-client": "true"}
	return &corev1.Pod{ObjectMeta: meta, Spec: corev1.PodSpec{
		ServiceAccountName: meta.Name, AutomountServiceAccountToken: &no, RestartPolicy: corev1.RestartPolicyNever,
		ActiveDeadlineSeconds: &deadline,
		Tolerations:           []corev1.Toleration{{Key: "shipstream.io/db-readonly-" + group, Operator: corev1.TolerationOpExists}, {Key: "shipstream.io/db-readonly", Operator: corev1.TolerationOpExists}},
		Containers: []corev1.Container{{Name: "client", Image: "python:3.13-alpine", Command: []string{"python", "-u", "-c", deployClientScript},
			Env: []corev1.EnvVar{{Name: "DEPLOY_URL", Value: "https://bloodraven." + meta.Namespace + ".svc.cluster.local:8443/deploy/v1/groups/" + group},
				{Name: "OPERATION_ID", Value: operation}, {Name: "INSTANCE", Value: meta.Name}, {Name: "MODE", Value: mode}},
			VolumeMounts: []corev1.VolumeMount{{Name: "token", MountPath: "/deploy-token", ReadOnly: true}, {Name: "ca", MountPath: "/deploy-ca", ReadOnly: true}},
		}},
		Volumes: []corev1.Volume{
			{Name: "token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: "bloodraven-deploy", ExpirationSeconds: &seconds, Path: "token"}}}}}},
			{Name: "ca", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: meta.Name}}}},
		},
	}}
}

func decodeDeployClientRecords(data []byte) ([]deployClientRecord, error) {
	var records []deployClientRecord
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var record deployClientRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("invalid deploy client log (inspect client Pod logs): %w", err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *deployScenarioState) waitClient(ctx context.Context, env *runner.Env, predicate func([]deployClientRecord) bool) error {
	return deployPoll(ctx, 2*time.Minute, func() (bool, error) {
		pod, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).Get(ctx, s.name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodPending {
			return false, nil
		}
		data, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).GetLogs(s.name, &corev1.PodLogOptions{Container: "client"}).DoRaw(ctx)
		if err != nil {
			return false, err
		}
		records, err := decodeDeployClientRecords(data)
		if err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("deploy client Pod %s failed; records=%+v", s.name, records)
		}
		if predicate(records) {
			env.Capture.Note(fmt.Sprintf("deploy client %s: %s", s.name, data))
			return true, nil
		}
		if pod.Status.Phase == corev1.PodSucceeded {
			return false, fmt.Errorf("deploy client exited without required evidence; records=%+v", records)
		}
		return false, nil
	})
}

func (s *deployScenarioState) observeHold(ctx context.Context, env *runner.Env, kill bool) error {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "Deferred/DeploymentHold", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		pf := mfg.Status.PlannedFailover
		if pf == nil || pf.StartTime == nil || pf.StartTime.Time.Before(env.StartTime.Add(-2*time.Second)) {
			return false, "waiting for current planned request", nil
		}
		if pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Reason != "DeploymentHold" {
			return false, fmt.Sprintf("phase=%s reason=%s", pf.Phase, pf.Reason), nil
		}
		if pf.RetryAfter == nil || !strings.Contains(pf.Message, s.operation) || !strings.Contains(pf.Message, s.name) {
			return false, "", fmt.Errorf("DeploymentHold missing retryAfter, operationId, or instance: %+v", pf)
		}
		if !kill && pf.RetryAfter.Time.Sub(s.expiry).Abs() > time.Second {
			return false, "", fmt.Errorf("retryAfter=%s does not match hold expiry=%s", pf.RetryAfter, s.expiry)
		}
		return true, "hold deferred with owner and expiry", nil
	})
	if err != nil {
		return err
	}
	if kill {
		// Refresh evidence immediately before killing: a heartbeat may have run
		// while the operator was first publishing Deferred.
		if err := s.waitClient(ctx, env, func(records []deployClientRecord) bool {
			for _, r := range records {
				if r.Action == "renew" && r.Kind == "failover-hold" && r.Status == 200 {
					s.expiry = r.Body.ExpiresAt
				}
			}
			return s.expiry.After(time.Now().Add(10 * time.Second))
		}); err != nil {
			return err
		}
		// No release request or signal handler: terminate the real renewal owner.
		zero := int64(0)
		if err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).Delete(ctx, s.name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			return err
		}
		env.Capture.Note("killed deployment lease owner without releasing leases")
	}
	if !s.expiry.After(time.Now().Add(5 * time.Second)) {
		return fmt.Errorf("hold expiry %s leaves no meaningful deferral observation window", s.expiry)
	}
	// The last observed grant/renewal must keep the plan deferred until expiry.
	return deployPoll(ctx, 45*time.Second, func() (bool, error) {
		if !time.Now().Before(s.expiry) {
			return true, nil
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return false, err
		}
		pf := mfg.Status.PlannedFailover
		if mfg.Status.ActiveSite != ctxFetch(env, "originalPrimary") || pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Reason != "DeploymentHold" {
			return false, fmt.Errorf("planned failover advanced before hold expiry %s", s.expiry)
		}
		return false, nil
	})
}

func (s *deployScenarioState) cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	if s.podCreated {
		zero := int64(0)
		err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).Delete(ctx, s.name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err)
		}
		// A crash intentionally leaves durable leases; let the last TTL elapse
		// before removing authorization. Never delete another client's leases.
		select {
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
		case <-time.After(s.leaseTTL + time.Second):
		}
	}
	for i := len(s.objects) - 1; i >= 0; i-- {
		obj := s.objects[i]
		if err := env.Kube.Controller.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err)
			if _, ok := obj.(*v1alpha1.MysqlDatabase); ok {
				return errors.Join(errs...)
			}
			continue
		}
		if _, ok := obj.(*v1alpha1.MysqlDatabase); ok {
			if err := deployPoll(ctx, 2*time.Minute, func() (bool, error) {
				live := obj.DeepCopyObject().(client.Object)
				err := env.Kube.Controller.Get(ctx, client.ObjectKeyFromObject(obj), live)
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}); err != nil {
				// Keep the owner Secret available to a pending database finalizer.
				return errors.Join(append(errs, err)...)
			}
		}
	}
	return errors.Join(errs...)
}

func deployPoll(ctx context.Context, timeout time.Duration, check func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if done, err := check(); done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("deployment scenario wait exceeded %s: %w", timeout, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
