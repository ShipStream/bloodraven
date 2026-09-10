package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	authv1 "k8s.io/api/authentication/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	authclient "k8s.io/client-go/kubernetes/typed/authentication/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type deployIdentity struct {
	namespace, serviceAccount string
	expires                   time.Time
}

type deployAPI struct {
	reader   client.Reader
	reviews  authclient.TokenReviewInterface
	leases   *controller.DeploymentLeaseManager
	leader   func() bool
	logger   *slog.Logger
	audience string
	now      func() time.Time
	mu       sync.Mutex
	tokens   map[[32]byte]deployIdentity
}

func newDeployAPI(reader client.Reader, reviews authclient.TokenReviewInterface, leases *controller.DeploymentLeaseManager, leader func() bool, logger *slog.Logger, audience string) *deployAPI {
	if audience == "" {
		audience = "bloodraven-deploy"
	}
	return &deployAPI{reader: reader, reviews: reviews, leases: leases, leader: leader, logger: logger, audience: audience, now: time.Now, tokens: make(map[[32]byte]deployIdentity)}
}

func (h *deployAPI) authenticate(ctx context.Context, header string) (deployIdentity, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) > 16384 {
		return deployIdentity{}, false
	}
	hash := sha256.Sum256([]byte(parts[1]))
	h.mu.Lock()
	identity, ok := h.tokens[hash]
	h.mu.Unlock()
	if ok && h.now().Before(identity.expires) {
		return identity, true
	}
	review, err := h.reviews.Create(ctx, &authv1.TokenReview{Spec: authv1.TokenReviewSpec{Token: parts[1], Audiences: []string{h.audience}}}, metav1.CreateOptions{})
	if err != nil || !review.Status.Authenticated || review.Status.Error != "" || !slices.Contains(review.Status.Audiences, h.audience) {
		return deployIdentity{}, false
	}
	name := strings.Split(review.Status.User.Username, ":")
	if len(name) != 4 || name[0] != "system" || name[1] != "serviceaccount" || len(validation.IsDNS1123Label(name[2])) != 0 || len(validation.IsDNS1123Subdomain(name[3])) != 0 {
		return deployIdentity{}, false
	}
	identity = deployIdentity{namespace: name[2], serviceAccount: name[3], expires: h.now().Add(15 * time.Second)}
	h.mu.Lock()
	for key, entry := range h.tokens {
		if !h.now().Before(entry.expires) {
			delete(h.tokens, key)
		}
	}
	// Bound retained credentials without letting untrusted tokens evict reviews.
	if len(h.tokens) < 1024 {
		h.tokens[hash] = identity
	}
	h.mu.Unlock()
	return identity, true
}

func deployJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	if value != nil {
		_ = json.NewEncoder(w).Encode(value)
	}
}

func deployError(w http.ResponseWriter, err error) {
	var leaseErr *controller.DeploymentLeaseError
	if errors.As(err, &leaseErr) {
		if leaseErr.Code == "held" && leaseErr.Holder != nil {
			deployJSON(w, leaseErr.Status, map[string]any{"error": "held", "holder": map[string]any{"operationId": leaseErr.Holder.OperationID, "instance": leaseErr.Holder.Instance, "expiresAt": leaseErr.Holder.ExpiresAt}})
			return
		}
		deployJSON(w, leaseErr.Status, leaseErr)
		return
	}
	deployJSON(w, 503, map[string]string{"error": "unavailable"})
}

func deployDecode(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(value)
	if err == nil {
		var extra any
		err = dec.Decode(&extra)
		if err == io.EOF {
			return true
		}
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		deployJSON(w, 413, map[string]string{"error": "request_too_large"})
	} else {
		deployJSON(w, 400, map[string]string{"error": "invalid_request"})
	}
	return false
}

func (h *deployAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := h.now()
	sw := &statusRecorder{ResponseWriter: w, status: 200}
	w = sw
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	handler, group, instance, namespace, operationID := "notfound", "", "", "", ""
	defer func() {
		duration := h.now().Sub(start)
		h.logger.Info("deploy api", "handler", handler, "group", group, "instance", instance, "namespace", namespace, "operationId", operationID, "status", sw.status, "duration_ms", duration.Milliseconds())
		metrics.HTTPRequestsTotal.WithLabelValues("deploy", handler, auxMetricMethod(r.Method), metrics.StatusClass(sw.status)).Inc()
		metrics.HTTPRequestDurationSeconds.WithLabelValues("deploy", handler, auxMetricMethod(r.Method)).Observe(duration.Seconds())
	}()
	if !h.leader() {
		deployJSON(w, 503, map[string]string{"error": "not_leader"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	identity, ok := h.authenticate(ctx, r.Header.Get("Authorization"))
	if !ok {
		deployJSON(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	path := strings.Split(strings.TrimPrefix(r.URL.Path, "/deploy/v1/"), "/")
	if len(path) < 2 || path[0] != "groups" || len(validation.IsDNS1123Subdomain(path[1])) != 0 {
		deployJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	group = path[1]
	kind := ""
	switch {
	case len(path) == 2:
		handler = "group"
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			deployJSON(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
	case len(path) == 3 && path[2] == "leases":
		handler = "grant"
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			deployJSON(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
	case len(path) == 5 && path[2] == "leases":
		kind, operationID = path[3], path[4]
		handler = "renew"
		if r.Method == http.MethodDelete {
			handler = "release"
		} else if r.Method != http.MethodPut {
			w.Header().Set("Allow", "PUT, DELETE")
			deployJSON(w, 405, map[string]string{"error": "method_not_allowed"})
			return
		}
		if (kind != "migration" && kind != "failover-hold") || len(operationID) == 0 || len(operationID) > 128 {
			deployJSON(w, 400, map[string]string{"error": "invalid_request"})
			return
		}
	default:
		deployJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	var req struct {
		Kind        string `json:"kind"`
		OperationID string `json:"operationId"`
		Instance    string `json:"instance"`
		TTLSeconds  int    `json:"ttlSeconds"`
	}
	if handler == "grant" {
		if !deployDecode(w, r, &req) {
			return
		}
		kind, operationID, instance = req.Kind, req.OperationID, req.Instance
		if (kind != "migration" && kind != "failover-hold") || len(operationID) == 0 || len(operationID) > 128 || len(validation.IsDNS1123Subdomain(instance)) != 0 {
			deployJSON(w, 400, map[string]string{"error": "invalid_request"})
			return
		}
	}
	// Resolve the namespace through authorization, never through group existence.
	var databases v1alpha1.MysqlDatabaseList
	if err := h.reader.List(ctx, &databases); err != nil {
		deployError(w, err)
		return
	}
	groupNamespace := ""
	instances := []string{}
	for _, db := range databases.Items {
		if !db.DeletionTimestamp.IsZero() || db.Spec.GroupRef.Name != group || (instance != "" && db.Name != instance) {
			continue
		}
		for _, caller := range db.Spec.DeploymentClients {
			if caller.Namespace != identity.namespace || caller.ServiceAccount != identity.serviceAccount {
				continue
			}
			if groupNamespace != "" && groupNamespace != db.Namespace {
				deployJSON(w, 403, map[string]string{"error": "forbidden"})
				return
			}
			groupNamespace = db.Namespace
			instances = append(instances, db.Name)
		}
	}
	if len(instances) == 0 {
		if handler == "grant" {
			metrics.DeployLeaseGrantsTotal.WithLabelValues("", kind, "unauthorized").Inc()
		}
		deployJSON(w, 403, map[string]string{"error": "forbidden"})
		return
	}
	if len(instances) == 1 {
		instance = instances[0]
	}
	namespace = groupNamespace
	fg := &v1alpha1.MysqlFailoverGroup{}
	if err := h.reader.Get(ctx, types.NamespacedName{Namespace: groupNamespace, Name: group}, fg); err != nil {
		deployError(w, err)
		return
	}
	if !h.leader() {
		deployJSON(w, 503, map[string]string{"error": "not_leader"})
		return
	}
	switch handler {
	case "group":
		fresh, leases, err := h.leases.Snapshot(ctx, fg)
		if err != nil {
			deployError(w, err)
			return
		}
		deployJSON(w, 200, deployGroupResponse(fresh, leases))
	case "grant":
		result, err := h.leases.Grant(ctx, fg, kind, operationID, instance, identity.namespace, identity.serviceAccount, req.TTLSeconds)
		if err != nil {
			deployError(w, err)
			return
		}
		deployJSON(w, result.Status, map[string]any{"kind": result.Kind, "operationId": result.OperationID, "token": result.Token, "expiresAt": result.ExpiresAt, "topologyGeneration": result.TopologyGeneration})
	case "renew", "release":
		var renewal struct {
			Token      string `json:"token"`
			TTLSeconds int    `json:"ttlSeconds"`
		}
		if handler == "renew" {
			if !deployDecode(w, r, &renewal) {
				return
			}
		} else {
			renewal.Token = r.Header.Get("X-Lease-Token")
		}
		var err error
		var result controller.DeploymentLeaseResult
		for _, candidate := range instances {
			if handler == "renew" {
				result, err = h.leases.Renew(ctx, fg, kind, operationID, candidate, identity.namespace, identity.serviceAccount, renewal.Token, renewal.TTLSeconds)
			} else {
				err = h.leases.Release(ctx, fg, kind, operationID, candidate, identity.namespace, identity.serviceAccount, renewal.Token)
			}
			var leaseErr *controller.DeploymentLeaseError
			if !errors.As(err, &leaseErr) || leaseErr.Code != "token_mismatch" {
				instance = candidate
				break
			}
		}
		if err != nil {
			deployError(w, err)
			return
		}
		if handler == "release" {
			deployJSON(w, 204, nil)
		} else {
			deployJSON(w, 200, map[string]any{"expiresAt": result.ExpiresAt, "topologyGeneration": result.TopologyGeneration})
		}
	}
}

func deployGroupResponse(fg *v1alpha1.MysqlFailoverGroup, leases []controller.DeploymentLeaseView) map[string]any {
	reason := controller.DeploymentUnstableReason(fg)
	ready := apimeta.IsStatusConditionTrue(fg.Status.Conditions, "Ready")
	degraded := ""
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, "Degraded"); c != nil && c.Status == metav1.ConditionTrue {
		degraded = c.Reason
	}
	replicating, recovery, lag := true, apimeta.IsStatusConditionTrue(fg.Status.Conditions, "RecoveryPending"), int64(0)
	for _, s := range fg.Status.Sites {
		if s.Name != fg.Status.ActiveSite {
			replicating = replicating && s.Replicating
			if s.SecondsBehindSource != nil {
				lag = max(lag, *s.SecondsBehindSource)
			}
		}
		recovery = recovery || s.RecoveryState != "" || s.SourceConvergenceState == v1alpha1.SourceConvergencePending || s.SourceConvergenceState == v1alpha1.SourceConvergenceBlocked
	}
	if len(fg.Status.Sites) < len(fg.Spec.Sites) {
		replicating = false
	}
	planned := map[string]string{"phase": "", "reason": ""}
	if p := fg.Status.PlannedFailover; p != nil {
		planned["phase"], planned["reason"] = string(p.Phase), p.Reason
	}
	restore, dragonfly := "", ""
	if s := fg.Status.RestoreInPlace; s != nil {
		restore = string(s.Phase)
	}
	if d := fg.Status.Dragonfly; d != nil && d.Upgrade != nil {
		dragonfly = string(d.Upgrade.Phase)
	}
	return map[string]any{"group": fg.Name, "namespace": fg.Namespace, "activeSite": fg.Status.ActiveSite, "topologyGeneration": fg.Status.TopologyGeneration, "health": map[string]any{"ready": ready, "degradedReason": degraded, "replicationRunning": replicating, "replicationLagSeconds": lag, "recoveryPending": recovery}, "operations": map[string]any{"plannedFailover": planned, "updatePhase": fg.Status.UpdatePhase, "restoreInPlace": restore, "dragonflyUpgrade": dragonfly}, "stable": reason == "", "unstableReason": reason, "leases": leases}
}
