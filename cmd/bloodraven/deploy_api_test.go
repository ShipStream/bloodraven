package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/platform"
	authv1 "k8s.io/api/authentication/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const deployTestPath = "/deploy/v1/groups/orders"

type deployAPIFixture struct {
	api     *deployAPI
	client  client.WithWatch
	reviews *kubefake.Clientset
	group   *v1alpha1.MysqlFailoverGroup
	db      *v1alpha1.MysqlDatabase
	logs    *bytes.Buffer
	now     time.Time
}

func newDeployAPIFixture(t *testing.T) *deployAPIFixture {
	t.Helper()
	f := &deployAPIFixture{now: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC), logs: new(bytes.Buffer)}
	f.group = &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "database-ns", UID: "group-uid"}}
	f.group.Spec.Sites = []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}
	f.group.Status.ActiveSite = "iad"
	f.group.Status.TopologyGeneration = 7
	f.group.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}, {Name: "pdx", State: "read-only", Replicating: true}}
	f.group.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy", LastTransitionTime: metav1.NewTime(f.now)}}
	f.db = &v1alpha1.MysqlDatabase{ObjectMeta: metav1.ObjectMeta{Name: "tenant", Namespace: f.group.Namespace}, Spec: v1alpha1.MysqlDatabaseSpec{
		GroupRef:          v1alpha1.LocalGroupRef{Name: f.group.Name},
		DeploymentClients: []v1alpha1.DeploymentClient{{Namespace: "app-ns", ServiceAccount: "deployer"}},
	}}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1alpha1.AddToScheme, authv1.AddToScheme, coordinationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	f.client = fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(f.group).WithObjects(f.group, f.db).Build()
	f.reviews = kubefake.NewClientset()
	f.reviews.PrependReactor("create", "tokenreviews", func(action kubetesting.Action) (bool, runtime.Object, error) {
		review := action.(kubetesting.CreateAction).GetObject().(*authv1.TokenReview)
		if review.Spec.Token != "sa-token" {
			return true, &authv1.TokenReview{}, nil
		}
		return true, &authv1.TokenReview{Status: authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{"bloodraven-deploy"}, User: authv1.UserInfo{Username: "system:serviceaccount:app-ns:deployer"}}}, nil
	})
	leases := controller.NewDeploymentLeaseManager(f.client, f.client, nil)
	leases.SetClock(func() time.Time { return f.now })
	f.api = newDeployAPI(f.client, f.reviews.AuthenticationV1().TokenReviews(), leases, func() bool { return true }, slog.New(slog.NewJSONHandler(f.logs, nil)), "")
	f.api.now = func() time.Time { return f.now }
	return f
}

func (f *deployAPIFixture) request(t *testing.T, method, path, body, token string, status int) map[string]any {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer sa-token")
	r.Header.Set("X-Lease-Token", token)
	w := httptest.NewRecorder()
	f.api.ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s %s = %d %s, want %d", method, path, w.Code, w.Body.String(), status)
	}
	if w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unsafe response headers: %v", w.Header())
	}
	if status == http.StatusNoContent {
		if w.Body.Len() != 0 {
			t.Fatalf("204 body = %q", w.Body.String())
		}
		return nil
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func (f *deployAPIFixture) grant(t *testing.T, kind, operation, instance string, ttl, status int) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"kind": kind, "operationId": operation, "instance": instance, "ttlSeconds": ttl})
	if err != nil {
		t.Fatal(err)
	}
	return f.request(t, "POST", deployTestPath+"/leases", string(body), "", status)
}

func TestDeployAPILeaseLifecycle(t *testing.T) {
	f := newDeployAPIFixture(t)
	first := f.grant(t, "migration", "op1", "tenant", 1, 201)
	if first["kind"] != "migration" || first["operationId"] != "op1" || first["topologyGeneration"] != float64(7) || first["expiresAt"] != f.now.Add(5*time.Second).Format(time.RFC3339) {
		t.Fatalf("grant = %v", first)
	}
	second := f.grant(t, "migration", "op1", "tenant", 999, 200)
	token := second["token"].(string)
	if token == first["token"] || len(token) != 64 || second["expiresAt"] != f.now.Add(120*time.Second).Format(time.RFC3339) {
		t.Fatalf("regrant did not rotate/clamp TTL: %v", second)
	}
	path := deployTestPath + "/leases/migration/op1"
	wrong := f.request(t, "PUT", path, fmt.Sprintf(`{"token":%q,"ttlSeconds":30}`, first["token"]), "", 403)
	if wrong["error"] != "token_mismatch" {
		t.Fatal(wrong)
	}
	f.now = f.now.Add(time.Second)
	renew := f.request(t, "PUT", path, fmt.Sprintf(`{"token":%q,"ttlSeconds":30}`, token), "", 200)
	if renew["expiresAt"] != f.now.Add(30*time.Second).Format(time.RFC3339) || renew["topologyGeneration"] != float64(7) || len(renew) != 2 {
		t.Fatalf("renew = %v", renew)
	}
	hold := f.grant(t, "failover-hold", "op1", "tenant", 30, 201)
	f.request(t, "PUT", deployTestPath+"/leases/failover-hold/op1", fmt.Sprintf(`{"token":%q,"ttlSeconds":30}`, hold["token"]), "", 200)
	conflict := f.grant(t, "migration", "op2", "tenant", 30, 409)
	if conflict["error"] != "held" || conflict["holder"].(map[string]any)["operationId"] != "op1" {
		t.Fatalf("conflict = %v", conflict)
	}
	if len(conflict["holder"].(map[string]any)) != 3 {
		t.Fatalf("unexpected holder fields: %v", conflict)
	}
	snapshot := f.request(t, "GET", deployTestPath, "", "", 200)
	if snapshot["stable"] != true || len(snapshot["leases"].([]any)) != 2 {
		t.Fatalf("snapshot = %v", snapshot)
	}
	for _, bad := range []string{"", "wrong", first["token"].(string)} {
		f.request(t, "DELETE", path, "", bad, 403)
	}
	f.request(t, "DELETE", path, "", token, 204)
	f.request(t, "DELETE", path, "", token, 204)
	f.request(t, "DELETE", path, "", "wrong", 403)
	f.request(t, "PUT", path, fmt.Sprintf(`{"token":%q}`, token), "", 404)
	f.request(t, "DELETE", deployTestPath+"/leases/failover-hold/op1", "", hold["token"].(string), 204)
	f.request(t, "DELETE", deployTestPath+"/leases/migration/never-existed", "", "", 204)

	var stored coordinationv1.LeaseList
	if err := f.client.List(context.Background(), &stored); err != nil {
		t.Fatal(err)
	}
	persisted, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	public, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"sa-token", first["token"].(string), token, hold["token"].(string)} {
		hash := sha256.Sum256([]byte(secret))
		if strings.Contains(f.logs.String(), secret) || strings.Contains(f.logs.String(), hex.EncodeToString(hash[:])) || bytes.Contains(persisted, []byte(secret)) || bytes.Contains(public, []byte(secret)) || bytes.Contains(public, []byte(hex.EncodeToString(hash[:]))) {
			t.Fatal("credentials leaked in logs, durable resources, or snapshot")
		}
	}
}

func TestDeployAPIUnauthorizedMetricsBounded(t *testing.T) {
	f := newDeployAPIFixture(t)
	before := testutil.ToFloat64(metrics.DeployLeaseGrantsTotal.WithLabelValues("", "migration", "unauthorized"))
	for i := range 10 {
		f.request(t, "POST", fmt.Sprintf("/deploy/v1/groups/missing-%d/leases", i), `{"kind":"migration","operationId":"op","instance":"tenant","ttlSeconds":30}`, "", 403)
	}
	after := testutil.ToFloat64(metrics.DeployLeaseGrantsTotal.WithLabelValues("", "migration", "unauthorized"))
	if after-before != 10 {
		t.Fatalf("unauthorized requests must share a bounded group label: %v", after-before)
	}
}

func TestDeployAPIExpiredAndRevoked(t *testing.T) {
	for _, state := range []string{"expired", "operator_revoked", "topology_changed"} {
		t.Run(state, func(t *testing.T) {
			f := newDeployAPIFixture(t)
			token := f.grant(t, "migration", "op1", "tenant", 5, 201)["token"].(string)
			status, code := 409, "revoked"
			switch state {
			case "expired":
				f.now = f.now.Add(5*time.Second + time.Nanosecond)
				status, code = 404, "not_found"
			case "operator_revoked":
				if err := f.api.leases.Revoke(context.Background(), f.group, state); err != nil {
					t.Fatal(err)
				}
			case "topology_changed":
				f.group.Status.TopologyGeneration++
				if err := f.client.Status().Update(context.Background(), f.group); err != nil {
					t.Fatal(err)
				}
			}
			path := deployTestPath + "/leases/migration/op1"
			response := f.request(t, "PUT", path, fmt.Sprintf(`{"token":%q,"ttlSeconds":30}`, token), "", status)
			if response["error"] != code || (status == 409 && response["reason"] != state) {
				t.Fatal(response)
			}
			f.request(t, "DELETE", path, "", "wrong", 403)
			f.request(t, "DELETE", path, "", token, 204)
			f.request(t, "DELETE", path, "", token, 204)
			if status == 409 {
				f.grant(t, "migration", "op1", "tenant", 30, 409)
			}
		})
	}
}

func TestDeployAPIAuthorizationDoesNotLeakGroupExistence(t *testing.T) {
	for _, mutation := range []string{"service account", "namespace", "group ref", "deleting", "missing database"} {
		t.Run(mutation, func(t *testing.T) {
			f := newDeployAPIFixture(t)
			switch mutation {
			case "service account":
				f.db.Spec.DeploymentClients[0].ServiceAccount = "other"
			case "namespace":
				f.db.Spec.DeploymentClients[0].Namespace = "other"
			case "group ref":
				f.db.Spec.GroupRef.Name = "other"
			case "deleting":
				f.db.Finalizers = []string{"test/keep"}
			}
			if err := f.client.Update(context.Background(), f.db); err != nil {
				t.Fatal(err)
			}
			if mutation == "deleting" || mutation == "missing database" {
				if err := f.client.Delete(context.Background(), f.db); err != nil {
					t.Fatal(err)
				}
			}
			f.api.reader = interceptor.NewClient(f.client, interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				t.Fatal("unauthorized request looked up group existence")
				return nil
			}})
			for _, group := range []string{"orders", "absent"} {
				path := "/deploy/v1/groups/" + group
				for _, request := range []struct{ method, path, body string }{
					{"GET", path, ""},
					{"POST", path + "/leases", `{"kind":"migration","operationId":"op1","instance":"tenant","ttlSeconds":30}`},
					{"PUT", path + "/leases/migration/op1", `{"token":"wrong"}`},
					{"DELETE", path + "/leases/migration/op1", ""},
				} {
					response := f.request(t, request.method, request.path, request.body, "", 403)
					if !reflect.DeepEqual(response, map[string]any{"error": "forbidden"}) {
						t.Fatalf("existence leak: %v", response)
					}
				}
			}
		})
	}
}

func TestDeployAPIMultipleInstancesAndAmbiguousNamespaces(t *testing.T) {
	f := newDeployAPIFixture(t)
	other := f.db.DeepCopy()
	other.Name, other.ResourceVersion = "aaa-other", ""
	if err := f.client.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	f.request(t, "GET", deployTestPath+"?namespace=attacker", "", "", 200)
	token := f.grant(t, "migration", "op1", "tenant", 30, 201)["token"].(string)
	f.request(t, "PUT", deployTestPath+"/leases/migration/op1", fmt.Sprintf(`{"token":%q,"ttlSeconds":30}`, token), "", 200)
	f.request(t, "DELETE", deployTestPath+"/leases/migration/op1", "", token, 204)
	f.grant(t, "migration", "op2", "not-authorized", 30, 403)
	other = f.db.DeepCopy()
	other.Namespace, other.ResourceVersion = "ambiguous-ns", ""
	if err := f.client.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	f.api.reader = interceptor.NewClient(f.client, interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
		t.Fatal("ambiguous authorization looked up a group")
		return nil
	}})
	f.request(t, "GET", deployTestPath, "", "", 403)
	f.grant(t, "migration", "op3", "tenant", 30, 403)
	f.request(t, "PUT", deployTestPath+"/leases/migration/op1", fmt.Sprintf(`{"token":%q}`, token), "", 403)
	f.request(t, "DELETE", deployTestPath+"/leases/migration/op1", "", token, 403)
}

func TestDeployAPITokenReviewFailuresAreNotCached(t *testing.T) {
	for _, name := range []string{"API error", "unauthenticated", "review error", "wrong audience", "empty audience", "non-SA", "bad namespace", "bad SA", "extra components"} {
		t.Run(name, func(t *testing.T) {
			f := newDeployAPIFixture(t)
			f.reviews.PrependReactor("create", "tokenreviews", func(action kubetesting.Action) (bool, runtime.Object, error) {
				review := action.(kubetesting.CreateAction).GetObject().(*authv1.TokenReview)
				if !reflect.DeepEqual(review.Spec.Audiences, []string{"bloodraven-deploy"}) || review.Spec.Token != "sa-token" {
					t.Fatalf("TokenReview spec = %+v", review.Spec)
				}
				status := authv1.TokenReviewStatus{Authenticated: true, Audiences: []string{"bloodraven-deploy"}, User: authv1.UserInfo{Username: "system:serviceaccount:app-ns:deployer"}}
				switch name {
				case "API error":
					return true, nil, errors.New("secret upstream diagnostic")
				case "unauthenticated":
					status.Authenticated = false
				case "review error":
					status.Error = "secret upstream diagnostic"
				case "wrong audience":
					status.Audiences = []string{"kubernetes"}
				case "empty audience":
					status.Audiences = nil
				case "non-SA":
					status.User.Username = "admin"
				case "bad namespace":
					status.User.Username = "system:serviceaccount:BAD:deployer"
				case "bad SA":
					status.User.Username = "system:serviceaccount:app-ns:"
				case "extra components":
					status.User.Username += ":extra"
				}
				return true, &authv1.TokenReview{Status: status}, nil
			})
			for range 2 {
				response := f.request(t, "GET", deployTestPath, "", "", 401)
				if !reflect.DeepEqual(response, map[string]any{"error": "unauthorized"}) {
					t.Fatal(response)
				}
			}
			if len(f.api.tokens) != 0 || len(f.reviews.Actions()) != 2 || strings.Contains(f.logs.String(), "secret upstream") {
				t.Fatal("failed review cached or diagnostic leaked")
			}
		})
	}
}

func TestDeployAPITokenReviewCacheAudienceAndRotation(t *testing.T) {
	f := newDeployAPIFixture(t)
	f.api.audience = "custom-deploy"
	valid := true
	f.reviews.PrependReactor("create", "tokenreviews", func(action kubetesting.Action) (bool, runtime.Object, error) {
		review := action.(kubetesting.CreateAction).GetObject().(*authv1.TokenReview)
		if !reflect.DeepEqual(review.Spec.Audiences, []string{"custom-deploy"}) {
			t.Fatalf("audiences = %v", review.Spec.Audiences)
		}
		return true, &authv1.TokenReview{Status: authv1.TokenReviewStatus{Authenticated: valid, Audiences: []string{"another", "custom-deploy"}, User: authv1.UserInfo{Username: "system:serviceaccount:app-ns:deployer"}}}, nil
	})
	for range 2 {
		f.request(t, "GET", deployTestPath, "", "", 200)
	}
	entry, ok := f.api.tokens[sha256.Sum256([]byte("sa-token"))]
	if !ok || len(f.api.tokens) != 1 || len(f.reviews.Actions()) != 1 || entry.expires.Sub(f.now) <= 0 || entry.expires.Sub(f.now) > 30*time.Second {
		t.Fatalf("positive cache = %+v, reviews = %d", f.api.tokens, len(f.reviews.Actions()))
	}
	if _, ok := f.api.authenticate(context.Background(), "Bearer rotated-sa-token"); !ok || len(f.reviews.Actions()) != 2 {
		t.Fatal("rotated token did not get a fresh review")
	}
	valid = false
	f.now = entry.expires
	for range 2 {
		f.request(t, "GET", deployTestPath, "", "", 401)
	}
	if len(f.reviews.Actions()) != 4 {
		t.Fatal("cache outlived TTL or cached rejection")
	}
	valid = true
	f.request(t, "GET", deployTestPath, "", "", 200)
	if len(f.api.tokens) != 1 {
		t.Fatal("expired entries not pruned on successful review")
	}
	// Authorization is re-read even when the ServiceAccount authentication is cached.
	f.db.Spec.DeploymentClients = nil
	if err := f.client.Update(context.Background(), f.db); err != nil {
		t.Fatal(err)
	}
	f.request(t, "GET", deployTestPath, "", "", 403)
}

func TestDeployAPIAuthenticationHeaders(t *testing.T) {
	for _, header := range []string{"", "Basic sa-token", "Bearer", "Bearer a b", "Bearer " + strings.Repeat("x", 16385), "Bearer invalid"} {
		t.Run(fmt.Sprintf("length-%d-%s", len(header), strings.Fields(header + " empty")[0]), func(t *testing.T) {
			f := newDeployAPIFixture(t)
			r := httptest.NewRequest("GET", deployTestPath, nil)
			r.Header.Set("Authorization", header)
			w := httptest.NewRecorder()
			f.api.ServeHTTP(w, r)
			if w.Code != 401 || w.Body.String() != "{\"error\":\"unauthorized\"}\n" {
				t.Fatalf("response = %d %s", w.Code, w.Body.String())
			}
			if header != "Bearer invalid" && len(f.reviews.Actions()) != 0 {
				t.Fatal("malformed header sent to TokenReview")
			}
		})
	}
	f := newDeployAPIFixture(t)
	if _, ok := f.api.authenticate(context.Background(), "bEaReR sa-token"); !ok {
		t.Fatal("Bearer scheme should be case insensitive")
	}
}

func TestDeployAPIBadRequestsAndMethods(t *testing.T) {
	for _, test := range []struct {
		name, method, path, body, allow string
		status                          int
	}{
		{"empty", "POST", "/leases", "", "", 400},
		{"null", "POST", "/leases", "null", "", 400},
		{"array", "POST", "/leases", "[]", "", 400},
		{"malformed", "POST", "/leases", "{", "", 400},
		{"unknown", "POST", "/leases", `{"surprise":true}`, "", 400},
		{"trailing", "POST", "/leases", `{} {}`, "", 400},
		{"trailing null", "POST", "/leases", `{} null`, "", 400},
		{"bad ttl", "POST", "/leases", `{"ttlSeconds":"30"}`, "", 400},
		{"large", "POST", "/leases", `{"instance":"` + strings.Repeat("a", 4096) + `"}`, "", 413},
		{"large trailing whitespace", "POST", "/leases", `{}` + strings.Repeat(" ", 4096), "", 413},
		{"renew unknown", "PUT", "/leases/migration/op1", `{"unknown":true}`, "", 400},
		{"renew large", "PUT", "/leases/migration/op1", `{"token":"` + strings.Repeat("a", 4096) + `"}`, "", 413},
		{"bad kind", "PUT", "/leases/other/op1", `{}`, "", 400},
		{"long operation", "DELETE", "/leases/migration/" + strings.Repeat("a", 129), "", "", 400},
		{"empty operation", "DELETE", "/leases/migration/", "", "", 400},
		{"group method", "POST", "", "", "GET", 405},
		{"grant method", "GET", "/leases", "", "POST", 405},
		{"renew method", "PATCH", "/leases/migration/op1", "", "PUT, DELETE", 405},
		{"unknown path", "GET", "/unknown", "", "", 404},
		{"extra component", "GET", "/leases/migration/op1/extra", "", "", 404},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newDeployAPIFixture(t)
			r := httptest.NewRequest(test.method, deployTestPath+test.path, strings.NewReader(test.body))
			r.Header.Set("Authorization", "Bearer sa-token")
			w := httptest.NewRecorder()
			f.api.ServeHTTP(w, r)
			if w.Code != test.status || w.Header().Get("Allow") != test.allow {
				t.Fatalf("response = %d %s Allow=%q, want %d %q", w.Code, w.Body.String(), w.Header().Get("Allow"), test.status, test.allow)
			}
		})
	}
	f := newDeployAPIFixture(t)
	for _, path := range []string{"/deploy/v1", "/deploy/v1/", "/deploy/v1/groups/UPPER", "/deploy/v1/groups/"} {
		f.request(t, "GET", path, "", "", 404)
	}
	for _, test := range []struct{ kind, operation, instance string }{{"other", "op1", "tenant"}, {"migration", "", "tenant"}, {"migration", strings.Repeat("a", 129), "tenant"}, {"migration", "op1", "UPPER"}} {
		f.grant(t, test.kind, test.operation, test.instance, 30, 400)
	}
}

func TestDeployAPILeaderAndReadFailures(t *testing.T) {
	for _, stage := range []string{"follower", "lost leadership", "database list", "group read", "snapshot read"} {
		t.Run(stage, func(t *testing.T) {
			f := newDeployAPIFixture(t)
			checks := 0
			f.api.leader = func() bool {
				checks++
				return stage != "follower" && (stage != "lost leadership" || checks == 1)
			}
			funcs := interceptor.Funcs{}
			if stage == "database list" {
				funcs.List = func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
					return errors.New("secret list diagnostic")
				}
			}
			if stage == "group read" || stage == "snapshot read" {
				funcs.Get = func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return errors.New("secret read diagnostic")
				}
			}
			reader := interceptor.NewClient(f.client, funcs)
			if stage == "snapshot read" {
				f.api.leases = controller.NewDeploymentLeaseManager(f.client, reader, nil)
			} else {
				f.api.reader = reader
			}
			response := f.request(t, "GET", deployTestPath, "", "", 503)
			want := "unavailable"
			if stage == "follower" || stage == "lost leadership" {
				want = "not_leader"
			}
			if !reflect.DeepEqual(response, map[string]any{"error": want}) {
				t.Fatal(response)
			}
			if stage == "follower" && len(f.reviews.Actions()) != 0 {
				t.Fatal("follower performed TokenReview")
			}
		})
	}
}

func TestDeployAPIUnstableAndFreshSnapshot(t *testing.T) {
	for _, runtimePending := range []bool{false, true} {
		t.Run(fmt.Sprint(runtimePending), func(t *testing.T) {
			f := newDeployAPIFixture(t)
			if runtimePending {
				f.api.leases.TopologyPending(client.ObjectKeyFromObject(f.group))
				f.request(t, "GET", deployTestPath, "", "", 423)
			} else {
				f.group.Status.UpdatePhase = "UpdatingReplicas"
				if err := f.client.Status().Update(context.Background(), f.group); err != nil {
					t.Fatal(err)
				}
				response := f.request(t, "GET", deployTestPath, "", "", 200)
				if response["stable"] != false || response["unstableReason"] != "OrderedUpdate" {
					t.Fatal(response)
				}
			}
			response := f.grant(t, "migration", "op1", "tenant", 30, 423)
			if response["error"] != "unstable" || response["retryAfterSeconds"] != float64(1) {
				t.Fatal(response)
			}
		})
	}
	f := newDeployAPIFixture(t)
	stale := f.group.DeepCopy()
	lag := int64(9)
	f.group.Status.ActiveSite = "pdx"
	f.group.Status.TopologyGeneration = 8
	f.group.Status.Sites = []v1alpha1.SiteStatus{{Name: "pdx", State: "writable"}, {Name: "iad", State: "read-only", SecondsBehindSource: &lag, RecoveryState: "Recloning", SourceConvergenceState: v1alpha1.SourceConvergencePending}}
	f.group.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}, {Type: "Degraded", Status: metav1.ConditionTrue, Reason: "ReplicaDown"}, {Type: "RecoveryPending", Status: metav1.ConditionTrue}}
	f.group.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDeferred, Reason: "DeploymentHold"}
	f.group.Status.UpdatePhase = "UpdatingReplicas"
	f.group.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceSucceeded}
	f.group.Status.Dragonfly = &v1alpha1.DragonflyStatus{Upgrade: &v1alpha1.DragonflyUpgradeStatus{Phase: v1alpha1.DragonflyUpgradePhaseSucceeded}}
	if err := f.client.Status().Update(context.Background(), f.group); err != nil {
		t.Fatal(err)
	}
	// The first HTTP read is stale; Snapshot must supply both health and leases.
	f.api.reader = interceptor.NewClient(f.client, interceptor.Funcs{Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
		stale.DeepCopyInto(obj.(*v1alpha1.MysqlFailoverGroup))
		return nil
	}})
	response := f.request(t, "GET", deployTestPath, "", "", 200)
	if response["namespace"] != "database-ns" || response["activeSite"] != "pdx" || response["topologyGeneration"] != float64(8) || response["stable"] != false || response["unstableReason"] != "PlannedFailover" {
		t.Fatalf("stale snapshot returned: %v", response)
	}
	wantHealth := map[string]any{"ready": false, "degradedReason": "ReplicaDown", "replicationRunning": false, "replicationLagSeconds": float64(9), "recoveryPending": true}
	wantOperations := map[string]any{"plannedFailover": map[string]any{"phase": string(v1alpha1.PlannedFailoverPhaseDeferred), "reason": "DeploymentHold"}, "updatePhase": "UpdatingReplicas", "restoreInPlace": string(v1alpha1.RestoreInPlaceSucceeded), "dragonflyUpgrade": string(v1alpha1.DragonflyUpgradePhaseSucceeded)}
	if !reflect.DeepEqual(response["health"], wantHealth) || !reflect.DeepEqual(response["operations"], wantOperations) {
		t.Fatalf("persisted fields missing: %v", response)
	}
	f.grant(t, "migration", "op1", "tenant", 30, 423)
}

func TestDeployAPITLSMuxLogsAndMetrics(t *testing.T) {
	f := newDeployAPIFixture(t)
	hub := platform.NewHub(f.api.logger)
	runner := controller.NewTopologyManagerRunner(f.client, nil, hub, nil, f.api.logger)
	plain := newAuxMux(runner, hub, f.client)
	for _, path := range []string{"/deploy/v1", "/deploy/v1/", deployTestPath, deployTestPath + "/leases"} {
		w := httptest.NewRecorder()
		plain.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 {
			t.Fatalf("plain HTTP exposes deployment route %s: %d", path, w.Code)
		}
	}
	mux := newEscrowMux(f.client, f.api.logger)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", deployTestPath, nil))
	if w.Code != 404 {
		t.Fatal("deployment API enabled without route registration")
	}
	mux.Handle("/deploy/v1/", f.api)
	mux.Handle("/deploy/v1", f.api)
	f.logs.Reset()
	counter := metrics.HTTPRequestsTotal.WithLabelValues("deploy", "grant", "POST", "2xx")
	auxCounter := metrics.HTTPRequestsTotal.WithLabelValues("aux", "grant", "POST", "2xx")
	before, auxBefore := testutil.ToFloat64(counter), testutil.ToFloat64(auxCounter)
	server := httptest.NewTLSServer(mux)
	defer server.Close()
	r, err := http.NewRequest("POST", server.URL+deployTestPath+"/leases", strings.NewReader(`{"kind":"migration","operationId":"op-log","instance":"tenant","ttlSeconds":30}`))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer sa-token")
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 201 || response.TLS == nil {
		t.Fatalf("TLS response = %v", response.Status)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	decoder := json.NewDecoder(bytes.NewReader(f.logs.Bytes()))
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		t.Fatalf("double request log: %s", f.logs.String())
	}
	delete(record, "time")
	want := map[string]any{"level": "INFO", "msg": "deploy api", "handler": "grant", "group": "orders", "instance": "tenant", "namespace": "database-ns", "operationId": "op-log", "status": float64(201), "duration_ms": float64(0)}
	if !reflect.DeepEqual(record, want) {
		t.Fatalf("log contract = %v, want %v", record, want)
	}
	if testutil.ToFloat64(counter)-before != 1 || testutil.ToFloat64(auxCounter) != auxBefore {
		t.Fatal("deployment request counted twice or under wrong server label")
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.HTTPRequestsTotal, metrics.HTTPRequestDurationSeconds)
	scrape := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(scrape, httptest.NewRequest("GET", "/metrics", nil))
	if scrape.Code != 200 {
		t.Fatal(scrape.Code)
	}
	for _, metric := range []string{`bloodraven_http_requests_total{handler="grant",method="POST",server="deploy",status="2xx"}`, `bloodraven_http_request_duration_seconds_count{handler="grant",method="POST",server="deploy"}`} {
		if !strings.Contains(scrape.Body.String(), metric) {
			t.Fatalf("metrics endpoint missing %s", metric)
		}
	}
}

func TestDeployAPIHoldRequiresSameMigrationHolder(t *testing.T) {
	f := newDeployAPIFixture(t)
	f.grant(t, "failover-hold", "op1", "tenant", 30, 403)
	f.grant(t, "migration", "op1", "tenant", 30, 201)
	f.grant(t, "failover-hold", "different-op", "tenant", 30, 403)
	other := f.db.DeepCopy()
	other.Name, other.ResourceVersion = "other", ""
	if err := f.client.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	f.grant(t, "failover-hold", "op1", "other", 30, 403)
	f.grant(t, "failover-hold", "op1", "tenant", 30, 201)
}

func TestDeployAPIRuntimeReservationBlocksRenewAndSnapshot(t *testing.T) {
	for _, pending := range []string{"topology", "planned"} {
		t.Run(pending, func(t *testing.T) {
			f := newDeployAPIFixture(t)
			token := f.grant(t, "migration", "op1", "tenant", 30, 201)["token"].(string)
			if pending == "topology" {
				f.api.leases.TopologyPending(client.ObjectKeyFromObject(f.group))
			} else if _, err := f.api.leases.BeginPlanned(context.Background(), f.group, "OrderedUpdate"); err != nil {
				t.Fatal(err)
			}
			f.request(t, "GET", deployTestPath, "", "", 423)
			path := deployTestPath + "/leases/migration/op1"
			f.request(t, "PUT", path, fmt.Sprintf(`{"token":%q,"ttlSeconds":30}`, token), "", 423)
			f.grant(t, "migration", "op1", "tenant", 30, 423)
			f.request(t, "DELETE", path, "", token, 204)
		})
	}
}

func TestDeployAPISnapshotSurvivesManagerRestart(t *testing.T) {
	f := newDeployAPIFixture(t)
	f.grant(t, "migration", "op1", "tenant", 30, 201)
	f.grant(t, "failover-hold", "op1", "tenant", 30, 201)
	before := f.request(t, "GET", deployTestPath, "", "", 200)
	f.api.leases = controller.NewDeploymentLeaseManager(f.client, f.client, nil)
	f.api.leases.SetClock(func() time.Time { return f.now })
	after := f.request(t, "GET", deployTestPath, "", "", 200)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("restart lost persisted snapshot fields: before=%v after=%v", before, after)
	}
	wantHealth := map[string]any{"ready": true, "degradedReason": "", "replicationRunning": true, "replicationLagSeconds": float64(0), "recoveryPending": false}
	if !reflect.DeepEqual(after["health"], wantHealth) {
		t.Fatalf("stable group health = %v", after["health"])
	}
	for _, raw := range after["leases"].([]any) {
		view := raw.(map[string]any)
		if len(view) != 5 || view["instance"] != "tenant" || view["operationId"] != "op1" || view["topologyGeneration"] != float64(7) || view["expiresAt"] != f.now.Add(30*time.Second).Format(time.RFC3339) {
			t.Fatalf("persisted public lease = %v", view)
		}
	}
}

func TestDeployAPICacheBoundDoesNotEvictValidCredentials(t *testing.T) {
	f := newDeployAPIFixture(t)
	for i := range 1024 {
		f.api.tokens[sha256.Sum256([]byte(fmt.Sprintf("cached-%d", i)))] = deployIdentity{namespace: "app-ns", serviceAccount: "deployer", expires: f.now.Add(time.Second)}
	}
	for range 2 {
		if _, ok := f.api.authenticate(context.Background(), "Bearer sa-token"); !ok {
			t.Fatal("full cache denied valid credentials")
		}
	}
	if len(f.api.tokens) != 1024 || len(f.reviews.Actions()) != 2 {
		t.Fatal("cache exceeded bound or evicted a still-valid entry")
	}
	f.now = f.now.Add(time.Second)
	if _, ok := f.api.authenticate(context.Background(), "Bearer sa-token"); !ok || len(f.api.tokens) != 1 {
		t.Fatal("expired cache entries were not reclaimed")
	}
}

func TestDeployAPIConcurrentAuthenticationCache(t *testing.T) {
	f := newDeployAPIFixture(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, ok := f.api.authenticate(context.Background(), "Bearer sa-token"); !ok {
				t.Error("authentication failed")
			}
		})
	}
	wg.Wait()
	if len(f.api.tokens) != 1 {
		t.Fatalf("cache entries = %d", len(f.api.tokens))
	}
}
