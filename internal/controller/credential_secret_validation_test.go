package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestValidateCredentialSecrets_RefusesUnusableValues pins the start check:
// an empty operator or role username/password is refused like a missing
// key (a manager dialing an empty account only reports every site
// unreachable), and a replication key naming the operator account with a
// different password is refused (two passwords for one MySQL account).
func TestValidateCredentialSecrets_RefusesUnusableValues(t *testing.T) {
	operator := func(edit func(map[string][]byte)) *corev1.Secret {
		data := map[string][]byte{
			"username":            []byte("bloodraven"),
			"password":            []byte("op-pw"),
			"MYSQL_ROOT_PASSWORD": []byte("root-pw"),
		}
		if edit != nil {
			edit(data)
		}
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mysql-operator-creds", Namespace: "shared-lion"},
			Data:       data,
		}
	}
	app := func(user, pass string) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mysql-app-creds", Namespace: "shared-lion"},
			Data:       map[string][]byte{"username": []byte(user), "password": []byte(pass)},
		}
	}
	tests := []struct {
		name      string
		secrets   []client.Object
		wantEvent string // substring; "" means the check passes
	}{
		{name: "valid", secrets: []client.Object{operator(nil), app("app", "app-pw")}},
		{name: "valid dedicated replication account", secrets: []client.Object{operator(func(d map[string][]byte) {
			d[replicationUserKey] = []byte("replicator")
			d[replicationPasswordKey] = []byte("repl-pw")
		}), app("app", "app-pw")}},
		{name: "replication key naming the operator with its password", secrets: []client.Object{operator(func(d map[string][]byte) {
			d[replicationUserKey] = []byte("bloodraven")
			d[replicationPasswordKey] = []byte("op-pw")
		}), app("app", "app-pw")}},
		{name: "empty root password is allowed", secrets: []client.Object{operator(func(d map[string][]byte) {
			d["MYSQL_ROOT_PASSWORD"] = nil
		}), app("app", "app-pw")}},
		{name: "missing operator username", secrets: []client.Object{operator(func(d map[string][]byte) {
			delete(d, "username")
		}), app("app", "app-pw")}, wantEvent: `missing required key "username"`},
		{name: "empty operator username", secrets: []client.Object{operator(func(d map[string][]byte) {
			d["username"] = []byte{}
		}), app("app", "app-pw")}, wantEvent: `has an empty value for key "username"`},
		{name: "empty operator password", secrets: []client.Object{operator(func(d map[string][]byte) {
			d["password"] = []byte{}
		}), app("app", "app-pw")}, wantEvent: `has an empty value for key "password"`},
		{name: "empty role password", secrets: []client.Object{operator(nil), app("app", "")},
			wantEvent: `app secret "mysql-app-creds" has an empty value for key "password"`},
		{name: "replication key naming the operator with another password", secrets: []client.Object{operator(func(d map[string][]byte) {
			d[replicationUserKey] = []byte("bloodraven")
			d[replicationPasswordKey] = []byte("other-pw")
		}), app("app", "app-pw")}, wantEvent: "names the operator account"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fg := newTestFGWithCredentials()
			fg.Spec.Credentials.BackupSecretName = ""
			r, _ := newReconciler(append([]client.Object{fg}, tt.secrets...)...)
			rec := record.NewFakeRecorder(4)
			r.Recorder = rec
			res, err := r.validateCredentialSecrets(context.Background(), fg)
			if err != nil {
				t.Fatalf("validateCredentialSecrets: %v", err)
			}
			var events []string
			for len(rec.Events) > 0 {
				events = append(events, <-rec.Events)
			}
			if tt.wantEvent == "" {
				if res != (ctrl.Result{}) || len(events) != 0 {
					t.Fatalf("want pass, got %+v events %q", res, events)
				}
				return
			}
			if res.RequeueAfter == 0 {
				t.Fatalf("want a requeue, got %+v", res)
			}
			if len(events) != 1 || !strings.Contains(events[0], "SecretMissingKey") || !strings.Contains(events[0], tt.wantEvent) {
				t.Fatalf("want one SecretMissingKey event containing %q, got %q", tt.wantEvent, events)
			}
		})
	}
}

// TestReconcile_ReplicationCredentialsMountedForInitScript pins how the
// first-boot script receives the replication account: an optional
// projection of the operator Secret's MYSQL_REPLICATION_USER /
// MYSQL_REPLICATION_PASSWORD keys, mounted only into the mysql container,
// so a Secret without the keys still starts the pod.
func TestReconcile_ReplicationCredentialsMountedForInitScript(t *testing.T) {
	fg := newTestFGWithCredentials()
	objs := []client.Object{fg}
	for _, s := range newTestCredentialSecrets() {
		objs = append(objs, s)
	}
	r, _ := newReconciler(objs...)
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	deploy := getDeployment(t, r, "dc1")

	var vol *corev1.Volume
	for i := range deploy.Spec.Template.Spec.Volumes {
		if v := &deploy.Spec.Template.Spec.Volumes[i]; v.Name == "creds-replication" {
			vol = v
		}
	}
	if vol == nil || vol.Secret == nil {
		t.Fatal("expected a creds-replication Secret volume")
	}
	if vol.Secret.SecretName != "mysql-operator-creds" {
		t.Errorf("creds-replication projects %q, want the operator Secret", vol.Secret.SecretName)
	}
	if vol.Secret.Optional == nil || !*vol.Secret.Optional {
		t.Error("creds-replication must be optional: the replication keys are optional")
	}
	want := map[string]string{replicationUserKey: "username", replicationPasswordKey: "password"}
	if len(vol.Secret.Items) != len(want) {
		t.Fatalf("items = %+v, want %v", vol.Secret.Items, want)
	}
	for _, item := range vol.Secret.Items {
		if want[item.Key] != item.Path {
			t.Errorf("item %s -> %s, want -> %s", item.Key, item.Path, want[item.Key])
		}
	}
	for _, ctr := range deploy.Spec.Template.Spec.Containers {
		mounted := false
		for _, m := range ctr.VolumeMounts {
			if m.Name == "creds-replication" {
				mounted = m.MountPath == "/etc/mysql/creds/replication" && m.ReadOnly
			}
		}
		if (ctr.Name == "mysql") != mounted {
			t.Errorf("container %s: creds-replication mounted read-only at /etc/mysql/creds/replication = %v", ctr.Name, mounted)
		}
	}
}
