package kube

import (
	"strings"
	"testing"
)

func TestRequirePlaygroundContext(t *testing.T) {
	t.Setenv("BLOODRAVEN_PLAYGROUND_CONTEXTS", "")

	cases := []struct {
		name    string
		ctx     string
		env     string
		wantErr bool
	}{
		{name: "empty context is rejected", ctx: "", wantErr: true},
		{name: "k3d-bloodraven exact match", ctx: "k3d-bloodraven"},
		{name: "k3d- prefix match", ctx: "k3d-other"},
		{name: "kind- prefix match", ctx: "kind-anything"},
		{name: "minikube prefix match", ctx: "minikube-staging"},
		{name: "production context rejected", ctx: "prod-aws", wantErr: true},
		{name: "env override accepts", ctx: "remote-dev", env: "remote-dev other-ctx"},
		{name: "env override does not match unrelated", ctx: "prod-aws", env: "remote-dev", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BLOODRAVEN_PLAYGROUND_CONTEXTS", tc.env)
			err := RequirePlaygroundContext(tc.ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RequirePlaygroundContext(%q) err=%v wantErr=%v", tc.ctx, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "playground:") {
				t.Fatalf("error message should be prefixed with playground: %q", err.Error())
			}
		})
	}
}
