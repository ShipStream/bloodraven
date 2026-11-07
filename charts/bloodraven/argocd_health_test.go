package bloodraven_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	lua "github.com/yuin/gopher-lua"
	"sigs.k8s.io/yaml"
)

// The chart ships each Argo CD health check twice: as a standalone script
// under argocd/ and inside argocd-cm-patch.yaml. Argo CD runs them with
// gopher-lua, as this test does.
var argoHealthChecks = []struct {
	kind, script, fixtures string
}{
	{"shipstream.io_MysqlFailoverGroup", "argocd/health-mysqlfailovergroup.lua", "testdata/argocd/mysqlfailovergroup"},
	{"shipstream.io_MysqlBackup", "argocd/health-mysqlbackup.lua", "testdata/argocd/mysqlbackup"},
}

// argoHealthTests is Argo CD's health_test.yaml format.
type argoHealthTests struct {
	Tests []struct {
		InputPath    string `json:"inputPath"`
		HealthStatus struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"healthStatus"`
	} `json:"tests"`
}

// TestArgoCDHealthChecks runs every fixture through both copies of each
// health check and asserts the status and message, and that the copies
// are the same script.
func TestArgoCDHealthChecks(t *testing.T) {
	raw, err := os.ReadFile("argocd/argocd-cm-patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal(raw, &cm); err != nil {
		t.Fatal(err)
	}
	for _, hc := range argoHealthChecks {
		t.Run(hc.kind, func(t *testing.T) {
			standalone, err := os.ReadFile(hc.script)
			if err != nil {
				t.Fatal(err)
			}
			patched, ok := cm.Data["resource.customizations.health."+hc.kind]
			if !ok {
				t.Fatalf("argocd-cm-patch.yaml has no health check for %s", hc.kind)
			}
			if strings.TrimSpace(patched) != strings.TrimSpace(string(standalone)) {
				t.Fatalf("argocd-cm-patch.yaml and %s differ; keep the two copies identical", hc.script)
			}
			raw, err := os.ReadFile(filepath.Join(hc.fixtures, "health_test.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var cases argoHealthTests
			if err := yaml.Unmarshal(raw, &cases); err != nil {
				t.Fatal(err)
			}
			if len(cases.Tests) == 0 {
				t.Fatal("no fixtures")
			}
			for _, tc := range cases.Tests {
				t.Run(strings.TrimSuffix(tc.InputPath, ".yaml"), func(t *testing.T) {
					obj, err := os.ReadFile(filepath.Join(hc.fixtures, tc.InputPath))
					if err != nil {
						t.Fatal(err)
					}
					status, message := runHealthCheck(t, string(standalone), obj)
					if status != tc.HealthStatus.Status || message != tc.HealthStatus.Message {
						t.Fatalf("health = %s %q, want %s %q", status, message, tc.HealthStatus.Status, tc.HealthStatus.Message)
					}
				})
			}
		})
	}
}

// runHealthCheck evaluates script with the YAML object bound to obj, as
// Argo CD does, and returns hs.status and hs.message.
func runHealthCheck(t *testing.T, script string, object []byte) (string, string) {
	t.Helper()
	asJSON, err := yaml.YAMLToJSON(object)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(asJSON, &obj); err != nil {
		t.Fatal(err)
	}
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	for _, lib := range []struct {
		name string
		open lua.LGFunction
	}{{lua.LoadLibName, lua.OpenPackage}, {lua.BaseLibName, lua.OpenBase}, {lua.TabLibName, lua.OpenTable}, {lua.StringLibName, lua.OpenString}} {
		L.Push(L.NewFunction(lib.open))
		L.Push(lua.LString(lib.name))
		L.Call(1, 0)
	}
	L.SetGlobal("obj", toLua(L, obj))
	if err := L.DoString(script); err != nil {
		t.Fatalf("health check: %v", err)
	}
	hs, ok := L.Get(-1).(*lua.LTable)
	if !ok {
		t.Fatalf("health check returned %s, want a table", L.Get(-1).Type())
	}
	return lua.LVAsString(hs.RawGetString("status")), lua.LVAsString(hs.RawGetString("message"))
}

// toLua converts a decoded JSON value to a Lua value, as Argo CD's
// object binding does: objects and arrays become tables, numbers numbers.
func toLua(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(x)
	case float64:
		return lua.LNumber(x)
	case string:
		return lua.LString(x)
	case []any:
		tb := L.NewTable()
		for _, e := range x {
			tb.Append(toLua(L, e))
		}
		return tb
	case map[string]any:
		tb := L.NewTable()
		for k, e := range x {
			tb.RawSetString(k, toLua(L, e))
		}
		return tb
	}
	return lua.LNil
}
