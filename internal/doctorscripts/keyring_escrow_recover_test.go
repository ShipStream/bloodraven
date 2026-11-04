package doctorscripts

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fakeKubectl serves just the calls keyring-escrow-recover.sh makes. State
// lives in $STATE: live-keyring (the MySQL pod's file; absent = exec
// fails), secret (the escrow Secret's bytes; absent = NotFound), pods (the
// Running MySQL pods: name<TAB>deletionTimestamp<TAB>mysql-ready; default
// one ready pod), log (every call). A pod listing with any other selector
// returns the site's Dragonfly pod, which has no keyring, the way the old
// selector matched it. Only POSIX tools, so it runs on macOS as well.
const fakeKubectl = `#!/usr/bin/env bash
set -u
echo "$*" >>"$STATE/log"
verb=$1; shift
case "$verb" in
get)
  kind=$1; shift
  case "$kind" in
  mysqlfailovergroup)
    expr=$(printf '%s\n' "$@" | sed -n 's/^jsonpath=//p')
    case "$expr" in
      *keyringSecret*) printf '%s' "$SECRET_NAME" ;;
      *keyringVersion*) printf '2' ;;
      *keyringDigest*) printf '%s' "$RECORDED_DIGEST" ;;
      *dataFileDir*) ;;
      *metadata.uid*) printf 'uid-1' ;;
      *'sites[*].name'*) printf 'dc1 dc2' ;;
    esac ;;
  pod)
    if [[ "$*" == *"-l app.kubernetes.io/name=mysql,app.kubernetes.io/instance=lion,shipstream.io/site=dc2 "* &&
          "$*" == *"--field-selector=status.phase=Running"* ]]; then
      if [[ -f "$STATE/pods" ]]; then cat "$STATE/pods"; else printf 'mysql-lion-dc2-0		true
'; fi
    else
      printf 'lion-dragonfly-dc2-0		true
'
    fi ;;
  secret)
    [[ -f "$STATE/secret" ]] || { echo "NotFound" >&2; exit 1; }
    case "$*" in
      *jsonpath=*) base64 <"$STATE/secret" | tr -d '\n' ;;
      *yaml*) echo "kind: Secret"; echo "data: {keyring: $(base64 <"$STATE/secret" | tr -d '\n')}" ;;
      *) echo "secret/$SECRET_NAME" ;;
    esac ;;
  esac ;;
exec)
  pod=$3
  [[ "$pod" == mysql-lion-dc2-* ]] || { echo "container mysql not found in pod $pod" >&2; exit 1; }
  [[ -f "$STATE/live-keyring" ]] || { echo "pod unavailable" >&2; exit 1; }
  cat "$STATE/live-keyring" ;;
create)
  [[ -f "$STATE/secret" ]] && { echo "AlreadyExists" >&2; exit 1; }
  src=$(printf '%s\n' "$@" | sed -n 's/^--from-file=keyring=//p')
  cp "$src" "$STATE/secret" ;;
delete)
  rm -f "$STATE/secret" ;;
label|annotate|patch) ;;
esac
`

type escrowEnv struct {
	state string
	log   string
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func newEscrowEnv(t *testing.T, live, existing []byte) escrowEnv {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	state := filepath.Join(dir, "state")
	for _, d := range []string{bin, state} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(fakeKubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	if live != nil {
		if err := os.WriteFile(filepath.Join(state, "live-keyring"), live, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if existing != nil {
		if err := os.WriteFile(filepath.Join(state, "secret"), existing, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return escrowEnv{state: state, log: filepath.Join(state, "log")}
}

func (e escrowEnv) run(t *testing.T, recorded string, args ...string) (string, error) {
	t.Helper()
	return e.runWithPath(t, filepath.Join(filepath.Dir(e.state), "bin")+":"+os.Getenv("PATH"), recorded, args...)
}

func (e escrowEnv) runWithPath(t *testing.T, path, recorded string, args ...string) (string, error) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(file), "..", "..", ".agents", "skills", "bloodraven-doctor", "scripts", "keyring-escrow-recover.sh")
	cmd := exec.Command("bash", append([]string{script, "-n", "orders", "lion", "dc2"}, args...)...)
	cmd.Dir = filepath.Dir(e.state)
	cmd.Env = append(os.Environ(),
		"PATH="+path,
		"STATE="+e.state, "SECRET_NAME=mysql-lion-dc2-keyring-v2", "RECORDED_DIGEST="+recorded)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (e escrowEnv) pods(t *testing.T, lines string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(e.state, "pods"), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (e escrowEnv) calls(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(e.log)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(b)
}

func (e escrowEnv) secret(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(e.state, "secret"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mutatingCalls(log string) []string {
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(log), "\n") {
		for _, verb := range []string{"create ", "delete ", "label ", "annotate ", "patch "} {
			if strings.HasPrefix(line, verb) {
				out = append(out, line)
			}
		}
	}
	return out
}

func TestKeyringEscrowRecover(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	keys := []byte("live keyring bytes")
	recorded := digest(keys)

	t.Run("missing secret is recreated from the verified pod keyring", func(t *testing.T) {
		e := newEscrowEnv(t, keys, nil)
		out, err := e.run(t, recorded)
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if got := e.secret(t); string(got) != string(keys) {
			t.Fatalf("escrow Secret = %q, want the pod's keyring", got)
		}
		log := e.calls(t)
		for _, want := range []string{"label secret mysql-lion-dc2-keyring-v2", "annotate secret", "patch mysqlfailovergroup lion -n orders --subresource=status"} {
			if !strings.Contains(log, want) {
				t.Errorf("missing call %q in:\n%s", want, log)
			}
		}
		if strings.Contains(log, "delete ") {
			t.Errorf("recreating a missing Secret deleted something:\n%s", log)
		}
	})

	t.Run("existing corrupt secret is never touched without confirmation", func(t *testing.T) {
		corrupt := []byte("tampered")
		e := newEscrowEnv(t, keys, corrupt)
		out, err := e.run(t, recorded)
		if err == nil {
			t.Fatalf("script succeeded on an existing corrupt Secret:\n%s", out)
		}
		if muts := mutatingCalls(e.calls(t)); len(muts) != 0 {
			t.Fatalf("mutating calls without confirmation: %v", muts)
		}
		if string(e.secret(t)) != string(corrupt) {
			t.Fatal("the existing Secret changed")
		}
		if !strings.Contains(out, "--replace-corrupt mysql-lion-dc2-keyring-v2") {
			t.Errorf("refusal does not say how to confirm:\n%s", out)
		}
	})

	t.Run("confirmation must name the exact secret", func(t *testing.T) {
		e := newEscrowEnv(t, keys, []byte("tampered"))
		if out, err := e.run(t, recorded, "--replace-corrupt", "some-other-secret"); err == nil {
			t.Fatalf("script accepted a mismatched confirmation:\n%s", out)
		}
		if muts := mutatingCalls(e.calls(t)); len(muts) != 0 {
			t.Fatalf("mutating calls on a mismatched confirmation: %v", muts)
		}
	})

	t.Run("confirmed replacement backs up first, then recreates", func(t *testing.T) {
		e := newEscrowEnv(t, keys, []byte("tampered"))
		out, err := e.run(t, recorded, "--replace-corrupt", "mysql-lion-dc2-keyring-v2")
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		backups, _ := filepath.Glob(filepath.Join(filepath.Dir(e.state), "keyring-escrow-backup-mysql-lion-dc2-keyring-v2-*.yaml"))
		if len(backups) != 1 {
			t.Fatalf("backups = %v, want exactly one", backups)
		}
		log := e.calls(t)
		if strings.Index(log, "-o yaml") > strings.Index(log, "delete secret") {
			t.Fatalf("Secret deleted before it was backed up:\n%s", log)
		}
		if got := e.secret(t); string(got) != string(keys) {
			t.Fatalf("escrow Secret = %q, want the pod's keyring", got)
		}
	})

	t.Run("an intact existing secret is left alone even when confirmed", func(t *testing.T) {
		e := newEscrowEnv(t, keys, keys)
		out, err := e.run(t, recorded, "--replace-corrupt", "mysql-lion-dc2-keyring-v2")
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if muts := mutatingCalls(e.calls(t)); len(muts) != 0 {
			t.Fatalf("an intact Secret was modified: %v", muts)
		}
	})

	t.Run("unreadable pod keyring writes nothing", func(t *testing.T) {
		e := newEscrowEnv(t, nil, []byte("tampered"))
		if out, err := e.run(t, recorded, "--replace-corrupt", "mysql-lion-dc2-keyring-v2"); err == nil {
			t.Fatalf("script succeeded without the pod's keyring:\n%s", out)
		}
		if muts := mutatingCalls(e.calls(t)); len(muts) != 0 {
			t.Fatalf("mutating calls without a readable keyring: %v", muts)
		}
	})

	t.Run("selects the site's MySQL pod, not Dragonfly, a terminating or an unready one", func(t *testing.T) {
		e := newEscrowEnv(t, keys, nil)
		e.pods(t, "mysql-lion-dc2-old\t2026-01-01T00:00:00Z\ttrue\nmysql-lion-dc2-starting\t\tfalse\nmysql-lion-dc2-new\t\ttrue\n")
		out, err := e.run(t, recorded)
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if log := e.calls(t); !strings.Contains(log, "exec -n orders mysql-lion-dc2-new -c mysql") {
			t.Fatalf("did not read the keyring from the one Ready, non-terminating MySQL pod:\n%s", log)
		}
	})

	t.Run("two Ready MySQL pods: refuses to guess, writes nothing", func(t *testing.T) {
		e := newEscrowEnv(t, keys, nil)
		e.pods(t, "mysql-lion-dc2-a\t\ttrue\nmysql-lion-dc2-b\t\ttrue\n")
		out, err := e.run(t, recorded)
		if err == nil || !strings.Contains(out, "--pod") {
			t.Fatalf("script did not refuse an ambiguous pod choice (err=%v):\n%s", err, out)
		}
		if strings.Contains(e.calls(t), "exec ") || len(mutatingCalls(e.calls(t))) != 0 {
			t.Fatalf("touched a pod or the API on an ambiguous choice:\n%s", e.calls(t))
		}
	})

	t.Run("--pod names the pod explicitly when the choice is ambiguous", func(t *testing.T) {
		e := newEscrowEnv(t, keys, nil)
		e.pods(t, "mysql-lion-dc2-a\t\ttrue\nmysql-lion-dc2-b\t\tfalse\nmysql-lion-dc2-c\t\ttrue\n")
		out, err := e.run(t, recorded, "--pod", "mysql-lion-dc2-b")
		if err != nil {
			t.Fatalf("script failed: %v\n%s", err, out)
		}
		if !strings.Contains(e.calls(t), "exec -n orders mysql-lion-dc2-b -c mysql") {
			t.Fatalf("did not use the named pod:\n%s", e.calls(t))
		}
		if out, err := newEscrowEnv(t, keys, nil).run(t, recorded, "--pod", "lion-dragonfly-dc2-0"); err == nil {
			t.Fatalf("accepted a --pod that is not the site's MySQL pod:\n%s", out)
		}
	})

	// macOS and BSD have no sha256sum; the script falls back to shasum.
	t.Run("works without GNU sha256sum", func(t *testing.T) {
		tools := []string{"bash", "cut", "base64", "awk", "grep", "mktemp", "rm", "date", "cat", "sed", "tr", "cp", "shasum", "perl"}
		e := newEscrowEnv(t, keys, nil)
		bin := filepath.Join(filepath.Dir(e.state), "bin")
		for _, tool := range tools {
			p, err := exec.LookPath(tool)
			if err != nil {
				t.Skipf("%s not available", tool)
			}
			if err := os.Symlink(p, filepath.Join(bin, tool)); err != nil {
				t.Fatal(err)
			}
		}
		out, err := e.runWithPath(t, bin, recorded)
		if err != nil {
			t.Fatalf("script failed without sha256sum: %v\n%s", err, out)
		}
		if got := e.secret(t); string(got) != string(keys) {
			t.Fatalf("escrow Secret = %q, want the pod's keyring", got)
		}
	})

	t.Run("pod keyring not matching status writes nothing", func(t *testing.T) {
		e := newEscrowEnv(t, []byte("diverged"), nil)
		if out, err := e.run(t, recorded); err == nil {
			t.Fatalf("script succeeded with a diverged pod keyring:\n%s", out)
		}
		if muts := mutatingCalls(e.calls(t)); len(muts) != 0 {
			t.Fatalf("mutating calls with a diverged keyring: %v", muts)
		}
	})
}
