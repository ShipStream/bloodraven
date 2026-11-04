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
// lives in $STATE: live-keyring (the pod's file; absent = exec fails),
// secret (the escrow Secret's bytes; absent = NotFound), log (every call).
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
  pod) printf 'mysql-lion-dc2-0' ;;
  secret)
    [[ -f "$STATE/secret" ]] || { echo "NotFound" >&2; exit 1; }
    case "$*" in
      *jsonpath=*) base64 -w0 "$STATE/secret" ;;
      *yaml*) echo "kind: Secret"; echo "data: {keyring: $(base64 -w0 "$STATE/secret")}" ;;
      *) echo "secret/$SECRET_NAME" ;;
    esac ;;
  esac ;;
exec)
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
	_, file, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(file), "..", "..", ".agents", "skills", "bloodraven-doctor", "scripts", "keyring-escrow-recover.sh")
	cmd := exec.Command("bash", append([]string{script, "-n", "orders", "lion", "dc2"}, args...)...)
	cmd.Dir = filepath.Dir(e.state)
	cmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(filepath.Dir(e.state), "bin")+":"+os.Getenv("PATH"),
		"STATE="+e.state, "SECRET_NAME=mysql-lion-dc2-keyring-v2", "RECORDED_DIGEST="+recorded)
	out, err := cmd.CombinedOutput()
	return string(out), err
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
