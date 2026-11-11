package doctorscripts

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// ruleKubectl answers every call from $RULES: lines "<glob><TAB><file>",
// where the first glob matching the whole argument list ("$*") names the
// file to print (an empty file name prints nothing and exits 1, the way a
// failed exec does). Calls no rule matches print nothing and exit 0, which
// is what kubectl -o jsonpath does for a missing field. Every call is
// appended to $STATE/log.
const ruleKubectl = `#!/usr/bin/env bash
set -u
echo "$*" >>"$STATE/log"
while IFS=$'\t' read -r pattern file; do
  [[ -z "$pattern" ]] && continue
  # shellcheck disable=SC2053
  if [[ "$*" == $pattern ]]; then
    [[ -z "$file" ]] && exit 1
    cat "$STATE/$file"
    exit 0
  fi
done <"$RULES"
exit 0
`

type ruleEnv struct {
	dir, state, rules string
}

func newRuleEnv(t *testing.T) ruleEnv {
	t.Helper()
	dir := t.TempDir()
	e := ruleEnv{dir: dir, state: filepath.Join(dir, "state"), rules: filepath.Join(dir, "rules")}
	for _, d := range []string{filepath.Join(dir, "bin"), e.state} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "kubectl"), []byte(ruleKubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(e.rules, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return e
}

// rule appends a rule printing content for calls matching glob ("" content
// with fail=true makes the call fail).
func (e ruleEnv) rule(t *testing.T, glob, content string, fail bool) {
	t.Helper()
	name := ""
	if !fail {
		f, err := os.CreateTemp(e.state, "out-*")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(content); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		name = filepath.Base(f.Name())
	}
	f, err := os.OpenFile(e.rules, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(glob + "\t" + name + "\n"); err != nil {
		t.Fatal(err)
	}
}

func scriptPath(name string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", ".agents", "skills", "bloodraven-doctor", "scripts", name)
}

func (e ruleEnv) run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", args...)
	cmd.Dir = e.dir
	cmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(e.dir, "bin")+":"+os.Getenv("PATH"),
		"STATE="+e.state, "RULES="+e.rules)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// The sidecar image has no wget or curl: the helper must fall through to
// another container of the pod (the mysql image ships curl), and then to
// the API server's pod proxy, and report what it tried when all fail.
func TestSidecarHTTPGetFallsBackFromDistrolessSidecar(t *testing.T) {
	const body = `{"site":"dc1"}`
	cases := []struct {
		name    string
		rules   func(e ruleEnv)
		wantOK  bool
		wantVia string
	}{
		{
			name: "curl in the mysql container",
			rules: func(e ruleEnv) {
				e.rule(t, "exec * -c sidecar -- *", "", true)
				e.rule(t, "exec * -c mysql -- wget *", "", true)
				e.rule(t, "exec * -c mysql -- curl *", body, false)
			},
			wantOK: true, wantVia: "container mysql",
		},
		{
			name: "pod proxy when no container can fetch",
			rules: func(e ruleEnv) {
				e.rule(t, "exec *", "", true)
				e.rule(t, "get --raw /api/v1/namespaces/orders/pods/mysql-lion-dc1-0:8080/proxy/archiver/status", body, false)
			},
			wantOK: true, wantVia: "pod proxy",
		},
		{
			name: "nothing answers",
			rules: func(e ruleEnv) {
				e.rule(t, "exec *", "", true)
				e.rule(t, "get --raw *", "", true)
			},
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newRuleEnv(t)
			e.rule(t, "get pod mysql-lion-dc1-0 -n orders -o jsonpath=*", "mysql sidecar", false)
			tc.rules(e)
			script := `source "$1"; if sidecar_http_get orders mysql-lion-dc1-0 /archiver/status; then printf 'OK|%s|%s\n' "$SIDECAR_HTTP_VIA" "$SIDECAR_HTTP_BODY"; else printf 'FAIL|%s\n' "$SIDECAR_HTTP_TRIED"; fi`
			out, err := e.run(t, "-c", "set -euo pipefail; "+script, "sh", scriptPath("lib-sidecar-http.sh"))
			if err != nil {
				t.Fatalf("helper failed: %v\n%s", err, out)
			}
			if tc.wantOK {
				if want := "OK|" + tc.wantVia + "|" + body; strings.TrimSpace(out) != want {
					t.Fatalf("got %q, want %q", out, want)
				}
				return
			}
			if want := "FAIL|sidecar, mysql, pod proxy"; strings.TrimSpace(out) != want {
				t.Fatalf("got %q, want %q", out, want)
			}
		})
	}
}

// support-bundle.sh writes the sidecar's /archiver/status through the
// fallback, verbatim (so the tail block of #264 is in the bundle), and an
// explicit error object (never an empty file) when nothing could fetch it.
func TestSupportBundleArchiverStatus(t *testing.T) {
	const status = `{"site":"dc1","tail":{"state":"Pending","activeFile":"mysql-bin.000012","newestSealedFile":"mysql-bin.000011","pendingFiles":1}}`
	for _, tc := range []struct {
		name string
		curl bool
		want string
	}{
		{name: "fetched through the mysql container", curl: true, want: status},
		{name: "unavailable", want: `{"error":"/archiver/status unavailable","tried":"sidecar, mysql, pod proxy"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newRuleEnv(t)
			e.rule(t, "get pods -n orders -l app.kubernetes.io/instance=lion,app.kubernetes.io/name=mysql -o jsonpath=*", "mysql-lion-dc1-0", false)
			e.rule(t, "get pod mysql-lion-dc1-0 -n orders -o jsonpath=*", "sidecar mysql", false)
			if tc.curl {
				e.rule(t, "exec -n orders mysql-lion-dc1-0 -c mysql -- curl *", status, false)
			}
			e.rule(t, "exec *", "", true)
			e.rule(t, "get --raw *", "", true)
			out := filepath.Join(e.dir, "bundle.tar.gz")
			if log, err := e.run(t, scriptPath("support-bundle.sh"), "-n", "orders", "-o", out, "lion"); err != nil {
				t.Fatalf("support-bundle.sh failed: %v\n%s", err, log)
			}
			got := bundleFile(t, out, "mysql-lion-dc1-0-archiver-status.json")
			if strings.TrimSpace(got) != tc.want {
				t.Fatalf("archiver status = %q, want %q", got, tc.want)
			}
		})
	}
}

func bundleFile(t *testing.T, tarball, base string) string {
	t.Helper()
	f, err := os.Open(tarball)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			t.Fatalf("%s not in bundle", base)
		}
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Base(h.Name) == base {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			return string(b)
		}
	}
}

// triage.sh reads /archiver/status through the fallback and lists PITR
// Warnings recorded on the group's verifications, not only on the group.
// It prints each site's tail block (#264): a read-only site that does not
// archive its tail points at playbook 46, and a sidecar that predates the
// block is "not-reported", never taken for drained. It prints each site's
// reissuableGtids (#271): a set points at playbook 41, an empty verdict is
// "none", and a status without the field is "not-evaluated", never "none".
func TestTriageArchiverStatusAndVerificationEvents(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not on PATH")
	}
	e := newRuleEnv(t)
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o json", "{}", false)
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath={.status.pitr.archive.state}*", "Verified ok archiveID=a1", false)
	e.rule(t, "get pods -n orders -l app.kubernetes.io/instance=lion,app.kubernetes.io/name=mysql -o jsonpath=*", "mysql-lion-dc1-0 mysql-lion-dc2-0 mysql-lion-dc3-0", false)
	e.rule(t, "get pod mysql-lion-dc?-0 -n orders -o jsonpath=*", "sidecar mysql", false)
	// dc1: a read-only former primary whose tail is refused, with divergent
	// rows its server cannot reissue. dc2: the primary, drained, whose
	// server could reissue archived GTIDs. dc3: a sidecar that predates the
	// tail block and the reissue verdict.
	e.rule(t, "exec -n orders mysql-lion-dc1-0 -c mysql -- curl *",
		`{"site":"dc1","primary":false,"archive":{"state":"Verified"},"divergentRows":2,"reissuableGtids":"","tail":{"state":"Blocked","activeFile":"mysql-bin.000012","newestSealedFile":"mysql-bin.000011","pendingFiles":1,"reason":"this site's latest record is not a promotion record"}}`, false)
	e.rule(t, "exec -n orders mysql-lion-dc2-0 -c mysql -- curl *",
		`{"site":"dc2","primary":true,"archive":{"state":"Verified"},"divergentRows":1,"reissuableGtids":"aaaaaaaa-0000-4000-8000-00000000000a:9-10","tail":{"state":"Drained","activeFile":"mysql-bin.000004","newestSealedFile":"mysql-bin.000003","pendingFiles":0}}`, false)
	e.rule(t, "exec -n orders mysql-lion-dc3-0 -c mysql -- curl *", `{"site":"dc3","primary":false,"archive":{"state":"Verified"}}`, false)
	e.rule(t, "exec *", "", true)
	e.rule(t, "get events -n orders --field-selector involvedObject.name=lion *",
		"2026-11-05T11:00:00Z   Warning   PITRArchiveTailNotArchived   1   site dc1 is being recloned although its binary log tail was not confirmed archived within 2m0s\n", false)
	e.rule(t, "get mysqlbackupverifications -n orders -l bloodraven.shipstream.io/failover-group=lion -o jsonpath=*", "verify-1", false)
	e.rule(t, "get events -n orders --field-selector involvedObject.kind=MysqlBackupVerification,involvedObject.name=verify-1 *",
		"2026-11-05T10:00:00Z   Warning   PITROnHistoryTransactionsExcluded   1   excluded 3 transactions on the target history\n", false)
	out, err := e.run(t, scriptPath("triage.sh"), "-n", "orders", "lion")
	if err != nil {
		t.Fatalf("triage.sh failed: %v\n%s", err, out)
	}
	for _, want := range []string{
		"mysql-lion-dc1-0 site=dc1 archive=Verified/",
		"divergentRows=2 quarantinedRows=0 reissuableGtids=none tail=Blocked active=mysql-bin.000012 newestSealed=mysql-bin.000011 pending=1 reason=this site's latest record is not a promotion record (read-only site not archiving its tail: playbook 46)",
		"mysql-lion-dc2-0 site=dc2 archive=Verified/",
		"divergentRows=1 quarantinedRows=0 reissuableGtids=aaaaaaaa-0000-4000-8000-00000000000a:9-10 (this server could reissue archived GTIDs: playbook 41) tail=Drained active=mysql-bin.000004 newestSealed=mysql-bin.000003 pending=0\n",
		"mysql-lion-dc3-0 site=dc3 archive=Verified/",
		"divergentRows=0 quarantinedRows=0 reissuableGtids=not-evaluated tail=not-reported\n",
		"PITRArchiveTailNotArchived: playbook 46",
		"2026-11-05T11:00:00Z   Warning   PITRArchiveTailNotArchived   1   site dc1 is being recloned",
		"[verification/verify-1] 2026-11-05T10:00:00Z   Warning   PITROnHistoryTransactionsExcluded",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("triage output lacks %q:\n%s", want, out)
		}
	}
}

// gtid-audit.sh --manifest: candidates include quarantined rows' divergent
// sets, a missingGtids that is not a GTID set never reaches the printed SQL,
// and the SQL subtracts the dump's own @.json set, not the status set.
func TestGTIDAuditManifestCandidates(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not on PATH")
	}
	const (
		a = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		b = "4e11fa47-71ca-11e1-9e33-c80aa9429562"
	)
	manifest := `{"version":2,"site":"dc1","archiveId":"x",
	  "promotions":[{"serverUuid":"` + b + `","serverId":2,"observedAt":"2026-11-05T10:00:00Z","gtidExecuted":"` + a + `:1-10","probeInterval":"5s","readOnlyGtidExecuted":"` + a + `:1-9"},
	                {"kind":"Reset","serverUuid":"` + b + `","serverId":2,"observedAt":"2026-11-05T11:00:00Z","gtidExecuted":"` + a + `:1-3"}],
	  "divergent":[{"name":"mysql-bin.000004","id":"r4","missingGtids":"` + a + `:11-12"},
	               {"name":"mysql-bin.000005","id":"r5","missingGtids":"x'); DROP TABLE t; -- "},
	               {"name":"mysql-bin.000006","id":"r6","missingGtids":17}],
	  "quarantined":[{"entry":{"name":"mysql-bin.000007","id":"r7"},"at":"2026-11-05T09:00:00Z","reason":"lost","resumeGtids":"",
	                  "divergent":{"name":"mysql-bin.000007","missingGtids":"` + b + `:1-4"}}]}`
	e := newRuleEnv(t)
	path := filepath.Join(e.dir, "manifest-dc1.json")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath=*spec.sites*", "dc1 dc2", false)
	out, _ := e.run(t, scriptPath("gtid-audit.sh"), "-n", "orders", "--manifest", path, "lion")
	joined := a + ":11-12," + b + ":1-4"
	for _, want := range []string{
		"Promotion " + b + " 2 2026-11-05T10:00:00Z 5s " + a + ":1-10 [read-only before: " + a + ":1-9]",
		"Reset " + b + " 2 2026-11-05T11:00:00Z unknown " + a + ":1-3",
		"mysql-bin.000007 r7 2026-11-05T09:00:00Z lost [divergent: " + b + ":1-4]",
		"divergent row mysql-bin.000005: missingGtids is not a GTID set, not used",
		"divergent row mysql-bin.000006: missingGtids is not a GTID set, not used",
		"SELECT GTID_SUBTRACT('" + joined + "', '<gtidExecuted from the dump's @.json>');",
		"BLOODRAVEN_PITR_DIVERGENT_SET",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("gtid-audit output lacks %q:\n%s", want, out)
		}
	}
	for _, bad := range []string{"GTID_SUBTRACT('x'", "DROP TABLE t; -- ',", "status.gtidExecuted>"} {
		if strings.Contains(out, bad) {
			t.Errorf("gtid-audit output contains %q:\n%s", bad, out)
		}
	}
}
