package doctorscripts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// triage.sh prints the PITRArchiveBindingReady condition with the playbook
// hint for its reason, and the bindings table (id, state/reason, mode,
// keySecretName, archiveID, destination), and never reads a Secret.
func TestTriagePITRBindings(t *testing.T) {
	const bindings = "pab-0123456789abcdef\tBound/Bound\tPassphrase\tmysql-lion-pitr-key-a1b2c3d4e5\tar1-abcdefghijklmnopqrstuvwxyz\ts3://pitr/p1/\n" +
		"pab-fedcba9876543210\tRefused/KeyMismatch\tPassphrase\tmysql-lion-pitr-key-0f0f0f0f0f\t\ts3://pitr/p2/\n"
	for _, tc := range []struct {
		name, condition string
		want            []string
		notWant         []string
	}{
		{
			name:      "key change refused",
			condition: "False KeyChangeRefused: profile a: passphrase differs from the accepted key snapshot",
			want: []string{
				"PITRArchiveBindingReady: False KeyChangeRefused: profile a",
				"pods still use the accepted key snapshot. See playbook 26",
			},
		},
		{
			name:      "key material lost",
			condition: "False KeyMaterialLost: Secret mysql-lion-pitr-key-a1b2c3d4e5 does not hold the accepted key",
			want:      []string{"mysqld keeps running, the archiver fails closed"},
		},
		{
			name:      "refused binding",
			condition: "False KeyMismatch: recover the archive's original passphrase",
			want:      []string{"A binding was refused"},
		},
		{
			// The message is free text: an "Archive..." word in it must not
			// trigger a refusal hint on a True condition.
			name:      "bound, message mentions an archive",
			condition: "True Bound: no full backup is stamped with archive ArchiveX yet",
			want:      []string{"PITRArchiveBindingReady: True Bound"},
			notWant:   []string{"See playbook 26"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newRuleEnv(t)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o json", "{}", false)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath=*PITRArchiveBindingReady*", tc.condition, false)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath=*pitrArchiveBindings*", bindings, false)
			e.rule(t, "get secret*", "", true)
			out, err := e.run(t, scriptPath("triage.sh"), "-n", "orders", "lion")
			if err != nil {
				t.Fatalf("triage.sh failed: %v\n%s", err, out)
			}
			want := append([]string{
				"ID\tSTATE/REASON\tMODE\tKEY_SECRET\tARCHIVE_ID\tDESTINATION",
				"  pab-0123456789abcdef\tBound/Bound\tPassphrase\tmysql-lion-pitr-key-a1b2c3d4e5\tar1-abcdefghijklmnopqrstuvwxyz\ts3://pitr/p1/",
				"  pab-fedcba9876543210\tRefused/KeyMismatch",
			}, tc.want...)
			for _, w := range want {
				if !strings.Contains(out, w) {
					t.Errorf("triage output lacks %q:\n%s", w, out)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(out, w) {
					t.Errorf("triage output contains %q:\n%s", w, out)
				}
			}
			assertNoSecretRead(t, e)
		})
	}
}

// support-bundle.sh lists the key snapshot Secrets by label through
// custom-columns only (never -o yaml/json, which would include data), and
// the pitr-archive-check Jobs and pods.
func TestSupportBundlePITRKeySnapshotsMetadataOnly(t *testing.T) {
	e := newRuleEnv(t)
	e.rule(t, "get secrets -n orders -l app.kubernetes.io/name=mysql-pitr-key,shipstream.io/failover-group=lion -o custom-columns=*",
		"NAME                             IMMUTABLE\nmysql-lion-pitr-key-a1b2c3d4e5   true\n", false)
	e.rule(t, "get secrets -n orders -l app.kubernetes.io/name=mysql-dump-key,shipstream.io/failover-group=lion -o custom-columns=*",
		"NAME                                         IMMUTABLE\nmysqlbackup-nightly-1-dump-key-1a2b3c4d   true\n", false)
	e.rule(t, "get jobs -n orders -l shipstream.io/failover-group=lion,shipstream.io/resource=pitr-archive-check -o custom-columns=*",
		"NAME                         KEY_SECRET\nmysql-lion-pitr-check-0a1b2c3d   mysql-lion-pitr-key-a1b2c3d4e5\n", false)
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o custom-columns=ID:.status.pitrArchiveBindings*",
		"ID                     STATE\npab-0123456789abcdef   Bound\n", false)
	e.rule(t, "get secret*", "", true)
	out := filepath.Join(e.dir, "bundle.tar.gz")
	if log, err := e.run(t, scriptPath("support-bundle.sh"), "-n", "orders", "-o", out, "lion"); err != nil {
		t.Fatalf("support-bundle.sh failed: %v\n%s", err, log)
	}
	for file, want := range map[string]string{
		"mysql-pitr-key-secrets.txt":  "mysql-lion-pitr-key-a1b2c3d4e5",
		"mysql-dump-key-secrets.txt":  "mysqlbackup-nightly-1-dump-key-1a2b3c4d",
		"pitr-archive-check-jobs.txt": "mysql-lion-pitr-check-0a1b2c3d",
		"pitr-bindings.txt":           "pab-0123456789abcdef",
	} {
		if got := bundleFile(t, out, file); !strings.Contains(got, want) {
			t.Errorf("%s = %q, want it to contain %q", file, got, want)
		}
	}
	// Every Secret read is a label-selected list projected through
	// custom-columns, and no column reads data or the acceptance annotation.
	for _, call := range kubectlCalls(t, e) {
		if !strings.HasPrefix(call, "get secret") {
			continue
		}
		if !strings.Contains(call, " -o custom-columns=") {
			t.Errorf("Secret read without custom-columns: %q", call)
		}
		for _, bad := range []string{".data", "stringData", "pitr-accepted"} {
			if strings.Contains(call, bad) {
				t.Errorf("Secret read projects %q: %q", bad, call)
			}
		}
	}
}

func kubectlCalls(t *testing.T, e ruleEnv) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(e.state, "log"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func assertNoSecretRead(t *testing.T, e ruleEnv) {
	t.Helper()
	for _, call := range kubectlCalls(t, e) {
		if strings.HasPrefix(call, "get secret") {
			t.Errorf("triage.sh read a Secret: %q", call)
		}
	}
}
