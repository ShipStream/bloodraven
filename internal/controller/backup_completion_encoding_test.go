package controller

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/backupmeta"
)

func TestParseDumpCompleteLine_LegacyIdentity(t *testing.T) {
	line := "BLOODRAVEN_DUMP_COMPLETE location=prod_backups/a%20b+c/ size=1.4_GiB gtidExecuted=uuid:my_tag:1-3 binlogFile=mysql_bin.000001"
	meta, ok := parseDumpCompleteLine(line)
	if !ok || meta.Location != "prod_backups/a%20b+c/" || meta.GtidExecuted != "uuid:my_tag:1-3" || meta.BinlogFile != "mysql_bin.000001" || meta.Size != "1.4 GiB" {
		t.Fatalf("legacy artifact identity changed: ok=%v meta=%+v", ok, meta)
	}
}

func TestDumpPythonToken_PreservesIdentity(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, dumpScript,
		"print(job['_format_dump_complete']('prod_backups/seed', 0, '', {'gtidExecuted': 'uuid:my_tag:1-3'}))\n", nil)
	if err != nil {
		t.Fatalf("encode: %v: %s", err, out)
	}
	meta, ok := parseDumpCompleteLine(string(out))
	if !ok || meta.Location != "prod_backups/seed" || meta.GtidExecuted != "uuid:my_tag:1-3" {
		t.Fatalf("Python producer lost artifact identity: ok=%v meta=%+v", ok, meta)
	}
}

func TestDumpPythonCompletion_RoundTrip(t *testing.T) {
	for _, value := range []string{"prod_backups/seed", "/backups/db dumps/seed", "s3://bucket/a_b/", "a%b+c=d", "tab\there\nnext\rline", "備份/seed", "", "1.4 GiB", "uuid:my_tag:1-3,uuid2:1-5"} {
		t.Run(value, func(t *testing.T) {
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			out, err := runEmbeddedBackupPython(t, dumpScript, "import json, os\nv = json.loads(os.environ['TEST_VALUE'])\nprint(job['_format_dump_complete'](v, 1024, '1.0 KiB', {'gtidExecuted': v, 'binlogFile': v, 'binlogPos': 42}))\n", map[string]string{"TEST_VALUE": string(encoded)})
			if err != nil {
				t.Fatalf("format Python completion: %v: %s", err, out)
			}
			meta, ok := parseDumpCompleteLine(string(out))
			if !ok || meta.Location != value || meta.GtidExecuted != value || meta.BinlogFile != value || meta.BinlogPos != 42 || meta.SizeBytes != 1024 || meta.Size != "1.0 KiB" {
				t.Fatalf("Python producer failed round trip: ok=%v meta=%+v", ok, meta)
			}
		})
	}
}

func TestVerifyScriptSentinelEncoding(t *testing.T) {
	start := strings.Index(verifyScript, "escape_sentinel_token() {")
	if start < 0 {
		t.Fatal("missing verification encoder")
	}
	end := strings.Index(verifyScript[start:], "\n}\n")
	if end < 0 {
		t.Fatal("missing encoder function boundary")
	}
	function := verifyScript[start : start+end+3]
	for _, value := range []string{"order_count", "ERROR_1146: table_name does not exist", "a%20b+c=d", "\tline\r\nnext\n", "備份"} {
		t.Run(value, func(t *testing.T) {
			cmd := exec.Command("bash", "-c", function+`printf 'BLOODRAVEN_VERIFY_SANITY_COMPLETE v=2 ran=1 durationMs=12 resultRow=%s error=%s\n' "$(escape_sentinel_token "$VALUE")" "$(escape_sentinel_token "$VALUE")"`)
			cmd.Env = []string{"VALUE=" + value}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("verification encoder: %v: %s", err, out)
			}
			res, ok := parseSanitySentinel(string(out))
			if !ok || res.ResultRow != value || res.Error != value || !res.Ran || res.DurationMs != 12 {
				t.Fatalf("sanity round trip failed: ok=%v result=%+v", ok, res)
			}
			if !strings.Contains(string(out), "resultRow="+backupmeta.EscapeValue(value)) {
				t.Fatalf("Bash encoder differs from Go: %s", out)
			}
		})
	}
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(verifyScript)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("verify.sh syntax: %v: %s", err, out)
	}
}

func TestParseSanitySentinel_LiteralUnderscore(t *testing.T) {
	res, ok := parseSanitySentinel("BLOODRAVEN_VERIFY_SANITY_COMPLETE ran=1 resultRow=order_count")
	if !ok || res.ResultRow != "order_count" {
		t.Fatalf("scalar identity changed: ok=%v result=%+v", ok, res)
	}
}
