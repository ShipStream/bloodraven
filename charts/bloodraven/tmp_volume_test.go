package bloodraven_test

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

// The operator's root filesystem is read-only, and the MysqlStandbyCluster
// reconciler stages a bucket's credentials under /tmp. The chart mounts an
// in-memory volume there, unless the values already mount something at
// /tmp: two mounts at one path make the pod invalid.
func TestOperatorTmpVolumeChart(t *testing.T) {
	requireHelm(t)
	render := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("helm", append([]string{"template", "test", ".", "--namespace", "operator-system"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("helm template: %v: %s", err, out)
		}
		return string(out)
	}

	def := render()
	if !strings.Contains(def, "readOnlyRootFilesystem: true") {
		t.Fatal("the operator's root filesystem is no longer read-only; this test assumes it is")
	}
	if strings.Count(def, "mountPath: /tmp") != 1 || !strings.Contains(def, "name: bloodraven-tmp\n          emptyDir:\n            medium: Memory") {
		t.Fatalf("the chart does not mount an in-memory volume at /tmp:\n%s", def)
	}

	own := render("--set", "extraVolumes[0].name=scratch", "--set", "extraVolumes[0].emptyDir.medium=Memory",
		"--set", "extraVolumeMounts[0].name=scratch", "--set", "extraVolumeMounts[0].mountPath=/tmp")
	if strings.Count(own, "mountPath: /tmp") != 1 || bytes.Contains([]byte(own), []byte("bloodraven-tmp")) {
		t.Fatalf("with a /tmp mount in the values the chart must not add its own:\n%s", own)
	}

	// Values of a release before this one, as --reuse-values renders them.
	if old := render("--set", "extraVolumeMounts=null", "--set", "extraVolumes=null"); strings.Count(old, "mountPath: /tmp") != 1 {
		t.Fatalf("without extraVolumeMounts in the values the chart does not mount /tmp:\n%s", old)
	}
}
