package controller

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/shipstream/bloodraven/internal/platform"
)

// operatorBinaryPath is where the operator image installs the bloodraven
// binary (Dockerfile: `COPY --from=builder /app/bloodraven /bloodraven`).
// The image is distroless with no PATH entry for it, so every container
// that runs operatorImageFromEnv with an explicit Command must start
// with this absolute path. TestOperatorBinaryPathMatchesDockerfile pins
// it to the Dockerfile.
const operatorBinaryPath = "/bloodraven"

// operatorCommand returns a container Command that runs a bloodraven
// subcommand from the operator image.
func operatorCommand(args ...string) []string {
	return append([]string{operatorBinaryPath}, args...)
}

// groupReadOnlyTolerations returns the tolerations every operator-built
// pod of a failover group carries: its own group's read-only taint
// (shipstream.io/db-readonly-<group>), any effect. The taint exists to
// evict write-dependent application pods from the demoted site; the
// group's own MySQL, Dragonfly, backup, verification, restore and
// cleanup pods are not such workloads and must not be evicted by it.
// Other groups' taint keys and the legacy unscoped key are deliberately
// not tolerated, matching the MySQL Deployment.
func groupReadOnlyTolerations(group string) []corev1.Toleration {
	return []corev1.Toleration{{
		Key:      platform.TaintKeyForGroup(group),
		Operator: corev1.TolerationOpExists,
	}}
}
