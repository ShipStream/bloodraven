package controller

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The operator and the sidecar are two halves of one contract: the
// operator renders environment variables and volume mounts that the
// sidecar binary has to know how to read. A group that pins
// spec.sidecarImage to a different release than the running operator
// gets whichever half of a newer contract the older binary happens to
// understand, and the failure is silent at the point of drift.
//
// The sharpest instance is TLS. spec.tls makes the operator set
// require_secure_transport=ON and configure the sidecar's MySQL client
// through BLOODRAVEN_MYSQL_TLS_* env vars. A sidecar too old to read
// them connects in plaintext, is rejected by mysqld with Error 3159,
// fails its health check, and is restarted by the liveness probe — which
// also stops the self-fencing monitor and the super_read_only safety
// net. The group reports Degraded while its split-brain guard is off.
//
// This is a warning, never a block: refusing to reconcile on a version
// string would turn a cosmetic mismatch (a re-tagged image, a local
// build) into an outage, and the operator cannot tell those apart from a
// real skew.

// warnOnSidecarVersionSkew emits a Warning event when the group's
// sidecar image is tagged with a different release than the operator's
// own image. Silent when either tag is unknown — a digest pin, an
// untagged reference, or an operator that was not told its own image are
// all legitimate, and guessing would make the event untrustworthy.
func (r *MysqlFailoverGroupReconciler) warnOnSidecarVersionSkew(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) {
	operatorTag := imageTag(operatorImageFromEnv)
	sidecarTag := imageTag(fg.Spec.SidecarImage)
	if operatorTag == "" || sidecarTag == "" || operatorTag == sidecarTag {
		return
	}

	detail := ""
	if fg.Spec.TLS != nil {
		detail = " Because this group sets spec.tls, a sidecar that predates the " +
			"BLOODRAVEN_MYSQL_TLS_* contract cannot reach MySQL at all: it will fail its " +
			"health check, restart continuously, and leave self-fencing and the " +
			"super_read_only safety net inactive."
	}

	log.FromContext(ctx).Info("sidecar image version differs from the operator",
		"operatorImage", operatorImageFromEnv,
		"sidecarImage", fg.Spec.SidecarImage,
		"operatorTag", operatorTag,
		"sidecarTag", sidecarTag)

	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, "SidecarVersionSkew",
		"spec.sidecarImage is tagged %q but the operator is running %q. "+
			"The operator and sidecar share a rendering contract and are only "+
			"supported at matching releases.%s",
		sidecarTag, operatorTag, detail)
}

// imageTag returns the tag portion of a container image reference, or ""
// when the reference carries no usable tag.
//
// Digest-pinned references return "" on purpose: the digest says nothing
// about the release, so any comparison against it would be noise. So
// does a bare "name" with no tag, where the implied ":latest" tells us
// only that someone opted out of pinning.
func imageTag(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	// A digest pin ends the reference; anything after "@" is not a tag.
	if i := strings.Index(ref, "@"); i >= 0 {
		return ""
	}
	// Only a colon *after* the final slash is a tag separator — a colon
	// before it is a registry port ("registry.local:5000/bloodraven").
	lastSlash := strings.LastIndex(ref, "/")
	lastColon := strings.LastIndex(ref, ":")
	if lastColon < 0 || lastColon < lastSlash {
		return ""
	}
	return ref[lastColon+1:]
}
