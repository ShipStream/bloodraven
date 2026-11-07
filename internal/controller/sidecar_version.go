package controller

import (
	"context"
	"errors"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// sidecarImageFromEnv is the sidecar image this operator release ships
// with (BLOODRAVEN_SIDECAR_IMAGE, rendered by the chart from
// sidecar.image.repository and sidecar.image.tag). A group that leaves
// spec.sidecar.image empty runs it.
var sidecarImageFromEnv = ""

// SetSidecarImageDefault is called from cmd/bloodraven/main.go with the
// operator's default sidecar image.
func SetSidecarImageDefault(image string) {
	sidecarImageFromEnv = strings.TrimSpace(image)
}

// errSidecarImageUnset refuses to render a group that pins no sidecar
// image when the operator was not told its default.
var errSidecarImageUnset = errors.New("spec.sidecar.image is empty and the operator has no default sidecar image " +
	"(BLOODRAVEN_SIDECAR_IMAGE, set by the chart from sidecar.image.repository and sidecar.image.tag); " +
	"set one of them")

// effectiveSidecarImage returns the group's pinned spec.sidecar.image, or
// the operator's default when the group pins none.
func effectiveSidecarImage(fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	if img := strings.TrimSpace(fg.Spec.Sidecar.Image); img != "" {
		return img, nil
	}
	if sidecarImageFromEnv == "" {
		return "", errSidecarImageUnset
	}
	return sidecarImageFromEnv, nil
}

// The operator and the sidecar are two halves of one contract: the
// operator renders environment variables and volume mounts that the
// sidecar binary has to know how to read. A group whose sidecar image
// (a spec.sidecar.image pin, or a mis-set operator default) is from a
// different release than the running operator gets whichever half of a
// newer contract the older binary happens to understand, and the failure
// is silent at the point of drift.
//
// The sharpest instance is TLS. spec.tls makes the operator set
// require_secure_transport=ON and configure the sidecar's MySQL client
// through BLOODRAVEN_MYSQL_TLS_* env vars. A sidecar too old to read
// them connects in plaintext, is rejected by mysqld with Error 3159,
// and fails its readiness check. Its self-fencing monitor and
// super_read_only safety net keep running but cannot reach MySQL. The
// group reports Degraded while its split-brain guard is off.
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
	sidecarImage, err := effectiveSidecarImage(fg)
	if err != nil {
		return
	}
	operatorTag := imageTag(operatorImageFromEnv)
	sidecarTag := imageTag(sidecarImage)
	if operatorTag == "" || sidecarTag == "" || operatorTag == sidecarTag {
		return
	}
	source := "spec.sidecar.image"
	if fg.Spec.Sidecar.Image == "" {
		source = "the operator's default sidecar image (BLOODRAVEN_SIDECAR_IMAGE)"
	}

	detail := ""
	if fg.Spec.TLS != nil {
		detail = " Because this group sets spec.tls, a sidecar that predates the " +
			"BLOODRAVEN_MYSQL_TLS_* contract cannot reach MySQL at all: the pod will " +
			"never become ready, and self-fencing and the super_read_only safety net " +
			"cannot act."
	}

	log.FromContext(ctx).Info("sidecar image version differs from the operator",
		"operatorImage", operatorImageFromEnv,
		"sidecarImage", sidecarImage,
		"operatorTag", operatorTag,
		"sidecarTag", sidecarTag)

	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventSidecarVersionSkew,
		"%s is tagged %q but the operator is running %q. "+
			"The operator and sidecar share a rendering contract and are only "+
			"supported at matching releases.%s",
		source, sidecarTag, operatorTag, detail)
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
