package controller

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// minRecloneGtidPrefix is the minimum number of characters of the
// divergent GTID that the admin must include in the reclone annotation
// when the target site has divergent transactions. 8 characters of a
// UUID is enough to be unique per incident without forcing the full
// 36-char UUID into a kubectl annotate command.
const minRecloneGtidPrefix = 8

// RecloneRequest is the parsed form of the
// bloodraven.shipstream.io/reclone-site annotation value. An admin
// writes either:
//
//	reclone-site=<siteName>
//	reclone-site=<siteName>:<divergentGtidPrefix>
//
// The second form is required when the target site has a non-empty
// status.sites[i].divergentGtid — matching the prefix against the
// already-observed divergent GTID proves the admin is acting on the
// state they think they are. See validateRecloneRequest for the full
// rule set.
type RecloneRequest struct {
	Site       string
	GtidPrefix string
}

// parseRecloneAnnotation splits the raw annotation value into a
// RecloneRequest. Whitespace is trimmed and the prefix separator ":"
// is honoured. A value without a separator yields an empty GtidPrefix.
func parseRecloneAnnotation(raw string) RecloneRequest {
	v := strings.TrimSpace(raw)
	if idx := strings.Index(v, ":"); idx >= 0 {
		return RecloneRequest{
			Site:       strings.TrimSpace(v[:idx]),
			GtidPrefix: strings.TrimSpace(v[idx+1:]),
		}
	}
	return RecloneRequest{Site: v}
}

// validateRecloneRequest enforces the safety interlock documented on
// RecloneAnnotation. Returns nil when the request is safe to execute,
// or an error whose message is suitable for a Kubernetes Event (it
// tells the admin exactly how to fix their annotation).
//
// Rules:
//  1. Site must name an entry in spec.sites.
//  2. If the target site has a non-empty divergentGtid in status, the
//     admin MUST include a prefix of at least minRecloneGtidPrefix
//     characters, and that prefix must match the observed GTID.
//  3. When divergentGtid is empty, either the bare site form or
//     site:prefix is accepted (there is nothing irrecoverable to
//     protect against yet; a cold reclone is always allowed).
func validateRecloneRequest(fg *v1alpha1.MysqlFailoverGroup, req RecloneRequest) error {
	if req.Site == "" {
		return fmt.Errorf("reclone annotation is empty; expected <site> or <site>:<divergentGtid-prefix>")
	}

	// Site name must match spec.sites. Built from spec rather than
	// status.sites to catch the case where the CR is freshly created
	// and status is still empty.
	known := false
	for _, s := range fg.Spec.Sites {
		if s.Name == req.Site {
			known = true
			break
		}
	}
	if !known {
		return fmt.Errorf("reclone annotation names unknown site %q; must match one of spec.sites[].name", req.Site)
	}

	// Is this site currently blocked by divergence? Only the presence
	// of divergentGtid matters for the interlock — RecoveryState
	// ("RecoveryBlocked") is a downstream UX field and could be
	// transiently unset during a reconcile.
	var divergentGtid string
	for _, s := range fg.Status.Sites {
		if s.Name == req.Site {
			divergentGtid = s.DivergentGtid
			break
		}
	}
	if divergentGtid == "" {
		// Cold reclone: the admin is asking for CLONE INSTANCE on a
		// site that doesn't have divergence recorded. Accept either
		// form — this path is used for PVC loss, manual rebuilds, and
		// first-time bootstrap mistakes.
		return nil
	}

	if req.GtidPrefix == "" {
		return fmt.Errorf(
			"reclone of %q rejected: site has divergent transactions (gtid %q); "+
				"annotation must include the divergent-GTID prefix to confirm intent — "+
				"use reclone-site=%s:%s",
			req.Site, divergentGtid, req.Site, truncateForHint(divergentGtid, minRecloneGtidPrefix))
	}
	if len(req.GtidPrefix) < minRecloneGtidPrefix {
		return fmt.Errorf(
			"reclone of %q rejected: divergent-GTID prefix %q is shorter than the required %d characters",
			req.Site, req.GtidPrefix, minRecloneGtidPrefix)
	}
	if !strings.HasPrefix(divergentGtid, req.GtidPrefix) {
		return fmt.Errorf(
			"reclone of %q rejected: divergent-GTID prefix %q does not match the observed divergentGtid %q — "+
				"double-check the site name and re-read status.sites[].divergentGtid",
			req.Site, req.GtidPrefix, divergentGtid)
	}
	return nil
}

// truncateForHint returns the first n characters of s without panicking
// on short strings. Used in the validation-error hint so we don't
// leak a truncated UUID mid-byte for exotic inputs.
func truncateForHint(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
