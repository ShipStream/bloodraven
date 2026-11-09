package controller

import (
	"fmt"
	"regexp"
	"strings"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/actionvalue"
)

// minRecloneGtidPrefix is the minimum number of characters of the
// divergent GTID that the admin must include in the reclone annotation
// when the target site has divergent transactions. 8 characters of a
// UUID is enough to be unique per incident without forcing the full
// 36-char UUID into a kubectl annotate command.
const minRecloneGtidPrefix = 8

// RecloneRequest is the parsed form of the
// bloodraven.shipstream.io/reclone-site annotation value, in A-78's grammar
// (internal/actionvalue). A reclone wipes the site's datadir, and its
// subject is a site name, so both forms carry confirm= and uid=:
//
//	reclone-site=<site>:confirm=<group name>:uid=<group uid>
//	reclone-site=<site>:confirm=<group name>:uid=<group uid>:gtidPrefix=<p>
//
// The first form is the cold reclone of a site with no recorded
// divergence. The second is required when the site has a non-empty
// status.sites[i].divergentGtid: gtidPrefix, a prefix of the divergent
// GTID's source UUID, proves the requester read the divergence. It is an
// extra interlock, not a replacement for confirm=. See
// validateRecloneRequest for the full rule set and recloneAnnotationValue
// for the operator's own requests.
type RecloneRequest struct {
	Site       string
	Confirm    string
	UID        string
	GtidPrefix string
	// Err is the grammar error of a malformed value; Site is still the
	// text before the first ':' so the annotation can be matched to a site.
	Err error
}

// value is req as a parsed value, for actionvalue.Confirm.
func (req RecloneRequest) value() actionvalue.Value {
	v := actionvalue.Value{Subjects: []string{req.Site}}
	if req.Confirm != "" {
		v.Pairs = append(v.Pairs, actionvalue.Pair{Key: actionvalue.ConfirmKey, Value: req.Confirm})
	}
	if req.UID != "" {
		v.Pairs = append(v.Pairs, actionvalue.Pair{Key: actionvalue.UIDKey, Value: req.UID})
	}
	return v
}

// recloneGrammar is the reclone-site annotation's grammar.
var recloneGrammar = actionvalue.Grammar{
	Keys:     []string{actionvalue.ConfirmKey, actionvalue.UIDKey, "gtidPrefix"},
	Required: []string{actionvalue.ConfirmKey, actionvalue.UIDKey},
}

// recloneGtidPrefixRE is the gtidPrefix charset: the hex digits and
// hyphens of a server UUID.
var recloneGtidPrefixRE = regexp.MustCompile(`^[0-9A-Fa-f-]+$`)

// parseRecloneAnnotation parses the raw annotation value into a
// RecloneRequest. A malformed value sets Err.
func parseRecloneAnnotation(raw string) RecloneRequest {
	v, err := actionvalue.Parse(raw, recloneGrammar)
	if err != nil {
		site, _, _ := strings.Cut(strings.TrimSpace(raw), ":")
		return RecloneRequest{Site: strings.TrimSpace(site), Err: err}
	}
	req := RecloneRequest{Site: v.Subject()}
	req.Confirm, _ = v.Get(actionvalue.ConfirmKey)
	req.UID, _ = v.Get(actionvalue.UIDKey)
	req.GtidPrefix, _ = v.Get("gtidPrefix")
	return req
}

// recloneFullForm is the value a request for site must have, for the
// rejection message: the divergent form when divergentGtid is set.
func recloneFullForm(fg *v1alpha1.MysqlFailoverGroup, site, divergentGtid string) string {
	pairs := actionvalue.ConfirmPairs(fg.Name, string(fg.UID))
	if divergentGtid != "" {
		pairs = append(pairs, actionvalue.Pair{Key: "gtidPrefix", Value: truncateForHint(divergentGTIDSourceUUID(divergentGtid), minRecloneGtidPrefix)})
	}
	return v1alpha1.RecloneAnnotation + "=" + actionvalue.Format(site, pairs...)
}

// validateRecloneRequest enforces the safety interlock documented on
// RecloneAnnotation. Returns nil when the request is safe to execute,
// or an error whose message is suitable for a Kubernetes Event (it
// prints the full form the value must have).
//
// Rules:
//  1. The value matches the grammar, and Site names an entry in
//     spec.sites.
//  2. confirm= names the group and uid= is its metadata.uid: a reclone
//     wipes the datadir, and a request written for an earlier group of
//     the same name must not apply to this one.
//  3. If the site has a non-empty divergentGtid in status, gtidPrefix=
//     is required: at least minRecloneGtidPrefix hex or '-' characters,
//     a prefix of the divergent GTID's source UUID. Without a recorded
//     divergence gtidPrefix= is refused: the requester acted on a state
//     that is not the current one.
func validateRecloneRequest(fg *v1alpha1.MysqlFailoverGroup, req RecloneRequest) error {
	if req.Site == "" {
		return fmt.Errorf("reclone annotation names no site; expected %s",
			actionvalue.Format("<site>", actionvalue.Pair{Key: actionvalue.ConfirmKey, Value: fg.Name}, actionvalue.Pair{Key: actionvalue.UIDKey, Value: string(fg.UID)}))
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
	full := recloneFullForm(fg, req.Site, divergentGtid)
	if req.Err != nil {
		return fmt.Errorf("reclone of %q rejected: the value %v; set annotation %s", req.Site, req.Err, full)
	}
	if err := actionvalue.Confirm(req.value(), fg.Name, string(fg.UID)); err != nil {
		return fmt.Errorf("reclone of %q rejected: CLONE INSTANCE wipes the datadir and must be confirmed (%v); set annotation %s",
			req.Site, err, full)
	}
	if divergentGtid == "" {
		if req.GtidPrefix != "" {
			return fmt.Errorf("reclone of %q rejected: gtidPrefix=%s was given but the site has no recorded divergent GTID; "+
				"re-read status.sites[].divergentGtid and set annotation %s", req.Site, req.GtidPrefix, full)
		}
		return nil
	}

	if req.GtidPrefix == "" {
		return fmt.Errorf(
			"reclone of %q rejected: site has divergent transactions (gtid %q); "+
				"the value must include gtidPrefix=, a prefix of the divergent GTID's source UUID, to confirm intent — "+
				"set annotation %s",
			req.Site, divergentGtid, full)
	}
	if len(req.GtidPrefix) < minRecloneGtidPrefix || !recloneGtidPrefixRE.MatchString(req.GtidPrefix) {
		return fmt.Errorf(
			"reclone of %q rejected: gtidPrefix %q must be at least %d hex digits or '-' of the divergent GTID's source UUID; set annotation %s",
			req.Site, req.GtidPrefix, minRecloneGtidPrefix, full)
	}
	if !strings.HasPrefix(divergentGTIDSourceUUID(divergentGtid), req.GtidPrefix) {
		return fmt.Errorf(
			"reclone of %q rejected: gtidPrefix %q does not match the observed divergentGtid %q — "+
				"double-check the site name and re-read status.sites[].divergentGtid",
			req.Site, req.GtidPrefix, divergentGtid)
	}
	return nil
}

// divergentGTIDSourceUUID returns the source UUID of the first entry of a
// GTID set: the text before the first ':' (and before any separator a set
// that spans entries or lines carries).
func divergentGTIDSourceUUID(gtid string) string {
	first := strings.TrimSpace(gtid)
	if i := strings.IndexAny(first, ",: \t\r\n"); i >= 0 {
		first = first[:i]
	}
	return first
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

// recloneAnnotationValue is the reclone-site annotation value the
// operator writes for its own reclone requests (the in-place restore's
// peer queue), and kubectl bloodraven reclone builds the same form. It
// satisfies validateRecloneRequest for the site's current status:
// confirm= and uid= always, plus gtidPrefix= with the source UUID of the
// recorded divergent GTID set's first entry when there is one.
func recloneAnnotationValue(fg *v1alpha1.MysqlFailoverGroup, site string) string {
	pairs := actionvalue.ConfirmPairs(fg.Name, string(fg.UID))
	for _, s := range fg.Status.Sites {
		if s.Name != site || s.DivergentGtid == "" {
			continue
		}
		pairs = append(pairs, actionvalue.Pair{Key: "gtidPrefix", Value: divergentGTIDSourceUUID(s.DivergentGtid)})
		break
	}
	return actionvalue.Format(site, pairs...)
}

// RecloneAnnotationValue is the value kubectl bloodraven reclone writes for
// site: recloneAnnotationValue, the form the operator's own requests use.
func RecloneAnnotationValue(fg *v1alpha1.MysqlFailoverGroup, site string) string {
	return recloneAnnotationValue(fg, site)
}

// RecloneAnnotationValueWithPrefix is RecloneAnnotationValue with an
// explicit gtidPrefix (kubectl bloodraven reclone --gtid-prefix).
func RecloneAnnotationValueWithPrefix(fg *v1alpha1.MysqlFailoverGroup, site, gtidPrefix string) string {
	pairs := append(actionvalue.ConfirmPairs(fg.Name, string(fg.UID)), actionvalue.Pair{Key: "gtidPrefix", Value: gtidPrefix})
	return actionvalue.Format(site, pairs...)
}

// ValidateRecloneAnnotation runs value through the operator's reclone
// interlock against fg, so kubectl bloodraven reclone fails before it
// writes a value the operator would reject.
func ValidateRecloneAnnotation(fg *v1alpha1.MysqlFailoverGroup, value string) error {
	return validateRecloneRequest(fg, parseRecloneAnnotation(value))
}
