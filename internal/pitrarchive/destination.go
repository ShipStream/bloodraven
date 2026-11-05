package pitrarchive

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
)

// DestinationKind is the storage backend of a PITR archive.
type DestinationKind string

const (
	DestinationS3  DestinationKind = "s3"
	DestinationPVC DestinationKind = "pvc"
)

// BinlogSubprefix is the directory under a profile's prefix (S3) or subPath
// (PVC) that holds the archive: manifests, binlog objects and archive.json.
const BinlogSubprefix = "binlogs"

// AWSEndpoint is the normalized endpoint of an S3 profile with no
// endpointURL.
const AWSEndpoint = "aws"

// Input bounds (D-18). They are derived from each other so that the worst
// case Destination.String(), with every byte %XX-escaped, fits
// MaxDestinationLen, which is the MaxLength of every status `destination`
// field. NewS3Destination and NewPVCDestination enforce them.
const (
	MaxBucketLen      = 63
	MaxKeyPrefixLen   = 512 // S3 spec prefix, before normalization
	MaxEndpointURLLen = 512
	MaxNamespaceLen   = 63
	MaxClaimNameLen   = 253
	MaxSubPathLen     = 512 // PVC spec subPath, before normalization
	MaxDestinationLen = 4096
)

// ErrInvalidDestination wraps every destination construction or parse error.
var ErrInvalidDestination = errors.New("pitrarchive: invalid destination")

// Destination is the normalized identity of a PITR archive location (§3.1).
// Build it with NewS3Destination, NewPVCDestination, ParseDestination or
// pitrprofile.DestinationFor; never fill the fields by hand.
//
// For S3, Endpoint, Bucket and KeyPrefix are set. For PVC, Namespace, Claim
// and KeyPrefix are set. KeyPrefix is the store-relative manifest prefix
// (NormalizeKeyPrefix), for both kinds.
type Destination struct {
	Kind      DestinationKind
	Endpoint  string
	Bucket    string
	Namespace string
	Claim     string
	KeyPrefix string
}

// IsZero reports whether d is the zero Destination.
func (d Destination) IsZero() bool { return d == Destination{} }

// NormalizeKeyPrefix turns a profile's S3 prefix or PVC subPath into the
// archive's key prefix: trim "/", path.Clean, "." becomes "", then join
// BinlogSubprefix. "" -> "binlogs"; "/a//b/" -> "a/b/binlogs".
//
// A leading "/" on an S3 prefix normalizes away. That is a deliberate break
// from the old path.Join, which kept it (§6.3).
func NormalizeKeyPrefix(userPrefix string) string {
	p := strings.Trim(userPrefix, "/")
	if p != "" {
		p = path.Clean(p)
	}
	if p == "." || p == "" {
		return BinlogSubprefix
	}
	return path.Join(p, BinlogSubprefix)
}

// NormalizeEndpoint normalizes an S3 endpointURL. "" -> AWSEndpoint.
// Otherwise the URL must be absolute http or https with a host and no
// credentials, query or fragment; the scheme and host are lowercased, a
// default port (:80 for http, :443 for https) is dropped, and the path is
// cleaned with any trailing "/" removed ("/" becomes ""). Distinct
// spellings of one AWS endpoint are deliberately not unified.
func NormalizeEndpoint(raw string) (string, error) {
	if raw == "" {
		return AWSEndpoint, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: endpoint %q: %v", ErrInvalidDestination, raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch {
	case scheme != "http" && scheme != "https":
		return "", fmt.Errorf("%w: endpoint %q: scheme must be http or https", ErrInvalidDestination, raw)
	case u.Opaque != "" || u.Host == "":
		return "", fmt.Errorf("%w: endpoint %q: no host", ErrInvalidDestination, raw)
	case u.User != nil:
		return "", fmt.Errorf("%w: endpoint %q: credentials are not allowed", ErrInvalidDestination, raw)
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(raw, "?#"):
		return "", fmt.Errorf("%w: endpoint %q: query and fragment are not allowed", ErrInvalidDestination, raw)
	}
	host := strings.ToLower(u.Host)
	if port := u.Port(); (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		host = strings.TrimSuffix(host, ":"+port)
	}
	host = strings.TrimSuffix(host, ":") // "h:" (empty port)
	p := u.EscapedPath()
	if p != "" {
		p = path.Clean(p)
		p = strings.TrimSuffix(p, "/")
	}
	return scheme + "://" + host + p, nil
}

// NewS3Destination builds a normalized S3 destination from a profile's
// bucket, prefix and endpointURL, enforcing the D-18 bounds.
func NewS3Destination(bucket, prefix, endpointURL string) (Destination, error) {
	switch {
	case bucket == "":
		return Destination{}, fmt.Errorf("%w: s3 bucket is empty", ErrInvalidDestination)
	case len(bucket) > MaxBucketLen:
		return Destination{}, fmt.Errorf("%w: s3 bucket is %d bytes, max %d", ErrInvalidDestination, len(bucket), MaxBucketLen)
	case len(prefix) > MaxKeyPrefixLen:
		return Destination{}, fmt.Errorf("%w: s3 prefix is %d bytes, max %d", ErrInvalidDestination, len(prefix), MaxKeyPrefixLen)
	case len(endpointURL) > MaxEndpointURLLen:
		return Destination{}, fmt.Errorf("%w: s3 endpointURL is %d bytes, max %d", ErrInvalidDestination, len(endpointURL), MaxEndpointURLLen)
	}
	ep, err := NormalizeEndpoint(endpointURL)
	if err != nil {
		return Destination{}, err
	}
	d := Destination{Kind: DestinationS3, Endpoint: ep, Bucket: bucket, KeyPrefix: NormalizeKeyPrefix(prefix)}
	return d, d.checkLen()
}

// NewPVCDestination builds a normalized PVC destination from the group's
// namespace, the effective claim name (an explicit claimName, or the
// operator-owned PVC's name) and the profile's subPath, enforcing the D-18
// bounds. A ".." segment in subPath is refused.
func NewPVCDestination(namespace, claim, subPath string) (Destination, error) {
	switch {
	case namespace == "":
		return Destination{}, fmt.Errorf("%w: pvc namespace is empty", ErrInvalidDestination)
	case len(namespace) > MaxNamespaceLen:
		return Destination{}, fmt.Errorf("%w: pvc namespace is %d bytes, max %d", ErrInvalidDestination, len(namespace), MaxNamespaceLen)
	case claim == "":
		return Destination{}, fmt.Errorf("%w: pvc claim name is empty", ErrInvalidDestination)
	case len(claim) > MaxClaimNameLen:
		return Destination{}, fmt.Errorf("%w: pvc claim name is %d bytes, max %d", ErrInvalidDestination, len(claim), MaxClaimNameLen)
	case strings.Contains(namespace, "/") || strings.Contains(claim, "/"):
		return Destination{}, fmt.Errorf("%w: pvc namespace and claim must not contain '/'", ErrInvalidDestination)
	case len(subPath) > MaxSubPathLen:
		return Destination{}, fmt.Errorf("%w: pvc subPath is %d bytes, max %d", ErrInvalidDestination, len(subPath), MaxSubPathLen)
	}
	for _, seg := range strings.Split(subPath, "/") {
		if seg == ".." {
			return Destination{}, fmt.Errorf("%w: pvc subPath %q must not contain '..' segments", ErrInvalidDestination, subPath)
		}
	}
	d := Destination{Kind: DestinationPVC, Namespace: namespace, Claim: claim, KeyPrefix: NormalizeKeyPrefix(subPath)}
	return d, d.checkLen()
}

func (d Destination) checkLen() error {
	if n := len(d.String()); n > MaxDestinationLen {
		return fmt.Errorf("%w: normalized destination is %d bytes, max %d", ErrInvalidDestination, n, MaxDestinationLen)
	}
	return nil
}

// String renders the destination's canonical, injective form:
//
//	s3://<PathEscape(bucket)>/<PathEscape per keyPrefix segment>?endpoint=<QueryEscape(endpoint)>
//	pvc://<PathEscape(namespace)>/<PathEscape(claim)>/<PathEscape per keyPrefix segment>
//
// url.PathEscape escapes '/', '?', '%' and '#', so no two destinations
// render alike. This string is the input to the archive ID derivation, so
// its golden vectors in destination_test.go must never change. The zero
// Destination renders "".
func (d Destination) String() string {
	switch d.Kind {
	case DestinationS3:
		return "s3://" + url.PathEscape(d.Bucket) + "/" + escapeSegments(d.KeyPrefix) + "?endpoint=" + url.QueryEscape(d.Endpoint)
	case DestinationPVC:
		return "pvc://" + url.PathEscape(d.Namespace) + "/" + url.PathEscape(d.Claim) + "/" + escapeSegments(d.KeyPrefix)
	default:
		return ""
	}
}

// ParseDestination parses the canonical form produced by String. It
// refuses any other spelling of the same destination, so the result's
// String() equals s.
func ParseDestination(s string) (Destination, error) {
	var d Destination
	switch {
	case strings.HasPrefix(s, "s3://"):
		rest := strings.TrimPrefix(s, "s3://")
		p, q, ok := strings.Cut(rest, "?")
		if !ok || !strings.HasPrefix(q, "endpoint=") {
			return Destination{}, fmt.Errorf("%w: %q has no endpoint", ErrInvalidDestination, s)
		}
		ep, err := url.QueryUnescape(strings.TrimPrefix(q, "endpoint="))
		if err != nil {
			return Destination{}, fmt.Errorf("%w: %q: endpoint: %v", ErrInvalidDestination, s, err)
		}
		segs, err := unescapeSegments(p)
		if err != nil || len(segs) < 2 {
			return Destination{}, fmt.Errorf("%w: %q: want s3://<bucket>/<keyPrefix>", ErrInvalidDestination, s)
		}
		d = Destination{Kind: DestinationS3, Endpoint: ep, Bucket: segs[0], KeyPrefix: strings.Join(segs[1:], "/")}
	case strings.HasPrefix(s, "pvc://"):
		segs, err := unescapeSegments(strings.TrimPrefix(s, "pvc://"))
		if err != nil || len(segs) < 3 {
			return Destination{}, fmt.Errorf("%w: %q: want pvc://<namespace>/<claim>/<keyPrefix>", ErrInvalidDestination, s)
		}
		d = Destination{Kind: DestinationPVC, Namespace: segs[0], Claim: segs[1], KeyPrefix: strings.Join(segs[2:], "/")}
	default:
		return Destination{}, fmt.Errorf("%w: %q: unknown scheme", ErrInvalidDestination, s)
	}
	if d.String() != s || !canonicalKeyPrefix(d.KeyPrefix) || !canonicalEndpoint(d) {
		return Destination{}, fmt.Errorf("%w: %q is not in canonical form", ErrInvalidDestination, s)
	}
	if err := d.checkFieldBounds(); err != nil {
		return Destination{}, err
	}
	return d, d.checkLen()
}

// checkFieldBounds re-applies the D-18 bounds to a parsed destination, so
// every Destination this package returns satisfies them. A normalized key
// prefix is at most the spec bound plus "/binlogs".
func (d Destination) checkFieldBounds() error {
	maxPrefix := MaxKeyPrefixLen + len("/"+BinlogSubprefix)
	switch d.Kind {
	case DestinationS3:
		if len(d.Bucket) > MaxBucketLen || len(d.Endpoint) > MaxEndpointURLLen || len(d.KeyPrefix) > maxPrefix {
			return fmt.Errorf("%w: %q exceeds the s3 field bounds", ErrInvalidDestination, d.String())
		}
	case DestinationPVC:
		if len(d.Namespace) > MaxNamespaceLen || len(d.Claim) > MaxClaimNameLen || len(d.KeyPrefix) > MaxSubPathLen+len("/"+BinlogSubprefix) {
			return fmt.Errorf("%w: %q exceeds the pvc field bounds", ErrInvalidDestination, d.String())
		}
		for _, seg := range strings.Split(d.KeyPrefix, "/") {
			if seg == ".." {
				return fmt.Errorf("%w: %q has a '..' segment", ErrInvalidDestination, d.String())
			}
		}
	}
	return nil
}

// canonicalKeyPrefix reports whether p is a NormalizeKeyPrefix output.
func canonicalKeyPrefix(p string) bool {
	if p == BinlogSubprefix {
		return true
	}
	base, ok := strings.CutSuffix(p, "/"+BinlogSubprefix)
	return ok && NormalizeKeyPrefix(base) == p
}

func canonicalEndpoint(d Destination) bool {
	if d.Kind != DestinationS3 {
		return true
	}
	if d.Endpoint == AWSEndpoint {
		return true
	}
	n, err := NormalizeEndpoint(d.Endpoint)
	return err == nil && n == d.Endpoint
}

func escapeSegments(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

func unescapeSegments(p string) ([]string, error) {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		u, err := url.PathUnescape(s)
		if err != nil {
			return nil, err
		}
		if u == "" {
			return nil, fmt.Errorf("empty segment")
		}
		segs[i] = u
	}
	return segs, nil
}
