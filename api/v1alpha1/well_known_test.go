package v1alpha1

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

// The A-15 name bounds the taint key is sized against. E6 enforces them at
// admission; until then this test pins that the key fits whatever names
// those bounds admit.
const (
	// maxNamespaceLength is a namespace name's limit (a DNS-1123 label).
	maxNamespaceLength = 63
	// maxGroupNameLength is the longest MysqlFailoverGroup name: the group
	// plus a site name of at least one character is at most 41 (A-15).
	maxGroupNameLength = 40
	// maxTaintKeyPrefixLength is the bound A-21 promises for the prefix.
	maxTaintKeyPrefixLength = 100
)

func TestReadOnlyTaintKeyFormat(t *testing.T) {
	if got := ReadOnlyTaintKey("prod", "orders"); got != "prod.db-readonly.bloodraven.shipstream.io/orders" {
		t.Fatalf("ReadOnlyTaintKey = %q", got)
	}
	if ReadOnlyTaintKey("a", "orders") == ReadOnlyTaintKey("b", "orders") {
		t.Fatal("same-name groups in two namespaces share a taint key")
	}
	if ReadOnlyTaintValue != "true" {
		t.Fatalf("ReadOnlyTaintValue = %q", ReadOnlyTaintValue)
	}
}

// TestReadOnlyTaintKeyLength: the longest namespace and group the bounds
// admit give a valid qualified name (a taint key's grammar), with a prefix
// within maxTaintKeyPrefixLength.
func TestReadOnlyTaintKeyLength(t *testing.T) {
	for _, tc := range []struct{ namespace, group string }{
		{"a", "b"},
		{strings.Repeat("n", maxNamespaceLength), strings.Repeat("g", maxGroupNameLength)},
		{"a-" + strings.Repeat("0", maxNamespaceLength-4) + "-z", "g-" + strings.Repeat("1", maxGroupNameLength-4) + "-h"},
	} {
		key := ReadOnlyTaintKey(tc.namespace, tc.group)
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			t.Errorf("ReadOnlyTaintKey(%d-char namespace, %d-char group) = %q is not a qualified name: %v",
				len(tc.namespace), len(tc.group), key, errs)
		}
		prefix, name, _ := strings.Cut(key, "/")
		if len(prefix) > maxTaintKeyPrefixLength {
			t.Errorf("prefix %q is %d characters, want at most %d", prefix, len(prefix), maxTaintKeyPrefixLength)
		}
		if name != tc.group {
			t.Errorf("name part %q, want the group name %q", name, tc.group)
		}
	}
}

// TestHoldKeyFitsAUID: the verification hold's prefix plus a UID is a
// valid qualified name.
func TestHoldKeyFitsAUID(t *testing.T) {
	key := VerificationHoldAnnotationPrefix + "6f1c2d3e-0000-4000-8000-000000000001"
	if errs := validation.IsQualifiedName(key); len(errs) > 0 {
		t.Fatalf("%q: %v", key, errs)
	}
	if !strings.HasSuffix(strings.TrimSuffix(VerificationHoldAnnotationPrefix, "/"), ".bloodraven.shipstream.io") ||
		!strings.HasPrefix(VerificationHoldAnnotationPrefix, "verification-hold.") {
		t.Fatalf("%q is not <kind>-hold.bloodraven.shipstream.io/", VerificationHoldAnnotationPrefix)
	}
}
