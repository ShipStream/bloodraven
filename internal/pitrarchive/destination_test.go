package pitrarchive

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeKeyPrefix(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", "binlogs"},
		{"/", "binlogs"},
		{"//", "binlogs"},
		{".", "binlogs"},
		{"/./", "binlogs"},
		{"a", "a/binlogs"},
		{"/a/", "a/binlogs"},
		{"a//b", "a/b/binlogs"},
		{"./a", "a/binlogs"},
		{"a/./b/", "a/b/binlogs"},
		{"team a", "team a/binlogs"},
	}
	for _, tt := range tests {
		if got := NormalizeKeyPrefix(tt.in); got != tt.want {
			t.Errorf("NormalizeKeyPrefix(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizeEndpoint(t *testing.T) {
	tests := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "", want: "aws"},
		{in: "https://s3.amazonaws.com", want: "https://s3.amazonaws.com"},
		{in: "HTTPS://S3.Example.COM:443/", want: "https://s3.example.com"},
		{in: "http://h:80", want: "http://h"},
		{in: "http://h:443", want: "http://h:443"},
		{in: "https://h:80", want: "https://h:80"},
		{in: "https://h:9000/a//b/", want: "https://h:9000/a/b"},
		{in: "https://h/", want: "https://h"},
		{in: "https://h/Path/Case", want: "https://h/Path/Case"},
		{in: "https://[::1]:443/x", want: "https://[::1]/x"},
		{in: "http://minio.minio.svc:9000", want: "http://minio.minio.svc:9000"},
		{in: "ftp://h", wantErr: true},
		{in: "h", wantErr: true},
		{in: "h:9000", wantErr: true},
		{in: "https://", wantErr: true},
		{in: "https://u:p@h", wantErr: true},
		{in: "https://h?x=1", wantErr: true},
		{in: "https://h?", wantErr: true},
		{in: "https://h#f", wantErr: true},
		{in: "https://h/%zz", wantErr: true},
	}
	for _, tt := range tests {
		got, err := NormalizeEndpoint(tt.in)
		if tt.wantErr {
			if err == nil || !errors.Is(err, ErrInvalidDestination) {
				t.Errorf("NormalizeEndpoint(%q) = %q, %v; want ErrInvalidDestination", tt.in, got, err)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("NormalizeEndpoint(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
		}
	}
}

// goldenDestinations pins Destination.String(). The archive ID derivation
// hashes this string, so a change here silently turns every existing archive
// into a copy. These strings were cross-checked with Python's urllib.parse
// (quote(safe="$&+:=@") per segment, quote_plus for the endpoint).
var goldenDestinations = []struct {
	name string
	dest func() (Destination, error)
	want string
}{
	{"s3 aws no prefix", func() (Destination, error) { return NewS3Destination("backups", "", "") },
		"s3://backups/binlogs?endpoint=aws"},
	{"s3 custom endpoint, messy prefix", func() (Destination, error) {
		return NewS3Destination("my-bucket", "/team a//prod/", "HTTPS://MinIO.Example.com:443/")
	}, "s3://my-bucket/team%20a/prod/binlogs?endpoint=https%3A%2F%2Fminio.example.com"},
	{"pvc", func() (Destination, error) { return NewPVCDestination("db", "mysql-orders-backup-pitr", "pitr") },
		"pvc://db/mysql-orders-backup-pitr/pitr/binlogs"},
	{"s3 delimiters escaped", func() (Destination, error) { return NewS3Destination("b", "a?b#c%d/e", "http://h:9000/path/") },
		"s3://b/a%3Fb%23c%25d/e/binlogs?endpoint=http%3A%2F%2Fh%3A9000%2Fpath"},
	{"s3 utf-8", func() (Destination, error) { return NewS3Destination("b", "ünï", "") },
		"s3://b/%C3%BCn%C3%AF/binlogs?endpoint=aws"},
}

func TestDestinationStringGolden(t *testing.T) {
	for _, g := range goldenDestinations {
		t.Run(g.name, func(t *testing.T) {
			d, err := g.dest()
			if err != nil {
				t.Fatal(err)
			}
			if got := d.String(); got != g.want {
				t.Fatalf("String() = %q, want golden %q", got, g.want)
			}
			back, err := ParseDestination(g.want)
			if err != nil {
				t.Fatalf("ParseDestination: %v", err)
			}
			if back != d {
				t.Fatalf("ParseDestination round trip = %+v, want %+v", back, d)
			}
		})
	}
}

// TestDestinationStringInjective lists inputs that a naive join would
// render alike.
func TestDestinationStringInjective(t *testing.T) {
	mk := []func() (Destination, error){
		func() (Destination, error) { return NewS3Destination("a/b", "c", "") },
		func() (Destination, error) { return NewS3Destination("a", "b/c", "") },
		func() (Destination, error) { return NewS3Destination("a", "b%2Fc", "") },
		func() (Destination, error) { return NewS3Destination("a", "x?endpoint=aws", "") },
		func() (Destination, error) { return NewS3Destination("a", "x", "") },
		func() (Destination, error) { return NewS3Destination("a", "x", "https://aws") },
		func() (Destination, error) { return NewS3Destination("a", "x", "https://h") },
		func() (Destination, error) { return NewS3Destination("a", "x", "https://h:8443") },
		func() (Destination, error) { return NewS3Destination("a", "x binlogs", "") },
		func() (Destination, error) { return NewS3Destination("a", "x+binlogs", "") },
		func() (Destination, error) { return NewPVCDestination("ns", "c", "") },
		func() (Destination, error) { return NewPVCDestination("ns", "c", "x") },
		func() (Destination, error) { return NewPVCDestination("ns", "c-x", "") },
		func() (Destination, error) { return NewPVCDestination("ns-c", "x", "") },
	}
	seen := map[string]int{}
	for i, f := range mk {
		d, err := f()
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		s := d.String()
		if j, dup := seen[s]; dup {
			t.Fatalf("cases %d and %d both render %q", j, i, s)
		}
		seen[s] = i
		back, err := ParseDestination(s)
		if err != nil || back != d {
			t.Fatalf("case %d: ParseDestination(%q) = %+v, %v; want %+v", i, s, back, err, d)
		}
	}
}

func TestParseDestinationRefusesNonCanonical(t *testing.T) {
	for _, s := range []string{
		"",
		"gs://b/binlogs",
		"s3://b/binlogs",                                // no endpoint
		"s3://b/binlogs?endpoint=",                      // empty endpoint
		"s3://b/binlogs?region=x",                       // not endpoint
		"s3://b/x?endpoint=aws",                         // prefix does not end in binlogs
		"s3://b//x/binlogs?endpoint=aws",                // empty segment
		"s3://b/x/./binlogs?endpoint=aws",               // uncleaned
		"s3://b/%78/binlogs?endpoint=aws",               // over-escaped 'x'
		"s3://b/binlogs?endpoint=https%3A%2F%2FH",       // uppercase host
		"s3://b/binlogs?endpoint=https%3A%2F%2Fh%3A443", // default port kept
		"s3://b/binlogs?endpoint=https%3A%2F%2Fh%2F",    // trailing slash
		"s3:///binlogs?endpoint=aws",                    // empty bucket
		"pvc://ns/c",                                    // no prefix
		"pvc://ns/c/x",                                  // prefix does not end in binlogs
		"pvc://ns/c/../binlogs",                         // dot-dot
		"pvc://ns/c/binlogs?endpoint=aws",               // pvc with query
		"s3://" + strings.Repeat("b", 64) + "/binlogs?endpoint=aws", // bucket bound
	} {
		if d, err := ParseDestination(s); err == nil {
			t.Errorf("ParseDestination(%q) = %+v, want error", s, d)
		}
	}
}

func TestDestinationBounds(t *testing.T) {
	long := func(n int) string { return strings.Repeat("x", n) }
	ok := []func() (Destination, error){
		func() (Destination, error) { return NewS3Destination(long(MaxBucketLen), long(MaxKeyPrefixLen), "") },
		func() (Destination, error) {
			return NewS3Destination("b", "", "https://h/"+long(MaxEndpointURLLen-len("https://h/")))
		},
		func() (Destination, error) {
			return NewPVCDestination(long(MaxNamespaceLen), long(MaxClaimNameLen), long(MaxSubPathLen))
		},
	}
	for i, f := range ok {
		if _, err := f(); err != nil {
			t.Errorf("at-bound case %d: %v", i, err)
		}
	}
	bad := []func() (Destination, error){
		func() (Destination, error) { return NewS3Destination("", "", "") },
		func() (Destination, error) { return NewS3Destination(long(MaxBucketLen+1), "", "") },
		func() (Destination, error) { return NewS3Destination("b", long(MaxKeyPrefixLen+1), "") },
		func() (Destination, error) {
			return NewS3Destination("b", "", "https://h/"+long(MaxEndpointURLLen+1-len("https://h/")))
		},
		func() (Destination, error) { return NewPVCDestination("", "c", "") },
		func() (Destination, error) { return NewPVCDestination("ns", "", "") },
		func() (Destination, error) { return NewPVCDestination(long(MaxNamespaceLen+1), "c", "") },
		func() (Destination, error) { return NewPVCDestination("ns", long(MaxClaimNameLen+1), "") },
		func() (Destination, error) { return NewPVCDestination("ns", "c", long(MaxSubPathLen+1)) },
		func() (Destination, error) { return NewPVCDestination("ns", "c", "a/../b") },
		func() (Destination, error) { return NewPVCDestination("ns", "c/d", "") },
	}
	for i, f := range bad {
		if d, err := f(); err == nil || !errors.Is(err, ErrInvalidDestination) {
			t.Errorf("over-bound case %d = %+v, %v; want ErrInvalidDestination", i, d, err)
		}
	}
}

// TestMaxDestinationLenWorstCase asserts D-18: with every input at its bound
// and every byte needing %XX escaping, String() fits MaxDestinationLen (and
// the design's ~3,300 estimate).
func TestMaxDestinationLenWorstCase(t *testing.T) {
	pct := func(n int) string { return strings.Repeat("%", n) }
	s3, err := NewS3Destination(pct(MaxBucketLen), pct(MaxKeyPrefixLen),
		"https://h/"+strings.Repeat("!", MaxEndpointURLLen-len("https://h/")))
	if err != nil {
		t.Fatal(err)
	}
	pvc, err := NewPVCDestination(pct(MaxNamespaceLen), pct(MaxClaimNameLen), pct(MaxSubPathLen))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []Destination{s3, pvc} {
		n := len(d.String())
		if n > 3300 || n > MaxDestinationLen {
			t.Errorf("%s destination worst case is %d bytes, want <= 3300 (MaxDestinationLen %d)", d.Kind, n, MaxDestinationLen)
		}
		if n < 2000 {
			t.Errorf("%s worst case %d is implausibly small; the test no longer exercises escaping", d.Kind, n)
		}
	}
}
