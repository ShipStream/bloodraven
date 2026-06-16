package runner

import "testing"

// TestSanitizeLogFilename guards the contract that forensic tailer logs are
// written with names accepted by actions/upload-artifact. Tailer component
// labels carry a colon ("sidecar:iad", "mysql:pdx"); a ':' in the filename
// makes upload-artifact reject the whole forensics upload (it bans
// " : < > | * ? \r \n), which previously hid every other capture in the dir.
func TestSanitizeLogFilename(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"operator", "operator"},
		{"sidecar:iad", "sidecar-iad"},
		{"mysql:pdx", "mysql-pdx"},
		{"a:b:c", "a-b-c"},
		{`weird"<>|*?name`, "weird------name"},
		{"with/slash", "with-slash"},
	}
	for _, tc := range cases {
		if got := sanitizeLogFilename(tc.in); got != tc.want {
			t.Errorf("sanitizeLogFilename(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	// No banned character may survive in any output.
	banned := ":\"<>|*?/\\\r\n"
	for _, tc := range cases {
		out := sanitizeLogFilename(tc.in)
		for _, r := range banned {
			for _, c := range out {
				if c == r {
					t.Errorf("sanitizeLogFilename(%q) = %q still contains banned %q", tc.in, out, string(r))
				}
			}
		}
	}
}
