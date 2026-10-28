package backupmeta

import "testing"

func TestDumpCompleteRoundTrip(t *testing.T) {
	for _, value := range []string{"prod_backups/x", "/backups/db dumps/x", "a%b", "a+b", "a=b", "\t\r\n", "備份", "uuid:my_tag:1-3,uuid2:1-5", "my_bin.000001", "1.4 GiB", ""} {
		t.Run(value, func(t *testing.T) {
			want := DumpComplete{Location: value, Size: value, SizeBytes: 1234, GtidExecuted: value, BinlogFile: value, BinlogPos: 42, Encrypted: true, EncryptionAlgorithm: value, CiphertextBytes: 1400, Files: 3}
			got, ok := ParseDumpComplete(FormatDumpComplete(want))
			if !ok || got != want {
				t.Fatalf("got %+v/%v, want %+v", got, ok, want)
			}
		})
	}
}

func TestParseDumpCompleteVersions(t *testing.T) {
	for _, tc := range []struct {
		line, location, size string
		ok                   bool
	}{
		{line: "BLOODRAVEN_DUMP_COMPLETE location=prod_backups/a%20b+c size=1.4_GiB", location: "prod_backups/a%20b+c", size: "1.4 GiB", ok: true},
		{line: "BLOODRAVEN_DUMP_COMPLETE location=a+b size=1.4%20GiB v=2 futureKey=value", location: "a+b", size: "1.4 GiB", ok: true},
		{line: "BLOODRAVEN_DUMP_COMPLETE v=2 location=a%2520b%2Bc", location: "a%20b+c", ok: true},
		{line: "BLOODRAVEN_DUMP_COMPLETE v=3 location=x"},
		{line: "BLOODRAVEN_DUMP_COMPLETE v= location=x"},
		{line: "BLOODRAVEN_DUMP_COMPLETE v=2 location=bad%escape"},
		{line: "BLOODRAVEN_DUMP_COMPLETE v=2 location=bad%"},
		{line: "BLOODRAVEN_DUMP_COMPLETE v=2 v=2 location=x"},
		{line: "BLOODRAVEN_DUMP_COMPLETE v=2 location=a location=b"},
		{line: "BLOODRAVEN_DUMP_COMPLETE_EXTRA location=x"},
		{line: "BLOODRAVEN_DUMP_START location=x"},
		{line: ""},
	} {
		t.Run(tc.line, func(t *testing.T) {
			got, ok := ParseDumpComplete(tc.line)
			if ok != tc.ok || ok && (got.Location != tc.location || got.Size != tc.size) {
				t.Fatalf("got %+v/%v, want location=%q size=%q ok=%v", got, ok, tc.location, tc.size, tc.ok)
			}
		})
	}
}
