package sidecar

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func TestReplayPrefixRequiresCompletedTransaction(t *testing.T) {
	for _, tc := range []struct {
		name      string
		committed bool
	}{
		{"incarnation-a.bin", true},
		{"native/initial.bin", true},
		{"native/xa-prepare.bin", false},
		{"compressed-transaction.bin", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + tc.name)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := InspectBinlog(bytes.NewReader(data), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if evidence.CompletedBeforeStopGTIDs.IsEmpty() == tc.committed {
				t.Fatalf("unexpected completed prefix: %+v", evidence)
			}
		})
	}
}
