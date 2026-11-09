package main

import (
	"encoding/json"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestScheduledVerificationKeepsTTLPresence (F-11): the scheduled trigger
// copies the profile's ttlSecondsAfterFinished pointer, so an omitted TTL
// stays omitted on the created CR (keep until retention) and an explicit 0
// stays 0 (clean up at once), on the Go object and on the wire.
func TestScheduledVerificationKeepsTTLPresence(t *testing.T) {
	for _, tc := range []struct {
		name string
		ttl  *int32
	}{{name: "omitted"}, {name: "zero", ttl: new(int32(0))}, {name: "set", ttl: new(int32(600))}} {
		t.Run(tc.name, func(t *testing.T) {
			fg := &v1alpha1.MysqlFailoverGroup{Spec: v1alpha1.MysqlFailoverGroupSpec{Backup: &v1alpha1.BackupSpec{
				Profiles: []v1alpha1.BackupProfile{{Name: "nightly", Verification: &v1alpha1.VerificationSpec{Enabled: true, TTLSecondsAfterFinished: tc.ttl}}},
			}}}
			cr := buildScheduledVerification(fg, "orders", "nightly", "ns")
			got := cr.Spec.TTLSecondsAfterFinished
			if (got == nil) != (tc.ttl == nil) || (got != nil && *got != *tc.ttl) {
				t.Fatalf("ttlSecondsAfterFinished = %v, want %v", got, tc.ttl)
			}
			if got != nil && got == tc.ttl {
				t.Fatal("the CR aliases the group's TTL pointer")
			}
			b, err := json.Marshal(cr.Spec)
			if err != nil {
				t.Fatal(err)
			}
			if has := strings.Contains(string(b), `"ttlSecondsAfterFinished"`); has != (tc.ttl != nil) {
				t.Fatalf("serialized spec %s: ttlSecondsAfterFinished present = %v", b, has)
			}
			if tc.name == "zero" && !strings.Contains(string(b), `"ttlSecondsAfterFinished":0`) {
				t.Fatalf("an explicit 0 was dropped: %s", b)
			}
		})
	}
}

// TestScheduledVerificationCarriesScheduleLabel: a scheduled verification
// carries the verification-schedule label with the value its CronJob has
// (the profile name), so the documented selector finds scheduled runs.
func TestScheduledVerificationCarriesScheduleLabel(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{Spec: v1alpha1.MysqlFailoverGroupSpec{Backup: &v1alpha1.BackupSpec{
		Profiles: []v1alpha1.BackupProfile{{Name: "nightly", Verification: &v1alpha1.VerificationSpec{Enabled: true}}},
	}}}
	cr := buildScheduledVerification(fg, "orders", "nightly", "ns")
	if got := cr.Labels[v1alpha1.VerificationScheduleLabel]; got != "nightly" {
		t.Fatalf("%s = %q, want the profile name %q", v1alpha1.VerificationScheduleLabel, got, "nightly")
	}
}
