package main

import (
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestBuildVerificationCR_InheritsProfileDefaults(t *testing.T) {
	truePtr := true
	fg := &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Backup: &v1alpha1.BackupSpec{
				Profiles: []v1alpha1.BackupProfile{
					{
						Name: "nightly",
						Verification: &v1alpha1.VerificationSpec{
							Enabled:                 true,
							KeepOnFailure:           &truePtr,
							TTLSecondsAfterFinished: 600,
							PointInTime: &v1alpha1.PointInTimeVerificationSpec{
								Mode: "latest",
							},
							SanityCheck: &v1alpha1.SanityCheckSpec{
								Query: "SELECT 1",
							},
						},
					},
				},
			},
		},
	}

	cr := buildVerificationCR(fg, "default", "orders", "nightly", "", "manual")
	if !strings.HasPrefix(cr.GenerateName, "orders-nightly-verify-") {
		t.Errorf("GenerateName = %q, want prefix orders-nightly-verify-", cr.GenerateName)
	}
	if cr.Spec.FailoverGroupRef.Name != "orders" {
		t.Errorf("FailoverGroupRef = %q, want orders", cr.Spec.FailoverGroupRef.Name)
	}
	if cr.Spec.TriggeredBy != "manual" {
		t.Errorf("TriggeredBy = %q, want manual", cr.Spec.TriggeredBy)
	}
	if cr.Spec.KeepOnFailure == nil || !*cr.Spec.KeepOnFailure {
		t.Errorf("KeepOnFailure not inherited from VerificationSpec")
	}
	if cr.Spec.TTLSecondsAfterFinished != 600 {
		t.Errorf("TTLSecondsAfterFinished = %d, want 600", cr.Spec.TTLSecondsAfterFinished)
	}
	if cr.Spec.PointInTime == nil || cr.Spec.PointInTime.Mode != "latest" {
		t.Errorf("PointInTime not deep-copied from VerificationSpec; got %+v", cr.Spec.PointInTime)
	}
	if cr.Spec.SanityCheck == nil || cr.Spec.SanityCheck.Query != "SELECT 1" {
		t.Errorf("SanityCheck not deep-copied from VerificationSpec; got %+v", cr.Spec.SanityCheck)
	}
	if cr.Spec.BackupRef != nil {
		t.Errorf("BackupRef should be nil when no --backup is passed; got %+v", cr.Spec.BackupRef)
	}
}

func TestBuildVerificationCR_PinsBackupRef(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Backup: &v1alpha1.BackupSpec{
				Profiles: []v1alpha1.BackupProfile{{Name: "nightly"}},
			},
		},
	}
	cr := buildVerificationCR(fg, "default", "orders", "nightly", "orders-nightly-abcde", "manual")
	if cr.Spec.BackupRef == nil || cr.Spec.BackupRef.Name != "orders-nightly-abcde" {
		t.Errorf("BackupRef = %+v, want Name=orders-nightly-abcde", cr.Spec.BackupRef)
	}
}

func TestBuildVerificationCR_HandlesMissingVerificationSpec(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Backup: &v1alpha1.BackupSpec{
				Profiles: []v1alpha1.BackupProfile{{Name: "nightly"}},
			},
		},
	}
	cr := buildVerificationCR(fg, "default", "orders", "nightly", "", "manual")
	// Log the pointer, not the dereferenced value — the failure branch
	// only fires when KeepOnFailure is non-nil today, but using `*ptr`
	// in the failure path is a panic waiting to happen on any future
	// regression that produces a different invariant. `%+v` of a
	// *bool prints "0x..." or "<nil>" without dereferencing.
	if cr.Spec.KeepOnFailure != nil {
		t.Errorf("KeepOnFailure should default to nil when no VerificationSpec is set; got %+v", cr.Spec.KeepOnFailure)
	}
	if cr.Spec.PointInTime != nil {
		t.Errorf("PointInTime should default to nil; got %+v", cr.Spec.PointInTime)
	}
}
