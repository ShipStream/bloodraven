package controller

import (
	"context"
	"strings"
	"testing"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// The archive of an encrypted group holds the plaintext of its binary logs.
// A PITR profile without client-side encryption is reported, in the
// EncryptionAtRestReady condition and once in an Event, and never refused:
// the store may encrypt, which the operator cannot see.
func TestEncryptionPITRArchiveWithoutClientEncryptionIsReported(t *testing.T) {
	profile := func(encrypted bool) *v1alpha1.BackupSpec {
		p := v1alpha1.BackupProfile{Name: "archive"}
		if encrypted {
			p.Encryption = &v1alpha1.BackupEncryptionSpec{}
		}
		return &v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{p}, PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "archive"}}
	}
	for name, tc := range map[string]struct {
		backup *v1alpha1.BackupSpec
		want   bool
	}{
		"no PITR":                         {nil, false},
		"PITR off":                        {&v1alpha1.BackupSpec{Profiles: []v1alpha1.BackupProfile{{Name: "archive"}}, PITR: &v1alpha1.PITRSpec{ProfileName: "archive"}}, false},
		"profile with encryption":         {profile(true), false},
		"profile without encryption":      {profile(false), true},
		"profile the group does not have": {&v1alpha1.BackupSpec{PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "archive"}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			fg := encTestFG()
			fg.Spec.Backup = tc.backup
			note := pitrArchivePlainNote(fg)
			if (note != "") != tc.want {
				t.Fatalf("note = %q, want one: %v", note, tc.want)
			}
			if tc.want && (!strings.Contains(note, pitrArchivePlainMarker) || !strings.Contains(note, `"archive"`)) {
				t.Fatalf("note = %q, want the marker and the profile", note)
			}
		})
	}
	plain := encTestFG()
	plain.Spec.Backup = profile(false)
	plain.Spec.EncryptionAtRest = nil
	if note := pitrArchivePlainNote(plain); note != "" {
		t.Fatalf("a group without encryption at rest is reported: %q", note)
	}

	fg := encTestFG()
	fg.Spec.Backup = profile(false)
	r, _ := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{"dc1": liveKeyring("", false)}), fg, unsealedDeployment(fg, "dc1"))
	events := record.NewFakeRecorder(20)
	r.Recorder = events
	ctx := context.Background()
	reported := func() int {
		n := 0
		for {
			select {
			case e := <-events.Events:
				if strings.Contains(e, v1alpha1.EventPITRArchiveNotEncrypted) {
					if !strings.HasPrefix(e, "Warning ") || !strings.Contains(e, pitrArchivePlainMarker) {
						t.Fatalf("event = %q", e)
					}
					n++
				}
			default:
				return n
			}
		}
	}
	for i, want := range []int{1, 0, 0} {
		if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if got := reported(); got != want {
			t.Fatalf("pass %d raised the Event %d times, want %d", i+1, got, want)
		}
		c := apiMeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionEncryptionAtRestReady)
		if c == nil || !strings.Contains(c.Message, pitrArchivePlainMarker) {
			t.Fatalf("pass %d: condition = %+v, want the note in its message", i+1, c)
		}
	}
	// The profile gets encryption: the note goes, and comes back with an
	// Event if the encryption is removed again.
	fg.Spec.Backup = profile(true)
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if c := apiMeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionEncryptionAtRestReady); c == nil || strings.Contains(c.Message, pitrArchivePlainMarker) || reported() != 0 {
		t.Fatalf("condition = %+v after the profile got encryption", c)
	}
	fg.Spec.Backup = profile(false)
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if got := reported(); got != 1 {
		t.Fatalf("the Event was raised %d times when the encryption was removed, want 1", got)
	}
}
