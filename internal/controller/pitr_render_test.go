package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
)

// testPITRRender is fg's render with no site Deployment (§4.6 rows 1, 2,
// 4a): what a group being created renders from its bindings and spec.
func testPITRRender(fg *v1alpha1.MysqlFailoverGroup) pitrRenderInputs {
	in, _ := resolvePITRRender(fg, nil)
	return in
}

// testGroupPITRRender is testPITRRender as reconcileDeployment takes it.
func testGroupPITRRender(fg *v1alpha1.MysqlFailoverGroup) groupPITRRender {
	return groupPITRRender{inputs: testPITRRender(fg), action: pitrRenderApply}
}

// testDumpKeySecret is the dump-key snapshot name BuildBackupJob tests pass.
const testDumpKeySecret = "mysqlbackup-test-dump-key"

// testPassphrase is the bytes withTestPassphraseSecrets puts in each
// encrypted profile's passphrase Secret.
const testPassphrase = "test-passphrase-0123456789abcdef"

// withTestPassphraseSecrets adds, for every encrypted profile of every
// group among objs, its passphrase Secret holding testPassphrase, unless
// objs already has a Secret of that name. Backup Jobs snapshot that Secret
// before they are created (I-17).
func withTestPassphraseSecrets(objs []client.Object) []client.Object {
	have := map[string]bool{}
	for _, o := range objs {
		if s, ok := o.(*corev1.Secret); ok {
			have[s.Namespace+"/"+s.Name] = true
		}
	}
	out := append([]client.Object(nil), objs...)
	for _, o := range objs {
		fg, ok := o.(*v1alpha1.MysqlFailoverGroup)
		if !ok || fg.Spec.Backup == nil {
			continue
		}
		for _, p := range fg.Spec.Backup.Profiles {
			if !p.EncryptionEnabled() {
				continue
			}
			ref := p.Encryption.PassphraseSecret
			if have[fg.Namespace+"/"+ref.Name] {
				continue
			}
			have[fg.Namespace+"/"+ref.Name] = true
			out = append(out, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: fg.Namespace, Name: ref.Name},
				Data:       map[string][]byte{ref.PassphraseSecretKeyOrDefault(): []byte(testPassphrase + "\n")},
			})
		}
	}
	return out
}

// bindTestPITRArchive records a Bound binding for profileName's
// destination in fg's status, as the engine would after a proof: source fg,
// the derived archive ID, the profile's encryption and, for Passphrase, a
// snapshot name. It returns the entry.
func bindTestPITRArchive(t testing.TB, fg *v1alpha1.MysqlFailoverGroup, profileName string) *v1alpha1.PITRArchiveBinding {
	t.Helper()
	profile := findProfile(fg, profileName)
	if profile == nil {
		t.Fatalf("profile %q not found", profileName)
	}
	dest, err := pitrprofile.DestinationFor(fg, profile)
	if err != nil {
		t.Fatal(err)
	}
	if e := findPITRBinding(fg, dest); e != nil {
		return e
	}
	snapshot := ""
	if profile.EncryptionEnabled() {
		snapshot = truncateDNS1123("mysql-" + fg.Name + "-pitr-key-" + "0123456789")
	}
	fg.Status.PITRArchiveBindings = append(fg.Status.PITRArchiveBindings, v1alpha1.PITRArchiveBinding{
		ID:          pitrBindingID(dest),
		Destination: dest.String(),
		ArchiveID:   pitrarchive.CreateArchiveID(fg.UID, dest),
		Source:      &v1alpha1.PITRArchiveSource{Namespace: fg.Namespace, Name: fg.Name, UID: fg.UID},
		Encryption:  pitrBindingEncryptionFor(pitrprofile.EncryptionIdentityFor(profile), snapshot),
		State:       v1alpha1.PITRArchiveBindingBound,
		Reason:      pitrBindingBound,
	})
	return &fg.Status.PITRArchiveBindings[len(fg.Status.PITRArchiveBindings)-1]
}

// bindAllTestPITRArchives records a Bound binding (bindTestPITRArchive) for
// every profile of fg whose destination resolves.
func bindAllTestPITRArchives(fg *v1alpha1.MysqlFailoverGroup) {
	if fg.Spec.Backup == nil {
		return
	}
	for i := range fg.Spec.Backup.Profiles {
		p := &fg.Spec.Backup.Profiles[i]
		if _, err := pitrprofile.DestinationFor(fg, p); err != nil {
			continue
		}
		bindTestPITRArchive(nopTB{}, fg, p.Name)
	}
}

// nopTB is a testing.TB for fixtures built outside a test's scope; its
// Fatal panics.
type nopTB struct{ testing.TB }

func (nopTB) Helper()                      {}
func (nopTB) Fatal(args ...any)            { panic(args) }
func (nopTB) Fatalf(f string, args ...any) { panic(f) }

// recordTestDumpKey records, as a backup Job's launch would (Δ1), the
// dump key an encrypted backup was written with: the "backup-passphrase"
// Secret's "passphrase" key, which the encrypted fixtures' profile names.
func recordTestDumpKey(b *v1alpha1.MysqlBackup) {
	if b.Status.ArtifactStorage == nil {
		b.Status.ArtifactStorage = &v1alpha1.BackupArtifactStorage{}
	}
	b.Status.ArtifactStorage.PassphraseSecret = &v1alpha1.BackupArtifactPassphrase{Name: "backup-passphrase", Key: "passphrase"}
}

// siteDeploy is fg's site Deployment carrying render (nil: no annotation;
// a string: that raw annotation).
func siteDeploy(fg *v1alpha1.MysqlFailoverGroup, site string, render any) appsv1.Deployment {
	d := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: resourceName(fg.Name, site), Namespace: fg.Namespace, Labels: commonLabels(fg.Name, site)}}
	switch r := render.(type) {
	case pitrRenderInputs:
		d.Annotations = map[string]string{annotationPITRRender: r.Canonical()}
	case string:
		d.Annotations = map[string]string{annotationPITRRender: r}
	}
	return d
}

// renderFG is a group archiving to an encrypted S3 profile "a" (prefix p1),
// with an alias "a2" on the same storage and a profile "c" on prefix p2.
func renderFG() *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.UID = "uid-render"
	s3 := func(prefix string) v1alpha1.BackupStorage {
		return v1alpha1.BackupStorage{Type: v1alpha1.BackupStorageS3, S3: &v1alpha1.S3Storage{
			Bucket: "archive", Prefix: prefix, Region: "us-east-1", EndpointURL: "https://s3.example.com", CredentialsSecret: "creds"}}
	}
	enc := func(secret string) *v1alpha1.BackupEncryptionSpec {
		return &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: secret}}
	}
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "a"},
		Profiles: []v1alpha1.BackupProfile{
			{Name: "a", Storage: s3("p1"), Encryption: enc("key-a")},
			{Name: "a2", Storage: s3("p1"), Encryption: enc("key-a2")},
			{Name: "c", Storage: s3("p2"), Encryption: enc("key-c")},
		},
	}
	return fg
}

// setBinding records a binding for profile's destination in the given state.
func setBinding(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, profile string, state v1alpha1.PITRArchiveBindingState, reason v1alpha1.PITRArchiveBindingReason, snapshot string) *v1alpha1.PITRArchiveBinding {
	t.Helper()
	e := bindTestPITRArchive(t, fg, profile)
	e.State, e.Reason = state, reason
	if snapshot != "" {
		e.Encryption.KeySecretName = snapshot
	}
	if state != v1alpha1.PITRArchiveBindingBound {
		e.ArchiveID, e.Source = "", nil
	}
	return e
}

func destOf(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, profile string) pitrarchive.Destination {
	t.Helper()
	d, err := pitrprofile.DestinationFor(fg, findProfile(fg, profile))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// The §4.6 resolution table, at group level, row by row (IMPL-203 §5 rows
// it owns are named in each case).
func TestResolvePITRRenderMatrix(t *testing.T) {
	const snapA, snapA2, snapC = "mysql-lion-pitr-key-aaaaaaaaaa", "mysql-lion-pitr-key-bbbbbbbbbb", "mysql-lion-pitr-key-cccccccccc"
	bound := func(fg *v1alpha1.MysqlFailoverGroup) pitrRenderInputs {
		in, _ := resolvePITRRender(fg, nil)
		return in
	}
	for _, tc := range []struct {
		name    string
		setup   func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment
		action  pitrRenderAction
		enabled bool
		dest    string // profile whose destination is rendered
		key     string // KeySecretName rendered
		check   func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, in pitrRenderInputs)
	}{
		{name: "row 1: PITR disabled", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			fg.Spec.Backup.PITR.Enabled = false
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			return nil
		}},
		{name: "row 1: active profile missing", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			fg.Spec.Backup.PITR.ProfileName = "ghost"
			return nil
		}},
		{name: "row 2: Bound and eligible renders the binding", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			return nil
		}, enabled: true, dest: "a", key: snapA},
		{name: "rows 2/5/6: Bound renders the binding under encryption, selector and alias drift", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecret.Name = "renamed"
			fg.Spec.Backup.PITR.ProfileName = "a2"
			return []appsv1.Deployment{siteDeploy(fg, "dc1", "garbage")}
		}, enabled: true, dest: "a", key: snapA},
		{name: "row 2: Bound plaintext binding under an encryption edit (EncryptionChangeRefused)", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			fg.Spec.Backup.Profiles[0].Encryption = nil
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, "")
			fg.Spec.Backup.Profiles[0].Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "key-a"}}
			return nil
		}, enabled: true, dest: "a", key: "", check: func(t *testing.T, _ *v1alpha1.MysqlFailoverGroup, in pitrRenderInputs) {
			if in.EncryptionMode != "None" || in.keyRevision() != pitrKeyRevisionPlaintext {
				t.Fatalf("plaintext binding rendered %s/%q", in.EncryptionMode, in.keyRevision())
			}
		}},
		{name: "rows 18/19: Bound/KeyMaterialLost still renders the binding", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingKeyMaterialLost, snapA)
			return nil
		}, enabled: true, dest: "a", key: snapA},
		{name: "rows 3/42: Bound foreign archive made the write target keeps H", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			old := setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapC)
			_ = old
			h := bound(fg) // a's render while c... compute H from a bound a
			_ = h
			ea := setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			ea.Source = &v1alpha1.PITRArchiveSource{Namespace: "ns", Name: "other", UID: "other-uid"}
			fg2 := fg.DeepCopy()
			fg2.Spec.Backup.PITR.ProfileName = "c"
			held := bound(fg2)
			return []appsv1.Deployment{siteDeploy(fg, "dc1", held), siteDeploy(fg, "dc2", held)}
		}, enabled: true, dest: "c", key: snapC},
		{name: "row 43: Bound copy (archive ID not derived here) keeps H", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapC)
			fg2 := fg.DeepCopy()
			fg2.Spec.Backup.PITR.ProfileName = "c"
			held := bound(fg2)
			ea := setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			ea.ArchiveID = pitrarchive.CreateArchiveID(fg.UID, destOf(t, fg, "c"))
			return []appsv1.Deployment{siteDeploy(fg, "dc1", held)}
		}, enabled: true, dest: "c", key: snapC},
		{name: "row 3: Bound not eligible, no hold → disabled", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			ea := setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			ea.Source.UID = "other-uid"
			return nil
		}},
		{name: "rows 4a/37: fresh group renders the candidate optimistically", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating, snapA)
			return nil
		}, enabled: true, dest: "a", key: snapA, check: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, in pitrRenderInputs) {
			if in.ArchiveID != pitrarchive.CreateArchiveID(fg.UID, destOf(t, fg, "a")) {
				t.Fatalf("candidate archive ID %s", in.ArchiveID)
			}
		}},
		{name: "rows 4a/46: H disabled (PITR just enabled) renders the candidate", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating, snapA)
			return []appsv1.Deployment{siteDeploy(fg, "dc1", pitrRenderDisabled()), siteDeploy(fg, "dc2", pitrRenderDisabled())}
		}, enabled: true, dest: "a", key: snapA},
		{name: "rows 4a/47: encrypted, no candidate snapshot → disabled", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			return nil
		}},
		{name: "rows 4a/16: plaintext needs no snapshot", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			fg.Spec.Backup.Profiles[0].Encryption = nil
			return nil
		}, enabled: true, dest: "a", key: ""},
		{name: "rows 4b/23: Establishing renders the candidate over a running writer", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapC)
			fg2 := fg.DeepCopy()
			fg2.Spec.Backup.PITR.ProfileName = "c"
			held := bound(fg2)
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingPending, pitrBindingEstablishing, snapA)
			return []appsv1.Deployment{siteDeploy(fg, "dc1", held)}
		}, enabled: true, dest: "a", key: snapA},
		{name: "rows 4c/24/30: refused switch keeps H's identity", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapC)
			fg2 := fg.DeepCopy()
			fg2.Spec.Backup.PITR.ProfileName = "c"
			held := bound(fg2)
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveLegacy, snapA)
			return []appsv1.Deployment{siteDeploy(fg, "dc1", held), siteDeploy(fg, "dc2", held)}
		}, enabled: true, dest: "c", key: snapC, check: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, in pitrRenderInputs) {
			// K13: H's destination differs from the active profile's, so
			// H's access fields are kept.
			if in.CredentialsSecret != "creds" || in.Region != "us-east-1" {
				t.Fatalf("held access fields %+v", in)
			}
		}},
		{name: "row 44: hold keeps identity but takes the profile's new access fields", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			fg.Spec.Backup.Profiles[0].Encryption = nil
			held := bound(fg)
			fg.Spec.Backup.Profiles[0].Storage.S3.CredentialsSecret = "creds-fixed"
			fg.Spec.Backup.Profiles[0].Encryption = nil
			return []appsv1.Deployment{siteDeploy(fg, "dc1", held), siteDeploy(fg, "dc2", held)}
		}, enabled: true, dest: "a", key: "", check: func(t *testing.T, _ *v1alpha1.MysqlFailoverGroup, in pitrRenderInputs) {
			if in.CredentialsSecret != "creds-fixed" {
				t.Fatalf("access field not taken from the profile: %+v", in)
			}
		}},
		{name: "row 45: a site being created renders H like the others", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapC)
			fg2 := fg.DeepCopy()
			fg2.Spec.Backup.PITR.ProfileName = "c"
			held := bound(fg2)
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch, snapA)
			// Only dc2 exists; dc1 (the active site) is being re-created.
			fg.Status.ActiveSite = "dc1"
			return []appsv1.Deployment{siteDeploy(fg, "dc2", held)}
		}, enabled: true, dest: "c", key: snapC},
		{name: "rows 4d/38/38a: pre-#203 Deployments defer; new sites render the candidate", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch, snapA)
			return []appsv1.Deployment{siteDeploy(fg, "dc1", nil), siteDeploy(fg, "dc2", `{"schemaVersion":2}`)}
		}, action: pitrRenderDefer, enabled: true, dest: "a", key: snapA},
		{name: "row 4d → row 2 once Verified", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			return []appsv1.Deployment{siteDeploy(fg, "dc1", nil)}
		}, enabled: true, dest: "a", key: snapA},
		{name: "rows 12/13 (4a): no writer, refused candidate renders optimistically (the sidecar refuses)", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveCopy, snapA)
			return nil
		}, enabled: true, dest: "a", key: snapA},
		{name: "row 22: PITR disabled while Bound", setup: func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) []appsv1.Deployment {
			held := setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
			_ = held
			fg.Spec.Backup.PITR.Enabled = false
			return []appsv1.Deployment{siteDeploy(fg, "dc1", bound(fg))}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := renderFG()
			deploys := tc.setup(t, fg)
			in, action := resolvePITRRender(fg, deploys)
			if action != tc.action {
				t.Fatalf("action = %s, want %s", action, tc.action)
			}
			if err := in.validate(); err != nil {
				t.Fatalf("render does not validate: %v", err)
			}
			if in.Enabled != tc.enabled {
				t.Fatalf("enabled = %t, want %t (%s)", in.Enabled, tc.enabled, in.Canonical())
			}
			if !tc.enabled {
				if in != pitrRenderDisabled() {
					t.Fatalf("disabled render carries fields: %s", in.Canonical())
				}
				return
			}
			if want := destOf(t, fg, tc.dest).String(); in.Destination != want {
				t.Fatalf("destination = %s, want %s (%s)", in.Destination, want, tc.dest)
			}
			if in.KeySecretName != tc.key {
				t.Fatalf("key snapshot = %q, want %q", in.KeySecretName, tc.key)
			}
			frags, err := buildPITRSidecarFragments(fg, in)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := envValue(frags.SidecarEnv, "BLOODRAVEN_PITR_PROFILE_NAME"); ok {
				t.Fatal("the profile name is rendered (K7)")
			}
			wantRev := tc.key
			if wantRev == "" {
				wantRev = pitrKeyRevisionPlaintext
			}
			if got, _ := envValue(frags.SidecarEnv, "BLOODRAVEN_PITR_KEY_REVISION"); got != wantRev {
				t.Fatalf("key revision = %q, want %q", got, wantRev)
			}
			vol := findVolume(frags.PodVolumes, "pitr-passphrase")
			if tc.key == "" {
				if vol != nil {
					t.Fatal("plaintext render mounts a passphrase")
				}
			} else if vol == nil || vol.Secret.SecretName != tc.key || vol.Secret.Optional == nil || !*vol.Secret.Optional ||
				vol.Secret.Items[0].Key != pitrKeySnapshotDataKey {
				t.Fatalf("passphrase volume %+v, want optional snapshot %s (Δ32)", vol, tc.key)
			}
			if tc.check != nil {
				tc.check(t, fg, in)
			}
		})
	}
}

// Rows 9 and 10: a same-prefix alias renders identically (no roll) with a
// Bound binding, whatever its bytes or encryption; row 46: enabling PITR
// changes my.cnf and the render in one hash change.
func TestPITRRenderHashRows(t *testing.T) {
	const snapA = "mysql-lion-pitr-key-aaaaaaaaaa"
	fg := renderFG()
	setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, snapA)
	site := fg.Spec.Sites[0]
	base := ComputeSpecHash(fg, site, "", testPITRRender(fg))
	alias := fg.DeepCopy()
	alias.Spec.Backup.PITR.ProfileName = "a2"
	alias.Spec.Backup.Profiles[1].Encryption = nil
	if got := ComputeSpecHash(alias, site, "", testPITRRender(alias)); got != base {
		t.Fatal("rows 9/10: an alias of the bound destination rolled the pods")
	}

	off := renderFG()
	off.Spec.Backup.Profiles[0].Encryption = nil
	off.Spec.Backup.PITR.Enabled = false
	on := off.DeepCopy()
	on.Spec.Backup.PITR.Enabled = true
	offRender, onRender := testPITRRender(off), testPITRRender(on)
	if !strings.Contains(generateMyCnf(on, onRender.Enabled, site), "max-binlog-size=") ||
		strings.Contains(generateMyCnf(off, offRender.Enabled, site), "max-binlog-size=") {
		t.Fatal("row 46: my.cnf does not follow the resolved render")
	}
	if ComputeSpecHash(off, site, "", offRender) == ComputeSpecHash(on, site, "", onRender) {
		t.Fatal("row 46: enabling PITR did not change the hash")
	}
	// Δ33: PITR enabled in the spec but rendered disabled (row 47) leaves
	// my.cnf as it was, so the enable rolls once, when the render does.
	enc := renderFG()
	if r := testPITRRender(enc); r.Enabled || strings.Contains(generateMyCnf(enc, r.Enabled, site), "max-binlog-size=") {
		t.Fatal("row 47: my.cnf took the spec's PITR before the render did")
	}
}

// §4.6 row 4d through the reconciler: an existing Deployment with no
// decodable annotation is not updated at all (K3), while a missing site's
// Deployment is created with the candidate render and its annotation.
func TestReconcileDeploymentDeferLeavesDeploymentUntouched(t *testing.T) {
	ctx := context.Background()
	fg := renderFG()
	fg.Spec.Backup.Profiles[0].Encryption = nil
	existing := siteDeploy(fg, "dc1", nil)
	existing.Annotations = map[string]string{specHashAnnotation: "pre-203"}
	existing.Spec.Template.Spec.Containers = []corev1.Container{{Name: "mysql", Image: "mysql:old"}}
	r, c := newReconciler(fg, &existing)
	render := groupPITRRender{inputs: testPITRRender(fg), action: pitrRenderDefer}
	for i, site := range fg.Spec.Sites {
		if err := r.reconcileDeployment(ctx, fg, site, int32(101+i), fg.Spec.Image, render); err != nil {
			t.Fatal(err)
		}
	}
	var got appsv1.Deployment
	if err := c.Get(ctx, client.ObjectKey{Namespace: fg.Namespace, Name: existing.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[specHashAnnotation] != "pre-203" || got.Annotations[annotationPITRRender] != "" || got.Spec.Template.Spec.Containers[0].Image != "mysql:old" {
		t.Fatalf("deferred Deployment was updated: %v %v", got.Annotations, got.Spec.Template.Spec.Containers)
	}
	var created appsv1.Deployment
	if err := c.Get(ctx, client.ObjectKey{Namespace: fg.Namespace, Name: resourceName(fg.Name, "dc2")}, &created); err != nil {
		t.Fatal(err)
	}
	if created.Annotations[annotationPITRRender] != render.inputs.Canonical() {
		t.Fatalf("new site annotation %q, want the candidate %q", created.Annotations[annotationPITRRender], render.inputs.Canonical())
	}
}

// Read-side helpers: pitrReadBinding's wait/fail split (Δ22, rows 26,
// 26a), BindingLimitReached for an absent entry with a full list, and
// PITRRetentionArchiveAllowed (Δ12).
func TestPITRReadBindingAndRetentionGate(t *testing.T) {
	fg := renderFG()
	for _, tc := range []struct {
		state    v1alpha1.PITRArchiveBindingState
		reason   v1alpha1.PITRArchiveBindingReason
		ready    bool
		terminal bool
	}{
		{v1alpha1.PITRArchiveBindingBound, pitrBindingBound, true, false},
		{v1alpha1.PITRArchiveBindingBound, pitrBindingKeyMaterialLost, false, false},
		{v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating, false, false},
		{v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch, false, false},
		{v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveEncryptionMismatch, false, false},
		{v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveListFailed, false, false},
		{v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveNotFound, false, true},
		{v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveLegacy, false, true},
		{v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveDescriptorMissing, false, true},
		{v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveVersionUnsupported, false, true},
	} {
		g := fg.DeepCopy()
		setBinding(t, g, "c", tc.state, tc.reason, "mysql-lion-pitr-key-cccccccccc")
		_, err := pitrReadBinding(g, "c")
		var nr *errPITRBindingNotReady
		if (err == nil) != tc.ready {
			t.Fatalf("%s/%s: err = %v", tc.state, tc.reason, err)
		}
		if err != nil && (!errors.As(err, &nr) || nr.terminal != tc.terminal || !strings.HasPrefix(err.Error(), reasonPITRArchiveBindingNotReady)) {
			t.Fatalf("%s/%s: err = %v, terminal want %t", tc.state, tc.reason, err, tc.terminal)
		}
	}
	full := fg.DeepCopy()
	for i := 0; i < pitrArchiveBindingsMax; i++ {
		full.Status.PITRArchiveBindings = append(full.Status.PITRArchiveBindings, v1alpha1.PITRArchiveBinding{ID: "x"})
	}
	var nr *errPITRBindingNotReady
	if _, err := pitrReadBinding(full, "c"); !errors.As(err, &nr) || nr.reason != reasonPITRBindingLimitReached || nr.terminal {
		t.Fatalf("full list: %v", err)
	}

	g := fg.DeepCopy()
	setBinding(t, g, "a", v1alpha1.PITRArchiveBindingPending, pitrBindingAuthenticating, "")
	own := pitrarchive.CreateArchiveID(g.UID, destOf(t, g, "a"))
	if !PITRRetentionArchiveAllowed(g, own) {
		t.Fatal("own archive refused")
	}
	if PITRRetentionArchiveAllowed(g, pitrarchive.CreateArchiveID("other-uid", destOf(t, g, "a"))) ||
		PITRRetentionArchiveAllowed(g, pitrarchive.CreateArchiveID(g.UID, destOf(t, g, "c"))) || PITRRetentionArchiveAllowed(g, "junk") {
		t.Fatal("foreign, unbound or malformed archive accepted")
	}
}

// D-10/Δ23 (row 40): the initFromBackup archive profile before the restore
// succeeded, an archive another group created, and a Refused
// ArchiveForeign/ArchiveCopy destination are read-only.
func TestPITRProfileReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup)
		want  bool
	}{
		{"own bound archive", func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, "")
		}, false},
		{"no binding", func(*testing.T, *v1alpha1.MysqlFailoverGroup) {}, false},
		{"foreign source", func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) {
			e := setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, "")
			e.Source.UID = "other"
		}, true},
		{"refused foreign", func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveForeign, "")
		}, true},
		{"refused copy", func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingRefused, pitrBindingArchiveCopy, "")
		}, true},
		{"refused key mismatch is not read-only", func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) {
			setBinding(t, fg, "c", v1alpha1.PITRArchiveBindingRefused, pitrBindingKeyMismatch, "")
		}, false},
		{"initFromBackup archive before success", func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "c"}}
		}, true},
		{"initFromBackup archive after success", func(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{PointInTime: &v1alpha1.PointInTimeSpec{ArchiveProfileName: "c"}}
			fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseSucceeded}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := renderFG()
			tc.setup(t, fg)
			if got, why := pitrProfileReadOnly(fg, "c"); got != tc.want || (got && why == "") {
				t.Fatalf("read-only = %t (%q), want %t", got, why, tc.want)
			}
		})
	}
}

// K5 (row 49): backups stamp the archive the rendered writers use; while
// the sites disagree, or one carries no readable annotation, there is none.
func TestRenderedWriterArchiveID(t *testing.T) {
	fg := renderFG()
	fg.Spec.Backup.Profiles[0].Encryption = nil
	setBinding(t, fg, "a", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, "")
	a := testPITRRender(fg)
	fg2 := fg.DeepCopy()
	fg2.Spec.Backup.Profiles[2].Encryption = nil
	fg2.Spec.Backup.PITR.ProfileName = "c"
	setBinding(t, fg2, "c", v1alpha1.PITRArchiveBindingBound, pitrBindingBound, "")
	c := testPITRRender(fg2)
	for _, tc := range []struct {
		name   string
		deps   []appsv1.Deployment
		wantID string
		ok     bool
	}{
		{"agree", []appsv1.Deployment{siteDeploy(fg, "dc1", a), siteDeploy(fg, "dc2", a)}, a.ArchiveID, true},
		{"all disabled", []appsv1.Deployment{siteDeploy(fg, "dc1", pitrRenderDisabled()), siteDeploy(fg, "dc2", pitrRenderDisabled())}, "", true},
		{"no writer", nil, "", true},
		{"roll in progress", []appsv1.Deployment{siteDeploy(fg, "dc1", a), siteDeploy(fg, "dc2", c)}, "", false},
		{"enable in progress", []appsv1.Deployment{siteDeploy(fg, "dc1", a), siteDeploy(fg, "dc2", pitrRenderDisabled())}, "", false},
		{"unannotated", []appsv1.Deployment{siteDeploy(fg, "dc1", a), siteDeploy(fg, "dc2", nil)}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, ok, why := renderedWriterArchiveID(fg, tc.deps)
			if id != tc.wantID || ok != tc.ok || (!ok && why == "") {
				t.Fatalf("got %q/%t/%q, want %q/%t", id, ok, why, tc.wantID, tc.ok)
			}
		})
	}
}
