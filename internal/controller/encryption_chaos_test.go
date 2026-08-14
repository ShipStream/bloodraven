package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// These tests are the fast half of the encryption-at-rest chaos plan
// (.tmp/chaos-plan-v1.0.md). Each one pins a fault the plan names as a
// data-loss, deadlock or silent-coverage risk, at the cheapest level that
// can actually observe the property. Real-MySQL behaviour that cannot be
// modelled here lives in the playground scenarios instead.

// -------------------------------------------------------------------
// EXP-02 · Escrow Secret delete while sealed
// -------------------------------------------------------------------

// TestEncryptionChaos_FailedSealedSiteKeepsSecretProjection is the half
// of EXP-02 that decides whether a deleted escrow Secret costs a re-clone
// or costs the data. Failing the site is correct; rolling it back onto a
// memory-backed emptyDir is not, because the in-memory keyring inside the
// running pod is at that point the only surviving copy of the keys.
func TestEncryptionChaos_FailedSealedSiteKeepsSecretProjection(t *testing.T) {
	fg := encTestFG()
	raw := []byte("keyring")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sealed: true,
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	ctx := context.Background()
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(ctx, fg, s, raw); err != nil {
			t.Fatalf("seed escrow: %v", err)
		}
	}
	if err := c.Status().Update(ctx, fg); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	// Establish the sealed rendering first.
	reconcileOnce(t, r)
	if vol := findVolume(getDeployment(t, r, "dc1").Spec.Template.Spec.Volumes, keyringVolumeName); vol == nil || vol.Secret == nil {
		t.Fatalf("sanity: dc1 should start from the sealed rendering: %+v", vol)
	}

	// Inject: the escrow Secret is deleted out from under a sealed site.
	if err := c.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: fg.Namespace, Name: "mysql-lion-dc1-keyring-v1",
	}}); err != nil {
		t.Fatalf("delete escrow secret: %v", err)
	}

	reconcileOnce(t, r)

	var after v1alpha1.MysqlFailoverGroup
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}
	if err := c.Get(ctx, nn, &after); err != nil {
		t.Fatalf("get group: %v", err)
	}
	s := after.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc1")
	if s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("dc1 phase = %q, want Failed once its only key custody is gone", s.Phase)
	}
	if s.KeyringSecret == "" {
		t.Error("the lost Secret name must be retained so the runbook can restore it by name")
	}
	if !after.SiteKeyringSealed("dc1") {
		t.Fatal("a Failed-but-previously-sealed site must keep the sealed rendering; " +
			"rolling it onto an emptyDir would discard the only surviving keyring copy")
	}

	vol := findVolume(getDeployment(t, r, "dc1").Spec.Template.Spec.Volumes, keyringVolumeName)
	if vol == nil || vol.Secret == nil {
		t.Fatalf("dc1 keyring volume = %+v, want the (now dangling) Secret projection", vol)
	}
	if vol.Secret.SecretName != "mysql-lion-dc1-keyring-v1" {
		t.Errorf("projected %q, want the recorded escrow Secret", vol.Secret.SecretName)
	}
	// The healthy peer must be untouched by its neighbour's key loss.
	if peer := after.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2"); peer.Phase != v1alpha1.KeyringPhaseSealed {
		t.Errorf("dc2 phase = %q: one site losing custody must not disturb the other", peer.Phase)
	}
}

// TestEncryptionChaos_RestoredEscrowSecretRecoversSealed is the DR drill
// from EXP-02: the pod is still up, the admin copies the live keyring back
// into a Secret, and the site must return to Sealed without a re-clone.
func TestEncryptionChaos_RestoredEscrowSecretRecoversSealed(t *testing.T) {
	fg := encTestFG()
	raw := []byte("keyring")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseFailed,
			KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest,
			Message: "escrow Secret mysql-lion-dc1-keyring-v1 is missing",
		}},
	}
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, true),
	}), fg, sealedDeployment(fg, "dc1", "mysql-lion-dc1-keyring-v1"))
	ctx := context.Background()

	// The restore path: recreate the Secret from the live keyring bytes.
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc1", raw); err != nil {
		t.Fatalf("restore escrow: %v", err)
	}

	// A Failed site that still renders sealed re-enters verifySealedSite,
	// which re-verifies the escrow Secret and requires MySQL to confirm
	// the same digest before recovering straight to Sealed.
	for i := 0; i < 3; i++ {
		if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseSealed {
		t.Fatalf("phase = %q (%s), want Sealed after the escrow Secret was restored", s.Phase, s.Message)
	}
	if s.KeyringDigest != digest {
		t.Errorf("digest = %q, want the restored keyring digest", s.KeyringDigest)
	}
}

// Digest match alone is not enough to leave Failed: a sidecar that did
// not report the keyring component has not confirmed Read_only.
func TestEncryptionChaos_RestoredEscrowDoesNotRecoverWithoutReadOnlyEvidence(t *testing.T) {
	fg := encTestFG()
	raw := []byte("keyring")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseFailed,
			KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest,
			Message: "escrow Secret mysql-lion-dc1-keyring-v1 is missing",
		}},
	}
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": {Enabled: true, Present: true, Digest: digest},
	}), fg, sealedDeployment(fg, "dc1", "mysql-lion-dc1-keyring-v1"))
	ctx := context.Background()

	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc1", raw); err != nil {
		t.Fatalf("restore escrow: %v", err)
	}

	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("phase = %q (%s), want Failed until MySQL reports a read-only keyring", s.Phase, s.Message)
	}
	if !strings.Contains(s.Message, "waiting for MySQL to confirm") {
		t.Errorf("message = %q, want the waiting-for-confirmation wording", s.Message)
	}
}

// -------------------------------------------------------------------
// EXP-03 · Seal against an empty / keyless keyring
// -------------------------------------------------------------------

// TestEncryptionChaos_NeverSealsAgainstAKeylessKeyring covers the
// operator half of EXP-03. The sidecar refuses to push the canonical
// empty document (it reports digest ""), but an escrow Secret may already
// exist from a previous incarnation of the site. Sealing on the strength
// of that Secret alone would project stale keys over a datadir whose redo
// log is encrypted under a key nobody has.
func TestEncryptionChaos_NeverSealsAgainstAKeylessKeyring(t *testing.T) {
	fg := encTestFG()
	ctx := context.Background()
	keyless := liveKeyring("", false)
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": keyless,
	}), fg, unsealedDeployment(fg, "dc1"))

	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc1", []byte("keys-from-a-previous-life")); err != nil {
		t.Fatalf("seed escrow: %v", err)
	}

	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("phase = %q, want Unsealed while MySQL has created no keys", s.Phase)
	}
	if s.KeyringSecret != "" || s.KeyringVersion != 0 {
		t.Errorf("a keyless site recorded an escrow reference: %+v", s)
	}
	if fg.SiteKeyringSealed("dc1") {
		t.Fatal("rendered sealed against a keyring MySQL has not populated")
	}
}

// -------------------------------------------------------------------
// EXP-05 · Ordered-update stall mid-encryption adoption
// -------------------------------------------------------------------

// TestEncryptionChaos_PartialAdoptionKeepsBothSitesRestartable is the
// multi-site form of the #136 invariant. The ordered updater rolls one
// site at a time; if it stalls after the first site (a follower that
// never catches up, an operator restart, a WaitReplica timeout) the group
// sits in a mixed state for as long as it takes an admin to notice. Every
// site must be independently restartable in that window: encryption
// settings and keyring wiring travel together, or neither travels.
func TestEncryptionChaos_PartialAdoptionKeepsBothSitesRestartable(t *testing.T) {
	fg := newTestFG()
	ctx := context.Background()
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	reconcileOnce(t, r)

	canonical := map[string]string{}
	for _, site := range []string{"dc1", "dc2"} {
		canonical[site] = deploymentConfigMapName(getDeployment(t, r, site))
	}

	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}
	var live v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &live); err != nil {
		t.Fatalf("get group: %v", err)
	}
	live.Spec.TLS = &v1alpha1.TLSSpec{
		SecretName: "mysql-tls",
		IssuerRef:  v1alpha1.IssuerRef{Name: "ca", Kind: "Issuer"},
	}
	live.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	live.Annotations = map[string]string{AdoptEncryptionAnnotation: "confirm"}
	if err := c.Update(ctx, &live); err != nil {
		t.Fatalf("enable encryption: %v", err)
	}
	reconcileOnce(t, r)

	var refreshed v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &refreshed); err != nil {
		t.Fatalf("refresh group: %v", err)
	}

	// Roll exactly one site, then stall — this is what a WaitReplica
	// timeout on the second follower leaves behind.
	if err := r.reconcileDeployment(ctx, &refreshed, refreshed.Spec.Sites[0], 101, defaultMySQLImage); err != nil {
		t.Fatalf("ordered update of dc1: %v", err)
	}

	assertSiteRestartable(t, r, c, &refreshed, "dc1", true)
	assertSiteRestartable(t, r, c, &refreshed, "dc2", false)

	// The stalled site's live ConfigMap must survive garbage collection
	// for as long as its pod may still restart against it.
	if err := r.cleanupObsoleteSiteConfigMaps(ctx, &refreshed); err != nil {
		t.Fatalf("cleanup during a stalled adoption: %v", err)
	}
	var stalledConfig corev1.ConfigMap
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: refreshed.Namespace, Name: canonical["dc2"],
	}, &stalledConfig); err != nil {
		t.Fatalf("stalled site lost its live ConfigMap and can no longer restart: %v", err)
	}

	// Resuming the update finishes the job rather than needing a reset.
	if err := r.reconcileDeployment(ctx, &refreshed, refreshed.Spec.Sites[1], 101, defaultMySQLImage); err != nil {
		t.Fatalf("resumed ordered update of dc2: %v", err)
	}
	assertSiteRestartable(t, r, c, &refreshed, "dc2", true)
}

// assertSiteRestartable checks the adopt atomicity invariant for one
// site: its Deployment references a ConfigMap that exists, and the
// encryption settings in that ConfigMap agree with the presence of
// keyring wiring in the pod spec. A site that fails this check
// CrashLoops with "Check keyring fail" the moment its pod restarts.
func assertSiteRestartable(
	t *testing.T,
	r *MysqlFailoverGroupReconciler,
	c client.Client,
	fg *v1alpha1.MysqlFailoverGroup,
	site string,
	wantEncrypted bool,
) {
	t.Helper()
	d := getDeployment(t, r, site)
	name := deploymentConfigMapName(d)
	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: fg.Namespace, Name: name,
	}, &cm); err != nil {
		t.Fatalf("%s references ConfigMap %q which does not exist: %v", site, name, err)
	}
	cnfEncrypted := strings.Contains(cm.Data["bloodraven.cnf"], "binlog-encryption=ON")
	hasKeyring := findVolume(d.Spec.Template.Spec.Volumes, keyringVolumeName) != nil
	if cnfEncrypted != hasKeyring {
		t.Fatalf("%s is not restartable: my.cnf encrypted=%v but keyring wiring present=%v",
			site, cnfEncrypted, hasKeyring)
	}
	if cnfEncrypted != wantEncrypted {
		t.Fatalf("%s encrypted=%v, want %v", site, cnfEncrypted, wantEncrypted)
	}
}

// -------------------------------------------------------------------
// EXP-08 · Escrow outage
// -------------------------------------------------------------------

// TestEncryptionChaos_EscrowOutageSurfacesReasonThenFailsLoudly walks the
// escrow-outage timeline the plan asks for: the site must stay unsealed
// (never sealed against nothing), the sidecar's own reason must reach
// .status so the stall is diagnosable without reading pod logs, and once
// escrowTimeoutSeconds elapses the site must go Failed so an alert fires
// instead of it sitting quietly in Unsealed forever.
func TestEncryptionChaos_EscrowOutageSurfacesReasonThenFailsLoudly(t *testing.T) {
	fg := encTestFG()
	fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{EscrowTimeoutSeconds: 60}
	ctx := context.Background()

	stalled := liveKeyring(keyringDigest([]byte("live-but-unescrowed")), false)
	stalled.LastError = "post escrow: x509: certificate signed by unknown authority"
	r, _ := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": stalled,
	}), fg, unsealedDeployment(fg, "dc1"))

	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("phase = %q, want Unsealed while nothing is escrowed", s.Phase)
	}
	if !strings.Contains(s.Message, "x509: certificate signed by unknown authority") {
		t.Errorf("status must carry the sidecar's reason for the stall, got %q", s.Message)
	}
	if fg.SiteKeyringSealed("dc1") {
		t.Fatal("an unescrowed site must never render sealed, however long the outage lasts")
	}

	// Wind the clock past the escrow deadline.
	old := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	siteStatus(fg, "dc1").UnsealedSince = &old
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile after the deadline: %v", err)
	}
	s = siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("phase = %q, want Failed once escrowTimeoutSeconds has elapsed", s.Phase)
	}
	if fg.SiteKeyringSealed("dc1") {
		t.Fatal("a site that failed to escrow must not be sealed by the failure itself")
	}
	cond := findCondition(fg.Status.Conditions, conditionEncryptionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("EncryptionAtRestReady = %+v, want False so the alert fires", cond)
	}
}

// -------------------------------------------------------------------
// EXP-09 · Kubernetes API failures mid-lifecycle
// -------------------------------------------------------------------

// TestEncryptionChaos_APIFailureStallsWithoutBricking covers EXP-09 at
// the level the plan calls for (fake client errors, not a real API
// outage). The requirement is narrow but important: an API failure must
// surface as a retried error and must never leave a site rendered sealed
// against a Secret the operator could not read.
func TestEncryptionChaos_APIFailureStallsWithoutBricking(t *testing.T) {
	cases := []struct {
		name  string
		funcs interceptor.Funcs
	}{
		{
			name: "escrow token mint is denied",
			funcs: interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if sec, ok := obj.(*corev1.Secret); ok && strings.HasSuffix(sec.Name, "-keyring-token") {
						return apierrors.NewInternalError(errors.New("etcd unavailable"))
					}
					return c.Create(ctx, obj, opts...)
				},
			},
		},
		{
			name: "escrow Secret list is denied",
			funcs: interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.SecretList); ok {
						return apierrors.NewInternalError(errors.New("etcd unavailable"))
					}
					return c.List(ctx, list, opts...)
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := encTestFG()
			scheme := testScheme()
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(newTestSecret(), fg, unsealedDeployment(fg, "dc1"), unsealedDeployment(fg, "dc2")).
				WithInterceptorFuncs(tc.funcs).
				Build()
			r := &MysqlFailoverGroupReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(64),
				keyringStatus: scriptedKeyring(map[string]*internalmysql.KeyringStatus{
					"dc1": liveKeyring(keyringDigest([]byte("k")), false),
					"dc2": liveKeyring(keyringDigest([]byte("k")), false),
				}),
			}

			if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err == nil {
				t.Fatal("an API failure must be returned so the reconciler retries, not swallowed")
			}
			for _, name := range []string{"dc1", "dc2"} {
				if fg.SiteKeyringSealed(name) {
					t.Errorf("%s rendered sealed despite an API failure mid-lifecycle", name)
				}
				if s := siteStatus(fg, name); s != nil && s.Phase == v1alpha1.KeyringPhaseSealed {
					t.Errorf("%s advanced to Sealed on an API failure", name)
				}
			}
		})
	}
}

// -------------------------------------------------------------------
// EXP-10 · Operator restart at every phase boundary
// -------------------------------------------------------------------

// TestEncryptionChaos_OperatorRestartAtEveryPhaseBoundary is EXP-10. The
// operator holds no keyring state in memory, so a restart is only safe if
// the rendering it derives from persisted status is identical to the one
// the pod is already running. Anything else rolls the pod — and rolling
// an unsealed pod destroys the tmpfs keyring.
func TestEncryptionChaos_OperatorRestartAtEveryPhaseBoundary(t *testing.T) {
	raw := []byte("keyring")
	digest := keyringDigest(raw)

	cases := []struct {
		name       string
		site       v1alpha1.SiteEncryptionStatus
		wantSealed bool
		// wantPhase is the phase the site must still be in (or better)
		// after a restarted operator's first reconcile.
		forbidPhase v1alpha1.SiteKeyringPhase
	}{
		{
			name:       "Pending",
			site:       v1alpha1.SiteEncryptionStatus{Name: "dc1", Phase: v1alpha1.KeyringPhasePending},
			wantSealed: false,
		},
		{
			name: "Unsealed/Bootstrap",
			site: v1alpha1.SiteEncryptionStatus{
				Name: "dc1", Phase: v1alpha1.KeyringPhaseUnsealed,
				UnsealReason: v1alpha1.UnsealReasonBootstrap,
			},
			wantSealed: false,
		},
		{
			name: "Unsealed/Clone",
			site: v1alpha1.SiteEncryptionStatus{
				Name: "dc1", Phase: v1alpha1.KeyringPhaseUnsealed,
				UnsealReason: v1alpha1.UnsealReasonClone,
			},
			wantSealed: false,
		},
		{
			name: "Unsealed/Rotation",
			site: v1alpha1.SiteEncryptionStatus{
				Name: "dc1", Phase: v1alpha1.KeyringPhaseUnsealed,
				UnsealReason:  v1alpha1.UnsealReasonRotation,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest,
			},
			wantSealed: false,
			// A restart must not "finish" a rotation that never rotated.
			forbidPhase: v1alpha1.KeyringPhaseSealed,
		},
		{
			name: "Escrowed",
			site: v1alpha1.SiteEncryptionStatus{
				Name: "dc1", Phase: v1alpha1.KeyringPhaseEscrowed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest,
			},
			wantSealed: true,
		},
		{
			name: "Sealed",
			site: v1alpha1.SiteEncryptionStatus{
				Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest,
			},
			wantSealed: true,
		},
		{
			name: "Failed after sealing",
			site: v1alpha1.SiteEncryptionStatus{
				Name: "dc1", Phase: v1alpha1.KeyringPhaseFailed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest,
			},
			wantSealed: true,
		},
		{
			name: "Failed before ever escrowing",
			site: v1alpha1.SiteEncryptionStatus{
				Name: "dc1", Phase: v1alpha1.KeyringPhaseFailed,
				UnsealReason: v1alpha1.UnsealReasonBootstrap,
			},
			wantSealed: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fg := encTestFG()
			fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
				Sites: []v1alpha1.SiteEncryptionStatus{
					tc.site,
					{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
						KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
				},
			}

			// The rendering the running pod already has, derived from the
			// same status the restarted operator will read back.
			beforeSealed := fg.SiteKeyringSealed("dc1")
			if beforeSealed != tc.wantSealed {
				t.Fatalf("pre-restart rendering sealed=%v, want %v", beforeSealed, tc.wantSealed)
			}

			// Simulate the restart: persist status, then build a brand new
			// reconciler whose only knowledge of the world is the CR.
			_, c := encReconciler(t, scriptedKeyring(nil), fg)
			if err := c.Status().Update(ctx, fg); err != nil {
				t.Fatalf("persist status: %v", err)
			}
			store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
			if _, err := store.put(ctx, fg, "dc1", raw); err != nil {
				t.Fatalf("seed dc1 escrow: %v", err)
			}
			if _, err := store.put(ctx, fg, "dc2", raw); err != nil {
				t.Fatalf("seed dc2 escrow: %v", err)
			}

			restarted := &MysqlFailoverGroupReconciler{
				Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(64),
				keyringStatus: scriptedKeyring(nil), // sidecars not yet reachable after a restart
			}
			var rehydrated v1alpha1.MysqlFailoverGroup
			nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}
			if err := c.Get(ctx, nn, &rehydrated); err != nil {
				t.Fatalf("rehydrate: %v", err)
			}
			if _, err := restarted.reconcileEncryptionAtRest(ctx, &rehydrated); err != nil {
				t.Fatalf("first reconcile after restart: %v", err)
			}

			if got := rehydrated.SiteKeyringSealed("dc1"); got != tc.wantSealed {
				t.Fatalf("post-restart rendering sealed=%v, want %v — a restart must not roll the pod", got, tc.wantSealed)
			}
			s := rehydrated.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc1")
			if tc.forbidPhase != "" && s.Phase == tc.forbidPhase {
				t.Fatalf("phase advanced to %q on a bare restart with no sidecar evidence", s.Phase)
			}
			// The peer must never be disturbed by a restart.
			if peer := rehydrated.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2"); peer.Phase != v1alpha1.KeyringPhaseSealed {
				t.Errorf("dc2 phase = %q after an operator restart, want Sealed", peer.Phase)
			}
		})
	}
}

// -------------------------------------------------------------------
// EXP-11 · Pod restart mid-unseal loses the tmpfs keyring
// -------------------------------------------------------------------

// TestEncryptionChaos_UnsealedPodRestartDoesNotSealTheOldEscrow is
// EXP-11. A rotating replica whose pod dies comes back with an empty
// tmpfs keyring. The escrow Secret it was seeded from still exists, so
// the tempting-but-fatal move is to declare the site sealed against it.
// MySQL may already have rotated; the pre-rotation Secret would then
// decrypt nothing.
func TestEncryptionChaos_UnsealedPodRestartDoesNotSealTheOldEscrow(t *testing.T) {
	fg := encTestFG()
	ctx := context.Background()
	raw := []byte("pre-rotation")
	digest := keyringDigest(raw)
	fg.Status.ActiveSite = "dc1"
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc2", Phase: v1alpha1.KeyringPhaseUnsealed,
			UnsealReason:  v1alpha1.UnsealReasonRotation,
			KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest,
		}},
	}
	// The restarted pod: keyring file present but keyless, rotation not done.
	fresh := liveKeyring("", false)
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc2": fresh,
	}), fg, unsealedDeployment(fg, "dc2"))
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc2", raw); err != nil {
		t.Fatalf("seed escrow: %v", err)
	}

	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc2")
	if s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("phase = %q, want to stay Unsealed after the tmpfs keyring was lost", s.Phase)
	}
	if fg.SiteKeyringSealed("dc2") {
		t.Fatal("sealed a restarted rotation site against its pre-rotation escrow")
	}

	// Now the rotation genuinely completes with a new digest that is not
	// yet escrowed. Still not sealable.
	rotated := liveKeyring(keyringDigest([]byte("post-rotation")), false)
	rotated.RotateDone = true
	r.keyringStatus = scriptedKeyring(map[string]*internalmysql.KeyringStatus{"dc2": rotated})
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("post-rotation reconcile: %v", err)
	}
	if s = siteStatus(fg, "dc2"); s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("phase = %q: sealed against v1 while MySQL is running the rotated keyring", s.Phase)
	}
	if s.KeyringVersion != 1 || s.KeyringDigest != digest {
		t.Errorf("the recorded escrow moved without a matching push: %+v", s)
	}
}

// -------------------------------------------------------------------
// EXP-13 · Escrow token deleted / corrupted
// -------------------------------------------------------------------

func TestEncryptionChaos_EscrowTokenIsRemintedAfterDeletion(t *testing.T) {
	fg := encTestFG()
	ctx := context.Background()
	r, c := encReconciler(t, scriptedKeyring(nil), fg)

	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	name := v1alpha1.KeyringTokenSecretName(fg.Name, "dc1")
	var minted corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &minted); err != nil {
		t.Fatalf("token not minted: %v", err)
	}
	original := string(minted.Data[v1alpha1.KeyringTokenKey])

	if err := c.Delete(ctx, &minted); err != nil {
		t.Fatalf("delete token: %v", err)
	}
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile after token deletion: %v", err)
	}
	var reminted corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &reminted); err != nil {
		t.Fatalf("token was not reminted: %v", err)
	}
	if len(reminted.Data[v1alpha1.KeyringTokenKey]) < 32 {
		t.Errorf("reminted token is too short: %d bytes", len(reminted.Data[v1alpha1.KeyringTokenKey]))
	}
	if string(reminted.Data[v1alpha1.KeyringTokenKey]) == original {
		t.Error("remint reused the deleted token value")
	}

	// A corrupted (truncated) token must also be replaced rather than
	// leaving the site unable to escrow forever.
	reminted.Data[v1alpha1.KeyringTokenKey] = []byte("short")
	if err := c.Update(ctx, &reminted); err != nil {
		t.Fatalf("corrupt token: %v", err)
	}
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile after token corruption: %v", err)
	}
	var repaired corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &repaired); err != nil {
		t.Fatalf("token missing after corruption: %v", err)
	}
	if len(repaired.Data[v1alpha1.KeyringTokenKey]) < 32 {
		t.Errorf("corrupt token was not replaced: %q", repaired.Data[v1alpha1.KeyringTokenKey])
	}
}

// -------------------------------------------------------------------
// EXP-14 · Stale / wrong escrow version projected
// -------------------------------------------------------------------

// TestEncryptionChaos_StaleKeyringProjectionIsRepaired is EXP-14. After a
// rotation the Deployment must reference vN+1; a hand-edited or
// half-applied Deployment left on vN would fail to decrypt tablespaces
// that were rewrapped under the new master key. Status is authoritative,
// so the next reconcile has to put the reference back.
func TestEncryptionChaos_StaleKeyringProjectionIsRepaired(t *testing.T) {
	fg := encTestFG()
	ctx := context.Background()
	rotated := []byte("rotated-keyring")
	digest := keyringDigest(rotated)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sealed: true,
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v2", KeyringVersion: 2, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc1", []byte("pre-rotation")); err != nil {
		t.Fatalf("seed v1: %v", err)
	}
	if _, err := store.put(ctx, fg, "dc1", rotated); err != nil {
		t.Fatalf("seed v2: %v", err)
	}
	if _, err := store.put(ctx, fg, "dc2", rotated); err != nil {
		t.Fatalf("seed dc2: %v", err)
	}
	if err := c.Status().Update(ctx, fg); err != nil {
		t.Fatalf("persist status: %v", err)
	}

	reconcileOnce(t, r)
	d := getDeployment(t, r, "dc1")
	if vol := findVolume(d.Spec.Template.Spec.Volumes, keyringVolumeName); vol == nil ||
		vol.Secret == nil || vol.Secret.SecretName != "mysql-lion-dc1-keyring-v2" {
		t.Fatalf("sanity: dc1 should project v2, got %+v", vol)
	}

	// Inject: roll the Deployment back onto the superseded version. Bulk
	// reconcile deliberately does not touch existing Deployments (that is
	// the #136 fix), so the repair happens on the ordered-update apply —
	// which is the same call the UpdateController makes per site.
	for i := range d.Spec.Template.Spec.Volumes {
		v := &d.Spec.Template.Spec.Volumes[i]
		if v.Name == keyringVolumeName && v.Secret != nil {
			v.Secret.SecretName = "mysql-lion-dc1-keyring-v1"
		}
	}
	if err := c.Update(ctx, d); err != nil {
		t.Fatalf("apply stale projection: %v", err)
	}

	var refreshed v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, types.NamespacedName{Name: "lion", Namespace: "shared-lion"}, &refreshed); err != nil {
		t.Fatalf("refresh group: %v", err)
	}
	if err := r.reconcileDeployment(ctx, &refreshed, refreshed.Spec.Sites[0], 101, defaultMySQLImage); err != nil {
		t.Fatalf("ordered update apply: %v", err)
	}

	repaired := getDeployment(t, r, "dc1")
	vol := findVolume(repaired.Spec.Template.Spec.Volumes, keyringVolumeName)
	if vol == nil || vol.Secret == nil {
		t.Fatalf("keyring volume = %+v", vol)
	}
	if vol.Secret.SecretName != "mysql-lion-dc1-keyring-v2" {
		t.Fatalf("stale projection %q was not repaired to the escrow the site is sealed against",
			vol.Secret.SecretName)
	}

	// The other half of EXP-14: while the pod is still running the
	// superseded keyring, the steady-state check must call that out
	// rather than reporting a healthy seal. Otherwise the group looks
	// green while one site cannot decrypt what the newer key wrapped.
	stale := liveKeyring(keyringDigest([]byte("pre-rotation")), true)
	r.keyringStatus = scriptedKeyring(map[string]*internalmysql.KeyringStatus{"dc1": stale})
	if _, err := r.reconcileEncryptionAtRest(ctx, &refreshed); err != nil {
		t.Fatalf("steady-state reconcile: %v", err)
	}
	s := refreshed.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc1")
	if s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("phase = %q (%s), want Failed while the pod runs a keyring the escrow does not match",
			s.Phase, s.Message)
	}
	if !refreshed.SiteKeyringSealed("dc1") {
		t.Error("a digest mismatch must not roll a sealed site onto a writable keyring")
	}
}

// -------------------------------------------------------------------
// EXP-16 · Rotation vs. other topology operations
// -------------------------------------------------------------------

// TestEncryptionChaos_RotationRefusedDuringTopologyOperations covers the
// direction of EXP-16 the operator already implements: rotation is
// refused whenever the topology is not settled. The inverse (refusing to
// promote a site that is mid-rotation) is EXP-01 and is not implemented
// yet — see the deferred list in the chaos plan.
func TestEncryptionChaos_RotationRefusedDuringTopologyOperations(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*v1alpha1.MysqlFailoverGroup)
		wantMsg string
	}{
		{
			name:    "no active primary is known",
			mutate:  func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.ActiveSite = "" },
			wantMsg: "no active primary",
		},
		{
			name: "an ordered update is running",
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.UpdatePhase = string(UpdatePhaseWaitReplica)
			},
			wantMsg: "ordered update",
		},
		{
			name: "a planned failover is in flight",
			mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
					Phase: v1alpha1.PlannedFailoverPhaseDraining, Target: "dc2",
				}
			},
			wantMsg: "planned failover",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fg := sealedGroupWithActive("dc1")
			tc.mutate(fg)
			fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
			r, c := encReconciler(t, scriptedKeyring(nil), fg)
			store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
			for _, s := range []string{"dc1", "dc2"} {
				if _, err := store.put(ctx, fg, s, []byte("k")); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}

			if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			s := siteStatus(fg, "dc2")
			if s.Phase != v1alpha1.KeyringPhaseSealed {
				t.Fatalf("phase = %q: rotation must not start while the topology is in motion", s.Phase)
			}
			if !strings.Contains(strings.ToLower(s.Message), tc.wantMsg) {
				t.Errorf("refusal message = %q, want it to mention %q", s.Message, tc.wantMsg)
			}
		})
	}
}

// -------------------------------------------------------------------
// EXP-19 · Disable encryption after sealing
// -------------------------------------------------------------------

// TestEncryptionChaos_DisableRetainsEscrowAndReEnableResumes is EXP-19.
// Turning the flag off does not decrypt anything, so deleting the escrow
// Secrets would strand every encrypted tablespace still on disk. The
// status must clear, the Secrets must survive, and re-enabling must pick
// the lifecycle back up rather than requiring a wipe.
func TestEncryptionChaos_DisableRetainsEscrowAndReEnableResumes(t *testing.T) {
	fg := encTestFG()
	ctx := context.Background()
	raw := []byte("keyring")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sealed: true,
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(ctx, fg, s, raw); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	fg.Spec.EncryptionAtRest.Enabled = false
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if fg.Status.EncryptionAtRest != nil {
		t.Fatal("status.encryptionAtRest must be cleared when the flag goes off")
	}
	for _, name := range []string{"mysql-lion-dc1-keyring-v1", "mysql-lion-dc2-keyring-v1"} {
		var sec corev1.Secret
		if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &sec); err != nil {
			t.Fatalf("escrow Secret %s was deleted on disable — the data on disk is still encrypted with it: %v", name, err)
		}
	}

	// Re-enable. The group is serving, so adoption needs the annotation;
	// with it, the lifecycle restarts from the retained versions.
	fg.Spec.EncryptionAtRest.Enabled = true
	fg.Status.ActiveSite = "dc1"
	fg.Annotations = map[string]string{AdoptEncryptionAnnotation: "confirm"}
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	if fg.Status.EncryptionAtRest == nil {
		t.Fatal("re-enabling must restart the keyring lifecycle")
	}
	for _, name := range []string{"dc1", "dc2"} {
		if s := siteStatus(fg, name); s == nil || s.Phase == "" {
			t.Errorf("%s has no phase after re-enable: %+v", name, s)
		}
	}
}
