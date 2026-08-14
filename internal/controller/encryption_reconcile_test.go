package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// scriptedKeyring returns a fetcher that serves a fixed status per site.
func scriptedKeyring(byS map[string]*internalmysql.KeyringStatus) keyringStatusFetcher {
	return func(_ context.Context, _ *v1alpha1.MysqlFailoverGroup, site string) (*internalmysql.KeyringStatus, error) {
		s, ok := byS[site]
		if !ok {
			return nil, fmt.Errorf("sidecar unreachable")
		}
		return s, nil
	}
}

func liveKeyring(digest string, readOnly bool) *internalmysql.KeyringStatus {
	return &internalmysql.KeyringStatus{
		Enabled: true,
		Present: true,
		Digest:  digest,
		Component: &internalmysql.KeyringComponentStatus{
			Name: "component_keyring_file", Status: "Active", ReadOnly: readOnly,
		},
		Coverage: &internalmysql.KeyringCoverage{
			SystemTablespaceEncrypted: true,
			RedoLogEncrypted:          true,
			UndoLogEncrypted:          true,
			BinlogEncrypted:           true,
		},
	}
}

// encReconciler builds a reconciler with a scripted keyring fetcher.
func encReconciler(t *testing.T, fetch keyringStatusFetcher, objs ...client.Object) (*MysqlFailoverGroupReconciler, client.Client) {
	t.Helper()
	r, c := newReconciler(objs...)
	r.keyringStatus = fetch
	return r, c
}

func siteStatus(fg *v1alpha1.MysqlFailoverGroup, name string) *v1alpha1.SiteEncryptionStatus {
	return fg.Status.EncryptionAtRest.SiteEncryptionStatusByName(name)
}

// --- disabled -------------------------------------------------------

func TestReconcileEncryption_DisabledIsNoop(t *testing.T) {
	fg := newTestFG()
	r, _ := encReconciler(t, nil, fg)
	requeue, err := r.reconcileEncryptionAtRest(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if requeue != 0 {
		t.Errorf("requeue = %v, want 0", requeue)
	}
	if fg.Status.EncryptionAtRest != nil {
		t.Error("disabled encryption must not populate status")
	}
}

func TestReconcileEncryption_DisablingClearsStatus(t *testing.T) {
	fg := newTestFG()
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{Sealed: true}
	r, _ := encReconciler(t, nil, fg)
	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fg.Status.EncryptionAtRest != nil {
		t.Error("status should be cleared when encryption is turned off")
	}
}

// --- bootstrap ------------------------------------------------------

func TestReconcileEncryption_FreshSitesStartPending(t *testing.T) {
	fg := encTestFG()
	// No sidecar reachable yet — the pods have not started.
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)

	requeue, err := r.reconcileEncryptionAtRest(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if requeue == 0 {
		t.Error("a site mid-lifecycle must requeue; nothing else wakes the reconciler")
	}
	for _, name := range []string{"dc1", "dc2"} {
		s := siteStatus(fg, name)
		if s == nil {
			t.Fatalf("no status entry for %s", name)
		}
		if s.Phase != v1alpha1.KeyringPhaseUnsealed {
			t.Errorf("%s phase = %q, want Unsealed", name, s.Phase)
		}
		if fg.SiteKeyringSealed(name) {
			t.Errorf("%s must not render sealed before anything is escrowed", name)
		}
	}
	if fg.Status.EncryptionAtRest.Sealed {
		t.Error("group must not report sealed while sites are bootstrapping")
	}
}

func TestReconcileEncryption_MintsEscrowTokens(t *testing.T) {
	fg := encTestFG()
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, name := range []string{"dc1", "dc2"} {
		var sec corev1.Secret
		if err := c.Get(context.Background(), types.NamespacedName{
			Namespace: fg.Namespace, Name: v1alpha1.KeyringTokenSecretName(fg.Name, name),
		}, &sec); err != nil {
			t.Errorf("token for %s not minted: %v", name, err)
		}
	}
}

// TestReconcileEncryption_RefusesToSealWithoutEscrow is the core safety
// assertion: a site whose keyring is not in a Secret must never be
// rendered sealed, because sealing projects the Secret over the live
// keyring and the site would come back with keys it cannot use.
func TestReconcileEncryption_RefusesToSealWithoutEscrow(t *testing.T) {
	fg := encTestFG()
	live := map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring("sha256:deadbeef", false),
		"dc2": liveKeyring("sha256:deadbeef", false),
	}
	r, _ := encReconciler(t, scriptedKeyring(live), fg)

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("phase = %q, want Unsealed — nothing has been escrowed", s.Phase)
	}
	if s.KeyringSecret != "" {
		t.Errorf("no escrow Secret should be recorded, got %q", s.KeyringSecret)
	}
}

// TestReconcileEncryption_RefusesToSealOnDigestMismatch covers the case
// where an escrow exists but is stale — for example a rotation wrote a
// new keyring the sidecar has not pushed yet. Sealing against the old
// version would strand the newly created master key.
func TestReconcileEncryption_RefusesToSealOnDigestMismatch(t *testing.T) {
	fg := encTestFG()
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(keyringDigest([]byte("NEW")), false),
	}), fg)

	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(context.Background(), fg, "dc1", []byte("OLD")); err != nil {
		t.Fatalf("seed escrow: %v", err)
	}

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase == v1alpha1.KeyringPhaseEscrowed || s.Phase == v1alpha1.KeyringPhaseSealed {
		t.Fatalf("phase = %q: sealed against a stale escrow", s.Phase)
	}
}

func TestReconcileEncryption_AdvancesToEscrowedOnDigestMatch(t *testing.T) {
	fg := encTestFG()
	raw := []byte("real-keyring")
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(keyringDigest(raw), false),
		"dc2": liveKeyring(keyringDigest(raw), false),
	}), fg, unsealedDeployment(fg, "dc1"), unsealedDeployment(fg, "dc2"))

	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, site := range []string{"dc1", "dc2"} {
		if _, err := store.put(context.Background(), fg, site, raw); err != nil {
			t.Fatalf("seed escrow: %v", err)
		}
	}

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseEscrowed {
		t.Fatalf("phase = %q, want Escrowed", s.Phase)
	}
	if s.KeyringSecret != "mysql-lion-dc1-keyring-v1" || s.KeyringVersion != 1 {
		t.Errorf("recorded escrow = %s v%d", s.KeyringSecret, s.KeyringVersion)
	}
	if s.KeyringDigest != keyringDigest(raw) {
		t.Errorf("recorded digest = %q", s.KeyringDigest)
	}
	// Escrowed must render sealed so the pod actually rolls.
	if !fg.SiteKeyringSealed("dc1") {
		t.Error("an Escrowed site must render the sealed keyring to start the roll")
	}
}

// --- sealing --------------------------------------------------------

func sealedDeployment(fg *v1alpha1.MysqlFailoverGroup, site, secret string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       resourceName(fg.Name, site),
			Namespace:  fg.Namespace,
			Generation: 3,
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{
						Name: keyringVolumeName,
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{SecretName: secret},
						},
					}},
				},
			},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 3, UpdatedReplicas: 1, ReadyReplicas: 1},
	}
}

func unsealedDeployment(fg *v1alpha1.MysqlFailoverGroup, site string) *appsv1.Deployment {
	d := sealedDeployment(fg, site, "")
	d.Spec.Template.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{
		EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
	}
	return d
}

func TestReconcileEncryption_SealsOnceDeploymentRolled(t *testing.T) {
	fg := encTestFG()
	raw := []byte("k")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseEscrowed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseEscrowed,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, true),
		"dc2": liveKeyring(digest, true),
	}), fg,
		sealedDeployment(fg, "dc1", "mysql-lion-dc1-keyring-v1"),
		sealedDeployment(fg, "dc2", "mysql-lion-dc2-keyring-v1"),
	)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(context.Background(), fg, s, raw); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	requeue, err := r.reconcileEncryptionAtRest(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if requeue != 0 {
		t.Errorf("a fully sealed group should not need a requeue, got %v", requeue)
	}
	for _, name := range []string{"dc1", "dc2"} {
		s := siteStatus(fg, name)
		if s.Phase != v1alpha1.KeyringPhaseSealed {
			t.Errorf("%s phase = %q (%s), want Sealed", name, s.Phase, s.Message)
		}
		if s.Coverage == nil || !s.Coverage.KeyringReadOnly {
			t.Errorf("%s coverage should record a read-only keyring: %+v", name, s.Coverage)
		}
	}
	if !fg.Status.EncryptionAtRest.Sealed {
		t.Error("group should report sealed once every site is sealed")
	}
}

func TestReconcileEncryption_WaitsForDeploymentRoll(t *testing.T) {
	fg := encTestFG()
	digest := keyringDigest([]byte("k"))
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseEscrowed,
			KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringDigest: digest,
		}},
	}
	r, _ := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, false),
	}), fg, unsealedDeployment(fg, "dc1"))

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if s := siteStatus(fg, "dc1"); s.Phase != v1alpha1.KeyringPhaseEscrowed {
		t.Errorf("phase = %q, want to stay Escrowed until the pod actually rolls", s.Phase)
	}
}

// TestReconcileEncryption_RefusesSealWhenMySQLKeyringWritable guards
// against a rendering bug (wrong plugin dir, stale ConfigMap) that would
// leave mysqld with a writable keyring behind a Secret mount. The
// operator must not call that sealed.
func TestReconcileEncryption_RefusesSealWhenMySQLKeyringWritable(t *testing.T) {
	fg := encTestFG()
	digest := keyringDigest([]byte("k"))
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseEscrowed,
			KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringDigest: digest,
		}},
	}
	r, _ := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, false), // component reports Read_only = No
	}), fg, sealedDeployment(fg, "dc1", "mysql-lion-dc1-keyring-v1"))

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if s := siteStatus(fg, "dc1"); s.Phase == v1alpha1.KeyringPhaseSealed {
		t.Fatal("declared sealed while MySQL reports a writable keyring")
	}
}

// --- steady state drift ---------------------------------------------

func TestReconcileEncryption_FailsWhenEscrowSecretDisappears(t *testing.T) {
	// A sealed site whose escrow Secret is gone is one pod restart away
	// from being permanently unreadable, so it must be loud.
	fg := encTestFG()
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
			KeyringSecret: "mysql-lion-dc1-keyring-v1",
			KeyringDigest: keyringDigest([]byte("k")),
		}},
	}
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("phase = %q, want Failed", s.Phase)
	}
	cond := findCondition(fg.Status.Conditions, conditionEncryptionReady)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("EncryptionAtRestReady should be False: %+v", cond)
	}
}

func TestReconcileEncryption_FailsOnEscrowDigestChange(t *testing.T) {
	fg := encTestFG()
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
			KeyringSecret: "mysql-lion-dc1-keyring-v1",
			KeyringDigest: keyringDigest([]byte("expected")),
		}},
	}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(context.Background(), fg, "dc1", []byte("something-else")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if s := siteStatus(fg, "dc1"); s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("phase = %q, want Failed on a digest change", s.Phase)
	}
}

func TestReconcileEncryption_SealedSurvivesUnreachableSidecar(t *testing.T) {
	// A brief sidecar outage must not knock a healthy site out of Sealed
	// — that would trigger a spurious unseal and pod roll.
	fg := encTestFG()
	raw := []byte("k")
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
			KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringDigest: keyringDigest(raw),
		}},
	}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(context.Background(), fg, "dc1", raw); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if s := siteStatus(fg, "dc1"); s.Phase != v1alpha1.KeyringPhaseSealed {
		t.Errorf("phase = %q, want to stay Sealed", s.Phase)
	}
}

// --- rotation -------------------------------------------------------

func sealedGroupWithActive(active string) *v1alpha1.MysqlFailoverGroup {
	fg := encTestFG()
	fg.Status.ActiveSite = active
	digest := keyringDigest([]byte("k"))
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sealed: true,
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringDigest: digest},
		},
	}
	return fg
}

// TestReconcileEncryption_RefusesRotationOnActivePrimary encodes the
// safety property that makes rotation acceptable at all: the only window
// where a keyring can be lost is while a site is unsealed, and on a
// replica that window is recoverable by re-cloning. On the primary it is
// not, so the operator refuses.
func TestReconcileEncryption_RefusesRotationOnActivePrimary(t *testing.T) {
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc1"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(context.Background(), fg, s, []byte("k")); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc1")
	if s.Phase != v1alpha1.KeyringPhaseSealed {
		t.Fatalf("phase = %q: the active primary must not be unsealed for rotation", s.Phase)
	}
	if s.Message == "" {
		t.Error("refusal should explain the supported procedure")
	}
}

func TestReconcileEncryption_RotatesReplica(t *testing.T) {
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(context.Background(), fg, s, []byte("k")); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc2")
	if s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("phase = %q, want Unsealed for rotation", s.Phase)
	}
	if s.UnsealReason != v1alpha1.UnsealReasonRotation {
		t.Errorf("unsealReason = %q", s.UnsealReason)
	}
	// The rendering must arm the sidecar's rotation step and seed from
	// the current escrow so existing keys survive.
	if !siteKeyringRotating(fg, "dc2") {
		t.Error("rotation must be armed in the rendering")
	}
	if siteEscrowSecretName(fg, "dc2") != "mysql-lion-dc2-keyring-v1" {
		t.Errorf("rotation unseal must seed from the current escrow, got %q", siteEscrowSecretName(fg, "dc2"))
	}
	// dc1 is untouched.
	if siteStatus(fg, "dc1").Phase != v1alpha1.KeyringPhaseSealed {
		t.Error("rotation must be scoped to the annotated site")
	}

	if err := c.Create(context.Background(), unsealedDeployment(fg, "dc2")); err != nil {
		t.Fatalf("create unsealed deployment: %v", err)
	}
	r.keyringStatus = scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc2": liveKeyring(keyringDigest([]byte("k")), false),
	})
	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("follow-up reconcile: %v", err)
	}
	s = siteStatus(fg, "dc2")
	if s.Phase != v1alpha1.KeyringPhaseUnsealed || s.UnsealReason != v1alpha1.UnsealReasonRotation {
		t.Fatalf("rotation accepted the old digest: %+v", s)
	}
	changed := liveKeyring(keyringDigest([]byte("changed")), false)
	r.keyringStatus = scriptedKeyring(map[string]*internalmysql.KeyringStatus{"dc2": changed})
	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("failed-rotation reconcile: %v", err)
	}
	if s = siteStatus(fg, "dc2"); s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("rotation advanced without RotateDone: %+v", s)
	}
	if siteStatus(fg, "dc1").Phase != v1alpha1.KeyringPhaseSealed {
		t.Error("follow-up rotation reconcile changed dc1")
	}
}

func TestReconcileEncryption_RefusesRotationDuringOrderedUpdate(t *testing.T) {
	fg := sealedGroupWithActive("dc1")
	fg.Status.UpdatePhase = "UpdatingReplica"
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(context.Background(), fg, s, []byte("k")); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if s := siteStatus(fg, "dc2"); s.Phase != v1alpha1.KeyringPhaseSealed {
		t.Fatalf("phase = %q: rotation must not race an ordered update", s.Phase)
	}
}

// --- clone gate -----------------------------------------------------

func TestRequestKeyringUnseal(t *testing.T) {
	ctx := context.Background()
	nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}

	t.Run("encryption off allows the clone immediately", func(t *testing.T) {
		fg := newTestFG()
		r, _ := encReconciler(t, nil, fg)
		ok, err := r.RequestKeyringUnseal(ctx, nn, "dc2")
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})

	t.Run("sealed site is unsealed and the clone deferred", func(t *testing.T) {
		fg := sealedGroupWithActive("dc1")
		r, c := encReconciler(t, scriptedKeyring(nil), fg)
		ok, err := r.RequestKeyringUnseal(ctx, nn, "dc2")
		if err != nil {
			t.Fatalf("unseal: %v", err)
		}
		if ok {
			t.Fatal("a sealed recipient must defer the clone until its pod has rolled — " +
				"CLONE INSTANCE cannot rewrap tablespace keys into a read-only keyring")
		}
		var latest v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &latest); err != nil {
			t.Fatalf("get: %v", err)
		}
		s := latest.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2")
		if s.Phase != v1alpha1.KeyringPhaseUnsealed || s.UnsealReason != v1alpha1.UnsealReasonClone {
			t.Errorf("status = %+v", s)
		}
		if s.KeyringSecret != "" || s.KeyringDigest != "" {
			t.Errorf("missing clone seed was retained: %+v", s)
		}
	})

	t.Run("already unsealed for clone allows the clone", func(t *testing.T) {
		fg := sealedGroupWithActive("dc1")
		fg.Status.EncryptionAtRest.Sites[1].Phase = v1alpha1.KeyringPhaseUnsealed
		fg.Status.EncryptionAtRest.Sites[1].UnsealReason = v1alpha1.UnsealReasonClone
		r, _ := encReconciler(t, scriptedKeyring(nil), fg, unsealedDeployment(fg, "dc2"))
		ok, err := r.RequestKeyringUnseal(ctx, nn, "dc2")
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
	})

	t.Run("already unsealed for bootstrap is stamped Clone and defers", func(t *testing.T) {
		fg := sealedGroupWithActive("dc1")
		fg.Status.EncryptionAtRest.Sites[1].Phase = v1alpha1.KeyringPhaseUnsealed
		fg.Status.EncryptionAtRest.Sites[1].UnsealReason = v1alpha1.UnsealReasonBootstrap
		r, c := encReconciler(t, scriptedKeyring(nil), fg, unsealedDeployment(fg, "dc2"))
		ok, err := r.RequestKeyringUnseal(ctx, nn, "dc2")
		if err != nil {
			t.Fatalf("unseal: %v", err)
		}
		if ok {
			t.Fatal("first Clone stamp must defer so the hold is observed before CLONE INSTANCE starts")
		}
		var latest v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &latest); err != nil {
			t.Fatalf("get: %v", err)
		}
		s := latest.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2")
		if s.UnsealReason != v1alpha1.UnsealReasonClone {
			t.Fatalf("unsealReason = %q, want Clone", s.UnsealReason)
		}
		ok, err = r.RequestKeyringUnseal(ctx, nn, "dc2")
		if err != nil || !ok {
			t.Fatalf("second call ok=%v err=%v, want ready after the hold is stamped", ok, err)
		}
	})

	t.Run("does not stamp Clone over an in-flight rotation", func(t *testing.T) {
		for _, phase := range []v1alpha1.SiteKeyringPhase{
			v1alpha1.KeyringPhaseUnsealed,
			v1alpha1.KeyringPhaseEscrowed,
			v1alpha1.KeyringPhaseFailed,
		} {
			fg := sealedGroupWithActive("dc1")
			fg.Status.EncryptionAtRest.Sites[1].Phase = phase
			fg.Status.EncryptionAtRest.Sites[1].UnsealReason = v1alpha1.UnsealReasonRotation
			r, c := encReconciler(t, scriptedKeyring(nil), fg, unsealedDeployment(fg, "dc2"))
			ok, err := r.RequestKeyringUnseal(ctx, nn, "dc2")
			if err != nil || ok {
				t.Fatalf("phase %s: ok=%v err=%v, want deferred during rotation", phase, ok, err)
			}
			var latest v1alpha1.MysqlFailoverGroup
			if err := c.Get(ctx, nn, &latest); err != nil {
				t.Fatalf("get: %v", err)
			}
			s := latest.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2")
			if s.UnsealReason != v1alpha1.UnsealReasonRotation || s.Phase != phase {
				t.Fatalf("phase %s: status = %+v, want Rotation left intact", phase, s)
			}
		}
	})

	t.Run("existing escrow is retained as clone seed", func(t *testing.T) {
		fg := sealedGroupWithActive("dc1")
		seedName := fg.Status.EncryptionAtRest.Sites[1].KeyringSecret
		seed := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: seedName, Namespace: fg.Namespace}}
		r, c := encReconciler(t, scriptedKeyring(nil), fg, seed)
		ok, err := r.RequestKeyringUnseal(ctx, nn, "dc2")
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		var latest v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &latest); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got := latest.Status.EncryptionAtRest.Sites[1].KeyringSecret; got != seedName {
			t.Fatalf("clone seed = %q, want %q", got, seedName)
		}
	})
}

func TestReconcileEncryption_CloneHoldDoesNotAdvanceToEscrowed(t *testing.T) {
	fg := encTestFG()
	raw := []byte("real-keyring")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseUnsealed,
				UnsealReason:  v1alpha1.UnsealReasonClone,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, true),
		"dc2": liveKeyring(digest, false),
	}), fg, unsealedDeployment(fg, "dc2"))
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(context.Background(), fg, "dc2", raw); err != nil {
		t.Fatalf("seed escrow: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
		s := siteStatus(fg, "dc2")
		if s.Phase != v1alpha1.KeyringPhaseUnsealed || s.UnsealReason != v1alpha1.UnsealReasonClone {
			t.Fatalf("pass %d: status = %+v, want Unsealed/Clone (the #144 livelock)", i, s)
		}
		if fg.SiteKeyringSealed("dc2") {
			t.Fatal("Clone hold rendered sealed; the pod would roll and the clone would never run")
		}
	}
}

func TestReconcileEncryption_CloneHoldIgnoresEscrowDeadline(t *testing.T) {
	fg := encTestFG()
	fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{EscrowTimeoutSeconds: 1}
	stale := metav1.NewTime(time.Now().Add(-time.Hour))
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseUnsealed,
				UnsealReason: v1alpha1.UnsealReasonClone, UnsealedSince: &stale},
		},
	}
	r, _ := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc2": liveKeyring("", false),
	}), fg, unsealedDeployment(fg, "dc2"))

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s := siteStatus(fg, "dc2")
	if s.Phase == v1alpha1.KeyringPhaseFailed {
		t.Fatalf("Clone hold flipped to Failed on the escrow clock: %+v", s)
	}
	if s.UnsealReason != v1alpha1.UnsealReasonClone {
		t.Fatalf("unsealReason = %q", s.UnsealReason)
	}
}

func TestNotifyCloneComplete_ReleasesHoldThenReseals(t *testing.T) {
	ctx := context.Background()
	nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}
	fg := encTestFG()
	raw := []byte("real-keyring")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseUnsealed,
				UnsealReason:  v1alpha1.UnsealReasonClone,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, true),
		"dc2": liveKeyring(digest, false),
	}), fg, unsealedDeployment(fg, "dc2"))
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc2", raw); err != nil {
		t.Fatalf("seed escrow: %v", err)
	}

	if err := r.NotifyCloneComplete(ctx, nn, "dc2"); err != nil {
		t.Fatalf("notify: %v", err)
	}
	var latest v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &latest); err != nil {
		t.Fatalf("get: %v", err)
	}
	s := latest.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2")
	if s.UnsealReason != "" || s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("after notify: %+v", s)
	}

	if _, err := r.reconcileEncryptionAtRest(ctx, &latest); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	s = siteStatus(&latest, "dc2")
	if s.Phase != v1alpha1.KeyringPhaseEscrowed {
		t.Fatalf("phase = %q, want Escrowed after the hold is released", s.Phase)
	}
	if !latest.SiteKeyringSealed("dc2") {
		t.Fatal("released clone site must render sealed so the pod rolls")
	}
}

func TestNotifyCloneComplete_FailedCloneDoesNotRenderSealed(t *testing.T) {
	ctx := context.Background()
	nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}
	fg := sealedGroupWithActive("dc1")
	fg.Status.EncryptionAtRest.Sites[1].Phase = v1alpha1.KeyringPhaseFailed
	fg.Status.EncryptionAtRest.Sites[1].UnsealReason = v1alpha1.UnsealReasonClone
	r, c := encReconciler(t, scriptedKeyring(nil), fg)

	if err := r.NotifyCloneComplete(ctx, nn, "dc2"); err != nil {
		t.Fatalf("notify: %v", err)
	}
	var latest v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &latest); err != nil {
		t.Fatalf("get: %v", err)
	}
	if latest.SiteKeyringSealed("dc2") {
		t.Fatal("releasing Failed/Clone must not render sealed against the old escrow")
	}
	s := latest.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2")
	if s.Phase != v1alpha1.KeyringPhaseUnsealed || s.UnsealReason != "" {
		t.Fatalf("after notify: %+v", s)
	}
}

func TestNotifyCloneComplete_Idempotent(t *testing.T) {
	ctx := context.Background()
	nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}

	t.Run("encryption off", func(t *testing.T) {
		fg := newTestFG()
		r, _ := encReconciler(t, nil, fg)
		if err := r.NotifyCloneComplete(ctx, nn, "dc2"); err != nil {
			t.Fatalf("notify: %v", err)
		}
	})
	t.Run("already sealed", func(t *testing.T) {
		fg := sealedGroupWithActive("dc1")
		r, _ := encReconciler(t, scriptedKeyring(nil), fg)
		if err := r.NotifyCloneComplete(ctx, nn, "dc2"); err != nil {
			t.Fatalf("notify: %v", err)
		}
		if siteStatus(fg, "dc2").Phase != v1alpha1.KeyringPhaseSealed {
			t.Fatal("notify must not disturb a sealed site")
		}
	})
}

func TestMergeEncryptionStatus_PreservesCloneHoldAndRelease(t *testing.T) {
	hold := &v1alpha1.SiteEncryptionStatus{
		Name: "dc2", Phase: v1alpha1.KeyringPhaseUnsealed,
		UnsealReason: v1alpha1.UnsealReasonClone,
	}
	released := &v1alpha1.SiteEncryptionStatus{
		Name: "dc2", Phase: v1alpha1.KeyringPhaseUnsealed,
	}
	staleAdvance := &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseEscrowed},
		},
	}
	got := mergeEncryptionStatus(staleAdvance, &v1alpha1.EncryptionAtRestStatus{Sites: []v1alpha1.SiteEncryptionStatus{*hold}})
	if s := got.SiteEncryptionStatusByName("dc2"); s.UnsealReason != v1alpha1.UnsealReasonClone || s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("stale Escrowed overwrote a live Clone hold: %+v", s)
	}

	staleBootstrap := &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{{
			Name: "dc2", Phase: v1alpha1.KeyringPhaseUnsealed,
			UnsealReason: v1alpha1.UnsealReasonBootstrap,
		}},
	}
	got = mergeEncryptionStatus(staleBootstrap, &v1alpha1.EncryptionAtRestStatus{Sites: []v1alpha1.SiteEncryptionStatus{*hold}})
	if s := got.SiteEncryptionStatusByName("dc2"); s.UnsealReason != v1alpha1.UnsealReasonClone {
		t.Fatalf("stale Bootstrap overwrote a live Clone hold: %+v", s)
	}

	staleHold := &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{*hold},
	}
	got = mergeEncryptionStatus(staleHold, &v1alpha1.EncryptionAtRestStatus{Sites: []v1alpha1.SiteEncryptionStatus{*released}})
	if s := got.SiteEncryptionStatusByName("dc2"); s.UnsealReason != "" || s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Fatalf("stale Clone hold resurrected after release: %+v", s)
	}

	liveSealed := &v1alpha1.SiteEncryptionStatus{
		Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
	}
	got = mergeEncryptionStatus(staleHold, &v1alpha1.EncryptionAtRestStatus{Sites: []v1alpha1.SiteEncryptionStatus{*liveSealed}})
	if s := got.SiteEncryptionStatusByName("dc2"); s.Phase != v1alpha1.KeyringPhaseSealed || s.UnsealReason != "" {
		t.Fatalf("stale Clone hold resurrected over live Sealed: %+v", s)
	}
}

// --- adoption guard -------------------------------------------------

func TestReconcileEncryption_RefusesAdoptionOnLiveGroup(t *testing.T) {
	fg := encTestFG()
	fg.Status.ActiveSite = "dc1" // already serving, no encryption status yet
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fg.Status.EncryptionAtRest != nil {
		t.Fatal("refused adoption must not start the keyring lifecycle")
	}
	cond := findCondition(fg.Status.Conditions, conditionEncryptionReady)
	if cond == nil || cond.Reason != "AdoptionRefused" {
		t.Fatalf("condition = %+v, want AdoptionRefused", cond)
	}
	// Rendering must stay unencrypted so the running cluster is not
	// disturbed by a refused flag.
	if fg.SiteKeyringSealed("dc1") {
		t.Error("a refused adoption must not change the rendering")
	}
}

func TestReconcileEncryption_AdoptionProceedsWithConfirmation(t *testing.T) {
	fg := encTestFG()
	fg.Status.ActiveSite = "dc1"
	fg.Annotations = map[string]string{AdoptEncryptionAnnotation: "confirm"}
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if fg.Status.EncryptionAtRest == nil {
		t.Fatal("explicit confirmation should start the lifecycle")
	}
	if s := siteStatus(fg, "dc1"); s == nil || s.Phase != v1alpha1.KeyringPhaseUnsealed {
		t.Errorf("site status = %+v", s)
	}
}

// --- site list alignment --------------------------------------------

func TestAlignSiteEncryptionStatus(t *testing.T) {
	existing := []v1alpha1.SiteEncryptionStatus{
		{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed, KeyringVersion: 4},
		{Name: "gone", Phase: v1alpha1.KeyringPhaseSealed},
	}
	got := alignSiteEncryptionStatus(existing, []string{"dc1", "dc3"})
	if len(got) != 2 {
		t.Fatalf("len = %d: %+v", len(got), got)
	}
	if got[0].Name != "dc1" || got[0].KeyringVersion != 4 {
		t.Errorf("existing site state must be preserved: %+v", got[0])
	}
	if got[1].Name != "dc3" || got[1].Phase != "" {
		t.Errorf("new site should start empty: %+v", got[1])
	}
}

func TestCheckEscrowDeadline(t *testing.T) {
	fg := encTestFG()
	fg.Spec.EncryptionAtRest.Keyring = &v1alpha1.KeyringSpec{EscrowTimeoutSeconds: 60}

	t.Run("within the window stays unsealed", func(t *testing.T) {
		recent := metav1.NewTime(time.Now().Add(-10 * time.Second))
		s := &v1alpha1.SiteEncryptionStatus{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseUnsealed, UnsealedSince: &recent,
		}
		checkEscrowDeadline(fg, s)
		if s.Phase != v1alpha1.KeyringPhaseUnsealed {
			t.Errorf("phase = %q", s.Phase)
		}
	})

	t.Run("past the window fails loudly", func(t *testing.T) {
		old := metav1.NewTime(time.Now().Add(-5 * time.Minute))
		s := &v1alpha1.SiteEncryptionStatus{
			Name: "dc1", Phase: v1alpha1.KeyringPhaseUnsealed, UnsealedSince: &old,
			Message: "waiting for the sidecar to escrow the keyring",
		}
		checkEscrowDeadline(fg, s)
		if s.Phase != v1alpha1.KeyringPhaseFailed {
			t.Errorf("phase = %q, want Failed", s.Phase)
		}
	})
}

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}
