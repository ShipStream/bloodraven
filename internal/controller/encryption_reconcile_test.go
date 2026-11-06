package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
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

// rotationRunner wires a topology manager for the test group into r, so
// rotation admission goes through the live reservation.
func rotationRunner(r *MysqlFailoverGroupReconciler, c client.Client) *TopologyManager {
	nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	settleAuthority(tm)
	r.Runner = &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{nn: {tm: tm}}}
	return tm
}

func seedEscrow(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(context.Background(), fg, s, []byte("k")); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

// status.updatePhase reaches the CR only through the topology snapshot, so
// an update that has just started is invisible to the status check; the
// live reservation must refuse the rotation.
func TestReconcileEncryption_RefusesRotationWhileUpdatePendingLive(t *testing.T) {
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	seedEscrow(t, c, fg)
	tm := rotationRunner(r, c)
	tm.mu.Lock()
	tm.deploymentUpdatePending = true
	tm.mu.Unlock()

	requeue, err := r.reconcileEncryptionAtRest(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// Nothing in status changes when the update ends, so the refusal must
	// requeue itself.
	if requeue == 0 {
		t.Error("a live refusal did not requeue the rotation")
	}
	s := siteStatus(fg, "dc2")
	if s.Phase != v1alpha1.KeyringPhaseSealed || s.UnsealReason != "" {
		t.Fatalf("site = %+v: rotation admitted while an ordered update was pending", s)
	}
	if !strings.Contains(s.Message, "rotation deferred: an ordered update is in progress") {
		t.Errorf("message = %q", s.Message)
	}
}

func TestReconcileEncryption_RotationReservationFollowsStatusWrite(t *testing.T) {
	t.Run("persisted stamp keeps the reservation until the rotation ends", func(t *testing.T) {
		fg := sealedGroupWithActive("dc1")
		fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
		r, c := encReconciler(t, scriptedKeyring(nil), fg)
		seedEscrow(t, c, fg)
		tm := rotationRunner(r, c)
		if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		tm.mu.RLock()
		durable, reserved := append([]string(nil), tm.keyringRotationBlocked...), append([]string(nil), tm.keyringRotationReserved...)
		tm.mu.RUnlock()
		if len(durable) != 1 || durable[0] != "dc2" || len(reserved) != 1 || reserved[0] != "dc2" {
			t.Fatalf("durable=%v reserved=%v, want the stamp pushed and the reservation kept", durable, reserved)
		}
		// A stale cache-derived push (runner sync) must not unblock it.
		tm.SetKeyringRotationBlocked(nil)
		if err := tm.liveHandoffRefusal("dc2")(); err == nil {
			t.Fatal("a stale runner-sync push unblocked a site whose rotation is persisted")
		}
	})
	t.Run("failed status write releases the reservation", func(t *testing.T) {
		fg := sealedGroupWithActive("dc1")
		fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
		r, c := encReconciler(t, scriptedKeyring(nil), fg)
		seedEscrow(t, c, fg)
		r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return errors.New("status write denied")
			},
		})
		tm := rotationRunner(r, c)
		if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err == nil {
			t.Fatal("reconcile succeeded with a failing status write")
		}
		tm.mu.RLock()
		blocked := tm.keyringRotationBlockedLocked("dc2")
		tm.mu.RUnlock()
		if blocked {
			t.Fatal("an unpersisted rotation left dc2 promotion-blocked")
		}
	})
}

// A status write can commit and still return an error. Only a rejection
// proves the stamp is absent; otherwise an uncached read decides, and if
// that fails the promotion block stays (fail closed).
func TestReconcileEncryption_AmbiguousStatusWriteSettlesReservation(t *testing.T) {
	timeout := apierrors.NewTimeoutError("status write timed out", 1)
	forbidden := apierrors.NewForbidden(v1alpha1.GroupVersion.WithResource("mysqlfailovergroups").GroupResource(), "lion", errors.New("rbac"))
	for _, tc := range []struct {
		name        string
		persist     bool
		writeErr    error
		readFails   bool
		wantBlocked bool
	}{
		{"committed then timed out", true, timeout, false, true},
		{"timed out without committing", false, timeout, false, false},
		{"timed out and the re-read failed", false, timeout, true, true},
		// A rejection is settled by the read too: the same site may hold
		// an earlier, possibly persisted stamp.
		{"rejected", false, forbidden, false, false},
		{"rejected and the re-read failed", false, forbidden, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := sealedGroupWithActive("dc1")
			fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
			r, c := encReconciler(t, scriptedKeyring(nil), fg)
			seedEscrow(t, c, fg)
			r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if tc.persist {
						if err := cl.SubResource(sub).Update(ctx, obj, opts...); err != nil {
							return err
						}
					}
					return tc.writeErr
				},
			})
			if tc.readFails {
				r.APIReader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
					Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
						return errors.New("api server unreachable")
					},
				})
			}
			tm := rotationRunner(r, c)
			if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err == nil {
				t.Fatal("reconcile succeeded with a failing status write")
			}
			tm.mu.RLock()
			blocked := tm.keyringRotationBlockedLocked("dc2")
			tm.mu.RUnlock()
			if blocked != tc.wantBlocked {
				t.Fatalf("dc2 promotion-blocked = %v, want %v", blocked, tc.wantBlocked)
			}
		})
	}
}

// A reservation kept fail-closed (write outcome unknown, re-read failed)
// must not outlive an unpersisted stamp just because later passes keep
// failing before their own status write: those passes settle every
// reservation the runner holds for the group.
func TestReconcileEncryption_EarlyFailingPassSettlesHeldReservations(t *testing.T) {
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	seedEscrow(t, c, fg)
	base := c.(client.WithWatch)
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
			return apierrors.NewTimeoutError("status write timed out", 1)
		},
	})
	readFails := true
	r.APIReader = interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if readFails {
				return errors.New("api server unreachable")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	tm := rotationRunner(r, c)
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	blocked := func() bool {
		tm.mu.RLock()
		defer tm.mu.RUnlock()
		return tm.keyringRotationBlockedLocked("dc2")
	}

	if _, err := r.reconcileEncryptionAtRest(context.Background(), fg); err == nil {
		t.Fatal("first pass: want the status write error")
	}
	if !blocked() {
		t.Fatal("first pass: an unconfirmed stamp must keep dc2 blocked")
	}

	// Later pass: the API server answers reads again, but the pass fails
	// before its status write (the keyring token cannot be read).
	readFails = false
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, secret := obj.(*corev1.Secret); secret {
				return errors.New("secret read failed")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	var latest v1alpha1.MysqlFailoverGroup
	if err := base.Get(context.Background(), nn, &latest); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileEncryptionAtRest(context.Background(), &latest); err == nil {
		t.Fatal("second pass: want the early error")
	}
	if blocked() {
		t.Fatal("a reservation whose stamp was never persisted outlived a pass that could confirm it")
	}
	if held := r.Runner.PendingKeyringRotationReservations(nn); len(held) != 0 {
		t.Fatalf("runner still holds %v", held)
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

// A refusal that clears on its own keeps the rotate-keyring request: once
// the block is gone the rotation proceeds without the admin re-annotating.
func TestReconcileEncryption_RetryableRefusalKeepsRotationRequest(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block func(fg *v1alpha1.MysqlFailoverGroup, tm *TopologyManager)
		clear func(fg *v1alpha1.MysqlFailoverGroup, tm *TopologyManager)
	}{
		{"live ordered update",
			func(_ *v1alpha1.MysqlFailoverGroup, tm *TopologyManager) { tm.deploymentUpdatePending = true },
			func(_ *v1alpha1.MysqlFailoverGroup, tm *TopologyManager) { tm.deploymentUpdatePending = false }},
		{"status ordered update",
			func(fg *v1alpha1.MysqlFailoverGroup, _ *TopologyManager) { fg.Status.UpdatePhase = "UpdatingReplica" },
			func(fg *v1alpha1.MysqlFailoverGroup, _ *TopologyManager) { fg.Status.UpdatePhase = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fg := sealedGroupWithActive("dc1")
			fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
			r, c := encReconciler(t, scriptedKeyring(nil), fg)
			seedEscrow(t, c, fg)
			tm := rotationRunner(r, c)
			nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

			tm.mu.Lock()
			tc.block(fg, tm)
			tm.mu.Unlock()
			requeue, err := r.reconcileEncryptionAtRest(ctx, fg)
			if err != nil {
				t.Fatalf("refused pass: %v", err)
			}
			if requeue == 0 {
				t.Error("a retryable refusal did not requeue")
			}
			var latest v1alpha1.MysqlFailoverGroup
			if err := c.Get(ctx, nn, &latest); err != nil {
				t.Fatal(err)
			}
			if latest.Annotations[RotateKeyringAnnotation] != "dc2" {
				t.Fatal("a retryable refusal consumed the rotate-keyring request")
			}

			tm.mu.Lock()
			tc.clear(&latest, tm)
			tm.mu.Unlock()
			if _, err := r.reconcileEncryptionAtRest(ctx, &latest); err != nil {
				t.Fatalf("retry pass: %v", err)
			}
			if s := siteStatus(&latest, "dc2"); s.Phase != v1alpha1.KeyringPhaseUnsealed || s.UnsealReason != v1alpha1.UnsealReasonRotation {
				t.Fatalf("dc2 = %+v: the kept request did not start the rotation once unblocked", s)
			}
		})
	}
}

// GPT round 4: a stale pass must not drop an earlier hold. Pass 1's write
// commits but times out and its re-read fails (held, fail closed). Pass 2
// starts from the stale Sealed snapshot and fails to write (its baseline
// moved, or the write is rejected). Only an uncached read may end the hold,
// and it shows the stamp persisted, so dc2 stays blocked.
func TestReconcileEncryption_StaleRejectedPassKeepsEarlierPersistedHold(t *testing.T) {
	ctx := context.Background()
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	stale := fg.DeepCopy()
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	seedEscrow(t, c, fg)
	base := c.(client.WithWatch)
	tm := rotationRunner(r, c)
	blocked := func() bool {
		tm.mu.RLock()
		defer tm.mu.RUnlock()
		return tm.keyringRotationBlockedLocked("dc2")
	}

	readFails := true
	r.APIReader = interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if readFails {
				return errors.New("api server unreachable")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	r.Client = interceptor.NewClient(base, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if err := cl.SubResource(sub).Update(ctx, obj, opts...); err != nil {
				return err
			}
			return apierrors.NewTimeoutError("status write timed out", 1)
		},
	})
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err == nil {
		t.Fatal("pass 1: want the timeout")
	}
	if !blocked() {
		t.Fatal("pass 1: a possibly persisted stamp must keep dc2 blocked")
	}

	for _, read := range []bool{false, true} {
		readFails = !read
		r.Client = interceptor.NewClient(base, interceptor.Funcs{
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return apierrors.NewConflict(v1alpha1.GroupVersion.WithResource("mysqlfailovergroups").GroupResource(), "lion", errors.New("stale"))
			},
		})
		// The stale pass now stops at its baseline check before writing;
		// either way it must not end the hold.
		_, _ = r.reconcileEncryptionAtRest(ctx, stale.DeepCopy())
		if !blocked() {
			t.Fatalf("pass 2 (re-read ok=%v) dropped the hold on a persisted rotation", read)
		}
	}
}

// A deferred rotation must not switch off the sealed-site drift check: a
// deleted escrow Secret still fails the site loudly, and the request stays.
func TestReconcileEncryption_DeferredRotationStillVerifiesSealedSite(t *testing.T) {
	ctx := context.Background()
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	seedEscrow(t, c, fg)
	tm := rotationRunner(r, c)
	tm.mu.Lock()
	tm.deploymentUpdatePending = true
	tm.mu.Unlock()
	recorder := record.NewFakeRecorder(50)
	r.Recorder = recorder

	escrow := siteStatus(fg, "dc2").KeyringSecret
	if err := c.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: fg.Namespace, Name: escrow}}); err != nil {
		t.Fatalf("delete escrow: %v", err)
	}
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if s := siteStatus(fg, "dc2"); s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("dc2 = %+v: a deferred rotation hid the missing escrow", s)
	}
	close(recorder.Events)
	missing := false
	for e := range recorder.Events {
		if strings.Contains(e, "KeyringEscrowMissing") {
			missing = true
		}
	}
	if !missing {
		t.Error("no KeyringEscrowMissing event")
	}
	var latest v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &latest); err != nil {
		t.Fatal(err)
	}
	if latest.Annotations[RotateKeyringAnnotation] != "dc2" {
		t.Error("the deferred rotation request was consumed")
	}
}

// The manager observing the target as the writable primary (status still
// naming another site) refuses like the status check does: the request is
// consumed, not retried forever.
func TestReconcileEncryption_LiveActivePrimaryRefusalConsumesRequest(t *testing.T) {
	ctx := context.Background()
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	seedEscrow(t, c, fg)
	tm := rotationRunner(r, c)
	tm.mu.Lock()
	tm.sites[0].state, tm.sites[1].state = state.StateReadOnly, state.StateWritable
	tm.mu.Unlock()

	requeue, err := r.reconcileEncryptionAtRest(ctx, fg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if s := siteStatus(fg, "dc2"); s.Phase != v1alpha1.KeyringPhaseSealed || s.Message != rotationRefusedActivePrimary {
		t.Fatalf("dc2 = %+v, want Sealed with the active-primary refusal", s)
	}
	if requeue != 0 {
		t.Error("an active-primary refusal requeued")
	}
	var latest v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &latest); err != nil {
		t.Fatal(err)
	}
	if _, kept := latest.Annotations[RotateKeyringAnnotation]; kept {
		t.Error("an active-primary request was kept for retry")
	}
}

// GPT round 5: a SUCCESSFUL stale pass must not erase a persisted rotation.
// The rotation is persisted and held; a reconcile starts from the stale
// Sealed snapshot while an ordered update is pending, so the request is
// deferred and the site stays Sealed in memory. Its status write would
// succeed, but it is conditioned on the baseline the pass read: the stamp
// survives in the API server and the promotion block survives with it.
func TestReconcileEncryption_StaleSuccessfulPassKeepsPersistedRotation(t *testing.T) {
	ctx := context.Background()
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	stale := fg.DeepCopy()
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	seedEscrow(t, c, fg)
	tm := rotationRunner(r, c)
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

	// Pass 1 persists the rotation stamp and holds the reservation.
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	var persisted v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &persisted); err != nil {
		t.Fatal(err)
	}
	if s := persisted.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2"); s.UnsealReason != v1alpha1.UnsealReasonRotation {
		t.Fatalf("setup: dc2 = %+v, want the rotation persisted", s)
	}

	// Pass 2: stale Sealed input, live update refusal, writes allowed.
	tm.mu.Lock()
	tm.deploymentUpdatePending = true
	tm.mu.Unlock()
	if _, err := r.reconcileEncryptionAtRest(ctx, stale.DeepCopy()); !errors.Is(err, errEncryptionStatusMoved) {
		t.Fatalf("pass 2 = %v, want errEncryptionStatusMoved", err)
	}

	var after v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &after); err != nil {
		t.Fatal(err)
	}
	if s := after.Status.EncryptionAtRest.SiteEncryptionStatusByName("dc2"); s.UnsealReason != v1alpha1.UnsealReasonRotation {
		t.Fatalf("stale pass erased the persisted rotation: dc2 = %+v", s)
	}
	tm.mu.RLock()
	blocked := tm.keyringRotationBlockedLocked("dc2")
	tm.mu.RUnlock()
	if !blocked {
		t.Fatal("stale pass dropped the promotion block on a persisted rotation")
	}
	if _, kept := after.Annotations[RotateKeyringAnnotation]; !kept {
		t.Error("stale pass consumed the rotation request")
	}
}

// The conditioned write still merges the clone gate's own transitions.
func TestEncryptionStatusMoved(t *testing.T) {
	base := &v1alpha1.EncryptionAtRestStatus{Sites: []v1alpha1.SiteEncryptionStatus{
		{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed, KeyringSecret: "s1"},
		{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed, KeyringSecret: "s2"},
	}}
	mod := func(f func(st *v1alpha1.EncryptionAtRestStatus)) *v1alpha1.EncryptionAtRestStatus {
		st := base.DeepCopy()
		f(st)
		return st
	}
	for _, tc := range []struct {
		name   string
		latest *v1alpha1.EncryptionAtRestStatus
		moved  bool
	}{
		{"unchanged", base.DeepCopy(), false},
		{"clone hold", mod(func(st *v1alpha1.EncryptionAtRestStatus) {
			st.Sites[1].Phase, st.Sites[1].UnsealReason = v1alpha1.KeyringPhaseUnsealed, v1alpha1.UnsealReasonClone
		}), false},
		{"rotation stamped", mod(func(st *v1alpha1.EncryptionAtRestStatus) {
			st.Sites[1].Phase, st.Sites[1].UnsealReason = v1alpha1.KeyringPhaseUnsealed, v1alpha1.UnsealReasonRotation
		}), true},
		{"failed", mod(func(st *v1alpha1.EncryptionAtRestStatus) { st.Sites[0].Phase = v1alpha1.KeyringPhaseFailed }), true},
		{"site added", mod(func(st *v1alpha1.EncryptionAtRestStatus) {
			st.Sites = append(st.Sites, v1alpha1.SiteEncryptionStatus{Name: "dc3"})
		}), true},
		{"status created", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := encryptionStatusMoved(base, tc.latest); got != tc.moved {
				t.Fatalf("moved = %v, want %v", got, tc.moved)
			}
		})
	}
	if encryptionStatusMoved(nil, nil) {
		t.Fatal("nil baseline and nil latest moved")
	}
}

// The deferral note goes when the request does, and a later request
// blocked for the same reason raises its event again.
func TestReconcileEncryption_DeferralNoteClearsWithRequest(t *testing.T) {
	ctx := context.Background()
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	seedEscrow(t, c, fg)
	tm := rotationRunner(r, c)
	tm.mu.Lock()
	tm.deploymentUpdatePending = true
	tm.mu.Unlock()
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	refusals := func(rec *record.FakeRecorder) int {
		n := 0
		for {
			select {
			case e := <-rec.Events:
				if strings.Contains(e, "KeyringRotationRefused") {
					n++
				}
			default:
				return n
			}
		}
	}
	pass := func(annotated bool) (*v1alpha1.MysqlFailoverGroup, int) {
		t.Helper()
		var latest v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &latest); err != nil {
			t.Fatal(err)
		}
		if annotated {
			latest.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
		} else {
			delete(latest.Annotations, RotateKeyringAnnotation)
		}
		if err := c.Update(ctx, &latest); err != nil {
			t.Fatal(err)
		}
		rec := record.NewFakeRecorder(50)
		r.Recorder = rec
		if _, err := r.reconcileEncryptionAtRest(ctx, &latest); err != nil {
			t.Fatal(err)
		}
		return &latest, refusals(rec)
	}

	got, events := pass(true)
	if !strings.Contains(siteStatus(got, "dc2").Message, rotationDeferredNote) || events != 1 {
		t.Fatalf("deferred pass: message %q, %d refusal events", siteStatus(got, "dc2").Message, events)
	}
	got, _ = pass(false)
	if msg := siteStatus(got, "dc2").Message; strings.Contains(msg, rotationDeferredNote) {
		t.Fatalf("deferral note outlived the request: %q", msg)
	}
	if _, events = pass(true); events != 1 {
		t.Fatalf("re-annotated request under the same block raised %d refusal events, want 1", events)
	}
}

// GPT round 7: a deferred request survives an escrow failure, and the
// repair (Failed back to Sealed, without rotating) must not consume it
// either. Once the block clears, the rotation starts without re-annotating.
func TestReconcileEncryption_DeferredRotationSurvivesEscrowRepair(t *testing.T) {
	ctx := context.Background()
	fg := sealedGroupWithActive("dc1")
	fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
	digest := siteStatus(fg, "dc2").KeyringDigest
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, true), "dc2": liveKeyring(digest, true),
	}), fg)
	seedEscrow(t, c, fg)
	tm := rotationRunner(r, c)
	tm.mu.Lock()
	tm.deploymentUpdatePending = true
	tm.mu.Unlock()
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	pass := func() *v1alpha1.MysqlFailoverGroup {
		t.Helper()
		var latest v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &latest); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileEncryptionAtRest(ctx, &latest); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if err := c.Get(ctx, nn, &latest); err != nil {
			t.Fatal(err)
		}
		if latest.Annotations[RotateKeyringAnnotation] != "dc2" {
			t.Fatalf("rotation request consumed; dc2 = %+v", siteStatus(&latest, "dc2"))
		}
		return &latest
	}

	escrowName := siteStatus(fg, "dc2").KeyringSecret
	var escrow corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: escrowName}, &escrow); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, escrow.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if s := siteStatus(pass(), "dc2"); s.Phase != v1alpha1.KeyringPhaseFailed {
		t.Fatalf("dc2 = %+v, want Failed on the missing escrow", s)
	}

	escrow.ResourceVersion = ""
	if err := c.Create(ctx, &escrow); err != nil {
		t.Fatalf("restore escrow: %v", err)
	}
	if s := siteStatus(pass(), "dc2"); s.Phase != v1alpha1.KeyringPhaseSealed {
		t.Fatalf("dc2 = %+v, want Sealed after the escrow repair", s)
	}

	tm.mu.Lock()
	tm.deploymentUpdatePending = false
	tm.mu.Unlock()
	if s := siteStatus(pass(), "dc2"); s.Phase != v1alpha1.KeyringPhaseUnsealed || s.UnsealReason != v1alpha1.UnsealReasonRotation {
		t.Fatalf("dc2 = %+v: the kept request did not start the rotation once unblocked", s)
	}
}

// GPT round 8: a rotation-stamp PUT whose response is lost may still commit
// after settlement has read the old status. Here it is held back until
// after the settlement read. Either settlement fences the object first, so
// the late PUT is rejected and no rotation can land beside a handoff; or it
// cannot fence, and the promotion block stays.
func TestReconcileEncryption_LateCommittingStampCannotOutliveItsHold(t *testing.T) {
	for _, fenceFails := range []bool{false, true} {
		name := "fence succeeds: the late PUT is rejected"
		if fenceFails {
			name = "fence fails: the hold stays"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fg := sealedGroupWithActive("dc1")
			fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
			r, c := encReconciler(t, scriptedKeyring(nil), fg)
			seedEscrow(t, c, fg)
			base := c.(client.WithWatch)
			tm := rotationRunner(r, c)
			nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

			var inFlight client.Object
			r.Client = interceptor.NewClient(base, interceptor.Funcs{
				SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
					// The PUT is on the wire; its response is lost.
					inFlight = obj.DeepCopyObject().(client.Object)
					return apierrors.NewTimeoutError("status write timed out", 1)
				},
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if fenceFails {
						return errors.New("api server unreachable")
					}
					return cl.Patch(ctx, obj, patch, opts...)
				},
			})
			if _, err := r.reconcileEncryptionAtRest(ctx, fg); err == nil {
				t.Fatal("want the lost-response error")
			}
			if inFlight == nil {
				t.Fatal("setup: no status write was attempted")
			}

			// Settlement has read Sealed. Now the delayed PUT reaches the
			// API server.
			lateErr := base.Status().Update(ctx, inFlight)
			var stored v1alpha1.MysqlFailoverGroup
			if err := base.Get(ctx, nn, &stored); err != nil {
				t.Fatal(err)
			}
			stamped := siteStatus(&stored, "dc2").UnsealReason == v1alpha1.UnsealReasonRotation
			handoffErr := tm.liveHandoffRefusal("dc2")()

			if fenceFails {
				if handoffErr == nil {
					t.Fatal("hold released without a fence; a late stamp could land beside a handoff")
				}
				return
			}
			if !apierrors.IsConflict(lateErr) || stamped {
				t.Fatalf("late PUT after the fence: err=%v, stamped=%v; want a conflict and no stamp", lateErr, stamped)
			}
			if handoffErr != nil {
				t.Fatalf("hold kept although the stamp can no longer land: %v", handoffErr)
			}
		})
	}
}

// GPT round 10: the API server answers an Update that changes nothing with
// success and the same resourceVersion. A later pass that only reproduces
// the persisted (deferred) status therefore does not invalidate an earlier
// rotation-stamp PUT whose response was lost: the stamp can still land at
// that version. The successful no-op write must not end the hold on its
// own; settlement fences first, so the late PUT is rejected, or, if it
// cannot fence, the hold stays.
func TestReconcileEncryption_NoOpStatusWriteDoesNotEndHoldBesideLostWrite(t *testing.T) {
	for _, fenceFails := range []bool{false, true} {
		name := "fence succeeds: the late PUT is rejected"
		if fenceFails {
			name = "fence fails: the hold stays"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fg := sealedGroupWithActive("dc1")
			fg.Annotations = map[string]string{RotateKeyringAnnotation: "dc2"}
			digest := siteStatus(fg, "dc2").KeyringDigest
			r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
				"dc1": liveKeyring(digest, true), "dc2": liveKeyring(digest, true),
			}), fg)
			// Pin the clock: the coverage sample stamps LastCheckTime, and a
			// pass that straddles a second boundary would otherwise not
			// reproduce the persisted status, so it would not be a no-op.
			frozen := time.Now()
			r.Now = func() time.Time { return frozen }
			seedEscrow(t, c, fg)
			base := c.(client.WithWatch)
			tm := rotationRunner(r, c)
			nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
			setUpdatePending := func(v bool) {
				tm.mu.Lock()
				tm.deploymentUpdatePending = v
				tm.mu.Unlock()
			}

			var (
				loseWrite, failFence bool
				inFlight             client.Object
				noOps                int
			)
			r.Client = interceptor.NewClient(base, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if loseWrite {
						inFlight = obj.DeepCopyObject().(client.Object)
						return apierrors.NewTimeoutError("status write timed out", 1)
					}
					// As the API server does: an Update that changes
					// nothing succeeds without a new resourceVersion.
					var stored v1alpha1.MysqlFailoverGroup
					if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), &stored); err != nil {
						return err
					}
					sent := obj.(*v1alpha1.MysqlFailoverGroup)
					if sent.ResourceVersion == stored.ResourceVersion && mustJSON(t, sent.Status) == mustJSON(t, stored.Status) {
						noOps++
						stored.DeepCopyInto(sent)
						return nil
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if failFence {
						return errors.New("api server unreachable")
					}
					return cl.Patch(ctx, obj, patch, opts...)
				},
			})
			pass := func() error {
				t.Helper()
				var latest v1alpha1.MysqlFailoverGroup
				if err := base.Get(ctx, nn, &latest); err != nil {
					t.Fatal(err)
				}
				_, err := r.reconcileEncryptionAtRest(ctx, &latest)
				return err
			}

			// A deferred request, persisted; a second deferral reproduces
			// it exactly.
			setUpdatePending(true)
			for i := 0; i < 2; i++ {
				if err := pass(); err != nil {
					t.Fatalf("deferring pass: %v", err)
				}
			}
			if noOps != 1 {
				t.Fatalf("setup: %d no-op writes, want the repeated deferral to be one", noOps)
			}

			// Admitted: the stamp's PUT loses its response, and settlement
			// cannot fence, so the hold stays.
			setUpdatePending(false)
			loseWrite, failFence = true, true
			if err := pass(); err == nil {
				t.Fatal("want the lost-response error")
			}
			if inFlight == nil {
				t.Fatal("setup: no stamp write was attempted")
			}
			if tm.liveHandoffRefusal("dc2")() == nil {
				t.Fatal("setup: the hold ended with its stamp still in flight")
			}

			// Deferred again: the write succeeds and changes nothing.
			setUpdatePending(true)
			loseWrite, failFence = false, fenceFails
			if err := pass(); err != nil {
				t.Fatalf("deferring pass: %v", err)
			}
			if noOps != 2 {
				t.Fatalf("setup: %d no-op writes, want the second deferral to be one too", noOps)
			}

			// The delayed PUT reaches the API server.
			lateErr := base.Status().Update(ctx, inFlight)
			var stored v1alpha1.MysqlFailoverGroup
			if err := base.Get(ctx, nn, &stored); err != nil {
				t.Fatal(err)
			}
			stamped := siteStatus(&stored, "dc2").UnsealReason == v1alpha1.UnsealReasonRotation
			handoffErr := tm.liveHandoffRefusal("dc2")()
			if stamped && handoffErr == nil {
				t.Fatal("the late stamp landed after a no-op write ended its hold: a rotation can run beside a handoff")
			}
			if fenceFails {
				if handoffErr == nil {
					t.Fatal("hold released without a fence")
				}
				return
			}
			if !apierrors.IsConflict(lateErr) || stamped {
				t.Fatalf("late PUT after the fence: err=%v, stamped=%v; want a conflict and no stamp", lateErr, stamped)
			}
			if handoffErr != nil {
				t.Fatalf("hold kept although the stamp can no longer land: %v", handoffErr)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Opus round 11, Minor 3: a successful write that would end a hold
// settles it, and if settlement cannot fence, the pass requeues: with
// every site Sealed nothing else would bring the group back soon, and the
// site would stay promotion-blocked.
func TestReconcileEncryption_SuccessPathSettlementFailureRequeues(t *testing.T) {
	ctx := context.Background()
	fg := sealedGroupWithActive("dc1")
	digest := siteStatus(fg, "dc2").KeyringDigest
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc1": liveKeyring(digest, true), "dc2": liveKeyring(digest, true),
	}), fg)
	seedEscrow(t, c, fg)
	tm := rotationRunner(r, c)
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	failFence := true
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if failFence {
				return apierrors.NewConflict(v1alpha1.GroupVersion.WithResource("mysqlfailovergroups").GroupResource(), obj.GetName(), errors.New("modified"))
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	// A hold whose stamp never landed (its write was lost).
	if ok, why := r.Runner.ReserveKeyringRotation(nn, "dc2"); !ok {
		t.Fatalf("setup: %s", why)
	}
	pass := func() time.Duration {
		t.Helper()
		var latest v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &latest); err != nil {
			t.Fatal(err)
		}
		requeue, err := r.reconcileEncryptionAtRest(ctx, &latest)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		return requeue
	}

	if requeue := pass(); requeue <= 0 {
		t.Fatalf("requeue=%s with a hold settlement could not end; want a retry", requeue)
	}
	if tm.liveHandoffRefusal("dc2")() == nil {
		t.Fatal("hold ended without a fence")
	}
	failFence = false
	if requeue := pass(); requeue != 0 {
		t.Fatalf("requeue=%s once settled; want none (every site Sealed)", requeue)
	}
	if err := tm.liveHandoffRefusal("dc2")(); err != nil {
		t.Fatalf("hold kept after a successful settlement: %v", err)
	}
}
