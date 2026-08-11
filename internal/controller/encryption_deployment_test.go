package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

type failDeploymentUpdateClient struct {
	client.Client
	remaining int
}

func (c *failDeploymentUpdateClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*appsv1.Deployment); ok && c.remaining > 0 {
		c.remaining--
		return errors.New("injected deployment update failure")
	}
	return c.Client.Update(ctx, obj, opts...)
}

// These tests drive the real Reconcile loop end to end so the wiring
// between the keyring state machine, the per-site ConfigMap and the
// rendered Deployment is exercised together — the individual unit tests
// can all pass while the pieces are never actually connected.

func reconcileOnce(t *testing.T, r *MysqlFailoverGroupReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func getDeployment(t *testing.T, r *MysqlFailoverGroupReconciler, site string) *appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	if err := r.Get(context.Background(), types.NamespacedName{
		Namespace: "shared-lion", Name: resourceName("lion", site),
	}, &d); err != nil {
		t.Fatalf("get deployment %s: %v", site, err)
	}
	return &d
}

func TestReconcile_EncryptedFreshDeployRendersUnsealed(t *testing.T) {
	fg := encTestFG()
	r, _ := encReconciler(t, scriptedKeyring(nil), fg)
	reconcileOnce(t, r)

	d := getDeployment(t, r, "dc1")
	vol := findVolume(d.Spec.Template.Spec.Volumes, keyringVolumeName)
	if vol == nil || vol.EmptyDir == nil || vol.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Fatalf("fresh encrypted site must boot with a memory-backed keyring: %+v", vol)
	}

	// The keyring must exist before mysqld starts or InnoDB aborts, so
	// keyring-init has to run before the config init container.
	inits := d.Spec.Template.Spec.InitContainers
	if len(inits) < 2 || inits[0].Name != "keyring-init" {
		t.Fatalf("keyring-init must run first: %v", initNames(inits))
	}

	// The keyring path must not be inside the data directory — MySQL
	// documents that explicitly, and co-locating them would put the key
	// on the same stolen volume as the ciphertext.
	mysqlC := containerByName(d.Spec.Template.Spec.Containers, "mysql")
	krMount := findMount(mysqlC.VolumeMounts, "/run/mysql-keyring")
	if krMount == nil {
		t.Fatal("mysql container has no keyring mount")
	}
	if dataMount := findMount(mysqlC.VolumeMounts, "/var/lib/mysql"); dataMount == nil {
		t.Fatal("sanity: data mount missing")
	}
}

func TestReconcile_EncryptedConfigMapCarriesComponentFiles(t *testing.T) {
	fg := encTestFG()
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	reconcileOnce(t, r)

	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "shared-lion", Name: desiredSiteConfigMapName(fg, fg.Spec.Sites[0]),
	}, &cm); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if cm.Data[keyringManifestKey] == "" {
		t.Error("per-site ConfigMap must carry the keyring manifest")
	}
	if cm.Data[keyringComponentKey] == "" {
		t.Error("per-site ConfigMap must carry the component config")
	}
	// Fresh deploy is unsealed, so the component must be writable —
	// otherwise MySQL cannot create its master keys at initialization.
	if got := cm.Data[keyringComponentKey]; !strings.Contains(got, `"read_only": false`) {
		t.Errorf("component config should be writable during bootstrap:\n%s", got)
	}
	if !strings.Contains(cm.Data["bloodraven.cnf"], "binlog-encryption=ON") {
		t.Errorf("my.cnf missing encryption settings:\n%s", cm.Data["bloodraven.cnf"])
	}
}

func TestReconcile_UnencryptedDeployHasNoKeyringArtifacts(t *testing.T) {
	// Regression guard: existing unencrypted clusters must render exactly
	// as before.
	fg := newTestFG()
	r, c := encReconciler(t, nil, fg)
	reconcileOnce(t, r)

	d := getDeployment(t, r, "dc1")
	if findVolume(d.Spec.Template.Spec.Volumes, keyringVolumeName) != nil {
		t.Error("unencrypted deployment must not have a keyring volume")
	}
	for _, ic := range d.Spec.Template.Spec.InitContainers {
		if ic.Name == "keyring-init" {
			t.Error("unencrypted deployment must not have keyring-init")
		}
	}
	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "shared-lion", Name: siteConfigMapName("lion", "dc1"),
	}, &cm); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if _, ok := cm.Data[keyringManifestKey]; ok {
		t.Error("unencrypted ConfigMap must not carry keyring files")
	}
	var tok corev1.Secret
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "shared-lion", Name: v1alpha1.KeyringTokenSecretName("lion", "dc1"),
	}, &tok)
	if err == nil {
		t.Error("no escrow token should be minted when encryption is off")
	}
}

func TestReconcile_SealedSiteRendersSecretProjection(t *testing.T) {
	fg := encTestFG()
	raw := []byte("keyring")
	digest := keyringDigest(raw)
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}
	// The sites are already Sealed and their escrow Secrets exist, so
	// verifySealedSite only needs the Secrets; the sidecar being
	// unreachable must not knock them out of Sealed.
	r, c := encReconciler(t, scriptedKeyring(nil), fg)

	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	for _, s := range []string{"dc1", "dc2"} {
		if _, err := store.put(context.Background(), fg, s, raw); err != nil {
			t.Fatalf("seed escrow: %v", err)
		}
	}
	// Persist the seeded status so Reconcile reads it back.
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	reconcileOnce(t, r)

	d := getDeployment(t, r, "dc1")
	vol := findVolume(d.Spec.Template.Spec.Volumes, keyringVolumeName)
	if vol == nil || vol.Secret == nil {
		t.Fatalf("sealed site must project the escrow Secret: %+v", vol)
	}
	if vol.Secret.SecretName != "mysql-lion-dc1-keyring-v1" {
		t.Errorf("projected %q", vol.Secret.SecretName)
	}
	for _, ic := range d.Spec.Template.Spec.InitContainers {
		if ic.Name == "keyring-init" {
			t.Error("a sealed site needs no keyring seeding")
		}
	}
	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "shared-lion", Name: desiredSiteConfigMapName(fg, fg.Spec.Sites[0]),
	}, &cm); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if !strings.Contains(cm.Data[keyringComponentKey], `"read_only": true`) {
		t.Errorf("sealed component config must be read-only:\n%s", cm.Data[keyringComponentKey])
	}
}

func TestReconcile_EncryptionAdoptionKeepsUnrolledSiteRestartable(t *testing.T) {
	fg := newTestFG()
	r, c := encReconciler(t, scriptedKeyring(nil), fg)
	reconcileOnce(t, r)

	// Establish the live unencrypted revision first. Its Deployment and
	// ConfigMap must remain a coherent, restartable pair while encryption
	// adoption prepares a different content-addressed revision.
	before := getDeployment(t, r, "dc1")
	canonical := deploymentConfigMapName(before)
	if canonical != siteConfigMapName("lion", "dc1") {
		t.Fatalf("initial config reference = %q", canonical)
	}

	var live v1alpha1.MysqlFailoverGroup
	nn := types.NamespacedName{Name: "lion", Namespace: "shared-lion"}
	if err := c.Get(context.Background(), nn, &live); err != nil {
		t.Fatalf("get group: %v", err)
	}
	live.Spec.TLS = &v1alpha1.TLSSpec{
		SecretName: "mysql-tls",
		IssuerRef:  v1alpha1.IssuerRef{Name: "ca", Kind: "Issuer"},
	}
	live.Spec.EncryptionAtRest = &v1alpha1.EncryptionAtRestSpec{Enabled: true}
	if live.Annotations == nil {
		live.Annotations = map[string]string{}
	}
	live.Annotations[AdoptEncryptionAnnotation] = "confirm"
	if err := c.Update(context.Background(), &live); err != nil {
		t.Fatalf("enable encryption: %v", err)
	}

	reconcileOnce(t, r)

	unrolled := getDeployment(t, r, "dc1")
	if got := deploymentConfigMapName(unrolled); got != canonical {
		t.Fatalf("bulk reconcile switched unrolled Deployment config from %q to %q", canonical, got)
	}
	if findVolume(unrolled.Spec.Template.Spec.Volumes, keyringVolumeName) != nil {
		t.Fatal("bulk reconcile added keyring wiring to an unrolled Deployment")
	}

	var oldConfig corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: live.Namespace, Name: canonical,
	}, &oldConfig); err != nil {
		t.Fatalf("old config disappeared before rollout: %v", err)
	}
	if strings.Contains(oldConfig.Data["bloodraven.cnf"], "binlog-encryption=ON") {
		t.Fatalf("unrolled site received encryption settings without keyring wiring:\n%s", oldConfig.Data["bloodraven.cnf"])
	}

	var refreshed v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &refreshed); err != nil {
		t.Fatalf("refresh group: %v", err)
	}
	desiredName := desiredSiteConfigMapName(&refreshed, refreshed.Spec.Sites[0])
	if desiredName == canonical {
		t.Fatal("encrypted adoption reused the mutable unencrypted ConfigMap")
	}
	var desired corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: refreshed.Namespace, Name: desiredName,
	}, &desired); err != nil {
		t.Fatalf("desired encrypted config missing: %v", err)
	}
	if desired.Immutable == nil || !*desired.Immutable {
		t.Fatal("encrypted ConfigMap revision must be immutable")
	}
	if !strings.Contains(desired.Data["bloodraven.cnf"], "binlog-encryption=ON") {
		t.Fatalf("desired revision missing encryption settings:\n%s", desired.Data["bloodraven.cnf"])
	}
	// A failed Deployment write leaves both restartability revisions in
	// place and the live pod template internally coherent. Retrying the same
	// ordered update then switches config and keyring wiring atomically.
	r.Client = &failDeploymentUpdateClient{Client: r.Client, remaining: 2}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := r.reconcileDeployment(context.Background(), &refreshed, refreshed.Spec.Sites[0], 101, defaultMySQLImage); err == nil {
			t.Fatalf("injected Deployment update failure %d was not returned", attempt)
		}
		unrolled = getDeployment(t, r, "dc1")
		if got := deploymentConfigMapName(unrolled); got != canonical {
			t.Fatalf("failed update %d changed config reference = %q, want %q", attempt, got, canonical)
		}
		if findVolume(unrolled.Spec.Template.Spec.Volumes, keyringVolumeName) != nil {
			t.Fatalf("failed update %d left an unencrypted config paired with keyring wiring", attempt)
		}
	}
	if err := r.reconcileDeployment(context.Background(), &refreshed, refreshed.Spec.Sites[0], 101, defaultMySQLImage); err != nil {
		t.Fatalf("retry ordered site update: %v", err)
	}
	rolled := getDeployment(t, r, "dc1")
	if got := deploymentConfigMapName(rolled); got != desiredName {
		t.Fatalf("ordered update config reference = %q, want %q", got, desiredName)
	}
	if findVolume(rolled.Spec.Template.Spec.Volumes, keyringVolumeName) == nil {
		t.Fatal("ordered update switched encrypted config without keyring wiring")
	}

	// Garbage collection is gated on rollout completion: until then the old
	// canonical revision remains available for any surviving old pod.
	if err := r.cleanupObsoleteSiteConfigMaps(context.Background(), &refreshed); err != nil {
		t.Fatalf("cleanup during rollout: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: refreshed.Namespace, Name: canonical,
	}, &oldConfig); err != nil {
		t.Fatalf("old config removed before rollout completion: %v", err)
	}

	rolled.Status.ObservedGeneration = rolled.Generation
	rolled.Status.Replicas = 1
	rolled.Status.UpdatedReplicas = 1
	rolled.Status.AvailableReplicas = 1
	if err := c.Status().Update(context.Background(), rolled); err != nil {
		t.Fatalf("mark rollout complete: %v", err)
	}
	if err := r.cleanupObsoleteSiteConfigMaps(context.Background(), &refreshed); err != nil {
		t.Fatalf("cleanup after rollout: %v", err)
	}
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: refreshed.Namespace, Name: canonical,
	}, &oldConfig)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("superseded config was not removed after rollout completion: %v", err)
	}
}

func initNames(cs []corev1.Container) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	return out
}

func containerByName(cs []corev1.Container, name string) *corev1.Container {
	for i := range cs {
		if cs[i].Name == name {
			return &cs[i]
		}
	}
	return nil
}
