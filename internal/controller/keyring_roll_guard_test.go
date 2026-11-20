package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// keyringPodSim stands in for the Deployment controller, the kubelet and
// the sidecar around the encrypted sites of one group: it replaces a
// site's pod when its Deployment is written, gives the new pod the keyring
// its template provides (the projected escrow Secret, the seed copied into
// the memory-backed keyring, or an empty document), serves that keyring's
// digest as the sidecar would, and records a crash when the keyring lacks
// a key the site's data directory needs (MY-013279 in the field).
type keyringPodSim struct {
	t  *testing.T
	c  client.Client
	fg *v1beta1.MysqlFailoverGroup

	mu sync.Mutex
	// live is the keyring file of each site's running pod.
	live map[string][]byte
	// needs is the digest of the keyring each site's data directory needs.
	needs map[string]string
	// crashed names the sites whose new pod could not start mysqld.
	crashed map[string]string
	pods    map[string]int
}

func newKeyringPodSim(t *testing.T, c client.Client, fg *v1beta1.MysqlFailoverGroup) *keyringPodSim {
	return &keyringPodSim{t: t, c: c, fg: fg, live: map[string][]byte{}, needs: map[string]string{},
		crashed: map[string]string{}, pods: map[string]int{}}
}

func (s *keyringPodSim) fetch(_ context.Context, _ *v1beta1.MysqlFailoverGroup, site string) (*internalmysql.KeyringStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.live[site]
	if !ok {
		return nil, fmt.Errorf("sidecar of %s unreachable", site)
	}
	var d appsv1.Deployment
	if err := s.c.Get(context.Background(), types.NamespacedName{Namespace: s.fg.Namespace, Name: resourceName(s.fg.Name, site)}, &d); err != nil {
		return nil, err
	}
	return liveKeyring(keyringDigest(raw), deploymentRendersSealedKeyring(&d)), nil
}

// setLive is MySQL writing the keyring of the running pod (a clone, a
// rotation, a restart that stores a binary log key).
func (s *keyringPodSim) setLive(site string, raw []byte, needed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live[site] = raw
	if needed {
		s.needs[site] = keyringDigest(raw)
	}
}

func (s *keyringPodSim) crashes() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for site, why := range s.crashed {
		out = append(out, site+": "+why)
	}
	return strings.Join(out, "; ")
}

// startPod replaces site's pods with one running its Deployment's current
// template, and completes the Deployment's rollout status.
func (s *keyringPodSim) startPod(site string) {
	s.t.Helper()
	ctx := context.Background()
	var d appsv1.Deployment
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.fg.Namespace, Name: resourceName(s.fg.Name, site)}, &d); err != nil {
		s.t.Fatalf("get deployment %s: %v", site, err)
	}
	var pods corev1.PodList
	if err := s.c.List(ctx, &pods, client.InNamespace(s.fg.Namespace), client.MatchingLabels(d.Spec.Selector.MatchLabels)); err != nil {
		s.t.Fatalf("list pods of %s: %v", site, err)
	}
	for i := range pods.Items {
		if err := s.c.Delete(ctx, &pods.Items[i]); err != nil {
			s.t.Fatalf("delete pod %s: %v", pods.Items[i].Name, err)
		}
	}
	s.mu.Lock()
	s.pods[site]++
	n := s.pods[site]
	s.mu.Unlock()
	hash := fmt.Sprintf("h%d", n)
	labels := map[string]string{appsv1.DefaultDeploymentUniqueLabelKey: hash}
	for k, v := range d.Spec.Template.Labels {
		labels[k] = v
	}
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s-x", d.Name, hash), Namespace: d.Namespace, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet",
				Name: d.Name + "-" + hash, UID: types.UID("rs-" + hash), Controller: &controller}},
		},
		Spec: *d.Spec.Template.Spec.DeepCopy(),
	}
	if err := s.c.Create(ctx, pod); err != nil {
		s.t.Fatalf("create pod for %s: %v", site, err)
	}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
		Name: "mysql", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}}
	if err := s.c.Status().Update(ctx, pod); err != nil {
		s.t.Fatalf("pod status for %s: %v", site, err)
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if err := s.c.Status().Update(ctx, &d); err != nil {
		s.t.Fatalf("deployment status for %s: %v", site, err)
	}

	// The keyring the new pod's mysqld opens.
	raw := []byte(emptyKeyringDocument + "\n")
	if secret := podKeyringSecret(&pod.Spec); secret != "" {
		var sec corev1.Secret
		if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.fg.Namespace, Name: secret}, &sec); err != nil {
			s.t.Fatalf("keyring Secret %s of the new %s pod: %v", secret, site, err)
		}
		raw = sec.Data[v1beta1.KeyringDataFileName]
	}
	s.mu.Lock()
	s.live[site] = raw
	if need := s.needs[site]; need != "" && need != keyringDigest(raw) {
		s.crashed[site] = "[MY-013279] Can't find key from keyring (pod " + pod.Name + ")"
	}
	s.mu.Unlock()
}

// podKeyringSecret is the escrow Secret a pod spec takes its keyring from:
// the sealed projection or the unsealed seed. Empty for a fresh keyring.
func podKeyringSecret(spec *corev1.PodSpec) string {
	for _, v := range spec.Volumes {
		if (v.Name == keyringVolumeName || v.Name == keyringSeedVolumeName) && v.Secret != nil {
			return v.Secret.SecretName
		}
	}
	return ""
}

// The v1.8.0 incident (two runs on the Seamus lab): a group bootstrapped
// with initFromBackup auto-cloned its second site. The clone recipient's
// post-clone keys (the restart that installs a clone stores a binary log
// key for the new server_uuid) were only in its memory-backed keyring when
// the ordered update, which had been waiting for the site to replicate,
// rolled it seconds later onto a pod seeded from the escrow recorded
// before the clone. mysqld could never start again.
//
// With the real reconciler, drift check and ordered updater: the recipient
// is not rolled while its running keyring holds keys that are not in the
// keyring its new pod would get, whether the sidecar has not escrowed them
// yet (run 2) or has but the status still records the pre-clone version
// (run 1). Once the escrow is recorded the roll goes ahead and the pod
// comes up on the post-clone keyring.
func TestOrderedUpdate_CloneRecipientIsNotRolledOntoAPreCloneKeyring(t *testing.T) {
	ctx := context.Background()
	fg := encTestFG()
	fg.Spec.Sites[0].Name, fg.Spec.Sites[1].Name = "primary", "follower-a"
	fg.Status.ActiveSite = "primary"
	nn := client.ObjectKeyFromObject(fg)

	primaryKeys := []byte(`{"version":"1.0","elements":[{"id":"primary-master"}]}`)
	bootKeys := []byte(`{"version":"1.0","elements":[{"id":"follower-master"}]}`)
	initKeys := []byte(`{"version":"1.0","elements":[{"id":"follower-master"},{"id":"follower-binlog-1"}]}`)
	cloneKeys := []byte(`{"version":"1.0","elements":[{"id":"follower-master"},{"id":"follower-binlog-1"},{"id":"follower-binlog-2"}]}`)

	fg.Status.EncryptionAtRest = &v1beta1.EncryptionAtRestStatus{Sites: []v1beta1.SiteEncryptionStatus{
		{Name: "primary", Phase: v1beta1.KeyringPhaseSealed, KeyringSecret: "mysql-lion-primary-keyring-v1",
			KeyringVersion: 1, KeyringDigest: keyringDigest(primaryKeys)},
		{Name: "follower-a", Phase: v1beta1.KeyringPhaseUnsealed, UnsealReason: v1beta1.UnsealReasonBootstrap},
	}}
	r, c := newReconciler(fg)
	sim := newKeyringPodSim(t, c, fg)
	r.keyringStatus = sim.fetch
	r.Recorder = record.NewFakeRecorder(1000)
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "primary", primaryKeys); err != nil {
		t.Fatalf("escrow primary: %v", err)
	}
	sim.setLive("primary", primaryKeys, true)

	// First render: the primary sealed, the follower on its bootstrap
	// keyring (memory-backed, no seed).
	reconcileOnce(t, r)
	sim.startPod("primary")
	sim.startPod("follower-a")

	// The follower's bootstrap: MySQL writes its keys, the sidecar escrows
	// them (v1, v2) and the operator records v2. Its seal roll waits for
	// the ordered update, which cannot roll a site that does not replicate
	// yet (the initFromBackup restore holds the auto-clone).
	sim.setLive("follower-a", bootKeys, true)
	if _, err := store.put(ctx, fg, "follower-a", bootKeys); err != nil {
		t.Fatal(err)
	}
	sim.setLive("follower-a", initKeys, true)
	if _, err := store.put(ctx, fg, "follower-a", initKeys); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r)
	if s := siteStatus(freshGroup(t, c, fg), "follower-a"); s.Phase != v1beta1.KeyringPhaseEscrowed || s.KeyringVersion != 2 {
		t.Fatalf("follower-a after bootstrap escrow = %s v%d (%s), want Escrowed v2", s.Phase, s.KeyringVersion, s.Message)
	}

	// The auto-clone: the gate unseals the recipient (its pod still runs
	// the writable keyring, so no roll is needed), the clone and its
	// restart write a new binary log key, and the bootstrap releases the
	// hold.
	if ok, err := r.RequestKeyringUnseal(ctx, nn, "follower-a"); err != nil || ok {
		t.Fatalf("first RequestKeyringUnseal = %v, %v; want a hold stamped and false", ok, err)
	}
	if ok, err := r.RequestKeyringUnseal(ctx, nn, "follower-a"); err != nil || !ok {
		t.Fatalf("RequestKeyringUnseal on the writable pod = %v, %v; want true", ok, err)
	}
	sim.setLive("follower-a", cloneKeys, true)
	if err := r.NotifyCloneComplete(ctx, nn, "follower-a"); err != nil {
		t.Fatalf("NotifyCloneComplete: %v", err)
	}

	// The ordered update, with the follower now a healthy replica.
	h := newStallHarness(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate})
	h.tm.ApplyUpdate = func(ctx context.Context, site string) error {
		h.mu.Lock()
		h.applied = append(h.applied, site)
		h.mu.Unlock()
		if err := r.applySiteDeployment(ctx, nn, site); err != nil {
			return err
		}
		sim.startPod(site)
		if sim.crashes() != "" {
			h.pods[site].breakIt()
		}
		return nil
	}
	h.tm.KeyringRollRefusal = func(ctx context.Context, site string) error {
		return r.SiteKeyringRollRefusal(ctx, nn, site)
	}
	runner := NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(100), testutil.TestLogger())
	pass := func() {
		t.Helper()
		runner.checkSpecDrift(ctx, freshGroup(t, c, fg), h.tm)
		h.tm.checkUpdate(ctx)
		h.waitIdle(t)
		h.observed()
		h.clk.Advance(keyringRollHoldRetry)
	}

	// Run 2: the post-clone keyring is not escrowed yet.
	pass()
	if got := sim.crashes(); got != "" {
		t.Fatalf("the ordered update rolled the clone recipient before its post-clone keyring was escrowed: %s", got)
	}
	if got := h.appliedSites(); got != "" {
		t.Fatalf("applied %q while follower-a's running keyring holds unescrowed keys", got)
	}

	// Run 1: the sidecar has escrowed it (v3), but status still records v2.
	if _, err := store.put(ctx, fg, "follower-a", cloneKeys); err != nil {
		t.Fatal(err)
	}
	pass()
	if got := sim.crashes(); got != "" {
		t.Fatalf("the ordered update rolled the clone recipient onto the escrow recorded before the clone: %s", got)
	}
	if got := h.appliedSites(); got != "" {
		t.Fatalf("applied %q before the post-clone escrow was recorded", got)
	}

	// The operator records v3; the roll onto it goes ahead.
	reconcileOnce(t, r)
	if s := siteStatus(freshGroup(t, c, fg), "follower-a"); s.Phase != v1beta1.KeyringPhaseEscrowed || s.KeyringVersion != 3 {
		t.Fatalf("follower-a = %s v%d (%s), want Escrowed v3", s.Phase, s.KeyringVersion, s.Message)
	}
	pass()
	if got := sim.crashes(); got != "" {
		t.Fatalf("roll after the escrow was recorded crashed: %s", got)
	}
	if got := h.appliedSites(); got != "follower-a" {
		t.Fatalf("applied %q, want follower-a rolled once its post-clone escrow is recorded", got)
	}
	if got := podKeyringSecret(&siteDeployment(t, c, fg, "follower-a").Spec.Template.Spec); got != "mysql-lion-follower-a-keyring-v3" {
		t.Fatalf("follower-a keyring source = %q, want the post-clone escrow v3", got)
	}
	reconcileOnce(t, r)
	if s := siteStatus(freshGroup(t, c, fg), "follower-a"); s.Phase != v1beta1.KeyringPhaseSealed {
		t.Fatalf("follower-a = %s (%s), want Sealed", s.Phase, s.Message)
	}
}

// The keyring-init bootstrap document and the digest the guard treats as
// "nothing stored yet" must be the file keyring-init writes.
func TestEmptyKeyringDocumentIsWhatKeyringInitWrites(t *testing.T) {
	if !strings.Contains(keyringInitScript(false), `printf '%s\n' '`+emptyKeyringDocument+`'`) {
		t.Fatalf("keyringInitScript(false) does not write emptyKeyringDocument:\n%s", keyringInitScript(false))
	}
	if emptyKeyringDigest != keyringDigest([]byte(emptyKeyringDocument+"\n")) {
		t.Fatal("emptyKeyringDigest is not the digest of the document with its newline")
	}
}

// guardDeployment is site's Deployment in a given keyring rendering:
// sealed against secret, or unsealed seeded from seed ("" = no seed).
func guardDeployment(fg *v1beta1.MysqlFailoverGroup, site string, sealed bool, secret string) *appsv1.Deployment {
	frags := buildEncryptionFragments(fg, *fg.Spec.SiteByName(site), sealed, secret, false)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(fg.Name, site), Namespace: fg.Namespace},
		Spec: appsv1.DeploymentSpec{
			Selector: mysqlDeploymentSelector(fg, site),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: mysqlDeploymentSelector(fg, site).MatchLabels},
				Spec:       corev1.PodSpec{Volumes: frags.PodVolumes},
			},
		},
	}
}

// guardPod is a pod of d's ReplicaSet hash, running d's template, whose
// mysql container has started when started is set.
func guardPod(d *appsv1.Deployment, hash string, started bool) *corev1.Pod {
	controller := true
	labels := map[string]string{appsv1.DefaultDeploymentUniqueLabelKey: hash}
	for k, v := range d.Spec.Template.Labels {
		labels[k] = v
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: d.Name + "-" + hash, Namespace: d.Namespace, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: d.Name + "-" + hash,
				UID: types.UID(hash), Controller: &controller}}},
		Spec:   *d.Spec.Template.Spec.DeepCopy(),
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	if started {
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "mysql",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	}
	return pod
}

func escrowSecret(fg *v1beta1.MysqlFailoverGroup, site string, version int32, raw []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: v1beta1.KeyringSecretName(fg.Name, site, version), Namespace: fg.Namespace},
		Data:       map[string][]byte{v1beta1.KeyringDataFileName: raw},
	}
}

func TestKeyringRollRefusal(t *testing.T) {
	v1, v2 := []byte("keyring-v1"), []byte("keyring-v2")
	v1Name, v2Name := "mysql-lion-dc2-keyring-v1", "mysql-lion-dc2-keyring-v2"
	written := keyringDigest([]byte("keys MySQL stored since the pod started"))
	present := func(digest string) *internalmysql.KeyringStatus { return liveKeyring(digest, false) }
	for _, tc := range []struct {
		name      string
		plaintext bool
		sealed    bool   // the running Deployment's rendering
		seed      string // its seed or projection
		pods      []bool // per pod: has its mysql container started
		live      *internalmysql.KeyringStatus
		next      string
		refused   string // substring of the refusal, "" = allowed
	}{
		{name: "encryption off", plaintext: true, pods: []bool{true}, next: v2Name},
		{name: "sealed pod keeps its keys in the projected Secret", sealed: true, seed: v1Name, pods: []bool{true}, next: v2Name},
		{name: "no pod", seed: v1Name, next: v2Name},
		{name: "mysqld never started", seed: v1Name, pods: []bool{false}, next: v2Name},
		{name: "unreadable sidecar", seed: v1Name, pods: []bool{true}, next: v2Name, refused: "cannot read the keyring"},
		{name: "sidecar without a keyring", seed: v1Name, pods: []bool{true}, live: &internalmysql.KeyringStatus{}, next: v2Name,
			refused: "does not report a keyring"},
		{name: "no keyring file", seed: v1Name, pods: []bool{true}, live: &internalmysql.KeyringStatus{Enabled: true}, next: v2Name},
		{name: "keyring file unreadable", seed: v1Name, pods: []bool{true},
			live: &internalmysql.KeyringStatus{Enabled: true, Present: true, LastError: "permission denied"}, next: v2Name,
			refused: "cannot read the keyring (sidecar reports: permission denied)"},
		{name: "nothing stored since the pod started from its seed", seed: v1Name, pods: []bool{true}, live: present(keyringDigest(v1)), next: v2Name},
		{name: "nothing stored since the pod started empty", pods: []bool{true}, live: present(emptyKeyringDigest), next: v2Name},
		{name: "the new pod gets the running keyring", seed: v1Name, pods: []bool{true}, live: present(keyringDigest(v2)), next: v2Name},
		{name: "keys only the running pod has", seed: v1Name, pods: []bool{true}, live: present(written), next: v2Name,
			refused: "not in escrow " + v2Name},
		{name: "keys only the running pod has, fresh keyring next", pods: []bool{true}, live: present(written), next: "",
			refused: "not in a fresh keyring"},
		{name: "keys only the running pod has, seed unchanged", seed: v1Name, pods: []bool{true}, live: present(written), next: v1Name,
			refused: "not in escrow " + v1Name},
		{name: "next escrow missing", seed: v1Name, pods: []bool{true}, live: present(written), next: "mysql-lion-dc2-keyring-v9",
			refused: "cannot read mysql-lion-dc2-keyring-v9"},
		{name: "two pods with writable keyrings", seed: v1Name, pods: []bool{true, true}, live: present(keyringDigest(v2)), next: v2Name,
			refused: "2 running pods"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := encTestFG()
			if tc.plaintext {
				fg.Spec.EncryptionAtRest = nil
			}
			d := guardDeployment(encTestFG(), "dc2", tc.sealed, tc.seed)
			objs := []client.Object{fg, d, escrowSecret(fg, "dc2", 1, v1), escrowSecret(fg, "dc2", 2, v2)}
			for i, started := range tc.pods {
				objs = append(objs, guardPod(d, fmt.Sprintf("h%d", i), started))
			}
			byS := map[string]*internalmysql.KeyringStatus{}
			if tc.live != nil {
				byS["dc2"] = tc.live
			}
			r, _ := encReconciler(t, scriptedKeyring(byS), objs...)
			err := r.keyringRollRefusal(context.Background(), fg, "dc2", d, tc.next)
			if tc.refused == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var refused *KeyringRollRefusedError
			if !errors.As(err, &refused) || refused.Site != "dc2" || !strings.Contains(err.Error(), tc.refused) {
				t.Fatalf("err = %v, want a refusal for dc2 containing %q", err, tc.refused)
			}
		})
	}
}

func keyringHeld(t *testing.T, err error, sites ...string) {
	t.Helper()
	var held *KeyringRollHeldError
	if !errors.As(err, &held) || !slices.Equal(held.Sites, sites) {
		t.Fatalf("err = %v, want a keyring roll hold of %v", err, sites)
	}
}

// A held follower is skipped; the other followers are still rolled.
func TestExecuteTargets_KeyringHeldFollowerIsNotRolled(t *testing.T) {
	uc := podGateController()
	active := &testutil.FakeMySQL{ReadOnlyVal: false}
	var applied []string
	refusal := &KeyringRollRefusedError{Site: "standby", Reason: "keys only its pod has"}
	processed, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: active},
		[]UpdateTarget{
			{Name: "standby", Host: "standby-internal", Checker: directReplicaOf("primary-internal"), Promotable: true, Drifted: true,
				ExpectedSource: "primary-internal", KeyringRollBlocked: func(context.Context) error { return refusal }},
			{Name: "reader", Host: "reader-internal", Checker: directReplicaOf("primary-internal"), Drifted: true,
				ExpectedSource: "primary-internal", KeyringRollBlocked: func(context.Context) error { return nil }},
		},
		func(_ context.Context, site string) error { applied = append(applied, site); return nil },
		func(site, _ string) { t.Errorf("authority moved to %s", site) })
	keyringHeld(t, err, "standby")
	if got := strings.Join(applied, ","); got != "reader" || !slices.Equal(processed, []string{"reader"}) {
		t.Fatalf("applied = %q, processed = %v, want only the reader rolled", got, processed)
	}
}

// The active site's only standby is held: nothing is fenced or handed off.
func TestExecuteTargets_KeyringHeldStandbyKeepsTheActiveSite(t *testing.T) {
	uc := podGateController()
	active := &testutil.FakeMySQL{ReadOnlyVal: false}
	standby := directReplicaOf("primary-internal")
	processed, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: active, Drifted: true},
		[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: standby, Promotable: true, Drifted: true,
			ExpectedSource: "primary-internal",
			KeyringRollBlocked: func(context.Context) error {
				return &KeyringRollRefusedError{Site: "standby", Reason: "keys only its pod has"}
			}}},
		func(_ context.Context, site string) error { t.Errorf("rolled %s", site); return nil },
		func(site, _ string) { t.Errorf("authority moved to %s", site) })
	keyringHeld(t, err, "standby")
	if len(processed) != 0 || fenced(active) || standby.Promoted {
		t.Fatalf("processed = %v, active fenced = %v, standby promoted = %v; want nothing done", processed, fenced(active), standby.Promoted)
	}
}

// The active site itself is held: its standby is rolled, the active site is
// not fenced, handed off or rolled.
func TestExecuteTargets_KeyringHeldActiveSiteIsNotHandedOff(t *testing.T) {
	uc := podGateController()
	active := &testutil.FakeMySQL{ReadOnlyVal: false}
	standby := directReplicaOf("primary-internal")
	var applied []string
	processed, err := uc.ExecuteTargets(context.Background(),
		UpdateTarget{Name: "primary", Host: "primary-internal", Checker: active, Drifted: true,
			KeyringRollBlocked: func(context.Context) error {
				return &KeyringRollRefusedError{Site: "primary", Reason: "keys only its pod has"}
			}},
		[]UpdateTarget{{Name: "standby", Host: "standby-internal", Checker: standby, Promotable: true, Drifted: true,
			ExpectedSource: "primary-internal", KeyringRollBlocked: func(context.Context) error { return nil }}},
		func(_ context.Context, site string) error { applied = append(applied, site); return nil },
		func(site, _ string) { t.Errorf("authority moved to %s", site) })
	keyringHeld(t, err, "primary")
	if got := strings.Join(applied, ","); got != "standby" || !slices.Equal(processed, []string{"standby"}) {
		t.Fatalf("applied = %q, processed = %v, want the standby rolled", got, processed)
	}
	if fenced(active) || standby.Promoted {
		t.Fatalf("active fenced = %v, standby promoted = %v; want no handoff", fenced(active), standby.Promoted)
	}
}

// Recreate writes every site's Deployment from the reconcile loop: the same
// guard holds an unsealed site whose running keyring is not escrowed, the
// pass requeues, and the write goes ahead once the escrow is recorded.
func TestReconcile_RecreateHoldsAnUnsealedSiteUntilItsKeyringIsRecorded(t *testing.T) {
	ctx := context.Background()
	fg := encTestFG()
	fg.Spec.UpdateStrategy.Type = v1beta1.UpdateStrategyRecreate
	fg.Status.ActiveSite = "dc1"
	fg.Status.EncryptionAtRest = &v1beta1.EncryptionAtRestStatus{Sites: []v1beta1.SiteEncryptionStatus{
		{Name: "dc1", Phase: v1beta1.KeyringPhaseUnsealed, UnsealReason: v1beta1.UnsealReasonBootstrap},
		{Name: "dc2", Phase: v1beta1.KeyringPhaseUnsealed, UnsealReason: v1beta1.UnsealReasonBootstrap},
	}}
	r, c := newReconciler(fg)
	sim := newKeyringPodSim(t, c, fg)
	r.keyringStatus = sim.fetch
	reconcileOnce(t, r)
	sim.startPod("dc1")
	sim.startPod("dc2")

	// dc1's mysqld creates its keys; nothing is escrowed yet. An unrelated
	// spec change would roll both pods.
	keys := []byte(`{"version":"1.0","elements":[{"id":"dc1-master"}]}`)
	sim.setLive("dc1", keys, true)
	sim.setLive("dc2", []byte(emptyKeyringDocument+"\n"), false)
	fresh := freshGroup(t, c, fg)
	fresh.Spec.PodAnnotations = map[string]string{"example.com/roll": "1"}
	if err := c.Update(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 || res.RequeueAfter > encryptionRequeue {
		t.Errorf("RequeueAfter = %v, want at most %v while dc1 is held", res.RequeueAfter, encryptionRequeue)
	}
	if got := siteDeployment(t, c, fg, "dc1").Spec.Template.Annotations["example.com/roll"]; got != "" {
		t.Fatal("dc1 was rolled while its keys were only in its pod")
	}
	if got := siteDeployment(t, c, fg, "dc2").Spec.Template.Annotations["example.com/roll"]; got != "1" {
		t.Fatal("dc2, which stored no keys, was held as well")
	}

	// The sidecar escrows dc1's keys and the operator records them.
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc1", keys); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r)
	if s := siteStatus(freshGroup(t, c, fg), "dc1"); s.Phase != v1beta1.KeyringPhaseEscrowed {
		t.Fatalf("dc1 = %s (%s), want Escrowed", s.Phase, s.Message)
	}
	reconcileOnce(t, r)
	d := siteDeployment(t, c, fg, "dc1")
	if d.Spec.Template.Annotations["example.com/roll"] != "1" || podKeyringSecret(&d.Spec.Template.Spec) != "mysql-lion-dc1-keyring-v1" {
		t.Fatalf("dc1 not rolled onto its recorded escrow: annotations %v, keyring %q",
			d.Spec.Template.Annotations, podKeyringSecret(&d.Spec.Template.Spec))
	}
	sim.startPod("dc1")
	if got := sim.crashes(); got != "" {
		t.Fatal(got)
	}
}

// An Escrowed site whose pod still runs the writable keyring, and whose
// MySQL stored a key after the escrow (a restart that adds a binary log
// key), goes back to Unsealed: the escrow handler then takes the new
// keyring, and the seal roll projects that instead of the stale version.
func TestReconcileEncryption_EscrowedSiteWhoseKeyringChangedReEscrows(t *testing.T) {
	ctx := context.Background()
	fg := encTestFG()
	before, after := []byte("escrowed"), []byte("escrowed plus a binlog key")
	fg.Status.EncryptionAtRest = &v1beta1.EncryptionAtRestStatus{Sites: []v1beta1.SiteEncryptionStatus{
		{Name: "dc2", Phase: v1beta1.KeyringPhaseEscrowed, UnsealReason: v1beta1.UnsealReasonBootstrap,
			KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: keyringDigest(before)},
	}}
	r, c := encReconciler(t, scriptedKeyring(map[string]*internalmysql.KeyringStatus{
		"dc2": liveKeyring(keyringDigest(after), false),
	}), fg, unsealedDeployment(fg, "dc2"))
	store := &keyringEscrowStore{client: c, scheme: c.Scheme()}
	if _, err := store.put(ctx, fg, "dc2", before); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if s := siteStatus(fg, "dc2"); s.Phase != v1beta1.KeyringPhaseUnsealed || !strings.Contains(s.Message, "changed after it was escrowed") {
		t.Fatalf("dc2 = %s (%s), want Unsealed to re-escrow", s.Phase, s.Message)
	}
	if fg.SiteKeyringSealed("dc2") {
		t.Fatal("dc2 still renders sealed; the escrow handler would refuse the new keyring")
	}
	if _, err := store.put(ctx, fg, "dc2", after); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileEncryptionAtRest(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if s := siteStatus(fg, "dc2"); s.Phase != v1beta1.KeyringPhaseEscrowed || s.KeyringVersion != 2 || s.KeyringDigest != keyringDigest(after) {
		t.Fatalf("dc2 = %s v%d, want Escrowed v2 on the new keyring", s.Phase, s.KeyringVersion)
	}
}

// The runner gives every manager it starts the reconciler's keyring roll
// check, next to ApplyUpdate: without it the ordered update would only
// learn of a refusal from the Deployment write, as a failed update.
func TestStartManager_WiresTheKeyringRollCheck(t *testing.T) {
	r, c, fg, _ := lifecycleRunner(t)
	rec := &MysqlFailoverGroupReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(10)}
	r.SetDeploymentReconciler(rec)
	if err := r.startManager(context.Background(), fg, mustTopologyConfig(fg), nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	mt := registeredManager(r, FailoverGroupNamespacedName(fg))
	if mt == nil || mt.tm.ApplyUpdate == nil || mt.tm.KeyringRollRefusal == nil {
		t.Fatal("the manager has no keyring roll check wired with ApplyUpdate")
	}
	if err := mt.tm.KeyringRollRefusal(context.Background(), "dc1"); err != nil {
		t.Fatalf("check on a plaintext group = %v, want nil", err)
	}
}
