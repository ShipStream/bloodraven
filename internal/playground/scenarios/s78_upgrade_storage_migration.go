package scenarios

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/crdcompat"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario78UpgradeStorageMigration())
}

const (
	s78ID = "78-upgrade-storage-migration"

	// s78CommandEnv overrides the command that runs migrate-storage. It
	// runs through `bash -c` in the runner's working directory;
	// "--context <the runner's context>" and, for the first run,
	// "--dry-run" are appended.
	s78CommandEnv     = "BLOODRAVEN_MIGRATE_STORAGE_COMMAND"
	s78DefaultCommand = "go run ./cmd/kubectl-bloodraven migrate-storage"
)

// s78Object is what the scenario remembers of one object across the
// migration.
type s78Object struct {
	uid        types.UID
	generation int64
}

type s78Run struct {
	before map[string]s78Object
}

var (
	s78Mu   sync.Mutex
	s78Runs = map[*runner.Env]*s78Run{}
)

func s78State(env *runner.Env) *s78Run {
	s78Mu.Lock()
	defer s78Mu.Unlock()
	r, ok := s78Runs[env]
	if !ok {
		r = &s78Run{}
		s78Runs[env] = r
	}
	return r
}

// scenario78UpgradeStorageMigration is the second half of the upgrade E2E
// (C-8 in proposals/20-api-v1beta1.md). Scenario 74 upgraded a cluster that
// ran the previous release, whose CRDs store shipstream.io/v1alpha1. This
// build's CRDs store v1beta1, so every object from before the upgrade is
// still encoded as v1alpha1 and status.storedVersions lists both. The
// scenario runs `kubectl bloodraven migrate-storage` as a user would, with
// the operator running, and requires that:
//
//   - the upgraded operator's check at start passed on the objects the
//     previous release stored;
//   - a dry run reports and changes nothing;
//   - the real run leaves status.storedVersions == [v1beta1] on all five
//     CRDs, with one object of every kind, each the same object (uid) with
//     the same spec (generation) as before;
//   - a second run finds nothing to do;
//   - the group is healthy afterwards.
//
// 01-clean-primary-kill runs after it in the `upgrade` job: a failover
// still works on the migrated objects.
//
// Quarantined like 74: it needs the cluster 74 leaves.
func scenario78UpgradeStorageMigration() runner.Scenario {
	return runner.Scenario{
		ID:    s78ID,
		Title: "After the upgrade: migrate-storage moves every object to v1beta1",
		Hypothesis: "On a cluster upgraded from a release that stores shipstream.io/v1alpha1, with one object of every kind created before the upgrade, " +
			"`kubectl bloodraven migrate-storage` rewrites every object while the operator runs, sets status.storedVersions of the five CRDs to [v1beta1], " +
			"deletes and recreates nothing, changes no spec, and leaves the group healthy.",
		Risk:    "low",
		DocLink: "playground/chaos-scenarios.md#78-upgrade-storage-migration",
		Quarantine: "requires a cluster upgraded from the previous release by scenario 74 (./playground/upgrade-e2e.sh); " +
			"the e2e `upgrade` job runs it by name",
		Timeout:        15 * time.Minute,
		CleanupTimeout: time.Minute,
		Precheck:       s78Precheck,
		Steps: []runner.Step{
			{Phase: runner.PhaseVerify, Name: "the upgraded operator's check at start passed on the previous release's objects", Do: s78VerifyStartupCheck},
			{Phase: runner.PhaseInject, Name: "migrate-storage --dry-run reports and writes nothing", Do: s78DryRun},
			{Phase: runner.PhaseInject, Name: "migrate-storage rewrites every object beside the running operator", Do: s78Migrate},
			{Phase: runner.PhaseVerify, Name: "storedVersions == [v1beta1]; every object kept its uid and its spec", Do: s78VerifyMigrated},
			{Phase: runner.PhaseVerify, Name: "a second run finds nothing to do, and the group is healthy", Do: s78VerifyIdempotentAndHealthy},
		},
	}
}

func s78Dynamic(env *runner.Env) (dynamic.Interface, error) {
	return dynamic.NewForConfig(env.Kube.Config)
}

// s78StoredVersions reads status.storedVersions and the storage version of
// the five CRDs.
func s78StoredVersions(ctx context.Context, env *runner.Env) (stored map[string][]string, storage map[string]string, err error) {
	dyn, err := s78Dynamic(env)
	if err != nil {
		return nil, nil, err
	}
	crds := dyn.Resource(apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions"))
	stored, storage = map[string][]string{}, map[string]string{}
	for _, r := range crdcompat.Resources {
		u, err := crds.Get(ctx, r.CRDName(), metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("read CustomResourceDefinition %s: %w", r.CRDName(), err)
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, crd); err != nil {
			return nil, nil, err
		}
		stored[r.CRDName()] = crd.Status.StoredVersions
		storage[r.CRDName()] = crdcompat.StoredAs(crd)
	}
	return stored, storage, nil
}

// s78Objects reads every object of the five kinds, in every namespace.
func s78Objects(ctx context.Context, env *runner.Env) (map[string]s78Object, map[string]int, error) {
	dyn, err := s78Dynamic(env)
	if err != nil {
		return nil, nil, err
	}
	out, perKind := map[string]s78Object{}, map[string]int{}
	for _, r := range crdcompat.Resources {
		err := crdcompat.EachObject(ctx, crdcompat.DynamicLister(dyn), r, func(o *unstructured.Unstructured) error {
			out[r.Kind+" "+o.GetNamespace()+"/"+o.GetName()] = s78Object{uid: o.GetUID(), generation: o.GetGeneration()}
			perKind[r.Kind]++
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return out, perKind, nil
}

func s78Precheck(ctx context.Context, env *runner.Env) error {
	if err := AssertHealthyBaseline(ctx, env); err != nil {
		return err
	}
	stored, storage, err := s78StoredVersions(ctx, env)
	if err != nil {
		return fmt.Errorf("precheck: %w", err)
	}
	target := v1beta1.GroupVersion.Version
	for _, r := range crdcompat.Resources {
		name := r.CRDName()
		if storage[name] != target {
			return fmt.Errorf("precheck: %s stores objects as %s, not %s: this cluster does not run this build's CRDs (run scenario 74 first)", name, storage[name], target)
		}
		if strings.Join(stored[name], ",") == target {
			return fmt.Errorf("precheck: %s storedVersions is already [%s]: this cluster was not upgraded from a release that stores %s, or was migrated already (start from ./playground/upgrade-e2e.sh install)",
				name, target, v1beta1.DeprecatedGroupVersion.Version)
		}
	}
	_, perKind, err := s78Objects(ctx, env)
	if err != nil {
		return fmt.Errorf("precheck: %w", err)
	}
	for _, r := range crdcompat.Resources {
		if perKind[r.Kind] == 0 {
			return fmt.Errorf("precheck: no %s exists: ./playground/upgrade-e2e.sh install creates one object of every kind before the upgrade", r.Kind)
		}
	}
	return nil
}

func s78VerifyStartupCheck(ctx context.Context, env *runner.Env) error {
	// The check ran when the operator pod started, before this scenario
	// (the log tailers only hold lines from its start on): read the pod's
	// log from its beginning. The check is among its first lines.
	pod, err := env.Kube.FindPodWithLabel(ctx, env.Namespace, pgmetrics.OperatorPodSelector)
	if err != nil {
		return fmt.Errorf("find the operator pod: %w", err)
	}
	raw, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		return fmt.Errorf("read the log of %s: %w", pod.Name, err)
	}
	var passed string
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case strings.Contains(line, "CRD check: the operator must not start"), strings.Contains(line, "CRD check failed; starting anyway"):
			return fmt.Errorf("the operator's check at start refused something and the operator runs anyway: %s", line)
		case strings.Contains(line, "CRD check passed"):
			passed = line
		}
	}
	if passed == "" {
		return fmt.Errorf("the log of %s has no \"CRD check passed\" line: the check at start did not run on the upgraded operator", pod.Name)
	}
	env.Capture.Note("operator: " + passed)
	return nil
}

// s78Command runs migrate-storage and returns its output.
func s78Command(ctx context.Context, env *runner.Env, extra string) (string, error) {
	command := strings.TrimSpace(os.Getenv(s78CommandEnv))
	if command == "" {
		command = s78DefaultCommand
	}
	command += " --context " + env.Kube.CurrentCtx + extra
	env.Capture.Note("migrate-storage command: " + command)
	cmdCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var out bytes.Buffer
	cmd := exec.CommandContext(cmdCtx, "bash", "-c", command)
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	fmt.Fprint(os.Stderr, out.String())
	_ = env.Capture.WriteFile("migrate-storage"+strings.ReplaceAll(extra, " ", "")+".txt", out.Bytes())
	if err != nil {
		return out.String(), fmt.Errorf("%q: %w\n%s", command, err, out.String())
	}
	return out.String(), nil
}

func s78DryRun(ctx context.Context, env *runner.Env) error {
	run := s78State(env)
	before, perKind, err := s78Objects(ctx, env)
	if err != nil {
		return err
	}
	run.before = before
	env.Capture.Note(fmt.Sprintf("objects before the migration: %v", perKind))
	storedBefore, _, err := s78StoredVersions(ctx, env)
	if err != nil {
		return err
	}
	out, err := s78Command(ctx, env, " --dry-run")
	if err != nil {
		return err
	}
	if !strings.Contains(out, "all conform") || !strings.Contains(out, "Dry run:") {
		return fmt.Errorf("the dry run did not report that every object conforms and that it changed nothing:\n%s", out)
	}
	storedAfter, _, err := s78StoredVersions(ctx, env)
	if err != nil {
		return err
	}
	for name, versions := range storedBefore {
		if strings.Join(versions, ",") != strings.Join(storedAfter[name], ",") {
			return fmt.Errorf("the dry run changed storedVersions of %s from %v to %v", name, versions, storedAfter[name])
		}
	}
	return nil
}

func s78Migrate(ctx context.Context, env *runner.Env) error {
	started := time.Now()
	out, err := s78Command(ctx, env, "")
	if err != nil {
		return err
	}
	if !strings.Contains(out, "Done. Every Bloodraven object is stored as shipstream.io/v1beta1.") {
		return fmt.Errorf("migrate-storage did not report that it finished:\n%s", out)
	}
	env.Capture.Measure(fmt.Sprintf("migrate-storage took %s for %d objects", time.Since(started).Round(100*time.Millisecond), len(s78State(env).before)))
	return nil
}

func s78VerifyMigrated(ctx context.Context, env *runner.Env) error {
	run := s78State(env)
	stored, _, err := s78StoredVersions(ctx, env)
	if err != nil {
		return err
	}
	target := v1beta1.GroupVersion.Version
	for _, r := range crdcompat.Resources {
		if got := strings.Join(stored[r.CRDName()], ","); got != target {
			return fmt.Errorf("%s storedVersions = [%s] after the migration, want [%s]", r.CRDName(), got, target)
		}
	}
	after, perKind, err := s78Objects(ctx, env)
	if err != nil {
		return err
	}
	var problems []string
	for key, was := range run.before {
		now, ok := after[key]
		switch {
		case !ok:
			// A finished verification or backup may be pruned by
			// retention at any time; an object of each kind must remain.
			continue
		case now.uid != was.uid:
			problems = append(problems, fmt.Sprintf("%s was deleted and recreated (uid %s, was %s)", key, now.uid, was.uid))
		case now.generation != was.generation:
			problems = append(problems, fmt.Sprintf("%s: its spec changed (generation %d, was %d)", key, now.generation, was.generation))
		}
		// An unchanged resourceVersion is not a failure: the API server
		// skips a write that would store the bytes already stored, which
		// is the case for an object the upgraded operator wrote before the
		// migration (it was encoded as v1beta1 then). The envtest proves
		// the rewrite itself, by removing v1alpha1 from the CRDs.
	}
	for _, r := range crdcompat.Resources {
		if perKind[r.Kind] == 0 {
			problems = append(problems, fmt.Sprintf("no %s is left after the migration", r.Kind))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("after the migration: %s", strings.Join(problems, "; "))
	}
	env.Capture.Note(fmt.Sprintf("storedVersions == [%s] on all five CRDs; %d objects kept their uid and generation", target, len(run.before)))
	return nil
}

func s78VerifyIdempotentAndHealthy(ctx context.Context, env *runner.Env) error {
	out, err := s78Command(ctx, env, "")
	if err != nil {
		return err
	}
	if !strings.Contains(out, "Nothing to do") {
		return fmt.Errorf("a second run did not report that nothing is left to do:\n%s", out)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	_, err = env.Wait.UntilCR(waitCtx, env.Namespace, "the group is Ready with one writable site after the migration",
		func(mfg *v1beta1.MysqlFailoverGroup) (bool, string, error) {
			ready := findCondition(mfg.Status.Conditions, v1beta1.ConditionReady)
			if ready == nil || ready.Status != metav1.ConditionTrue {
				return false, "Ready is not True", nil
			}
			return mfg.Status.ActiveSite != "", fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite), nil
		})
	return err
}
