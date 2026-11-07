package controller

import (
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// A Deployment's and a StatefulSet's selector is immutable. When the live
// object's selector differs from the one the operator renders (an object
// left from a release with other label keys, or one created by hand under
// the operator's name), an update can only fail, and retrying it would be
// a hot loop. The operator refuses that site's workload instead: it does
// not update the object, records a group refusal
// (Degraded=True/WorkloadSelectorMismatch and a Warning Event naming the
// object) and leaves it until a human deletes the object, which the
// operator then re-creates.

// mysqlDeploymentSelector is the selector of a site's MySQL Deployment.
func mysqlDeploymentSelector(fg *v1alpha1.MysqlFailoverGroup, site string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{
		labelAppName:       "mysql",
		labelInstance:      fg.Name,
		v1alpha1.SiteLabel: site,
	}}
}

// dragonflyStatefulSetSelector is the selector of a site's Dragonfly
// StatefulSet.
func dragonflyStatefulSetSelector(fg *v1alpha1.MysqlFailoverGroup, site string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{
		labelAppName:       dragonflyAppName,
		labelInstance:      fg.Name,
		v1alpha1.SiteLabel: site,
	}}
}

// workloadSelectorMismatchError is a site workload whose live selector
// differs from the rendered one.
type workloadSelectorMismatchError struct {
	kind, name, site string
	have, want       *metav1.LabelSelector
}

func (e *workloadSelectorMismatchError) Error() string { return e.message() }

// message is the refusal message: the object, both selectors and the fix.
func (e *workloadSelectorMismatchError) message() string {
	return fmt.Sprintf("%s %s of site %s has selector %s, but the operator renders %s; the selector is immutable, so the %s is not updated. "+
		"Delete it (kubectl delete %s %s) and the operator re-creates it",
		e.kind, e.name, e.site, formatSelector(e.have), formatSelector(e.want), e.kind, strings.ToLower(e.kind), e.name)
}

// refusalKey is the group refusal key of the object.
func (e *workloadSelectorMismatchError) refusalKey() string {
	return workloadRefusalKey(e.kind, e.name)
}

// workloadRefusalPrefix prefixes every workload selector refusal key.
const workloadRefusalPrefix = "selector/"

func workloadRefusalKey(kind, name string) string {
	return workloadRefusalPrefix + kind + "/" + name
}

// checkWorkloadSelector returns the mismatch of a live object's selector
// against want, or nil when they agree. An object without a selector (one
// about to be created) never mismatches.
func checkWorkloadSelector(kind, name, site string, have, want *metav1.LabelSelector) *workloadSelectorMismatchError {
	if have == nil || equality.Semantic.DeepEqual(have, want) {
		return nil
	}
	return &workloadSelectorMismatchError{kind: kind, name: name, site: site, have: have.DeepCopy(), want: want}
}

// deploymentSelectorMismatch checks a site's live MySQL Deployment.
func deploymentSelectorMismatch(d *appsv1.Deployment, fg *v1alpha1.MysqlFailoverGroup, site string) *workloadSelectorMismatchError {
	if d == nil || d.ResourceVersion == "" {
		return nil
	}
	return checkWorkloadSelector("Deployment", d.Name, site, d.Spec.Selector, mysqlDeploymentSelector(fg, site))
}

// statefulSetSelectorMismatch checks a site's live Dragonfly StatefulSet.
func statefulSetSelectorMismatch(s *appsv1.StatefulSet, fg *v1alpha1.MysqlFailoverGroup, site string) *workloadSelectorMismatchError {
	if s == nil || s.ResourceVersion == "" {
		return nil
	}
	return checkWorkloadSelector("StatefulSet", s.Name, site, s.Spec.Selector, dragonflyStatefulSetSelector(fg, site))
}

// noteWorkloadSelector records the outcome of a selector check of the
// object kind/name: a mismatch becomes a group refusal (the runner emits
// its Event once and reports Degraded), and an agreeing or absent object
// clears it.
func (r *MysqlFailoverGroupReconciler) noteWorkloadSelector(fg *v1alpha1.MysqlFailoverGroup, kind, name string, mismatch *workloadSelectorMismatchError) {
	if mismatch != nil {
		r.Runner.SetGroupRefusal(fg, mismatch.refusalKey(), v1alpha1.ReasonWorkloadSelectorMismatch, mismatch.message())
		return
	}
	r.Runner.ClearGroupRefusal(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, workloadRefusalKey(kind, name))
}

// clearDragonflySelectorRefusals clears the StatefulSet refusals of fg,
// once Dragonfly is torn down.
func (r *MysqlFailoverGroupReconciler) clearDragonflySelectorRefusals(fg *v1alpha1.MysqlFailoverGroup) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	for _, k := range r.Runner.GroupRefusalKeys(nn, workloadRefusalKey("StatefulSet", "")) {
		r.Runner.ClearGroupRefusal(nn, k)
	}
}

// pruneRemovedSiteSelectorRefusals clears fg's workload refusals whose
// object belongs to no site in spec.sites (a site removed while its
// Deployment or Dragonfly StatefulSet was refused). The per-site checks
// only ever look at spec sites, so nothing else would clear them, and a
// standing refusal keeps Degraded=True and refuses deploy leases.
func (r *MysqlFailoverGroupReconciler) pruneRemovedSiteSelectorRefusals(fg *v1alpha1.MysqlFailoverGroup) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	keys := r.Runner.GroupRefusalKeys(nn, workloadRefusalPrefix)
	if len(keys) == 0 {
		return
	}
	current := make(map[string]struct{}, 2*len(fg.Spec.Sites))
	for _, site := range fg.Spec.Sites {
		current[workloadRefusalKey("Deployment", resourceName(fg.Name, site.Name))] = struct{}{}
		current[workloadRefusalKey("StatefulSet", dragonflyStatefulSetName(fg.Name, site.Name))] = struct{}{}
	}
	for _, k := range keys {
		if _, ok := current[k]; !ok {
			r.Runner.ClearGroupRefusal(nn, k)
		}
	}
}

// formatSelector renders a selector's match labels as k=v,k=v (sorted).
func formatSelector(s *metav1.LabelSelector) string {
	if s == nil {
		return "{}"
	}
	parts := make([]string, 0, len(s.MatchLabels))
	for k, v := range s.MatchLabels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	out := strings.Join(parts, ",")
	if len(s.MatchExpressions) > 0 {
		out += fmt.Sprintf(" with %d match expressions", len(s.MatchExpressions))
	}
	return "{" + out + "}"
}
