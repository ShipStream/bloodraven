package kube

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// SiteDeploymentProblems lists the sites of mfg whose MySQL Deployment
// does not yet run the group's spec: a mysql container whose resources
// differ from spec.sites[].resources (the operator has not written the
// change yet, for example while it defers an ordered update), or a
// rollout the Deployment controller has not finished. An empty result
// means every site runs its spec's resources on a finished rollout.
//
// The operator only writes a changed pod template when its ordered update
// reaches the site, and it does not publish drift it has not acted on, so
// status.updatePhase alone reads empty between a spec change and the start
// of the update. A scenario that changed the spec and a cleanup that
// reverted it must wait for this instead (#236: a reverted memory patch
// rolled the primary under the next scenario).
func (c *Client) SiteDeploymentProblems(ctx context.Context, namespace string, mfg *v1alpha1.MysqlFailoverGroup) []string {
	var problems []string
	for i := range mfg.Spec.Sites {
		site := &mfg.Spec.Sites[i]
		dep, err := c.GetDeployment(ctx, namespace, MysqlDeploymentName(mfg.Name, site.Name))
		if apierrors.IsNotFound(err) {
			// Not created yet: the site has no status or no pod, which
			// the callers' status checks report.
			continue
		}
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s=deployment-unreadable(%v)", site.Name, err))
			continue
		}
		if p := SiteDeploymentProblem(site, dep); p != "" {
			problems = append(problems, site.Name+"="+p)
		}
	}
	return problems
}

// SiteDeploymentProblem is SiteDeploymentProblems for one site, or "".
func SiteDeploymentProblem(site *v1alpha1.SiteSpec, dep *appsv1.Deployment) string {
	found := false
	for _, ctr := range dep.Spec.Template.Spec.Containers {
		if ctr.Name != "mysql" {
			continue
		}
		found = true
		if !equality.Semantic.DeepEqual(ctr.Resources, site.Resources) {
			return "resources-not-applied"
		}
	}
	if !found {
		return "no-mysql-container"
	}
	want := int32(1)
	if dep.Spec.Replicas != nil {
		want = *dep.Spec.Replicas
	}
	st := dep.Status
	switch {
	case st.ObservedGeneration < dep.Generation:
		return "rollout-not-observed"
	case st.UpdatedReplicas != want || st.Replicas != want:
		return fmt.Sprintf("rolling(updated=%d replicas=%d want=%d)", st.UpdatedReplicas, st.Replicas, want)
	case st.AvailableReplicas != want:
		return fmt.Sprintf("unavailable(available=%d want=%d)", st.AvailableReplicas, want)
	}
	return ""
}
