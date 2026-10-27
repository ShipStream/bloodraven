package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;create;update;patch

// scheduleTriggerName names the per-group ServiceAccount, Role and
// RoleBinding that the backup and verification schedule CronJob pods run
// under.
func scheduleTriggerName(fgName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-schedule-trigger", fgName))
}

// scheduleTriggerNeeded reports whether the group renders any schedule
// CronJob, mirroring reconcileBackupSchedules and
// reconcileVerificationSchedules.
func scheduleTriggerNeeded(fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg.Spec.Backup == nil {
		return false
	}
	if len(fg.Spec.Backup.Schedules) > 0 {
		return true
	}
	for _, p := range fg.Spec.Backup.Profiles {
		if v := p.Verification; v != nil && v.Enabled && v.Schedule != "" {
			return true
		}
	}
	return false
}

// scheduleTriggerRules is all the trigger pods need: `trigger-backup`
// creates a MysqlBackup, and `trigger-verification` reads its own group
// and creates a MysqlBackupVerification. The operator holds each of these
// verbs itself, which RBAC requires before it can grant them.
func scheduleTriggerRules(fgName string) []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{
			APIGroups: []string{v1alpha1.GroupVersion.Group},
			Resources: []string{"mysqlbackups", "mysqlbackupverifications"},
			Verbs:     []string{"create"},
		},
		{
			APIGroups:     []string{v1alpha1.GroupVersion.Group},
			Resources:     []string{"mysqlfailovergroups"},
			ResourceNames: []string{fgName},
			Verbs:         []string{"get"},
		},
	}
}

// reconcileScheduleTriggerRBAC gives the schedule CronJob pods a
// ServiceAccount of their own in the group's namespace, bound to only
// what the trigger commands do. The pods used to run as the operator's
// ServiceAccount, which held the operator's cluster-wide token in the
// operator namespace and did not exist in any other. The objects are
// owned by the group, so they go when it does; a group that stops
// scheduling keeps them until then.
func (r *MysqlFailoverGroupReconciler) reconcileScheduleTriggerRBAC(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if !scheduleTriggerNeeded(fg) {
		return nil
	}
	name := scheduleTriggerName(fg.Name)
	objMeta := metav1.ObjectMeta{Name: name, Namespace: fg.Namespace}
	sa := &corev1.ServiceAccount{ObjectMeta: objMeta}
	role := &rbacv1.Role{ObjectMeta: objMeta}
	binding := &rbacv1.RoleBinding{ObjectMeta: objMeta}

	// Read through the API server rather than the manager cache, so the
	// operator does not watch every ServiceAccount, Role and RoleBinding
	// in the cluster for three objects per group.
	c := client.Client(r.Client)
	if r.APIReader != nil {
		c = uncachedGetClient{Client: r.Client, reader: r.APIReader}
	}
	labels := map[string]string{
		labelInstance:      fg.Name,
		labelFailoverGroup: fg.Name,
		labelManagedBy:     managerName,
	}
	mutate := func(obj client.Object, set func()) controllerutil.MutateFn {
		return func() error {
			obj.SetLabels(labels)
			set()
			return controllerutil.SetControllerReference(fg, obj, r.Scheme)
		}
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, sa, mutate(sa, func() {})); err != nil {
		return fmt.Errorf("reconcile schedule trigger serviceaccount: %w", err)
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, role, mutate(role, func() {
		role.Rules = scheduleTriggerRules(fg.Name)
	})); err != nil {
		return fmt.Errorf("reconcile schedule trigger role: %w", err)
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, c, binding, mutate(binding, func() {
		// RoleRef is immutable; it never changes for a given name.
		binding.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}
		binding.Subjects = []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: name, Namespace: fg.Namespace}}
	})); err != nil {
		return fmt.Errorf("reconcile schedule trigger rolebinding: %w", err)
	}
	return nil
}

// uncachedGetClient serves Get from an uncached reader and everything
// else from the wrapped client.
type uncachedGetClient struct {
	client.Client
	reader client.Reader
}

func (c uncachedGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reader.Get(ctx, key, obj, opts...)
}
