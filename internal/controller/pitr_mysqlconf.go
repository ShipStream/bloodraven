package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	pitrMysqlConfAcceptedText          = "spec.mysqlConf sets no option that breaks PITR replay"
	pitrMysqlConfRefusedMessagePreface = "not rendered while spec.backup.pitr.enabled is true (the archiver would otherwise set the permanent history-integrity marker for the prefix); remove the setting: "
)

// pitrUnsafeMySQLSetting returns why a normalized my.cnf option breaks
// PITR replay certification, or "": a replication or binlog filter,
// binlog_format other than ROW, or binlog_row_image other than FULL.
// replica_preserve_commit_order is operator-owned and always ON.
func pitrUnsafeMySQLSetting(key, value string) string {
	name := strings.TrimPrefix(key, "loose-")
	v := strings.ToUpper(strings.TrimSpace(value))
	switch {
	case strings.HasPrefix(name, "replicate-do-") || strings.HasPrefix(name, "replicate-ignore-") ||
		strings.HasPrefix(name, "replicate-wild-") || name == "replicate-rewrite-db" ||
		name == "binlog-do-db" || name == "binlog-ignore-db":
		return "a replication or binlog filter"
	case name == "binlog-format" && v != "ROW":
		return "PITR needs ROW"
	case name == "binlog-row-image" && v != "FULL":
		return "PITR needs FULL"
	}
	return ""
}

// pitrMySQLConfRefusal is one refused option of one site's effective
// configuration.
type pitrMySQLConfRefusal struct {
	key, value string
	// field is the spec path the effective value comes from.
	field string
	why   string
}

func (r pitrMySQLConfRefusal) String() string {
	setting := r.key
	if r.why != "a replication or binlog filter" {
		setting += "=" + r.value
	}
	return fmt.Sprintf("%s sets %s (%s)", r.field, setting, r.why)
}

// pitrRefusedMySQLConf returns the PITR-unsafe options of site's effective
// configuration (the group's spec.mysqlConf overlaid with the site's, as
// generateMyCnf merges them), sorted by key; nil without PITR. A group
// setting every site overrides with a safe value is not refused (F033),
// and a refusal names the field it comes from (F145).
func pitrRefusedMySQLConf(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) []pitrMySQLConfRefusal {
	if !pitrEnabled(fg) {
		return nil
	}
	type origin struct{ value, field string }
	effective := map[string]origin{}
	for k, v := range normalizedMySQLSettings(fg.Spec.MysqlConf) {
		effective[k] = origin{v, "spec.mysqlConf"}
	}
	for k, v := range normalizedMySQLSettings(site.MysqlConf) {
		effective[k] = origin{v, fmt.Sprintf("spec.sites[%s].mysqlConf", site.Name)}
	}
	keys := make([]string, 0, len(effective))
	for k := range effective {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []pitrMySQLConfRefusal
	for _, k := range keys {
		if why := pitrUnsafeMySQLSetting(k, effective[k].value); why != "" {
			out = append(out, pitrMySQLConfRefusal{key: k, value: effective[k].value, field: effective[k].field, why: why})
		}
	}
	return out
}

// pitrMySQLConfRefusalMessage lists every site's refusals, sorted by site,
// as "site: field sets key (...)", or "" when nothing is refused.
func pitrMySQLConfRefusalMessage(fg *v1alpha1.MysqlFailoverGroup) string {
	sites := append([]v1alpha1.SiteSpec(nil), fg.Spec.Sites...)
	sort.Slice(sites, func(i, j int) bool { return sites[i].Name < sites[j].Name })
	var parts []string
	for _, site := range sites {
		for _, ref := range pitrRefusedMySQLConf(fg, site) {
			parts = append(parts, fmt.Sprintf("%s: %s", site.Name, ref))
		}
	}
	return strings.Join(parts, "; ")
}

// reconcilePITRMySQLConfCondition sets PITRMysqlConfAccepted from the
// refusals generateMyCnf applies, emits a Warning PITRMysqlConfRefused when
// the refused set changes, and removes the condition without PITR.
func (r *MysqlFailoverGroupReconciler) reconcilePITRMySQLConfCondition(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	var cond *metav1.Condition
	if pitrEnabled(fg) {
		cond = &metav1.Condition{
			Type:               v1alpha1.ConditionPITRMysqlConfAccepted,
			Status:             metav1.ConditionTrue,
			Reason:             v1alpha1.ReasonAccepted,
			Message:            pitrMysqlConfAcceptedText,
			ObservedGeneration: fg.Generation,
			LastTransitionTime: metav1.Now(),
		}
		if refused := pitrMySQLConfRefusalMessage(fg); refused != "" {
			cond.Status = metav1.ConditionFalse
			cond.Reason = v1alpha1.ReasonPITRUnsafeSettingRefused
			cond.Message = pitrMysqlConfRefusedMessagePreface + refused
		}
	}
	var (
		previous *metav1.Condition
		written  *v1alpha1.MysqlFailoverGroup
	)
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		written = nil
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &fresh); err != nil {
			return err
		}
		before := fresh.Status.DeepCopy()
		previous = nil
		if c := apimeta.FindStatusCondition(before.Conditions, v1alpha1.ConditionPITRMysqlConfAccepted); c != nil {
			previous = c.DeepCopy()
		}
		if cond != nil {
			setCondition(&fresh.Status.Conditions, *cond)
		} else {
			removeCondition(&fresh.Status.Conditions, v1alpha1.ConditionPITRMysqlConfAccepted)
		}
		if equality.Semantic.DeepEqual(before, &fresh.Status) {
			return nil
		}
		if err := r.Status().Update(ctx, &fresh); err != nil {
			return err
		}
		written = &fresh
		return nil
	})
	if err != nil {
		return err
	}
	if written != nil && r.Recorder != nil && cond != nil && cond.Status == metav1.ConditionFalse &&
		(previous == nil || previous.Status != metav1.ConditionFalse || previous.Message != cond.Message) {
		r.Recorder.Event(written, corev1.EventTypeWarning, v1alpha1.EventPITRMysqlConfRefused, cond.Message)
	}
	return nil
}
