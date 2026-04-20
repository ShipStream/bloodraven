package controller

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// reconcileVerificationSchedules materializes one CronJob per profile
// whose spec.backup.profiles[].verification.enabled=true, and prunes
// CronJobs that belonged to profiles whose verification was removed or
// disabled. Mirrors reconcileBackupSchedules.
func (r *MysqlFailoverGroupReconciler) reconcileVerificationSchedules(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	desired := map[string]struct{}{}

	if fg.Spec.Backup != nil {
		for _, profile := range fg.Spec.Backup.Profiles {
			if profile.Verification == nil || !profile.Verification.Enabled {
				continue
			}
			if profile.Verification.Schedule == "" {
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, "VerificationScheduleInvalid",
					"profile %q has verification.enabled=true but verification.schedule is empty", profile.Name)
				continue
			}
			name := verificationScheduleCronJobName(fg.Name, profile.Name)
			desired[name] = struct{}{}
			if err := r.reconcileOneVerificationSchedule(ctx, fg, profile); err != nil {
				return fmt.Errorf("reconcile verification schedule for profile %s: %w", profile.Name, err)
			}
		}
	}

	// Prune orphan verification CronJobs.
	var existing batchv1.CronJobList
	if err := r.List(ctx, &existing,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{
			labelFailoverGroup: fg.Name,
			labelResourceKind:  verificationResourceKindCJ,
		},
	); err != nil {
		return fmt.Errorf("list verification cronjobs: %w", err)
	}
	for i := range existing.Items {
		cj := &existing.Items[i]
		if _, ok := desired[cj.Name]; ok {
			continue
		}
		if err := r.Delete(ctx, cj); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete orphan verification cronjob %s: %w", cj.Name, err)
		}
	}
	return nil
}

// reconcileOneVerificationSchedule creates or updates a single CronJob
// for a profile's verification block. The CronJob pod invokes
// `bloodraven trigger-verification` under the operator's ServiceAccount
// so it inherits the RBAC to create MysqlBackupVerification CRs.
func (r *MysqlFailoverGroupReconciler) reconcileOneVerificationSchedule(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, profile v1alpha1.BackupProfile) error {
	v := profile.Verification

	image := operatorImageFromEnv
	if image == "" {
		image = "bloodraven:latest"
	}
	sa := operatorServiceAccountFromEnv
	if sa == "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "VerificationScheduleServiceAccountMissing",
			"operator ServiceAccount not configured (set BLOODRAVEN_OPERATOR_SA env var on the operator deployment); falling back to %q for verification schedule %q",
			"bloodraven", profile.Name)
		sa = "bloodraven"
	}

	tz := v.TimeZone
	if tz == "" {
		tz = defaultScheduleTimeZone
	}

	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      verificationScheduleCronJobName(fg.Name, profile.Name),
			Namespace: fg.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cj, func() error {
		if err := controllerutil.SetControllerReference(fg, cj, r.Scheme); err != nil {
			return err
		}
		labels := map[string]string{
			labelAppName:              "mysql-verify",
			labelInstance:             fg.Name,
			labelFailoverGroup:        fg.Name,
			labelBackupProfile:        profile.Name,
			labelVerificationSchedule: profile.Name,
			labelResourceKind:         verificationResourceKindCJ,
			labelManagedBy:            managerName,
		}
		cj.Labels = labels

		concurrency := batchv1.ForbidConcurrent
		if v.ConcurrencyPolicy == "Replace" {
			concurrency = batchv1.ReplaceConcurrent
		}

		cj.Spec.Schedule = v.Schedule
		tzCopy := tz
		cj.Spec.TimeZone = &tzCopy
		cj.Spec.Suspend = boolPtr(v.Suspend)
		cj.Spec.ConcurrencyPolicy = concurrency
		cj.Spec.StartingDeadlineSeconds = v.StartingDeadlineSeconds
		cj.Spec.SuccessfulJobsHistoryLimit = v.SuccessfulJobsHistoryLimit
		cj.Spec.FailedJobsHistoryLimit = v.FailedJobsHistoryLimit

		backoff := int32(2)
		cj.Spec.JobTemplate = batchv1.JobTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: batchv1.JobSpec{
				BackoffLimit: &backoff,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						RestartPolicy:      corev1.RestartPolicyOnFailure,
						ServiceAccountName: sa,
						Containers: []corev1.Container{
							{
								Name:  "trigger",
								Image: image,
								Command: []string{
									"/bloodraven",
									"trigger-verification",
									"--group=" + fg.Name,
									"--profile=" + profile.Name,
									"--namespace=" + fg.Namespace,
								},
							},
						},
					},
				},
			},
		}
		return nil
	})
	return err
}
