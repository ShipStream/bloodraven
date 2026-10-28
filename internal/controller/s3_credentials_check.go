package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// conditionS3CredentialsReady reports whether every S3 credentialsSecret the
// group references holds the keys backup, restore and cleanup Jobs require.
// Those Jobs fail rather than fall back to ambient (IRSA, Workload Identity,
// instance-profile) credentials, so a Secret that relied on the fallback
// would otherwise surface only when the next scheduled Job fails.
const conditionS3CredentialsReady = "S3CredentialsReady"

// requiredS3CredentialKeys are the Secret keys the Job scripts and the
// sidecar's PITR store refuse to run without.
var requiredS3CredentialKeys = []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}

type s3CredentialRef struct {
	field  string
	secret string
}

// s3CredentialRefs lists the S3 credentialsSecret references on fg's spec.
func s3CredentialRefs(fg *v1alpha1.MysqlFailoverGroup) []s3CredentialRef {
	var refs []s3CredentialRef
	if fg.Spec.Backup != nil {
		for _, p := range fg.Spec.Backup.Profiles {
			if s3 := p.Storage.S3; s3 != nil {
				refs = append(refs, s3CredentialRef{
					field:  fmt.Sprintf("spec.backup.profiles[%s].storage.s3.credentialsSecret", p.Name),
					secret: s3.CredentialsSecret,
				})
			}
		}
	}
	if in := fg.Spec.InitFromBackup; in != nil && in.Source.S3 != nil {
		refs = append(refs, s3CredentialRef{field: "spec.initFromBackup.source.s3.credentialsSecret", secret: in.Source.S3.CredentialsSecret})
	}
	if in := fg.Spec.RestoreInPlace; in != nil && in.Source.S3 != nil {
		refs = append(refs, s3CredentialRef{field: "spec.restoreInPlace.source.s3.credentialsSecret", secret: in.Source.S3.CredentialsSecret})
	}
	return refs
}

// s3CredentialProblems checks each referenced Secret and returns one
// message per Secret that a Job would reject. A read error other than
// NotFound is returned as an error, not a problem.
func (r *MysqlFailoverGroupReconciler) s3CredentialProblems(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) ([]string, error) {
	var problems []string
	for _, ref := range s3CredentialRefs(fg) {
		var s corev1.Secret
		err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: ref.secret}, &s)
		if apierrors.IsNotFound(err) {
			problems = append(problems, fmt.Sprintf("%s: Secret %q not found", ref.field, ref.secret))
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get S3 credentials secret %s: %w", ref.secret, err)
		}
		var missing []string
		for _, key := range requiredS3CredentialKeys {
			// The Job scripts strip the mounted value, so whitespace
			// alone counts as missing.
			if strings.TrimSpace(string(s.Data[key])) == "" {
				missing = append(missing, key)
			}
		}
		if len(missing) > 0 {
			problems = append(problems, fmt.Sprintf("%s: Secret %q has no non-empty %s",
				ref.field, ref.secret, strings.Join(missing, " or ")))
		}
	}
	sort.Strings(problems)
	return problems, nil
}

// reconcileS3CredentialsCondition reports incomplete S3 credentials Secrets
// as a Warning event and the S3CredentialsReady condition, and drops the
// condition when the group references no S3 storage.
func (r *MysqlFailoverGroupReconciler) reconcileS3CredentialsCondition(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	var cond *metav1.Condition
	if len(s3CredentialRefs(fg)) > 0 {
		problems, err := r.s3CredentialProblems(ctx, fg)
		if err != nil {
			return err
		}
		cond = &metav1.Condition{
			Type:               conditionS3CredentialsReady,
			Status:             metav1.ConditionTrue,
			Reason:             "CredentialsComplete",
			Message:            "every S3 credentialsSecret has AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
			ObservedGeneration: fg.Generation,
			LastTransitionTime: metav1.Now(),
		}
		if len(problems) > 0 {
			cond.Status = metav1.ConditionFalse
			cond.Reason = "CredentialsIncomplete"
			cond.Message = "backup, restore and cleanup Jobs will fail; they do not fall back to ambient credentials: " +
				strings.Join(problems, "; ")
			if r.Recorder != nil {
				r.Recorder.Event(fg, corev1.EventTypeWarning, "S3CredentialsIncomplete", cond.Message)
			}
		}
	}

	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &fresh); err != nil {
			return err
		}
		before := fresh.Status.DeepCopy()
		if cond != nil {
			setCondition(&fresh.Status.Conditions, *cond)
		} else {
			removeCondition(&fresh.Status.Conditions, conditionS3CredentialsReady)
		}
		if equality.Semantic.DeepEqual(before, &fresh.Status) {
			return nil
		}
		return r.Status().Update(ctx, &fresh)
	})
}
