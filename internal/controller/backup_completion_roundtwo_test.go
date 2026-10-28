package controller

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Model the kubelet's single-pass substitutions independently of recovery's parser.
func metadataRoundTripRuntimeEnv(container corev1.Container) map[string]string {
	values := map[string]string{}
	tokens := regexp.MustCompile(`\$\$|\$\([^)]*\)`)
	for _, env := range container.Env {
		if env.ValueFrom != nil {
			continue
		}
		values[env.Name] = tokens.ReplaceAllStringFunc(env.Value, func(token string) string {
			if token == "$$" {
				return "$"
			}
			if value, ok := values[token[2:len(token)-1]]; ok {
				return value
			}
			return token
		})
	}
	return values
}

func TestBackupMetadataReview_LiteralDestinationRoundTrip(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, storage := range []v1alpha1.BackupStorageType{v1alpha1.BackupStorageS3, v1alpha1.BackupStoragePVC} {
		for _, encrypted := range []bool{false, true} {
			for _, prefix := range []struct{ template, literal string }{
				{"$$(BLOODRAVEN_STORAGE_TYPE)", "$(BLOODRAVEN_STORAGE_TYPE)"},
				{"$$$$cash/$$(BLOODRAVEN_DATA_DIR)/$$(NOT_DECLARED)/$$", "$$cash/$(BLOODRAVEN_DATA_DIR)/$(NOT_DECLARED)/$"},
				{"ordinary", "ordinary"},
			} {
				t.Run(fmt.Sprintf("%s/encrypted-%v/%s", storage, encrypted, prefix.template), func(t *testing.T) {
					ctx := context.Background()
					fg := fgWithBackup()
					if encrypted {
						fg = fgWithEncryptedBackup()
					}
					if storage == v1alpha1.BackupStoragePVC {
						fg.Spec.Backup.Profiles[0].Storage = v1alpha1.BackupStorage{Type: storage, PVC: &v1alpha1.PVCStorage{ClaimName: "backups", SubPath: prefix.template}}
					} else {
						fg.Spec.Backup.Profiles[0].Storage.S3.Prefix = prefix.template
					}
					fg.Status.ActiveSite = "iad"
					fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
					mb := backupCR("metadata", fg.Name, "nightly-s3")
					mb.UID = "literal-backup"
					r, c := newBackupReconciler(t, fg, mb, dsnSecret())
					reconcileUntilStable(t, r, mb.Name)
					var producer batchv1.Job
					if err := c.Get(ctx, client.ObjectKey{Namespace: mb.Namespace, Name: backupJobName(mb.Name)}, &producer); err != nil {
						t.Fatal(err)
					}
					key := prefix.literal + "/metadata"
					wantLocation := key
					if storage == v1alpha1.BackupStoragePVC {
						wantLocation = "/backups/" + key
					} else if encrypted {
						wantLocation = "s3://bloodraven-backups/" + key + "/"
					}
					wantOutput := key
					if storage == v1alpha1.BackupStoragePVC {
						wantOutput = wantLocation
					}
					if actual := metadataRoundTripRuntimeEnv(*backupJobMainContainer(&producer))["BLOODRAVEN_OUTPUT_URL"]; actual != wantOutput {
						t.Fatalf("producer runtime path=%q want %q", actual, wantOutput)
					}
					producer.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
					if err := c.Status().Update(ctx, &producer); err != nil {
						t.Fatal(err)
					}
					// No log client: recover the actual path solely from the producing Job.
					if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
						t.Fatal(err)
					}
					if err := c.Get(ctx, client.ObjectKeyFromObject(mb), mb); err != nil {
						t.Fatal(err)
					}
					if mb.Status.Location != wantLocation || mb.Status.Phase != v1alpha1.BackupPhaseSucceeded {
						t.Fatalf("recovery changed actual artifact identity: %+v want %q", mb.Status, wantLocation)
					}
					fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: mb.Name}}}
					rr, _ := newReconciler(fg, mb)
					restore, err := rr.buildRestoreJob(ctx, fg, "iad", "creds")
					if err != nil {
						t.Fatal(err)
					}
					v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "verify-literal", Namespace: mb.Namespace, UID: "literal-verification"}}
					verify, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Backup: mb, Verification: v, CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
					if err != nil {
						t.Fatal(err)
					}
					if err := controllerutil.SetControllerReference(v, verify, r.Scheme); err != nil {
						t.Fatal(err)
					}
					for name, job := range map[string]*batchv1.Job{"restore": restore, "verification": verify} {
						container := job.Spec.Template.Spec.Containers[0]
						variable, want := "BLOODRAVEN_INPUT_URL", wantLocation
						if storage == v1alpha1.BackupStoragePVC {
							want += "/"
						}
						if encrypted {
							variable, want = "BLOODRAVEN_SOURCE_PREFIX", key
							for _, init := range job.Spec.Template.Spec.InitContainers {
								if init.Name == "decrypt-download" {
									container = init
									break
								}
							}
						}
						if actual := metadataRoundTripRuntimeEnv(container)[variable]; actual != want {
							t.Errorf("%s runtime source=%q want literal %q", name, actual, want)
						}
					}
					if _, err := verificationJobSource(v, verify); err != nil {
						t.Errorf("verification source snapshot rejected the literal runtime source: %v", err)
					}
					cleanup, err := buildCleanupJob(cleanupJobInputs{FailoverGroup: fg, Profile: &fg.Spec.Backup.Profiles[0], Backup: mb, CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
					if err != nil {
						t.Fatal(err)
					}
					if actual := metadataRoundTripRuntimeEnv(cleanup.Spec.Template.Spec.Containers[0])["BLOODRAVEN_OUTPUT_URL"]; actual != wantLocation {
						t.Errorf("cleanup runtime path=%q want literal %q", actual, wantLocation)
					}
					if encrypted && prefix.template == "$$(BLOODRAVEN_STORAGE_TYPE)" {
						// A pre-fix Job can carry a matching snapshot yet read a different path.
						for i := range verify.Spec.Template.Spec.InitContainers {
							for j := range verify.Spec.Template.Spec.InitContainers[i].Env {
								env := &verify.Spec.Template.Spec.InitContainers[i].Env[j]
								if env.Name == "BLOODRAVEN_SOURCE_PREFIX" {
									env.Value = key
								}
							}
						}
						if _, err := verificationJobSource(v, verify); err == nil {
							t.Error("verification accepted a snapshot whose runtime source expands to a different path")
						}
					}
				})
			}
		}
	}
}

func TestBackupMetadataReview_LegacyProducerLocationPreserved(t *testing.T) {
	for _, reason := range []string{"", "Collected", "SentinelUnavailable", "SentinelMissing", "LocationMismatch"} {
		t.Run("reason-"+reason, func(t *testing.T) {
			ctx := context.Background()
			r, c, _, mb, job := backupMetadataFixture(t, false)
			const literal = "$(NOT_DECLARED)/metadata"
			for i := range job.Spec.Template.Spec.Containers[0].Env {
				if job.Spec.Template.Spec.Containers[0].Env[i].Name == "BLOODRAVEN_OUTPUT_URL" {
					job.Spec.Template.Spec.Containers[0].Env[i].Value = literal
				}
			}
			if err := c.Update(ctx, job); err != nil {
				t.Fatal(err)
			}
			mb.Status.Phase, mb.Status.Location = v1alpha1.BackupPhaseSucceeded, literal
			mb.Status.CompletionTime = job.Status.CompletionTime
			mb.Status.Conditions = []metav1.Condition{{Type: ConditionBackupReady, Status: metav1.ConditionTrue, Reason: "Succeeded", Message: "original producer reported success", LastTransitionTime: metav1.Now()}}
			if reason != "" {
				status := metav1.ConditionFalse
				if reason == "Collected" {
					status = metav1.ConditionTrue
				}
				mb.Status.Conditions = append(mb.Status.Conditions, metav1.Condition{Type: ConditionBackupMetadataComplete, Status: status, Reason: reason, Message: "previous collection", LastTransitionTime: metav1.Now()})
			}
			if err := c.Status().Update(ctx, mb); err != nil {
				t.Fatal(err)
			}
			before := *mb.Status.DeepCopy()
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(mb), mb); err != nil {
				t.Fatal(err)
			}
			if reason == "" || reason == "Collected" {
				if mb.Status.Location != literal || !reflect.DeepEqual(mb.Status.Conditions, before.Conditions) {
					t.Fatalf("producer-reported legacy destination was altered: %+v", mb.Status)
				}
				// Exercise the collector directly as well as the repair predicate.
				for i := 0; i < 2; i++ {
					if _, err := r.collectBackupCompletion(ctx, mb, job, &before); err != nil || before.Location != literal || !reflect.DeepEqual(before.Conditions, mb.Status.Conditions) {
						t.Fatalf("collector erased producer location/provenance: status=%+v err=%v", before, err)
					}
				}
			} else if mb.Status.Location != "" {
				t.Fatalf("fallback-written unknown template was not invalidated: %+v", mb.Status)
			}
		})
	}
}

func TestBackupMetadataReview_CleanupWarningWithoutJob(t *testing.T) {
	ctx := context.Background()
	r, c, _, mb, job := backupMetadataFixture(t, false)
	if err := c.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}
	mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
	mb.Status.Location = ""
	if err := c.Status().Update(ctx, mb); err != nil {
		t.Fatal(err)
	}
	recorder := record.NewFakeRecorder(20)
	r.Recorder = recorder
	if done, err := r.finalize(ctx, mb); err != nil || !done {
		t.Fatalf("missing producer blocked deletion: done=%v err=%v", done, err)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "ArtifactCleanupSkipped no recorded artifact destination") || !strings.Contains(event, job.Name) {
			t.Fatalf("cleanup warning misattributes the missing destination: %s", event)
		}
	default:
		t.Fatal("missing cleanup warning")
	}
}
