package controller

import (
	"bufio"
	"context"
	"io"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func pitrFailureReason(message string) string {
	first := len(message)
	primary := ""
	for _, reason := range []string{"PITRBackupBoundUnknown", "PITRCrossSiteOrder", "PITRArchiveSiteOmitted", "PITRTargetNotCovered", "PITRArchiveListFailed", "PITRNoArchivedReplay", "PITRCoverageInvalid"} {
		if index := strings.Index(message, reason); index >= 0 && index < first {
			first = index
			primary = reason
		}
	}
	return primary
}

// Init-container failures precede main-container logs. Prefer termination
// messages, with a log-tail fallback for older Jobs and truncated pod status.
func pitrJobFailure(ctx context.Context, c client.Client, cs kubernetes.Interface, job *batchv1.Job) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	pods, err := restoreJobPods(ctx, c, job)
	if err != nil {
		return "", false
	}
	var chosen *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if podBelongsToJob(p, job) && (chosen == nil || p.CreationTimestamp.After(chosen.CreationTimestamp.Time)) {
			chosen = p
		}
	}
	if chosen == nil {
		return "", true
	}
	statuses := append(append([]corev1.ContainerStatus(nil), chosen.Status.InitContainerStatuses...), chosen.Status.ContainerStatuses...)
	for _, status := range statuses {
		if term := status.State.Terminated; term != nil && term.ExitCode != 0 && strings.TrimSpace(term.Message) != "" {
			return term.Message, true
		}
	}
	if cs == nil {
		return "", false
	}
	resolved := false
	fallback := ""
	for _, container := range []string{restorePITRInitContainerName, backupJobContainerName} {
		stream, err := cs.CoreV1().Pods(job.Namespace).GetLogs(chosen.Name, &corev1.PodLogOptions{Container: container, TailLines: ptr64(50)}).Stream(ctx)
		if err != nil {
			continue
		}
		last := ""
		generic := ""
		sc := bufio.NewScanner(io.LimitReader(stream, 64*1024))
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) != "" {
				generic = sc.Text()
			}
			if pitrFailureReason(sc.Text()) != "" {
				last = sc.Text()
			}
		}
		_ = stream.Close()
		if sc.Err() != nil {
			continue
		}
		resolved = true
		if last != "" {
			return last, true
		}
		if generic != "" {
			fallback = generic
		}
	}
	return fallback, resolved
}

type completionLogState int

const (
	completionLogsTransient completionLogState = iota
	completionLogsRead
	completionLogsNoPod
)

func completionLogsExpired(job *batchv1.Job, now time.Time) bool {
	deadline, known := completionLogDeadline(job)
	return !known || !now.Before(deadline)
}

func completionLogDeadline(job *batchv1.Job) (time.Time, bool) {
	completed := job.Status.CompletionTime
	if completed == nil || completed.IsZero() {
		completed = stableJobCompletionTime(job)
	}
	if completed != nil && !completed.IsZero() {
		return completed.Time.Add(10 * time.Minute), true
	}
	if !job.CreationTimestamp.IsZero() {
		return job.CreationTimestamp.Time.Add(10 * time.Minute), true
	}
	return time.Time{}, false
}
func jobHasPITR(job *batchv1.Job) bool {
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.Name == restorePITRInitContainerName {
			return true
		}
	}
	return false
}
