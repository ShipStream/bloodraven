package scenarios

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// The Bootstrapping condition is True only while a clone runs.
// False/NotBootstrapping is both "never cloned" and "the clone finished",
// and False/BootstrapFailed is a failed clone (Degraded=True/BootstrapFailed
// carries it too). So a scenario cannot wait for a token to know a clone it
// asked for finished: it waits for False/NotBootstrapping whose
// lastTransitionTime is not before the request (or the observed start) and
// for the target site to replicate.

// bootstrapRunning reports a clone in progress.
func bootstrapRunning(c *metav1.Condition) bool {
	return c != nil && c.Status == metav1.ConditionTrue
}

// bootstrapFailed reports that the last clone failed.
func bootstrapFailed(c *metav1.Condition) bool {
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == v1alpha1.ReasonBootstrapFailed
}

// bootstrapCompletedSince reports that a clone into target finished at or
// after since: Bootstrapping is False/NotBootstrapping, its last
// transition is not before since (condition times have second precision),
// and target replicates (Healthy or Lagging).
func bootstrapCompletedSince(mfg *v1alpha1.MysqlFailoverGroup, since time.Time, target string) (bool, string) {
	c := findCondition(mfg.Status.Conditions, v1alpha1.ConditionBootstrapping)
	if c == nil {
		return false, "no Bootstrapping condition yet"
	}
	msg := fmt.Sprintf("status=%s reason=%s lastTransitionTime=%s message=%q",
		c.Status, c.Reason, c.LastTransitionTime.UTC().Format(time.RFC3339), c.Message)
	if c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonNotBootstrapping {
		return false, msg
	}
	if c.LastTransitionTime.Time.Before(since.Truncate(time.Second)) {
		return false, msg + " (transition predates the request)"
	}
	for _, s := range mfg.Status.Sites {
		if s.Name == target {
			if !s.Replication.Running() {
				return false, fmt.Sprintf("%s; %s replication=%s", msg, target, s.Replication)
			}
			return true, msg
		}
	}
	return false, fmt.Sprintf("%s; %s not in status.sites", msg, target)
}

// stashTime and fetchTime carry an instant between a scenario's steps.
func stashTime(env *runner.Env, key string, t time.Time) {
	stashSet(env, key, t.UTC().Format(time.RFC3339Nano))
}

func fetchTime(env *runner.Env, key string) (time.Time, error) {
	raw := stashGet(env, key)
	if raw == "" {
		return time.Time{}, fmt.Errorf("no %s recorded by an earlier step", key)
	}
	return time.Parse(time.RFC3339Nano, raw)
}
