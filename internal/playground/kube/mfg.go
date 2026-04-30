package kube

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// MFGKey returns the controller-runtime key for the playground MFG
// in the given namespace. Pass an empty namespace to default to the
// playground namespace.
func MFGKey(namespace string) client.ObjectKey {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	return client.ObjectKey{Namespace: namespace, Name: FailoverGroupName}
}

// GetMFG fetches the playground MysqlFailoverGroup. The returned
// object is a fresh deep copy — safe to read without locks.
func (c *Client) GetMFG(ctx context.Context, namespace string) (*v1alpha1.MysqlFailoverGroup, error) {
	mfg := &v1alpha1.MysqlFailoverGroup{}
	if err := c.Controller.Get(ctx, MFGKey(namespace), mfg); err != nil {
		return nil, fmt.Errorf("get MysqlFailoverGroup: %w", err)
	}
	return mfg, nil
}

// JSONPatchOp is one operation in an RFC 6902 JSON Patch.
type JSONPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// PatchMFG applies a JSON Patch to the MysqlFailoverGroup. JSON Patch
// is the only patch type accepted here on purpose: merge and
// strategic-merge patches drop required fields on this CRD (the
// well-documented mfg patch trap), and accidentally calling them via
// kubectl is a reliable way to turn a single chaos action into a
// reconciliation storm.
func (c *Client) PatchMFG(ctx context.Context, namespace string, ops []JSONPatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	body, err := json.Marshal(ops)
	if err != nil {
		return fmt.Errorf("marshal JSON Patch: %w", err)
	}
	mfg := &v1alpha1.MysqlFailoverGroup{}
	mfg.Namespace = namespace
	if mfg.Namespace == "" {
		mfg.Namespace = PlaygroundNamespace
	}
	mfg.Name = FailoverGroupName
	patch := client.RawPatch(types.JSONPatchType, body)
	if err := c.Controller.Patch(ctx, mfg, patch); err != nil {
		return fmt.Errorf("patch MysqlFailoverGroup: %w", err)
	}
	return nil
}

// AnnotateMFG sets a single annotation key/value on the MFG via
// strategic-merge over metadata only (the mfg patch trap concerns
// spec.sites; metadata annotations are safe to merge-patch). Use
// empty value to delete the annotation key.
func (c *Client) AnnotateMFG(ctx context.Context, namespace, key, value string) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				key: nil,
			},
		},
	}
	if value != "" {
		patch["metadata"].(map[string]any)["annotations"].(map[string]any)[key] = value
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal annotation patch: %w", err)
	}
	mfg := &v1alpha1.MysqlFailoverGroup{}
	mfg.Namespace = namespace
	mfg.Name = FailoverGroupName
	rp := client.RawPatch(types.MergePatchType, body)
	if err := c.Controller.Patch(ctx, mfg, rp); err != nil {
		return fmt.Errorf("annotate MysqlFailoverGroup: %w", err)
	}
	return nil
}

// SiteState returns the observed state for the named site, or "" if
// the site is unknown to the status.
func SiteState(mfg *v1alpha1.MysqlFailoverGroup, site string) string {
	for _, s := range mfg.Status.Sites {
		if s.Name == site {
			return s.State
		}
	}
	return ""
}

// ReadyCondition returns the Ready condition status from the MFG, or
// "Unknown" if no Ready condition has been written yet.
func ReadyCondition(mfg *v1alpha1.MysqlFailoverGroup) string {
	for _, c := range mfg.Status.Conditions {
		if c.Type == "Ready" {
			return string(c.Status)
		}
	}
	return "Unknown"
}
