package kube

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// MFGKey returns the controller-runtime key for the default playground
// MFG in the given namespace. Pass an empty namespace to default to the
// playground namespace.
func MFGKey(namespace string) client.ObjectKey {
	return MFGKeyForName(namespace, FailoverGroupName)
}

// MFGKeyForName returns the controller-runtime key for a named
// MysqlFailoverGroup. Empty namespace/name values default to the
// playground namespace and failover group name.
func MFGKeyForName(namespace, name string) client.ObjectKey {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	if name == "" {
		name = FailoverGroupName
	}
	return client.ObjectKey{Namespace: namespace, Name: name}
}

// GetMFG fetches the playground MysqlFailoverGroup. The returned
// object is a fresh deep copy — safe to read without locks.
func (c *Client) GetMFG(ctx context.Context, namespace string) (*v1alpha1.MysqlFailoverGroup, error) {
	return c.GetMFGNamed(ctx, namespace, FailoverGroupName)
}

// GetMFGNamed fetches the named MysqlFailoverGroup. The returned
// object is a fresh deep copy — safe to read without locks.
func (c *Client) GetMFGNamed(ctx context.Context, namespace, name string) (*v1alpha1.MysqlFailoverGroup, error) {
	mfg := &v1alpha1.MysqlFailoverGroup{}
	if err := c.Controller.Get(ctx, MFGKeyForName(namespace, name), mfg); err != nil {
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
	return c.PatchMFGNamed(ctx, namespace, FailoverGroupName, ops)
}

// PatchMFGNamed applies a JSON Patch to the named MysqlFailoverGroup.
func (c *Client) PatchMFGNamed(ctx context.Context, namespace, name string, ops []JSONPatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	body, err := json.Marshal(ops)
	if err != nil {
		return fmt.Errorf("marshal JSON Patch: %w", err)
	}
	mfg := &v1alpha1.MysqlFailoverGroup{}
	key := MFGKeyForName(namespace, name)
	mfg.Namespace = key.Namespace
	mfg.Name = key.Name
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
	return c.AnnotateMFGNamed(ctx, namespace, FailoverGroupName, key, value)
}

// AnnotateMFGNamed sets a single annotation key/value on the named MFG.
func (c *Client) AnnotateMFGNamed(ctx context.Context, namespace, name, key, value string) error {
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
	objKey := MFGKeyForName(namespace, name)
	mfg.Namespace = objKey.Namespace
	mfg.Name = objKey.Name
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
