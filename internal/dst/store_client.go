package dst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// storeClient serves the simulated object to production code that talks to
// the API server through a controller-runtime client: the out-of-band
// failover-record recorder (NewAnnotationFailoverStateRecorder), whose
// read-compare-patch conditioned on the resourceVersion is exactly what the
// trial must exercise (§5.3, rows 22 and 26).
//
// Only Get and a metadata merge Patch (MergeFromWithOptimisticLock) are
// served; every other client method is nil and panics, so production code
// that starts using another one fails the trial loudly instead of reading an
// empty fake.
type storeClient struct {
	client.Client // nil: unmodeled methods panic

	store *simPlannedStore
	// denied reports the out-of-band store outage (OpStateOutage), which
	// rejects metadata patches of the record independently of /status.
	denied func() bool
}

var gr = schema.GroupResource{Group: "shipstream.io", Resource: "mysqlfailovergroups"}

func (c *storeClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	fg, ok := obj.(*v1alpha1.MysqlFailoverGroup)
	if !ok {
		return fmt.Errorf("dst: storeClient.Get of %T is not modeled", obj)
	}
	c.store.mu.Lock()
	cur, err := c.store.loadLocked()
	c.store.mu.Unlock()
	if err != nil {
		return err
	}
	if cur == nil {
		return apierrors.NewNotFound(gr, key.Name)
	}
	cur.DeepCopyInto(fg)
	return nil
}

// metadataPatch is the shape of a metadata-only JSON merge patch.
type metadataPatch struct {
	Metadata struct {
		Annotations     map[string]*string `json:"annotations"`
		ResourceVersion string             `json:"resourceVersion"`
	} `json:"metadata"`
}

func (c *storeClient) Patch(_ context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	if patch.Type() != "application/merge-patch+json" {
		return fmt.Errorf("dst: storeClient.Patch of type %s is not modeled", patch.Type())
	}
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	var mp metadataPatch
	if err := json.Unmarshal(data, &mp); err != nil {
		return err
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	for k := range probe {
		if k != "metadata" {
			return fmt.Errorf("dst: storeClient.Patch of %q is not modeled", k)
		}
	}
	if c.denied != nil && c.denied() {
		return apierrors.NewForbidden(gr, obj.GetName(), errors.New("sim: out-of-band store outage"))
	}
	s := c.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deadLocked() {
		return errOperatorDead
	}
	if s.obj == nil {
		return apierrors.NewNotFound(gr, obj.GetName())
	}
	if mp.Metadata.ResourceVersion != "" && parseRV(mp.Metadata.ResourceVersion) != s.rv {
		s.logLocked("recorderPatch", "conflict")
		return apierrors.NewConflict(gr, obj.GetName(), errStoreConflict)
	}
	kv := map[string]string{}
	for k, v := range mp.Metadata.Annotations {
		if v == nil {
			kv[k] = ""
		} else {
			kv[k] = *v
		}
	}
	s.patchAnnotationsLocked("failoverRecord", kv)
	s.logLocked("recorderPatch", "committed")
	s.obj.DeepCopyInto(obj.(*v1alpha1.MysqlFailoverGroup))
	return nil
}

// NewPlannedStoreClient returns a controller-runtime client over store for
// the production out-of-band recorder; denied (may be nil) rejects its
// patches as an out-of-band outage.
func NewPlannedStoreClient(store *PlannedStore, denied func() bool) client.Client {
	return &storeClient{store: store, denied: denied}
}
