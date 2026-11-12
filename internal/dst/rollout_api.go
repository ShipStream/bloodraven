package dst

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// readCache serves reads of the simulated API server from decoded copies.
//
// controller-runtime's fake client answers every Get and List by encoding
// the stored objects to JSON and decoding them again, and the operator's
// loops read the same unchanged objects on every pass (the pacer alone
// lists every group and reads each group's Deployments and Secrets). A
// rollout trial spent most of its time in that codec. The cache keeps what
// a read returned, per namespace, until anything in that namespace is
// written; a hit is a deep copy. What a caller gets is what the fake
// client would have returned, resource version included.
type readCache struct {
	c client.WithWatch

	mu sync.Mutex
	// gen counts the writes per namespace ("" for cluster-scoped objects);
	// all counts every write, for lists across namespaces.
	gen   map[string]uint64
	all   uint64
	gets  map[cachedGetKey]cachedRead
	lists map[string]cachedRead
}

type cachedGetKey struct {
	kind reflect.Type
	key  client.ObjectKey
}

type cachedRead struct {
	gen uint64
	obj runtime.Object
	err error
}

func newReadCache(c client.WithWatch) *readCache {
	return &readCache{c: c, gen: map[string]uint64{}, gets: map[cachedGetKey]cachedRead{}, lists: map[string]cachedRead{}}
}

// wrote invalidates what was read from namespace. Call it after the write
// was sent, whatever its outcome: a read that raced the write stored its
// result under the count from before it.
func (r *readCache) wrote(namespace string) {
	r.mu.Lock()
	r.gen[namespace]++
	r.all++
	r.mu.Unlock()
}

func copyInto(dst, src runtime.Object) {
	reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(src.DeepCopyObject()).Elem())
}

func (r *readCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, unstructured := obj.(runtime.Unstructured); unstructured || len(opts) > 0 {
		return r.c.Get(ctx, key, obj, opts...)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	k := cachedGetKey{kind: reflect.TypeOf(obj), key: key}
	gen := r.gen[key.Namespace]
	if hit, ok := r.gets[k]; ok && hit.gen == gen {
		if hit.err != nil {
			return hit.err
		}
		copyInto(obj, hit.obj)
		return nil
	}
	err := r.c.Get(ctx, key, obj)
	switch {
	case err == nil:
		r.gets[k] = cachedRead{gen: gen, obj: obj.DeepCopyObject()}
	case apierrors.IsNotFound(err):
		r.gets[k] = cachedRead{gen: gen, err: err}
	}
	return err
}

func (r *readCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	lo := &client.ListOptions{}
	lo.ApplyOptions(opts)
	if _, unstructured := list.(runtime.Unstructured); unstructured || lo.Raw != nil || lo.Limit != 0 || lo.Continue != "" {
		return r.c.List(ctx, list, opts...)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	k := fmt.Sprintf("%T|%s|%v|%v", list, lo.Namespace, lo.LabelSelector, lo.FieldSelector)
	gen := r.all
	if lo.Namespace != "" {
		gen = r.gen[lo.Namespace]
	}
	if hit, ok := r.lists[k]; ok && hit.gen == gen {
		copyInto(list, hit.obj)
		return nil
	}
	err := r.c.List(ctx, list, opts...)
	if err == nil {
		r.lists[k] = cachedRead{gen: gen, obj: list.DeepCopyObject()}
	}
	return err
}
