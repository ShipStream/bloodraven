package controller

import "sigs.k8s.io/controller-runtime/pkg/client"

// metadataPatch is the write for a metadata-only edit of an object
// (finalizers, labels, annotations, owner references): a merge patch from
// base, the deep copy taken before the edit. A full Update would send the
// whole spec re-serialized from the typed object and rewrite what the user
// wrote ("2h" becomes "2h0m0s", a time with an offset becomes UTC), which
// moves metadata.generation and shows as drift in a GitOps tool. The patch
// carries only the edit; its resourceVersion precondition keeps Update's
// conflict semantics.
func metadataPatch(base client.Object) client.Patch {
	return client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
}
