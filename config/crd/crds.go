// Package crd embeds the CustomResourceDefinitions this build was
// generated with (config/crd/bases, written by `make manifests`). The
// operator and the kubectl plugin compare them with the CRDs a cluster has
// installed (internal/crdcompat).
package crd

import "embed"

// Bases holds bases/*.yaml, one CRD per file.
//
//go:embed bases/*.yaml
var Bases embed.FS
