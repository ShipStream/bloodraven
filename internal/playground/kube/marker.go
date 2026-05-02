package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ChaosInProgressAnnotation is the MFG annotation the chaos runner sets
// while a scenario is running. The domain is intentionally distinct
// from operator annotations so it is unmistakably tooling-side.
const ChaosInProgressAnnotation = "chaos.playground.bloodraven.io/in-progress"

// inProgressJSONPointer is the JSON-Pointer-escaped form of
// ChaosInProgressAnnotation. The "/" in the annotation key must be
// escaped as "~1" per RFC 6901.
const inProgressJSONPointer = "/metadata/annotations/chaos.playground.bloodraven.io~1in-progress"

// ChaosMarker is the payload of the in-progress annotation. It lets
// preflight on a future run identify the owner: same host + live pid =
// "wait", same host + dead pid = "abandoned", different host = "wait".
type ChaosMarker struct {
	Scenario   string    `json:"scenario"`
	StartedAt  time.Time `json:"startedAt"`
	PID        int       `json:"pid"`
	Host       string    `json:"host"`
	CaptureDir string    `json:"captureDir,omitempty"`
}

// ErrChaosMarkerConflict is returned by SetChaosMarker when the MFG was
// modified between the read and the patch. Callers can retry or surface
// the conflict to the user.
var ErrChaosMarkerConflict = errors.New("chaos marker: resourceVersion conflict (concurrent modification)")

// ReadChaosMarker returns the parsed marker on the MFG, or (nil, nil)
// if no marker is present. Returns an error only on API failure or a
// malformed payload.
func (c *Client) ReadChaosMarker(ctx context.Context, namespace string) (*ChaosMarker, error) {
	mfg, err := c.GetMFG(ctx, namespace)
	if err != nil {
		return nil, err
	}
	raw, ok := mfg.Annotations[ChaosInProgressAnnotation]
	if !ok || raw == "" {
		return nil, nil
	}
	var m ChaosMarker
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("parse chaos marker: %w", err)
	}
	return &m, nil
}

// SetChaosMarker installs the in-progress marker on the MFG. The patch
// is guarded by a JSON-Patch `test` op against metadata.resourceVersion
// so two concurrent chaos runners cannot both think they own the MFG.
//
// Returns ErrChaosMarkerConflict if the MFG was mutated between the
// read and the patch. The caller is expected to bail with a clear
// message rather than retry blindly — a conflict here usually means a
// second chaos process is racing us, which is exactly the case the
// marker is meant to detect.
func (c *Client) SetChaosMarker(ctx context.Context, namespace string, m ChaosMarker) error {
	mfg, err := c.GetMFG(ctx, namespace)
	if err != nil {
		return err
	}
	rv := mfg.ResourceVersion
	if rv == "" {
		return fmt.Errorf("chaos marker: MFG returned with empty resourceVersion")
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("chaos marker: marshal payload: %w", err)
	}

	ops := []JSONPatchOp{
		{Op: "test", Path: "/metadata/resourceVersion", Value: rv},
	}
	// JSON Patch can't add /metadata/annotations/<key> if annotations is
	// missing. If the live object has no annotations map yet, prepend an
	// `add` of an empty map.
	if mfg.Annotations == nil {
		ops = append(ops, JSONPatchOp{
			Op:    "add",
			Path:  "/metadata/annotations",
			Value: map[string]string{},
		})
	}
	ops = append(ops, JSONPatchOp{
		Op:    "add",
		Path:  inProgressJSONPointer,
		Value: string(payload),
	})

	if err := c.PatchMFG(ctx, namespace, ops); err != nil {
		if isPatchTestOpFailure(err) {
			return ErrChaosMarkerConflict
		}
		return err
	}
	return nil
}

// ClearChaosMarker removes the marker. Best-effort: missing-marker is
// not an error (idempotent); a "remove" op against a non-existent path
// is swallowed. Returns the underlying API error otherwise.
func (c *Client) ClearChaosMarker(ctx context.Context, namespace string) error {
	// Use a merge patch with the key set to nil — this is the
	// idempotent "delete annotation" pattern that AnnotateMFG already
	// relies on, and it sidesteps the JSON-Patch "remove on missing
	// path fails" trap.
	return c.AnnotateMFG(ctx, namespace, ChaosInProgressAnnotation, "")
}

// isPatchTestOpFailure recognizes the apierrors.StatusError shape
// kube returns when a JSON-Patch test op fails. The API server reports
// it as a 422 Invalid with a "test operation" string in the message;
// we match defensively because the exact wording has shifted between
// kube versions.
func isPatchTestOpFailure(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsInvalid(err) || apierrors.IsConflict(err) {
		msg := err.Error()
		if strings.Contains(msg, "test operation") || strings.Contains(msg, "the testing value") {
			return true
		}
	}
	return false
}
