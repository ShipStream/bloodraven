# P2: TLS Certificate Rotation Detection

## Source
Percona bugs: K8SPS-430, K8SPS-354

## Problem
Percona found that TLS certificates were not updated when new SANs were added
to the CR. They also had a bug where custom SSL secrets were deleted on cluster
deletion even when the delete-ssl finalizer was disabled.

Bloodraven generates `my.cnf` with TLS settings pointing to certificate files,
but there is no mechanism to detect when the underlying Secret changes (cert
renewal, rotation, or SAN update) and apply the change to running MySQL instances.

## Current State in Bloodraven
- `internal/controller/reconciler.go` -- generates my.cnf with TLS paths
- No Secret watch or comparison logic
- No `FLUSH SSL` or rolling restart triggered on cert changes
- No protection against accidental Secret deletion

## Proposed Fix
1. **Hash the TLS Secret and store in a pod annotation:**
   Compute SHA256 of the Secret data and store it as an annotation on the
   Deployment's pod template (e.g., `shipstream.io/tls-secret-hash`). When
   the Secret changes, the hash changes, triggering a rolling update
   automatically via Kubernetes Deployment controller.

2. **Alternative: FLUSH SSL without restart:**
   If the cert files are mounted via a Secret volume (auto-updated by kubelet),
   run `FLUSH SSL` on MySQL periodically or on Secret change detection.
   This avoids downtime but requires connecting to MySQL to execute the command.

3. **Do not delete user-provided Secrets on CR deletion:**
   If the TLS Secret was not created by the operator (no owner reference or
   a specific label), do not delete it during CR cleanup. Only delete
   operator-generated secrets.

## Files to Modify
- `internal/controller/reconciler.go` -- add Secret hash annotation to pod template
- `internal/controller/reconciler.go` -- filter Secret deletion on CR cleanup

## Testing
- Unit test: verify hash annotation changes when Secret data changes
- Unit test: verify user-provided Secrets are not deleted on CR cleanup
- Integration: rotate a certificate and verify MySQL picks it up
