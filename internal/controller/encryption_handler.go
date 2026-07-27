package controller

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// KeyringEscrowRequest is the body a sidecar POSTs to /keyring/escrow.
// Wire-compatible with internal/sidecar.escrowRequest.
type KeyringEscrowRequest struct {
	Namespace string `json:"namespace"`
	Group     string `json:"group"`
	Site      string `json:"site"`
	Digest    string `json:"digest"`
	Keyring   string `json:"keyring"` // base64
}

// KeyringEscrowResponse is the operator's reply. The echoed digest is
// load-bearing: the sidecar treats a mismatch as a failed escrow and
// retries, so a truncated or mangled body can never be mistaken for a
// durable capture.
type KeyringEscrowResponse struct {
	Version int32  `json:"version"`
	Digest  string `json:"digest"`
	Secret  string `json:"secret"`
}

// NewKeyringEscrowHandler returns the HTTP handler for the operator's
// auxiliary /keyring/escrow endpoint.
//
// This is the one endpoint on the auxiliary server that mutates cluster
// state, so it is the one endpoint that authenticates. Every request
// must present the per-site bearer token the operator minted into a
// Secret that is mounted only into that site's pod, and only while that
// site is deliberately unsealed. A sealed site carries no token at all,
// so a compromised workload elsewhere in the namespace has nothing to
// replay.
//
// Defence in depth beyond the token:
//
//   - the body is capped at maxKeyringBytes;
//   - the claimed digest must match a fresh hash of the decoded bytes,
//     so a corrupted upload cannot be recorded as authoritative;
//   - escrow Secrets are immutable, so an accepted push can only ever
//     add a new version, never rewrite the version a running MySQL is
//     currently sealed against;
//   - the reconcile loop independently re-reads the stored Secret and
//     compares it against the sidecar's reported live digest before it
//     will seal anything.
func NewKeyringEscrowHandler(c client.Client, logger *slog.Logger) http.Handler {
	var store *keyringEscrowStore
	if c != nil {
		store = &keyringEscrowStore{client: c, scheme: c.Scheme()}
	}

	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodPost {
			writeKeyringError(rw, http.StatusMethodNotAllowed, "POST required")
			return
		}
		if c == nil {
			writeKeyringError(rw, http.StatusServiceUnavailable, "operator k8s client not configured")
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxKeyringBytes+1))
		if err != nil {
			writeKeyringError(rw, http.StatusBadRequest, "cannot read request body")
			return
		}
		if len(body) > maxKeyringBytes {
			writeKeyringError(rw, http.StatusRequestEntityTooLarge, "keyring payload too large")
			return
		}

		var req KeyringEscrowRequest
		if err := json.Unmarshal(body, &req); err != nil {
			writeKeyringError(rw, http.StatusBadRequest, "malformed JSON body")
			return
		}
		if req.Namespace == "" || req.Group == "" || req.Site == "" || req.Keyring == "" {
			writeKeyringError(rw, http.StatusBadRequest, "namespace, group, site, and keyring are required")
			return
		}

		token := bearerToken(r.Header.Get("Authorization"))
		if token == "" {
			writeKeyringError(rw, http.StatusUnauthorized, "bearer token required")
			return
		}

		ctx := r.Context()
		if err := verifyEscrowToken(ctx, c, req.Namespace, req.Group, req.Site, token); err != nil {
			// Deliberately vague to the caller — the log carries the
			// detail. An attacker probing the endpoint should not learn
			// whether a site exists or whether a token was ever issued.
			logger.Warn("keyring escrow rejected",
				"namespace", req.Namespace, "group", req.Group, "site", req.Site,
				"reason", err.Error())
			writeKeyringError(rw, http.StatusForbidden, "escrow token rejected")
			return
		}

		raw, err := base64.StdEncoding.DecodeString(req.Keyring)
		if err != nil {
			writeKeyringError(rw, http.StatusBadRequest, "keyring is not valid base64")
			return
		}
		if len(raw) == 0 {
			writeKeyringError(rw, http.StatusBadRequest, "keyring is empty")
			return
		}
		if len(raw) > maxKeyringBytes {
			writeKeyringError(rw, http.StatusRequestEntityTooLarge, "keyring payload too large")
			return
		}

		computed := keyringDigest(raw)
		if req.Digest != "" && req.Digest != computed {
			logger.Warn("keyring escrow digest mismatch",
				"namespace", req.Namespace, "group", req.Group, "site", req.Site,
				"claimed", req.Digest, "computed", computed)
			writeKeyringError(rw, http.StatusBadRequest, "digest does not match payload")
			return
		}

		var fg v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, types.NamespacedName{
			Namespace: req.Namespace, Name: req.Group,
		}, &fg); err != nil {
			if apierrors.IsNotFound(err) {
				writeKeyringError(rw, http.StatusNotFound, "failover group not found")
				return
			}
			logger.Error("keyring escrow: cannot read failover group",
				"namespace", req.Namespace, "group", req.Group, "error", err)
			writeKeyringError(rw, http.StatusInternalServerError, "internal error")
			return
		}
		if !fg.Spec.EncryptionEnabled() {
			writeKeyringError(rw, http.StatusConflict, "encryption at rest is not enabled for this group")
			return
		}
		if fg.Spec.SiteByName(req.Site) == nil {
			writeKeyringError(rw, http.StatusNotFound, "site not found in failover group")
			return
		}

		version, err := store.put(ctx, &fg, req.Site, raw)
		if err != nil {
			logger.Error("keyring escrow: store failed",
				"namespace", req.Namespace, "group", req.Group, "site", req.Site, "error", err)
			writeKeyringError(rw, http.StatusInternalServerError, "could not store keyring")
			return
		}

		logger.Info("keyring escrowed",
			"namespace", req.Namespace, "group", req.Group, "site", req.Site,
			"version", version.Version, "secret", version.Name, "digest", version.Digest)

		_ = json.NewEncoder(rw).Encode(KeyringEscrowResponse{
			Version: version.Version,
			Digest:  version.Digest,
			Secret:  version.Name,
		})
	})
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

func writeKeyringError(rw http.ResponseWriter, code int, msg string) {
	rw.WriteHeader(code)
	_ = json.NewEncoder(rw).Encode(map[string]string{"error": msg})
}
