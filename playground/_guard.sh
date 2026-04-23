#!/usr/bin/env bash
# _guard.sh — sourced by every playground script to make sure we're not
# pointed at a production cluster.
#
# All playground scripts label nodes, force-delete pods, apply
# NetworkPolicies, or strip MySQL PVCs. None of that is reversible
# against a real cluster. A developer whose kubeconfig current-context
# happens to point at production (easy to forget after a debug session)
# would immediately mutate prod state (AUDIT M7).
#
# Allowed contexts default to the names produced by k3d / kind /
# minikube. Export BLOODRAVEN_PLAYGROUND_CONTEXTS (space-separated) to
# add additional allowlist entries (e.g., a named remote dev cluster).
require_playground_context() {
	local ctx
	ctx=$(kubectl config current-context 2>/dev/null || true)
	if [[ -z "${ctx}" ]]; then
		echo "playground: no kubectl current-context set — refusing to run" >&2
		exit 1
	fi

	local allow=("k3d-bloodraven" "kind-bloodraven" "minikube")
	if [[ -n "${BLOODRAVEN_PLAYGROUND_CONTEXTS:-}" ]]; then
		# shellcheck disable=SC2206
		local extra=(${BLOODRAVEN_PLAYGROUND_CONTEXTS})
		allow+=("${extra[@]}")
	fi

	# Also accept any context whose prefix matches a known local cluster
	# tool: k3d-*, kind-*, minikube* — handy for multi-cluster playgrounds.
	case "${ctx}" in
		k3d-*|kind-*|minikube*) return 0 ;;
	esac

	local a
	for a in "${allow[@]}"; do
		if [[ "${ctx}" == "${a}" ]]; then
			return 0
		fi
	done

	echo "playground: current kubectl context '${ctx}' is not in the allowlist." >&2
	echo "playground: refusing to mutate a non-playground cluster." >&2
	echo "playground: allowed contexts: ${allow[*]} (prefix matches: k3d-*, kind-*, minikube*)" >&2
	echo "playground: override with BLOODRAVEN_PLAYGROUND_CONTEXTS='ctx-a ctx-b'" >&2
	exit 1
}
