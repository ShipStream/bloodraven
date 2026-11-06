# shellcheck shell=bash
#
# bloodraven-doctor: shared helper for reading a sidecar HTTP endpoint.
# Sourced by sidecar-probe.sh, triage.sh and support-bundle.sh; not run on
# its own.
#
# The sidecar image is distroless: it has no shell, wget or curl, so
# `kubectl exec -c sidecar -- wget` always fails. Every container of the pod
# shares its network namespace, so the helper tries each container (sidecar
# first, then the others; the mysql image ships curl) with wget and then
# curl against 127.0.0.1:8080, and finally the API server's pod proxy
# (needs the pods/proxy permission).

# sidecar_http_candidates <namespace> <pod>: the pod's containers, sidecar
# ones first.
sidecar_http_candidates() {
    local ns="$1" pod="$2" all c existing seen
    all=$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null || echo "")
    local candidates=()
    for c in $all; do
        if [[ "$c" == "sidecar" || "$c" == "bloodraven-sidecar" ]]; then
            candidates+=("$c")
        fi
    done
    for c in $all; do
        if [[ "$c" == *"sidecar"* && "$c" != "sidecar" && "$c" != "bloodraven-sidecar" ]]; then
            candidates+=("$c")
        fi
    done
    for c in $all; do
        seen=0
        for existing in "${candidates[@]:-}"; do
            if [[ "$c" == "$existing" ]]; then
                seen=1
                break
            fi
        done
        if [[ $seen -eq 0 ]]; then
            candidates+=("$c")
        fi
    done
    echo "${candidates[@]:-}"
}

# sidecar_http_get <namespace> <pod> <path>: sets SIDECAR_HTTP_BODY to the
# body of a 2xx answer from the pod's sidecar and returns 0, or returns 1
# when nothing could fetch one. SIDECAR_HTTP_VIA names what answered
# ("container <name>" or "pod proxy"); SIDECAR_HTTP_TRIED lists what was
# tried. Call it directly, not in $(...), or the variables are lost.
sidecar_http_get() {
    local ns="$1" pod="$2" path="$3" container body
    SIDECAR_HTTP_BODY=""
    SIDECAR_HTTP_VIA=""
    SIDECAR_HTTP_TRIED=""
    for container in $(sidecar_http_candidates "$ns" "$pod"); do
        SIDECAR_HTTP_TRIED+="${SIDECAR_HTTP_TRIED:+, }$container"
        body=$(kubectl exec -n "$ns" "$pod" -c "$container" -- wget -T 5 -qO- "http://127.0.0.1:8080${path}" 2>/dev/null || true)
        if [[ -z "$body" ]]; then
            body=$(kubectl exec -n "$ns" "$pod" -c "$container" -- curl -fsS --max-time 5 "http://127.0.0.1:8080${path}" 2>/dev/null || true)
        fi
        if [[ -n "$body" ]]; then
            SIDECAR_HTTP_VIA="container $container"
            SIDECAR_HTTP_BODY="$body"
            return 0
        fi
    done
    SIDECAR_HTTP_TRIED+="${SIDECAR_HTTP_TRIED:+, }pod proxy"
    body=$(kubectl get --raw "/api/v1/namespaces/${ns}/pods/${pod}:8080/proxy${path}" 2>/dev/null || true)
    if [[ -n "$body" ]]; then
        SIDECAR_HTTP_VIA="pod proxy"
        SIDECAR_HTTP_BODY="$body"
        return 0
    fi
    return 1
}
