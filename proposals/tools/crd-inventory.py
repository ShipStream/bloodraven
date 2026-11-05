#!/usr/bin/env python3
"""Generate the API field inventory (markdown) from the controller-gen CRDs.

Usage (from the repo root; needs yq v4 on PATH):

    python3 -I proposals/tools/crd-inventory.py config/crd/bases \
        > proposals/20-api-v1beta1-inventory.md

Embedded upstream Kubernetes types (containers, resources, security
contexts, conditions) are printed as one row rather than expanded. A
resources block is recognised by its shape, so a property such as
`sidecarResources` collapses too.

Every served version must have the same schema, subresources and printer
columns (D1). The inventory is printed once, and the script exits non-zero
if the served versions differ, rather than describing only the first one.
"""
import json
import sys
import glob
import os
import subprocess

# Properties whose schema is an embedded upstream Kubernetes type. We print one
# row for them instead of expanding thousands of upstream fields.
COLLAPSE = {
    "extraContainers": "[]corev1.Container",
    "extraInitContainers": "[]corev1.Container",
    "resources": "corev1.ResourceRequirements",
    "podSecurityContext": "corev1.PodSecurityContext",
    "containerSecurityContext": "corev1.SecurityContext",
    "conditions": "[]metav1.Condition",
}

# The property set of corev1.ResourceRequirements, recognised structurally.
RESOURCE_REQUIREMENTS_KEYS = {"claims", "limits", "requests"}

# Scalar constraints reported for a field, and for its list items and map
# values with an "items " or "values " prefix.
SCALAR_KEYS = ("minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems",
               "minProperties", "maxProperties")

ORDER = [
    "MysqlFailoverGroup",
    "MysqlBackup",
    "MysqlBackupVerification",
    "MysqlDatabase",
    "MysqlStandbyCluster",
]


def esc(s):
    return str(s).replace("|", "\\|").replace("\n", " ")


def typ(s):
    t = s.get("type", "")
    if s.get("x-kubernetes-int-or-string"):
        return "int-or-string"
    if t == "array":
        it = s.get("items", {})
        return "[]" + (it.get("type", "object"))
    if t == "object" and isinstance(s.get("additionalProperties"), dict):
        return "map[string]" + (typ(s["additionalProperties"]) or "object")
    if s.get("x-kubernetes-preserve-unknown-fields"):
        return (t or "any") + " (preserve-unknown)"
    if s.get("format"):
        return f"{t} ({s['format']})"
    return t


def scalar_constraints(s, prefix=""):
    out = []
    if "enum" in s:
        out.append(prefix + "enum: " + ", ".join(f"`{e}`" for e in s["enum"]))
    for k in SCALAR_KEYS:
        if k in s:
            out.append(f"{prefix}{k}={s[k]}")
    if "pattern" in s:
        out.append(f"{prefix}pattern=`{s['pattern']}`")
    return out


def validation(s):
    out = scalar_constraints(s)
    if s.get("x-kubernetes-list-type"):
        lt = s["x-kubernetes-list-type"]
        if lt == "map":
            lt += " keys=" + ",".join(s.get("x-kubernetes-list-map-keys", []))
        out.append("listType=" + lt)
    if s.get("x-kubernetes-map-type"):
        out.append("mapType=" + s["x-kubernetes-map-type"])
    it = s.get("items") or {}
    out.extend(scalar_constraints(it, "items "))
    if it.get("format"):
        out.append(f"items format={it['format']}")
    ap = s.get("additionalProperties")
    ap = ap if isinstance(ap, dict) else {}
    out.extend(scalar_constraints(ap, "values "))
    if ap.get("format"):
        out.append(f"values format={ap['format']}")
    for v in s.get("x-kubernetes-validations", []) or []:
        rule = v.get("rule", "")
        tag = "CEL"
        if "oldSelf" in rule:
            tag = "CEL(transition)"
        out.append(f"{tag}: `{rule}`")
    for v in (it.get("x-kubernetes-validations") or []):
        out.append(f"items CEL: `{v.get('rule','')}`")
    for v in (ap.get("x-kubernetes-validations") or []):
        out.append(f"values CEL: `{v.get('rule','')}`")
    return "; ".join(out)


def collapsed_type(name, p, full, collapse_paths):
    if name in COLLAPSE:
        return COLLAPSE[name]
    if full in collapse_paths:
        return collapse_paths[full]
    if set((p.get("properties") or {}).keys()) == RESOURCE_REQUIREMENTS_KEYS:
        return "corev1.ResourceRequirements"
    return None


def rows(s, path, acc, collapse_paths):
    props = s.get("properties") or {}
    req = set(s.get("required", []) or [])
    for name in sorted(props):
        p = props[name]
        full = f"{path}.{name}" if path else name
        collapsed = collapsed_type(name, p, full, collapse_paths)
        t = collapsed if collapsed else typ(p)
        dflt = ""
        if "default" in p:
            dflt = "`" + json.dumps(p["default"]) + "`"
        acc.append((full, t, "yes" if name in req else "", dflt, validation(p)))
        if collapsed:
            continue
        if p.get("type") == "object" and p.get("properties"):
            rows(p, full, acc, collapse_paths)
        elif p.get("type") == "array" and (p.get("items") or {}).get("properties"):
            rows(p["items"], full + "[]", acc, collapse_paths)
        elif isinstance(p.get("additionalProperties"), dict) and p["additionalProperties"].get("properties"):
            rows(p["additionalProperties"], full + "{}", acc, collapse_paths)


def table(acc):
    lines = ["| Path | Type | Req | Default | Validation |", "|---|---|---|---|---|"]
    for r in acc:
        lines.append("| `" + esc(r[0]) + "` | " + " | ".join(esc(x) for x in r[1:]) + " |")
    return "\n".join(lines)


def main():
    d = sys.argv[1]
    crds = {}
    for f in sorted(glob.glob(os.path.join(d, "*.yaml"))):
        c = json.loads(subprocess.run(["yq", "-o=json", ".", f], check=True,
                                      capture_output=True, text=True).stdout)
        crds[c["spec"]["names"]["kind"]] = c
    print("# Appendix to proposal 20: API field inventory\n")
    print("<!-- Generated by proposals/tools/crd-inventory.py from config/crd/bases. Do not edit by hand. -->\n")
    print("Generated from the controller-gen CRDs. `Req` means listed in the parent's `required`. "
          "`CEL(transition)` rules reference `oldSelf`. Embedded upstream Kubernetes types are collapsed to one row. "
          "`items` and `values` prefix the bounds on list items and map values.\n")
    for kind in ORDER:
        c = crds[kind]
        spec = c["spec"]
        names = spec["names"]
        print(f"## {kind}\n")
        vers = [v["name"] + (" (storage)" if v.get("storage") else "") + ("" if v.get("served") else " (not served)")
                for v in spec["versions"]]
        print(f"- Scope: {spec['scope']}; plural `{names['plural']}`; shortNames: "
              f"{', '.join('`'+x+'`' for x in names.get('shortNames', [])) or 'none'}; "
              f"categories: {', '.join(names.get('categories', [])) or 'none'}")
        served = [x for x in spec["versions"] if x.get("served")] or spec["versions"]
        v = served[0]
        for other in served[1:]:
            for key in ("schema", "subresources", "additionalPrinterColumns"):
                if other.get(key) != v.get(key):
                    sys.exit(f"{kind}: served versions {v['name']} and {other['name']} differ in {key}; "
                             "D1 requires identical served schemas (C-4)")
        same = " (schema identical across served versions)" if len(served) > 1 else ""
        print(f"- Versions: {', '.join(vers)}{same}; subresources: {', '.join(sorted((v.get('subresources') or {}).keys())) or 'none'}")
        cols = v.get("additionalPrinterColumns") or []
        if cols:
            print("- Printer columns: " + ", ".join(f"{x['name']} (`{x['jsonPath']}`)" for x in cols))
        root = v["schema"]["openAPIV3Schema"]
        if root.get("required"):
            print("- Root required: " + ", ".join(f"`{x}`" for x in root["required"]))
        top = root.get("x-kubernetes-validations") or []
        for x in top:
            print(f"- Root CEL: `{x['rule']}`")
        collapse_paths = {}
        if kind == "MysqlStandbyCluster":
            collapse_paths["spec.template.spec"] = "MysqlFailoverGroupSpec (see MysqlFailoverGroup spec)"
        for part in ("spec", "status"):
            ps = root["properties"].get(part)
            if not ps:
                continue
            for x in ps.get("x-kubernetes-validations") or []:
                print(f"- `{part}` CEL: `{x['rule']}`")
            acc = []
            rows(ps, part, acc, collapse_paths)
            print(f"\n### {kind} `{part}` ({len(acc)} fields)\n")
            print(table(acc))
            print()


if __name__ == "__main__":
    main()
