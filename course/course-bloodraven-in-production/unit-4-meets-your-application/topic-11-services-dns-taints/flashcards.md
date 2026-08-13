# Flashcards — Services, DNS steering, and taints

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** How many Service objects does Bloodraven create for a failover group, and what is the formula?

**Back:** 2 × len(sites) + 2 — two per-site kinds plus two shared kinds. Three-site `playground` therefore has eight Service objects.

---

**Front:** `mysql-playground-iad-internal` — what is this Service for?

**Back:** Sidecar and peer traffic only. It carries the sidecar port beside MySQL, publishes not-ready addresses, and is the canonical replication source host. Applications never use it.

---

**Front:** Which labels are in the `mysql-<group>-primary` selector?

**Back:** Two: `app.kubernetes.io/instance=<group>` and `shipstream.io/role=primary`. No `healthy` key, and `publishNotReadyAddresses` is false.

---

**Front:** Which labels are in the `mysql-<group>-replicas` selector?

**Back:** Three: `app.kubernetes.io/instance=<group>`, `shipstream.io/role=replica`, and `shipstream.io/healthy=yes`.

---

**Front:** What stamps a pod with `role="fenced"`?

**Back:** A full-instance in-place restore, or a planned failover once it enters Draining — both strip the primary role from the source pod.

---

**Front:** Name the five conjuncts a `role: read-only` site must satisfy to be labelled `healthy=yes`.

**Back:** Source convergence `converged`; `replicating` true; non-nil `secondsBehindSource`; reported source host canonically equal to the active site's internal per-site Service (a direct source); and lag within `EffectiveReadOnlyMaxLagSeconds()`.

---

**Front:** What do the shared Services do when authority is invalid or incomplete?

**Back:** They shed every endpoint — every site is left non-primary and every reader non-serving, so clients get a connection failure instead of a stale read.

---

**Front:** apiVersion and kind of the object Bloodraven writes for DNS steering?

**Back:** `externaldns.k8s.io/v1alpha1`, kind `DNSEndpoint`. An approved upstream proposal targets `v1beta1`, with no date attached.

---

**Front:** What is the DNSEndpoint object named, and what record type does it always carry?

**Back:** `bloodraven-<group>` — so `bloodraven-playground` — and always an `A` record.

---

**Front:** Default value of `spec.dns.ttl`?

**Back:** 60 seconds. The playground overrides it to 10.

---

**Front:** Give the full taint Bloodraven applies to a demoted site's nodes in group `playground`.

**Back:** `shipstream.io/db-readonly-playground=true:NoExecute` — prefix `shipstream.io/db-readonly-`, group suffix, constant value `true`, effect `NoExecute`.

---

**Front:** Which site role is never tainted?

**Back:** `read-only`. A reader is already read-only, so there is no demotion to enforce; the tainter returns early for it (and for an empty taint selector).
