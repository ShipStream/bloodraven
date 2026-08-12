# Quiz — Services, DNS steering, and taints

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

A nightly reconciliation job writes heavily to `orders`. A BI reporting job only reads from it. Which Services should each bind to?

- Reconciliation job → `mysql-orders-primary`; BI job → `mysql-orders-replicas`
- Reconciliation job → `mysql-orders-pdx` (the current active site); BI job → `mysql-orders-replicas`
- Reconciliation job → `mysql-orders-primary`; BI job → `mysql-orders-reader-internal`
- Both jobs → `mysql-orders-primary`, because only the primary is guaranteed to have current data

**Correct option index:** 0

**Explanation:**

`-primary` selects on `role=primary` and follows the promotion; `-replicas` selects on `role=replica` plus `healthy=yes` and drops a lagging or non-converged reader automatically. Option 2 pins writes to whichever site is active today — the per-site Service does not follow a promotion, so the job writes to a read-only MySQL the moment `orders` fails over. Option 3 sends reads at the internal per-site Service, which exists for sidecar and peer traffic and applies no health gate at all. Option 4 is the common over-caution: it works, but it puts every reporting query on the write path and throws away the reader tier you are paying for. (objective 1)

## Question 2

**Type:** SHORT_ANSWER

A planned failover has stamped `role="fenced"` on the source site's MySQL pod. Which Services still reach that pod, and which do not?

**Sample answer:**

Neither shared Service reaches it. `mysql-orders-primary` requires `role=primary` and `mysql-orders-replicas` requires `role=replica` plus `healthy=yes`; `"fenced"` matches neither, so the pod drops out of both at once. Its own per-site Service, `mysql-orders-<site>`, selects on name/instance/site and does not look at `role`, so that one still reaches it — which is how you keep an operator path to a fenced instance.

**A full-credit answer shows:**

A strong answer names both shared Services and why the label misses each selector, and says the per-site Service still selects the pod because `role` is not in its selector. Credit answers that note this is what fencing looks like at the Service layer. Do not credit an answer claiming the pod is unreachable from everywhere, or that fencing works by deleting the Service.

**Explanation:**

Fencing at the Service layer is a label change, not an object deletion. Because the two shared selectors name mutually exclusive role values, one label edit removes the pod from application writes and application reads in a single step, while the per-site Service keeps it reachable for diagnosis. (objective 1)

## Question 3

**Type:** MULTIPLE_CHOICE

The `reader` site in `orders` reports `replicating: true`, source convergence `converged`, and `secondsBehindSource: 45`. The group sets `replication.maxLagSeconds: 30` and does not set `readOnlyMaxLagSeconds`. Why is `reader` absent from `mysql-orders-replicas`?

- `role: read-only` sites are never members of `-replicas`; they are reachable only through their own per-site Service
- The lag conjunct fails: `readOnlyMaxLagSeconds` is unset, so the effective reader threshold inherits `maxLagSeconds` = 30, and 45 > 30 leaves the pod labelled `healthy=no`
- `maxLagSeconds` only drives the `ReplicationLagging` condition, so it cannot affect Services — the pod must be failing its readiness probe instead
- Endpoints are withheld until the anti-flap cooldown from the last failover has expired

**Correct option index:** 1

**Explanation:**

All five reader conjuncts must hold; four of five is `healthy=no`, and here the fifth — lag within `EffectiveReadOnlyMaxLagSeconds()` — is the one that fails, because a nil `readOnlyMaxLagSeconds` inherits `maxLagSeconds`. Option 1 inverts the design: serving reads is exactly what a `read-only` site is for. Option 3 is a real half-truth worth unlearning — `maxLagSeconds` does drive the `ReplicationLagging` condition, but it is also what the reader threshold inherits, so it reaches the endpoint list too. Option 4 confuses the failover cooldown, which gates promotion, with endpoint membership, which is recomputed every reconcile. (objective 1)

## Question 4

**Type:** TRUE_FALSE

An RBAC rule denied the operator's `DNSEndpoint` apply during the promotion of `orders`. Once the rule is fixed, an operator has to re-run the failover or re-apply the record by hand to get DNS pointing at the new primary.

**Correct answer:** false

**Explanation:**

The reverse is true: nobody has to do anything. `reconcileDNS` runs on every poll, re-derives the desired target from live topology rather than replaying a memoized one, and the write itself is one idempotent server-side apply with forced ownership — no create/update split. So the record heals on a later poll, with no second failover and no MySQL mutation, and the heal survives an operator restart because nothing needed to be remembered. Chaos scenario 38 is exactly this experiment. What manual action cannot fix is the other half: the operator cannot accelerate DNS propagation once the record is written. (objective 2)

## Question 5

**Type:** MULTIPLE_CHOICE

Bloodraven taints iad's nodes with `shipstream.io/db-readonly-orders=true:NoExecute` after promoting pdx. What happens to workloads already running on those nodes?

- Nothing is evicted; the taint only stops new pods being scheduled onto iad's nodes until it is removed
- Every pod on the node is evicted — tolerations affect scheduling decisions only, not running pods
- A pod with no matching toleration is evicted immediately; a pod tolerating the taint with `tolerationSeconds: 60` stays bound for 60 s and is then evicted by the node lifecycle controller
- Pods covered by a PodDisruptionBudget stay on the node; only pods without one are evicted

**Correct option index:** 2

**Explanation:**

That is upstream `NoExecute` semantics, and chaos scenario 21 verifies it against a real promotion: the non-tolerating canary is evicted while a canary tolerating the same taint stays `Running`. Option 1 describes `NoSchedule`, the effect people assume; `NoExecute` reaches pods that are already running, which is the entire point of using it for a demotion. Option 2 forgets that a toleration on the key is exactly what keeps a pod bound — scenario 21's tolerating canary stays put. Option 4 is the most dangerous distractor: a PDB protects only against *voluntary* evictions, and a taint-driven eviction is not one. (objective 3)
