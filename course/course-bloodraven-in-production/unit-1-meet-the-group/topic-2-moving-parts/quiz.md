# Quiz — The moving parts

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

A nightly reporting job against `playground` runs long analytical SELECTs and must never issue a write. It can tolerate a few seconds of staleness but not minutes. Which Service should it connect to?

- `mysql-playground-replicas`
- `mysql-playground-primary`, because it is the only endpoint guaranteed to have a pod behind it
- `mysql-playground-reader`, because the `reader` site is the read-only site
- `mysql-playground-reader-internal`, the stable in-cluster address

**Correct option index:** 0

**Explanation:**

`mysql-playground-replicas` is the group read endpoint — instance, `shipstream.io/role=replica` and `shipstream.io/healthy=yes` — so it pools every replica currently fit to serve and drops one silently when it is not. Pointing at `mysql-playground-primary` works but wastes the primary's capacity on reads and puts an accidental write one bug away from succeeding. `mysql-playground-reader` is the near miss, and worth being exact about: because `reader` is a `role: read-only` site, its per-site Service *does* carry the same `healthy=yes` gate, so staleness is covered — but it pins the job to one pod, and the moment that pod loses the label the Service has no endpoints at all rather than falling back to `pdx`. `mysql-playground-reader-internal` is the one with no health gate: it exists for sidecar and peer traffic and sets `publishNotReadyAddresses: true`, so it will hand you a pod that is not serving yet. (objective 4)

## Question 2

**Type:** MULTIPLE_CHOICE

During the draining phase of a planned failover, the operator stamps the `iad` pod `shipstream.io/role=fenced`. Which Service still resolves to that pod?

- `mysql-playground-iad`, the per-site Service, which selects on site rather than role
- `mysql-playground-primary` — the pod is still the active site in status, so the write endpoint still points at it
- `mysql-playground-replicas` — losing the primary role makes it a replica
- None; the operator deletes the pod as part of fencing

**Correct option index:** 0

**Explanation:**

The per-site Services select on name, instance and `shipstream.io/site` — never on role — so `mysql-playground-iad` and `mysql-playground-iad-internal` still reach the pod, which is how the operator and the sidecars keep talking to a fenced instance. (`iad` is a `primary-candidate`; on a `role: read-only` site the per-site Service adds a `healthy=yes` conjunct, but role is still not in the selector.) `-primary` requires `role=primary` and `-replicas` requires `role=replica`; `fenced` is neither, which is the entire mechanism. Nothing is deleted: the pod keeps running, keeps its PVC and keeps its IP — only its label changed. (objective 4)

## Question 3

**Type:** TRUE_FALSE

A `dr-only` site holds the freshest GTID set when the primary dies, so Bloodraven will promote it rather than lose those transactions.

**Correct answer:** false

**Explanation:**

The reversal: GTID freshness never rescues a non-candidate, because promotability is checked first and is exactly `role == primary-candidate`. A `dr-only` site is counted in the topology tallies — unlike a `read-only` site it is a full core participant — which is what makes this tempting, but counting and being promotable are different properties. If such a site ever comes up writable it is routed to fencing rather than accepted. To make a site promotable you change its declared role; you cannot earn it with fresh data. (objective 6)

## Question 4

**Type:** SHORT_ANSWER

The binlog archiver runs inside the per-pod sidecar rather than centrally in the operator. Give the physical reason, and say what would break if it were moved into the operator.

**Sample answer:**

The archiver has to inotify-watch `mysql-bin.index` in `/var/lib/mysql` and read the sealed binlog files directly off disk, so it needs the MySQL data PVC mounted. That PVC is ReadWriteOnce, which binds it to a single node — the node running that site's MySQL pod. A central operator scheduled on any other node simply could not mount it, so it would have no way to see rotations or read the files. The sidecar is co-located with the data by construction, so it gets the mount (read-only) for free.

**A full-credit answer shows:**

A strong answer covers: (a) the archiver needs the data PVC mounted, for inotify on the binlog index and for direct file reads; (b) the PVC is ReadWriteOnce, meaning one node, so a central component elsewhere cannot mount it; (c) the sidecar is in the same pod, hence on the same node as the data. Credit also for noting the mount is read-only, or that the archiver gates on `@@read_only` so only the primary uploads. Do not credit 'to reduce operator load' or 'for scalability' alone — the constraint is a mount constraint, not a performance one.

**Explanation:**

The decision is forced by storage topology, not by design taste: inotify and direct binlog reads both require the data volume, and ReadWriteOnce means one node, not one pod. Anything that must touch the data directory has to run where the data directory is. (objective 5)

## Question 5

**Type:** MULTIPLE_CHOICE

The operator Deployment in `playground` is scaled to zero and stays down for an hour while the counter application keeps writing. What happens?

- Writes keep flowing through `mysql-playground-primary`, and each site's sidecar can still fence its own MySQL, but nothing will be promoted if the primary dies
- Writes stop immediately, because every write is proxied through the operator
- The `-primary` and `-replicas` Services shed their endpoints, since the operator is no longer confirming the topology
- Nothing changes — the sidecars elect a new leader among themselves and take over promotion decisions

**Correct option index:** 0

**Explanation:**

The operator is on the failure-detection and promotion path, not the request path, so a healthy primary and replica keep serving with zero operator involvement; correctness is still held by the sidecars, which fence locally without asking anyone. Writes do not stop — nothing is proxied through the operator, and its single replica with leader election exists for safe single-writer decisions, not for traffic. Endpoints do not shed either: shedding is an active label write the operator performs, and a dead operator writes nothing, so the labels simply freeze as they were. And the sidecars never elect anything — they enforce locally and have no cross-site view to decide a promotion with. (objectives 4, 5)
