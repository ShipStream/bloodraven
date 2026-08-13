# Unit 1 test — Reading a failover group

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Assesses:** Quick check: can you name the active site from a status dump, say which pod the `-primary` Service selects, and explain why a `read-only` site can never be promoted?

**Passing score:** 70%

## Question 1

**Type:** MULTIPLE_CHOICE

Four teams describe the MySQL they want to run. Which description is the deployment Bloodraven is built for?

- Two datacentres on a wide-area link, ordinary asynchronous replication, exactly one site writable at a time, and a business that accepts losing a few seconds of committed writes when a site dies suddenly
- Three nodes in one rack on a low-latency link, where a sudden node loss must never cost a single committed transaction
- One site with two MySQL instances that both accept writes, so write throughput scales with the number of instances
- Two sites plus a witness in a third location, so that a majority always exists and the survivors can vote on who leads

**Correct option index:** 0

**Explanation:**

That first line is the design point stated outright: two or more sites (`spec.sites` takes 2 to 16), ordinary asynchronous replication, exactly one writable site, and a non-zero RPO accepted on unplanned loss. The three-nodes-never-lose-a-transaction team wants zero RPO on sudden loss, which is a published non-goal — send them to a quorum system such as Group Replication. The multi-writer team wants something Bloodraven never does at all: authority is a single site, and any other site found writable is fenced rather than accepted. The witness answer is quorum thinking imported from etcd or Group Replication; Bloodraven has no vote and no majority to reach, which is exactly why two sites is a legitimate topology for it and a pathological one for them. (objective 1)

## Question 2

**Type:** MULTIPLE_CHOICE

You are reviewing an architecture document that depends on `playground`. Four lines describe what the team expects Bloodraven to do for them. Which one is the expectation Bloodraven will actually meet?

- "Bloodraven runs external-dns for us, so DNS publishing is one less component we have to deploy and keep alive ourselves."
- "Bloodraven reports the transactions lost on promotion — a count as a metric, the exact GTID set in status — so we reconcile by hand."
- "Bloodraven merges the losing side's writes back into the survivor once a split brain has been resolved, so nothing has to be replayed."
- "Bloodraven keeps our PVC-local backups durable across the loss of the cluster, so an off-cluster object store is optional for us."

**Correct option index:** 1

**Explanation:**

Bloodraven makes the loss observable, not invisible: it computes the set difference between the old primary's GTIDs and the new primary's, publishes the count as `bloodraven_divergent_transactions` and records the set in `status.sites[].divergentGtid`. Reconciling those writes is your job — reconciliation of divergent writes is a named non-goal, which is why the merge line is wrong however tempting GTID identity makes it look. Replacing external-dns is also a non-goal: Bloodraven writes a `DNSEndpoint` object and something else has to publish it, so removing external-dns removes the thing that makes the record real. And PVC-local backup durability after cluster or storage loss is the third non-goal in play — when a PVC is destroyed the binlog that lived on it is gone forever, which is precisely why an off-cluster store is not optional. (objective 2)

## Question 3

**Type:** MULTIPLE_CHOICE

The wide-area link between `iad` and `pdx` degrades for forty minutes. Both sites' MySQL stays up and healthy locally; neither can see the other. What does Bloodraven do with writes, and what would a majority-based group do on the same link?

- Both refuse writes for the duration, because neither design will accept a write it cannot confirm the peer has seen
- Bloodraven promotes `pdx` as well so each side keeps serving its local clients, then reconciles the two histories when the link returns
- Bloodraven keeps the site that already holds authority writable and lets the peer fall behind; a two-member majority-based group has no majority on either side of that link and refuses writes until it returns
- A majority-based group keeps both sides writable, since members replicate to each other asynchronously in the background

**Correct option index:** 2

**Explanation:**

This is the trade, made visible: Bloodraven buys write availability with consistency-on-unplanned-loss, so the site holding authority keeps taking writes and the peer simply falls behind. A quorum design buys the reverse — it refuses writes rather than accept ones it might have to discard, and with two members split one-and-one neither side holds a majority, so both go read-only. Two sides both refusing is the quorum outcome only, not Bloodraven's; Bloodraven has no quorum to lose. Promoting `pdx` as well is the failure the whole design exists to prevent — two writable sites is split brain, and a non-authoritative site is fenced rather than promoted. And the last option simply misdescribes Group Replication: its cost is paid as a cross-node round trip on every commit, which is what makes it consistent and what makes it slow across a WAN. (objective 3)

## Question 4

**Type:** MULTIPLE_CHOICE

`playground` reports `activeSite: iad`; `iad` state `writable`; `pdx` state `read-only`, `replicating: true`; and `reader` — declared `role: read-only` in the spec — state `writable`. What `reason` does the `Degraded` condition carry, and what happens to `reader`?

- `SplitBrain`, and the operator resolves it by picking a winner between `iad` and `reader`
- `Healthy`, and nothing happens to `reader` — a `read-only` site is excluded from the tallies, so the operator ignores it entirely
- `NoPrimary`, and the operator waits for a human because authority is now ambiguous
- `Degraded`, and `reader` is fenced — a writable non-`primary-candidate` site is routed straight to fencing, never to promotion

**Correct option index:** 3

**Explanation:**

Two rules meet here. A `role: read-only` site is excluded from `coreCount` and from all three state tallies, so it cannot make the group `SplitBrain` — that reason needs more than one *core* site writable, and only `iad` qualifies. But exclusion from the tallies is not the same as being ignored: any site that is writable while its role is not `primary-candidate` is routed to fencing, and that shape reports `Degraded`. `Healthy` is the trap for anyone who learns the exclusion rule and stops there — the tallies ignore `reader`, the fencing path does not. `NoPrimary` describes the opposite topology, no core site writable and none unreachable, whereas `iad` is writable right now. And `reader` is never a promotion candidate to pick a winner between: promotability is exactly `role == primary-candidate`, and fresh data or a writable state does not earn it. (objectives 6, 8)

## Question 5

**Type:** TRUE_FALSE

PITR is enabled on `playground`. Both the `iad` and `pdx` sidecars are archiving binlogs to your object store, which gives you two independent archive streams to restore from.

**Correct answer:** false

**Explanation:**

The opposite is true: only one sidecar is uploading at any moment. The archiver carries a role gate — it checks `@@read_only` and returns without archiving when it is on — so only the site that is currently writable ships binlogs, and `pdx` as a replica ships nothing. The archiver runs in the sidecar rather than the operator for a physical reason, not a redundancy one: it inotify-watches the binlog directory and reads the files straight off `/var/lib/mysql`, so it needs the MySQL data PVC mounted, and a `ReadWriteOnce` PVC is bound to one node. Co-location is the requirement, not duplication. It also uploads only *sealed* binlogs, dropping the last index entry because that is the file MySQL is still writing — so even the one live stream deliberately lags the primary's newest transactions. (objective 5)

## Question 6

**Type:** SHORT_ANSWER

You have just run `./playground/setup.sh` against a fresh three-agent k3d cluster. Describe the checks you run to confirm the group is genuinely up, and name the observed values you must not carry into a production manifest.

**Sample answer:**

First the pods: `kubectl -n bloodraven-playground get pods -l app.kubernetes.io/name=mysql` should list three, one per site, and each should read 2/2 Ready — `mysql` plus its `sidecar` container. Then the status. `status.activeSite` names exactly one site, say `iad`. In `status.sites[]`, `iad` reads state `writable` with no `replicating`, `secondsBehindSource` or `gtidExecuted` keys at all, because the operator only probes replication on read-only sites; `pdx` and `reader` read state `read-only` with `replicating: true` and `secondsBehindSource: 0`. The `Ready` condition is True and the `Degraded` condition's `reason` is `Healthy`. The values I would not carry to production are the playground's deliberate overrides: `failoverCooldown: 30s` against a shipped default of 5m, `replication.maxLagSeconds: 30` against 300, and `dns.ttl: 10` against 60. Everything else — `pollInterval`, `failureThreshold`, `recoveryThreshold`, `leaseTimeout`, `peerCheckInterval`, `maxSyncWait` — is at its shipped default here.

**A full-credit answer shows:**

A strong answer covers: (1) three MySQL pods, each 2/2 because every MySQL pod runs a sidecar container beside mysqld; (2) reading the status — one named `activeSite`, that site `writable`, the other two `read-only` with `replicating: true` and lag 0, `Degraded` reason `Healthy`; (3) at least two of the three playground overrides with their shipped defaults: `failoverCooldown` 30s vs 5m, `maxLagSeconds` 30 vs 300, `dns.ttl` 10 vs 60. Credit for noting that the primary's entry has replication fields absent rather than zero or false, and for noting that timings observed in the playground are not shipped timings. Do not credit an answer that reads a 1/1 pod as healthy, or that presents the overridden values as defaults.

**Explanation:**

Confirming a group is up is two checks, not one: containers running is a Kubernetes fact, and authority plus replication is a Bloodraven fact that only the status block carries. Learning the healthy shape on a cluster where nothing is wrong is what makes a wrong shape obvious later. The overrides matter because a cooldown or a lag threshold you internalise here is six to ten times tighter than what ships, and a timing measured in the playground is not a timing you can quote for production. (objectives 7, 8)

## Question 7

**Type:** MULTIPLE_CHOICE

You press **+ Increment** five times in the counter app, which writes through `mysql-playground-primary`. You want to prove that the resulting row actually reached `pdx`. Which check proves it?

- `kubectl exec deploy/mysql-playground-pdx -c mysql -- mysql -h127.0.0.1 -Nse "SELECT value, updated_at FROM counter_db.counters WHERE id = 1"`, and compare the value with the counter
- Read `status.sites[]` and confirm `pdx` shows `secondsBehindSource: 0`, which means it has applied everything the primary has committed
- Query `SELECT value FROM counter_db.counters WHERE id = 1` through the `mysql-playground-replicas` Service and compare the value with the counter
- Read `status.sites[]` and confirm `pdx` shows `replicating: true`, the operator's verdict that replication on that follower is healthy

**Correct option index:** 0

**Explanation:**

Only reading the row out of `pdx` by name proves the row is on `pdx`. `secondsBehindSource: 0` does not: it compares the last transaction executed against the last event *downloaded*, so it reads zero when the receiver thread has stalled or the replica is idle — a replica that stopped fetching an hour ago still reports 0. `replicating: true` is a verdict on replication health, not a statement that any particular transaction has landed. And `mysql-playground-replicas` is the wrong endpoint for this experiment: it selects every replica currently carrying `shipstream.io/healthy=yes`, so the answer might have come from `reader` rather than `pdx` and you would have proved nothing about the site you asked about. Write through the group endpoint, read back by site name. (objective 9)

## Question 8

**Type:** MULTIPLE_CHOICE

Authority in `playground` moves from `iad` to `pdx`. Your application holds no connection open and simply opens a new one to `mysql-playground-primary`. What concretely makes that `INSERT` land on the `pdx` pod?

- The operator rewrites the `mysql-playground-primary` Service's selector so it names the `pdx` site instead of `iad`
- The `DNSEndpoint` A record for the group hostname is repointed at the `pdx` load-balancer IP, which is what moves in-cluster traffic
- The `pdx` sidecar takes over the primary role and forwards traffic to its local `mysqld` on behalf of the group endpoint
- The operator stamps `shipstream.io/role=primary` on the `pdx` pod and removes it from `iad`; the Service's fixed selector then matches a different pod and the endpoint follows

**Correct option index:** 3

**Explanation:**

The `-primary` Service selector is fixed at two labels — `app.kubernetes.io/instance=playground` and `shipstream.io/role=primary` — and never changes. The operator owns the pod labels, so moving authority is a label write; the endpoint controller does the rest, which is also exactly how a pod stamped `role=fenced` drops out of both group endpoints without being deleted. Rewriting the selector to name a site is the plausible-sounding inversion of that mechanism and would defeat the point: the selector is stable so the Service object never has to change. The `DNSEndpoint` object matters for clients resolving the group's external hostname; a pod resolving `mysql-playground-primary` inside the cluster never consults it. And nothing proxies MySQL traffic — the sidecar decides nothing about routing and forwards no queries; it fences its own `mysqld` locally, which is what it can do when the operator is gone, while the operator is the only component that decides across sites. (objectives 4, 5)

## Question 9

**Type:** MULTIPLE_CHOICE

You add a third site, `fra`, purely to serve analytics reads. It must never be promoted. A colleague sets `role: dr-only` rather than `role: read-only`. `fra` later becomes unreachable while `iad` is still writable. What is the practical difference?

- None — neither role is promotable, so the two behave identically once you have ruled out promotion
- `dr-only` counts as a core site, so the unreachable `fra` moves the `Degraded` reason off `Healthy`; declared `read-only`, `fra` is excluded from the tallies and its loss leaves the group reading `Healthy`
- `dr-only` makes `fra` promotable as a last resort when no `primary-candidate` site survives, which is why it is the safer choice for a third site
- `read-only` would have made `fra` unreachable-tolerant, because a `read-only` site never appears in `status.sites[]` at all

**Correct option index:** 1

**Explanation:**

The two non-promotable roles differ in visibility, not in promotability. A `dr-only` site is a full participant in the topology view: it is counted in `coreCount` and in the writable, read-only and unreachable tallies, so losing it produces the shape "a site is unreachable while the primary is up" and the `Degraded` reason follows. A `read-only` site is excluded from all of that, so a dead `fra` would leave the group reporting `Healthy` — and it also never taints its node and is refused as a backup source. "Identical" is the common flattening and it will surprise you at 03:00, when a reporting replica takes your alerting off `Healthy`. `dr-only` as a last-resort candidate is the more dangerous misreading: promotability is exactly `role == primary-candidate`, with no fallback, and a planned failover naming a non-candidate is hard-refused. And `read-only` sites do appear in `status.sites[]` with their own state and replication fields; what they are absent from is the tallies that compute the condition reasons. (objectives 6, 8)

## Question 10

**Type:** TRUE_FALSE

`./playground/setup.sh` has finished and every site is working correctly. A `kubectl get pods -l app.kubernetes.io/name=mysql` therefore shows each MySQL pod as `1/1` Ready.

**Correct answer:** false

**Explanation:**

A fully healthy site reads `2/2`, not `1/1`. Every MySQL pod in a failover group runs two containers: `mysqld` itself and the Bloodraven sidecar beside it, which is what lets the site fence its own MySQL with the operator dead, unreachable or crash-looping. A pod sitting at `1/1` on a three-site group is a site whose sidecar is not up — the pod is serving queries while nothing local can enforce read-only on it. `setup.sh` brings up three such pods, one per site, which is also why the playground wants three worker nodes with the third dedicated to the reader. Check the container count as well as the Ready column, then read the status block for authority and replication. (objective 7)
