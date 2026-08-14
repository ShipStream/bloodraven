# Unit 4 test — The application's half of failover

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Assesses:** Quick check: can you say why a pooled connection keeps serving stale reads after a correct promotion, name the one path that actually drains connections, and read a `sessionsPreserved` of nil without guessing?

**Passing score:** 70%

## Question 1

**Type:** MULTIPLE_CHOICE

A new export job is being deployed against the three-site `playground` group (iad, pdx, reader). It writes a small audit row per run and then reads several million rows back out. A colleague has already put `mysql-playground-reader-internal` in the read config because "it always has an endpoint, even when the pod is still starting". Which binding is right?

- Writes to `mysql-playground-primary`, reads to `mysql-playground-reader-internal` — pinning reads to the reader site's own Service is the only way to keep the export job off the primary while the group is degraded.
- Writes to `mysql-playground-primary`, reads to `mysql-playground-replicas` — the internal per-site Service exists for sidecar and peer traffic, and its `publishNotReadyAddresses: true` is exactly why it will hand you a pod that is not serving yet.
- Writes to `mysql-playground-iad`, the per-site Service of the site that is currently writable, reads to `mysql-playground-replicas` — naming the site makes the write path unambiguous.
- Both through `mysql-playground-primary` — `mysql-playground-replicas` can select nothing at all when the reader's eligibility conjuncts fail, so the write endpoint is the only one safe to depend on.

**Correct option index:** 1

**Explanation:**

Applications get two of the four Service kinds: `-primary` for writes, `-replicas` for reads. The internal per-site Service is never yours — it publishes the sidecar port, carries the canonical replication source host, and sets `publishNotReadyAddresses: true` so peers can reach a pod that is not serving yet. Your colleague's reason for choosing it is precisely the reason not to. Option 1 inverts that guarantee into a feature. Option 3 pins writes to a site: `mysql-playground-iad` selects iad whatever iad's role is, so the day pdx is promoted the job writes at a read-only site and gets ERROR 1290 — the `-primary` selector moves, a per-site selector never does. Option 4 misreads endpoint shedding. A `-replicas` Service that selects nothing is the design working: five conjuncts must all hold before a reader appears behind it, and a refused connection is a failure your retry logic can see, where a successful read of yesterday's data is not. Concentrating a multi-million-row scan on the primary to dodge that is trading a visible failure for an invisible one. (objective 1)

## Question 2

**Type:** MULTIPLE_CHOICE

pdx has been promoted. `kubectl get dnsendpoint bloodraven-playground -o yaml` shows `recordType: A`, `recordTTL: 60`, and `targets` already holding pdx's IP; the operator logged `failover complete` fifty seconds ago. Clients resolving the hostname are still being sent to iad. Which reading of the situation is correct?

- `recordTTL` is the cadence at which Bloodraven re-applies the record, so lowering `spec.dns.ttl` would have made the operator publish the new target sooner.
- The record moves only on a create-or-update transition, so an apply that was rejected once stays stale until somebody re-applies it by hand.
- Bloodraven has already finished: the desired target is re-derived from live topology and server-side applied on every poll. The TTL is the floor on how long a cached stale answer can survive, and everything past the CR — external-dns, your resolver, your client's cache — is outside the operator's reach.
- Lowering `spec.dns.ttl` from 60 to 10 would also have shortened the window in which the application's existing connections kept talking to iad.

**Correct option index:** 2

**Explanation:**

Writing the `DNSEndpoint` is where Bloodraven's authority ends. The operator cannot accelerate DNS propagation; chaos scenario 38 makes the point by denying the operator write verbs on `dnsendpoints` and watching the CR promote perfectly while the record stays stale. Option 1 confuses the record's TTL with the operator's write cadence — there is no such cadence to tune, because `reconcileDNS` runs on every poll regardless of the TTL. Option 2 assumes a create/update split that does not exist: the write is one idempotent server-side apply with forced field ownership, and because nothing is memoized, a rejected apply self-heals on a later poll with no human involved. Option 4 is the one that will actually cost you an incident — a TTL governs the *next* lookup. An established socket never performs one, so `spec.dns.ttl` (60 shipped, 10 in the playground) does nothing at all for the connections already open against the demoted site. (objective 2)

## Question 3

**Type:** MULTIPLE_CHOICE

After promoting pdx, Bloodraven taints iad's nodes with `shipstream.io/db-readonly-playground=true:NoExecute`. On one of those nodes sit a batch worker with no tolerations and a metrics agent that tolerates the key with `tolerationSeconds: 600`. The `reader` site's MySQL pod sits on a third node. What happens?

- The batch worker is evicted immediately. The metrics agent stays bound for its 600 s, after which the node lifecycle controller evicts it. The reader's node is never tainted at all — a `role: read-only` site is already read-only, so there is nothing to demote.
- All three go immediately: `NoExecute` evicts every pod on a tainted node, and `tolerationSeconds` only has meaning under `NoSchedule`.
- Nothing already running is touched. The taint stops new pods being scheduled onto iad's nodes; the existing ones leave only when you cordon and drain.
- The batch worker is evicted, the metrics agent runs indefinitely because a toleration on the key is unbounded, and the reader's node is tainted too since it belongs to the same failover group.

**Correct option index:** 0

**Explanation:**

`NoExecute` is upstream Kubernetes behaviour and it reaches pods that are already running: non-tolerating pods are evicted immediately, and a toleration carrying `tolerationSeconds` buys exactly that much time before the node lifecycle controller evicts anyway. That is why the taint belongs to failover — an application pod pinned to the demoted site is removed rather than left pointing at a read-only MySQL, and whatever replaces it starts with a fresh pool. Chaos scenario 21 verifies the chain end to end: a non-tolerating canary on the old primary's node is evicted while a canary tolerating the same taint stays Running. Option 2 discards the toleration entirely; a toleration that matches is honoured, and `tolerationSeconds` is a `NoExecute` mechanism, not a `NoSchedule` one. Option 3 is `NoSchedule` semantics, which is the wrong effect — and it would defeat the whole purpose, since the point is to remove pods that are already there. Option 4 gets the deadline backwards (a `tolerationSeconds` toleration is bounded, not unbounded) and taints a reader, which never happens: readers never taint. (objective 3)

## Question 4

**Type:** TRUE_FALSE

During a failover drill the counter app returns HTTP 200 with `value: 41041`, `readOnly: true` and `dbHost` still naming iad. A colleague argues: the operator had already run `SET GLOBAL super_read_only = ON` on iad by then, so that response must have come from a request that reached iad before the fence — a read issued after it would have failed on the fenced socket.

**Correct answer:** false

**Explanation:**

False, and the response is proof of the opposite. Fencing is a variable change, not a disconnection: `super_read_only` blocks writes and closes no sockets, so a session that existed before the fence keeps serving reads until the site is next promoted or demoted. That is exactly what the counter app's three fields show you on one connection — the read succeeded, `readOnly` is `true`, and `dbHost` still names the demoted site. The reads are not errors and they are not old responses; they are fresh reads of data that is wrong by however far iad has drifted since the promotion. MySQL's own manual is blunt about what the statement does — it blocks while other clients have an ongoing statement or commit and waits for them; it never cuts a writer off. The first thing that actually fails is a write, with ERROR 1290 or 1792, which is why a pool validation query passed the whole time. (objective 4)

## Question 5

**Type:** MULTIPLE_CHOICE

The counter app's pods run on the cluster's general application nodes, not on iad's or pdx's database nodes. Twelve seconds after the kill, pdx is writable, the `-primary` selector has moved, the `DNSEndpoint` target has moved, and iad's nodes are tainted. The app is still reading from iad on the sockets it opened before the kill. Which of Bloodraven's surfaces ended those sessions?

- The `-primary` selector flip removed iad from the Service's endpoint list, and kube-proxy resets flows whose backend it no longer has.
- Fencing did it — `SET GLOBAL super_read_only = ON` terminates client sessions on the demoted site, so the pool reopened through `-primary`.
- DNS did it — the playground runs `spec.dns.ttl` at 10, so the pool re-resolved the hostname and landed on pdx within about ten seconds.
- None of them. A selector flip routes new connections only, a DNS record governs only the next lookup, and the `NoExecute` taint is the one surface that removes a running process — and it reaches only pods on the demoted site's nodes, which these were not.

**Correct option index:** 3

**Explanation:**

Every mechanism fired correctly and every one of them missed, which is the whole shape of this failure. kube-proxy picks a backend at connection establishment and does not re-evaluate or reset an established flow, so option 1 describes something kube-proxy does not do — relabelling changes routing for new connections only. Option 2 is the same error the previous question turns on: fencing closes no sockets. Option 3 fails on a socket that never performs another lookup, and against a caching runtime — the JVM's default DNS cache can be infinite for the process lifetime — a short TTL may govern nothing at all. That leaves the taint, and the taint has a node boundary: it evicts pods on the demoted site's nodes, and these pods were never there. Note what the correct answer does *not* say. Bloodraven does have one mitigation, `KillAppConnections` at step 2 plus post-promotion retries bounded by `spec.connectionDrainTimeout`, but every pass needs a working handle to the old primary. In this question the site is down, so the drain is a no-op. Issue #123 is closed in 1.0.0; the remaining gap is an unreachable old primary, and an autonomous sidecar self-fence. (objectives 3, 4)

## Question 6

**Type:** MULTIPLE_CHOICE

A long-running JVM service talks to `playground`. It runs in a third site with no MySQL in it, and restarting it is expensive — a cold instance takes a long warm-up before it serves at rate. Cross-site write latency is comfortably within budget. Which approach gets it across a primary move?

- Taint-based: drop the service's toleration for `shipstream.io/db-readonly-playground` so the `NoExecute` taint evicts it at a promotion and it returns with a fresh pool.
- Service-based: keep it up, bound the pool's connection lifetime, retry specifically on the read-only write errors 1290 and 1792, and split reads to `mysql-playground-replicas` while writes resolve through `mysql-playground-primary`.
- Site-local warm standby: run an instance at every site and let only the one co-located with the current primary take writes.
- Service-based, tuned for the failure: raise the pool size so a promotion has spare connections, set `reconnect=true`, and drop `spec.dns.ttl` to 10.

**Correct option index:** 1

**Explanation:**

The selection rule is about where the app runs and what a restart costs. Service-based fits: the app stays up, and bounded lifetime plus error-class retry plus a read/write split carries it across. All three parts are needed and none works alone — lifetime bounds how long a connection can outlive a promotion, error-class retry replays only the read-only write failures rather than blanket-retrying a statement that failed for some other reason, and the split keeps reads off the write endpoint. Option 1 cannot work at all: the taint is applied to the demoted *database* site's nodes, and this service is not on them, so nothing would evict it — and even if it were co-located, restarts are the expensive thing here. Option 3 solves a problem this application does not have; warm standby is for when cross-site write latency is the binding constraint, and it leaves the pool unfixed anyway. Option 4 is the three classic non-fixes: a bigger pool means more connections pinned to the demoted site held longer, `reconnect=true` only acts after something has already broken and a stale read does not break, and a TTL never touches an established socket. These are architecture choices, not Bloodraven features — the operator moves labels, records and taints, and the strategy is yours. (objectives 5, 6)

## Question 7

**Type:** SHORT_ANSWER

You annotate `playground` with `bloodraven.shipstream.io/planned-failover=pdx`. `status.plannedFailover.phase` walks Pending, Validating, Draining, and then sits in `WaitingForLag` for four minutes; pdx is genuinely behind because of a long batch job. Explain what the operator did at `Draining`, what it is comparing in `WaitingForLag`, why `transactionsLost: 0` is a construction rather than a measurement, and what you should do about the stuck gate.

**Sample answer:**

At `Draining` the operator set `super_read_only = ON` on iad, then read iad's `GTID_EXECUTED` and recorded it in `status.plannedFailover.sourceGtidAtFence`. Fence first, snapshot second. It also stripped iad's primary role label to `fenced`, so the pod matches neither the `-primary` nor the `-replicas` selector and new connections stop arriving, and it repeatedly killed the connections already open under a 30 s `drainTimeout`. In `WaitingForLag` it polls pdx's `GTID_EXECUTED` and asks one question: does that set *contain* `sourceGtidAtFence`? It is a genuine GTID-set superset test, not a lag-seconds heuristic — `spec.replication.maxLagSeconds` drives only the `ReplicationLagging` Degraded condition and is never consulted here, and `Seconds_Behind_Source` would be a bad gate anyway because it compares last-executed against last-downloaded and reads 0 when the IO thread has stalled. `transactionsLost: 0` follows from the ordering: because the source was fenced before the snapshot was taken, the set pdx must catch up to cannot grow underneath the gate, so promotion after a true superset result cannot leave anything behind. The field exists for symmetry with the emergency path's accounting, not because a clean switchover can produce a number. For the stuck gate: let it fail. `maxLagWait` defaults to 5m and `WaitingForLag` is the only phase from which rollback unfences the source — a `LagTimeout` puts iad back writable with nothing lost but the fenced window. Raising `maxLagWait` does not make pdx catch up any faster; it only lengthens the interval in which the source is fenced and writes are refused. Fix the lag first, then re-annotate.

**A full-credit answer shows:**

A strong answer covers: (1) Draining = `super_read_only = ON` on the source, then the `GTID_EXECUTED` snapshot into `sourceGtidAtFence`, in that order, plus the role label stripped to `fenced` and connections killed under `drainTimeout` 30s. (2) WaitingForLag compares GTID sets — a superset/contains test on `sourceGtidAtFence` — explicitly not seconds, and states that `maxLagSeconds` is a Degraded condition only and is never a promotion gate. (3) The RPO 0 argument is the fence-before-snapshot ordering: a fenced primary accepts no new client writes so the target set cannot grow underneath the gate. (4) The right response to a stuck gate is to let `maxLagWait` expire and the rollback fire, because `WaitingForLag` is the only phase that unfences the source; raising `maxLagWait` only extends the window in which the source is fenced and writes refused. Credit for naming the annotation key, the 5m/30s defaults, or noting that failures in `Promoting` or `Resuming` leave the source fenced for a human. Do not credit an answer that treats the gate as a lag threshold, or that recommends raising `maxLagWait` to force the switchover through.

**Explanation:**

Planned failover is RPO 0 by construction, and the construction is an ordering, not luck and not a threshold: fence the source, snapshot its `GTID_EXECUTED` at that fence, promote only once the target's set contains that snapshot. The common error is to read `WaitingForLag` as a seconds-based wait and reach for `maxLagSeconds` or `Seconds_Behind_Source`, neither of which the gate consults. The second common error is operational: raising `maxLagWait` to force a stuck gate closed. Rollback lives in exactly one phase — `LagTimeout` and `InvalidGTID` in `WaitingForLag` unfence the source and cost you nothing but the fenced window, while a failure in `Promoting` or `Resuming` stamps `Failed` without unfencing and hands you a fenced primary and a pager. The lag gate that never closes is the good failure. (objectives 7, 8, 9)

## Question 8

**Type:** MULTIPLE_CHOICE

A planned failover of `playground` from iad to pdx logs `drain budget exhausted after 30s with 3 connection(s) remaining on "iad"; proceeding`, then reaches `Succeeded` with `transactionsLost: 0`. The application's pool sets a maximum connection lifetime of ten minutes. What is the situation, and what do you change?

- The drain is a barrier: the operator keeps killing until iad is clear, so the log line is a report and the pool's lifetime has no bearing on it.
- Those three connections were already dead — stamping iad's pod `fenced` dropped it out of the `-primary` Service, and an endpoint removal tears down the flows behind it.
- The switchover carried on regardless. Those three sockets are still open against a fenced iad, will pass any validation query, and will serve stale reads until the pool retires them — so the pool's maximum connection lifetime has to sit inside the drain budget rather than twenty times outside it.
- Raise `drainTimeout` to ten minutes so it always exceeds the pool's connection lifetime and the drain can finish properly.

**Correct option index:** 2

**Explanation:**

`drainTimeout` is a deadline, not a barrier: when the budget runs out with connections still open, the operator logs that it is proceeding and proceeds, because a stuck client may not block a switchover indefinitely. That is a defensible choice and it is one you inherit — the connections that outlive the drain are exactly the stale-read connections from the pool topic, open against a demoted primary, passing every validation query until something tries to write. Your `drainTimeout` and your pool's maximum connection lifetime are two halves of one setting, and a ten-minute lifetime against a thirty-second drain makes the drain decoration. Option 1 reads the log line backwards; the word in it is `proceeding`. Option 2 repeats the endpoint-removal misconception: dropping out of a Service selector stops new connections arriving and does nothing to established ones — that is why the drain exists at all. Option 4 is the wrong lever on the wrong side: the drain budget is time during which the source is fenced and writes are refused, so extending it to ten minutes buys a ten-minute write outage to accommodate a pool you could have fixed instead, and a genuinely wedged client would still outlast it. (objectives 6, 7)

## Question 9

**Type:** SHORT_ANSWER

iad's MySQL primary dies. pdx is promoted 12.0 s later. Afterwards the counter app's Dragonfly-backed session counter is back at 0, and `status.plannedFailover.dragonfly.sessionsPreserved` is not present in the YAML at all. What happened to the cache, what does the absent field tell you, and what does any of it mean for the MySQL promotion?

**Sample answer:**

This was an emergency promotion, so the Dragonfly path tried `REPLTAKEOVER` on the target first and, on failure, fell back to `REPLICAOF NO ONE` — which promotes the target as an empty master and loses sessions, logged in those words: `dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)`. The session counter resetting to 0 is the visible half of that; the log line and the event are where the outcome is actually recorded. The absent `sessionsPreserved` tells you nothing about it. That field is a `*bool` on the *planned*-failover status and it is tri-state: true means sessions survived, false means they were lost, and nil means unknown — not false. The emergency path never writes it, so absence here is expected and is not evidence either way. As for MySQL: none of it mattered. The emergency Dragonfly attempt runs after MySQL has already been promoted, inside a hard-coded 10 s budget, and by explicit design it never returns an error to its caller, never blocks longer than that budget, and never leaves Dragonfly in a state that affects MySQL durability. A wedged or unreachable Dragonfly cannot extend the 12.0 s promotion. Dragonfly here is cache and session state, never durable application data — if losing it costs you a transaction, it was in the wrong place.

**A full-credit answer shows:**

A strong answer covers: (1) the emergency path tries `REPLTAKEOVER` and falls back to `REPLICAOF NO ONE`, promoting an empty master and losing sessions, and says where that outcome is recorded (the log line / event), not in status; (2) `sessionsPreserved` is tri-state and nil means unknown, not false — and it lives on the planned-failover status, which an emergency promotion never writes, so its absence here proves nothing; (3) the cache never delays or endangers MySQL — the emergency attempt runs after MySQL has promoted, under a hard-coded 10 s budget, and never affects MySQL durability. Credit for stating the scope boundary (cache and session state, never durable data) or for noting that the planned path has no `REPLICAOF NO ONE` fallback because a planned move can simply be retried. Do not credit an answer that reads the absent field as `false`, or that claims the Dragonfly outcome could have delayed or compromised the MySQL promotion.

**Explanation:**

Two misreadings are being separated here. The first is treating nil as false: a learner who does that chases incidents that did not happen, and one who reads nil as success misses ones that did — nil is what you get when the field was never written, which is always the case on the emergency path. The second is assuming the cache is on the MySQL critical path. It is not, and the asymmetry between the two promotion paths follows from the same boundary: an emergency promotion cannot be retried, so it takes the empty master and says so in the log, while a planned switchover can be retried and therefore refuses to buy availability with sessions — the planned path has no `REPLICAOF NO ONE` fallback at all, and `onSyncTimeout` (`proceed` by default, with `maxSyncWait` at 30 s plus 5 s of client I/O grace) decides what it does instead. (objectives 10, 11)

## Question 10

**Type:** TRUE_FALSE

While Bloodraven moves the Dragonfly master, both the old and the new instance briefly satisfy the active `playground-dragonfly` Service selector, so for a moment session writes can be load-balanced across two masters.

**Correct answer:** false

**Explanation:**

False — and the mechanism that prevents it is deliberate. The active Service AND-gates two labels the operator stamps on Dragonfly pods, `shipstream.io/dragonfly-role=master` and `shipstream.io/dragonfly-traffic=enabled`, and a pod is an endpoint only when both match. To shed an endpoint the operator *deletes* the traffic key rather than stamping some disabled value, because the selector is an exists-and-equals check on `enabled` and an absent key cannot match. The takeover strips the source's traffic key before it promotes the target and stamps the target's key only afterwards, so there is no instant at which both carry it; the steady-state label sweep honours the same window by skipping the source mid-takeover, since re-stamping it would re-attach the old master to the active Service — exactly the bug the strip prevents. Had the operator written `dragonfly-traffic=disabled` instead, correctness would depend on the selector and the writer agreeing about a magic string. Deletion needs no agreement. (objective 12)
