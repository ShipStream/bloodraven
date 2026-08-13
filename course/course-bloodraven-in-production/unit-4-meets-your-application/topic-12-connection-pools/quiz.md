# Quiz — Connection pools that survive a promotion

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** TRUE_FALSE

During a failover drill on `playground`, your application's `SELECT` queries keep returning rows the whole time. A colleague concludes the application must already be talking to the newly promoted `pdx`. Is that conclusion sound?

**Correct answer:** false

**Explanation:**

The reversal: a successful `SELECT` proves the socket is open and the server is alive, and proves nothing about which site is on the other end. `super_read_only` blocks writes but closes no sockets, and a Service selector flip only routes new connections — so a pooled connection opened before the promotion still reaches the demoted site and still answers reads, with data that is stale by however far that site has drifted. The only cheap way to know which end you are on is to ask the connection directly, which is why the counter app reads `@@hostname` and `@@global.read_only` on the same connection it just used. (objective 4)

## Question 2

**Type:** MULTIPLE_CHOICE

Your pool runs `SELECT 1` as its validation query and it never flagged a single connection during the promotion. What would the check have to do differently to catch a demoted primary?

- Run the validation query more often, so the stale connection is sampled sooner
- Ask the connection whether it is writable — `@@global.read_only`, or an actual write — instead of only whether it answers
- Enable TCP keepalive so a dead peer is detected at the socket layer
- Raise the pool size so healthy connections outnumber the stale ones

**Correct option index:** 1

**Explanation:**

A demoted primary is fully alive: it accepts connections, parses SQL and returns rows, so any read-only probe passes no matter how often you run it — that is why the HikariCP issue exists and why drivers grew `rejectReadOnly`. Running it more often samples the same passing check faster. TCP keepalive detects a *dead* peer; this peer is healthy, which is the whole problem. Raising the pool size adds more connections established the same way, all pinned to the same backend. Only a check that tests writability distinguishes primary from demoted primary. (objectives 4, 6)

## Question 3

**Type:** SHORT_ANSWER

A network partition isolates `iad` from the operator while your application, which sits in the same site, keeps reaching `iad`'s mysqld normally. The operator promotes `pdx`. Describe what `KillAppConnections` does here, and what that means for the application.

**Sample answer:**

It does not run at all. `KillAppConnections` is step 2 of the failover sequence and executes against the old primary only when the operator has a working handle to it; under a partition the operator cannot reach `iad`, so the kill pass is skipped, exactly as it was when the site was held down in Unit 3. Even when it does run it is one best-effort pass, never retried, and it spares only its own session and the binlog dump threads. So the application's established connections to `iad` survive intact. They keep serving reads that succeed and are stale, until `iad` is next promoted or demoted. Nothing alerts on it — `BloodravenFailoverOccurred` only reports that a promotion happened.

**A full-credit answer shows:**

A strong answer covers: (1) it does not run, because the old primary must be reachable from the operator; (2) it is best-effort during the sequence and, after promotion, retried once per poll inside `spec.connectionDrainTimeout` — but every one of those passes is a SQL statement against the site being drained, so an unreachable site gets none of them; (3) the consequence — established application connections survive and serve stale reads until the next promotion or demotion; (4) bonus for noting no alert fires, or that only planned failover truly drains. An answer that says the kill 'fails' or 'will retry later' has missed the point: the limit is reachability, not the retry count.

**Explanation:**

The mitigation exists and is real, but it is precisely absent in the failure modes that produce this symptom — a held-down site or a partition. Both leave the old primary unreachable to the operator while it remains perfectly reachable to an application co-located with it. The retry window the operator does have is bounded by `spec.connectionDrainTimeout`, and every attempt in it is a statement issued over a connection to the site being drained — so the bound that matters is reachability, not the budget. (objective 4)

## Question 4

**Type:** MULTIPLE_CHOICE

You want to shorten the window in which your application can read stale data from a demoted site. Which change actually shortens it?

- Raise the pool size so more connections are available after the promotion
- Set `reconnect=true` so the driver re-establishes broken connections
- Bound the connection lifetime so every socket is retired and reopened
- Shorten `spec.dns.ttl` from 60 to 10 so the record is re-resolved sooner

**Correct option index:** 2

**Explanation:**

Only a bounded lifetime forces an established socket to close, and the ceiling on staleness becomes the lifetime you chose. Raising the pool size makes it worse: more connections, all established the same way, all pinned to the same backend, held longer. `reconnect=true` acts after something has already broken — a stale read succeeds, so the reconnect path is never entered. A shorter DNS TTL governs the next lookup, not an open socket, and against a runtime that caches DNS for the process lifetime it may govern nothing at all. (objectives 4, 6)

## Question 5

**Type:** MULTIPLE_CHOICE

A batch worker runs in the same site as MySQL, is stateless, and restarts in under two seconds. Which failover strategy fits it best?

- Taint-based: let the `NoExecute` taint on the demoted site evict the pod, and take the fresh pool the restart gives you
- Service-based: leave the pod running and rely on bounded lifetime plus error-class retry
- Site-local warm standby: run an instance at every site and let only the co-located one take writes
- Point the worker at the `-replicas` Service so it is never affected by a promotion

**Correct option index:** 0

**Explanation:**

Co-located plus cheap to restart is the exact case the taint is for: the `shipstream.io/db-readonly-<group>` `NoExecute` taint evicts a non-tolerating pod from the demoted site, and the restart guarantees a fresh pool with no configuration to get wrong. Service-based is the right answer for the opposite application — long-running or expensive to restart — and here it buys complexity for nothing. Warm standby addresses cross-site write latency, which a co-located worker does not have. Sending it to `-replicas` is not a strategy but a category error: `-replicas` never serves writes, so a worker that writes cannot use it. (objective 5)
