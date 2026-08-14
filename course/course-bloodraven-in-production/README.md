# Bloodraven in Production

> Stop treating the loss of a database site as an incident. You will hold a site down and watch Bloodraven promote another one in about 12 seconds, move the primary on purpose at an RPO of exactly zero, read the precise count of transactions an emergency failover cost you off the cluster itself, and roll a MySQL upgrade underneath live traffic without an incident.

Most people meet a failover operator and assume the hard part is the promotion. It is not. Promotion is nine SQL statements and takes about twelve seconds — that number is measured, not estimated, across nine recorded runs against a real cluster. The hard part is everything around it: knowing whether the operator will act at all, proving what the failover cost you, and discovering that a textbook-perfect promotion leaves your application reading yesterday's data on one code path and hard-failing on the other, with nothing anywhere paging anyone.

You start by standing up a real three-site failover group on your laptop with an application reading and writing through it, and learning to read its status well enough to name the active site. Then you learn the decision loop — a two-second poll and a small table — until you can predict the operator's next move from a status dump alone. Unit 3 makes it real: you hold a site down, time the promotion, and audit the exact GTID set you lost. Unit 4 is where the database meets the application, and where the wall you hit at the end of Unit 3 gets explained. Unit 5 removes the assumption that anyone can see the truth: sidecars that fence themselves, split brain, five shapes of network partition, and the operator itself going away. Unit 6 adds backups, point-in-time recovery, encryption, alerting and a disaster-recovery drill, and finishes with a go-live checklist you would actually sign. Unit 7 is the two days the rest of the course skips: building a group from nothing, the TLS the CRD makes you have, and upgrading MySQL and the operator underneath live traffic.

You need Docker, kubectl, helm, and a machine that can run a three-worker k3d cluster — about two minutes of setup, no cloud account, no DNS provider. You do not need to know MySQL replication internals, though you will finish able to argue about GTID sets, super_read_only and relay logs with anyone who does. Every number in this course traces to a row on the [sources](SOURCES.md) page: operator source code at v1.0.0, the shipped CRDs, recorded chaos-run forensics, and the MySQL 9.7 manual. Where the official documentation disagrees with the code, this course teaches the code and shows you the discrepancy.

## What you'll be able to do

- **Predict the operator** — Read a failover group's status and say what Bloodraven will do next, and why — before it does it.
- **Audit a failover** — Time a real promotion and prove what it cost in transactions using promotionGtidExecuted and divergentGtid.
- **Explain every fence** — Look at a read-only site and say which of the sidecar's rules put it there, and whether that was correct.
- **Connect an app that survives** — Wire an application through the right Service with a pool that recovers from a promotion instead of serving stale reads.
- **Restore under pressure** — Verify a backup, restore from it, and bootstrap a disaster-recovery group in a second cluster.
- **Run it on call** — Build the alert set, map each alert to a first command, and hold a go-live gate that catches what the defaults miss.
- **Build one, and keep it alive** — Provision a failover group into an empty namespace, wire the TLS the CRD demands, and roll MySQL and the operator underneath live traffic without turning a Tuesday into an incident.

## Syllabus

### Unit 1 — Meet the group

You are on call for a MySQL that has to survive losing an entire site. This unit puts a real three-site group on your laptop, then names the parts you can already see, then explains the bet Bloodraven is making. By the end you can point at any pod and say what role it holds and what it is allowed to become. Then the obvious question: who decides that, and how fast?

**Stand it up and read its status** — Two minutes to a live three-site group with an app reading and writing through it. Then the skill everything else rests on: reading the status and saying what is true right now.

  7. Bring up a three-site group with `./playground/setup.sh` and confirm every site reports Ready
  8. Read a `MysqlFailoverGroup` status and name the active site, each site's state, and the replica's lag
  9. Watch the counter application write through `mysql-playground-primary` and find the same row on the replica

**The moving parts** — An operator that polls, a sidecar that fences, four kinds of Service, and three site roles. Which component can do what — and which one can act when the other is gone.

  4. Trace a write from the application through `mysql-playground-primary` to the pod that currently owns it
  5. Say what the sidecar does that the operator cannot, and why the binlog archiver lives there
  6. Tell `primary-candidate`, `dr-only` and `read-only` sites apart by what each is allowed to become

**The bet Bloodraven makes** — Two sites, asynchronous replication, and an RPO that is deliberately not zero. What that buys, what it costs, and the six things Bloodraven refuses to do.

  1. Say when Bloodraven is the right tool by naming the two-site, non-zero-RPO deployment it targets
  2. Name three things Bloodraven refuses to do, from its own non-goals list
  3. Explain why asynchronous replication with automatic promotion was chosen over Group Replication

*Unit test — Reading a failover group* (10 questions, pass at 70%). Quick check: can you name the active site from a status dump, say which pod the `-primary` Service selects, and explain why a `read-only` site can never be promoted?

*Project — brstatus — the one-screen status reader.* Write a tool that turns a MysqlFailoverGroup status into a one-screen summary and a meaningful exit code, so that from Unit 2 onward you can tell at a glance whether `playground` is healthy — and so you learn, the hard way, that a lagging reader is not an unhealthy group.

### Unit 2 — How the operator decides

Under the failover machinery there is a two-second poll loop and a small table. The loop turns each site into one of four states with a debounce in front of it; the table turns the set of states into exactly one action. This unit teaches you to run that table in your head, then read the same decision back out of the logs and the metrics so you are never guessing what the operator is about to do. Knowing the table is not the same as knowing the timing — Unit 3 holds a site down and puts a clock on it.

**The poll loop and per-site state** — Every two seconds, one `SELECT @@read_only` per site. Four states, two debounce counters that behave asymmetrically on purpose, and an adaptive backoff that quietly rewrites your detection budget.

  1. Walk a site from `unknown` to `writable`, `read-only` or `unreachable` and say what each transition needs
  2. Work out the detection delay from `pollInterval` and `failureThreshold` (2s × 3 = 6s by default)
  3. Explain why `read-only` is entered on a single poll while `writable` needs `recoveryThreshold` successes

**The six-row table that decides everything** — Turn a set of site states into exactly one action. Evaluation order matters, the fence-first return preempts every row below it, and the published table is missing a row you will meet in production.

  4. Turn any set of site states into the operator's action using the cross-site evaluation table
  5. Say why all-read-only and all-unreachable get an alert and no automatic action
  6. Predict what happens when a non-promotable reader turns up writable

**Cooldown, history, and the one exception** — `failoverCooldown` suppresses far less than its name suggests. Two durable copies of `lastFailover`, one deliberate duplication, and the single path that restores writability inside the cooldown window.

  7. Say what `failoverCooldown` suppresses and, more importantly, what it does not
  8. Find `lastFailover` in both places the operator writes it and say why there are two
  9. Name the conditions that let the operator re-assert a fenced promoted primary instead of alerting

**Reading the operator's mind from logs and metrics** — Follow one poll cycle through the structured log, read the three gauges that describe a group, and work a real incident where the dashboard said `writable` for two minutes after the site had already been fenced.

  10. Follow one poll cycle through the structured log using the documented `msg` strings
  11. Read `bloodraven_site_state`, `bloodraven_replication_lag_seconds` and `bloodraven_state_transitions_total` and say what the group is doing
  12. Tell a lagging replica apart from a lagging reader in the same metric

*Unit test — Predicting the operator* (10 questions, pass at 70%). Quick check: can you compute the 6s detection delay from `pollInterval` and `failureThreshold`, name the `Reason` string for a group with one writable and one unreachable site, and say what the anti-flap cooldown will not stop?

*Project — brdecide — the failover predictor.* *(Optional — see the brief for the shorter `jq` route and what skipping it costs you.)* Build a tool that takes a failover-group status plus a clock and prints the decision the operator would take — the action, the alert, the `Reason` string, and whether cooldown will let it run. Check your own mental model against the real table now, on your terms, instead of at 3am while an incident checks it for you.

### Unit 3 — Emergency failover, end to end

You hold a site down and watch the whole sequence run — fence, kill, drain, promote, confirm, record, flip. You time it against a stopwatch rather than against the documentation, then prove exactly what the outage cost you in transactions, and decide what to do with the old primary when it comes back carrying writes nobody else has. It is a textbook-perfect promotion: `pdx` is writable in about twelve seconds and the counter application carries on as if nothing happened. That is the problem. It is still reading from `iad`, the site you just demoted.

**The nine steps of a promotion** — Fence, kill, drain, stop, reset, record, unfence twice, confirm. What the code actually does, which steps are fatal, and why the same failover takes twelve seconds one day and thirty-six the next.

  1. Put a site down hard enough to trigger a failover, and say why a container restart may not be enough
  2. Name the steps of the failover sequence in order and say which ones are fatal on error
  3. Explain the 30-second relay-log drain and when it costs the full budget

**What it cost you** — The RPO contract in one sentence, the two durability settings a tenant can silently switch off, and the GTID arithmetic that turns an outage into an exact number of lost transactions.

  4. State the RPO contract in one sentence and say which settings are guarantees and which are merely defaults
  5. Read `promotionGtidExecuted` and `divergentGtid` to get an exact lost-transaction count
  6. Pick the right row of the per-failure-mode RPO matrix for an outage in front of you

**The old primary comes back** — One GTID containment test decides everything: rejoin as a replica, or sit in `RecoveryBlocked` holding transactions the new primary never saw. Then the reclone interlock, and the only two sanctioned ways out.

  7. Predict whether a returning primary rejoins automatically or lands in `RecoveryBlocked`
  8. Run the reclone interlock with the `divergentGtid` confirmation token
  9. Choose between recloning and replaying the divergent set onto the new primary

*Unit test — Failover, measured* (10 questions, pass at 70%). Quick check: can you name which failover steps abort on error and which only warn, say why the same promotion takes 12 seconds one day and 36 the next, and read two GTID sets to decide whether a returning primary rejoins or blocks?

*Project — The post-failover audit report.* *(Optional — see the brief for the shorter `jq` route and what skipping it costs you.)* Build a tool that turns a post-failover status into an incident record — when it happened, from which site to which, the promotion GTID, the divergent set and its transaction count, and a verdict of RPO 0 or N transactions lost — so that the number you report after an outage is measured rather than assumed.

### Unit 4 — Where failover meets your application

Bloodraven's job ends at a label selector and a DNS record; your application's job starts there. This unit closes the gap Unit 3 opened — the promotion that worked perfectly while the counter app went on serving stale reads from the demoted site and failing every write against it, with nothing paging for either. You will fix a connection pool, move a primary on purpose at an RPO of zero, and decide what your cache and sessions owe you. Everything so far has assumed somebody can see the truth about `playground`; Unit 5 removes that assumption.

**Services, DNS steering, and taints** — The three surfaces Bloodraven actually moves when it promotes: a label selector, an A record, and a node taint. What each one reaches, and where each one stops.

  1. Choose which of the four Service kinds an application should use for writes, for reads, and never
  2. Follow a `DNSEndpoint` CR out to an external-dns record and say what TTL costs you
  3. Explain what the `db-readonly` NoExecute taint evicts and why that is part of failover

**Connection pools that survive a promotion** — Why your application went on reading stale data from the demoted site while its very next write failed outright, why nothing paged for either half, and the three-part fix that is not a bigger pool.

  4. Explain why an open pooled connection keeps serving stale reads after a correct failover
  5. Choose a failover strategy — taint-based, Service-based, or site-local warm standby — for a given application
  6. Fix a pool with bounded connection lifetime, error-class retry, and a read/write split

**Planned failover: moving the primary on purpose** — One annotation, twelve phases, and an RPO of zero by construction. The only path on which Bloodraven drains your application's connections — and the only phase from which it will roll back.

  7. Trigger a planned failover with the annotation and follow the phases through to `Succeeded`
  8. Explain why planned failover is RPO 0 by construction and what the lag gate actually compares
  9. Choose the rollback behaviour for a lag gate that never closes

**Cache and sessions that follow the primary** — Optional Dragonfly moves with the primary on a best-effort budget. What `sessionsPreserved` actually tells you, and why the version floor is a promise rather than a check.

  10. Say what Bloodraven guarantees about Dragonfly and what it explicitly does not
  11. Trace a Dragonfly promotion through `REPLTAKEOVER` and its fallback, and read `sessionsPreserved`
  12. Explain how the active Dragonfly Service sheds an endpoint atomically during a takeover

*Unit test — The application's half of failover* (10 questions, pass at 70%). Quick check: can you say why a pooled connection keeps serving stale reads after a correct promotion, name the one path that actually drains connections, and read a `sessionsPreserved` of nil without guessing?

*Project — Make the writer survive.* Instrument a writer against `playground`, run both a planned and an emergency failover underneath it, and produce a drill record with the measured write-gap for each — so the recovery time you claim for your application is one you observed rather than one you assumed.

### Unit 5 — When the world misbehaves

Everything so far assumed the operator can see the truth. This unit removes that assumption: sidecars fence themselves when they cannot confirm they are the active site, split brain gets a tiered response driven by `sitePriorities`, five documented partition shapes get five different answers, and the operator itself can be gone while `playground` carries on serving. You will learn which of those the system handles for you and which ones hand you a decision at 3am. None of it protects you from losing the whole cluster, the bucket, or last Tuesday — that is Unit 6.

**Self-fencing: the sidecar's two rules** — The sidecar stops its own MySQL writing when it can no longer confirm it is the active site. Two rules, evaluated in a fixed order, plus a startup safety net that is not a rule at all.

  1. Name the two rules the fencing monitor evaluates each tick and the order it evaluates them in
  2. Explain why one reachable peer keeps a primary writable, and why that is not a quorum
  3. Say what the startup safety net does before MySQL is allowed to accept writes

**Split brain, and what sitePriorities really buys you** — Two writable sites is a three-tier problem: prior failover history, a priority list, or nothing but an alert. The field is `sitePriorities`, the published docs name a field that does not exist, and the resolution is a policy that discards writes.

  4. Walk the three tiers of split-brain response and say which one your group is on today
  5. Explain why `sitePriorities` is a policy, not a safety feature, and what it silently discards
  6. Recover a split brain: pick a winner, fence, audit, reclone

**Five partitions, five answers** — Five documented partition shapes, five different operator responses, and an honest account of which ones are actually tested. Plus why the partition you injected may not have partitioned anything.

  7. Match an observed symptom to one of the five documented partition scenarios
  8. Explain why a broken MySQL-to-MySQL link is not a failover
  9. Work the on-call partition checklist without guessing

**When the operator is down** — The operator is not on the request path, so `playground` keeps serving without it. What you lose is the ability to fail over — and a write that returned an error may have landed anyway.

  10. Say what keeps working and what stops when the operator pod is gone
  11. Explain why operator downtime costs write availability but not RPO
  12. Decide whether to wait for the operator or promote by hand

*Unit test — Fencing under uncertainty* (10 questions, pass at 70%). Quick check: can you say which rule fenced a site, which split-brain tier `playground` is on, and why a broken cross-site replication link produces no automatic action?

*Project — Fencing forensics.* *(Optional — see the brief for the shorter `jq` route and what skipping it costs you.)* Build a timeline tool that reads a bundle of operator and sidecar logs from an injected fault and reports every fence, which rule caused it, and whether it was correct — so that a read-only site you did not expect becomes a question you can answer from evidence rather than a guess.

### Unit 6 — Backups, disaster recovery, and going live

A failover group protects you from losing a site. It does not protect you from losing the cluster, the bucket, or last Tuesday. This unit adds backups, PITR, verification, restore and encryption at rest, then the alerting and runbooks that make `playground` operable at 3am, and finishes with a go-live gate you would actually sign. By the end you can hand `playground` to an on-call rotation and state precisely what it will and will not do for them.

**Backups and the binlog archiver** — Where the dump comes from, why a reader can never supply it, and how a sealed binlog gets from a rotate on the primary into object storage — plus the tail that is gone forever.

  1. Choose S3 or PVC storage for `playground` and say what PVC-local costs you
  2. Say which site a backup runs from and why a `read-only` reader is never eligible
  3. Trace a sealed binlog from rotation to object storage, and name what can never be archived

**Verify, and restore** — An unverified backup is a schrödinger backup. Prove it loads, then restore `playground` in place behind a confirmation token that has to be a timestamp.

  4. Run a backup verification and say precisely what a `Succeeded` result proved
  5. Restore `playground` in place using the RFC 3339 confirmation token
  6. Explain why a `pointInTime` request is rejected when PITR is disabled

**Encryption at rest and the keyring lifecycle** — Five phases, one steady state, and the moment etcd becomes part of your key custody. Plus the one site you must never rotate.

  7. Walk a site through the keyring phases and say which phase is the steady state
  8. Explain why enabling encryption at rest makes etcd part of your key custody
  9. Rotate a keyring without rotating the one site that must not be rotated

**Alerts, runbooks, and the 3am path** — The minimum alert set built from metric names that actually exist — and the discrimination that matters most: a lagging reader must never page you.

  10. Build the minimum alert set for `playground` from real metric names and say what each one means at 3am
  11. Map an alert to its runbook and a first command without reading the whole docs site
  12. Write an alert that ignores reader lag on purpose

**Losing a whole cluster, and the go-live gate** — Fence the lost source on two independent signals, bootstrap elsewhere from the bucket, and then decide — on evidence — whether `playground` is fit to go live.

  13. Fence a lost source on two independent signals before bootstrapping a DR group
  14. Bootstrap a disaster-recovery group for `playground` from object storage and cut DNS over to it
  15. Walk a production hardening checklist and say which items you would block a launch on

*Unit test — Ready for the rotation* (10 questions, pass at 70%). Quick check: can you say which site a backup ran from and why, what a `Succeeded` verification did not prove, which site in `playground` cannot have its keyring rotated right now, and why an alert on `bloodraven_replication_lag_seconds` might page you for a reader doing exactly what it was designed to do?

*Project — The go-live pack for playground.* Assemble the artefacts you would actually hand an on-call rotation: a Prometheus rules file built only from metric names the operator really exports, a one-page alert-to-runbook-to-first-command map, and a DR drill record showing you restored `playground` and measured how far back you could reach. Then let a checker prove the thing that matters — that your rules stay silent while a reader soaks past three times `maxLagSeconds`.

### Unit 7 — Day 0 and day 2

Six units started from a group somebody else's script created. This one builds one from an empty namespace — credentials, storage, node labels, and the certificate the CRD made mandatory back in Unit 6 — and then keeps it alive: rolling MySQL and the operator underneath live traffic without turning a Tuesday into an incident. It closes with the one page you actually keep: every default, reason string, promotion step and metric label set on a single screen, beside a dated appendix for the facts that will not stay true.

**Building a group from nothing** — An empty namespace, and every line of the manifest a decision you already know how to defend. Credentials, storage, node labels — and which of your mistakes admission catches.

  1. Write a `MysqlFailoverGroup` into an empty namespace and say, line by line, which earlier unit decided it
  2. Choose between `secretName` and `credentials` and name the users and grants each one produces
  3. Separate the mistakes admission rejects from the ones you discover during an incident

**TLS, and what it unlocks** — The field the CRD demanded in Unit 6 and nobody taught. A SAN list that keeps the sidecar alive, a renewal that moves your primary, and the door it opens to encryption at rest.

  4. Wire `spec.tls` to a cert-manager issuer and design a SAN list every client can verify against
  5. Explain why a certificate renewal rolls your pods and moves your primary
  6. Say what TLS changes about replication, cloning, and the backup path

**Upgrading without an incident** — Three things to upgrade, three mechanisms, one of them automatic. The standby goes first, your primary moves, and Helm will not touch your CRDs.

  7. Roll a MySQL image change and name the ordered-update phase you are in from status
  8. Say why the standby is upgraded first, and what makes the updater abort early
  9. Upgrade the operator without letting Helm silently leave the CRDs behind

**The card you keep** — Every default, reason string, promotion step, Service selector and metric label set on one screen — plus a glossary, and a rule for which facts belong in the dated appendix instead.

  10. Recall any shipped default and its playground override from one page
  11. Name the five condition reasons, the nine promotion steps with fatality, and the four Service kinds without looking them up
  12. Use the version appendix to decide whether a fact you remember is still true

*Unit test — Day 0 and day 2* (10 questions, pass at 70%). Quick check: can you build a group from an empty namespace, design a certificate every client can verify against, and roll a MySQL upgrade without turning it into an incident?

*Project — brprep — the day-0 pre-flight and the change plan.* Write a shell pre-flight that reads a MysqlFailoverGroup manifest and reports what the API server will reject, what it will silently admit, and what an ordered update of it would actually do — so the first time anyone sees your production group's problems is before it is applied, not during an incident.

## FAQ

**What does this give me that the Bloodraven documentation doesn't?**

The docs are the best reference in this field, and that is the honest comparison: their failure-mode matrix is the only one anywhere with all four columns — failure, signal, action, time to act, and what the operator will not do. Oracle's MySQL operator manual has no failover chapter at all. Vitess's reparenting page never mentions data loss. What reference material cannot do is make you produce the failure. The docs tell you the RPO; they never have you commit writes, kill the primary mid-flight, and count what did not survive. Their own divergent-recovery runbook opens with "investigate the lost transactions" and gives you no command that touches MySQL — and the next step runs `CLONE INSTANCE`, which destroys the evidence. This course fills in that step. It also teaches from the source code rather than the pages, which is how it found roughly a dozen places where the two disagree; you will be shown each one, because the day you need this knowledge is the day a stale page costs you an hour.

**Can my organisation actually use Bloodraven? What is it licensed under?**

Settle this before you plan a dependency, because it is not a technical question — and check it in the repository rather than here, because it is the fastest-moving fact in the course. The short version: Bloodraven is source-available, not open source. The full source is public and you may read, build and modify it; running it in production at a commercial company is a licensing question with a real answer. v1.0.0 ships the Business Source License 1.1: source-available, not OSI open source. Production use by a company over $1M annual revenue needs a commercial license; everything else in the additional-use grant stays free. Because terms can still move, they are recorded once — with the two commands that settle them (ls LICENSE*, and gh repo view ShipStream/bloodraven --json licenseInfo) — in section D of the version appendix on the sources page, rather than repeated through the units. This course is separately licensed; the notice is in every page footer.

**Do I need a cloud account or a production cluster?**

No. Everything runs on a local k3d cluster with three worker nodes — about two minutes of setup with Docker, kubectl and helm. You get a real three-site MySQL failover group, a live dashboard, an application writing through the primary Service, and a simulated external-dns pipeline. No cloud account, no DNS provider, no production infrastructure.

**How much MySQL replication do I need to know already?**

Enough to know that replicas follow primaries. You do not need to have configured GTID replication, and you will not be asked to memorise CRD fields. You will finish able to read a GTID set, explain why super_read_only is the fence and read_only is not, say what a relay log is and why draining one bounds a promotion, and use GTID_SUBTRACT to compute exactly what a failover lost.

**Will this teach me every configuration field?**

Deliberately not. Field shapes are what the CRD reference is for, and memorising them is the fastest way to learn nothing durable. This course teaches which class of knob to reach for and what it actually controls — including the several cases where a field does not mean what its name implies. maxLagSeconds is an alerting threshold and not a promotion gate. connectionDrainTimeout bounds how long the operator keeps trying, not how long your pool holds a socket. Knowing that is worth more than knowing forty defaults.

**Is this a course about writing Kubernetes operators?**

No. It is a course about running this one. There is no controller-runtime, no reconciler-writing, and no Go beyond reading the occasional line to settle an argument about what actually happens. The audience is the person who gets paged, not the person who ships the operator.

**We already rehearse failover, and our chaos suite is green. What is left?**

Two things, and they are the same thing seen twice. First, a green suite proves the happy path: Bloodraven ships 51 real-cluster chaos scenarios, two of which touch data integrity, and both assert that nothing was lost. That is the correct thing for CI to assert and it means CI has never shown you the sad path. Second, almost every rehearsal in this field — here and everywhere else — injects a process kill: delete the pod, kill the sandbox instance, power off the node. That is precisely the failure mode where fencing works best, because the node stops and everyone agrees it stopped. GitHub ran a deliberate promotion in February 2020 specifically to give their teams visibility, and it recreated the outage, because a silently clamped file-descriptor limit had never been exercised under load. Rehearsing the happy path is not rehearsing the failover.

**Can Bloodraven give me zero data loss?**

On a planned switchover, yes, by construction — the target is only promoted once its GTID set provably contains the fenced source's, so there is nothing left to lose. On an unplanned failover, no, and no asynchronous replication system can. What Bloodraven does instead is measure the loss exactly and hand you the GTID set and a transaction count. This course teaches you to read that number rather than to hope for zero, which is the difference between an RPO you can state in a meeting and one you are guessing at.

**How is my progress tracked, and who grades the quizzes?**

You do, and only this browser knows. The whole site is static: there is no account, no server, and nothing leaves the machine you are reading on. Progress, quiz scores, flashcard state and the certificate all live in a single key in this browser's own local storage — clear the site data, switch browsers or open it in a private window and you start from zero, with no way to recover it. Multiple-choice and true/false questions are marked automatically against the answer key shipped in the page. Short-answer questions are **self-graded**: you write an answer, then reveal a sample answer and a note saying what a full-credit response has to cover, and you decide. The certificate is a self-reported record of that self-assessment, verified by nobody. Treat it as a study aid, not a credential — and if you want an audit trail, print or export it, because nothing else is keeping one.

**How much programming is this, and do I have to do every project?**

Less than the project list suggests, and no. Four of the seven projects are the spine and are worth doing in order: brstatus (Unit 1) because status literacy is what every later unit reads, the failover drill (Unit 4) because your own application's write-gap is the only recovery number that means anything, the go-live pack (Unit 6) because it is the artefact you hand a rotation, and brprep (Unit 7), which is bash and jq with no Python at all. The other three — brdecide, the post-failover audit, and fencing forensics — are marked optional in their briefs: each is a deeper drill on a mechanism the reading already covers, and each brief opens by saying what you give up by skipping it. Two of them also ship a shorter route: brstatus and the audit report both have a 'without Python' section that does the same job with kubectl, jq and one MySQL function, which is closer to what you would actually type during an incident anyway. The Python that remains is standard-library only — no frameworks, no cluster at grading time, and every project runs against JSON fixtures so nothing depends on your laptop.
