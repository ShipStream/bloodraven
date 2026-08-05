# Deterministic Simulation Testing (DST)

This package runs the **real** failover control loop — `TopologyManager.Poll`,
`FailoverController.Execute`, old-primary recovery, source convergence, DNS
reconcile, primary re-assert — against an in-memory model of a multi-site
MySQL cluster, under seeded random fault schedules. It is the fast,
reproducible complement to the playground chaos suite: a trial takes
milliseconds instead of minutes, and every failure replays exactly from its
seed.

## Running

```sh
make test            # includes the quick sweep (TestDST_Quick, ~400 trials)
make dst             # full saturation campaign; report → playground/chaos-results/dst-report.txt
DST_SEED=61 make dst-repro              # replay one trial with events + operator logs
DST_SEED=61 DST_SKIP=1,3 make dst-repro # replay with schedule ops 1 and 3 masked out
```

Campaign knobs (env): `DST_BATCH` (default 1000), `DST_DRY_BATCHES` (default
3), `DST_MAX_TRIALS` (500000), `DST_WALL_SECONDS` (1800), `DST_REPORT`.

The campaign stops at **coverage saturation**: a batch is "dry" when it
produces no new failure fingerprint and fewer than 1% of its trials yield a
new behavior signature; after `DST_DRY_BATCHES` consecutive dry batches, more
trials are diminishing returns. Failures are deduplicated by the set of
violated invariants (see the fingerprint table under Known finding classes)
and shrunk to a minimal fault schedule.

Every actor added to the model widens the signature space, so keep an eye on
the stop reason: `wall-clock cap` rather than `saturated` means the run was
cut off mid-exploration, and either `DST_WALL_SECONDS` needs raising or a
newly added signature dimension is too fine-grained to ever go dry. The
signature is deliberately coarse for exactly this reason — fault-injection
events are excluded entirely, and mid-`Execute` deaths collapse to one
dimension rather than one per statement.

## How a trial works

1. `GenerateTrial(seed)` derives everything from the seed: 2 or 3 sites and
   their roles, split-brain priorities, anti-flap cooldown, whether failover
   history is rehydrated, the sidecar deployment shape (lease timeout,
   topology-aware fencing on/off, whether its tick lands before or after the
   operator's poll), and an explicit fault-op schedule (crashes with
   fenced/writable boot modes, operator↔site and site↔site partitions,
   operator restarts, mid-`Execute` operator deaths, ambiguous mutations —
   applied but the operator saw an error — failing mutations, replication
   fetch/apply stalls, relay-drain stalls, rogue fences, rogue `read_only=0`,
   CR-status write outages, out-of-band anti-flap store outages, DNS
   outages). Fault kinds are drawn from the `faultWeights` table, whose
   weights must sum to 100 (checked at `init`). Execution consumes no
   randomness after generation.
2. The harness builds a production `TopologyManager` via
   `NewTopologyManagerWithClock` with a `FakeClock`, the simulated
   `mysql.Checker`s, DNS updater/reader, tainter, and out-of-band anti-flap
   store. `StatusCallback` snapshots act as the simulated CR status
   subresource and `simFailoverStore` as the annotation store; an
   operator-restart fault rebuilds the manager and rehydrates from whichever
   durable copy is newer, exactly the way `runner.go` does.
3. Every site also runs a **real `sidecar.FencingMonitor`** (`sidecar.go`)
   on the same fake clock — the real `Check` → `evaluate` → `doFence` path,
   the real `TopologyCache` including peer relay, and the operator's
   `/healthz` + `/active-site` and the peers' `/peer/ping` +
   `/peer/active-site` served in-process by an `http.RoundTripper`. It is
   driven synchronously once per poll rather than by `Run`, so ordering
   stays exact.
4. Each poll: apply due fault ops → restart the operator if a mid-`Execute`
   death's down window has expired → advance the data plane one tick (app
   writes land on every writable site; replication fetch/apply moves GTID
   ranges subject to links and stalls) → tick every live sidecar and run
   `tm.Poll(ctx)`, in the trial's chosen order → run per-poll invariants →
   advance the clock one poll interval. All faults heal at `HealAt`; the
   settle window outlasts the cooldown, the 30s recovery stabilization
   delay, and debounce thresholds, so end-state invariants judge a genuinely
   quiesced cluster.

## Invariants

Per poll:
- **PromoteWhileObservedWritable** — never grant writability while another
  site is observed writable, unless it was fenced in the same cycle.
- **FenceAfterPromote** — fencing of other sites precedes the writable grant.
- **DualWritableUnresolved** — a dual-writable state that is fully
  operator-observable, with a resolution policy available (failover history
  or split-brain priorities), must be fenced within a grace window.
- **RepointDivergent** (model-level) — `CHANGE REPLICATION SOURCE` must never
  point a site at a source missing that site's transactions.

At end of trial (post-heal, post-settle):
- **EndSplitBrain / WedgedNoPrimary / NonConvergence** — the cluster
  converges to exactly one writable primary with healthy, caught-up replicas,
  unless the end state is an explicitly reported blocked condition
  (`RecoveryBlocked` divergence, source-convergence `Blocked`) or a
  documented needs-human state (no history, no writable, no priorities).
- **SilentDataLoss** — every application-acked transaction is on the final
  primary, or covered by a reported divergence, or held only by sites that
  never came back (inherent async-DR RPO). Anything else is data loss the
  operator failed to surface.
- **CooldownViolated** — successive promotions respect the anti-flap
  cooldown.
- **StatusClaimMismatch / DNSStale** — the CR's `activeSite` claim and the
  DNS record match ground truth once settled.

At every operator restart:
- **AntiFlapStateLost** — a restarted process must rehydrate an anti-flap
  record at least as new as the last promotion the harness observed, unless
  the out-of-band store had an unhealed rejection. The operator writes that
  record at promotion time and retries it every poll, so a healthy store
  means a current record; anything else is a failure to write it or to read
  it back.

## Model fidelity notes

Faithful where correctness depends on it: `super_read_only=ON` forces
`read_only=ON` and `read_only=OFF` clears `super_read_only`; `RESET REPLICA
ALL` / `CHANGE REPLICATION SOURCE` require stopped threads (error 3081) and
purge relay logs; a replica whose GTID set exceeds its source breaks the IO
thread like error 1236; relay drain applies local backlog even when the
source is unreachable; crashed sites keep durable state and can boot fenced
or writable (both happen in production).

Operator death is modeled as "the process stops interacting", not as a
panic: the fatal statement lands (or does not, per the op's `PreApply`), and
from then until the harness builds a replacement every operator-side call —
reads, mutations, DNS, status, the anti-flap store — fails or is dropped, so
the remainder of the dying `Poll` cannot change the model. Unwinding the
real stack would be no more faithful (the manager is discarded either way)
and would risk unwinding through one of `Poll`'s per-site goroutines. The
sidecars are unaffected: they are separate processes, and a dead operator is
exactly what their lease rule exists for.

Not modeled (excluded from trials, guarded by loud errors): CLONE /
bootstrap (the bootstrap controller is real and production-shaped, but the
model never produces an empty site so no clone goroutine can start), ordered
updates, planned failovers, Dragonfly, and wall-clock
`context.WithTimeout` expiry (timeouts are modeled as injected call errors
instead).

The simulator only contains failure modes we teach it. When the playground or
production surfaces a new MySQL behavior, encode it here too — and keep the
playground E2E suite as the fidelity check for this model.

## Known finding classes (expected in campaign output)

- **`CooldownViolated(restart+stateLost)`** — a restart between the two
  promotions that came up without the durable anti-flap record. That
  requires **both** durable paths (CR status and the out-of-band annotation
  store) to have been rejecting writes at once; nothing could then have
  carried the cooldown across the restart. See
  `docs/docs/known-limitations.mdx` → Operator availability.

`CooldownViolated` splits three ways so the inherent class cannot absorb a
regression, and only the third is expected:

| Fingerprint | Meaning | Verdict |
|---|---|---|
| `CooldownViolated` | no restart between the promotions | regression |
| `CooldownViolated(restart)` | a restart, but the record survived it | regression |
| `CooldownViolated(restart+stateLost)` | a restart with every durable path denied | inherent |

`CooldownViolated(restart)` was the expected class before the out-of-band
store existed. It is a regression now: the restarted process had the record
and promoted anyway.

## Model coverage gaps (documented, not yet closed)

- The cluster is always seeded with baseline data, so **empty/fresh sites
  never occur in trials** — the operator's empty-site guards and clone paths
  are covered by component tests only (CLONE is unmodeled).
- Fault injection applies to mutations; **reads never fail independently**,
  so "mutation applied, subsequent read fails" interleavings inside a single
  operator action are unexplored. The sidecar shares the operator's
  ambiguous/failing-mutation counters, so its own fence write can be
  ambiguous, but its reads cannot fail independently either.
- Sidecars tick exactly once per operator poll, at a per-trial phase. Real
  monitors run on their own interval, so sub-poll interleavings (two sidecar
  ticks inside one operator action) are unexplored.
