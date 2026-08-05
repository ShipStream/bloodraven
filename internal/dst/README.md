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

The campaign stops at **coverage saturation**: after `DST_DRY_BATCHES`
consecutive batches produce no new behavior signature and no new failure
fingerprint, more trials are diminishing returns. Failures are deduplicated by
the set of violated invariants and shrunk to a minimal fault schedule.

## How a trial works

1. `GenerateTrial(seed)` derives everything from the seed: 2 or 3 sites and
   their roles, split-brain priorities, anti-flap cooldown, whether failover
   history is rehydrated, and an explicit fault-op schedule (crashes with
   fenced/writable boot modes, operator↔site and site↔site partitions,
   operator restarts, ambiguous mutations — applied but the operator saw an
   error — failing mutations, replication fetch/apply stalls, relay-drain
   stalls, rogue fences, rogue `read_only=0`, CR-status write outages, DNS
   outages). Execution consumes no randomness after generation.
2. The harness builds a production `TopologyManager` via
   `NewTopologyManagerWithClock` with a `FakeClock`, the simulated
   `mysql.Checker`s, DNS updater/reader, and tainter. `StatusCallback`
   snapshots act as the simulated CR status subresource; an operator-restart
   fault rebuilds the manager and rehydrates from the last persisted snapshot
   exactly the way `runner.go` does.
3. Each poll: apply due fault ops → advance the data plane one tick (app
   writes land on every writable site; replication fetch/apply moves GTID
   ranges subject to links and stalls) → `tm.Poll(ctx)` → run per-poll
   invariants → advance the clock one poll interval. All faults heal at
   `HealAt`; the settle window outlasts the cooldown, the 30s recovery
   stabilization delay, and debounce thresholds, so end-state invariants
   judge a genuinely quiesced cluster.

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

## Model fidelity notes

Faithful where correctness depends on it: `super_read_only=ON` forces
`read_only=ON` and `read_only=OFF` clears `super_read_only`; `RESET REPLICA
ALL` / `CHANGE REPLICATION SOURCE` require stopped threads (error 3081) and
purge relay logs; a replica whose GTID set exceeds its source breaks the IO
thread like error 1236; relay drain applies local backlog even when the
source is unreachable; crashed sites keep durable state and can boot fenced
or writable (both happen in production).

Not modeled (excluded from trials, guarded by loud errors): CLONE / bootstrap
(the bootstrap controller is nil; replication credentials are still set so
recovery and source convergence run), ordered updates, planned failovers,
Dragonfly, the sidecar's own fencing monitor (its *effects* are modeled as
rogue-fence faults), mid-`Execute` operator crashes (restarts land on poll
boundaries), and wall-clock `context.WithTimeout` expiry (timeouts are
modeled as injected call errors instead).

The simulator only contains failure modes we teach it. When the playground or
production surfaces a new MySQL behavior, encode it here too — and keep the
playground E2E suite as the fidelity check for this model.

## Known finding classes (expected in campaign output)

- **`CooldownViolated` with `operator restarts between: ≥1`** — the anti-flap
  cooldown and failover target are durable only in CR status; a status-write
  outage plus an operator restart legitimately resets them (see
  `docs/docs/known-limitations.mdx` → Operator availability). A durable
  out-of-band store is the eventual fix; until then this class is expected,
  and any `CooldownViolated` *without* a restart between the promotions is a
  regression.
