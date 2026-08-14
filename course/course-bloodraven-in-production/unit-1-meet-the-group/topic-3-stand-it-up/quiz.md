# Quiz — Stand it up and read its status

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

`playground` reports `activeSite: iad`, and its site entries read: `iad` state `writable`, no other fields; `pdx` state `read-only`, `replicating: true`, `secondsBehindSource: 0`; `reader` state `read-only`, `replicating: true`, `secondsBehindSource: 0`. Which site is the primary, and what in the dump establishes it?

- `iad` — it is named in `activeSite` and it is the one site whose state is `writable`
- `pdx` — it is the only site the operator reports as `replicating`, which is what a primary does
- `iad` — it is the first entry in `status.sites[]`, and the operator lists the primary first
- You cannot tell: `activeSite` records the last site that was promoted, not the site that is writable now

**Correct option index:** 0

**Explanation:**

`activeSite` names the site the operator currently treats as the writable authority, and `iad` is the only entry in state `writable` — the two agree, which is what a healthy group looks like. `pdx` is wrong because `replicating: true` marks a follower applying the primary's stream, not a primary; the primary is never probed for replication at all. The first-entry answer is wrong because `status.sites[]` runs parallel to `spec.sites`, in declaration order, with no relation to who is primary. The last option confuses `activeSite` with `status.lastFailoverTarget`, which is the field that records a past promotion. (objective 8)

## Question 2

**Type:** MULTIPLE_CHOICE

Same dump. `pdx` and `reader` both read state `read-only`, `replicating: true`, `secondsBehindSource: 0`. How do you establish which of them is the dedicated non-promotable reader?

- Read `spec.sites[].role` — the status block carries no role field
- Read `status.sites[].role`, which reports `read-only` for the reader and `primary-candidate` for `pdx`
- The reader is the site whose `state` is `read-only` while the other candidate's state would be `unknown`
- The reader is the one with no `lbIP` in its status entry

**Correct option index:** 0

**Explanation:**

Role is spec, not status: `status.sites[]` carries name, state, lastSeen, replication fields and recovery fields, and nothing about role. The second option is the common trap — the documentation talks about site roles beside site state and it is easy to assume both live in the same block; they do not, and `status.sites[].role` simply does not exist. The third confuses role with state: both a `primary-candidate` follower and a `read-only` reader sit in state `read-only` when they are healthy, which is exactly why the dump cannot separate them. The fourth invents a field — `lbIP` is spec, and status never mirrors it. (objective 8)

## Question 3

**Type:** MULTIPLE_CHOICE

`playground` now reports `activeSite: iad`; `iad` state `writable`; `pdx` state `read-only`, `replicating: true`; `reader` state `unreachable`. What `reason` does the `Degraded` condition carry?

- `Healthy` — `role: read-only` sites are excluded from the writable/read-only/unreachable tallies
- `Degraded` — one site is unreachable while the primary is up
- `TotalLoss` — a site the operator cannot reach counts as lost
- `NoPrimary` — the group is down to one writable site with no spare reader

**Correct option index:** 0

**Explanation:**

The reasons are computed over core sites only, and a site with `role: read-only` is not a core site — so a dead reader leaves the tally at one writable, zero unreachable, which is `Healthy`. The second option is the most tempting and would be right if the unreachable site were `pdx`: a live primary with an unreachable *core* peer is exactly the `Degraded` shape, but the reader does not count. `TotalLoss` requires every core site unreachable, not one site of any kind. `NoPrimary` describes the opposite situation — no writable site at all, every core site read-only — and here `iad` is writable. (objective 8)

## Question 4

**Type:** TRUE_FALSE

Your counter application has just written through `mysql-playground-primary`. `status.sites[]` shows `pdx` with `replicating: true` and `secondsBehindSource: 0`, so that write is certainly present on `pdx`.

**Correct answer:** false

**Explanation:**

The reversal: a zero does not prove the replica is caught up. `Seconds_Behind_Source` compares the last transaction the replica has executed against the last event it has *downloaded*, so it reads 0 when the receiver thread has stalled or the replica is simply idle — a replica that stopped fetching an hour ago can still report 0. That is why the check in this topic is to go and read the row: `SELECT value, updated_at FROM counter_db.counters WHERE id = 1` against `pdx` by name. Nothing in the status block substitutes for reading the data. (objectives 8, 9)

## Question 5

**Type:** SHORT_ANSWER

In one status dump `pdx` shows `secondsBehindSource: 0`; in another, `pdx` has no `secondsBehindSource` key at all. What does each tell you, and what is the trap in treating them as the same?

**Sample answer:**

`secondsBehindSource: 0` is a reported measurement — MySQL answered the replication probe and gave a lag of zero seconds. An absent key means no value was reported: MySQL returned NULL, which is what it does when replication is not running, or the operator never probed that site at all, which is the case for the writable primary. Treating absence as zero reads a site that is not replicating as a site that is perfectly caught up — precisely backwards. Absence means unmeasured, not good.

**A full-credit answer shows:**

A strong answer covers: (1) `0` is a value MySQL actually reported; (2) an absent key is a null/unreported value, arising when replication is not running or when the site was never probed — the primary is never probed; (3) the failure mode is inverted meaning, reading 'unmeasured' as 'zero lag'. Credit also for noting the field is optional in the status schema, so it is omitted rather than serialised as null.

**Explanation:**

The two look alike in a quick scan and mean opposite things. A reported `0` says the probe ran; an absent key says it did not, or MySQL had nothing to report. On a healthy group the primary's entry is the everyday example of the absent form, which is why it is worth learning on a healthy cluster rather than in an incident. (objective 8)
