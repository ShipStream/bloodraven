# Quiz — The poll loop and per-site state

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

`orders` runs at defaults: `pollInterval: 2s`, `failureThreshold: 3`, `recoveryThreshold: 2`. All three sites have been healthy for hours. `iad` stops answering entirely. How long until the operator records `iad` as `unreachable`?

- 6 s
- 10 s
- 4 s
- 15 s

**Correct option index:** 0

**Explanation:**

Detection delay is `pollInterval × failureThreshold` = 2 s × 3 = 6 s: three probes 2 s apart, and the third returns `unreachable`. 10 s is the classic wrong derivation — it adds `recoveryThreshold` into the sum (2 s × (3 + 2)); `recoveryThreshold` gates the opposite transition, back to `writable`, and is not a term here. 4 s uses `recoveryThreshold` instead of `failureThreshold` (2 s × 2), the same confusion in the other direction. 15 s comes from mistaking the 5 s per-site `context.WithTimeout` for the loop's pace — that ceiling bounds one probe, it does not set the interval between polls. (objective 2)

## Question 2

**Type:** MULTIPLE_CHOICE

At defaults, how many polls does a healthy site need to be recorded as `read-only`, and how many to be recorded as `writable`?

- 1 poll to `read-only`; 2 consecutive polls to `writable`
- 2 consecutive polls to `read-only`; 1 poll to `writable`
- 3 consecutive polls to `read-only`; 2 consecutive polls to `writable`
- 1 poll to `read-only`; 1 poll to `writable`

**Correct option index:** 0

**Explanation:**

`read_only=1` returns `StateReadOnly` on the answer itself with no counter involved, while `read_only=0` from a non-writable site increments `recoveryCount` and only returns `StateWritable` at `recoveryThreshold` (default 2). Option 2 inverts the asymmetry — it treats the dangerous direction as the cheap one, which is exactly backwards: read-only is the safe direction, so it is believed instantly. Option 3 borrows `failureThreshold: 3` for the read-only transition, but 3 gates `unreachable`, not `read-only`. Option 4 drops the debounce entirely, which would let one stale or mid-restart `read_only=0` mint a second authority. (objective 3)

## Question 3

**Type:** MULTIPLE_CHOICE

`pdx` was `writable`. Its last two probes both failed with connection errors; `failureThreshold` is 3. What does the operator record as `pdx`'s state right now?

- `writable` — `failCount` is 2, and `computeState` returns the site's current state until the threshold is met
- `unreachable` — the probes failed, and the threshold only controls how loudly it is reported
- `unknown` — a failed probe invalidates what the operator previously believed
- `read-only` — an unresponsive site is treated as unable to serve writes

**Correct option index:** 0

**Explanation:**

Below the threshold, `computeState` increments `failCount` and returns `site.state` unchanged — so `pdx` is still `writable`, and a third consecutive failure is what flips it. Option 2 is the debounce misread: the threshold decides the state, it is not a reporting filter. Option 3 is wrong because `unknown` means no usable answer has ever been obtained, not that the last one failed; the operator holds its previous belief instead. Option 4 confuses two different facts — `read-only` is only ever recorded from a *successful* probe returning `read_only=1`, and a failed probe carries no `@@read_only` value at all. Note the third probe would also reset `recoveryCount`, which each failure has already been doing. (objective 1)

## Question 4

**Type:** MULTIPLE_CHOICE

`pdx` has been down for five minutes, so the poll loop has settled at its cap. `iad` now fails too. At defaults, how long until the operator records `iad` as `unreachable`?

- 90 s
- 6 s
- 30 s
- 12 s

**Correct option index:** 0

**Explanation:**

The adaptive backoff is driven by the worst `failCount` across all sites, so `pdx`'s sustained outage has pushed the single loop to its 30 s hard cap. `iad` still needs `failureThreshold` = 3 consecutive failures, and they now arrive 30 s apart: 30 s × 3 = 90 s. 6 s is the healthy-cluster answer (2 s × 3) and assumes the interval is still the base — the trap the backoff sets, because it is undocumented and cluster-wide rather than per-site. 30 s is the interval itself, mistaken for the detection delay; the threshold still needs three of them. 12 s reads the 30 s cap as if only the failing site backed off while `iad` kept some faster cadence — there is one loop, not one per site. (objective 2)

## Question 5

**Type:** TRUE_FALSE

`reader` in `orders` has `role: read-only`. It answers `read_only=0` on one probe. The operator waits for `recoveryThreshold` consecutive writable answers before recording it as `writable`, exactly as it would for a `primary-candidate` site.

**Correct answer:** false

**Explanation:**

The reversal: a non-promotable site is the one case that *skips* `recoveryThreshold` entirely and is recorded `writable` on the first successful writable observation. The debounce exists to stop a flapping candidate being wrongly believed writable, which is a promotion-safety concern; a writable reader is not a candidate for anything, it is an immediate safety fact — authority is now ambiguous — and the code refuses to debounce authority invalidation. The tempting wrong model is 'one debounce rule, applied uniformly'; role is a term in this transition. (objective 1)
