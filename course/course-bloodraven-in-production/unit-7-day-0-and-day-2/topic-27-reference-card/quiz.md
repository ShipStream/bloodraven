# Quiz — The card you keep

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

## Question 1

**Type:** MULTIPLE_CHOICE

Your on-call runbook says 'the operator detects a dead site in about 10 seconds (2 s poll × 3 failures, plus 2 recovery polls × 2 s)'. Correct it.

- Detection is 6 s — `pollInterval` × `failureThreshold`. `recoveryThreshold` gates the transition back to `writable` and is never a term in the detection sum
- Detection is 11 s — the sum is right but the 5 s per-probe ceiling has to be added once
- Detection is 10 s and the runbook is correct; `recoveryThreshold` is consulted before a site is declared unreachable
- Detection cannot be stated as a fixed number because the poll interval is adaptive from the first failure onward

**Correct option index:** 0

**Explanation:**

Adding `recoveryThreshold` to the detection sum is the single most common wrong answer about this operator. The two counters gate opposite transitions: `failureThreshold` guards the way to `unreachable`, `recoveryThreshold` guards the way back to `writable`. Option 2 adds the per-probe ceiling, which bounds one probe and does not pace the loop. Option 4 overstates the backoff: it engages only once a site's `failCount` climbs *past* `failureThreshold`, so the first detection is always 6 s — but it is a real caveat worth adding to the runbook for the second fault during an existing outage, where the answer becomes 30 s × 3 = 90 s. (objectives 10, 11)

## Question 2

**Type:** MULTIPLE_CHOICE

A colleague is building an alert set for a new group and copies your `BloodravenPITRArchiveLagging` rule, changing only the group name in the label matcher. It never fires, even during a real archiver backlog. What did they get wrong?

- The label sets are not uniform: archiver metrics carry `{namespace, group, site}` while backup metrics carry `{group, profile}` and site metrics carry `{site}` alone, so a selector copied between families silently matches nothing
- `bloodraven_archiver_backlog_files` is a counter, not a gauge, so it needs `increase()` before it can be compared to a threshold
- Archiver metrics are exported by the sidecar on a different port, so the scrape config needs a second job before any rule against them can fire
- The rule needs a `for:` duration shorter than `archivePollInterval`, or the backlog clears before the rule's evaluation window closes

**Correct option index:** 0

**Explanation:**

A selector naming a label the series does not carry matches nothing and reports nothing — the failure mode of an alert that is silently always-green. Reading the label set before copying a selector is the habit; the reference card exists so that reading takes five seconds. Options 2 and 4 are plausible-sounding PromQL claims that do not hold here. Option 3 confuses where the archiver runs with where its metrics are exported. (objective 11)

## Question 3

**Type:** TRUE_FALSE

You remember from this course that Bloodraven ships without a licence file and that a particular fix was still an open issue. Both are safe to state in a production readiness review.

**Correct answer:** false

**Explanation:**

Neither is safe to state without re-checking, and that is what the version appendix is for. Both facts were true and grounded on the date the course was written, and both are exactly the kind that move: an issue gets closed, a pull request merges, a `LICENSE` file appears. The appendix records each with a date and the command that re-checks it — `gh issue view`, `gh repo view --json licenseInfo`, a `grep` over the source — so one lookup replaces a guess. The mechanisms you learned do not expire. The dated facts about them do. (objective 12)

## Question 4

**Type:** MULTIPLE_CHOICE

Which of these belongs on the reference card, and which belongs in the version appendix?

- `failoverCooldown` defaults to `5m` belongs on the card; 'issue #123 is open' belongs in the appendix, because one is a CRD default and the other is a fact with a date on it
- Both belong on the card: the appendix is only for sources and citations
- Both belong in the appendix: anything traceable to the repository is version-specific
- `failoverCooldown` belongs in the appendix because defaults change between releases; issue numbers belong on the card because they never change

**Correct option index:** 0

**Explanation:**

The split is between facts that describe a mechanism and facts that describe a moment. A CRD default is readable off the shipped CRD at any time and changes rarely and loudly; an issue's state changes quietly and without a release. Option 4 inverts it exactly — an issue number is stable but its *state* is the fact you care about, and that is the perishable part. Option 3 would make the card empty, which defeats the point of having one screen at 3am. (objective 12)

## Question 5

**Type:** SHORT_ANSWER

You are handed a group you have never seen. Using only the reference card, write the four commands or reads you would perform first, and say what each one rules in or out.

**Sample answer:**

One: `kubectl get mysqlfailovergroup <group> -o jsonpath='{.status.activeSite}'` — an empty answer means authority is ambiguous and every Service endpoint has been shed, which is the shape of split brain and of no-primary; a name means there is one writable authority. Two: the `Degraded` condition's `reason`, which is one of exactly five strings and tells me the topology shape rather than the action — and specifically rules out looking for a `Failover` reason, which does not exist. Three: `status.updatePhase` — non-empty means somebody is mid-rollout and any failover I am looking at is theirs, not a fault. Four: the per-site rows, `state` plus `secondsBehindSource` plus `recoveryState`, remembering that the writable primary's replication keys are absent rather than zero, that a lag of `-1` means not replicating at all, and that `role` is not in status so I have to read `spec.sites[].role` to tell a `read-only` reader from a lagging candidate.

**A full-credit answer shows:**

A strong answer names `status.activeSite` and reads an empty value correctly (ambiguous authority, endpoints shed); names the `Degraded` reason and that there are five of them with no `Failover`; checks `status.updatePhase` to separate a rollout from an incident; and reads the per-site rows with at least two of the three traps — absent-is-not-zero, `-1` is not a small lag, and role lives in spec rather than status. Credit any order that starts with authority and ends with per-site detail.

**Explanation:**

The value of the card is not that it contains these facts but that it puts them one screen apart, so the first thirty seconds of an incident are spent reading rather than recalling. Every trap in the answer above is one this course spent a topic on. (objectives 10, 11)
