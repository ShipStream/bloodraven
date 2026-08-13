# Rubric — brprep

Total 100. `./tests/run.sh` is the machine half; the criteria below are what a human reading the
script is looking for beyond a green run.

## Admission rules reproduced faithfully — 35

All nine admission rules are checked, each against the manifest rather than against a fixture name,
and each emits its exact `REJECT <rule>` token in the declared order. A manifest violating several
produces several lines.

Two specifics carry most of the weight. **The role default**: `spec.sites[].role` is optional and
defaults to `primary-candidate`, so a site with no `role` key counts toward the two-candidate minimum
and is required to carry `lbIP` and `taintNodeSelector`. **The reader exemption**: those two fields
are required *unless* the role is `read-only`. `reader-lbip.json` is the discriminator — a legitimate
reader without them beside a `primary-candidate` missing both, and exactly one `REJECT` is correct. A
check that demands the fields unconditionally fails it; one that never demands them fails it too.

Full marks need the interval rules done as arithmetic through `secs`, not as string comparison:
`20s` versus `10s` is a ratio question, and `leaseTimeout: 1m` against `peerCheckInterval: 30s` has to
come out as a rejection.

## Silent findings separated from rejections — 30

`check_silent` reports all six documented findings as `WARN`, never as `REJECT`, and never changes the
exit code by itself. `silently-wrong.json` produces exactly five `WARN` lines, zero `REJECT` lines and
exit `0`.

The floating-tag check catches `mysql:9` and `mysql:latest` and leaves `mysql:9.7` alone — a tag with
no dot in it floats, and an image with no tag at all floats hardest. The QoS check compares requests to
limits for **both** CPU and memory rather than checking that limits merely exist, and a site that omits
`resources` entirely is not a finding. The PVC and QoS findings name the profile and the site
respectively; a generic "a backup profile uses PVC" does not earn this.

Deduct for any implementation that makes a finding fatal. The reason the split exists is that a
pre-flight which refuses to let you apply a valid manifest gets removed from the pipeline within a
week, taking the rejections with it.

## The change plan is specific and correct — 25

`plan_upgrade` names the standby that is upgraded first, gives the ordering reason, states that a real
promotion follows, names the site that ends up active, and lists the three observable consequences. It
prints `no change` — and nothing else — when the target already matches `spec.image`, and prints
nothing at all when `--target-image` is absent.

"The pods will restart" does not earn this. The three consequences are the reason the plan is worth
writing down: a routine image bump increments `bloodraven_failovers_total`, stamps `lastFailover` so
the anti-flap budget is spent on the rollout rather than on the next real failure, and flips the DNS
record. Someone is being paged for that, and this plan is what tells them it was you.

Credit an answer that notes in a comment what the script cannot know: a manifest carries no
`status.activeSite`, so declaration order is the only signal available, and against a live group you
would read the status instead.

## Shell discipline — 10

`set -euo pipefail` retained. Every field read goes through `q` / `jq` rather than `grep` or `sed` over
JSON. No `eval`. The script exits `0` with findings and non-zero with rejections. Output is stable and
diffable — no timestamps, no absolute paths, no colour codes — because the first thing anyone does with
a pre-flight is put it in CI and diff two runs.
