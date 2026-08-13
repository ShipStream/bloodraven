# Failover drill record — `playground`

Fill this in from `brdrill` output. Keep the drills separate: they are different
events making different claims. Replace every `TODO`.

## Drills

| Drill | Trigger | Moved | Capture |
| --- | --- | --- | --- |
| emergency | TODO | TODO -> TODO | `tests/fixtures/emergency-probe.jsonl` |
| planned | TODO | TODO -> TODO | `tests/fixtures/planned-probe.jsonl` |
| planned, baseline (unfixed pool) | TODO | TODO -> TODO | `tests/fixtures/planned-probe-unbounded.jsonl` |

## Measured

Only numbers these captures produced. Paste the `brdrill` lines.

- emergency: `writeGapSeconds` TODO
- planned: `writeGapSeconds` TODO
- planned, baseline: `writeGapSeconds` TODO, `gapDeltaSeconds` TODO
- stale reads, baseline: TODO reads over TODO s, first at TODO
- error classes, planned: TODO

## Assumed

Everything carried over rather than observed here — the RPO verdicts, and any
claim about what happens on a capture you did not take.

- verdict (emergency): TODO
- verdict (planned): TODO
- what this drill did **not** prove: TODO

## Attribution

Bloodraven's measured emergency promotion is 12.0 s to the `activeSite` flip
(detection is `pollInterval` × `failureThreshold` = 2 s × 3 = 6 s of it). Subtract
it from your emergency write-gap and the remainder belongs to your writer.

Not Bloodraven's: TODO s

## What would change the number

TODO — name the pool setting, the drain budget, or the strategy, and say which
direction it moves the gap.
