# Rubric — Make the writer survive

Five criteria, integer weights summing to 100. Graded against the learner's `brdrill.py` and
`drill-record.md`, using the captures in `tests/fixtures/`.

| Criterion | Weight |
| --- | --- |
| The write-gap is measured from writes, per site, and reports an unclosed gap as unclosed | 30 |
| Stale reads and error classes are counted on the right signal | 20 |
| The verdict separates what the drill proved from what it did not | 20 |
| The drill record is a usable operational artefact | 15 |
| Craft: clear code, honest failure handling, no invented data | 15 |
| **Total** | **100** |

## What each criterion is looking for

### The write-gap is measured from writes, per site, and reports an unclosed gap as unclosed — 30

`write_gap` selects on `op == "write"` and `ok` being true, uses `demoted_site(drill)` and `promoted_site(drill)` rather than literal site names, and requires the closing write to come after the opening one. When either end is absent the result is `closed: False` with `gapSeconds: None`, and the CLI prints `UNCLOSED` and exits 2. Full marks require all three: writes only, sites from the drill, and no zero substituted for an unmeasured gap.

### Stale reads and error classes are counted on the right signal — 20

`stale_read_window` counts successful reads served by the demoted site at or after `status.lastFailover`, and returns a real zero for the emergency capture rather than a crash or a `None`. It does not use `readOnly` as the test, which would sweep in the `reader` site, which reports read_only=1 by design. `error_classes` returns all three keys always, puts both 1290 and 1792 in `readOnlyRefusal`, and puts a null error code in `connection` rather than `other`.

### The verdict separates what the drill proved from what it did not — 20

`verdict` claims RPO 0 only for a planned failover that reached `Succeeded` with `transactionsLost: 0`, and attributes it to the GTID superset gate rather than to the measured gap. A planned failover in any other phase, and every emergency failover, return a not-established string. Credit is lost if a write-gap is anywhere presented as an RPO, or if an emergency drill is allowed to claim zero loss.

### The drill record is a usable operational artefact — 15

`drill-record.md` names both drills with their triggers and the direction each moved the primary, carries the measured gaps and the baseline delta, keeps `## Measured` and `## Assumed` genuinely separate, and states what the drill did not prove. The attribution line subtracts Bloodraven's 12.0 s promotion from the emergency gap and shows the arithmetic. No `TODO` left behind.

### Craft: clear code, honest failure handling, no invented data — 15

The four functions are short and readable, return the documented shapes exactly, and do not mutate the loaded samples. Missing ends, empty selections and absent `plannedFailover` blocks are handled without tracebacks and without silently substituting defaults. No timestamp, count or site name is hardcoded from the fixtures; every printed number is derived from the capture that was loaded.
