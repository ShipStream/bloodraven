# The poll loop and per-site state

`orders` is healthy. Three sites — `iad` writable, `pdx` and `reader` read-only — and the counter
application is committing rows into `iad` without knowing the other two exist. Nothing is on fire,
which makes this the only calm moment you will get to ask the question that decides your outage
budget: what is the operator actually doing in the two seconds between one status read and the next?

## The loop, the probe, the four states

`spec.pollInterval` defaults to `2s`, and the operator hard-defaults to 2 s again in Go when the
field's pointer is nil — so an omitted `pollInterval` is 2 s, not zero and not "never". Every tick,
the loop does the same four things: probe every site, apply the debounce counters, compute each
site's state, then look for transitions.

The probe is one statement. `SELECT @@read_only`. Not a ping, not a replication check, not a
`SELECT 1` — one server variable, read over the connection the operator already holds. Every site is
probed in parallel, each under its own `context.WithTimeout(ctx, 5*time.Second)`. Two consequences
follow, and you want both in your head: a dark site cannot cost the cycle more than 5 s, and a slow
site does not delay its healthy peers' probes — but the cycle itself does not finish until the
slowest probe returns.

That single boolean, plus whether the query returned an error at all, is the entire input to the
state machine. There are four states, and they are exactly these:

| Constant | Rendered in status | Meaning |
| --- | --- | --- |
| `StateUnknown` | `unknown` | no usable answer yet |
| `StateWritable` | `writable` | `read_only=0` |
| `StateReadOnly` | `read-only` | `read_only=1` |
| `StateUnreachable` | `unreachable` | connection failed |

```widget
{
  "type": "flow",
  "title": "One poll cycle",
  "steps": [
    {
      "label": "Probe every site in parallel",
      "detail": "SELECT @@read_only per site, each under a 5 s context.WithTimeout. The cycle waits for the slowest probe."
    },
    {
      "label": "Apply the debounce counters",
      "detail": "An error increments failCount and zeroes recoveryCount. A success zeroes failCount."
    },
    {
      "label": "Compute each site's state",
      "detail": "unknown / writable / read-only / unreachable, from the counters plus this poll's answer."
    },
    {
      "label": "Detect transitions",
      "detail": "Compare the new state against the previous one. Only a change is a transition."
    }
  ]
}
```

## The artifact: the three knobs, and the function they feed

Nothing in `orders` changed since you stood it up. What changed is that three lines of that spec are
now load-bearing rather than boilerplate:

```yaml
# orders — unchanged since topic 3; these three lines are the subject now
spec:
  pollInterval: 2s        # <-- the tick
  failureThreshold: 3     # <-- consecutive failures before "unreachable"
  recoveryThreshold: 2    # <-- consecutive writable answers before "writable"
```

They land in one function, and reading it is faster than reading any prose about it:

```go
func (tm *TopologyManager) computeState(site *siteTracker, readOnly bool, err error) state.SiteState {
	if err != nil {
		site.recoveryCount = 0
		site.failCount++
		if site.failCount >= tm.cfg.FailureThreshold {
			return state.StateUnreachable
		}
		return site.state // not enough failures yet, keep current state
	}

	// Successful poll.
	site.failCount = 0

	if readOnly {
		site.recoveryCount = 0
		return state.StateReadOnly
	}

	// read_only=0 (writable)
	if site.state != state.StateWritable {
		site.recoveryCount++
		if site.recoveryCount >= tm.cfg.RecoveryThreshold {
			return state.StateWritable
		}
		return site.state // not enough recoveries yet
	}

	return state.StateWritable
}
```

Both counters reset on the opposite outcome, so "three failures" means three *consecutive* failures.
One good answer in the middle puts `failCount` back to zero and you start again.

## The asymmetry, and the number people get wrong

Now look at what that function believes instantly and what it makes prove itself. A `read_only=1`
answer becomes `read-only` on a **single** poll — no counter, no waiting — and it zeroes
`recoveryCount` on the way through. A `read_only=0` answer from a site that is not already writable
becomes `writable` only after `recoveryThreshold` consecutive successes. A connection error becomes
`unreachable` only after `failureThreshold` consecutive failures.

```widget
{
  "type": "compare",
  "title": "Two transitions, opposite treatment",
  "rows": [
    {
      "aspect": "How many polls?",
      "cells": [
        "1 — believed on the answer itself",
        "recoveryThreshold consecutive polls (default 2)"
      ]
    },
    {
      "aspect": "Which counter?",
      "cells": [
        "None. It resets recoveryCount to 0.",
        "recoveryCount, incremented then tested against the threshold"
      ]
    },
    {
      "aspect": "Why that choice?",
      "cells": [
        "Read-only is the safe direction: a site that cannot take writes cannot cause divergence, so believing it early costs nothing.",
        "Writable is the dangerous direction: a site wrongly believed writable is a second authority. Make it prove itself."
      ]
    }
  ],
  "columns": [
    {
      "label": "→ read-only"
    },
    {
      "label": "→ writable"
    }
  ]
}
```

One logic, two directions. Read-only is cheap to be wrong about; writable is expensive.

Which gives you the derivation everyone reaches for and half of everyone gets wrong. Detection delay
for a dead site is `pollInterval × failureThreshold` = **2 s × 3 = 6 s**. Three probes, one every
2 s, and the third one returns `unreachable`. `recoveryThreshold` is **not** a term in that sum. It
never was. It gates the opposite transition — the way back to `writable` — and adding it to get 10 s
is the single most common wrong answer about this operator. The 5 s probe ceiling is not a term
either; it bounds one probe, it does not pace the loop.

## The backoff nobody documented

Here is the part that appears in no documentation and will surprise you at 3 a.m. The poll interval
is adaptive. Once any site's `failCount` climbs past `failureThreshold`, the interval doubles per
extra failure — `interval := base * time.Duration(1<<uint(backoffFails))` — with the exponent capped
at `maxPollBackoffExponent = 4` and a 30 s hard cap on top.

Walk it at defaults. `failCount` 3 gives no backoff; 4 → 2 s × 2 = 4 s; 5 → 8 s; 6 → 16 s; 7 → 2 s ×
2⁴ = 32 s, which the 30 s cap clips to 30 s. So roughly half a minute into a site being down, the
loop has settled at 30 s.

And it is **one loop**, driven by the worst `failCount` across all sites. That is the operational
consequence you came for. `pdx` has been down for five minutes, so the loop is polling every 30 s —
including its probes of `iad`, which is fine. Now `iad` dies. Detection needs three consecutive
failures, and they now arrive 30 s apart: **30 s × 3 = 90 s**, not 6 s. Fifteen times slower, on the
site that matters, at exactly the moment you have no spare site left. The backoff is trading
detection latency for polling waste, and one existing outage silently spends that trade on every
other site.

## One bypass

Last thing. A writable observation on a **non-promotable** site — anything whose role is not
`primary-candidate`, so `reader` in `orders` — skips `recoveryThreshold` entirely and records
`writable` on the first poll. The comment in the code is the whole argument: a writable
non-promotable site is an immediate safety fact, and authority invalidation must not be debounced
behind the normal recovery threshold. Being slow to believe a new primary is prudence. Being slow to
notice a reader taking writes is not.

You can now take any sequence of probe results for one site and say which state it lands in and
after how many seconds, including under backoff. What you cannot yet say is what the operator *does*
with three states at once — because `iad` unreachable while `pdx` and `reader` are read-only is not
three independent facts, it is one row in a table. That table is next.
