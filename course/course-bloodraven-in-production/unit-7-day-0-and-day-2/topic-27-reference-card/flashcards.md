# Flashcards — The card you keep

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Detection delay, from first principles

**Back:** `pollInterval` × `failureThreshold` = 2 s × 3 = **6 s**. `recoveryThreshold` gates the opposite transition and is never a term in that sum; neither is the 5 s per-probe ceiling.

---

**Front:** The three playground overrides

**Back:** `failoverCooldown` 30s (shipped 5m), `replication.maxLagSeconds` 30 (shipped 300), `dns.ttl` 10 (shipped 60). Everything else matches the shipped defaults, so no timing measured in the playground transfers unchanged.

---

**Front:** The five condition reasons

**Back:** `Healthy`, `Degraded`, `SplitBrain`, `NoPrimary`, `TotalLoss`. There is no `Failover` reason — a rule matching one never fires.

---

**Front:** The four fatal steps of a promotion

**Back:** `STOP REPLICA`, `RESET REPLICA ALL`, `SET GLOBAL super_read_only = OFF`, `SET GLOBAL read_only = OFF`. Everything else — fencing the old primary, killing its connections, the relay-log drain, recording the promotion GTID — warns and carries on.

---

**Front:** Which two things are *not* steps of the failover sequence

**Back:** Node taints (a pure function of per-site transitions, applied earlier in the same poll) and source convergence (an independent poll stage with its own 20 s budget). Both are poll neighbours, not links in the chain.

---

**Front:** The four Service kinds and the object count

**Back:** `-primary`, `-replicas`, `<site>`, `<site>-internal` — `2 × len(sites) + 2` objects, so eight for three sites.

---

**Front:** `bloodraven_replication_lag_seconds` reading `-1`

**Back:** Not a small lag: the site is not replicating at all. A dashboard that averages `-1` with real seconds inverts the most important signal on the page.

---

**Front:** The label-set trap across metrics

**Back:** Site metrics carry `{site}` only. The archiver carries `{namespace, group, site}`, backup metrics `{group, profile}`, and `bloodraven_keyring_phase` `{mysql_namespace, failover_group, site, phase}` — four words for two concepts.

---

**Front:** The three roles in one line each

**Back:** `primary-candidate` — the only promotable role, counted everywhere. `dr-only` — never promoted but **counted** in `coreCount` and the tallies. `read-only` — never promoted, invisible to the matrix, never tainted, refused as a backup source, skipped by every replication condition.

---

**Front:** The three annotations you apply by hand

**Back:** `planned-failover=<site>[:maxLagWait=…]`, `reclone-site=<site>:<divergentGtid prefix ≥8 chars>` (cold form `<site>:confirm=<group>`), and `rotate-keyring=<site>`. All under `bloodraven.shipstream.io/`, all consumed and cleared.

---

**Front:** Which two annotations the operator writes and you never should

**Back:** `bloodraven.shipstream.io/last-failover` and `…/last-failover-target`, RFC3339 at second precision, written as a pair by one JSON merge patch.

---

**Front:** What belongs in the version appendix rather than in your memory

**Back:** Anything dated: issue and PR numbers, upstream version pins, licence status, and 'the published page currently says X'. Check it before quoting any of them.
