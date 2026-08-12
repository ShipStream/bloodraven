# Flashcards — Alerts, runbooks, and the 3am path

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** `bloodraven_site_state`

**Back:** State-set gauge, labels `{site, state}` — 1 for the site's current state and 0 for the other three. Zero writable core sites means writes are down right now.

---

**Front:** Your `bloodraven_replication_lag_seconds > 30` rule has been silent for an hour while `pdx` has not replicated at all. Why?

**Back:** The gauge reads `-1`, not a large number, when lag is NULL because the site is not replicating. A positive threshold never crosses it — `bloodraven_replication_running{site,thread}` == 0 is the rule that catches a stopped replica.

---

**Front:** `bloodraven_failovers_total` — what does an increment prove, and what does it not?

**Back:** It proves the operator completed a MySQL promotion on `target_site`; the counter is stamped before the DNS flip so a DNS outage cannot erase it. It does not prove traffic recovered, which is why `BloodravenFailoverOccurred` is informational, not a page.

---

**Front:** `bloodraven_divergent_transactions`

**Back:** Gauge per site, 0 when healthy. Above 0 means that site is `RecoveryBlocked` and will wait forever until a human makes a reclone decision.

---

**Front:** `bloodraven_split_brain_auto_resolve_total`

**Back:** Counter labelled by the winning site. Every increment means two sides took writes and the loser's unreplicated writes were discarded — the loss is surfaced loudly, not prevented.

---

**Front:** `bloodraven_primary_reassert_total` is climbing steadily. What do you go and check?

**Back:** Sidecar connectivity to the operator's auxiliary Service — something keeps fencing the promoted primary, typically a sidecar re-fencing on a stale lease straight after promotion.

---

**Front:** `bloodraven_poll_latency_seconds` — why alert at p99 > 2?

**Back:** 2 s is the `pollInterval` default. Once p99 poll latency reaches the poll interval, the 6 s detection budget is no longer real and only the per-site 5 s probe ceiling is holding the loop together.

---

**Front:** `bloodraven_archiver_backlog_files` — which component emits it, and about what?

**Back:** The per-site sidecar's PITR archiver, labels `{namespace, group, site}`. It counts sealed binlogs not yet uploaded at the end of the last scan; only the primary archives and only sealed binlogs ever upload.

---

**Front:** `bloodraven_backup_last_success_timestamp_seconds` — what caps the staleness threshold you set on it?

**Back:** `binlog-expire-logs-seconds`, 1209600 s (14 days) by default. Past that floor PITR has no binlog material left to bridge your last backup to now, so your threshold must sit well below it.

---

**Front:** `bloodraven_keyring_phase` — the alerting form

**Back:** One-hot gauge over the five phases, labels `{mysql_namespace, failover_group, site, phase}`. Alert on `{phase="sealed"} == 0` for any site that should be protected.

---

**Front:** `bloodraven_dns_flips_total`

**Back:** Counter per target site. A `bloodraven_failovers_total` increment with no matching flip means the CR promoted correctly while external-dns never moved the record — still a write outage.

---

**Front:** `bloodraven_state_transitions_total`

**Back:** Counter labelled `{site, from, to}`. Forensics, never a page — it is what you read after the incident to reconstruct the order sites changed state in.

---

**Front:** The label matcher that stops a soaked reader paging you

**Back:** `{site!="reader"}` — `bloodraven_replication_lag_seconds` carries a `site` label only, so the exclusion is by site name and you maintain it by hand each time you add a reader.

---

**Front:** Which alert tells you the application is still broken after a successful failover?

**Back:** None that Bloodraven ships. You write it: repeated read-only errors, write failures and pool exhaustion from your own application.
