# Flashcards — Self-fencing: the sidecar's two rules

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** FencingMonitor rule #1

**Back:** Topology mismatch: the cached operator-authoritative active site is non-empty and is not this site, so fence immediately and return without consulting the lease.

---

**Front:** FencingMonitor rule #2

**Back:** Lease expiry: the operator AND every peer have been silent for longer than leaseTimeout.

---

**Front:** First thing a FencingMonitor tick does, before either rule

**Back:** Reads @@read_only and returns early if it is set — a read-only instance never self-fences.

---

**Front:** spec.sidecar.leaseTimeout and spec.sidecar.peerCheckInterval defaults

**Back:** leaseTimeout 20s, peerCheckInterval 5s.

---

**Front:** The three CEL invariants on spec.sidecar

**Back:** peerCheckInterval >= 1s, leaseTimeout >= 3s, and leaseTimeout >= 3 x peerCheckInterval.

---

**Front:** TopologyCache.Adopt

**Back:** Writes a peer-relayed view only when its observedAt is strictly newer than the cached value; otherwise it returns false and changes nothing.

---

**Front:** TopologyCache.Set

**Back:** Overwrites the cached active site unconditionally, with no timestamp comparison, because the operator is always authoritative.

---

**Front:** What setting super_read_only=ON does to read_only (and the reverse)

**Back:** Setting super_read_only=ON implicitly forces read_only=ON; setting read_only=OFF implicitly forces super_read_only=OFF.

---

**Front:** Log line: `safety net: no active site reported by operator, staying fenced`

**Back:** The operator answered the startup query but named no active site, so this pod stays fenced and has never been allowed to write.

---

**Front:** What a fenced MySQL site can still do

**Back:** Keep applying replication — replication threads are permitted under super_read_only — so a fenced site remains a working replica.
