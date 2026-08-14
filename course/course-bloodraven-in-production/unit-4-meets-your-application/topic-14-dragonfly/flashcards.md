# Flashcards — Cache and sessions that follow the primary

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** What class of state is Bloodraven's Dragonfly integration for?

**Back:** Cache and session state only, never durable application data — the CRD calls it "non-durable cache/session state: emergency failover never blocks on it".

---

**Front:** Which two labels does the active Dragonfly Service AND-gate?

**Back:** shipstream.io/dragonfly-role=master AND shipstream.io/dragonfly-traffic=enabled — a pod is an endpoint only when both match.

---

**Front:** You need to shed a Dragonfly pod from the active Service. What does the operator do to the traffic label?

**Back:** Deletes the key outright. The selector is an exists-and-equals check on "enabled", so a missing key sheds the endpoint with no ambiguity.

---

**Front:** You find the log line "dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)". Which branch ran?

**Back:** The emergency fallback: REPLTAKEOVER was tried first and failed, so the target was promoted as an empty master.

---

**Front:** What does the planned Dragonfly path do when REPLTAKEOVER fails?

**Back:** There is no REPLICAOF NO ONE fallback. onSyncTimeout decides: proceed continues flagged unpreserved, fail rolls back the MySQL fence.

---

**Front:** How long is the emergency Dragonfly budget, and where is it set?

**Back:** 10 seconds, hard-coded in the operator (const budget = 10 * time.Second). It is not configurable.

---

**Front:** Where does status.plannedFailover.dragonfly.sessionsPreserved come from after an emergency promotion?

**Back:** Nowhere — emergency promotions never write the field. The outcome lives only in the log line and the Kubernetes event.

---

**Front:** spec.dragonfly.plannedFailover.maxSyncWait — default and second job?

**Back:** 30s. As well as bounding WaitingForDragonflySync it is passed as the REPLTAKEOVER timeout argument.

---

**Front:** Why does the Dragonfly client set its read deadline to maxSyncWait + 5 s?

**Back:** So the client cannot give up before the server has spent its full drain budget and leave the caller unsure whether the promotion happened. At the 30 s default that is a 35 s deadline.

---

**Front:** onSyncTimeout — allowed values and default?

**Back:** An enum of proceed or fail, defaulting to proceed: the promotion goes ahead and the cache outcome is whatever it is.

---

**Front:** What kind of command is REPLTAKEOVER, and when did it appear?

**Back:** An ADMIN-port, GLOBAL_TRANS command taking a timeout in seconds, introduced in Dragonfly v1.5.0 and never given an official documentation page.

---

**Front:** Which two CEL rules does the CRD actually enforce on spec.dragonfly?

**Back:** An image is required when Dragonfly is enabled, and the image may not be tagged :latest. Nothing checks the Dragonfly version.
