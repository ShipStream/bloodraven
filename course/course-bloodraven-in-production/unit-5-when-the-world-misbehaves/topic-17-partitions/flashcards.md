# Flashcards — Five partitions, five answers

<!-- Rendered from course.json by course-template/tools/render-views.mjs.
     Edit course.json, then re-render. Edits here are overwritten. -->

**Front:** Partition scenario A

**Back:** The operator cannot reach site A while site B is reachable — the only one of the five shapes that produces an automatic promotion. Exercised by chaos scenarios 09 and 06 plus the DST fault `partitionOperatorSite`.

---

**Front:** Partition scenario B

**Back:** The replica site is isolated while the primary stays reachable to the operator. Exercised by chaos scenario 17, a negative-assertion suite that fails if a failover or a self-fence ever happens.

---

**Front:** Partition scenario C

**Back:** The MySQL-to-MySQL link is broken while the operator still polls both sites cleanly. Covered by the deterministic simulator only, as the `partitionPair` fault — no live chaos scenario exercises it.

---

**Front:** Partition scenario D

**Back:** Asymmetric peer reachability — `iad` reaches `pdx` but `pdx` cannot reach `iad`. Analysis only: the DST fault model keys link state on a symmetric `pairKey`, so one-way reachability is unrepresentable and no test of any kind exercises this shape.

---

**Front:** Partition scenario E

**Back:** Every site is unreachable to the operator — total loss, no promotion, because no reachable candidate exists. Covered only indirectly, by chaos scenario 11, which scales every site to zero rather than breaking a network.

---

**Front:** Why a broken cross-site replication link triggers no automatic operator action

**Back:** Because from the operator's point of view it is indistinguishable from "the replica fell behind because of I/O pressure" — the observable evidence is identical, so human judgement decides.

---

**Front:** Injection trap: host-netns iptables rules on a k3d node

**Back:** They do not partition Kubernetes Service traffic — kube-proxy's DNAT happens in different chains, so the operator keeps reaching MySQL through the ClusterIP while you believe the link is severed.

---

**Front:** Injection trap: how a NetworkPolicy becomes a silent no-op

**Back:** A CNI that evaluates the policy post-DNAT sees the backend pod IP, so a rule written against the Service ClusterIP never matches — chaos 33's DNS deny kept resolving through the whole 45-second hold.

---

**Front:** What Kubernetes does with a pod on a node that has become unreachable

**Back:** Nothing. It refuses to delete the pod, which sits `Terminating` or `Unknown` indefinitely — deliberately, to protect at-most-one identity.

---

**Front:** What force-deleting a stuck `Terminating` pod costs you

**Back:** At-most-one identity: the API object disappears while the process may still be running on the partitioned node.

---

**Front:** Who sets a pod's `Terminating` status

**Back:** The API server, not the kubelet — which is why on an unreachable node the container never gets the message and keeps running.

---

**Front:** What `ReadWriteOnce` actually restricts

**Back:** Access to one node, not one pod — storage attach is not a fencing mechanism.
