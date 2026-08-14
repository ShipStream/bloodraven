---
seo:
  title: Bloodraven - MySQL failover for Kubernetes
  description: Kubernetes operator for MySQL async replication failover groups across sites. Automatic promotion, DNS steering, split-brain fencing, clone bootstrap, encryption and backups.
  # Setting ogImage makes Docus' landing template skip its generated OG card and
  # use the brand lockup instead.
  ogImage: /img/brand/og.png
---

::home-hero
---
kicker: Kubernetes operator for MySQL
title: Give your MySQL topology
accent: a survival instinct.
description: >-
  Bloodraven runs MySQL async replication failover groups across sites. It owns detection,
  fencing, promotion, DNS steering, clone bootstrap and backups — so losing an entire site
  is a status change, not an incident.
links:
  - label: Get started
    to: /docs/get-started/getting-started
    variant: primary
    icon: i-lucide-arrow-right
  - label: Try the playground
    to: /docs/get-started/playground
    variant: outline
    icon: i-lucide-flask-conical
  - label: GitHub
    to: https://github.com/ShipStream/bloodraven
    target: _blank
    variant: ghost
    icon: i-simple-icons-github
---
::

::home-stats
---
items:
  - value: "~6s"
    label: to declare a site unreachable (3 failed 2s polls)
  - value: "RPO 0"
    label: on a planned switchover, by construction
  - value: "47+"
    label: scripted chaos scenarios run in CI
  - value: "1"
    label: CRD, one controller, one reconcile loop
---
::

::home-paths
---
kicker: Three ways in
source-label: github.com/ShipStream/bloodraven
source-to: https://github.com/ShipStream/bloodraven
paths:
  - n: "01"
    meta: Docs
    label: Get started
    title: Install the operator
    description: >-
      Prerequisites, the Helm chart, and your first two-site MysqlFailoverGroup —
      with the checks that tell you it actually converged.
    to: /docs/get-started/getting-started
  - n: "02"
    meta: ~2 min
    label: Playground
    title: Break it on a laptop
    description: >-
      One script stands up two MySQL sites, a live dashboard, a counter app and a
      chaos menu on k3d, kind or minikube.
    to: /docs/get-started/playground
  - n: "03"
    meta: Free
    label: Course
    title: Train the rotation
    description: >-
      Bloodraven in Production — seven hands-on units on a real cluster your team
      breaks on purpose, with graded projects.
    to: /courses/
    target: _blank
---
::

::home-ask-ai
---
kicker: Sixty pages of docs, one prompt
title: Don't read 60 pages.
accent: Just ask.
description: >-
  The assistant is grounded in this entire documentation set — the CRD reference, every
  runbook, the failure-mode matrix and the log schema. Ask it in your own words and it
  answers with links to the exact page.
llms: bloodraven.dev/llms-full.txt
questions:
  - How does Bloodraven decide which site to promote?
  - Show me a minimal two-site MysqlFailoverGroup.
  - What is my RPO when the primary site dies?
  - How do I schedule encrypted backups to S3?
  - A site is stuck in RecoveryBlocked — what now?
  - Which alerts should page someone at 3am?
---
::

::home-safety
---
# Target of the header's "Features" nav link.
id: features
kicker: Detection · fencing · promotion
title: Losing a site
title-two: should be boring.
lede: >-
  The operator watches every site on a two-second poll, decides with a small documented
  state machine, and executes the same promotion sequence every time.
cards:
  - token: DNS
    title: Automated DNS failover
    description: >-
      Be genuinely geo-redundant without a global load balancer in front of your database.
      Bloodraven writes an external-dns DNSEndpoint with your hostname and TTL, and moves
      it to the promoted site as part of the promotion sequence — no cross-region LB bill,
      no anycast VIP, no proxy hop on every query.
    detail: external-dns DNSEndpoint · your hostname · your TTL
    to: /docs/architecture/multi-site
  - token: SAFE
    title: Split-brain safe, and tested that way
    description: >-
      Two sites never both accept writes. The operator fences the old primary, and each
      sidecar self-fences with super_read_only when it can reach neither the operator nor
      its peer. GTID divergence is detected, reported in divergentGtid, and blocks an
      unsafe rejoin until a human decides.
    detail: operator fencing · sidecar super_read_only · divergentGtid
    to: /docs/operations/network-partitions
  - token: ROLL
    title: Zero-downtime updates
    description: >-
      OrderedUpdate upgrades the standby first, fails over to it, then upgrades the old
      active — the direction MySQL's rolling-upgrade contract requires. Node taints and the
      placement contract keep application workloads on the same site as the writable MySQL
      as the primary moves.
    detail: OrderedUpdate · standby first · placement contract
    to: /docs/operations/upgrade-policy
---
::

::home-data
---
kicker: Bootstrap, scale out, encrypt, back up
title: Data that looks
title-two: after itself.
lede: >-
  Bootstrap, read scale-out, encryption and backup are part of the operator, not four more
  systems you have to wire together.
cards:
  - token: CLONE
    title: Clone-based bootstrap
    description: >-
      New replicas seed themselves with MySQL's clone plugin and pick up replication with
      GTID auto-positioning. No mysqldump window, no snapshot juggling, no manual data
      transfer. The same path repairs a site whose PVC was lost, or one you deliberately
      reclone after divergence.
    to: /docs/operations/failover#failover-sequence
    code: |-
      CLONE INSTANCE FROM 'replicator'@'orders-iad:3306' …
      CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION = 1;
      START REPLICA;
  - token: AES
    title: Data-at-rest encryption, for free
    description: >-
      InnoDB tablespace encryption on ordinary PVCs, using the GPL keyring component that
      ships with MySQL Community Edition. No Oracle Enterprise licence, no encrypted CSI
      storage class, and the master key never lands on the data PVC or a worker-node disk.
      Rotation, sealing and escrow are handled by the operator.
    to: /docs/configuration/encryption-at-rest
    code: |-
      spec:
        encryptionAtRest:
          enabled: true
  - token: RO
    title: Read-only replicas
    description: >-
      Append a read-only site and it follows whichever site is active — never promoted,
      never a planned-failover target, never a DNS target, never a clone donor. It gets
      its own client Service and its own mysqlConf, so you can size it for reporting,
      analytics or a CDC tap without touching the write path. Fall behind
      readOnlyMaxLagSeconds and its endpoint sheds until it catches up; lose its data and
      it reclones itself.
    to: /docs/architecture/multi-site
    code: |-
      - name: reader
        role: read-only
        zone: us-east-1b
backup-title: Backup and restore.
backup-title-two: The whole nine yards.
backup-to: /docs/backup-and-restore/backup-overview
backup-description: >-
  Not a cron job that shells out to mysqldump. Backup is a first-class part of the
  operator, from the schedule all the way through to proving the artifact can actually be
  restored.
backup-points:
  - S3 or PVC — object storage for durability, PVC for labs
  - Scheduled and on-demand — a MysqlBackup whenever you need one
  - Structured retention — plus exponential-backoff retries on failure
  - Point-in-time recovery — binlog archiving between full dumps
  - Encrypted artifacts — application-level, independent of the store
  - Prometheus metrics — every run, every failure, every sweep
  - Automatic cleanup — deleting the object removes its artifacts
  - Verification — load the dump into a throwaway MySQL and prove it
  - initFromBackup — bootstrap a brand-new failover group from a dump
flow:
  - n: "01"
    title: Dump
    detail: consistent, scheduled or on demand
  - n: "02"
    title: Retain
    detail: S3 or PVC · structured retention
  - n: "03"
    title: Verify
    detail: restore into a throwaway MySQL
  - n: "04"
    title: Bootstrap
    detail: initFromBackup into a new group
---
::

::home-control-plane
---
kicker: No quorum. No coordinator.
title: One controller. One CRD.
title-two: One loop.
lede: >-
  There is no distributed consensus in Bloodraven, no coordinator to keep quorum, and no
  second system to reconcile against. A single reconcile loop reads the observed topology
  and writes the decision — which is why the failure modes fit on one page.
filename: orders.yaml
link-label: Read the failure-mode matrix
link-to: /docs/operations/failure-mode-matrix
points:
  - title: No coordination problem
    description: >-
      Nothing to elect, nothing to split. State lives in the CR's status and in MySQL itself.
  - title: The data plane doesn't need the operator
    description: >-
      A healthy primary and replica keep serving reads and writes with zero operator
      involvement — it is on the detection path, not the request path.
  - title: Correct even while the operator is down
    description: >-
      Sidecars self-fence when they can reach neither the operator nor their peer, so no
      split brain is possible while the control plane is missing.
  - title: Every failure mode is written down
    description: >-
      A documented matrix of faults, what the operator does, how long it takes, and what
      it costs you.
yaml: |-
  apiVersion: shipstream.io/v1alpha1
  kind: MysqlFailoverGroup
  metadata:
    name: orders
  spec:
    dns:
      hostname: orders.az.example.com
      ttl: 60
    replication:
      readOnlyMaxLagSeconds: 30
    sites:
      - name: iad
        zone: us-east-1a
        lbIP: 10.0.1.1
      - name: pdx
        zone: us-west-2a
        lbIP: 10.0.2.1
      - name: reader
        role: read-only
        zone: us-east-1b
---
::

::home-app-layer
---
kicker: The app finds out in milliseconds
title: Your application finds
title-two: out immediately.
lede: >-
  A failover the app never notices is the point. Bloodraven pushes topology changes to
  connected clients and moves your cache along with the database.
ws-label: Real-time status push
ws-title: Push the topology
ws-title-two: the instant it changes.
ws-to: /docs/observability/monitoring
ws-description: >-
  A WebSocket stream publishes the full topology of every failover group the moment it
  changes, so an app can force a pool reconnect on promotion instead of discovering it
  through a wall of write errors. A REST status API and Prometheus metrics expose the
  same state.
ws-code: |-
  const ws = new WebSocket('ws://bloodraven:8082/ws/status')
  ws.onmessage = e => pool.reconnectIfPrimaryMoved(JSON.parse(e.data))
df-label: Dragonfly follows the writer
df-title: Cache and sessions
df-title-two: move with the primary.
df-to: /docs/configuration/app-integration
df-description: >-
  Turn on spec.dragonfly and Bloodraven runs a Redis-compatible cache and session store
  per site that follows MySQL. Cache and session continuity, not another failover to
  operate.
df-steps:
  - cmd: WAIT
    detail: replica offset catches up
  - cmd: REPLTAKEOVER
    detail: promote without a restart
  - cmd: CLIENT KILL
    detail: old-master clients reconnect to the active endpoint
---
::

::home-proof
---
# Target of the header's "Proof" nav link.
id: proof
kicker: Run it on your laptop first
title: Proof,
title-two: not promises.
title-two-accent: true
lede: >-
  Every claim on this page is exercised against a real Kubernetes cluster — nightly in CI,
  and on your laptop in about two minutes.
chaos-label: Chaos-tested in CI
chaos-title: Primary kills. Partitions. Self-fencing. GTID divergence. Data wipes.
chaos-to: /docs/get-started/playground#automated-chaos-suite
chaos-description: >-
  47+ scripted chaos scenarios, including operator crashes mid-failover, rolling updates,
  Dragonfly failover, and backup and PITR verification. Each one states a hypothesis,
  injects a real fault, asserts on operator behaviour and captures full forensics when it
  fails. A smoke subset gates every release before artifacts are published.
chaos-commands:
  - make chaos-run SCENARIO=06-self-fence-isolated-primary
  - make chaos-run-all-profile PROFILE=smoke
chaos-layers:
  - DST
  - COMPONENT
  - ENVTEST
  - K3D
play-label: Interactive playground
play-title: A whole cluster.
play-title-two: One script.
play-to: /docs/get-started/playground
play-description: >-
  Two MySQL sites, a live dashboard, a counter app that writes through the failover
  hostname, DNS visualisation and a chaos menu. Break it on purpose and watch the whole
  promotion happen in front of you.
play-command: ./playground/setup.sh
play-targets:
  - k3d
  - kind
  - minikube
  - docker or podman
play-image: /img/playground.png
play-image-alt: The Bloodraven playground dashboard, showing site state, DNS records and a live event log
---
::

::home-course
---
kicker: Train the on-call rotation
title: Bloodraven
title-two: in Production.
primary-label: Start the course
primary-to: /courses/
secondary-label: Browse the docs
secondary-to: /docs
description: >-
  Train your on-call team on a real cluster, not on slides. Hold a site down and time the
  promotion. Audit the exact transactions an emergency failover cost you. Wire an
  application that survives one. Roll a MySQL upgrade underneath live traffic.
note: >-
  Every number in the course traces to a source: operator code, the shipped CRDs, recorded
  chaos-run forensics, and the MySQL manual.
stats:
  - value: "7"
    label: units
  - value: "27"
    label: topics
  - value: "34"
    label: quizzes and tests
units:
  - n: "1"
    title: Meet the group — stand up a real three-site failover group and read its status.
  - n: "2"
    title: How the operator decides — predict its next move from a status dump alone.
  - n: "3"
    title: Emergency failover end to end — time the promotion, audit what it cost.
  - n: "4"
    title: Where failover meets your application — pools, reconnects and stale reads.
  - n: "5"
    title: When the world misbehaves — self-fencing, split brain, five kinds of partition.
  - n: "6"
    title: Backups, disaster recovery and a go-live checklist you would actually sign.
  - n: "7"
    title: Day 0 and day 2 — build a group from nothing, then upgrade under live traffic.
---
::

::home-final-cta
---
mark: Bloodraven · ShipStream
title: Try it in two minutes.
title-two: Then break it on purpose.
description: >-
  Install the operator, create a failover group, then kill a site on purpose and watch it
  recover without you. Everything on this page runs on a laptop first.
links:
  - label: Get started
    to: /docs/get-started/getting-started
    variant: primary
    icon: i-lucide-arrow-right
  - label: Run the playground
    to: /docs/get-started/playground
    variant: outline
    icon: i-lucide-flask-conical
  - label: Star on GitHub
    to: https://github.com/ShipStream/bloodraven
    target: _blank
    variant: ghost
    icon: i-simple-icons-github
---
::
