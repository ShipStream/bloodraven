/**
 * Scripted replay of the `/deploy/v1` deployment coordination contract.
 *
 * Nothing here talks to a cluster. Each scenario is an ordered list of steps
 * on a virtual clock; the steps mutate one reactive world (group status, the
 * deploy clients, the leases) and append a wire line. The lane instrument
 * renders from that world, so the drawn bars can never disagree with the wire
 * log.
 *
 * Every status code, reason string and field name is taken from
 * docs/configuration/deployment-api: TTL clamp [5,120], renew at ttl/3,
 * 201/200 grant, 409 held, 423 unstable, 409 revoked (topology_changed),
 * 204 release, Deferred/DeploymentHold, topologyGeneration fencing.
 */
export const DEPLOY_CLOCK_RATE = 2.2
export const DEPLOY_TICK_MS = 50
export const DEPLOY_TTL = 15

export type LeaseKind = 'migration' | 'failover-hold'
export type LeaseState = 'active' | 'released' | 'revoked'
export type WireTone = 'muted' | 'req' | 'ok' | 'warn' | 'danger' | 'primary'
export type Actor = 'a' | 'b' | 'operator' | 'chaos'
export type ClientPhase = 'idle' | 'observing' | 'holding' | 'migrating' | 'done' | 'blocked' | 'fenced'
export type SiteRole = 'primary' | 'replica' | 'unreachable' | 'fenced'
export type PlannedPhase = '' | 'Requested' | 'Deferred' | 'Promoting' | 'Complete'
export type ScenarioId = 'mutex' | 'hold' | 'fence'

export interface Lease {
  kind: LeaseKind
  operationId: string
  instance: string
  grantedAt: number
  expiresAt: number
  topologyGeneration: number
  state: LeaseState
  endedAt?: number
  endReason?: 'released' | 'topology_changed' | 'operator_revoked'
  renewals: { at: number, expiresAt: number }[]
}

export interface DeployClient {
  id: 'a' | 'b'
  operationId: string
  instance: string
  phase: ClientPhase
  detail: string
  ddl: number
  /** Time of the most recent request, used by the instruments to flash. */
  lastAt: number
  lastStatus: string
}

export interface GroupWorld {
  activeSite: 'iad' | 'pdx'
  topologyGeneration: number
  stable: boolean
  unstableReason: string
  planned: { phase: PlannedPhase, reason: string, retryAfter: number | null, requestedAt: number | null, promotingAt: number | null, doneAt: number | null }
  sites: { iad: SiteRole, pdx: SiteRole }
  generations: { at: number, gen: number, site: 'iad' | 'pdx' }[]
  leases: Lease[]
  event: string
  /** When the active site was lost, for the emergency branch. */
  siteLostAt: number | null
}

export interface WireLine {
  at: number
  t: string
  actor: Actor
  tone: WireTone
  text: string
}

export interface Step {
  at: number
  actor: Actor
  tone: WireTone
  text: string
  apply?: (w: GroupWorld, c: { a: DeployClient, b: DeployClient }) => void
}

export interface Scenario {
  id: ScenarioId
  label: string
  claim: string
  steps: Step[]
}

export const OP_A = 'a1c3f0e2'
export const OP_B = 'b7f04d19'

export function fmtT(seconds: number) {
  return `T+${seconds.toFixed(1).padStart(4, '0')}s`
}

function findLease(w: GroupWorld, op: string, kind: LeaseKind) {
  return w.leases.find(l => l.operationId === op && l.kind === kind && l.state === 'active')
}

function grant(w: GroupWorld, op: string, kind: LeaseKind, at: number) {
  w.leases.push({
    kind,
    operationId: op,
    instance: 'orders-app',
    grantedAt: at,
    expiresAt: at + DEPLOY_TTL,
    topologyGeneration: w.topologyGeneration,
    state: 'active',
    renewals: [],
  })
}

function renew(w: GroupWorld, op: string, kind: LeaseKind, at: number) {
  const lease = findLease(w, op, kind)
  if (!lease) return
  lease.expiresAt = at + DEPLOY_TTL
  lease.renewals.push({ at, expiresAt: lease.expiresAt })
  if (w.planned.phase === 'Deferred' && kind === 'failover-hold') w.planned.retryAfter = lease.expiresAt
}

function release(w: GroupWorld, op: string, kind: LeaseKind, at: number) {
  const lease = findLease(w, op, kind)
  if (!lease) return
  lease.state = 'released'
  lease.endedAt = at
  lease.endReason = 'released'
}

function bumpGeneration(w: GroupWorld, site: 'iad' | 'pdx', at: number) {
  w.topologyGeneration += 1
  w.activeSite = site
  w.generations.push({ at, gen: w.topologyGeneration, site })
  for (const lease of w.leases) {
    if (lease.state !== 'active') continue
    lease.state = 'revoked'
    lease.endedAt = at
    lease.endReason = 'topology_changed'
  }
}

/** The acquisition every scenario starts with: observe, then migration, then hold. */
function acquisition(): Step[] {
  return [
    { at: 0.0, actor: 'a', tone: 'req', text: 'GET /deploy/v1/groups/orders', apply: (_w, c) => { c.a.phase = 'observing'; c.a.detail = 'observing group'; c.a.lastAt = 0; c.a.lastStatus = 'GET' } },
    { at: 0.4, actor: 'a', tone: 'ok', text: '200 stable=true activeSite=iad topologyGeneration=17 leases=[]', apply: (_w, c) => { c.a.lastAt = 0.4; c.a.lastStatus = '200' } },
    { at: 0.9, actor: 'a', tone: 'req', text: `POST /deploy/v1/groups/orders/leases {kind:migration, operationId:${OP_A}…, instance:orders-app, ttlSeconds:15}`, apply: (_w, c) => { c.a.lastAt = 0.9; c.a.lastStatus = 'POST' } },
    { at: 1.3, actor: 'a', tone: 'ok', text: '201 token=•••• expiresAt=T+16.3s topologyGeneration=17', apply: (w, c) => { grant(w, OP_A, 'migration', 1.3); c.a.phase = 'holding'; c.a.detail = 'migration mutex held'; c.a.lastAt = 1.3; c.a.lastStatus = '201' } },
    { at: 1.7, actor: 'a', tone: 'req', text: `POST /deploy/v1/groups/orders/leases {kind:failover-hold, operationId:${OP_A}…, ttlSeconds:15}`, apply: (_w, c) => { c.a.lastAt = 1.7; c.a.lastStatus = 'POST' } },
    { at: 2.1, actor: 'a', tone: 'ok', text: '201 token=•••• expiresAt=T+17.1s topologyGeneration=17', apply: (w, c) => { grant(w, OP_A, 'failover-hold', 2.1); c.a.detail = 'mutex + hold · renew every 5s'; c.a.lastAt = 2.1; c.a.lastStatus = '201' } },
    { at: 2.8, actor: 'a', tone: 'muted', text: 'ALTER TABLE orders ADD COLUMN fulfillment_site VARCHAR(8) (1/3)', apply: (_w, c) => { c.a.phase = 'migrating'; c.a.ddl = 1; c.a.detail = 'DDL 1/3 · schema v41 → v42' } },
  ]
}

function renewBoth(at: number, expiresAt: number): Step[] {
  return [
    { at, actor: 'a', tone: 'req', text: `PUT /deploy/v1/groups/orders/leases/migration/${OP_A}… {token, ttlSeconds:15}`, apply: (_w, c) => { c.a.lastAt = at; c.a.lastStatus = 'PUT' } },
    { at: at + 0.2, actor: 'a', tone: 'ok', text: `200 expiresAt=T+${expiresAt.toFixed(1)}s topologyGeneration=17`, apply: (w, c) => { renew(w, OP_A, 'migration', at + 0.2); c.a.lastAt = at + 0.2; c.a.lastStatus = '200' } },
    { at: at + 0.4, actor: 'a', tone: 'req', text: `PUT /deploy/v1/groups/orders/leases/failover-hold/${OP_A}… {token, ttlSeconds:15}`, apply: (_w, c) => { c.a.lastAt = at + 0.4; c.a.lastStatus = 'PUT' } },
    { at: at + 0.6, actor: 'a', tone: 'ok', text: `200 expiresAt=T+${(expiresAt + 0.4).toFixed(1)}s topologyGeneration=17`, apply: (w, c) => { renew(w, OP_A, 'failover-hold', at + 0.6); c.a.lastAt = at + 0.6; c.a.lastStatus = '200' } },
  ]
}

function releaseBoth(at: number): Step[] {
  return [
    { at, actor: 'a', tone: 'req', text: `DELETE /deploy/v1/groups/orders/leases/failover-hold/${OP_A}… X-Lease-Token: ••••`, apply: (_w, c) => { c.a.lastAt = at; c.a.lastStatus = 'DELETE' } },
    { at: at + 0.2, actor: 'a', tone: 'ok', text: '204 hold released · planned disruption may proceed', apply: (w, c) => { release(w, OP_A, 'failover-hold', at + 0.2); c.a.lastAt = at + 0.2; c.a.lastStatus = '204' } },
    { at: at + 0.6, actor: 'a', tone: 'req', text: `DELETE /deploy/v1/groups/orders/leases/migration/${OP_A}… X-Lease-Token: ••••`, apply: (_w, c) => { c.a.lastAt = at + 0.6; c.a.lastStatus = 'DELETE' } },
    { at: at + 0.8, actor: 'a', tone: 'ok', text: '204 migration mutex released', apply: (w, c) => { release(w, OP_A, 'migration', at + 0.8); c.a.phase = 'done'; c.a.detail = 'deploy complete · schema v42'; c.a.lastAt = at + 0.8; c.a.lastStatus = '204' } },
  ]
}

const mutex: Scenario = {
  id: 'mutex',
  label: 'Two deploys collide',
  claim: 'One migration per group. The second deploy gets 409 held, with the holder, and waits its turn.',
  steps: [
    ...acquisition(),
    { at: 3.6, actor: 'b', tone: 'req', text: 'GET /deploy/v1/groups/orders', apply: (_w, c) => { c.b.phase = 'observing'; c.b.detail = 'observing group'; c.b.lastAt = 3.6; c.b.lastStatus = 'GET' } },
    { at: 4.0, actor: 'b', tone: 'ok', text: `200 stable=true topologyGeneration=17 leases=[migration ${OP_A}…, failover-hold ${OP_A}…]`, apply: (_w, c) => { c.b.lastAt = 4.0; c.b.lastStatus = '200' } },
    { at: 4.5, actor: 'b', tone: 'req', text: `POST /deploy/v1/groups/orders/leases {kind:migration, operationId:${OP_B}…, instance:orders-app, ttlSeconds:15}`, apply: (_w, c) => { c.b.lastAt = 4.5; c.b.lastStatus = 'POST' } },
    { at: 4.9, actor: 'b', tone: 'warn', text: `409 held holder={operationId:${OP_A}…, instance:orders-app, expiresAt:T+16.3s}`, apply: (_w, c) => { c.b.phase = 'blocked'; c.b.detail = `409 held · waiting on ${OP_A}…`; c.b.lastAt = 4.9; c.b.lastStatus = '409' } },
    ...renewBoth(6.3, 21.5),
    { at: 8.0, actor: 'a', tone: 'muted', text: 'ALTER TABLE shipments ADD COLUMN routed_at DATETIME (2/3)', apply: (_w, c) => { c.a.ddl = 2; c.a.detail = 'DDL 2/3 · schema v41 → v42' } },
    ...renewBoth(11.3, 26.5),
    { at: 12.6, actor: 'a', tone: 'muted', text: 'CREATE INDEX ix_orders_site ON orders (fulfillment_site) (3/3)', apply: (_w, c) => { c.a.ddl = 3; c.a.detail = 'DDL 3/3 · deploy state recorded' } },
    ...releaseBoth(13.6),
    { at: 15.2, actor: 'b', tone: 'req', text: `POST /deploy/v1/groups/orders/leases {kind:migration, operationId:${OP_B}…, instance:orders-app, ttlSeconds:15}`, apply: (_w, c) => { c.b.lastAt = 15.2; c.b.lastStatus = 'POST' } },
    { at: 15.6, actor: 'b', tone: 'ok', text: '201 token=•••• expiresAt=T+30.6s topologyGeneration=17', apply: (w, c) => { grant(w, OP_B, 'migration', 15.6); c.b.phase = 'holding'; c.b.detail = 'migration mutex held · same generation'; c.b.lastAt = 15.6; c.b.lastStatus = '201' } },
    { at: 16.0, actor: 'b', tone: 'req', text: `POST /deploy/v1/groups/orders/leases {kind:failover-hold, operationId:${OP_B}…, ttlSeconds:15}`, apply: (_w, c) => { c.b.lastAt = 16.0; c.b.lastStatus = 'POST' } },
    { at: 16.4, actor: 'b', tone: 'ok', text: '201 token=•••• expiresAt=T+31.4s topologyGeneration=17', apply: (w, c) => { grant(w, OP_B, 'failover-hold', 16.4); c.b.lastAt = 16.4; c.b.lastStatus = '201' } },
    { at: 17.0, actor: 'b', tone: 'muted', text: `migration ${OP_B}… running · serialized behind ${OP_A}… · never concurrent`, apply: (_w, c) => { c.b.phase = 'migrating'; c.b.ddl = 1; c.b.detail = 'DDL 1/2 · serialized, not raced' } },
  ],
}

const hold: Scenario = {
  id: 'hold',
  label: 'Planned failover mid-deploy',
  claim: 'A live hold defers planned promotion until the deploy releases it. The switchover then proceeds on its own.',
  steps: [
    ...acquisition(),
    { at: 3.8, actor: 'operator', tone: 'primary', text: 'kubectl bloodraven promote orders --to pdx', apply: (w) => { w.planned.phase = 'Requested'; w.planned.reason = ''; w.planned.requestedAt = 3.8 } },
    { at: 4.3, actor: 'operator', tone: 'warn', text: `plannedFailover phase=Deferred reason=DeploymentHold retryAfter=T+17.1s · blocked by ${OP_A}… (orders-app)`, apply: (w) => { w.planned.phase = 'Deferred'; w.planned.reason = 'DeploymentHold'; w.planned.retryAfter = 17.1; w.stable = false; w.unstableReason = 'PlannedFailover'; w.event = 'PlannedFailoverDeferred' } },
    ...renewBoth(6.3, 21.5),
    { at: 7.2, actor: 'operator', tone: 'muted', text: 'planned failover still Deferred · retryAfter follows the renewed hold (T+21.9s)' },
    { at: 8.0, actor: 'a', tone: 'muted', text: 'ALTER TABLE shipments ADD COLUMN routed_at DATETIME (2/3)', apply: (_w, c) => { c.a.ddl = 2; c.a.detail = 'DDL 2/3 · schema v41 → v42' } },
    ...renewBoth(11.3, 26.5),
    { at: 12.6, actor: 'a', tone: 'muted', text: 'CREATE INDEX ix_orders_site ON orders (fulfillment_site) (3/3)', apply: (_w, c) => { c.a.ddl = 3; c.a.detail = 'DDL 3/3 · deploy state recorded' } },
    ...releaseBoth(13.6),
    { at: 15.0, actor: 'operator', tone: 'primary', text: 'no hold remains · planned failover revalidates: replication running, lag 0s, GTID caught up', apply: (w) => { w.planned.phase = 'Promoting'; w.planned.reason = ''; w.planned.retryAfter = null; w.planned.promotingAt = 15.0; w.event = '' } },
    { at: 15.8, actor: 'operator', tone: 'ok', text: 'pdx promoted · activeSite=pdx · topologyGeneration=18 · 0 leases to revoke', apply: (w) => { bumpGeneration(w, 'pdx', 15.8); w.sites.iad = 'fenced'; w.sites.pdx = 'primary'; w.planned.phase = 'Complete'; w.planned.doneAt = 15.8 } },
    { at: 16.6, actor: 'operator', tone: 'muted', text: 'iad rejoining as replica · group stable=true', apply: (w) => { w.sites.iad = 'replica'; w.stable = true; w.unstableReason = '' } },
  ],
}

const fence: Scenario = {
  id: 'fence',
  label: 'Site dies mid-migration',
  claim: 'Emergency failover never waits for a hold. The generation moves, the lease is revoked, and the migration stops instead of continuing on the new primary.',
  steps: [
    ...acquisition(),
    ...renewBoth(6.3, 21.5),
    { at: 8.0, actor: 'a', tone: 'muted', text: 'ALTER TABLE shipments ADD COLUMN routed_at DATETIME (2/3)', apply: (_w, c) => { c.a.ddl = 2; c.a.detail = 'DDL 2/3 · schema v41 → v42' } },
    { at: 8.8, actor: 'chaos', tone: 'danger', text: 'chaos: site iad lost (node gone)', apply: (w) => { w.sites.iad = 'unreachable'; w.stable = false; w.unstableReason = 'Degraded'; w.siteLostAt = 8.8 } },
    { at: 9.0, actor: 'operator', tone: 'warn', text: 'probe iad: dial tcp 10.0.1.1:3306 i/o timeout (1/3)' },
    { at: 9.9, actor: 'operator', tone: 'warn', text: 'probe iad: dial tcp 10.0.1.1:3306 i/o timeout (3/3) · failureThreshold reached' },
    { at: 10.3, actor: 'operator', tone: 'danger', text: 'emergency failover · deployment leases not consulted · candidate pdx: freshest GTID set', apply: (w) => { w.unstableReason = 'NoPrimary' } },
    { at: 11.0, actor: 'operator', tone: 'ok', text: 'pdx promoted · activeSite=pdx · topologyGeneration=18', apply: (w) => { bumpGeneration(w, 'pdx', 11.0); w.sites.pdx = 'primary'; w.unstableReason = 'RecoveryInProgress' } },
    { at: 11.2, actor: 'operator', tone: 'danger', text: `event DeploymentLeaseRevoked: migration + failover-hold for ${OP_A}… revoked: topology_changed`, apply: (w) => { w.event = 'DeploymentLeaseRevoked' } },
    { at: 11.5, actor: 'a', tone: 'req', text: `PUT /deploy/v1/groups/orders/leases/migration/${OP_A}… {token, ttlSeconds:15}`, apply: (_w, c) => { c.a.lastAt = 11.5; c.a.lastStatus = 'PUT' } },
    { at: 11.8, actor: 'a', tone: 'danger', text: '409 revoked reason=topology_changed topologyGeneration=18', apply: (_w, c) => { c.a.phase = 'fenced'; c.a.detail = '409 revoked · gen 17 ≠ 18 · heartbeat stopped'; c.a.lastAt = 11.8; c.a.lastStatus = '409' } },
    { at: 12.4, actor: 'a', tone: 'danger', text: 'stop: DDL at 2/3 · not reconnecting to pdx · manual schema verification required', apply: (_w, c) => { c.a.detail = 'fenced at DDL 2/3 · verify schema before retry' } },
    { at: 13.4, actor: 'b', tone: 'req', text: `POST /deploy/v1/groups/orders/leases {kind:migration, operationId:${OP_B}…, instance:orders-app, ttlSeconds:15}`, apply: (_w, c) => { c.b.phase = 'observing'; c.b.detail = 'retry deploy'; c.b.lastAt = 13.4; c.b.lastStatus = 'POST' } },
    { at: 13.8, actor: 'b', tone: 'warn', text: '423 unstable reason=RecoveryInProgress retryAfterSeconds=5 · old primary iad still being reconfigured', apply: (_w, c) => { c.b.phase = 'blocked'; c.b.detail = '423 unstable · retry in 5s'; c.b.lastAt = 13.8; c.b.lastStatus = '423' } },
    { at: 14.6, actor: 'operator', tone: 'muted', text: `revocation tombstone kept for ${OP_A}… · a surviving old client can never re-acquire silently` },
  ],
}

export const DEPLOY_SCENARIOS: Scenario[] = [mutex, hold, fence]

export function scenarioById(id: ScenarioId) {
  return DEPLOY_SCENARIOS.find(s => s.id === id) || mutex
}

export function scenarioDuration(s: Scenario) {
  return s.steps[s.steps.length - 1]!.at + 1.2
}

function freshWorld(): GroupWorld {
  return {
    activeSite: 'iad',
    topologyGeneration: 17,
    stable: true,
    unstableReason: '',
    planned: { phase: '', reason: '', retryAfter: null, requestedAt: null, promotingAt: null, doneAt: null },
    sites: { iad: 'primary', pdx: 'replica' },
    generations: [{ at: 0, gen: 17, site: 'iad' }],
    leases: [],
    event: '',
    siteLostAt: null,
  }
}

function freshClient(id: 'a' | 'b'): DeployClient {
  return {
    id,
    operationId: id === 'a' ? OP_A : OP_B,
    instance: 'orders-app',
    phase: 'idle',
    detail: id === 'a' ? 'deploy pipeline · release 2.14' : 'deploy pipeline · release 2.15',
    ddl: 0,
    lastAt: -1,
    lastStatus: '',
  }
}

/**
 * One virtual clock per instrument. The world is rebuilt on every reset so a
 * replay starts from the same generation and lease set every time.
 */
export function useDeployReplay(initial: ScenarioId = 'mutex') {
  const scenarioId = ref<ScenarioId>(initial)
  const scenario = computed(() => scenarioById(scenarioId.value))
  const duration = computed(() => scenarioDuration(scenario.value))

  const world = reactive<GroupWorld>(freshWorld())
  const clients = reactive({ a: freshClient('a'), b: freshClient('b') })
  const wire = ref<WireLine[]>([])
  const clock = ref(0)
  const running = ref(false)
  const finished = ref(false)

  let timer: ReturnType<typeof setInterval> | null = null
  let cursor = 0

  function stop() {
    if (timer) clearInterval(timer)
    timer = null
    running.value = false
  }

  function reset() {
    stop()
    cursor = 0
    clock.value = 0
    finished.value = false
    wire.value = []
    Object.assign(world, freshWorld())
    Object.assign(clients.a, freshClient('a'))
    Object.assign(clients.b, freshClient('b'))
  }

  /** Apply every step at or before `t`. Used by run() and by seek(). */
  function advanceTo(t: number) {
    const steps = scenario.value.steps
    while (cursor < steps.length && steps[cursor]!.at <= t) {
      const step = steps[cursor]!
      step.apply?.(world, clients)
      wire.value.push({ at: step.at, t: fmtT(step.at), actor: step.actor, tone: step.tone, text: step.text })
      cursor++
    }
  }

  /** Start the clock from wherever it is. */
  function start() {
    if (timer || finished.value) return
    running.value = true
    timer = setInterval(() => {
      clock.value += (DEPLOY_TICK_MS / 1000) * DEPLOY_CLOCK_RATE
      advanceTo(clock.value)
      if (clock.value >= duration.value) {
        clock.value = duration.value
        stop()
        finished.value = true
      }
    }, DEPLOY_TICK_MS)
  }

  function run() {
    reset()
    start()
  }

  /** Deterministic jump, for reduced motion and visual testing. */
  function seek(t: number) {
    reset()
    clock.value = Math.min(t, duration.value)
    advanceTo(clock.value)
    finished.value = clock.value >= duration.value
  }

  function select(id: ScenarioId, andRun = true) {
    scenarioId.value = id
    if (andRun) run()
    else reset()
  }

  onBeforeUnmount(stop)

  return { scenarioId, scenario, duration, world, clients, wire, clock, running, finished, run, start, reset, seek, select, stop }
}
