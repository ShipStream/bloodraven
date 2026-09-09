<script setup lang="ts">
/**
 * Lease lanes: a time-lane instrument for the deployment API. Each deploy is a lane, its
 * two leases are bars that grow with the clock, every renewal is a heartbeat
 * tick, and the faint runway ahead of the playhead is the lease's `expiresAt`.
 * The planned-ops lane shows a switchover being deferred behind the hold, and
 * the topology lane shows `topologyGeneration` -- when it moves, a fence line
 * cuts every active lease in the same instant.
 *
 * Geometry is a pure function of the replay world and the clock; the SVG is a
 * pixel canvas (viewBox == container size) so type stays legible at any width.
 */
import { useDeployReplay, DEPLOY_TTL, OP_A, OP_B, fmtT } from '~/composables/useDeployReplay'
import type { Lease, LeaseKind, ScenarioId, WireLine } from '~/composables/useDeployReplay'

const props = defineProps<{ initialScenario?: ScenarioId, initialT?: number }>()

const replay = useDeployReplay(props.initialScenario || 'mutex')
const { scenario, duration, world, clients, wire, clock, running, finished } = replay

/* Layout ------------------------------------------------------------------ */
const width = ref(680)
const HEIGHT = 270
const compact = computed(() => width.value < 560)
const gutter = computed(() => (compact.value ? 64 : 118))
const x0 = computed(() => gutter.value + 6)
const x1 = computed(() => width.value - 14)
const scale = computed(() => (x1.value - x0.value) / duration.value)
const x = (t: number) => x0.value + Math.max(0, Math.min(t, duration.value)) * scale.value

const LANES = {
  a: { top: 12, height: 60 },
  b: { top: 78, height: 60 },
  planned: { top: 144, height: 44 },
  topology: { top: 194, height: 48 },
} as const
const AXIS_Y = 258
const GEN_Y = LANES.topology.top + 20
const ROW = { migration: 14, 'failover-hold': 34 } as const
const ROW_H = { migration: 13, 'failover-hold': 10 } as const

const laneA = LANES.a
const laneB = LANES.b

/* Projection -------------------------------------------------------------- */
interface BarGeom {
  key: string
  lane: 'a' | 'b'
  kind: LeaseKind
  state: Lease['state']
  x: number
  w: number
  y: number
  h: number
  runwayW: number
  ticks: number[]
  endX: number | null
}

const bars = computed<BarGeom[]>(() => world.leases.map((lease, i) => {
  const lane = lease.operationId === OP_A ? 'a' : 'b'
  const top = LANES[lane].top + ROW[lease.kind]
  const end = lease.endedAt ?? clock.value
  const solidEnd = Math.min(end, clock.value)
  const active = lease.state === 'active'
  return {
    key: `${lease.operationId}-${lease.kind}-${i}`,
    lane,
    kind: lease.kind,
    state: lease.state,
    x: x(lease.grantedAt),
    w: Math.max(0, x(solidEnd) - x(lease.grantedAt)),
    y: top,
    h: ROW_H[lease.kind],
    runwayW: active ? Math.max(0, x(lease.expiresAt) - x(clock.value)) : 0,
    ticks: lease.renewals.filter(r => r.at <= clock.value).map(r => x(r.at)),
    endX: lease.endedAt != null ? x(lease.endedAt) : null,
  }
}))

interface Chip {
  key: string
  lane: 'a' | 'b'
  kind: LeaseKind
  x: number
  code: string
  word: string
  tone: WireLine['tone']
}

/**
 * Status chips sit on the row of the lease they answer for. Renewal 200s are
 * already visible as heartbeat ticks, so they are not repeated as chips.
 */
const chips = computed<Chip[]>(() => {
  const out: Chip[] = []
  const lastKind: Record<string, LeaseKind> = {}
  for (const [i, line] of wire.value.entries()) {
    if (line.actor !== 'a' && line.actor !== 'b') continue
    if (line.tone === 'req') {
      lastKind[line.actor] = line.text.includes('failover-hold') ? 'failover-hold' : 'migration'
      continue
    }
    const m = line.text.match(/^(\d{3})\s+(\S+)/)
    if (!m) continue
    const [, code, word] = m
    if (code === '200') continue
    const label = code === '201' ? 'granted' : code === '204' ? 'released' : code === '423' ? 'unstable' : word!
    out.push({ key: `${i}`, lane: line.actor, kind: lastKind[line.actor] || 'migration', x: x(line.at), code: code!, word: label, tone: line.tone })
  }
  return out
})

const generationSegments = computed(() => world.generations.map((g, i) => {
  const next = world.generations[i + 1]
  const from = x(g.at)
  const to = x(next ? next.at : clock.value)
  return { key: `${g.gen}`, gen: g.gen, site: g.site, x: from, w: Math.max(0, to - from), fenceX: i > 0 ? from : null }
}))

const outage = computed(() => {
  if (world.siteLostAt == null) return null
  const bump = world.generations[1]
  const from = x(world.siteLostAt)
  const to = x(bump ? bump.at : clock.value)
  return { x: from, w: Math.max(0, to - from) }
})

const planned = computed(() => {
  const p = world.planned
  if (!p.requestedAt) return null
  const deferredEnd = p.promotingAt ?? clock.value
  return {
    requestX: x(p.requestedAt),
    deferred: { x: x(p.requestedAt + 0.5), w: Math.max(0, x(Math.min(deferredEnd, clock.value)) - x(p.requestedAt + 0.5)) },
    promoting: p.promotingAt != null ? { x: x(p.promotingAt), w: Math.max(0, x(Math.min(p.doneAt ?? clock.value, clock.value)) - x(p.promotingAt)) } : null,
    retryX: p.phase === 'Deferred' && p.retryAfter != null ? x(p.retryAfter) : null,
    phase: p.phase,
    reason: p.reason,
  }
})

const axisTicks = computed(() => {
  const ticks: number[] = []
  for (let t = 0; t <= duration.value; t += 5) ticks.push(t)
  return ticks
})

const playheadX = computed(() => x(clock.value))
const playheadLabelLeft = computed(() => playheadX.value > x1.value - 60)

const laneLabel = {
  a: { name: OP_A.slice(0, 4) + '…', sub: 'deploy · orders-app' },
  b: { name: OP_B.slice(0, 4) + '…', sub: 'deploy · orders-app' },
}

const phaseBadge: Record<string, { label: string, tone: string }> = {
  idle: { label: 'IDLE', tone: 'idle' },
  observing: { label: 'OBSERVING', tone: 'idle' },
  holding: { label: 'HOLDING', tone: 'ok' },
  migrating: { label: 'MIGRATING', tone: 'ok' },
  done: { label: 'DONE', tone: 'ok' },
  blocked: { label: 'WAITING', tone: 'warn' },
  fenced: { label: 'FENCED', tone: 'danger' },
}

const topologyBadge = computed(() => {
  if (world.sites.iad === 'unreachable') return { label: 'iad unreachable', tone: 'danger' }
  if (!world.stable) return { label: `stable=false · ${world.unstableReason}`, tone: 'warn' }
  return { label: 'stable=true', tone: 'ok' }
})

/* Lifecycle --------------------------------------------------------------- */
const root = ref<HTMLElement | null>(null)
const reduced = ref(true)
let io: IntersectionObserver | null = null
let ro: ResizeObserver | null = null
let hasAutoPlayed = false
let pausedOffscreen = false

function select(id: ScenarioId) {
  replay.select(id, false)
  if (reduced.value) replay.seek(duration.value)
  else replay.run()
}

function onReplay() {
  if (reduced.value) replay.seek(duration.value)
  else replay.run()
}

function onSeek(t: number) {
  replay.seek(t)
}

onMounted(() => {
  reduced.value = window.matchMedia('(prefers-reduced-motion: reduce)').matches

  ro = new ResizeObserver(([entry]) => {
    if (entry) width.value = Math.max(320, Math.round(entry.contentRect.width))
  })
  if (root.value) ro.observe(root.value)

  if (props.initialT != null) {
    replay.seek(props.initialT)
    hasAutoPlayed = true
    return
  }
  if (reduced.value) {
    replay.seek(duration.value)
    return
  }

  io = new IntersectionObserver(([entry]) => {
    if (!entry) return
    if (entry.isIntersecting) {
      if (!hasAutoPlayed) {
        hasAutoPlayed = true
        replay.run()
      } else if (pausedOffscreen) {
        pausedOffscreen = false
        replay.start()
      }
    } else if (running.value) {
      replay.stop()
      pausedOffscreen = true
    }
  }, { threshold: 0.35, rootMargin: '80px 0px' })
  if (root.value) io.observe(root.value)
})

onBeforeUnmount(() => {
  io?.disconnect()
  ro?.disconnect()
})
</script>

<template>
  <div ref="root" class="lanes">
    <div class="bar">
      <span class="live">
        <i data-br-motion />
        lease lanes
      </span>
      <code class="bar-res">https://bloodraven:8443/deploy/v1/groups/orders</code>
      <span class="bar-ttl" title="TTL is clamped to [5, 120] seconds; clients renew at ttl/3">ttl {{ DEPLOY_TTL }}s · renew every {{ DEPLOY_TTL / 3 }}s</span>
    </div>

    <p class="claim">
      <b>{{ scenario.label }}.</b> {{ scenario.claim }}
    </p>

    <svg
      class="canvas"
      :viewBox="`0 0 ${width} ${HEIGHT}`"
      :width="width"
      :height="HEIGHT"
      role="img"
      :aria-label="`Timeline of scenario '${scenario.label}' at ${fmtT(clock)}: deploy ${OP_A} is ${clients.a.detail}; deploy ${OP_B} is ${clients.b.detail}; active site ${world.activeSite}, topology generation ${world.topologyGeneration}.`"
    >
      <defs>
        <pattern id="br-deferred" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <line x1="0" y1="0" x2="0" y2="6" stroke="var(--br-amber)" stroke-width="1.4" stroke-opacity="0.55" />
        </pattern>
        <pattern id="br-outage" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(-45)">
          <line x1="0" y1="0" x2="0" y2="6" stroke="var(--br-red)" stroke-width="1.4" stroke-opacity="0.6" />
        </pattern>
      </defs>

      <!-- Lane backgrounds and labels -->
      <g v-for="(lane, key) in LANES" :key="key" class="lane" :data-lane="key">
        <rect :x="0" :y="lane.top" :width="width" :height="lane.height" class="lane-bg" />
        <line :x1="x0" :x2="x1" :y1="lane.top + lane.height" :y2="lane.top + lane.height" class="lane-rule" />
      </g>

      <!-- Deploy lane labels -->
      <g v-for="id in (['a', 'b'] as const)" :key="`label-${id}`" class="lane-label" :data-tone="phaseBadge[clients[id].phase]!.tone">
        <text :x="10" :y="LANES[id].top + 22" class="label-name">{{ laneLabel[id].name }}</text>
        <text v-if="!compact" :x="10" :y="LANES[id].top + 36" class="label-sub">{{ laneLabel[id].sub }}</text>
        <text :x="10" :y="LANES[id].top + (compact ? 38 : 50)" class="label-phase">{{ phaseBadge[clients[id].phase]!.label }}</text>
      </g>
      <g class="lane-label" :data-tone="planned?.phase === 'Deferred' ? 'warn' : planned?.phase === 'Complete' ? 'ok' : 'idle'">
        <text :x="10" :y="LANES.planned.top + 20" class="label-name">{{ compact ? 'planned' : 'planned ops' }}</text>
        <text :x="10" :y="LANES.planned.top + 34" class="label-phase">{{ planned?.phase ? planned.phase.toUpperCase() : 'NONE' }}</text>
      </g>
      <g class="lane-label" :data-tone="topologyBadge.tone">
        <text :x="10" :y="LANES.topology.top + 22" class="label-name">topology</text>
        <text :x="10" :y="LANES.topology.top + 36" class="label-phase">gen {{ world.topologyGeneration }}</text>
      </g>

      <!-- Row captions inside the deploy lanes -->
      <g v-for="id in (['a', 'b'] as const)" v-show="!compact" :key="`rows-${id}`" class="row-caption">
        <text :x="x0 + 2" :y="LANES[id].top + ROW.migration - 3">migration</text>
        <text :x="x0 + 2" :y="LANES[id].top + ROW['failover-hold'] - 3">failover-hold</text>
      </g>

      <!-- Axis -->
      <g class="axis">
        <line :x1="x0" :x2="x1" :y1="AXIS_Y - 8" :y2="AXIS_Y - 8" />
        <g v-for="t in axisTicks" :key="t">
          <line :x1="x(t)" :x2="x(t)" :y1="AXIS_Y - 11" :y2="AXIS_Y - 5" />
          <text :x="x(t)" :y="AXIS_Y + 4" text-anchor="middle">{{ t === 0 ? 'T+0' : `${t}s` }}</text>
        </g>
      </g>

      <!-- Topology lane: active site by generation, outage hatch, fence lines -->
      <g class="topology">
        <rect
          v-for="seg in generationSegments"
          :key="seg.key"
          :x="seg.x"
          :y="GEN_Y"
          :width="seg.w"
          height="20"
          rx="3"
          class="gen-seg"
          :data-site="seg.site"
        />
        <rect
          v-if="outage"
          :x="outage.x"
          :y="GEN_Y"
          :width="outage.w"
          height="20"
          rx="3"
          fill="url(#br-outage)"
        />
        <text
          v-for="seg in generationSegments"
          :key="`t-${seg.key}`"
          :x="seg.x + 6"
          :y="GEN_Y + 14"
          class="gen-text"
        >{{ seg.w > 150 ? `activeSite ${seg.site} · gen ${seg.gen}` : seg.w > 62 ? `${seg.site} · gen ${seg.gen}` : '' }}</text>
        <text
          v-if="outage && outage.w > 70"
          :x="outage.x + outage.w / 2"
          :y="GEN_Y + 14"
          text-anchor="middle"
          class="outage-text"
        >iad unreachable</text>
      </g>

      <!-- Fence: a generation change severs every lane -->
      <g v-for="seg in generationSegments" :key="`fence-${seg.key}`">
        <template v-if="seg.fenceX != null">
          <line :x1="seg.fenceX" :x2="seg.fenceX" :y1="LANES.a.top - 4" :y2="AXIS_Y - 8" class="fence" />
          <g :transform="`translate(${seg.fenceX}, ${LANES.topology.top + 3})`" class="fence-tag">
            <rect :x="seg.fenceX > x1 - 120 ? -118 : 4" y="0" width="114" height="13" rx="3" />
            <text :x="seg.fenceX > x1 - 120 ? -61 : 61" y="9.5" text-anchor="middle">topologyGeneration → {{ seg.gen }}</text>
          </g>
        </template>
      </g>

      <!-- Planned ops lane -->
      <g v-if="planned" class="planned">
        <rect
          v-if="planned.deferred.w > 0"
          :x="planned.deferred.x"
          :y="LANES.planned.top + 14"
          :width="planned.deferred.w"
          height="16"
          rx="3"
          fill="url(#br-deferred)"
          class="deferred"
        />
        <text
          v-if="planned.deferred.w > 130"
          :x="planned.deferred.x + planned.deferred.w / 2"
          :y="LANES.planned.top + 25"
          text-anchor="middle"
          class="deferred-text"
        >Deferred · DeploymentHold</text>
        <rect
          v-if="planned.promoting && planned.promoting.w > 0"
          :x="planned.promoting.x"
          :y="LANES.planned.top + 14"
          :width="planned.promoting.w"
          height="16"
          rx="3"
          class="promoting"
        />
        <circle :cx="planned.requestX" :cy="LANES.planned.top + 22" r="4" class="request-dot" />
        <text :x="planned.requestX" :y="LANES.planned.top + 10" text-anchor="middle" class="request-text">promote → pdx</text>
        <g v-if="planned.retryX != null" :transform="`translate(${planned.retryX}, ${LANES.planned.top + 22})`" class="retry">
          <path d="M0,-6 L6,0 L0,6 L-6,0 Z" />
          <text y="-9" :text-anchor="planned.retryX > x1 - 90 ? 'end' : 'middle'" :x="planned.retryX > x1 - 90 ? 8 : 0">retryAfter {{ world.planned.retryAfter != null ? fmtT(world.planned.retryAfter) : '' }}{{ planned.retryX >= x1 ? ' ›' : '' }}</text>
        </g>
      </g>

      <!-- Lease bars -->
      <g v-for="bar in bars" :key="bar.key" class="lease" :data-kind="bar.kind" :data-state="bar.state">
        <rect
          v-if="bar.runwayW > 0"
          :x="playheadX"
          :y="bar.y"
          :width="bar.runwayW"
          :height="bar.h"
          rx="2"
          class="runway"
        />
        <rect :x="bar.x" :y="bar.y" :width="bar.w" :height="bar.h" rx="2" class="solid" />
        <line
          v-for="(tick, i) in bar.ticks"
          :key="i"
          :x1="tick"
          :x2="tick"
          :y1="bar.y - 2"
          :y2="bar.y + bar.h + 2"
          class="tick"
        />
        <g v-if="bar.endX != null" :transform="`translate(${bar.endX}, ${bar.y + bar.h / 2})`" class="end">
          <template v-if="bar.state === 'revoked'">
            <line x1="-4" y1="-4" x2="4" y2="4" />
            <line x1="-4" y1="4" x2="4" y2="-4" />
          </template>
          <line v-else x1="0" :y1="-bar.h / 2 - 2" x2="0" :y2="bar.h / 2 + 2" />
        </g>
      </g>

      <!-- Status chips -->
      <g
        v-for="chip in chips"
        :key="chip.key"
        class="chip"
        :data-tone="chip.tone"
        :transform="`translate(${chip.x}, ${LANES[chip.lane].top + (chip.kind === 'migration' ? 1 : 47)})`"
      >
        <rect x="0" y="0" :width="compact ? 26 : 11 + (chip.code.length + 1 + chip.word.length) * 4.9" height="12" rx="6" />
        <text x="6" y="8.5"><tspan class="chip-code">{{ chip.code }}</tspan><tspan v-if="!compact" class="chip-word">{{ '\u00A0' + chip.word }}</tspan></text>
      </g>

      <!-- Playhead -->
      <g class="playhead" :transform="`translate(${playheadX}, 0)`">
        <line x1="0" x2="0" :y1="LANES.a.top - 4" :y2="AXIS_Y - 8" />
        <rect :x="playheadLabelLeft ? -54 : 4" :y="AXIS_Y - 22" width="50" height="13" rx="3" />
        <text :x="playheadLabelLeft ? -29 : 29" :y="AXIS_Y - 12" text-anchor="middle">{{ fmtT(clock) }}</text>
      </g>
    </svg>

    <HomeDeployWire
      :lines="wire"
      :rows="4"
      :idle="`deploy client ready · ${scenario.label.toLowerCase()} · press Play to watch the API arbitrate.`"
    />

    <HomeDeployControls
      :scenario-id="replay.scenarioId.value"
      :clock="clock"
      :duration="duration"
      :running="running"
      :finished="finished"
      :reduced="reduced"
      @select="select"
      @replay="onReplay"
      @seek="onSeek"
    />
  </div>
</template>

<style scoped>
.lanes {
  overflow: hidden;
  border: 1px solid rgba(255, 255, 255, 0.14);
  border-radius: 12px;
  color: #dce4ef;
  background: linear-gradient(145deg, rgba(18, 24, 35, 0.98), rgba(9, 13, 20, 0.98));
  box-shadow: 0 36px 80px rgba(0, 0, 0, 0.4), inset 0 1px rgba(255, 255, 255, 0.05);
}

/* Bar ------------------------------------------------------------------- */
.bar {
  display: flex;
  align-items: center;
  gap: 12px;
  height: 46px;
  padding: 0 16px;
  border-bottom: 1px solid var(--br-line-dark);
  font-family: var(--br-mono);
  font-size: 10px;
  letter-spacing: 0.04em;
}

.live {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #d9e1eb;
  font-weight: 700;
  white-space: nowrap;
}

.live i {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--br-green);
  box-shadow: 0 0 10px rgba(52, 211, 153, 0.8);
}

.bar-res {
  min-width: 0;
  color: var(--br-on-ink-faint);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bar-ttl {
  margin-left: auto;
  color: var(--br-on-ink-faint);
  white-space: nowrap;
}

.claim {
  margin: 0;
  padding: 12px 16px 6px;
  color: var(--br-on-ink-dim);
  font-size: 12.5px;
  line-height: 1.55;
  text-wrap: pretty;
}

.claim b {
  color: #f2f5f8;
}

/* Canvas ---------------------------------------------------------------- */
.canvas {
  display: block;
  width: 100%;
  height: auto;
  font-family: var(--br-mono);
}

.lane-bg { fill: rgba(255, 255, 255, 0.018); }
.lane[data-lane='b'] .lane-bg { fill: rgba(255, 255, 255, 0.005); }
.lane-rule { stroke: var(--br-line-dark-soft); stroke-width: 1; }

.label-name {
  fill: #f2f5f8;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.label-sub {
  fill: var(--br-on-ink-faint);
  font-size: 8.5px;
}

.label-phase {
  fill: var(--br-on-ink-faint);
  font-size: 8px;
  font-weight: 700;
  letter-spacing: 0.1em;
}

.lane-label[data-tone='ok'] .label-phase { fill: var(--br-green); }
.lane-label[data-tone='warn'] .label-phase { fill: var(--br-amber); }
.lane-label[data-tone='danger'] .label-phase { fill: var(--br-red-bright); }

.row-caption text {
  fill: #4d5769;
  font-size: 7.5px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.axis line { stroke: rgba(255, 255, 255, 0.18); stroke-width: 1; }
.axis text { fill: #4d5769; font-size: 8px; }

/* Topology ---------------------------------------------------------------- */
.gen-seg { fill: rgba(255, 255, 255, 0.06); stroke: rgba(255, 255, 255, 0.14); stroke-width: 1; }
.gen-seg[data-site='iad'] { fill: rgba(251, 113, 133, 0.12); stroke: rgba(251, 113, 133, 0.4); }
.gen-seg[data-site='pdx'] { fill: rgba(34, 211, 238, 0.12); stroke: rgba(34, 211, 238, 0.4); }
.gen-text { fill: #d7dee8; font-size: 9px; font-weight: 700; }
.outage-text { fill: var(--br-red-bright); font-size: 8px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; }

.fence { stroke: var(--br-red); stroke-width: 1.5; stroke-dasharray: 3 3; }
.fence-tag rect { fill: rgba(76, 5, 25, 0.92); stroke: rgba(225, 29, 72, 0.6); stroke-width: 1; }
.fence-tag text { fill: #fecdd6; font-size: 8px; font-weight: 700; }

/* Planned ops ------------------------------------------------------------- */
.deferred { stroke: rgba(251, 191, 36, 0.5); stroke-width: 1; }
.deferred-text { fill: var(--br-amber); font-size: 8px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; paint-order: stroke; stroke: #0b0f16; stroke-width: 3px; }
.promoting { fill: rgba(34, 211, 238, 0.28); stroke: rgba(34, 211, 238, 0.7); stroke-width: 1; }
.request-dot { fill: #0b0f16; stroke: var(--br-cyan); stroke-width: 1.5; }
.request-text { fill: var(--br-cyan-soft); font-size: 8px; font-weight: 700; }
.retry path { fill: #0b0f16; stroke: var(--br-amber); stroke-width: 1.4; }
.retry text { fill: var(--br-amber); font-size: 7.5px; letter-spacing: 0.06em; }

/* Leases -------------------------------------------------------------------- */
.lease .solid { fill: rgba(251, 113, 133, 0.85); }
.lease[data-kind='failover-hold'] .solid { fill: rgba(34, 211, 238, 0.8); }
.lease[data-state='released'] .solid { opacity: 0.55; }
.lease[data-state='revoked'] .solid { fill: rgba(225, 29, 72, 0.55); }

.runway {
  fill: none;
  stroke: rgba(251, 113, 133, 0.55);
  stroke-width: 1;
  stroke-dasharray: 3 3;
}

.lease[data-kind='failover-hold'] .runway { stroke: rgba(34, 211, 238, 0.55); }

.tick { stroke: #ffffff; stroke-width: 1.5; }
.end line { stroke: #ffffff; stroke-width: 1.5; }
.lease[data-state='revoked'] .end line { stroke: var(--br-red-bright); stroke-width: 2; }

/* Chips --------------------------------------------------------------------- */
.chip rect { fill: rgba(7, 10, 15, 0.95); stroke-width: 1; }
.chip text { font-size: 8px; font-weight: 700; }
.chip-word { font-weight: 400; }
.chip[data-tone='ok'] rect { stroke: rgba(52, 211, 153, 0.6); }
.chip[data-tone='ok'] text { fill: var(--br-green); }
.chip[data-tone='warn'] rect { stroke: rgba(251, 191, 36, 0.6); }
.chip[data-tone='warn'] text { fill: var(--br-amber); }
.chip[data-tone='danger'] rect { stroke: rgba(225, 29, 72, 0.75); fill: rgba(76, 5, 25, 0.95); }
.chip[data-tone='danger'] text { fill: var(--br-red-bright); }

/* Playhead --------------------------------------------------------------- */
.playhead line { stroke: rgba(255, 255, 255, 0.6); stroke-width: 1; }
.playhead rect { fill: rgba(7, 10, 15, 0.95); stroke: rgba(255, 255, 255, 0.3); stroke-width: 1; }
.playhead text { fill: #f2f5f8; font-size: 8.5px; font-variant-numeric: tabular-nums; }

@media (max-width: 620px) {
  .bar-res { display: none; }
  .bar-ttl { margin-left: auto; }
  .claim { font-size: 12px; }
}
</style>
