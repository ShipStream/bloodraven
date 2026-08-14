<script setup lang="ts">
/**
 * Live failover simulation for the landing hero.
 *
 * Nothing here talks to a cluster -- it replays the documented promotion
 * sequence (docs/operations/failover) on a virtual cluster clock so a visitor
 * can watch detection, fencing, promotion and DNS steering in order.
 *
 * The clock runs at CLOCK_RATE x wall time; step timestamps are the real
 * documented values (3 failed polls at the 2s base interval = ~6s to declare a
 * site unreachable). Every label on the instrument is a real Bloodraven fact --
 * site names, zones, the DNS hostname and TTL, the poll interval, the GTID set.
 */
const CLOCK_RATE = 2.5
const TICK_MS = 50

type SiteState = 'writable' | 'replica' | 'degraded' | 'unreachable' | 'fenced'

type LogTone = 'muted' | 'warn' | 'danger' | 'ok' | 'primary'

interface LogLine {
  t: string
  msg: string
  tone: LogTone
}

interface Site {
  name: string
  zone: string
  ip: string
  state: SiteState
  detail: string
}

const iad = reactive<Site>({ name: 'iad', zone: 'us-east-1a', ip: '10.0.1.1', state: 'writable', detail: 'read_only=OFF' })
const pdx = reactive<Site>({ name: 'pdx', zone: 'us-west-2a', ip: '10.0.2.1', state: 'replica', detail: 'lag 0s · source iad' })

/**
 * The read-only site. It is never promoted and never a DNS target, so it has no
 * SiteState of its own -- it either serves, serves stale, or is repointing at
 * the site that just won the election.
 */
const reader = reactive({
  state: 'serving' as 'serving' | 'stale' | 'repointing',
  source: 'iad',
  detail: 'lag 0s · serving reads',
})

const dns = reactive({ target: '10.0.1.1', site: 'iad', flashing: false })
const log = ref<LogLine[]>([])
const clock = ref(0)
const running = ref(false)
const finished = ref(false)
const logBox = ref<HTMLElement | null>(null)

let timer: ReturnType<typeof setInterval> | null = null
let cursor = 0
let hasAutoPlayed = false

interface Step {
  at: number
  msg: string
  tone: LogTone
  apply?: () => void
}

const steps: Step[] = [
  { at: 0.0, msg: 'chaos: site iad lost (node gone)', tone: 'danger', apply: () => { iad.state = 'degraded'; iad.detail = 'probe timeout' } },
  { at: 0.1, msg: 'probe iad: dial tcp 10.0.1.1:3306 i/o timeout (1/3)', tone: 'warn' },
  { at: 2.0, msg: 'probe iad: dial tcp 10.0.1.1:3306 i/o timeout (2/3)', tone: 'warn', apply: () => { iad.detail = 'failed polls 2/3' } },
  { at: 4.0, msg: 'probe iad: dial tcp 10.0.1.1:3306 i/o timeout (3/3)', tone: 'warn', apply: () => { iad.detail = 'failed polls 3/3' } },
  { at: 6.0, msg: 'site iad unreachable · failureThreshold reached · taint applied', tone: 'danger', apply: () => { iad.state = 'unreachable'; iad.detail = 'unreachable'; reader.state = 'stale'; reader.detail = 'source gone · still serving' } },
  { at: 6.4, msg: 'candidate pdx selected: freshest GTID set, primary-candidate', tone: 'primary', apply: () => { pdx.detail = 'candidate · GTID freshest' } },
  { at: 6.8, msg: 'fence old primary: SET GLOBAL super_read_only=ON (best-effort)', tone: 'muted' },
  { at: 7.2, msg: 'evict application connections from iad', tone: 'muted' },
  { at: 7.6, msg: 'drain relay logs on pdx (bounded 30s)', tone: 'muted', apply: () => { pdx.detail = 'draining relay log' } },
  { at: 8.6, msg: 'STOP REPLICA; RESET REPLICA ALL on pdx', tone: 'muted', apply: () => { pdx.detail = 'replication stopped' } },
  { at: 9.0, msg: 'promotionGtidExecuted = 3e11fa…:1-48210', tone: 'muted' },
  { at: 9.4, msg: 'SET super_read_only=OFF; SET read_only=OFF on pdx', tone: 'ok' },
  { at: 9.8, msg: 'writability confirmed · pdx is primary', tone: 'ok', apply: () => { pdx.state = 'writable'; pdx.detail = 'read_only=OFF' } },
  { at: 10.3, msg: 'DNSEndpoint updated: orders.az.example.com A 10.0.2.1 ttl 60', tone: 'primary', apply: () => { dns.target = '10.0.2.1'; dns.site = 'pdx'; dns.flashing = true } },
  { at: 11.0, msg: 'orders-primary Service selector moved · taints reconciled', tone: 'muted', apply: () => { dns.flashing = false } },
  { at: 11.4, msg: 'reader: CHANGE REPLICATION SOURCE TO pdx · client endpoint retained', tone: 'muted', apply: () => { reader.state = 'repointing'; reader.source = 'pdx'; reader.detail = 'repointing at pdx' } },
  { at: 12.2, msg: 'reader replicating from pdx · never a promotion candidate', tone: 'ok', apply: () => { reader.state = 'serving'; reader.source = 'pdx'; reader.detail = 'lag 0s · serving reads' } },
  { at: 13.0, msg: 'iad returns · super_read_only=ON · rejoining as replica', tone: 'muted', apply: () => { iad.state = 'fenced'; iad.detail = 'read_only=ON' } },
  { at: 14.2, msg: 'iad replicating from pdx · group healthy · 0 humans paged', tone: 'ok', apply: () => { iad.state = 'replica'; iad.detail = 'lag 0s · source pdx' } },
]

const totalDuration = steps[steps.length - 1]!.at + 0.8

function fmt(seconds: number) {
  return `T+${seconds.toFixed(1).padStart(4, '0')}s`
}

function reset() {
  stop()
  cursor = 0
  clock.value = 0
  finished.value = false
  log.value = []
  iad.state = 'writable'
  iad.detail = 'read_only=OFF'
  pdx.state = 'replica'
  pdx.detail = 'lag 0s · source iad'
  dns.target = '10.0.1.1'
  dns.site = 'iad'
  dns.flashing = false
  reader.state = 'serving'
  reader.source = 'iad'
  reader.detail = 'lag 0s · serving reads'
}

function stop() {
  if (timer) clearInterval(timer)
  timer = null
  running.value = false
}

function run() {
  reset()
  running.value = true
  timer = setInterval(() => {
    clock.value += (TICK_MS / 1000) * CLOCK_RATE

    while (cursor < steps.length && steps[cursor]!.at <= clock.value) {
      const step = steps[cursor]!
      step.apply?.()
      log.value.push({ t: fmt(step.at), msg: step.msg, tone: step.tone })
      cursor++
      nextTick(() => {
        if (logBox.value) logBox.value.scrollTop = logBox.value.scrollHeight
      })
    }

    if (clock.value >= totalDuration) {
      stop()
      finished.value = true
    }
  }, TICK_MS)
}

/**
 * Replication direction is derived from the two site states rather than from
 * the step index, so the drawn link can never disagree with the badges.
 */
const link = computed<'ltr' | 'rtl' | 'broken'>(() => {
  if (iad.state === 'writable' && pdx.state === 'replica') return 'ltr'
  if (pdx.state === 'writable' && iad.state === 'replica') return 'rtl'
  return 'broken'
})

const PATH_LTR = 'M156,64 C198,28 262,100 304,64'
const PATH_RTL = 'M304,64 C262,100 198,28 156,64'
const linkPath = computed(() => (link.value === 'rtl' ? PATH_RTL : PATH_LTR))

const linkLabel = computed(() => {
  if (link.value === 'broken') return 'replication down'
  return link.value === 'ltr' ? 'iad → pdx · async' : 'pdx → iad · async'
})

/**
 * The reader's feed, drawn from whichever node is currently its source. The
 * curve swings across to the other node on repoint, so the flip is visible
 * rather than just a changed label.
 */
const READER_PATH_FROM_A = 'M87,0 C87,25 230,19 230,44'
const READER_PATH_FROM_B = 'M373,0 C373,25 230,19 230,44'
const readerPath = computed(() => (reader.source === 'pdx' ? READER_PATH_FROM_B : READER_PATH_FROM_A))

const readerLink = computed(() => {
  if (reader.state === 'stale') return 'broken'
  if (reader.state === 'repointing') return 'moving'
  return 'live'
})

const fence = computed(() => {
  if (iad.state === 'unreachable') return { text: 'old primary fenced · super_read_only=ON', hot: true }
  if (iad.state === 'fenced') return { text: 'iad read_only=ON · rejoining', hot: true }
  return { text: 'fencing armed · exactly one writer', hot: false }
})

const badge: Record<SiteState, { label: string, tone: string }> = {
  writable: { label: 'PRIMARY', tone: 'primary' },
  replica: { label: 'REPLICA', tone: 'replica' },
  degraded: { label: 'DEGRADED', tone: 'degraded' },
  unreachable: { label: 'UNREACHABLE', tone: 'down' },
  fenced: { label: 'FENCED', tone: 'fenced' },
}

const root = ref<HTMLElement | null>(null)
const card = ref<HTMLElement | null>(null)
const reduced = ref(true)
let io: IntersectionObserver | null = null

function tilt(event: PointerEvent) {
  if (!card.value || reduced.value || event.pointerType === 'touch') return
  const bounds = card.value.getBoundingClientRect()
  const x = (event.clientX - bounds.left) / bounds.width
  const y = (event.clientY - bounds.top) / bounds.height
  card.value.style.setProperty('--pointer-x', `${x * 100}%`)
  card.value.style.setProperty('--pointer-y', `${y * 100}%`)
  card.value.style.setProperty('--tilt-x', `${(0.5 - y) * 3}deg`)
  card.value.style.setProperty('--tilt-y', `${(x - 0.5) * 3.5}deg`)
}

function untilt() {
  card.value?.style.setProperty('--tilt-x', '0deg')
  card.value?.style.setProperty('--tilt-y', '0deg')
}

onMounted(() => {
  reduced.value = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  if (reduced.value) return

  io = new IntersectionObserver(([entry]) => {
    if (entry?.isIntersecting && !hasAutoPlayed) {
      hasAutoPlayed = true
      run()
    }
  }, { threshold: 0.4 })
  if (root.value) io.observe(root.value)
})

onBeforeUnmount(() => {
  stop()
  io?.disconnect()
})
</script>

<template>
  <div ref="root" class="stage-wrap">
    <div
      ref="card"
      class="stage"
      @pointermove="tilt"
      @pointerleave="untilt"
    >
      <!-- Instrument bar -->
      <div class="bar">
        <span class="live">
          <i data-br-motion />
          live topology
        </span>
        <code class="bar-res">mysqlfailovergroup/orders</code>
        <span class="poll" title="The operator polls every site every 2 seconds">
          <i data-br-motion />
          poll 2s
        </span>
      </div>

      <!-- Topology -->
      <div class="body" :data-link="link">
        <svg
          class="link"
          viewBox="0 0 460 130"
          preserveAspectRatio="none"
          aria-hidden="true"
        >
          <defs>
            <linearGradient id="br-repl" x1="0" x2="1">
              <stop offset="0" stop-color="var(--br-red-bright)" />
              <stop offset="1" stop-color="var(--br-cyan-soft)" />
            </linearGradient>
          </defs>
          <path class="link-halo" :d="linkPath" vector-effect="non-scaling-stroke" />
          <path class="link-line" :d="linkPath" vector-effect="non-scaling-stroke" />
          <template v-if="link !== 'broken' && !reduced">
            <circle :key="`a-${link}`" class="packet" r="3.6" fill="#ffffff">
              <animateMotion dur="2.6s" repeatCount="indefinite" :path="linkPath" />
            </circle>
            <circle :key="`b-${link}`" class="packet" r="2.8" fill="var(--br-cyan-soft)">
              <animateMotion begin="-1.3s" dur="2.6s" repeatCount="indefinite" :path="linkPath" />
            </circle>
          </template>
        </svg>

        <span class="link-label">{{ linkLabel }}</span>

        <div
          v-for="site in [iad, pdx]"
          :key="site.name"
          class="node"
          :class="site.name === 'iad' ? 'node-a' : 'node-b'"
          :data-tone="badge[site.state].tone"
        >
          <span class="node-badge">
            <i data-br-motion />
            {{ badge[site.state].label }}
          </span>
          <strong>{{ site.name.toUpperCase() }}</strong>
          <small>{{ site.zone }}<span class="node-ip"> · {{ site.ip }}</span></small>
          <small class="node-detail">{{ site.detail }}</small>
        </div>

        <!-- Read-only site: follows whichever site is active, never promoted. -->
        <svg
          class="reader-link"
          :data-state="readerLink"
          viewBox="0 0 460 44"
          preserveAspectRatio="none"
          aria-hidden="true"
        >
          <path class="reader-link-line" :d="readerPath" vector-effect="non-scaling-stroke" />
          <circle
            v-if="readerLink === 'live' && !reduced"
            :key="`reader-${reader.source}`"
            class="packet"
            r="3"
            fill="var(--br-cyan-soft)"
          >
            <animateMotion dur="2.2s" repeatCount="indefinite" :path="readerPath" />
          </circle>
        </svg>

        <div class="reader" :data-tone="reader.state">
          <span class="reader-badge">
            <i data-br-motion />
            <span class="reader-badge-label">READ-ONLY</span>
          </span>
          <strong>reader</strong>
          <small>{{ reader.detail }}</small>
          <span class="reader-source">source {{ reader.source }}</span>
        </div>

        <span class="fence" :data-hot="fence.hot">
          <i aria-hidden="true">◇</i>
          {{ fence.text }}
        </span>
      </div>

      <!-- DNS -->
      <div class="dns" :data-flash="dns.flashing">
        <span class="dns-icon" aria-hidden="true">↳</span>
        <span class="dns-meta">
          <small>ACTIVE DNS · TTL 60</small>
          <code>orders.az.example.com</code>
        </span>
        <span class="dns-target">
          <b>{{ dns.target }}</b>
          <small>{{ dns.site }}</small>
        </span>
      </div>

      <!-- Event log -->
      <div ref="logBox" class="log" role="log" aria-live="polite" aria-label="Operator event log">
        <p v-if="!log.length" class="log-idle">
          operator ready · watching 3 sites · press
          <b>Kill the active site</b>
          to watch a failover.
        </p>
        <p v-for="(line, index) in log" :key="index" class="log-line" :data-tone="line.tone">
          <span class="log-t">{{ line.t }}</span>
          <span class="log-msg">{{ line.msg }}</span>
        </p>
      </div>

      <!-- Controls -->
      <div class="controls">
        <button type="button" class="kill br-focus" :disabled="running" @click="run">
          <span class="kill-dot" :data-running="running" />
          {{ finished ? 'Run it again' : 'Kill the active site' }}
        </button>
        <!-- Deliberately not disabled while running: a visitor who wants out of
             the sequence should not have to wait ~6s for the control to return. -->
        <button type="button" class="soft br-focus" @click="reset">
          Reset
        </button>
        <span class="clock">{{ fmt(Math.min(clock, totalDuration)) }}</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.stage-wrap {
  perspective: 1200px;
}

.stage {
  --pointer-x: 60%;
  --pointer-y: 30%;
  --tilt-x: 0deg;
  --tilt-y: 0deg;
  position: relative;
  overflow: hidden;
  border: 1px solid rgba(255, 255, 255, 0.16);
  border-radius: 12px;
  color: #dce4ef;
  background: linear-gradient(145deg, rgba(18, 24, 35, 0.96), rgba(9, 13, 20, 0.94));
  box-shadow: 0 42px 90px rgba(0, 0, 0, 0.45), inset 0 1px rgba(255, 255, 255, 0.05);
  transform: rotateX(var(--tilt-x)) rotateY(var(--tilt-y));
  transition: transform 140ms ease-out;
}

.stage::after {
  content: '';
  position: absolute;
  inset: 0;
  z-index: 0;
  pointer-events: none;
  background: radial-gradient(420px circle at var(--pointer-x) var(--pointer-y), rgba(34, 211, 238, 0.07), transparent 48%);
}

.stage > * {
  position: relative;
  z-index: 1;
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
}

.live i {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--br-green);
  box-shadow: 0 0 10px rgba(52, 211, 153, 0.8);
}

.bar-res {
  color: var(--br-on-ink-faint);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.poll {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  margin-left: auto;
  color: var(--br-on-ink-faint);
  white-space: nowrap;
}

.poll i {
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: var(--br-cyan);
  animation: br-poll 2s linear infinite;
}

/* Topology -------------------------------------------------------------- */
.body {
  position: relative;
  height: 324px;
  background-image: radial-gradient(rgba(255, 255, 255, 0.07) 0.8px, transparent 0.8px);
  background-size: 18px 18px;
}

.link {
  position: absolute;
  left: 0;
  right: 0;
  top: 36px;
  width: 100%;
  height: 130px;
  overflow: visible;
}

.link-halo {
  fill: none;
  stroke: rgba(34, 211, 238, 0.09);
  stroke-width: 12;
}

.link-line {
  fill: none;
  stroke: url(#br-repl);
  stroke-width: 1.4;
  stroke-dasharray: 6 6;
  transition: stroke 300ms ease;
}

.body[data-link='broken'] .link-line {
  stroke: rgba(225, 29, 72, 0.55);
  stroke-dasharray: 3 9;
}

.body[data-link='broken'] .link-halo {
  stroke: rgba(225, 29, 72, 0.07);
}

.packet {
  filter: drop-shadow(0 0 6px var(--br-cyan));
}

.link-label {
  position: absolute;
  left: 50%;
  top: 86px;
  translate: -50% 0;
  padding: 3px 9px;
  border: 1px solid var(--br-line-dark);
  border-radius: 999px;
  color: var(--br-on-ink-faint);
  background: rgba(7, 10, 15, 0.9);
  font-family: var(--br-mono);
  font-size: 8.5px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  white-space: nowrap;
}

.body[data-link='broken'] .link-label {
  color: var(--br-red-bright);
  border-color: rgba(225, 29, 72, 0.4);
}

.node {
  position: absolute;
  top: 24px;
  width: 30%;
  min-height: 136px;
  padding: 13px;
  border: 1px solid var(--br-line-dark);
  border-radius: 7px;
  background: rgba(8, 12, 18, 0.94);
  box-shadow: 0 16px 35px rgba(0, 0, 0, 0.45);
  transition: border-color 300ms ease, box-shadow 300ms ease;
}

.node-a { left: 4%; }
.node-b { right: 4%; }

.node[data-tone='primary'] {
  border-color: rgba(251, 113, 133, 0.55);
  box-shadow: 0 16px 35px rgba(0, 0, 0, 0.45), 0 0 0 1px rgba(251, 113, 133, 0.12);
}

.node[data-tone='replica'] { border-color: rgba(34, 211, 238, 0.38); }
.node[data-tone='degraded'] { border-color: rgba(251, 191, 36, 0.5); }
.node[data-tone='down'] { border-color: rgba(225, 29, 72, 0.6); }
.node[data-tone='fenced'] { border-color: rgba(255, 255, 255, 0.2); }

.node-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 8px;
  font-weight: 700;
  letter-spacing: 0.1em;
}

.node-badge i {
  width: 5px;
  height: 5px;
  flex: none;
  border-radius: 50%;
  background: currentColor;
}

.node[data-tone='primary'] .node-badge { color: var(--br-red-bright); }
.node[data-tone='replica'] .node-badge { color: var(--br-cyan); }
.node[data-tone='degraded'] .node-badge { color: var(--br-amber); }
.node[data-tone='down'] .node-badge { color: #fb7185; }
.node[data-tone='degraded'] .node-badge i { animation: br-blink 1s steps(1) infinite; }

.node strong {
  display: block;
  margin-top: 13px;
  font-size: 26px;
  line-height: 1;
  letter-spacing: -0.035em;
  color: #f2f5f8;
}

.node small {
  display: block;
  margin-top: 8px;
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 9.5px;
  line-height: 1.35;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* Two lines are reserved so the longest status string ("candidate · GTID
   freshest") wraps without resizing the node mid-run. Selector matches the
   specificity of `.node small` above, which otherwise wins on white-space. */
.node small.node-detail {
  min-height: 26px;
  margin-top: 3px;
  overflow: visible;
  white-space: normal;
  color: var(--br-on-ink-dim);
}

/* Reader ---------------------------------------------------------------- */
/* Starts below the deepest the candidate nodes reach (their `min-height` is
   136px, but the two-line status detail pushes them to ~165px at desktop
   widths), so the curve's origin never sits inside a node's bottom edge. The
   viewBox height matches the band so the packet stays round rather than being
   squashed by `preserveAspectRatio: none`. */
.reader-link {
  position: absolute;
  left: 0;
  right: 0;
  top: 170px;
  width: 100%;
  height: 44px;
  overflow: visible;
}

.reader-link-line {
  fill: none;
  stroke: rgba(34, 211, 238, 0.45);
  stroke-width: 1.2;
  stroke-dasharray: 5 5;
  /* `d` is animatable as a presentation attribute in current browsers, so the
     curve sweeps across on repoint; where it is not, it simply snaps. */
  transition: stroke 300ms ease, d 500ms ease;
}

.reader-link[data-state='broken'] .reader-link-line {
  stroke: rgba(225, 29, 72, 0.5);
  stroke-dasharray: 3 8;
}

.reader-link[data-state='moving'] .reader-link-line {
  stroke: rgba(251, 113, 133, 0.75);
}

.reader {
  position: absolute;
  left: 50%;
  top: 214px;
  translate: -50% 0;
  display: flex;
  align-items: center;
  gap: 10px;
  width: 78%;
  padding: 9px 13px;
  border: 1px solid rgba(34, 211, 238, 0.3);
  border-radius: 7px;
  background: rgba(8, 12, 18, 0.94);
  box-shadow: 0 12px 26px rgba(0, 0, 0, 0.4);
  transition: border-color 300ms ease, opacity 300ms ease;
}

.reader[data-tone='stale'] {
  border-color: rgba(251, 191, 36, 0.45);
}

.reader[data-tone='repointing'] {
  border-color: rgba(251, 113, 133, 0.5);
}

.reader-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  flex: none;
  color: var(--br-cyan);
  font-family: var(--br-mono);
  font-size: 7.5px;
  font-weight: 700;
  letter-spacing: 0.1em;
}

.reader-badge i {
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: currentColor;
}

.reader[data-tone='stale'] .reader-badge { color: var(--br-amber); }
.reader[data-tone='repointing'] .reader-badge { color: var(--br-red-bright); }
.reader[data-tone='repointing'] .reader-badge i { animation: br-blink 0.7s steps(1) infinite; }

.reader strong {
  flex: none;
  color: #e8edf5;
  font-family: var(--br-mono);
  font-size: 12px;
  font-weight: 700;
}

.reader small {
  min-width: 0;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 9.5px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.reader-source {
  margin-left: auto;
  flex: none;
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 9px;
  letter-spacing: 0.05em;
}

.fence {
  position: absolute;
  left: 50%;
  bottom: 12px;
  translate: -50% 0;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  max-width: 92%;
  padding: 6px 11px;
  border: 1px solid var(--br-line-dark);
  border-radius: 999px;
  color: var(--br-on-ink-faint);
  background: rgba(7, 10, 15, 0.9);
  font-family: var(--br-mono);
  font-size: 8.5px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  transition: color 300ms ease, border-color 300ms ease;
}

.fence i {
  color: var(--br-red-bright);
  font-style: normal;
}

.fence[data-hot='true'] {
  color: #e6ebf3;
  border-color: rgba(225, 29, 72, 0.45);
  background: rgba(76, 5, 25, 0.6);
}

/* DNS ------------------------------------------------------------------- */
.dns {
  display: flex;
  align-items: center;
  gap: 12px;
  min-height: 62px;
  padding: 0 16px;
  border-block: 1px solid var(--br-line-dark);
  background: rgba(255, 255, 255, 0.025);
  transition: background 300ms ease, box-shadow 300ms ease;
}

.dns[data-flash='true'] {
  background: rgba(225, 29, 72, 0.14);
  box-shadow: inset 0 0 0 1px rgba(251, 113, 133, 0.4);
}

.dns-icon {
  display: grid;
  place-items: center;
  width: 28px;
  height: 28px;
  flex: none;
  border: 1px solid rgba(34, 211, 238, 0.24);
  border-radius: 50%;
  color: var(--br-cyan);
  font-size: 12px;
}

.dns-meta {
  min-width: 0;
}

.dns-meta small {
  display: block;
  color: #596477;
  font-family: var(--br-mono);
  font-size: 8px;
  font-weight: 700;
  letter-spacing: 0.12em;
}

.dns-meta code {
  display: block;
  margin-top: 5px;
  color: #d7dee8;
  font-family: var(--br-mono);
  font-size: 10.5px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.dns-target {
  margin-left: auto;
  flex: none;
  text-align: right;
}

.dns-target b {
  display: block;
  color: #f2f5f8;
  font-family: var(--br-mono);
  font-size: 12px;
  font-weight: 700;
  /* Tabular so the A record swap does not nudge the row. */
  font-variant-numeric: tabular-nums;
}

.dns-target small {
  display: block;
  margin-top: 4px;
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: 8.5px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

/* Log ------------------------------------------------------------------- */
.log {
  height: 148px;
  overflow-y: auto;
  padding: 12px 16px;
  font-family: var(--br-mono);
  font-size: 10.5px;
  line-height: 1.75;
  scrollbar-width: thin;
}

/* Chrome makes a scrollable region keyboard-focusable; give it the same ring
   as everything else instead of the 1px browser default. */
.log:focus-visible {
  outline: 3px solid var(--br-cyan);
  outline-offset: -3px;
}

.log-idle {
  margin: 0;
  color: var(--br-on-ink-faint);
}

.log-idle b {
  color: var(--br-red-bright);
  font-weight: 700;
}

.log-line {
  display: flex;
  gap: 9px;
  margin: 0;
}

.log-t {
  flex: none;
  color: #4d5769;
  font-variant-numeric: tabular-nums;
}

.log-msg {
  min-width: 0;
  word-break: break-word;
}

.log-line[data-tone='muted'] .log-msg { color: #7a8598; }
.log-line[data-tone='warn'] .log-msg { color: var(--br-amber); }
.log-line[data-tone='danger'] .log-msg { color: var(--br-red-bright); }
.log-line[data-tone='ok'] .log-msg { color: var(--br-green); }
.log-line[data-tone='primary'] .log-msg { color: var(--br-cyan-soft); }

/* Controls -------------------------------------------------------------- */
.controls {
  display: flex;
  align-items: center;
  gap: 9px;
  padding: 13px 16px;
  border-top: 1px solid var(--br-line-dark);
}

.kill,
.soft {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 9px;
  min-height: 36px;
  padding: 0 15px;
  border: 1px solid transparent;
  border-radius: 6px;
  font-size: 12.5px;
  font-weight: 700;
  cursor: pointer;
  transition: transform 160ms ease, background 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
}

.kill {
  color: white;
  background: var(--br-red);
  box-shadow: 0 8px 24px rgba(225, 29, 72, 0.28);
}

.kill:hover:not(:disabled) {
  transform: translateY(-2px);
  background: #f43f5e;
  box-shadow: 0 12px 30px rgba(225, 29, 72, 0.38);
}

.kill:disabled,
.soft:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.kill-dot {
  width: 7px;
  height: 7px;
  flex: none;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.85);
}

.kill-dot[data-running='true'] {
  animation: br-blink 0.7s steps(1) infinite;
}

.soft {
  color: #c4cddb;
  border-color: rgba(255, 255, 255, 0.18);
  background: rgba(255, 255, 255, 0.045);
}

.soft:hover:not(:disabled) {
  color: white;
  border-color: rgba(255, 255, 255, 0.4);
}

.clock {
  margin-left: auto;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}

@media (prefers-reduced-motion: reduce) {
  .stage {
    transform: none !important;
    transition: none;
  }
  .kill:hover:not(:disabled) {
    transform: none;
  }
}

@media (max-width: 620px) {
  .body { height: 324px; }
  .node { width: 33%; padding: 11px; }
  .node-a { left: 3%; }
  .node-b { right: 3%; }
  .node strong { font-size: 21px; }

  /* The IP is already in the DNS row and the log; at this width it only
     ellipsises the zone. */
  .node-ip { display: none; }

  /* Reclaims the badge's width for the status text; the coloured dot still
     carries the reader's state, and the row keeps the source name visible so
     the repoint is legible. */
  .reader { width: 94%; gap: 8px; padding: 8px 10px; }
  .reader-badge-label { display: none; }
  .reader small { font-size: 9px; }
  .reader-source { font-size: 8.5px; }

  /* The gap between the nodes is too narrow for the label at this width, so it
     moves above them instead of overlapping both cards. */
  .link-label { top: 6px; font-size: 8px; }
  .fence { font-size: 7.5px; padding: 5px 9px; }
  .bar-res { display: none; }
  .log { height: 132px; }

  .controls {
    flex-wrap: wrap;
  }

  .kill,
  .soft {
    flex: 1 1 100%;
    min-height: 42px;
  }

  .clock {
    flex: 1 1 100%;
    margin-left: 0;
    text-align: center;
  }
}
</style>
