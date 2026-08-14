<script setup lang="ts">
defineProps<{
  kicker?: string
  title?: string
  titleTwo?: string
  lede?: string
  wsLabel?: string
  wsTitle?: string
  wsTitleTwo?: string
  wsDescription?: string
  wsCode?: string
  wsTo?: string
  dfLabel?: string
  dfTitle?: string
  dfTitleTwo?: string
  dfDescription?: string
  dfSteps?: { cmd: string, detail: string }[]
  dfTo?: string
}>()
</script>

<template>
  <section class="app">
    <div class="br-shell">
      <HomeSectionHead
        tone="ink"
        :kicker="kicker"
        :title="title"
        :title-two="titleTwo"
        :lede="lede"
      />

      <div class="panels">
        <NuxtLink :to="wsTo" class="ws br-focus">
          <div class="ws-copy">
            <span class="label">{{ wsLabel }}</span>
            <h3 class="panel-title">
              {{ wsTitle }}
              <span>{{ wsTitleTwo }}</span>
            </h3>
            <p class="panel-description">{{ wsDescription }}</p>
            <pre class="ws-snippet"><code>{{ wsCode }}</code></pre>
            <span class="panel-go">Integrate your app <b aria-hidden="true">→</b></span>
          </div>

          <div class="event" aria-hidden="true">
            <div class="event-bar">
              <i />
              <code>GET /ws/status</code>
              <span>LIVE</span>
            </div>
            <pre class="event-json"><code><span class="p">{</span>
  <span class="k">"group"</span><span class="p">:</span> <span class="s">"orders"</span><span class="p">,</span>
  <span class="k">"activeSite"</span><span class="p">:</span> <span class="hot">"pdx"</span><span class="p">,</span>
  <span class="k">"previousSite"</span><span class="p">:</span> <span class="s">"iad"</span><span class="p">,</span>
  <span class="k">"reason"</span><span class="p">:</span> <span class="s">"site_unreachable"</span><span class="p">,</span>
  <span class="k">"hostname"</span><span class="p">:</span> <span class="s">"orders.az.example.com"</span>
<span class="p">}</span></code></pre>
            <div class="event-foot">
              <span>↻</span> pools reconnect before the first write error
            </div>
          </div>
        </NuxtLink>

        <NuxtLink :to="dfTo" class="df br-focus">
          <span class="label label-dark">{{ dfLabel }}</span>
          <h3 class="panel-title panel-title-dark">
            {{ dfTitle }}
            <span>{{ dfTitleTwo }}</span>
          </h3>
          <p class="panel-description panel-description-dark">{{ dfDescription }}</p>

          <ol class="df-steps">
            <li v-for="(step, index) in dfSteps" :key="step.cmd">
              <span class="df-n">{{ index + 1 }}</span>
              <code>{{ step.cmd }}</code>
              <small>{{ step.detail }}</small>
            </li>
          </ol>

          <span class="panel-go panel-go-dark">See Dragonfly continuity <b aria-hidden="true">→</b></span>
        </NuxtLink>
      </div>
    </div>
  </section>
</template>

<style scoped>
.app {
  padding: 112px 0;
  color: var(--br-on-ink);
  background: var(--br-ink);
}

.panels {
  display: grid;
  grid-template-columns: 1.12fr 0.88fr;
  gap: 16px;
}

/* Pre-formatted snippets must scroll inside their card, not widen the track. */
.panels > *,
.ws > * {
  min-width: 0;
}

.label {
  color: var(--br-red);
  font-family: var(--br-mono);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.panel-title {
  margin: 22px 0 0;
  font-size: 30px;
  line-height: 1.08;
  letter-spacing: -0.04em;
}

.panel-title span {
  display: block;
  color: var(--br-cyan);
}

.panel-description {
  margin: 15px 0 0;
  color: var(--br-on-ink-dim);
  font-size: 13px;
  line-height: 1.68;
  text-wrap: pretty;
}

.panel-go {
  margin-top: auto;
  padding-top: 22px;
  color: #dce3ec;
  font-size: 11px;
  font-weight: 800;
}

.panel-go b {
  margin-left: 6px;
  color: var(--br-cyan);
}

/* WebSocket panel ------------------------------------------------------- */
.ws {
  display: grid;
  grid-template-columns: 0.94fr 1.06fr;
  gap: 30px;
  padding: 38px;
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  color: white;
  background: var(--br-ink-card);
  text-decoration: none;
  transition: border-color 180ms ease;
}

.ws:hover {
  border-color: rgba(34, 211, 238, 0.42);
}

.ws-copy {
  display: flex;
  flex-direction: column;
}

.ws-snippet {
  margin: 20px 0 0;
  padding: 13px 14px;
  overflow-x: auto;
  border: 1px solid var(--br-line-dark);
  border-radius: 6px;
  background: rgba(0, 0, 0, 0.32);
  color: #94a3b8;
  font-family: var(--br-mono);
  font-size: 10px;
  line-height: 1.75;
  /* The column is too narrow for these lines; wrapping reads better than a
     snippet that visibly cuts off mid-identifier. */
  white-space: pre-wrap;
  word-break: break-word;
}

.event {
  align-self: center;
  padding: 18px;
  border: 1px solid rgba(34, 211, 238, 0.2);
  border-radius: 8px;
  background: var(--br-ink);
  box-shadow: 0 24px 50px rgba(0, 0, 0, 0.35);
  transform: rotate(1.6deg);
}

.event-bar {
  display: flex;
  align-items: center;
  gap: 9px;
  padding-bottom: 13px;
  margin-bottom: 14px;
  border-bottom: 1px solid var(--br-line-dark);
  font-family: var(--br-mono);
  font-size: 9px;
  color: var(--br-on-ink-dim);
}

.event-bar i {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--br-green);
  box-shadow: 0 0 10px var(--br-green);
}

.event-bar span {
  margin-left: auto;
  color: var(--br-green);
  font-size: 8px;
  letter-spacing: 0.1em;
}

.event-json {
  margin: 0;
  overflow-x: auto;
  font-family: var(--br-mono);
  font-size: 10.5px;
  line-height: 1.85;
  white-space: pre;
}

.event-json .k { color: var(--br-cyan-soft); }
.event-json .s { color: #94a3b8; }
.event-json .p { color: #4d5769; }
.event-json .hot { color: var(--br-red-bright); font-weight: 700; }

.event-foot {
  margin-top: 18px;
  padding: 11px 12px;
  border: 1px solid rgba(225, 29, 72, 0.24);
  border-radius: 5px;
  background: rgba(225, 29, 72, 0.08);
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 9px;
  line-height: 1.5;
}

.event-foot span {
  margin-right: 7px;
  color: var(--br-red-bright);
}

/* Dragonfly panel ------------------------------------------------------- */
.df {
  position: relative;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  padding: 38px;
  border-radius: 8px;
  color: #121826;
  background: #dfe8e8;
  text-decoration: none;
}

.df::after {
  content: '';
  position: absolute;
  width: 300px;
  height: 300px;
  right: -120px;
  top: -110px;
  border: 1px solid rgba(15, 23, 42, 0.14);
  border-radius: 50%;
  pointer-events: none;
}

.label-dark {
  color: #4a5566;
}

.panel-title-dark span {
  color: var(--br-red-deep);
}

.panel-description-dark {
  color: #55627a;
}

.df-steps {
  margin: 26px 0 0;
  padding: 0;
  list-style: none;
  display: grid;
  gap: 8px;
}

.df-steps li {
  display: grid;
  grid-template-columns: 22px 1fr;
  gap: 4px 10px;
  align-items: center;
  padding: 11px 13px;
  border: 1px solid rgba(15, 23, 42, 0.14);
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.5);
}

.df-n {
  grid-row: span 2;
  display: grid;
  place-items: center;
  width: 22px;
  height: 22px;
  border-radius: 50%;
  background: var(--br-red-deep);
  color: white;
  font-family: var(--br-mono);
  font-size: 10px;
  font-weight: 800;
}

.df-steps code {
  font-family: var(--br-mono);
  font-size: 11.5px;
  font-weight: 700;
  color: #111827;
}

.df-steps small {
  color: #5a6678;
  font-family: var(--br-mono);
  font-size: 9.5px;
}

.panel-go-dark {
  color: #192331;
}

.panel-go-dark b {
  color: var(--br-red-deep);
}

@media (max-width: 1080px) {
  .panels {
    grid-template-columns: 1fr;
  }

  .ws {
    grid-template-columns: 1fr;
  }

  .event {
    justify-self: center;
    width: min(400px, 100%);
    transform: none;
  }
}

@media (max-width: 620px) {
  .app {
    padding: 72px 0;
  }

  .ws,
  .df {
    padding: 26px 20px;
  }

  .panel-title {
    font-size: 26px;
  }
}
</style>
