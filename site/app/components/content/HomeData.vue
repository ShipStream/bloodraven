<script setup lang="ts">
interface Card {
  token: string
  title: string
  description: string
  code: string
  to: string
}

interface FlowStep {
  n: string
  title: string
  detail: string
}

defineProps<{
  kicker?: string
  title?: string
  titleTwo?: string
  lede?: string
  cards?: Card[]
  backupTitle?: string
  backupTitleTwo?: string
  backupDescription?: string
  backupTo?: string
  backupPoints?: string[]
  flow?: FlowStep[]
}>()
</script>

<template>
  <section class="data">
    <div class="br-shell">
      <HomeSectionHead
        tone="ink"
        :kicker="kicker"
        :title="title"
        :title-two="titleTwo"
        :lede="lede"
      />

      <div class="cards">
        <NuxtLink
          v-for="card in cards"
          :key="card.title"
          :to="card.to"
          class="card br-focus"
        >
          <span class="card-token">{{ card.token }}</span>
          <h3 class="card-title">{{ card.title }}</h3>
          <p class="card-description">{{ card.description }}</p>
          <pre class="card-code"><code>{{ card.code }}</code></pre>
          <span class="card-go">Explore <b aria-hidden="true">→</b></span>
        </NuxtLink>
      </div>

      <NuxtLink :to="backupTo" class="backup br-focus">
        <div class="backup-copy">
          <span class="card-token">B/R</span>
          <div class="backup-head">
            <h3 class="backup-title">
              {{ backupTitle }}
              <span>{{ backupTitleTwo }}</span>
            </h3>
            <span class="backup-arrow" aria-hidden="true">↗</span>
          </div>
          <p class="backup-description">{{ backupDescription }}</p>
          <ul class="backup-points">
            <li v-for="point in backupPoints" :key="point">
              {{ point }}
            </li>
          </ul>
        </div>

        <ol class="flow" aria-label="Backup lifecycle">
          <li v-for="(step, index) in flow" :key="step.n" class="flow-step">
            <span class="flow-n">{{ step.n }}</span>
            <span class="flow-body">
              <b>{{ step.title }}</b>
              <small>{{ step.detail }}</small>
            </span>
            <span v-if="index < (flow?.length || 0) - 1" class="flow-arrow" aria-hidden="true">↓</span>
          </li>
        </ol>
      </NuxtLink>
    </div>
  </section>
</template>

<style scoped>
.data {
  padding: 112px 0;
  color: var(--br-on-ink);
  background: var(--br-ink);
}

.cards {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 16px;
}

.card {
  display: flex;
  flex-direction: column;
  /* Grid items default to min-width:auto, so the `white-space: pre` code block
     below would widen the track past the viewport instead of scrolling. */
  min-width: 0;
  padding: 26px;
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  color: white;
  background: var(--br-ink-card);
  text-decoration: none;
  transition: transform 180ms ease, border-color 180ms ease, background 180ms ease;
}

.card:hover {
  transform: translateY(-4px);
  border-color: rgba(34, 211, 238, 0.38);
  background: var(--br-ink-raised);
}

.card-token {
  display: grid;
  place-items: center;
  width: 54px;
  height: 54px;
  border: 1px solid rgba(34, 211, 238, 0.28);
  color: var(--br-cyan);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.04em;
  transform: rotate(-3deg);
}

.card-title {
  margin: 30px 0 0;
  font-size: 24px;
  line-height: 1.1;
  letter-spacing: -0.035em;
}

.card-description {
  margin: 14px 0 20px;
  color: var(--br-on-ink-dim);
  font-size: 13px;
  line-height: 1.65;
  text-wrap: pretty;
}

.card-code {
  margin: 0 0 22px;
  padding: 14px;
  overflow-x: auto;
  border: 1px solid var(--br-line-dark);
  border-radius: 6px;
  background: rgba(0, 0, 0, 0.32);
  color: #94a3b8;
  font-family: var(--br-mono);
  font-size: 10.5px;
  line-height: 1.7;
  white-space: pre;
}

.card-go {
  margin-top: auto;
  color: #e5eaf1;
  font-size: 10.5px;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.card-go b {
  margin-left: 5px;
  color: var(--br-red-bright);
}

/* Backup ---------------------------------------------------------------- */
.backup {
  display: grid;
  grid-template-columns: 1.15fr 0.85fr;
  gap: 48px;
  margin-top: 16px;
  padding: 38px;
  border: 1px solid rgba(225, 29, 72, 0.34);
  border-radius: 8px;
  color: white;
  background:
    radial-gradient(circle at 84% 30%, rgba(225, 29, 72, 0.14), transparent 36%),
    #10131a;
  text-decoration: none;
  transition: border-color 180ms ease;
}

.backup > * {
  min-width: 0;
}

.backup:hover {
  border-color: rgba(225, 29, 72, 0.75);
}

.backup .card-token {
  border-color: rgba(225, 29, 72, 0.4);
  color: var(--br-red-bright);
}

.backup-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  margin-top: 28px;
}

.backup-title {
  margin: 0;
  font-size: clamp(1.9rem, 3.2vw, 2.9rem);
  line-height: 1.02;
  letter-spacing: -0.045em;
}

.backup-title span {
  display: block;
  color: var(--br-red-bright);
}

.backup-arrow {
  flex: none;
  color: var(--br-red-bright);
  font-size: 26px;
  line-height: 1;
}

.backup-description {
  max-width: 560px;
  margin: 20px 0 0;
  color: var(--br-on-ink-dim);
  font-size: 13.5px;
  line-height: 1.7;
  text-wrap: pretty;
}

.backup-points {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 5px 26px;
  margin: 22px 0 0;
  padding: 0;
  list-style: none;
}

.backup-points li {
  position: relative;
  padding-left: 15px;
  color: #97a2b4;
  font-family: var(--br-mono);
  font-size: 10.5px;
  line-height: 1.7;
}

.backup-points li::before {
  content: '·';
  position: absolute;
  left: 2px;
  color: var(--br-red-bright);
  font-weight: 800;
}

.flow {
  display: flex;
  flex-direction: column;
  justify-content: center;
  /* Wide enough for the connector glyph to sit clear of both rows. */
  gap: 16px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.flow-step {
  position: relative;
  display: grid;
  grid-template-columns: 34px 1fr;
  align-items: center;
  min-height: 60px;
  padding: 0 16px;
  border: 1px solid var(--br-line-dark);
  border-radius: 6px;
  background: rgba(4, 7, 11, 0.6);
}

.flow-n {
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: 9px;
  font-weight: 800;
}

.flow-body b {
  display: block;
  font-size: 12.5px;
}

.flow-body small {
  display: block;
  margin-top: 3px;
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 9px;
}

.flow-arrow {
  position: absolute;
  left: 21px;
  bottom: -14px;
  z-index: 1;
  color: var(--br-on-ink-faint);
  font-size: 11px;
  line-height: 1;
}

@media (prefers-reduced-motion: reduce) {
  .card:hover {
    transform: none;
  }
}

@media (max-width: 1080px) {
  .cards {
    grid-template-columns: 1fr;
  }

  .backup {
    grid-template-columns: 1fr;
    gap: 32px;
  }
}

@media (max-width: 620px) {
  .data {
    padding: 72px 0;
  }

  .card {
    padding: 22px 20px;
  }

  .backup {
    padding: 26px 20px;
  }

  .backup-points {
    grid-template-columns: 1fr;
  }
}
</style>
