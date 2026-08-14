<script setup lang="ts">
/**
 * The three promises the operator makes on a bad day. Numbered cards that
 * invert to ink on hover -- the hover state is the "lights out" reading of the
 * same card.
 */
interface Card {
  token: string
  title: string
  description: string
  detail: string
  to: string
}

defineProps<{
  /** Anchor id, so the header's "Features" link can jump here. */
  id?: string
  kicker?: string
  title?: string
  titleTwo?: string
  lede?: string
  cards?: Card[]
}>()
</script>

<template>
  <section :id="id" class="safety">
    <div class="br-shell">
      <HomeSectionHead
        :kicker="kicker"
        :title="title"
        :title-two="titleTwo"
        :lede="lede"
      />

      <div class="grid">
        <NuxtLink
          v-for="(card, index) in cards"
          :key="card.title"
          :to="card.to"
          class="card br-focus"
        >
          <span class="card-n">{{ String(index + 1).padStart(2, '0') }}</span>
          <span class="card-token">{{ card.token }}</span>
          <h3 class="card-title">{{ card.title }}</h3>
          <p class="card-description">{{ card.description }}</p>
          <span class="card-detail">{{ card.detail }}</span>
          <span class="card-go">Read how it works <b aria-hidden="true">↗</b></span>
        </NuxtLink>
      </div>
    </div>
  </section>
</template>

<style scoped>
.safety {
  padding: 112px 0;
  border-top: 1px solid var(--br-line-light);
  color: var(--br-text);
  background: var(--br-paper-2);
  /* Keeps the sticky header from covering the heading when jumped to. */
  scroll-margin-top: var(--ui-header-height, 64px);
}

.grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  border-top: 1px solid var(--br-line-light);
  border-left: 1px solid var(--br-line-light);
}

.card {
  position: relative;
  display: flex;
  flex-direction: column;
  min-height: 430px;
  padding: 26px 26px 62px;
  border-right: 1px solid var(--br-line-light);
  border-bottom: 1px solid var(--br-line-light);
  color: var(--br-text);
  text-decoration: none;
  transition: color 180ms ease, background 180ms ease, transform 180ms ease, box-shadow 180ms ease;
}

.card:hover {
  z-index: 1;
  color: white;
  background: var(--br-ink-soft);
  transform: translateY(-5px);
  box-shadow: 0 26px 60px rgba(7, 10, 15, 0.28);
}

.card-n {
  color: var(--br-text-dim);
  font-family: var(--br-mono);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.12em;
}

.card:hover .card-n {
  color: var(--br-on-ink-dim);
}

.card-token {
  display: grid;
  place-items: center;
  width: 56px;
  height: 56px;
  margin: 52px 0 24px;
  border: 1px solid rgba(225, 29, 72, 0.38);
  color: var(--br-red);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.04em;
  transform: rotate(-3deg);
  transition: color 180ms ease, border-color 180ms ease;
}

.card:hover .card-token {
  color: var(--br-cyan);
  border-color: rgba(34, 211, 238, 0.45);
}

.card-title {
  margin: 0;
  font-size: 24px;
  line-height: 1.1;
  letter-spacing: -0.035em;
}

.card-description {
  margin: 15px 0 0;
  color: var(--br-text-dim);
  font-size: 13px;
  line-height: 1.65;
  text-wrap: pretty;
}

.card:hover .card-description {
  color: var(--br-on-ink-dim);
}

.card-detail {
  margin-top: auto;
  padding-top: 15px;
  border-top: 1px solid var(--br-line-light);
  color: var(--br-text-faint);
  font-family: var(--br-mono);
  font-size: 9px;
  line-height: 1.6;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  transition: border-color 180ms ease, color 180ms ease;
}

.card:hover .card-detail {
  color: var(--br-on-ink-faint);
  border-color: var(--br-line-dark);
}

.card-go {
  position: absolute;
  left: 26px;
  bottom: 26px;
  color: var(--br-text-dim);
  font-size: 11px;
  font-weight: 800;
}

.card:hover .card-go {
  color: #dbe3ee;
}

.card-go b {
  margin-left: 5px;
  color: var(--br-red);
}

.card:hover .card-go b {
  color: var(--br-red-bright);
}

@media (prefers-reduced-motion: reduce) {
  .card:hover {
    transform: none;
  }
}

@media (max-width: 1080px) {
  .grid {
    grid-template-columns: 1fr;
  }

  .card {
    min-height: 0;
    padding-bottom: 66px;
  }

  .card-token {
    margin-top: 28px;
  }
}

@media (max-width: 620px) {
  .safety {
    padding: 72px 0;
  }

  .card {
    padding: 22px 20px 62px;
  }

  .card-go {
    left: 20px;
  }
}
</style>
