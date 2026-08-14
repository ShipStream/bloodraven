<script setup lang="ts">
/**
 * The three ways into the product, stated once, immediately under the hero.
 * Course is a first-class path here rather than a footer link.
 */
interface Path {
  n: string
  label: string
  title: string
  description: string
  meta: string
  to: string
  target?: string
}

defineProps<{
  kicker?: string
  paths?: Path[]
  sourceLabel?: string
  sourceTo?: string
}>()
</script>

<template>
  <section class="paths">
    <div class="br-shell">
      <p v-if="kicker" class="paths-kicker">
        {{ kicker }}
      </p>

      <div class="paths-grid">
        <NuxtLink
          v-for="path in paths"
          :key="path.n"
          :to="path.to"
          :target="path.target"
          class="path br-focus"
        >
          <span class="path-top">
            <span class="path-n">{{ path.n }}</span>
            <span class="path-meta">{{ path.meta }}</span>
          </span>
          <span class="path-label">{{ path.label }}</span>
          <span class="path-title">{{ path.title }}</span>
          <span class="path-description">{{ path.description }}</span>
          <span class="path-go">
            Open <b aria-hidden="true">→</b>
          </span>
        </NuxtLink>
      </div>

      <a
        v-if="sourceTo"
        :href="sourceTo"
        target="_blank"
        rel="noopener"
        class="paths-source br-focus"
      >
        <UIcon name="i-simple-icons-github" class="paths-source-icon" />
        {{ sourceLabel }}
        <b aria-hidden="true">↗</b>
      </a>
    </div>
  </section>
</template>

<style scoped>
.paths {
  padding: 64px 0 78px;
  border-top: 1px solid var(--br-line-dark-soft);
  color: var(--br-on-ink);
  background: var(--br-ink);
}

.paths-kicker {
  margin: 0 0 26px;
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.paths-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 14px;
}

.path {
  display: flex;
  flex-direction: column;
  padding: 22px;
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  background: var(--br-ink-card);
  text-decoration: none;
  transition: transform 180ms ease, border-color 180ms ease, background 180ms ease;
}

.path:hover {
  transform: translateY(-3px);
  border-color: rgba(251, 113, 133, 0.5);
  background: var(--br-ink-raised);
}

.path-top {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
}

.path-n {
  color: var(--br-red);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.1em;
}

.path-meta {
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 9.5px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.path-label {
  margin-top: 34px;
  color: var(--br-cyan);
  font-family: var(--br-mono);
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.path-title {
  margin-top: 9px;
  color: #f2f5f8;
  font-size: 21px;
  font-weight: 700;
  line-height: 1.15;
  letter-spacing: -0.028em;
}

.path-description {
  margin-top: 11px;
  color: var(--br-on-ink-dim);
  font-size: 13px;
  line-height: 1.6;
}

.path-go {
  margin-top: 22px;
  color: #dbe3ee;
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.path-go b {
  margin-left: 6px;
  color: var(--br-red-bright);
}

.paths-source {
  display: inline-flex;
  align-items: center;
  gap: 9px;
  margin-top: 20px;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 11.5px;
  text-decoration: none;
  transition: color 160ms ease;
}

.paths-source:hover {
  color: white;
}

.paths-source-icon {
  width: 15px;
  height: 15px;
}

.paths-source b {
  color: var(--br-red-bright);
}

@media (prefers-reduced-motion: reduce) {
  .path:hover {
    transform: none;
  }
}

@media (max-width: 900px) {
  .paths-grid {
    grid-template-columns: 1fr;
  }

  .path-label {
    margin-top: 20px;
  }
}

@media (max-width: 620px) {
  .paths {
    padding: 48px 0 56px;
  }
}
</style>
