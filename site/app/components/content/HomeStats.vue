<script setup lang="ts">
defineProps<{
  items?: { value: string, label: string }[]
}>()
</script>

<template>
  <section class="stats" aria-label="Bloodraven by the numbers">
    <dl class="br-shell stats-grid">
      <div v-for="item in items" :key="item.label" class="stat">
        <dt class="stat-value">
          {{ item.value }}
        </dt>
        <dd class="stat-label">
          {{ item.label }}
        </dd>
      </div>
    </dl>
  </section>
</template>

<style scoped>
.stats {
  border-top: 1px solid var(--br-line-dark-soft);
  background: #080b10;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  padding: 0;
  margin-block: 0;
}

.stat {
  padding: 26px 22px 26px 0;
  border-right: 1px solid var(--br-line-dark-soft);
}

.stat:first-child {
  padding-left: 0;
}

.stat:not(:first-child) {
  padding-left: 22px;
}

.stat:last-child {
  border-right: 0;
}

.stat-value {
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: clamp(1.5rem, 2.4vw, 2rem);
  font-weight: 800;
  letter-spacing: -0.03em;
  line-height: 1;
}

.stat-label {
  margin: 10px 0 0;
  max-width: 220px;
  color: var(--br-on-ink-dim);
  font-size: 12px;
  line-height: 1.5;
  text-wrap: pretty;
}

@media (max-width: 900px) {
  .stats-grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .stat {
    padding: 20px 18px 20px 0;
  }

  .stat:nth-child(odd) {
    padding-left: 0;
  }

  .stat:nth-child(even) {
    padding-left: 18px;
    border-right: 0;
  }

  .stat:nth-child(-n + 2) {
    border-bottom: 1px solid var(--br-line-dark-soft);
  }
}
</style>
