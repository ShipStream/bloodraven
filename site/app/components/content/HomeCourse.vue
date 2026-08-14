<script setup lang="ts">
defineProps<{
  kicker?: string
  title?: string
  titleTwo?: string
  description?: string
  stats?: { value: string, label: string }[]
  units?: { n: string, title: string }[]
  note?: string
  primaryLabel?: string
  primaryTo?: string
  secondaryLabel?: string
  secondaryTo?: string
}>()
</script>

<template>
  <section class="course">
    <div class="course-glow" aria-hidden="true" />

    <div class="br-shell course-shell">
      <div class="course-copy">
        <p v-if="kicker" class="br-kicker course-kicker">
          {{ kicker }}
        </p>

        <h2 class="br-display course-title">
          {{ title }}
          <span>{{ titleTwo }}</span>
        </h2>

        <p class="course-description">
          {{ description }}
        </p>

        <dl class="course-stats">
          <div v-for="stat in stats" :key="stat.label">
            <dt>{{ stat.value }}</dt>
            <dd>{{ stat.label }}</dd>
          </div>
        </dl>

        <div class="course-actions">
          <NuxtLink :to="primaryTo" target="_blank" class="btn btn-primary br-focus">
            {{ primaryLabel }}
            <b aria-hidden="true">→</b>
          </NuxtLink>
          <NuxtLink :to="secondaryTo" class="btn btn-soft br-focus">
            {{ secondaryLabel }}
          </NuxtLink>
        </div>

        <p class="course-note">
          {{ note }}
        </p>
      </div>

      <div class="course-units">
        <div class="ticks" aria-hidden="true">
          <span v-for="unit in units" :key="unit.n" />
        </div>
        <ol>
          <li v-for="unit in units" :key="unit.n">
            <span class="unit-n">{{ unit.n }}</span>
            <span class="unit-title">{{ unit.title }}</span>
          </li>
        </ol>
      </div>
    </div>
  </section>
</template>

<style scoped>
.course {
  position: relative;
  overflow: hidden;
  padding: 112px 0;
  color: var(--br-on-ink);
  background: var(--br-ink);
}

.course-glow {
  position: absolute;
  z-index: 0;
  width: 540px;
  height: 540px;
  right: -220px;
  top: -160px;
  border-radius: 50%;
  background: rgba(225, 29, 72, 0.1);
  filter: blur(90px);
}

.course-shell {
  position: relative;
  z-index: 1;
  display: grid;
  grid-template-columns: 1fr 0.92fr;
  gap: 64px;
  align-items: center;
}

.course-kicker {
  margin: 0 0 18px;
  color: var(--br-red-bright);
}

.course-title {
  color: white;
}

.course-title span {
  display: block;
  color: var(--br-cyan);
}

.course-description {
  max-width: 540px;
  margin: 24px 0 0;
  color: var(--br-on-ink-dim);
  font-size: 15px;
  line-height: 1.72;
  text-wrap: pretty;
}

.course-stats {
  display: flex;
  flex-wrap: wrap;
  gap: 12px 34px;
  margin: 28px 0 0;
}

.course-stats dt {
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: 26px;
  font-weight: 800;
  line-height: 1;
}

.course-stats dd {
  margin: 7px 0 0;
  color: var(--br-on-ink-dim);
  font-size: 12px;
}

.course-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 11px;
  margin-top: 32px;
}

.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 9px;
  min-height: 48px;
  padding: 0 20px;
  border: 1px solid transparent;
  border-radius: 6px;
  font-size: 13.5px;
  font-weight: 700;
  text-decoration: none;
  transition: transform 180ms ease, background 180ms ease, border-color 180ms ease, box-shadow 180ms ease;
}

.btn:hover {
  transform: translateY(-2px);
}

.btn-primary {
  color: white;
  background: var(--br-red);
  box-shadow: 0 10px 30px rgba(225, 29, 72, 0.28);
}

.btn-primary:hover {
  background: #f43f5e;
  box-shadow: 0 14px 40px rgba(225, 29, 72, 0.4);
}

.btn-soft {
  color: #e8edf5;
  border-color: rgba(255, 255, 255, 0.2);
  background: rgba(255, 255, 255, 0.05);
}

.btn-soft:hover {
  border-color: rgba(255, 255, 255, 0.45);
}

.course-note {
  margin: 20px 0 0;
  max-width: 520px;
  color: var(--br-on-ink-faint);
  font-size: 11.5px;
  line-height: 1.6;
}

/* Units ----------------------------------------------------------------- */
.ticks {
  display: grid;
  grid-template-columns: repeat(7, 1fr);
  gap: 5px;
  margin-bottom: 16px;
}

.ticks span {
  height: 3px;
  border-radius: 2px;
  background: var(--br-red);
}

.course-units ol {
  display: grid;
  gap: 7px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.course-units li {
  display: grid;
  grid-template-columns: 26px 1fr;
  gap: 12px;
  align-items: start;
  padding: 12px 15px;
  border: 1px solid var(--br-line-dark);
  border-radius: 7px;
  background: rgba(13, 17, 24, 0.75);
  transition: border-color 160ms ease;
}

.course-units li:hover {
  border-color: rgba(251, 113, 133, 0.4);
}

.unit-n {
  display: grid;
  place-items: center;
  width: 24px;
  height: 24px;
  border-radius: 5px;
  background: rgba(225, 29, 72, 0.14);
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 800;
}

.unit-title {
  color: var(--br-on-ink-dim);
  font-size: 12.5px;
  line-height: 1.55;
  text-wrap: pretty;
}

@media (prefers-reduced-motion: reduce) {
  .btn:hover {
    transform: none;
  }
}

@media (max-width: 1080px) {
  .course-shell {
    grid-template-columns: 1fr;
    gap: 44px;
  }

  .course-description {
    max-width: 640px;
  }
}

@media (max-width: 620px) {
  .course {
    padding: 72px 0;
  }

  .course-actions {
    flex-direction: column;
    align-items: stretch;
  }

  .btn {
    width: 100%;
  }
}
</style>
