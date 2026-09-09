<script setup lang="ts">
/**
 * The deployment API section: a section head, the lease-lane replay, and the
 * benefit points the instrument just demonstrated.
 *
 * `?deployT=<seconds>` and `?deployScenario=<mutex|hold|fence>` freeze a
 * deterministic frame for design review and screenshots.
 */
import type { ScenarioId } from '~/composables/useDeployReplay'

defineProps<{
  id?: string
  kicker?: string
  title?: string
  titleTwo?: string
  lede?: string
  points?: { token: string, title: string, description: string }[]
  linkLabel?: string
  linkTo?: string
}>()

const route = useRoute()

const initialT = computed(() => {
  const q = route.query.deployT
  const n = typeof q === 'string' ? Number.parseFloat(q) : Number.NaN
  return Number.isFinite(n) ? n : undefined
})

const initialScenario = computed<ScenarioId | undefined>(() => {
  const q = route.query.deployScenario
  return q === 'mutex' || q === 'hold' || q === 'fence' ? q : undefined
})
</script>

<template>
  <section :id="id" class="deploy">
    <div class="br-shell">
      <HomeSectionHead
        tone="paper"
        :kicker="kicker"
        :title="title"
        :title-two="titleTwo"
        accent
        :lede="lede"
      />

      <HomeDeployLanes
        :key="`lanes-${initialScenario}-${initialT}`"
        :initial-scenario="initialScenario"
        :initial-t="initialT"
      />

      <dl class="points">
        <div v-for="point in points" :key="point.title" class="point">
          <dt>
            <code>{{ point.token }}</code>
            {{ point.title }}
          </dt>
          <dd>{{ point.description }}</dd>
        </div>
      </dl>

      <NuxtLink v-if="linkTo" :to="linkTo" class="deploy-link br-focus">
        {{ linkLabel }}
        <b aria-hidden="true">→</b>
      </NuxtLink>
    </div>
  </section>
</template>

<style scoped>
.deploy {
  padding: 112px 0;
  border-top: 1px solid var(--br-line-light);
  color: var(--br-text);
  background: var(--br-paper);
}

.points {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 22px 30px;
  margin: 44px 0 0;
}

.point {
  padding-top: 16px;
  border-top: 1px solid var(--br-line-light);
}

.point dt {
  display: flex;
  align-items: center;
  gap: 9px;
  font-size: 13.5px;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.point dt code {
  flex: none;
  padding: 2px 6px;
  border: 1px solid var(--br-line-light);
  border-radius: 4px;
  color: var(--br-red);
  font-family: var(--br-mono);
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.point dd {
  margin: 8px 0 0;
  color: var(--br-text-dim);
  font-size: 12.5px;
  line-height: 1.6;
  text-wrap: pretty;
}

.deploy-link {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 44px;
  margin-top: 34px;
  padding: 0 18px;
  border: 1px solid var(--br-line-light);
  border-radius: 6px;
  color: var(--br-text);
  font-size: 13px;
  font-weight: 700;
  text-decoration: none;
  transition: border-color 160ms ease, background 160ms ease;
}

.deploy-link:hover {
  border-color: color-mix(in srgb, var(--br-red) 55%, transparent);
  background: color-mix(in srgb, var(--br-red) 7%, transparent);
}

.deploy-link b {
  color: var(--br-red);
}

@media (max-width: 1080px) {
  .points {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 620px) {
  .deploy {
    padding: 72px 0;
  }

  .points {
    grid-template-columns: 1fr;
    gap: 18px;
  }
}
</style>
