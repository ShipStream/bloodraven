<script setup lang="ts">
import { DEPLOY_SCENARIOS, fmtT } from '~/composables/useDeployReplay'
import type { ScenarioId } from '~/composables/useDeployReplay'

const props = defineProps<{
  scenarioId: ScenarioId
  clock: number
  duration: number
  running: boolean
  finished: boolean
  /** Reduced motion: chips jump straight to the terminal frame. */
  reduced: boolean
}>()

const emit = defineEmits<{
  select: [id: ScenarioId]
  replay: []
  seek: [t: number]
}>()

const scrub = computed({
  get: () => Math.min(props.clock, props.duration),
  set: (value: number) => emit('seek', value),
})
</script>

<template>
  <div class="controls">
    <div class="chips" role="group" aria-label="Choose a deployment scenario">
      <button
        v-for="s in DEPLOY_SCENARIOS"
        :key="s.id"
        type="button"
        class="chip br-focus"
        :aria-pressed="s.id === scenarioId"
        :data-active="s.id === scenarioId"
        @click="emit('select', s.id)"
      >
        {{ s.label }}
      </button>
    </div>

    <div class="transport">
      <button type="button" class="replay br-focus" @click="emit('replay')">
        <span class="replay-dot" :data-running="running" />
        {{ reduced ? 'Show end state' : finished ? 'Run it again' : running ? 'Restart' : 'Play' }}
      </button>
      <label class="scrub">
        <span class="sr-only">Scrub the virtual clock</span>
        <input
          v-model.number="scrub"
          class="br-focus"
          type="range"
          min="0"
          :max="duration"
          step="0.1"
          :aria-valuetext="fmtT(scrub)"
        >
      </label>
      <span class="clock">{{ fmtT(Math.min(clock, duration)) }}</span>
    </div>
  </div>
</template>

<style scoped>
.controls {
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 12px 16px 14px;
  border-top: 1px solid var(--br-line-dark);
}

.chips {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.chip {
  min-height: 28px;
  padding: 0 11px;
  border: 1px solid rgba(255, 255, 255, 0.16);
  border-radius: 999px;
  color: #c4cddb;
  background: rgba(255, 255, 255, 0.035);
  font-family: var(--br-mono);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.04em;
  cursor: pointer;
  transition: border-color 160ms ease, background 160ms ease, color 160ms ease;
}

.chip:hover {
  color: white;
  border-color: rgba(255, 255, 255, 0.38);
}

.chip[data-active='true'] {
  color: white;
  border-color: rgba(251, 113, 133, 0.7);
  background: rgba(225, 29, 72, 0.18);
}

.transport {
  display: flex;
  align-items: center;
  gap: 12px;
}

.replay {
  display: inline-flex;
  align-items: center;
  gap: 9px;
  flex: none;
  min-height: 34px;
  padding: 0 14px;
  border: 1px solid transparent;
  border-radius: 6px;
  color: white;
  background: var(--br-red);
  box-shadow: 0 8px 24px rgba(225, 29, 72, 0.28);
  font-size: 12px;
  font-weight: 700;
  cursor: pointer;
  transition: transform 160ms ease, background 160ms ease;
}

.replay:hover {
  transform: translateY(-2px);
  background: #f43f5e;
}

.replay-dot {
  width: 7px;
  height: 7px;
  flex: none;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.85);
}

.replay-dot[data-running='true'] {
  animation: br-blink 0.7s steps(1) infinite;
}

.scrub {
  display: flex;
  flex: 1 1 auto;
  min-width: 0;
}

.scrub input {
  width: 100%;
  accent-color: var(--br-red-bright);
  cursor: pointer;
}

.clock {
  flex: none;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}

.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
}

@media (prefers-reduced-motion: reduce) {
  .replay:hover {
    transform: none;
  }
}

@media (max-width: 620px) {
  .transport {
    flex-wrap: wrap;
  }

  .replay {
    flex: 1 1 100%;
    justify-content: center;
    min-height: 40px;
  }
}
</style>
