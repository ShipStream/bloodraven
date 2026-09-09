<script setup lang="ts">
import type { WireLine } from '~/composables/useDeployReplay'

const props = defineProps<{
  lines: WireLine[]
  idle: string
  /** Number of trailing lines kept visible; older ones scroll away. */
  rows?: number
}>()

const box = ref<HTMLElement | null>(null)

const actorLabel: Record<WireLine['actor'], string> = {
  a: 'a1c3',
  b: 'b7f0',
  operator: 'oper',
  chaos: 'chaos',
}

watch(() => props.lines.length, () => {
  nextTick(() => {
    if (box.value) box.value.scrollTop = box.value.scrollHeight
  })
})
</script>

<template>
  <div
    ref="box"
    class="wire"
    :style="{ '--rows': rows || 5 }"
    role="log"
    aria-live="polite"
    aria-label="Deployment API wire log"
  >
    <p v-if="!lines.length" class="wire-idle">
      {{ idle }}
    </p>
    <p
      v-for="(line, index) in lines"
      :key="index"
      class="wire-line"
      :data-tone="line.tone"
      :data-actor="line.actor"
    >
      <span class="wire-t">{{ line.t }}</span>
      <span class="wire-actor">{{ actorLabel[line.actor] }}</span>
      <span class="wire-dir" aria-hidden="true">{{ line.tone === 'req' ? '→' : line.actor === 'a' || line.actor === 'b' ? '←' : '·' }}</span>
      <span class="wire-msg">{{ line.text }}</span>
    </p>
  </div>
</template>

<style scoped>
.wire {
  height: calc(var(--rows) * 19.5px + 22px);
  overflow-y: auto;
  padding: 11px 16px;
  font-family: var(--br-mono);
  font-size: 10.5px;
  line-height: 19.5px;
  scrollbar-width: thin;
}

.wire:focus-visible {
  outline: 3px solid var(--br-cyan);
  outline-offset: -3px;
}

.wire-idle {
  margin: 0;
  color: var(--br-on-ink-faint);
}

.wire-line {
  display: flex;
  gap: 8px;
  margin: 0;
}

.wire-t {
  flex: none;
  color: #4d5769;
  font-variant-numeric: tabular-nums;
}

.wire-actor {
  flex: none;
  width: 34px;
  color: #6b7688;
  font-weight: 700;
  letter-spacing: 0.04em;
}

.wire-line[data-actor='a'] .wire-actor { color: var(--br-red-bright); }
.wire-line[data-actor='b'] .wire-actor { color: #c084fc; }
.wire-line[data-actor='operator'] .wire-actor { color: var(--br-cyan); }
.wire-line[data-actor='chaos'] .wire-actor { color: var(--br-amber); }

.wire-dir {
  flex: none;
  width: 10px;
  color: #4d5769;
}

.wire-msg {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.wire-line[data-tone='muted'] .wire-msg { color: #7a8598; }
.wire-line[data-tone='req'] .wire-msg { color: #b9c3d2; }
.wire-line[data-tone='ok'] .wire-msg { color: var(--br-green); }
.wire-line[data-tone='warn'] .wire-msg { color: var(--br-amber); }
.wire-line[data-tone='danger'] .wire-msg { color: var(--br-red-bright); }
.wire-line[data-tone='primary'] .wire-msg { color: var(--br-cyan-soft); }

@media (max-width: 620px) {
  .wire {
    font-size: 9.5px;
    padding: 9px 12px;
  }

  .wire-actor { width: 30px; }
}
</style>
