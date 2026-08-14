<script setup lang="ts">
defineProps<{
  kicker?: string
  title?: string
  /**
   * Rendered on its own line. `outline` strokes it instead of filling it;
   * `accent` fills it in blood red. Both are deliberately rare -- the stroke
   * treatment is reserved for the Ask console and the closing CTA.
   */
  titleTwo?: string
  outline?: boolean
  accent?: boolean
  lede?: string
  /** `ink` inverts the heading colours for use on a dark band. */
  tone?: 'paper' | 'ink'
}>()
</script>

<template>
  <div class="head" :data-tone="tone || 'paper'">
    <div class="head-title">
      <p v-if="kicker" class="br-kicker head-kicker">
        {{ kicker }}
      </p>
      <h2 class="br-display">
        {{ title }}
        <span
          v-if="titleTwo"
          class="head-two"
          :class="{ 'br-outline': outline, 'head-two-accent': accent }"
        >{{ titleTwo }}</span>
      </h2>
    </div>
    <p v-if="lede" class="head-lede">
      {{ lede }}
    </p>
  </div>
</template>

<style scoped>
.head {
  display: grid;
  grid-template-columns: 1.25fr 0.75fr;
  gap: 72px;
  align-items: end;
  margin-bottom: 56px;
}

.head-kicker {
  margin: 0 0 18px;
}

.head[data-tone='ink'] .br-display {
  color: white;
}

.head-two {
  display: block;
}

.head-two-accent {
  color: var(--br-red);
}

.head[data-tone='ink'] .head-two-accent {
  color: var(--br-red-bright);
}

.head-lede {
  max-width: 430px;
  margin: 0 0 4px;
  color: var(--br-text-dim);
  font-size: 15.5px;
  line-height: 1.75;
  text-wrap: pretty;
}

.head[data-tone='ink'] .head-lede {
  color: var(--br-on-ink-dim);
}

@media (max-width: 1080px) {
  .head {
    grid-template-columns: 1fr;
    gap: 24px;
    margin-bottom: 44px;
  }

  .head-lede {
    max-width: 640px;
  }
}
</style>
