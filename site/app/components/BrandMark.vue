<script setup lang="ts">
/**
 * The Bloodraven mark.
 *
 * Rendered as an image rather than inline SVG because the master is a painted
 * raster (see `scripts/build-brand-assets.mjs`). Two variants ship: the plain
 * alpha cut-out, and a rim-lit one for dark surfaces -- the raven is nearly
 * black, so without the rim it reads as a floating red eye on a dark header.
 *
 * Both are rendered and swapped in CSS rather than through UColorModeImage:
 * the site's dark mode is a `.dark` class, not an OS media query, and doing it
 * in CSS keeps the swap free of hydration flicker. `surface="dark"` forces the
 * rim-lit variant for components that are dark in both colour modes (the Ask
 * console, the ink bands).
 */
withDefaults(defineProps<{
  height?: number
  surface?: 'auto' | 'dark'
}>(), { height: 26, surface: 'auto' })
</script>

<template>
  <span class="br-mark" :style="{ height: `${height}px` }" aria-hidden="true">
    <img
      v-if="surface === 'auto'"
      src="/img/brand/mark.png"
      alt=""
      width="128"
      height="96"
      class="br-mark-img br-mark-light"
    >
    <img
      src="/img/brand/mark-dark.png"
      alt=""
      width="128"
      height="96"
      class="br-mark-img"
      :class="surface === 'auto' ? 'br-mark-dark' : ''"
    >
  </span>
</template>

<style scoped>
.br-mark {
  display: inline-flex;
  flex: none;
  align-items: center;
}

.br-mark-img {
  display: block;
  width: auto;
  height: 100%;
}

/* Only the auto variant participates in the colour-mode swap; a forced
   `surface="dark"` mark renders its single image unconditionally. */
.br-mark-dark {
  display: none;
}

:global(.dark) .br-mark-light {
  display: none;
}

:global(.dark) .br-mark-dark {
  display: block;
}
</style>
