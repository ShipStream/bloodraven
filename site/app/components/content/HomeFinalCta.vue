<script setup lang="ts">
interface CtaLink {
  label: string
  to: string
  target?: string
  icon?: string
  variant?: 'primary' | 'outline' | 'ghost'
}

defineProps<{
  mark?: string
  title?: string
  titleTwo?: string
  description?: string
  links?: CtaLink[]
}>()
</script>

<template>
  <section class="final">
    <div class="final-grid" aria-hidden="true" />
    <div class="final-glow" aria-hidden="true" />

    <div class="br-shell final-inner">
      <p v-if="mark" class="final-mark">
        {{ mark }}
      </p>

      <h2 class="br-display final-title">
        {{ title }}
        <span class="br-outline">{{ titleTwo }}</span>
      </h2>

      <p class="final-description">
        {{ description }}
      </p>

      <div class="final-actions">
        <NuxtLink
          v-for="link in links"
          :key="link.label"
          :to="link.to"
          :target="link.target"
          class="btn br-focus"
          :data-variant="link.variant || 'ghost'"
        >
          <UIcon v-if="link.icon" :name="link.icon" class="btn-icon" />
          {{ link.label }}
        </NuxtLink>
      </div>
    </div>
  </section>
</template>

<style scoped>
.final {
  position: relative;
  overflow: hidden;
  padding: 120px 0;
  border-top: 1px solid var(--br-line-dark-soft);
  color: var(--br-on-ink);
  background: #080a0e;
  text-align: center;
}

.final-grid {
  position: absolute;
  inset: 0;
  opacity: 0.2;
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.08) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.08) 1px, transparent 1px);
  background-size: 54px 54px;
  mask-image: radial-gradient(circle, black, transparent 68%);
}

.final-glow {
  position: absolute;
  width: 480px;
  height: 240px;
  left: 50%;
  top: 46%;
  translate: -50% -50%;
  border-radius: 50%;
  background: rgba(225, 29, 72, 0.16);
  filter: blur(80px);
}

.final-inner {
  position: relative;
  z-index: 1;
}

.final-mark {
  margin: 0 0 24px;
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.final-title {
  color: white;
  --br-stroke: rgba(255, 255, 255, 0.62);
}

.final-description {
  max-width: 560px;
  margin: 26px auto 0;
  color: var(--br-on-ink-dim);
  font-size: 15.5px;
  line-height: 1.7;
  text-wrap: pretty;
}

.final-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  gap: 11px;
  margin-top: 36px;
}

.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  min-height: 50px;
  padding: 0 22px;
  border: 1px solid transparent;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 700;
  text-decoration: none;
  transition: transform 180ms ease, background 180ms ease, border-color 180ms ease, box-shadow 180ms ease;
}

.btn:hover {
  transform: translateY(-2px);
}

.btn[data-variant='primary'] {
  color: white;
  background: var(--br-red);
  box-shadow: 0 10px 34px rgba(225, 29, 72, 0.3);
}

.btn[data-variant='primary']:hover {
  background: #f43f5e;
  box-shadow: 0 14px 44px rgba(225, 29, 72, 0.42);
}

.btn[data-variant='outline'] {
  color: #e8edf5;
  border-color: rgba(255, 255, 255, 0.22);
  background: rgba(255, 255, 255, 0.05);
}

.btn[data-variant='outline']:hover {
  border-color: rgba(255, 255, 255, 0.45);
  background: rgba(255, 255, 255, 0.09);
}

.btn[data-variant='ghost'] {
  color: #b9c3d2;
}

.btn[data-variant='ghost']:hover {
  color: white;
  background: rgba(255, 255, 255, 0.06);
}

.btn-icon {
  width: 17px;
  height: 17px;
  flex: none;
}

@media (prefers-reduced-motion: reduce) {
  .btn:hover {
    transform: none;
  }
}

@media (max-width: 620px) {
  .final {
    padding: 78px 0;
  }

  .final-actions {
    flex-direction: column;
    align-items: stretch;
  }

  .btn {
    width: 100%;
  }
}
</style>
