<script setup lang="ts">
/**
 * The one place the landing page states the commercial position. It carries no
 * numbers on purpose — `/pricing` is the canonical price table, and a marketing
 * band that disagrees with it is worse than no band at all.
 */
interface PricingLink {
  label: string
  to: string
  target?: string
  variant?: 'primary' | 'outline'
}

defineProps<{
  kicker?: string
  title?: string
  titleTwo?: string
  lede?: string
  points?: string[]
  links?: PricingLink[]
}>()
</script>

<template>
  <section class="pricing">
    <div class="br-shell pricing-inner">
      <div class="pricing-copy">
        <p v-if="kicker" class="pricing-kicker">
          {{ kicker }}
        </p>
        <h2 class="pricing-title">
          {{ title }}
          <span class="pricing-title-two">{{ titleTwo }}</span>
        </h2>
        <p v-if="lede" class="pricing-lede">
          {{ lede }}
        </p>
        <div class="pricing-actions">
          <NuxtLink
            v-for="link in links"
            :key="link.label"
            :to="link.to"
            :target="link.target"
            class="pricing-btn br-focus"
            :data-variant="link.variant || 'outline'"
          >
            {{ link.label }}
          </NuxtLink>
        </div>
      </div>

      <ul v-if="points?.length" class="pricing-points">
        <li v-for="point in points" :key="point">
          {{ point }}
        </li>
      </ul>
    </div>
  </section>
</template>

<style scoped>
.pricing {
  padding: 92px 0;
  border-top: 1px solid var(--br-line-dark-soft);
  color: var(--br-on-ink);
  background: var(--br-ink-soft);
}

.pricing-inner {
  display: grid;
  grid-template-columns: 1fr;
  gap: 34px;
  align-items: start;
}

.pricing-kicker {
  margin: 0 0 18px;
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.pricing-title {
  margin: 0;
  font-size: clamp(2rem, 3.6vw, 3rem);
  line-height: 1.02;
  letter-spacing: -0.045em;
  font-weight: 760;
  color: white;
  text-wrap: balance;
}

.pricing-title-two {
  display: block;
  color: var(--br-on-ink-dim);
}

.pricing-lede {
  max-width: 560px;
  margin: 22px 0 0;
  color: var(--br-on-ink-dim);
  font-size: 15.5px;
  line-height: 1.7;
  text-wrap: pretty;
}

.pricing-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 28px;
}

.pricing-btn {
  display: inline-flex;
  align-items: center;
  min-height: 42px;
  padding: 0 18px;
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  color: var(--br-on-ink);
  font-size: 14.5px;
  font-weight: 650;
  text-decoration: none;
  transition: background 150ms ease, border-color 150ms ease;
}

.pricing-btn[data-variant='primary'] {
  border-color: transparent;
  background: var(--br-red);
  color: #fff;
}

.pricing-btn[data-variant='primary']:hover {
  background: var(--br-red-bright);
}

.pricing-btn[data-variant='outline']:hover {
  border-color: color-mix(in srgb, var(--br-red-bright) 55%, transparent);
  background: rgba(255, 255, 255, 0.05);
}

.pricing-points {
  margin: 0;
  padding: 0;
  list-style: none;
  display: grid;
  gap: 10px;
}

.pricing-points li {
  position: relative;
  padding: 14px 16px 14px 34px;
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  background: var(--br-ink-card);
  color: var(--br-on-ink-dim);
  font-size: 14px;
  line-height: 1.5;
}

.pricing-points li::before {
  content: '';
  position: absolute;
  left: 16px;
  top: 21px;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--br-cyan);
}

@media (min-width: 900px) {
  .pricing-inner {
    grid-template-columns: 1.05fr 0.95fr;
    gap: 56px;
  }
}
</style>
