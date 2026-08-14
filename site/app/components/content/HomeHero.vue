<script setup lang="ts">
interface HeroLink {
  label: string
  to: string
  target?: string
  icon?: string
  variant?: 'primary' | 'outline' | 'ghost'
}

defineProps<{
  kicker?: string
  title?: string
  accent?: string
  description?: string
  links?: HeroLink[]
}>()
</script>

<template>
  <section class="hero">
    <div class="hero-grid" aria-hidden="true" />
    <div class="hero-glow" aria-hidden="true" />

    <div class="br-shell hero-inner">
      <div class="hero-copy">
        <p v-if="kicker" class="hero-kicker">
          <span class="hero-kicker-dot" data-br-motion />
          {{ kicker }}
        </p>

        <h1 class="hero-title">
          {{ title }}
          <span class="hero-accent">{{ accent }}</span>
        </h1>

        <p class="hero-description">
          {{ description }}
        </p>

        <div class="hero-actions">
          <NuxtLink
            v-for="link in links"
            :key="link.label"
            :to="link.to"
            :target="link.target"
            class="hero-btn br-focus"
            :data-variant="link.variant || 'ghost'"
          >
            <UIcon v-if="link.icon" :name="link.icon" class="hero-btn-icon" />
            {{ link.label }}
          </NuxtLink>
        </div>
      </div>

      <div class="hero-instrument">
        <HomeFailoverStage />
        <p class="hero-instrument-note">
          Scripted replay of the documented promotion sequence. Kill the site and read the clock.
        </p>
      </div>
    </div>
  </section>
</template>

<style scoped>
.hero {
  position: relative;
  isolation: isolate;
  overflow: hidden;
  color: var(--br-on-ink);
  background:
    radial-gradient(circle at 76% 28%, rgba(225, 29, 72, 0.16), transparent 28%),
    linear-gradient(135deg, #07090d 0%, #090c13 52%, #101018 100%);
}

.hero-grid {
  position: absolute;
  inset: 0;
  z-index: -2;
  opacity: 0.25;
  background-image:
    linear-gradient(rgba(255, 255, 255, 0.07) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255, 255, 255, 0.07) 1px, transparent 1px);
  background-size: 64px 64px;
  mask-image: linear-gradient(to bottom, black, transparent 94%);
}

.hero-glow {
  position: absolute;
  z-index: -2;
  width: 620px;
  height: 620px;
  left: -340px;
  top: 4%;
  border-radius: 50%;
  background: rgba(225, 29, 72, 0.1);
  filter: blur(90px);
}

.hero-inner {
  position: relative;
  display: grid;
  grid-template-columns: minmax(0, 1.02fr) minmax(420px, 0.98fr);
  gap: 64px;
  align-items: center;
  padding: 92px 0 96px;
}

.hero-inner > * {
  min-width: 0;
}

.hero-copy {
  max-width: 660px;
}

.hero-kicker {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  margin: 0 0 26px;
  color: #d7dee8;
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.hero-kicker-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--br-red-bright);
  box-shadow: 0 0 0 0 rgba(251, 113, 133, 0.6);
  animation: br-pulse-ring 2.3s infinite;
}

.hero-title {
  margin: 0;
  max-width: 640px;
  /* Sized so the rose payoff line sets whole in the ~570px copy column rather
     than breaking mid-phrase. */
  font-size: clamp(2.6rem, 4.1vw, 3.9rem);
  line-height: 0.98;
  letter-spacing: -0.05em;
  font-weight: 760;
  text-wrap: balance;
}

.hero-accent {
  display: block;
  color: var(--br-red-bright);
}

.hero-description {
  max-width: 590px;
  margin: 30px 0 0;
  color: #aeb7c6;
  font-size: clamp(1.02rem, 1.4vw, 1.16rem);
  line-height: 1.65;
  text-wrap: pretty;
}

.hero-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 11px;
  margin-top: 34px;
}

.hero-btn {
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

.hero-btn:hover {
  transform: translateY(-2px);
}

.hero-btn[data-variant='primary'] {
  color: white;
  background: var(--br-red);
  box-shadow: 0 10px 34px rgba(225, 29, 72, 0.28);
}

.hero-btn[data-variant='primary']:hover {
  background: #f43f5e;
  box-shadow: 0 14px 44px rgba(225, 29, 72, 0.4);
}

.hero-btn[data-variant='outline'] {
  color: #e8edf5;
  border-color: rgba(255, 255, 255, 0.22);
  background: rgba(255, 255, 255, 0.05);
}

.hero-btn[data-variant='outline']:hover {
  border-color: rgba(255, 255, 255, 0.45);
  background: rgba(255, 255, 255, 0.09);
}

.hero-btn[data-variant='ghost'] {
  color: #b9c3d2;
}

.hero-btn[data-variant='ghost']:hover {
  color: white;
  background: rgba(255, 255, 255, 0.06);
}

.hero-btn-icon {
  width: 17px;
  height: 17px;
  flex: none;
}

.hero-instrument-note {
  margin: 14px 2px 0;
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 10px;
  line-height: 1.6;
  letter-spacing: 0.03em;
}

@media (prefers-reduced-motion: reduce) {
  .hero-btn:hover {
    transform: none;
  }
}

@media (max-width: 1080px) {
  .hero-inner {
    grid-template-columns: 1fr;
    gap: 52px;
    padding: 72px 0 68px;
  }

  .hero-copy {
    max-width: 760px;
  }

  .hero-title {
    max-width: 820px;
  }

  .hero-instrument {
    width: min(620px, 100%);
  }
}

@media (max-width: 620px) {
  .hero-inner {
    padding: 56px 0 56px;
    gap: 40px;
  }

  .hero-title {
    font-size: clamp(2.4rem, 11.5vw, 3.6rem);
  }

  .hero-description {
    font-size: 1rem;
  }

  .hero-actions {
    flex-direction: column;
    align-items: stretch;
  }

  .hero-btn {
    width: 100%;
  }
}
</style>
