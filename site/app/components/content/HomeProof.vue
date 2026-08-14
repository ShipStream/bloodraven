<script setup lang="ts">
defineProps<{
  kicker?: string
  title?: string
  titleTwo?: string
  lede?: string
  /** Anchor id, so the header's "Proof" link can jump here. */
  id?: string
  chaosLabel?: string
  chaosTitle?: string
  chaosDescription?: string
  chaosCommands?: string[]
  chaosLayers?: string[]
  chaosTo?: string
  playLabel?: string
  playTitle?: string
  playTitleTwo?: string
  playDescription?: string
  playCommand?: string
  playTargets?: string[]
  playImage?: string
  playImageAlt?: string
  playTo?: string
  titleTwoAccent?: boolean
}>()
</script>

<template>
  <section :id="id" class="proof">
    <div class="br-shell">
      <HomeSectionHead
        :kicker="kicker"
        :title="title"
        :title-two="titleTwo"
        :lede="lede"
        :accent="titleTwoAccent"
      />

      <div class="layout">
        <NuxtLink :to="chaosTo" class="chaos br-focus">
          <!-- Every blip is a real scenario family from playground/chaos-scenarios.md. -->
          <div class="radar" aria-hidden="true">
            <span class="radar-ring radar-ring-1" />
            <span class="radar-ring radar-ring-2" />
            <span class="radar-ring radar-ring-3" />
            <span class="radar-sweep" data-br-motion />
            <i class="blip blip-1" /><i class="blip blip-2" /><i class="blip blip-3" /><i class="blip blip-4" />
          </div>

          <div class="chaos-copy">
            <span class="label">{{ chaosLabel }}</span>
            <h3 class="chaos-title">{{ chaosTitle }}</h3>
            <p class="chaos-description">{{ chaosDescription }}</p>

            <pre class="commands"><code>{{ chaosCommands?.join('\n') }}</code></pre>

            <div class="layers">
              <span v-for="layer in chaosLayers" :key="layer">{{ layer }}</span>
            </div>
          </div>
        </NuxtLink>

        <NuxtLink :to="playTo" class="play br-focus">
          <div class="play-top">
            <span class="label">{{ playLabel }}</span>
            <b aria-hidden="true">↗</b>
          </div>
          <h3 class="play-title">
            {{ playTitle }}
            <span>{{ playTitleTwo }}</span>
          </h3>
          <p class="play-description">{{ playDescription }}</p>

          <code class="terminal">
            <span class="prompt">$</span>{{ playCommand }}<i class="cursor" data-br-motion>▍</i>
          </code>

          <div class="targets">
            <span v-for="target in playTargets" :key="target">{{ target }}</span>
          </div>

          <img
            v-if="playImage"
            :src="playImage"
            :alt="playImageAlt"
            class="play-shot"
            width="1176"
            height="784"
            loading="lazy"
          >
        </NuxtLink>
      </div>
    </div>
  </section>
</template>

<style scoped>
.proof {
  padding: 112px 0;
  border-top: 1px solid var(--br-line-light);
  color: var(--br-text);
  background: var(--br-paper-2);
  /* Keeps the sticky header from covering the heading when jumped to. */
  scroll-margin-top: var(--ui-header-height, 64px);
}

.layout {
  display: grid;
  grid-template-columns: 1.15fr 0.85fr;
  gap: 16px;
  align-items: stretch;
}

/* Pre-formatted make commands must scroll inside the card, not widen the track. */
.layout > *,
.chaos > * {
  min-width: 0;
}

.label {
  color: var(--br-red-bright);
  font-family: var(--br-mono);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

/* Chaos ----------------------------------------------------------------- */
.chaos {
  display: grid;
  grid-template-rows: 1fr auto;
  padding: 38px;
  border: 1px solid var(--br-line-dark);
  border-radius: 8px;
  color: white;
  background: #0c1017;
  text-decoration: none;
  transition: border-color 180ms ease;
}

.chaos:hover {
  border-color: rgba(225, 29, 72, 0.5);
}

.radar {
  position: relative;
  align-self: center;
  justify-self: center;
  overflow: hidden;
  width: 280px;
  height: 280px;
  margin-bottom: 30px;
  border-radius: 50%;
  background: radial-gradient(circle, rgba(225, 29, 72, 0.12), transparent 62%);
}

/* Named `radar-ring`, not `ring`: Tailwind ships a `.ring` utility, and it wins
   the cascade and paints its own light box-shadow over the border. */
.radar-ring {
  position: absolute;
  inset: 50%;
  translate: -50% -50%;
  border: 1px solid rgba(34, 211, 238, 0.22);
  border-radius: 50%;
}

.radar-ring-1 { width: 30%; height: 30%; }
.radar-ring-2 { width: 62%; height: 62%; }
.radar-ring-3 { width: 94%; height: 94%; }

.radar::before,
.radar::after {
  content: '';
  position: absolute;
  background: rgba(34, 211, 238, 0.14);
}

.radar::before { width: 1px; top: 3%; bottom: 3%; left: 50%; }
.radar::after { height: 1px; left: 3%; right: 3%; top: 50%; }

/* Full-bleed disc with the cone centred on the dish, so the beam sweeps from
   the middle out to the rim. Sizing this to a quarter box (as the wedge
   previously was) puts the conic gradient's own centre off the dish centre and
   the beam renders as a detached shard. */
.radar-sweep {
  position: absolute;
  inset: 0;
  border-radius: 50%;
  /* Conic gradients run clockwise from 12 o'clock and `br-sweep` rotates
     clockwise, so the higher-angle edge is the one leading the sweep. The beam
     therefore ramps up to full at 48deg and is cut dead there (two stops at the
     same angle) -- a hard leading edge with the phosphor trail decaying behind
     it, rather than the other way round. */
  background: conic-gradient(
    from 0deg at 50% 50%,
    transparent 0deg,
    rgba(225, 29, 72, 0.07) 18deg,
    rgba(225, 29, 72, 0.18) 36deg,
    rgba(225, 29, 72, 0.46) 48deg,
    transparent 48deg
  );
  animation: br-sweep 5s linear infinite;
}

.blip {
  position: absolute;
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--br-red-bright);
  box-shadow: 0 0 14px var(--br-red);
}

.blip-1 { left: 29%; top: 31%; }
.blip-2 { right: 18%; top: 46%; }
.blip-3 { left: 43%; bottom: 17%; }
.blip-4 { right: 34%; top: 22%; }

.chaos-title {
  margin: 16px 0 0;
  max-width: 640px;
  font-size: clamp(1.6rem, 2.6vw, 2.2rem);
  line-height: 1.08;
  letter-spacing: -0.04em;
}

.chaos-description {
  max-width: 660px;
  margin: 16px 0 0;
  color: var(--br-on-ink-dim);
  font-size: 13px;
  line-height: 1.68;
  text-wrap: pretty;
}

.commands {
  margin: 20px 0 0;
  padding: 14px;
  overflow-x: auto;
  border: 1px solid var(--br-line-dark);
  border-radius: 6px;
  background: rgba(0, 0, 0, 0.34);
  color: #94a3b8;
  font-family: var(--br-mono);
  font-size: 10.5px;
  line-height: 1.85;
  white-space: pre;
}

.layers {
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
  margin-top: 18px;
}

.layers span {
  padding: 7px 9px;
  border: 1px solid rgba(34, 211, 238, 0.2);
  border-radius: 4px;
  color: var(--br-cyan);
  font-family: var(--br-mono);
  font-size: 8.5px;
  font-weight: 800;
  letter-spacing: 0.09em;
}

/* Playground ------------------------------------------------------------ */
.play {
  display: flex;
  flex-direction: column;
  padding: 32px;
  border: 1px solid rgba(34, 211, 238, 0.22);
  border-radius: 8px;
  color: white;
  background: #111923;
  text-decoration: none;
  transition: border-color 180ms ease;
}

.play:hover {
  border-color: rgba(34, 211, 238, 0.55);
}

.play-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.play-top b {
  color: var(--br-red-bright);
  font-size: 17px;
}

.play-title {
  margin: 26px 0 0;
  font-size: 26px;
  line-height: 1.1;
  letter-spacing: -0.04em;
}

.play-title span {
  display: block;
  color: var(--br-cyan);
}

.play-description {
  margin: 13px 0 20px;
  color: var(--br-on-ink-dim);
  font-size: 13px;
  line-height: 1.68;
  text-wrap: pretty;
}

.terminal {
  display: block;
  padding: 13px 15px;
  border: 1px solid var(--br-line-dark);
  border-radius: 5px;
  background: rgba(0, 0, 0, 0.3);
  color: #cbd5e1;
  font-family: var(--br-mono);
  font-size: 11px;
}

.prompt {
  margin-right: 8px;
  color: var(--br-cyan);
}

.cursor {
  color: var(--br-red-bright);
  font-style: normal;
  animation: br-blink 1.1s steps(1) infinite;
}

/* The chips and the shot are pushed to the bottom as a pair, so whatever extra
   height the taller chaos card imposes opens up above them rather than
   stretching the screenshot into a portrait crop that hides the dashboard. */
.targets {
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
  margin-top: auto;
  padding-top: 20px;
}

.targets span {
  padding: 7px 9px;
  border: 1px solid rgba(34, 211, 238, 0.2);
  border-radius: 4px;
  color: var(--br-cyan);
  font-family: var(--br-mono);
  font-size: 8.5px;
  font-weight: 800;
  letter-spacing: 0.09em;
  text-transform: uppercase;
}

.play-shot {
  display: block;
  width: 100%;
  height: auto;
  aspect-ratio: 16 / 9;
  margin-top: 16px;
  object-fit: cover;
  object-position: top left;
  border: 1px solid var(--br-line-dark);
  border-radius: 6px;
}

@media (max-width: 1080px) {
  .layout {
    grid-template-columns: 1fr;
  }

  .chaos {
    grid-template-columns: 0.8fr 1.2fr;
    grid-template-rows: 1fr;
    gap: 32px;
    align-items: center;
  }

  .radar {
    margin-bottom: 0;
  }
}

@media (max-width: 620px) {
  .proof {
    padding: 72px 0;
  }

  .chaos {
    grid-template-columns: 1fr;
    padding: 26px 20px;
  }

  .radar {
    width: 220px;
    height: 220px;
  }

  .play {
    padding: 26px 20px;
  }
}
</style>
