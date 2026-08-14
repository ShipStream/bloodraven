<script setup lang="ts">
/**
 * The architecture claim, with the manifest that backs it up.
 *
 * The YAML is highlighted here rather than through Shiki so the card can be a
 * designed artifact (filename bar, gutter, tuned palette) instead of a prose
 * code fence dropped into a card.
 */
const props = defineProps<{
  kicker?: string
  title?: string
  titleTwo?: string
  lede?: string
  points?: { title: string, description: string }[]
  filename?: string
  yaml?: string
  linkLabel?: string
  linkTo?: string
}>()

interface Token { text: string, kind: 'key' | 'value' | 'punct' }

const lines = computed<Token[][]>(() => (props.yaml || '').split('\n').map((line) => {
  const match = line.match(/^(\s*-?\s*)([\w.$-]+)(:)(.*)$/)
  if (!match) return [{ text: line, kind: 'punct' as const }]

  const [, indent, key, colon, rest] = match
  return [
    { text: indent || '', kind: 'punct' as const },
    { text: key || '', kind: 'key' as const },
    { text: colon || '', kind: 'punct' as const },
    { text: rest || '', kind: 'value' as const },
  ]
}))
</script>

<template>
  <section class="cp">
    <div class="br-shell cp-shell">
      <div class="cp-copy">
        <p v-if="kicker" class="br-kicker">
          {{ kicker }}
        </p>
        <h2 class="br-display cp-title">
          {{ title }}
          <span>{{ titleTwo }}</span>
        </h2>
        <p class="cp-lede">
          {{ lede }}
        </p>

        <dl class="points">
          <div v-for="point in points" :key="point.title" class="point">
            <dt>{{ point.title }}</dt>
            <dd>{{ point.description }}</dd>
          </div>
        </dl>

        <NuxtLink v-if="linkTo" :to="linkTo" class="cp-link br-focus">
          {{ linkLabel }}
          <b aria-hidden="true">→</b>
        </NuxtLink>
      </div>

      <div class="manifest">
        <div class="manifest-bar">
          <span class="manifest-dots" aria-hidden="true"><i /><i /><i /></span>
          <code>{{ filename }}</code>
        </div>
        <pre class="manifest-body"><code><span
          v-for="(tokens, index) in lines"
          :key="index"
          class="manifest-line"
        ><span class="gutter">{{ String(index + 1).padStart(2, ' ') }}</span><span
          v-for="(token, tokenIndex) in tokens"
          :key="tokenIndex"
          :class="`t-${token.kind}`"
        >{{ token.text }}</span></span></code></pre>
      </div>
    </div>
  </section>
</template>

<style scoped>
.cp {
  padding: 112px 0;
  border-top: 1px solid var(--br-line-light);
  color: var(--br-text);
  background: var(--br-paper);
}

.cp-shell {
  display: grid;
  grid-template-columns: 1.05fr 0.95fr;
  gap: 64px;
  align-items: center;
}

/* Without this the pre-formatted manifest widens its grid track instead of
   scrolling inside the card. */
.cp-shell > * {
  min-width: 0;
}

.cp-title {
  margin-top: 18px;
}

.cp-title span {
  display: block;
  color: var(--br-red);
}

.cp-lede {
  max-width: 520px;
  margin: 24px 0 0;
  color: var(--br-text-dim);
  font-size: 15.5px;
  line-height: 1.75;
  text-wrap: pretty;
}

.points {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 22px 30px;
  margin: 34px 0 0;
}

.point {
  padding-top: 16px;
  border-top: 1px solid var(--br-line-light);
}

.point dt {
  font-size: 13.5px;
  font-weight: 700;
  letter-spacing: -0.01em;
}

.point dd {
  margin: 7px 0 0;
  color: var(--br-text-dim);
  font-size: 12.5px;
  line-height: 1.6;
  text-wrap: pretty;
}

.cp-link {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 44px;
  margin-top: 32px;
  padding: 0 18px;
  border: 1px solid var(--br-line-light);
  border-radius: 6px;
  color: var(--br-text);
  font-size: 13px;
  font-weight: 700;
  text-decoration: none;
  transition: border-color 160ms ease, background 160ms ease;
}

.cp-link:hover {
  border-color: color-mix(in srgb, var(--br-red) 55%, transparent);
  background: color-mix(in srgb, var(--br-red) 7%, transparent);
}

.cp-link b {
  color: var(--br-red);
}

/* Manifest -------------------------------------------------------------- */
.manifest {
  overflow: hidden;
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 10px;
  background: var(--br-ink-soft);
  box-shadow: 0 30px 64px rgba(15, 23, 42, 0.22);
}

.manifest-bar {
  display: flex;
  align-items: center;
  gap: 12px;
  height: 42px;
  padding: 0 16px;
  border-bottom: 1px solid var(--br-line-dark);
}

.manifest-dots {
  display: inline-flex;
  gap: 6px;
}

.manifest-dots i {
  width: 9px;
  height: 9px;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.14);
}

.manifest-dots i:first-child {
  background: rgba(225, 29, 72, 0.6);
}

.manifest-bar code {
  color: var(--br-on-ink-faint);
  font-family: var(--br-mono);
  font-size: 10.5px;
}

.manifest-body {
  margin: 0;
  padding: 18px 18px 20px;
  overflow-x: auto;
  font-family: var(--br-mono);
  font-size: 11.5px;
  line-height: 1.85;
}

.manifest-line {
  display: block;
  white-space: pre;
}

.gutter {
  display: inline-block;
  width: 26px;
  color: #3c4557;
  user-select: none;
}

.t-key { color: var(--br-cyan-soft); }
.t-value { color: #e2e8f0; }
.t-punct { color: #64748b; }

@media (max-width: 1080px) {
  .cp-shell {
    grid-template-columns: 1fr;
    gap: 44px;
  }

  .cp-lede {
    max-width: 640px;
  }
}

@media (max-width: 620px) {
  .cp {
    padding: 72px 0;
  }

  .points {
    grid-template-columns: 1fr;
    gap: 18px;
  }

  .manifest-body {
    font-size: 10.5px;
  }
}
</style>
