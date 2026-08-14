<script setup lang="ts">
/**
 * The site's single Ask-AI module. Deliberately not repeated in the hero or the
 * closing CTA -- one console, placed after the simulation and the metrics.
 *
 * The assistant only exists when the site was built with AI gateway
 * credentials; without them `useAssistant().isEnabled` is false, so we fall
 * back to the docs search palette rather than offering a dead button.
 */
const props = defineProps<{
  kicker?: string
  title?: string
  accent?: string
  description?: string
  questions?: string[]
  llms?: string
}>()

const { open, isEnabled } = useAssistant()
const { open: openSearch } = useContentSearch()

const question = ref('')
const copied = ref(false)

const suggestions = computed(() => props.questions ?? [])
const llmsUrl = computed(() => props.llms || 'bloodraven.dev/llms-full.txt')

function ask(text?: string) {
  const value = (text ?? question.value).trim()
  if (!value) return

  if (isEnabled.value) {
    open(value, true)
    question.value = ''
  }
  else {
    openSearch.value = true
  }
}

async function copyLlms() {
  try {
    await navigator.clipboard.writeText(`https://${llmsUrl.value}`)
    copied.value = true
    setTimeout(() => (copied.value = false), 2000)
  }
  catch {
    // Clipboard permission denied -- the URL is visible on screen anyway.
  }
}
</script>

<template>
  <section class="ask">
    <div class="br-shell ask-shell">
      <div class="ask-copy">
        <p v-if="kicker" class="br-kicker">
          {{ kicker }}
        </p>

        <h2 class="br-display ask-title">
          {{ title }}
          <span class="br-outline ask-accent">{{ accent }}</span>
        </h2>

        <p class="ask-description">
          {{ description }}
        </p>

        <button type="button" class="ask-llms br-focus" @click="copyLlms">
          <span class="ask-llms-label">Driving your own agent?</span>
          <code>{{ llmsUrl }}</code>
          <UIcon :name="copied ? 'i-lucide-check' : 'i-lucide-copy'" class="ask-llms-icon" />
        </button>
      </div>

      <div class="console">
        <div class="console-bar">
          <span class="console-id">
            <BrandMark :height="30" surface="dark" class="console-mark" />
            <span>
              <strong>Bloodraven AI</strong>
              <small>Grounded in this documentation set</small>
            </span>
          </span>
          <span class="console-status"><i />ready</span>
        </div>

        <form class="console-form" @submit.prevent="ask()">
          <label for="br-ask-input">Ask anything about Bloodraven</label>
          <div class="console-input">
            <textarea
              id="br-ask-input"
              v-model="question"
              rows="2"
              maxlength="1000"
              placeholder="What happens to in-flight transactions when iad disappears?"
              @keydown.enter.exact.prevent="ask()"
            />
            <button
              type="submit"
              class="console-send br-focus"
              :disabled="!question.trim()"
              aria-label="Ask the assistant"
            >
              Ask
              <b aria-hidden="true">↑</b>
            </button>
          </div>
        </form>

        <div class="console-suggestions">
          <p class="console-suggestions-title">
            Start here
          </p>
          <button
            v-for="item in suggestions"
            :key="item"
            type="button"
            class="suggestion br-focus"
            @click="ask(item)"
          >
            <span>{{ item }}</span>
            <b aria-hidden="true">↗</b>
          </button>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.ask {
  position: relative;
  padding: 108px 0;
  background:
    radial-gradient(circle at 12% 45%, rgba(225, 29, 72, 0.07), transparent 30%),
    var(--br-paper);
  color: var(--br-text);
}

.ask-shell {
  display: grid;
  grid-template-columns: 0.86fr 1.14fr;
  gap: 72px;
  align-items: center;
}

.ask-shell > * {
  min-width: 0;
}

.ask-title {
  margin-top: 20px;
}

.ask-accent {
  --br-stroke: var(--br-red);
}

.ask-description {
  max-width: 460px;
  margin: 26px 0 0;
  color: var(--br-text-dim);
  font-size: 15.5px;
  line-height: 1.75;
  text-wrap: pretty;
}

.ask-llms {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  margin-top: 28px;
  padding: 9px 12px;
  border: 1px solid var(--br-line-light);
  border-radius: 7px;
  color: var(--br-text-dim);
  background: transparent;
  cursor: pointer;
  transition: border-color 160ms ease, color 160ms ease;
}

.ask-llms:hover {
  color: var(--br-text);
  border-color: color-mix(in srgb, var(--br-red) 45%, transparent);
}

.ask-llms-label {
  font-size: 12.5px;
  font-weight: 600;
}

.ask-llms code {
  font-family: var(--br-mono);
  font-size: 11.5px;
  color: var(--br-red);
}

.ask-llms-icon {
  width: 14px;
  height: 14px;
}

/* Console --------------------------------------------------------------- */
.console {
  overflow: hidden;
  border: 1px solid rgba(255, 255, 255, 0.11);
  border-radius: 12px;
  color: #dbe2ec;
  background: var(--br-ink-soft);
  box-shadow: 0 34px 70px rgba(15, 23, 42, 0.22);
}

.console-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 16px 20px;
  border-bottom: 1px solid var(--br-line-dark);
}

.console-id {
  display: flex;
  align-items: center;
  gap: 12px;
}

.console-mark {
  color: #94a3b8;
}

.console-id strong {
  display: block;
  font-size: 12.5px;
}

.console-id small {
  display: block;
  margin-top: 3px;
  color: var(--br-on-ink-dim);
  font-size: 10px;
}

.console-status {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

.console-status i {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--br-green);
  box-shadow: 0 0 10px rgba(52, 211, 153, 0.75);
}

.console-form {
  padding: 22px 20px 18px;
}

.console-form label {
  display: block;
  margin-bottom: 9px;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 9px;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

.console-input {
  position: relative;
}

.console-input textarea {
  display: block;
  width: 100%;
  min-height: 92px;
  resize: none;
  padding: 16px 92px 16px 16px;
  border: 1px solid rgba(255, 255, 255, 0.14);
  border-radius: 8px;
  outline: none;
  color: #f4f7fa;
  background: var(--br-ink);
  font-family: var(--br-mono);
  font-size: 12.5px;
  line-height: 1.55;
  transition: border-color 160ms ease, box-shadow 160ms ease;
}

.console-input textarea::placeholder {
  color: #4e5868;
}

.console-input textarea:focus {
  border-color: rgba(34, 211, 238, 0.55);
  box-shadow: 0 0 0 3px rgba(34, 211, 238, 0.12);
}

.console-send {
  position: absolute;
  right: 10px;
  bottom: 10px;
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 34px;
  padding: 0 14px;
  border: 0;
  border-radius: 6px;
  color: white;
  background: var(--br-red);
  font-size: 12px;
  font-weight: 800;
  cursor: pointer;
  transition: transform 160ms ease, background 160ms ease;
}

.console-send:hover:not(:disabled) {
  transform: translateY(-2px);
  background: #f43f5e;
}

.console-send:disabled {
  opacity: 0.35;
  cursor: not-allowed;
}

.console-send b {
  font-size: 14px;
}

.console-suggestions {
  padding: 16px 20px 20px;
  border-top: 1px solid var(--br-line-dark);
}

.console-suggestions-title {
  margin: 0 0 8px;
  color: var(--br-on-ink-dim);
  font-family: var(--br-mono);
  font-size: 8.5px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.suggestion {
  display: flex;
  width: 100%;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 9px 0;
  border: 0;
  color: #97a2b4;
  background: transparent;
  font-size: 12px;
  text-align: left;
  cursor: pointer;
  transition: color 150ms ease, padding 150ms ease;
}

.suggestion + .suggestion {
  border-top: 1px solid rgba(255, 255, 255, 0.055);
}

.suggestion:hover {
  padding-left: 5px;
  color: white;
}

.suggestion b {
  flex: none;
  color: var(--br-red-bright);
}

@media (prefers-reduced-motion: reduce) {
  .console-send:hover:not(:disabled) {
    transform: none;
  }
  .suggestion:hover {
    padding-left: 0;
  }
}

@media (max-width: 1080px) {
  .ask-shell {
    grid-template-columns: 1fr;
    gap: 44px;
  }

  .ask-copy {
    max-width: 760px;
  }

  .ask-description {
    max-width: 620px;
  }
}

@media (max-width: 620px) {
  .ask {
    padding: 72px 0;
  }

  .ask-llms {
    flex-wrap: wrap;
    gap: 6px 10px;
  }
}
</style>
