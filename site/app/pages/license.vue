<script setup lang="ts">
useSeoMeta({
  title: 'Get a license key',
  description: 'Fetch a Bloodraven license token with your Polar order ID and purchase email.',
})

const orderId = ref('')
const email = ref('')
const pending = ref(false)
const error = ref('')
const result = ref<{
  token: string
  edition: string
  org: string
  updatesUntil: number
  issuedFor: string
} | null>(null)
const copied = ref(false)

const updatesUntilLabel = computed(() => {
  if (!result.value) {
    return ''
  }
  return new Date(result.value.updatesUntil * 1000).toISOString().slice(0, 10)
})

async function submit() {
  error.value = ''
  result.value = null
  copied.value = false
  pending.value = true
  try {
    const response = await $fetch<{
      token: string
      edition: string
      org: string
      updatesUntil: number
      issuedFor: string
      error?: string
    }>('/api/license', {
      method: 'POST',
      body: {
        orderId: orderId.value,
        email: email.value,
      },
    })
    result.value = response
  }
  catch (err: unknown) {
    const fetched = err as { data?: { error?: string }, statusCode?: number }
    error.value = fetched?.data?.error || 'Could not issue a license for that order.'
  }
  finally {
    pending.value = false
  }
}

async function copyToken() {
  if (!result.value?.token) {
    return
  }
  try {
    await navigator.clipboard.writeText(result.value.token)
    copied.value = true
    setTimeout(() => {
      copied.value = false
    }, 2000)
  }
  catch {
    copied.value = false
  }
}
</script>

<template>
  <main class="page">
    <div class="br-shell inner">
      <p class="br-kicker">
        License key
      </p>
      <h1 class="title">
        Fetch your Bloodraven token
      </h1>
      <p class="lede">
        You do not need this key to run Bloodraven. There is no activation, no
        license server, and no phone-home. The software is fully functional
        without it. The token is a signed record of your purchase for your own
        files, expense report, and cluster inventory.
      </p>
      <p class="lede">
        Enter the Polar order ID from your receipt and the email you used at
        checkout. A lost key can be fetched again. If you later buy a renewal
        that Polar attaches to the same order, fetching again picks up the new
        update period.
      </p>

      <form class="form" @submit.prevent="submit">
        <label class="field">
          <span>Polar order ID</span>
          <input
            v-model="orderId"
            class="input br-focus"
            type="text"
            name="orderId"
            autocomplete="off"
            spellcheck="false"
            required
          >
        </label>
        <label class="field">
          <span>Purchase email</span>
          <input
            v-model="email"
            class="input br-focus"
            type="email"
            name="email"
            autocomplete="email"
            required
          >
        </label>
        <button
          class="submit br-focus"
          type="submit"
          :disabled="pending"
        >
          {{ pending ? 'Checking...' : 'Get token' }}
        </button>
      </form>

      <p v-if="error" class="error" role="alert">
        {{ error }}
      </p>

      <section v-if="result" class="result">
        <h2>Token</h2>
        <p class="meta">
          {{ result.org }}
          · {{ result.edition }}
          · updates through {{ updatesUntilLabel }}
          · order {{ result.issuedFor }}
        </p>
        <textarea
          class="token br-focus"
          readonly
          rows="6"
          :value="result.token"
        />
        <button class="copy br-focus" type="button" @click="copyToken">
          {{ copied ? 'Copied' : 'Copy token' }}
        </button>
        <p class="hint">
          Paste it as <code>license</code> on the operator (Organization) or as
          <code>spec.license</code> on one failover group (Production). The
          operator verifies it offline. See
          <NuxtLink to="/docs/licensing">
            Licensing
          </NuxtLink>.
        </p>
      </section>
    </div>
  </main>
</template>

<style scoped>
.page {
  background: var(--br-paper);
  color: var(--br-text);
}

.inner {
  max-width: 640px;
  padding: 72px 0 96px;
}

.title {
  margin: 12px 0 18px;
  font-size: clamp(2rem, 4vw, 2.8rem);
  line-height: 1.05;
  letter-spacing: -0.04em;
  font-weight: 750;
}

.lede {
  margin: 0 0 14px;
  color: var(--br-text-dim);
  font-size: 16px;
  line-height: 1.55;
}

.form {
  display: grid;
  gap: 16px;
  margin: 32px 0 20px;
}

.field {
  display: grid;
  gap: 6px;
  font-size: 13px;
  font-weight: 600;
}

.input,
.token {
  width: 100%;
  border: 1px solid var(--br-line-light);
  border-radius: 8px;
  background: var(--br-paper-2);
  color: var(--br-text);
  font: inherit;
}

.input {
  height: 42px;
  padding: 0 12px;
}

.token {
  padding: 12px;
  font-family: var(--br-mono);
  font-size: 12px;
  line-height: 1.45;
  resize: vertical;
}

.submit,
.copy {
  justify-self: start;
  height: 40px;
  padding: 0 16px;
  border: 0;
  border-radius: 8px;
  background: var(--br-red);
  color: #fff;
  font-size: 14px;
  font-weight: 650;
  cursor: pointer;
}

.submit:disabled {
  opacity: 0.6;
  cursor: wait;
}

.copy {
  margin-top: 10px;
  background: transparent;
  color: var(--br-text);
  border: 1px solid var(--br-line-light);
}

.error {
  color: var(--br-red-deep);
  font-size: 14px;
}

.result {
  margin-top: 28px;
  padding-top: 24px;
  border-top: 1px solid var(--br-line-light);
}

.result h2 {
  margin: 0 0 8px;
  font-size: 18px;
}

.meta,
.hint {
  color: var(--br-text-dim);
  font-size: 14px;
  line-height: 1.5;
}

.hint code {
  font-family: var(--br-mono);
  font-size: 12px;
}
</style>
