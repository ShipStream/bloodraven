<script lang="ts">
// Module scope, deliberately shared across every instance on the page.
//
// mermaid.render() builds a temporary DOM node keyed by the id you pass it. Two
// renders that share an id, or that overlap in time, clobber each other's temp
// node and resolve with empty markup - which is how a page with nine diagrams
// ended up showing two. `seq` keeps ids globally unique and `queue` serialises
// the renders so they can never overlap.
let seq = 0
let queue: Promise<unknown> = Promise.resolve()
</script>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'

const props = defineProps<{ code: string }>()

const svg = ref('')
const error = ref('')
const colorMode = useColorMode()

function render() {
  queue = queue.then(async () => {
    try {
      const mermaid = (await import('mermaid')).default
      mermaid.initialize({
        startOnLoad: false,
        securityLevel: 'strict',
        theme: colorMode.value === 'dark' ? 'dark' : 'default',
        fontFamily: 'inherit',
      })
      const { svg: out } = await mermaid.render(`mermaid-${seq++}`, props.code)
      svg.value = out
      error.value = ''
    } catch (e) {
      // A malformed diagram should not blank the page it lives on.
      error.value = e instanceof Error ? e.message : String(e)
      svg.value = ''
    }
  })
  return queue
}

onMounted(render)
watch(() => [props.code, colorMode.value], render)
</script>

<template>
  <div class="my-5">
    <div v-if="svg" class="flex justify-center overflow-x-auto" v-html="svg" />
    <pre v-else-if="error" class="text-error text-xs whitespace-pre-wrap">{{ error }}</pre>
    <pre v-else class="text-muted text-xs">Rendering diagram…</pre>
  </div>
</template>
