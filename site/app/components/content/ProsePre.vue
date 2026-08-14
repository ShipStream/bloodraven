<script setup lang="ts">
// Docus/Nuxt UI has no Mermaid renderer - Shiki only highlights the grammar.
// Intercept ```mermaid fences and render them as diagrams; delegate everything
// else to the stock Nuxt UI ProsePre so copy buttons, filenames, and
// highlighting keep working.
import { computed } from 'vue'
import UProsePre from '@nuxt/ui/runtime/components/prose/Pre.vue'

const props = defineProps<{
  icon?: string
  code?: string
  language?: string
  filename?: string
  highlights?: number[]
  hideHeader?: boolean
  meta?: string
  copy?: boolean | object
  class?: unknown
  ui?: Record<string, unknown>
}>()

const isMermaid = computed(() => props.language === 'mermaid')
</script>

<template>
  <MermaidDiagram v-if="isMermaid" :code="props.code || ''" />
  <UProsePre v-else v-bind="props">
    <slot />
  </UProsePre>
</template>
