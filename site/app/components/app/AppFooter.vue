<script setup lang="ts">
/**
 * Product footer, overriding the stock Docus one (which is a copyright line and
 * a row of social icons).
 */
const appConfig = useAppConfig()

const githubUrl = computed(() => appConfig.github?.url as string | undefined)

const links = computed(() => [
  { label: 'Docs', to: '/docs' },
  { label: 'Playground', to: '/docs/get-started/playground' },
  { label: 'Course', to: '/courses/', target: '_blank' },
  ...(githubUrl.value ? [{ label: 'GitHub', to: githubUrl.value, target: '_blank' }] : []),
  { label: 'Licensing', to: '/docs/licensing' },
  { label: 'License key', to: '/license' },
  { label: 'llms-full.txt', to: '/llms-full.txt', target: '_blank' },
])

const year = new Date().getFullYear()
</script>

<template>
  <footer class="br-footer">
    <div class="br-shell br-footer-inner">
      <NuxtLink to="/" class="br-footer-brand br-focus" aria-label="Bloodraven home">
        <BrandMark :height="20" />
        <span>Bloodraven</span>
      </NuxtLink>

      <nav class="br-footer-links" aria-label="Footer">
        <NuxtLink
          v-for="link in links"
          :key="link.label"
          :to="link.to"
          :target="link.target"
          class="br-footer-link br-focus"
        >
          {{ link.label }}
        </NuxtLink>
      </nav>

      <p class="br-footer-copy">
        © {{ year }} ShipStream
      </p>
    </div>
  </footer>
</template>

<style scoped>
.br-footer {
  border-top: 1px solid var(--br-line-light);
  background: var(--br-paper);
}

.br-footer-inner {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 18px 28px;
  padding: 30px 0;
}

.br-footer-brand {
  display: inline-flex;
  align-items: center;
  gap: 9px;
  color: var(--br-text);
  font-size: 14px;
  font-weight: 700;
  text-decoration: none;
}

.br-footer-links {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 20px;
}

.br-footer-link {
  color: var(--br-text-dim);
  font-size: 13px;
  text-decoration: none;
  transition: color 150ms ease;
}

.br-footer-link:hover {
  color: var(--br-red);
}

.br-footer-copy {
  margin: 0 0 0 auto;
  color: var(--br-text-faint);
  font-family: var(--br-mono);
  font-size: 11px;
}

@media (max-width: 760px) {
  .br-footer-copy {
    margin-left: 0;
  }
}
</style>
