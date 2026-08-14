<script setup lang="ts">
/**
 * Product header, overriding the stock Docus one.
 *
 * Docus' default puts a full-width "Search..." field in the centre, which makes
 * every page read as a docs site. Here the centre is product navigation and
 * search is a compact affordance on the right, next to GitHub and the theme
 * toggle.
 *
 * Nuxt resolves components by name across layers, so this file (same path as
 * `docus/app/components/app/AppHeader.vue`) replaces the layer's version.
 */
const appConfig = useAppConfig()
const { open: searchOpen } = useContentSearch()
const { isEnabled: isAssistantEnabled } = useAssistant()

const links = [
  // Absolute rather than a bare '#features' so it also works from a docs page,
  // where it navigates home first and then jumps.
  { label: 'Features', to: '/#features' },
  { label: 'Proof', to: '/#proof' },
  { label: 'Docs', to: '/docs' },
  { label: 'Playground', to: '/docs/get-started/playground' },
  { label: 'Course', to: '/courses/', target: '_blank' as const },
]

const githubUrl = computed(() => appConfig.github?.url as string | undefined)

const route = useRoute()
const navOpen = ref(false)

/**
 * Same-page hash jumps from the mobile drawer.
 *
 * The drawer locks body scroll while open and restores the previous offset as
 * it closes. That restore lands after the router's hash scroll, leaving the
 * target a couple of hundred pixels off. Closing first and scrolling once the
 * drawer has released the lock is deterministic. Cross-page links (and any
 * link without a hash) fall through to normal navigation.
 */
function onMobileNav(to: string, event: MouseEvent) {
  const [path, hash] = to.split('#')
  if (!hash || (path || '/') !== route.path) return

  event.preventDefault()
  navOpen.value = false

  const target = document.getElementById(hash)
  if (!target) return

  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  // One frame past the drawer's close transition, so the scroll lock is gone.
  setTimeout(() => {
    target.scrollIntoView({ behavior: reduced ? 'auto' : 'smooth', block: 'start' })
    history.replaceState(history.state, '', `#${hash}`)
  }, 320)
}
</script>

<template>
  <UHeader
    v-model:open="navOpen"
    :ui="{
      root: 'br-header',
      center: 'flex-1 justify-start',
      container: 'br-header-container',
    }"
    :to="'/'"
  >
    <nav class="br-nav" aria-label="Primary">
      <NuxtLink
        v-for="link in links"
        :key="link.label"
        :to="link.to"
        :target="link.target"
        class="br-nav-link br-focus"
      >
        {{ link.label }}
      </NuxtLink>
    </nav>

    <template #left>
      <NuxtLink to="/" class="br-brand br-focus" aria-label="Bloodraven home">
        <BrandMark :height="26" />
        <span class="br-wordmark">Bloodraven</span>
      </NuxtLink>
    </template>

    <template #right>
      <button
        type="button"
        class="br-search br-focus"
        aria-label="Search the documentation"
        @click="searchOpen = true"
      >
        <UIcon name="i-lucide-search" class="br-search-icon" />
        <span class="br-search-label">Search</span>
        <ClientOnly>
          <span class="br-search-kbd">
            <UKbd value="meta" size="sm" />
            <UKbd value="K" size="sm" />
          </span>
          <template #fallback>
            <span class="br-search-kbd br-search-kbd-ghost" aria-hidden="true" />
          </template>
        </ClientOnly>
      </button>

      <AssistantChat v-if="isAssistantEnabled" />

      <UButton
        v-if="githubUrl"
        :to="githubUrl"
        target="_blank"
        color="neutral"
        variant="ghost"
        icon="i-simple-icons-github"
        aria-label="Bloodraven on GitHub"
      />

      <ClientOnly>
        <UColorModeButton />
        <!-- Fixed-size, non-animated placeholder: the stock Docus fallback is a
             pulsing block that reads as a broken avatar on first paint. -->
        <template #fallback>
          <span class="br-mode-placeholder" aria-hidden="true" />
        </template>
      </ClientOnly>
    </template>

    <template #toggle="{ open, toggle }">
      <IconMenuToggle
        :open="open"
        class="lg:hidden"
        @click="toggle"
      />
    </template>

    <template #body>
      <nav class="br-nav-mobile" aria-label="Primary">
        <NuxtLink
          v-for="link in links"
          :key="link.label"
          :to="link.to"
          :target="link.target"
          class="br-nav-mobile-link br-focus"
          @click="onMobileNav(link.to, $event)"
        >
          {{ link.label }}
        </NuxtLink>
        <NuxtLink
          v-if="githubUrl"
          :to="githubUrl"
          target="_blank"
          class="br-nav-mobile-link br-focus"
        >
          GitHub
        </NuxtLink>
      </nav>
      <AppHeaderBody />
    </template>
  </UHeader>
</template>

<style scoped>
.br-brand {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  color: var(--br-text);
  text-decoration: none;
}

.br-wordmark {
  font-size: 16px;
  font-weight: 750;
  letter-spacing: -0.025em;
}

.br-nav {
  display: none;
  align-items: center;
  gap: 4px;
  margin-left: 26px;
}

.br-nav-link {
  padding: 6px 11px;
  border-radius: 6px;
  color: var(--br-text-dim);
  font-size: 14px;
  font-weight: 550;
  text-decoration: none;
  transition: color 150ms ease, background 150ms ease;
}

.br-nav-link:hover {
  color: var(--br-text);
  background: color-mix(in srgb, var(--br-text) 7%, transparent);
}

.br-nav-link.router-link-active {
  color: var(--br-text);
}

.br-search {
  display: none;
  align-items: center;
  gap: 8px;
  height: 32px;
  padding: 0 8px 0 10px;
  border: 1px solid var(--br-line-light);
  border-radius: 7px;
  color: var(--br-text-faint);
  background: transparent;
  font-size: 13px;
  cursor: pointer;
  transition: border-color 150ms ease, color 150ms ease;
}

.br-search:hover {
  border-color: color-mix(in srgb, var(--br-red) 45%, transparent);
  color: var(--br-text);
}

.br-search-icon {
  width: 15px;
  height: 15px;
  flex: none;
}

.br-search-label {
  min-width: 46px;
  text-align: left;
}

.br-search-kbd {
  display: inline-flex;
  gap: 3px;
}

/* Reserves the exact width of the two key caps so the header does not shift
   when the client-only kbd hydrates. */
.br-search-kbd-ghost {
  width: 44px;
  height: 18px;
}

.br-mode-placeholder {
  display: inline-block;
  width: 32px;
  height: 32px;
}

@media (min-width: 1024px) {
  .br-nav,
  .br-search {
    display: flex;
  }
}

.br-nav-mobile {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding-bottom: 12px;
  margin-bottom: 12px;
  border-bottom: 1px solid var(--br-line-light);
}

.br-nav-mobile-link {
  padding: 9px 10px;
  border-radius: 6px;
  color: var(--br-text);
  font-size: 15px;
  font-weight: 600;
  text-decoration: none;
}

.br-nav-mobile-link:hover {
  background: color-mix(in srgb, var(--br-text) 7%, transparent);
}
</style>
