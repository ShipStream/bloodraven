<script setup lang="ts">
/**
 * The commercial surface. This page owns every Bloodraven price.
 *
 * `/docs/licensing` explains why the licence is source-available and what a
 * purchase does and does not include; it links here for numbers rather than
 * restating them, so there is exactly one place a buyer can read a price.
 *
 * The checkout URLs are Polar checkout links. Each one redirects to /license
 * after payment, where the buyer exchanges the order ID for a signed token.
 * Priority Triage deliberately has no link: it is a support agreement, it
 * carries no `edition` metadata in Polar, and the license endpoint refuses to
 * mint a token for it.
 */
useSeoMeta({
  title: 'Pricing',
  description:
    'Bloodraven is free for individuals, non-profits, companies under $1M revenue, and all '
    + 'non-production use. Production at a larger company is a one-time license: $990 per '
    + 'failover group, or $4,900 for the whole organization.',
})

const eligibility = [
  { who: 'An individual, for personal or non-commercial use', price: 'Free' },
  { who: 'A student, researcher, or educator, non-commercially', price: 'Free' },
  { who: 'A non-profit or charity', price: 'Free' },
  { who: 'A company under $1M annual revenue', price: 'Free' },
  { who: 'Anyone, for dev / test / staging / CI / evaluation, at any scale', price: 'Free' },
  { who: 'A company over $1M annual revenue, running production', price: 'Licensed', paid: true },
]

const editions = [
  {
    name: 'Community',
    price: 'Free',
    unit: 'no purchase, no signup',
    summary: 'The Business Source License grant. Everything in the table above.',
    points: [
      'Every non-production cluster, at any scale, forever',
      'Production use if you are an individual, a non-profit, or under $1M revenue',
      'No key, no expiry, no reduced feature set',
    ],
    action: { label: 'Install it', to: '/docs/get-started/getting-started', internal: true },
  },
  {
    name: 'Production',
    price: '$990',
    unit: 'one-time, per failover group',
    summary: 'Production use of one failover group, perpetual, plus 12 months of updates.',
    points: [
      'One failover group, however many sites and replicas it holds',
      '12 months of updates, then optional renewal',
      'Perpetual — the license does not expire',
    ],
    action: {
      label: 'Buy Production',
      to: 'https://buy.polar.sh/polar_cl_RVWZ66OGYpdPvmMAXuVkjV7YMoGk9XmbKPKxg3hMbvt',
    },
    featured: true,
  },
  {
    name: 'Organization',
    price: '$4,900',
    unit: 'one-time, whole company',
    summary: 'Unlimited failover groups across your company, perpetual, plus 12 months of updates.',
    points: [
      'Unlimited failover groups in one legal entity and its affiliates',
      'No counting, no true-ups',
      'Usually the cheaper choice at five or more groups',
    ],
    action: {
      label: 'Buy Organization',
      to: 'https://buy.polar.sh/polar_cl_kwX26ctNzaj9KfRHcOK5sIjdar7sLhm4ZueJY34DPkK',
    },
  },
]

const renewals = [
  {
    name: 'Production, per failover group',
    price: '$390',
    unit: 'per year',
    action: {
      label: 'Renew Production',
      to: 'https://buy.polar.sh/polar_cl_G0nZJkVrEzAcbAqvuxAbi4evtOSxtoXdB6RWs1rcbQa',
    },
  },
  {
    name: 'Organization',
    price: '$1,900',
    unit: 'per year',
    action: {
      label: 'Renew Organization',
      to: 'https://buy.polar.sh/polar_cl_BNJ8rC3RoPU8FQCfdqFMpMtoSbXoiUYxHogQ62kzZfM',
    },
  },
]
</script>

<template>
  <main class="page">
    <section class="br-shell head">
      <p class="br-kicker">
        Pricing
      </p>
      <h1 class="title">
        What Bloodraven costs
      </h1>
      <p class="lede">
        Free forever for individuals, non-commercial use, non-profits, companies
        under $1M annual revenue, and every dev, test, staging, CI and evaluation
        cluster at any scale. Production use at a company over $1M annual revenue
        is a one-time license.
      </p>
      <p class="note">
        There is no activation, no license server, no feature gating, and no
        phone-home. <strong>The software is fully functional without a key</strong>,
        on every tier. What you buy is the right to run it in production and a
        signed receipt to file — not an unlock code.
      </p>
    </section>

    <section id="eligibility" class="br-shell block">
      <h2 class="h2">
        Do I need to pay?
      </h2>
      <ul class="eligibility">
        <li v-for="row in eligibility" :key="row.who" class="elig-row">
          <span class="elig-who">{{ row.who }}</span>
          <span class="elig-price" :data-paid="row.paid ? 'true' : 'false'">{{ row.price }}</span>
        </li>
      </ul>
      <p class="aside">
        If you are not sure, email
        <a class="link br-focus" href="mailto:licensing@shipstream.io">licensing@shipstream.io</a>
        and ask. The answer is free and we will put it in writing.
      </p>
    </section>

    <section id="editions" class="br-shell block">
      <h2 class="h2">
        Editions
      </h2>
      <p class="aside aside-top">
        One-time charges. No subscription required. Every purchase includes 12
        months of updates.
      </p>

      <div class="editions">
        <article
          v-for="edition in editions"
          :key="edition.name"
          class="edition"
          :data-featured="edition.featured ? 'true' : 'false'"
        >
          <h3 class="edition-name">
            {{ edition.name }}
          </h3>
          <p class="edition-price">
            {{ edition.price }}
          </p>
          <p class="edition-unit">
            {{ edition.unit }}
          </p>
          <p class="edition-summary">
            {{ edition.summary }}
          </p>
          <ul class="edition-points">
            <li v-for="point in edition.points" :key="point">
              {{ point }}
            </li>
          </ul>
          <NuxtLink
            v-if="edition.action.internal"
            :to="edition.action.to"
            class="buy buy-quiet br-focus"
          >
            {{ edition.action.label }}
            <b aria-hidden="true">→</b>
          </NuxtLink>
          <a
            v-else
            :href="edition.action.to"
            rel="noopener"
            class="buy br-focus"
          >
            {{ edition.action.label }}
            <span class="buy-price">{{ edition.price }}</span>
          </a>
        </article>
      </div>

      <p class="aside">
        A failover group is one <code>MysqlFailoverGroup</code> resource, however
        many sites, replicas, or read-only instances it contains. Two sites is
        still one failover group.
      </p>
    </section>

    <section id="renewals" class="br-shell block">
      <h2 class="h2">
        Renewing updates
      </h2>
      <p class="aside aside-top">
        Optional. <strong>Your license never expires.</strong> If you stop
        renewing you keep the perpetual right to run every version published
        while your update period was active, and you stop receiving new ones
        until you renew. Nothing in the cluster changes when an update period
        ends — the operator logs it and carries on.
      </p>

      <ul class="renewals">
        <li v-for="renewal in renewals" :key="renewal.name" class="renewal">
          <span class="renewal-name">{{ renewal.name }}</span>
          <span class="renewal-price">
            {{ renewal.price }}
            <span class="renewal-unit">{{ renewal.unit }}</span>
          </span>
          <a
            :href="renewal.action.to"
            rel="noopener"
            class="buy buy-small br-focus"
          >{{ renewal.action.label }}</a>
        </li>
      </ul>
    </section>

    <section id="priority-triage" class="br-shell block">
      <h2 class="h2">
        Priority Triage
      </h2>
      <div class="addon">
        <p class="addon-price">
          $6,000 <span class="addon-unit">per year</span>
        </p>
        <p class="addon-body">
          A named email channel, best-effort response during US business hours,
          priority on issue triage, and direct input on the roadmap. No SLA, no
          uptime guarantee, no on-call, and no emergency incident response.
        </p>
        <p class="addon-body">
          There is no checkout link for this one. It is a support agreement
          rather than a license, so it does not mint a license token, and we
          would rather agree the scope with you in writing before you pay for
          it. Email
          <a class="link br-focus" href="mailto:licensing@shipstream.io">licensing@shipstream.io</a>.
        </p>
      </div>
    </section>

    <section id="how-buying-works" class="br-shell block">
      <h2 class="h2">
        How buying works
      </h2>
      <div class="prose">
        <p>
          Licenses are sold through
          <a class="link br-focus" href="https://polar.sh" rel="noopener">Polar</a>,
          which is the merchant of record. Polar handles VAT and sales tax
          worldwide and issues a proper invoice for expense reports and
          procurement.
        </p>
        <p>
          Checkout returns you to
          <NuxtLink class="link br-focus" to="/license">
            /license
          </NuxtLink>. Paste the Polar order ID from your receipt and the email
          you used, and you get a signed token. You can fetch it again if you
          lose it. You do not need it to run Bloodraven — it exists so you have
          an artifact for procurement records, an expense report, and your
          internal software inventory. Compliance is on the honor system, and
          your receipt is the license.
        </p>
        <p>
          <strong>30-day refund, no questions asked.</strong> Email
          <a class="link br-focus" href="mailto:licensing@shipstream.io">licensing@shipstream.io</a>
          with the receipt.
        </p>
        <p>
          Volume pricing, multi-year terms, purchase-order billing, a signed
          order form, a W-9, or a completed security questionnaire: email
          <a class="link br-focus" href="mailto:licensing@shipstream.io">licensing@shipstream.io</a>
          before buying.
        </p>
      </div>
    </section>

    <section class="br-shell block block-last">
      <div class="outbound">
        <NuxtLink class="out br-focus" to="/docs/licensing">
          <span class="out-label">Licensing</span>
          <span class="out-text">Why source-available, what a purchase does not include, and the two-year Apache 2.0 Change Date</span>
        </NuxtLink>
        <a
          class="out br-focus"
          href="https://github.com/ShipStream/bloodraven/blob/main/LICENSE-COMMERCIAL.md"
          rel="noopener"
        >
          <span class="out-label">Commercial terms</span>
          <span class="out-text">The full agreement you are buying, in plain language</span>
        </a>
      </div>
    </section>
  </main>
</template>

<style scoped>
.page {
  background: var(--br-paper);
  color: var(--br-text);
  padding-bottom: 96px;
}

/* `br-shell` centres itself, so a max-width on the section would indent the
   whole block relative to every section below it. Constrain the text instead. */
.head {
  padding: 72px 0 8px;
}

.head .title,
.head .lede,
.head .note {
  max-width: 720px;
}

.title {
  margin: 12px 0 18px;
  font-size: clamp(2rem, 4vw, 2.8rem);
  line-height: 1.05;
  letter-spacing: -0.04em;
  font-weight: 750;
}

.lede,
.note {
  margin: 0 0 14px;
  color: var(--br-text-dim);
  font-size: 16px;
  line-height: 1.55;
}

.note strong {
  color: var(--br-text);
}

.block {
  padding-top: 48px;
}

.block-last {
  padding-top: 56px;
}

.h2 {
  margin: 0 0 18px;
  font-size: 22px;
  font-weight: 700;
  letter-spacing: -0.02em;
}

.aside {
  margin: 18px 0 0;
  max-width: 720px;
  color: var(--br-text-dim);
  font-size: 14px;
  line-height: 1.6;
}

.aside-top {
  margin: -6px 0 22px;
}

.aside strong {
  color: var(--br-text);
}

.aside code {
  font-family: var(--br-mono);
  font-size: 12.5px;
}

.link {
  color: var(--br-red);
  text-decoration: none;
  font-weight: 600;
}

.link:hover {
  text-decoration: underline;
}

/* Eligibility ------------------------------------------------------------ */

.eligibility {
  margin: 0;
  padding: 0;
  list-style: none;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  overflow: hidden;
}

.elig-row {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  justify-content: space-between;
  gap: 4px 18px;
  padding: 13px 16px;
  font-size: 15px;
  line-height: 1.45;
}

.elig-row + .elig-row {
  border-top: 1px solid var(--br-line-light);
}

.elig-price {
  flex: none;
  font-family: var(--br-mono);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--br-green);
}

.elig-price[data-paid='true'] {
  color: var(--br-red);
}

/* Editions --------------------------------------------------------------- */

.editions {
  display: grid;
  grid-template-columns: 1fr;
  gap: 14px;
}

.edition {
  display: flex;
  flex-direction: column;
  padding: 22px;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  background: var(--br-paper-2);
}

.edition[data-featured='true'] {
  border-color: color-mix(in srgb, var(--br-red) 55%, transparent);
}

.edition-name {
  margin: 0;
  font-family: var(--br-mono);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--br-red);
}

.edition-price {
  margin: 14px 0 2px;
  font-size: 34px;
  font-weight: 760;
  letter-spacing: -0.04em;
  line-height: 1;
}

.edition-unit {
  margin: 0;
  color: var(--br-text-faint);
  font-family: var(--br-mono);
  font-size: 11.5px;
}

.edition-summary {
  margin: 16px 0 0;
  color: var(--br-text-dim);
  font-size: 14.5px;
  line-height: 1.55;
}

.edition-points {
  margin: 14px 0 22px;
  padding: 0;
  list-style: none;
  display: grid;
  gap: 8px;
}

.edition-points li {
  position: relative;
  padding-left: 18px;
  color: var(--br-text-dim);
  font-size: 13.5px;
  line-height: 1.5;
}

.edition-points li::before {
  content: '';
  position: absolute;
  left: 0;
  top: 8px;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--br-red);
}

/* Buttons ---------------------------------------------------------------- */

.buy {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  margin-top: auto;
  min-height: 42px;
  padding: 0 16px;
  border: 1px solid transparent;
  border-radius: 8px;
  background: var(--br-red);
  color: #fff;
  font-size: 14.5px;
  font-weight: 650;
  text-decoration: none;
  transition: background 150ms ease, border-color 150ms ease;
}

.buy:hover {
  background: var(--br-red-deep);
}

.buy-price {
  font-family: var(--br-mono);
  font-size: 12.5px;
  opacity: 0.85;
}

.buy-quiet {
  background: transparent;
  border-color: var(--br-line-light);
  color: var(--br-text);
}

.buy-quiet:hover {
  background: color-mix(in srgb, var(--br-text) 7%, transparent);
}

.buy-small {
  min-height: 38px;
  margin-top: 0;
  font-size: 13.5px;
}

/* Renewals --------------------------------------------------------------- */

.renewals {
  margin: 0;
  padding: 0;
  list-style: none;
  display: grid;
  gap: 12px;
}

.renewal {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px 18px;
  padding: 16px;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  background: var(--br-paper-2);
}

.renewal-name {
  font-size: 15px;
  font-weight: 600;
}

.renewal-price {
  margin-left: auto;
  font-size: 20px;
  font-weight: 720;
  letter-spacing: -0.02em;
}

.renewal-unit {
  margin-left: 6px;
  color: var(--br-text-faint);
  font-family: var(--br-mono);
  font-size: 11.5px;
  font-weight: 500;
  letter-spacing: 0;
}

/* Add-on ----------------------------------------------------------------- */

.addon {
  max-width: 720px;
  padding: 22px;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  background: var(--br-paper-2);
}

.addon-price {
  margin: 0 0 12px;
  font-size: 28px;
  font-weight: 750;
  letter-spacing: -0.03em;
}

.addon-unit {
  margin-left: 6px;
  color: var(--br-text-faint);
  font-family: var(--br-mono);
  font-size: 11.5px;
  font-weight: 500;
  letter-spacing: 0;
}

.addon-body {
  margin: 0 0 12px;
  color: var(--br-text-dim);
  font-size: 14.5px;
  line-height: 1.6;
}

.addon-body:last-child {
  margin-bottom: 0;
}

/* Prose ------------------------------------------------------------------ */

.prose {
  max-width: 720px;
}

.prose p {
  margin: 0 0 14px;
  color: var(--br-text-dim);
  font-size: 15px;
  line-height: 1.65;
}

.prose p:last-child {
  margin-bottom: 0;
}

.prose strong {
  color: var(--br-text);
}

/* Outbound --------------------------------------------------------------- */

.outbound {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
  padding-top: 26px;
  border-top: 1px solid var(--br-line-light);
}

.out {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 18px;
  border: 1px solid var(--br-line-light);
  border-radius: 10px;
  text-decoration: none;
  transition: border-color 150ms ease, background 150ms ease;
}

.out:hover {
  border-color: color-mix(in srgb, var(--br-red) 45%, transparent);
  background: var(--br-paper-2);
}

.out-label {
  color: var(--br-red);
  font-family: var(--br-mono);
  font-size: 10.5px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.out-text {
  color: var(--br-text-dim);
  font-size: 14px;
  line-height: 1.5;
}

/* Narrow widths: stack each eligibility and renewal row rather than letting
   flex wrap decide per row, which leaves the short rows right-aligned and the
   long ones stacked. */
@media (max-width: 600px) {
  .elig-row,
  .renewal {
    flex-direction: column;
    align-items: stretch;
    gap: 8px;
  }

  .renewal-price {
    margin-left: 0;
  }

  .buy-small {
    justify-content: center;
  }
}

@media (min-width: 760px) {
  .outbound {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (min-width: 900px) {
  .editions {
    grid-template-columns: repeat(3, 1fr);
  }
}
</style>
