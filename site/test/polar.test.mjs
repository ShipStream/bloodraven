import assert from 'node:assert/strict';
import test from 'node:test';
import {
  addCalendarMonthsUtc,
  createPolarClient,
  editionFromProduct,
  emailsMatch,
  getOrder,
  looksLikeOrderId,
  normalizePolarBase,
  orderIsPaid,
  orgFromOrder,
  PolarError,
  updatesUntilUnix,
} from '../server/license/polar.mjs';

test('email match is case-insensitive and timing-safe', () => {
  assert.equal(emailsMatch('Buyer@Example.COM', 'buyer@example.com'), true);
  assert.equal(emailsMatch('buyer@example.com', 'other@example.com'), false);
  assert.equal(emailsMatch('buyer@example.com', ''), false);
});

test('order id must look like a UUID', () => {
  assert.equal(looksLikeOrderId('2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e'), true);
  assert.equal(looksLikeOrderId('ord_example'), false);
  assert.equal(looksLikeOrderId('../subscriptions'), false);
});

test('Polar base is allowlisted', () => {
  assert.equal(normalizePolarBase(''), 'https://api.polar.sh');
  assert.equal(normalizePolarBase('https://sandbox-api.polar.sh/v1'), 'https://sandbox-api.polar.sh');
  assert.throws(() => normalizePolarBase('https://evil.example'), PolarError);
});

test('edition comes only from product metadata', () => {
  assert.equal(editionFromProduct({ metadata: { edition: 'production' } }), 'production');
  assert.equal(editionFromProduct({ metadata: { edition: 'organization' } }), 'organization');
  assert.equal(editionFromProduct({ metadata: { edition: 'community' } }), null);
  assert.equal(editionFromProduct({ metadata: {} }), null);
  assert.equal(editionFromProduct(null), null);
});

test('org falls back to Individual and never uses email', () => {
  assert.equal(orgFromOrder({ customer: { name: 'Acme Corp', email: 'a@b.c' } }), 'Acme Corp');
  assert.equal(orgFromOrder({ billingName: 'Billing Co', customer: { email: 'a@b.c' } }), 'Billing Co');
  assert.equal(orgFromOrder({ billing_name: 'Legacy Co', customer: { email: 'a@b.c' } }), 'Legacy Co');
  assert.equal(orgFromOrder({ customer: { email: 'a@b.c' } }), 'Individual');
});

test('paid requires status paid, paid flag, and no refund', () => {
  assert.equal(orderIsPaid({ paid: true, status: 'paid', refundedAmount: 0 }), true);
  assert.equal(orderIsPaid({ paid: true, status: 'refunded', refunded_amount: 0 }), false);
  assert.equal(orderIsPaid({ paid: true, status: 'paid', refundedAmount: 10 }), false);
  assert.equal(orderIsPaid({ paid: false, status: 'paid' }), false);
});

test('updatesUntil uses subscription period when present, else created_at + 12 months', () => {
  const created = '2025-08-15T00:00:00Z';
  assert.equal(
    updatesUntilUnix({ createdAt: created }),
    Math.floor(addCalendarMonthsUtc(new Date(created), 12).getTime() / 1000),
  );
  assert.equal(
    updatesUntilUnix({
      created_at: created,
      subscription: { status: 'active', currentPeriodEnd: '2027-01-01T00:00:00Z' },
    }),
    Date.parse('2027-01-01T00:00:00Z') / 1000,
  );
  assert.equal(
    updatesUntilUnix({
      createdAt: new Date(created),
      subscription: { status: 'incomplete', current_period_end: '2027-01-01T00:00:00Z' },
    }),
    Math.floor(addCalendarMonthsUtc(new Date(created), 12).getTime() / 1000),
  );
});

test('February 29 plus 12 calendar months lands on February 28', () => {
  const end = addCalendarMonthsUtc(new Date('2024-02-29T12:00:00Z'), 12);
  assert.equal(end.toISOString(), '2025-02-28T12:00:00.000Z');
});

function jsonResponse(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

test('getOrder times out and never includes the token in the thrown error', async () => {
  const fetchImpl = () => new Promise(() => {});
  const client = createPolarClient({
    token: 'polar_oat_secret',
    base: 'https://api.polar.sh',
    timeoutMs: 20,
    fetchImpl,
  });
  await assert.rejects(
    () => getOrder(client, '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e', { timeoutMs: 20 }),
    (error) => {
      assert.equal(error instanceof PolarError, true);
      assert.equal(error.code, 'timeout');
      assert.equal(String(error.message).includes('polar_oat_secret'), false);
      return true;
    },
  );
});

test('getOrder maps Polar statuses through the SDK', async () => {
  const missing = createPolarClient({
    token: 'polar_oat_secret',
    base: 'https://api.polar.sh',
    fetchImpl: async () => jsonResponse(404, { error: 'ResourceNotFound', detail: 'missing' }),
  });
  assert.deepEqual(await getOrder(missing, '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e'), { kind: 'not-found' });

  const unauthorized = createPolarClient({
    token: 'polar_oat_secret',
    base: 'https://api.polar.sh',
    fetchImpl: async () => jsonResponse(401, { error: 'Unauthorized' }),
  });
  await assert.rejects(
    () => getOrder(unauthorized, '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e'),
    (error) => error.code === 'unauthorized',
  );

  const down = createPolarClient({
    token: 'polar_oat_secret',
    base: 'https://api.polar.sh',
    fetchImpl: async () => jsonResponse(503, { error: 'unavailable' }),
  });
  await assert.rejects(
    () => getOrder(down, '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e'),
    (error) => error.code === 'unavailable',
  );
});
