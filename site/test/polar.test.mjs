import assert from 'node:assert/strict';
import test from 'node:test';
import {
  addCalendarMonthsUtc,
  editionFromProduct,
  emailsMatch,
  fetchOrder,
  looksLikeOrderId,
  normalizePolarBase,
  orderIsPaid,
  orgFromOrder,
  PolarError,
  readOrderResponse,
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
  assert.equal(orgFromOrder({ billing_name: 'Billing Co', customer: { email: 'a@b.c' } }), 'Billing Co');
  assert.equal(orgFromOrder({ customer: { email: 'a@b.c' } }), 'Individual');
});

test('paid requires status paid, paid flag, and no refund', () => {
  assert.equal(orderIsPaid({ paid: true, status: 'paid', refunded_amount: 0 }), true);
  assert.equal(orderIsPaid({ paid: true, status: 'refunded', refunded_amount: 0 }), false);
  assert.equal(orderIsPaid({ paid: true, status: 'paid', refunded_amount: 10 }), false);
  assert.equal(orderIsPaid({ paid: false, status: 'paid' }), false);
});

test('updatesUntil uses subscription period when present, else created_at + 12 months', () => {
  const created = '2025-08-15T00:00:00Z';
  assert.equal(
    updatesUntilUnix({ created_at: created }),
    Math.floor(addCalendarMonthsUtc(new Date(created), 12).getTime() / 1000),
  );
  assert.equal(
    updatesUntilUnix({
      created_at: created,
      subscription: { status: 'active', current_period_end: '2027-01-01T00:00:00Z' },
    }),
    Date.parse('2027-01-01T00:00:00Z') / 1000,
  );
  assert.equal(
    updatesUntilUnix({
      created_at: created,
      subscription: { status: 'incomplete', current_period_end: '2027-01-01T00:00:00Z' },
    }),
    Math.floor(addCalendarMonthsUtc(new Date(created), 12).getTime() / 1000),
  );
});

test('February 29 plus 12 calendar months lands on February 28', () => {
  const end = addCalendarMonthsUtc(new Date('2024-02-29T12:00:00Z'), 12);
  assert.equal(end.toISOString(), '2025-02-28T12:00:00.000Z');
});

test('fetchOrder times out and never includes the token in the thrown error', async () => {
  const fetchImpl = (_url, init) => new Promise((_resolve, reject) => {
    const signal = init?.signal;
    const fail = () => {
      const error = new Error('aborted');
      error.name = 'AbortError';
      reject(error);
    };
    if (signal?.aborted) {
      fail();
      return;
    }
    signal?.addEventListener('abort', fail, { once: true });
  });
  await assert.rejects(
    () => fetchOrder({
      base: 'https://api.polar.sh',
      token: 'polar_oat_secret',
      orderId: '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e',
      timeoutMs: 20,
      fetchImpl,
    }),
    (error) => {
      assert.equal(error instanceof PolarError, true);
      assert.equal(error.code, 'timeout');
      assert.equal(String(error.message).includes('polar_oat_secret'), false);
      return true;
    },
  );
});

test('successful Polar response is returned and can still be read', async () => {
  const fetchImpl = async () => ({
    status: 200,
    json: async () => ({ id: '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e', paid: true }),
  });
  const response = await fetchOrder({
    base: 'https://api.polar.sh',
    token: 'polar_oat_secret',
    orderId: '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e',
    timeoutMs: 200,
    fetchImpl,
  });
  const body = await response.json();
  assert.equal(body.id, '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e');
});

test('readOrderResponse maps Polar statuses', async () => {
  const json = (status, body) => ({
    status,
    json: async () => body,
  });
  assert.deepEqual(await readOrderResponse(json(404, { error: 'ResourceNotFound' })), { kind: 'not-found' });
  await assert.rejects(() => readOrderResponse(json(401, {})), (error) => error.code === 'unauthorized');
  await assert.rejects(() => readOrderResponse(json(503, {})), (error) => error.code === 'unavailable');
  const order = await readOrderResponse(json(200, { id: 'ord_1' }));
  assert.equal(order.kind, 'ok');
  assert.equal(order.order.id, 'ord_1');
});
