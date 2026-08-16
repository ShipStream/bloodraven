import assert from 'node:assert/strict';
import test from 'node:test';
import { Webhook } from 'standardwebhooks';
import {
  identifiersFromRaw,
  receivePolarWebhook,
  webhookIdentifiers,
} from '../server/license/webhook.mjs';

const SECRET = 'test-polar-webhook-secret';

function sign(payload, { secret = SECRET, id = 'msg_test_1', timestamp = new Date() } = {}) {
  const encoded = Buffer.from(secret, 'utf-8').toString('base64');
  const webhook = new Webhook(encoded);
  const body = typeof payload === 'string' ? payload : JSON.stringify(payload);
  return {
    body,
    headers: {
      'webhook-id': id,
      'webhook-timestamp': String(Math.floor(timestamp.getTime() / 1000)),
      'webhook-signature': webhook.sign(id, timestamp, body),
    },
  };
}

function captureLog() {
  const lines = [];
  return {
    lines,
    log(msg, fields = {}) {
      lines.push({ msg, ...fields });
    },
    joined() {
      return JSON.stringify(lines);
    },
  };
}

test('invalid signature is 401 and does not log the body', () => {
  const logs = captureLog();
  const body = JSON.stringify({
    type: 'order.refunded',
    data: { id: 'ord_1', customer: { email: 'buyer@example.com' } },
  });
  const result = receivePolarWebhook({
    rawBody: body,
    headers: {
      'webhook-id': 'msg_1',
      'webhook-timestamp': String(Math.floor(Date.now() / 1000)),
      'webhook-signature': 'v1,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
    },
    secret: SECRET,
    log: logs.log,
  });
  assert.equal(result.status, 401);
  assert.equal(result.body.error, 'Invalid signature.');
  assert.equal(logs.lines.length, 1);
  assert.equal(logs.lines[0].msg, 'polar webhook signature rejected');
  assert.equal(logs.joined().includes('buyer@example.com'), false);
  assert.equal(logs.joined().includes(body), false);
});

test('missing secret is 503 and does not log the body', () => {
  const logs = captureLog();
  const body = '{"type":"order.created","data":{"customer":{"email":"buyer@example.com"}}}';
  const result = receivePolarWebhook({
    rawBody: body,
    headers: {},
    secret: '',
    log: logs.log,
  });
  assert.equal(result.status, 503);
  assert.equal(logs.joined().includes('buyer@example.com'), false);
});

test('valid signature returns 202 and logs identifiers only', () => {
  const logs = captureLog();
  const payload = {
    type: 'order.paid',
    data: {
      id: '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e',
      product_id: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
      subscription_id: '11111111-2222-4333-8444-555555555555',
      customer: { email: 'buyer@example.com', name: 'Acme', billing_address: { city: 'Austin' } },
    },
  };
  const signed = sign(payload);
  const result = receivePolarWebhook({
    rawBody: signed.body,
    headers: signed.headers,
    secret: SECRET,
    log: logs.log,
  });
  assert.equal(result.status, 202);
  assert.equal(logs.lines[0].msg, 'polar webhook received');
  assert.equal(logs.lines[0].type, 'order.paid');
  assert.equal(logs.lines[0].orderId, payload.data.id);
  assert.equal(logs.lines[0].productId, payload.data.product_id);
  assert.equal(logs.lines[0].subscriptionId, payload.data.subscription_id);
  assert.equal(Object.hasOwn(logs.lines[0], 'email'), false);
  assert.equal(logs.joined().includes('buyer@example.com'), false);
  assert.equal(logs.joined().includes('Austin'), false);
});

test('order.refunded and subscription.revoked are logged as notable', () => {
  for (const type of ['order.refunded', 'subscription.revoked']) {
    const logs = captureLog();
    const signed = sign({
      type,
      data: {
        id: '2c1d0a6a-7c3e-4f1b-9a2d-8e5f0b1c2d3e',
        product_id: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',
        customer: { email: 'buyer@example.com' },
      },
    });
    const result = receivePolarWebhook({
      rawBody: signed.body,
      headers: signed.headers,
      secret: SECRET,
      log: logs.log,
    });
    assert.equal(result.status, 202);
    assert.equal(logs.lines[0].msg, 'polar webhook notable');
    assert.equal(logs.lines[0].type, type);
    assert.equal(logs.joined().includes('buyer@example.com'), false);
  }
});

test('identifier helper never copies email or address fields', () => {
  const ids = webhookIdentifiers({
    type: 'order.refunded',
    data: {
      id: 'ord_1',
      productId: 'prod_1',
      email: 'buyer@example.com',
      customer: { email: 'buyer@example.com' },
      billing_address: { line1: '1 Main' },
    },
  });
  assert.deepEqual(ids, { type: 'order.refunded', orderId: 'ord_1', productId: 'prod_1' });
  assert.equal(JSON.stringify(ids).includes('buyer@example.com'), false);
  assert.equal(identifiersFromRaw('not-json').type, 'unknown');
});
