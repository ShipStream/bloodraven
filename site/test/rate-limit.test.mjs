import assert from 'node:assert/strict';
import test from 'node:test';
import { createLimiter } from '../server/license/rate-limit.mjs';

test('allows up to max requests per window then denies', () => {
  let now = 1_000_000;
  const limiter = createLimiter({ max: 3, windowMs: 1000, now: () => now });
  assert.equal(limiter.allow('1.1.1.1').ok, true);
  assert.equal(limiter.allow('1.1.1.1').ok, true);
  assert.equal(limiter.allow('1.1.1.1').ok, true);
  assert.equal(limiter.allow('1.1.1.1').ok, false);
  assert.equal(limiter.allow('2.2.2.2').ok, true);
  now += 1000;
  assert.equal(limiter.allow('1.1.1.1').ok, true);
});

test('evicts when the map is full', () => {
  const limiter = createLimiter({ max: 1, windowMs: 60_000, maxKeys: 2 });
  assert.equal(limiter.allow('a').ok, true);
  assert.equal(limiter.allow('b').ok, true);
  assert.equal(limiter.allow('c').ok, true);
  assert.ok(limiter.size() <= 2);
});
