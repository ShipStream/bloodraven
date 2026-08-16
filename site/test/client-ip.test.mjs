import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import test from 'node:test';
import { trustedClientIp } from '../server/license/client-ip.mjs';
import { BodyLimitError, parseJsonObject, readLimitedRaw } from '../server/license/request-body.mjs';
import { readConfig, signerFromConfig } from '../server/license/config.mjs';

test('prefers X-Real-IP over a spoofed X-Forwarded-For chain', () => {
  assert.equal(
    trustedClientIp({
      'x-real-ip': '203.0.113.9',
      'x-forwarded-for': '1.2.3.4, 203.0.113.9',
    }),
    '203.0.113.9',
  );
});

test('uses the last X-Forwarded-For hop when X-Real-IP is absent', () => {
  assert.equal(
    trustedClientIp({ 'x-forwarded-for': '1.2.3.4, 10.0.0.1, 198.51.100.20' }),
    '198.51.100.20',
  );
  assert.equal(trustedClientIp({}, '127.0.0.1'), '127.0.0.1');
  assert.equal(trustedClientIp({}), 'unknown');
});

test('readLimitedRaw rejects bodies over the cap without buffering the rest', async () => {
  const stream = Readable.from([Buffer.alloc(100, 97), Buffer.alloc(100, 98)]);
  await assert.rejects(() => readLimitedRaw(stream, 50), BodyLimitError);
});

test('parseJsonObject accepts a small object and rejects arrays', () => {
  assert.deepEqual(parseJsonObject('{"orderId":"x"}'), { orderId: 'x' });
  assert.throws(() => parseJsonObject('[]'), /object/);
});

test('custom signing kid requires an explicit public-key pin', () => {
  const seedB64 = Buffer.alloc(32, 7).toString('base64');
  const rejected = signerFromConfig(readConfig({
    LICENSE_SIGNING_SEED_B64: seedB64,
    LICENSE_SIGNING_KID: 'br-2',
  }));
  assert.equal(rejected.ok, false);
  assert.match(rejected.reason, /LICENSE_EXPECTED_PUB_HEX/);
});
