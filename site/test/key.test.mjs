import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import {
  BR1_PUBLIC_KEY_HEX,
  decodeSeedB64,
  loadSigningKey,
  privateKeyFromSeed,
  publicKeyHex,
} from '../server/license/key.mjs';

// RFC 8032 §7.1 test vector 1.
const RFC_SEED_HEX = '9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60';
const RFC_PUB_HEX = 'd75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a';

test('RFC 8032 seed derives the documented public key', () => {
  const seed = Buffer.from(RFC_SEED_HEX, 'hex');
  const key = privateKeyFromSeed(seed);
  assert.equal(publicKeyHex(key), RFC_PUB_HEX);
});

test('canonical standard base64 seed decodes to 32 bytes', () => {
  const seed = Buffer.from(RFC_SEED_HEX, 'hex');
  const b64 = seed.toString('base64');
  assert.deepEqual(decodeSeedB64(b64), seed);
});

test('rejects padded-looking but non-canonical or wrong-size seeds', () => {
  assert.throws(() => decodeSeedB64('not-base64!!!'), /canonical/);
  assert.throws(() => decodeSeedB64(Buffer.from('short').toString('base64')), /32 bytes/);
  assert.throws(() => decodeSeedB64(''), /canonical/);
});

test('br-1 public key sha256 prefix matches the operator comment', () => {
  const pub = Buffer.from(BR1_PUBLIC_KEY_HEX, 'hex');
  assert.equal(pub.length, 32);
  const digest = createHash('sha256').update(pub).digest('hex');
  assert.ok(digest.startsWith('2a94018110e06c67'));
});

test('loadSigningKey refuses a br-1 mismatch', () => {
  const seedB64 = Buffer.from(RFC_SEED_HEX, 'hex').toString('base64');
  assert.throws(
    () => loadSigningKey(seedB64, { kid: 'br-1' }),
    /does not match/,
  );
});

test('loadSigningKey accepts a throwaway kid without the production pin', () => {
  const seedB64 = Buffer.from(RFC_SEED_HEX, 'hex').toString('base64');
  const loaded = loadSigningKey(seedB64, { kid: 'test-only-1' });
  assert.equal(loaded.publicKeyHex, RFC_PUB_HEX);
  assert.equal(loaded.kid, 'test-only-1');
});
