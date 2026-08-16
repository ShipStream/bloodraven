import assert from 'node:assert/strict';
import test from 'node:test';
import { privateKeyFromSeed } from '../server/license/key.mjs';
import { ISSUER, compactClaims, decodePayload, signLicense } from '../server/license/sign.mjs';

const SEED = Buffer.from('9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60', 'hex');

function claims(overrides = {}) {
  return {
    iss: ISSUER,
    sub: 'cus_example',
    org: 'Acme Corp',
    edition: 'organization',
    issuedFor: 'ord_example',
    iat: 1755216000,
    updatesUntil: 1786752000,
    ...overrides,
  };
}

test('signed token is compact JWS without exp', () => {
  const token = signLicense({
    privateKey: privateKeyFromSeed(SEED),
    kid: 'test-only-1',
    claims: claims(),
  });
  const parts = token.split('.');
  assert.equal(parts.length, 3);
  assert.equal(parts.every((p) => !p.includes('=')), true);
  const header = JSON.parse(Buffer.from(parts[0], 'base64url').toString('utf8'));
  assert.deepEqual(header, { alg: 'EdDSA', kid: 'test-only-1', typ: 'JWT' });
  const payload = decodePayload(token);
  assert.equal(payload.iss, ISSUER);
  assert.equal(payload.edition, 'organization');
  assert.equal(Object.hasOwn(payload, 'exp'), false);
  assert.equal(Number.isInteger(payload.iat), true);
  assert.equal(Number.isInteger(payload.updatesUntil), true);
});

test('refuses to emit exp', () => {
  assert.throws(
    () => compactClaims(claims({ exp: 1786752000 })),
    /exp must be omitted/,
  );
});

test('rejects empty required claims and unknown editions', () => {
  assert.throws(() => compactClaims(claims({ org: '' })), /org/);
  assert.throws(() => compactClaims(claims({ edition: 'community' })), /edition/);
  assert.throws(() => compactClaims(claims({ iat: 1.5 })), /iat/);
  assert.throws(() => compactClaims(claims({ iss: 'https://example.com' })), /issuer/);
});
