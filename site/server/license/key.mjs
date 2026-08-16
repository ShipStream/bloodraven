import { createPrivateKey, createPublicKey } from 'node:crypto';

// RFC 8410 PKCS#8 prefix for a 32-byte Ed25519 seed.
const PKCS8_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex');
// RFC 8410 SPKI prefix for a 32-byte Ed25519 public key.
const SPKI_PREFIX = Buffer.from('302a300506032b6570032100', 'hex');

export const BR1_KID = 'br-1';
export const BR1_PUBLIC_KEY_HEX = '1b3bea77364fff24dad67d2727c3861c1a93dd3f22faccfac4b0fdecd69f6f02';

export class SeedError extends Error {
  constructor(message) {
    super(message);
    this.name = 'SeedError';
  }
}

function isCanonicalStdBase64(value) {
  if (typeof value !== 'string' || value.length === 0 || value.length % 4 !== 0) {
    return false;
  }
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(value)) {
    return false;
  }
  const padding = value.endsWith('==') ? 2 : value.endsWith('=') ? 1 : 0;
  if (padding && value.slice(0, -padding).includes('=')) {
    return false;
  }
  return true;
}

export function decodeSeedB64(value) {
  if (!isCanonicalStdBase64(value)) {
    throw new SeedError('LICENSE_SIGNING_SEED_B64 is not canonical standard base64');
  }
  const seed = Buffer.from(value, 'base64');
  if (seed.length !== 32) {
    throw new SeedError('LICENSE_SIGNING_SEED_B64 must decode to 32 bytes');
  }
  if (seed.toString('base64') !== value) {
    throw new SeedError('LICENSE_SIGNING_SEED_B64 is not canonical standard base64');
  }
  return seed;
}

export function privateKeyFromSeed(seed) {
  if (!Buffer.isBuffer(seed) || seed.length !== 32) {
    throw new SeedError('Ed25519 seed must be 32 bytes');
  }
  const pkcs8 = Buffer.concat([PKCS8_PREFIX, seed]);
  return createPrivateKey({ key: pkcs8, format: 'der', type: 'pkcs8' });
}

export function publicKeyRaw(privateKey) {
  const spki = createPublicKey(privateKey).export({ type: 'spki', format: 'der' });
  if (!Buffer.isBuffer(spki) || spki.length !== 44 || !spki.subarray(0, 12).equals(SPKI_PREFIX)) {
    throw new SeedError('derived public key is not an Ed25519 SPKI');
  }
  return spki.subarray(12);
}

export function publicKeyHex(privateKey) {
  return publicKeyRaw(privateKey).toString('hex');
}

export function loadSigningKey(seedB64, { kid = BR1_KID, expectedPubHex = '' } = {}) {
  const seed = decodeSeedB64(seedB64);
  const privateKey = privateKeyFromSeed(seed);
  const pubHex = publicKeyHex(privateKey);
  const expected = (expectedPubHex || (kid === BR1_KID ? BR1_PUBLIC_KEY_HEX : '')).toLowerCase();
  if (expected && pubHex !== expected) {
    throw new SeedError('derived public key does not match the expected kid');
  }
  return { privateKey, publicKeyHex: pubHex, kid };
}
