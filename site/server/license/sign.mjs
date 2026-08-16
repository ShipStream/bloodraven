import { sign as cryptoSign } from 'node:crypto';

export const ISSUER = 'https://license.shipstream.io/bloodraven';
export const ALG = 'EdDSA';

const EDITIONS = new Set(['production', 'organization']);

export class SignError extends Error {
  constructor(message) {
    super(message);
    this.name = 'SignError';
  }
}

function b64url(buf) {
  return Buffer.from(buf).toString('base64url');
}

function requireNonEmptyString(claims, key) {
  const value = claims[key];
  if (typeof value !== 'string' || value.length === 0) {
    throw new SignError(`claim ${key} must be a non-empty string`);
  }
  return value;
}

function requireUnixSeconds(claims, key) {
  const value = claims[key];
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0 || !Number.isSafeInteger(value)) {
    throw new SignError(`claim ${key} must be a non-negative integer unix timestamp`);
  }
  return value;
}

export function compactClaims(input) {
  if (input == null || typeof input !== 'object' || Array.isArray(input)) {
    throw new SignError('claims must be an object');
  }
  if (Object.prototype.hasOwnProperty.call(input, 'exp')) {
    throw new SignError('exp must be omitted');
  }

  const edition = requireNonEmptyString(input, 'edition');
  if (!EDITIONS.has(edition)) {
    throw new SignError('edition must be production or organization');
  }

  const claims = {
    edition,
    iat: requireUnixSeconds(input, 'iat'),
    iss: requireNonEmptyString(input, 'iss'),
    issuedFor: requireNonEmptyString(input, 'issuedFor'),
    org: requireNonEmptyString(input, 'org'),
    sub: requireNonEmptyString(input, 'sub'),
    updatesUntil: requireUnixSeconds(input, 'updatesUntil'),
  };

  if (claims.iss !== ISSUER) {
    throw new SignError('iss does not match the Bloodraven license issuer');
  }

  if (Object.prototype.hasOwnProperty.call(input, 'nbf')) {
    claims.nbf = requireUnixSeconds(input, 'nbf');
  }

  return claims;
}

export function signLicense({ privateKey, kid, claims }) {
  if (!privateKey) {
    throw new SignError('private key is required');
  }
  if (typeof kid !== 'string' || kid.length === 0) {
    throw new SignError('kid is required');
  }

  const payload = compactClaims(claims);
  const header = { alg: ALG, kid, typ: 'JWT' };
  const headerB64 = b64url(JSON.stringify(header));
  const payloadB64 = b64url(JSON.stringify(payload));
  const signingInput = `${headerB64}.${payloadB64}`;
  const signature = cryptoSign(null, Buffer.from(signingInput), privateKey);
  if (signature.length !== 64) {
    throw new SignError('Ed25519 signature must be 64 bytes');
  }
  return `${signingInput}.${b64url(signature)}`;
}

export function decodePayload(token) {
  const parts = String(token).split('.');
  if (parts.length !== 3) {
    throw new SignError('token is not a compact JWS');
  }
  return JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
}
