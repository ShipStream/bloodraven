import { BR1_KID, BR1_PUBLIC_KEY_HEX, SeedError, loadSigningKey } from './key.mjs';
import { PolarError, normalizePolarBase } from './polar.mjs';

export function readConfig(env = process.env) {
  const seedB64 = typeof env.LICENSE_SIGNING_SEED_B64 === 'string'
    ? env.LICENSE_SIGNING_SEED_B64.trim()
    : '';
  const polarToken = typeof env.POLAR_API_TOKEN === 'string'
    ? env.POLAR_API_TOKEN.trim()
    : '';
  const kid = (typeof env.LICENSE_SIGNING_KID === 'string' && env.LICENSE_SIGNING_KID.trim())
    ? env.LICENSE_SIGNING_KID.trim()
    : BR1_KID;
  const expectedPubHex = typeof env.LICENSE_EXPECTED_PUB_HEX === 'string'
    ? env.LICENSE_EXPECTED_PUB_HEX.trim().toLowerCase()
    : '';

  let polarBase = 'https://api.polar.sh';
  let polarBaseError = '';
  try {
    polarBase = normalizePolarBase(env.POLAR_API_BASE);
  } catch (error) {
    polarBaseError = error instanceof PolarError ? error.message : 'POLAR_API_BASE is invalid';
  }

  return {
    seedB64,
    polarToken,
    polarBase,
    polarBaseError,
    kid,
    expectedPubHex,
  };
}

export function missingSignerReason(config) {
  if (config.polarBaseError) {
    return 'polar-base';
  }
  if (!config.polarToken) {
    return 'polar-token';
  }
  if (!config.seedB64) {
    return 'seed';
  }
  return '';
}

export function signerFromConfig(config) {
  try {
    return {
      ok: true,
      ...loadSigningKey(config.seedB64, {
        kid: config.kid,
        expectedPubHex: config.expectedPubHex || (config.kid === BR1_KID ? BR1_PUBLIC_KEY_HEX : ''),
      }),
    };
  } catch (error) {
    const safe = error instanceof SeedError ? error.message : 'signing key is not usable';
    return { ok: false, reason: safe };
  }
}
