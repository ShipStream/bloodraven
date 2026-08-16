#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { loadSigningKey } from '../server/license/key.mjs';
import { signLicense } from '../server/license/sign.mjs';

const raw = readFileSync(0, 'utf8');
let input;
try {
  input = JSON.parse(raw);
} catch {
  console.error('sign-license-token: stdin must be JSON');
  process.exit(2);
}

if (!input || typeof input !== 'object' || typeof input.seedB64 !== 'string') {
  console.error('sign-license-token: seedB64 is required');
  process.exit(2);
}

const loaded = loadSigningKey(input.seedB64, {
  kid: input.kid,
  expectedPubHex: input.expectedPubHex || '',
});
const token = signLicense({
  privateKey: loaded.privateKey,
  kid: loaded.kid,
  claims: input.claims,
});

process.stdout.write(`${JSON.stringify({
  token,
  publicKeyHex: loaded.publicKeyHex,
  kid: loaded.kid,
})}\n`);
