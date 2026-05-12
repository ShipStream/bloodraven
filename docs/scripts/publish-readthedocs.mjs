import { setTimeout as sleep } from 'node:timers/promises';
import process from 'node:process';

const token = requiredEnv('READTHEDOCS_TOKEN');
const projectSlug = process.env.READTHEDOCS_PROJECT_SLUG || 'bloodraven';
const versionSlug = process.env.READTHEDOCS_VERSION_SLUG || 'latest';
const apiBaseUrl = ensureTrailingSlash(process.env.READTHEDOCS_API_BASE_URL || 'https://app.readthedocs.org/api/v3/');
const timeoutSeconds = Number.parseInt(process.env.READTHEDOCS_WAIT_TIMEOUT_SECONDS || '900', 10);
const pollSeconds = Number.parseInt(process.env.READTHEDOCS_POLL_SECONDS || '15', 10);

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`${name} is required to publish ReadTheDocs.`);
    process.exit(1);
  }
  return value;
}

function ensureTrailingSlash(value) {
  return value.endsWith('/') ? value : `${value}/`;
}

function apiUrl(pathOrUrl) {
  if (pathOrUrl.startsWith('http://') || pathOrUrl.startsWith('https://')) {
    return pathOrUrl;
  }
  if (pathOrUrl.startsWith('/api/')) {
    const base = new URL(apiBaseUrl);
    return `${base.origin}${pathOrUrl}`;
  }
  return new URL(pathOrUrl.replace(/^\//, ''), apiBaseUrl).href;
}

async function readJsonResponse(response) {
  const text = await response.text();
  if (!text) {
    return {};
  }
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Token ${token}`,
      Accept: 'application/json',
      ...(options.body ? { 'Content-Type': 'application/json' } : {}),
      ...(options.headers || {}),
    },
  });
  const body = await readJsonResponse(response);
  if (!response.ok) {
    throw new Error(`${options.method || 'GET'} ${url} failed with HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

function buildUrlFromPayload(payload) {
  const build = payload.build;
  if (!build) {
    return '';
  }
  if (typeof build === 'object') {
    const self = build._links?._self || build.url || build.api_url;
    if (self) {
      return apiUrl(self);
    }
    const id = build.id || build.pk;
    if (id) {
      return apiUrl(`projects/${projectSlug}/builds/${id}/`);
    }
  }
  if (typeof build === 'string') {
    if (/^\d+$/.test(build)) {
      return apiUrl(`projects/${projectSlug}/builds/${build}/`);
    }
    return apiUrl(build);
  }
  return '';
}

async function latestBuildUrl() {
  const payload = await requestJson(apiUrl(`projects/${projectSlug}/builds/?limit=1`));
  const first = payload.results?.[0];
  if (!first) {
    return '';
  }
  if (first._links?._self) {
    return apiUrl(first._links._self);
  }
  if (first.id || first.pk) {
    return apiUrl(`projects/${projectSlug}/builds/${first.id || first.pk}/`);
  }
  return '';
}

async function main() {
  const triggerUrl = apiUrl(`projects/${projectSlug}/versions/${versionSlug}/builds/`);
  const triggerResponse = await requestJson(triggerUrl, { method: 'POST' });
  let buildUrl = buildUrlFromPayload(triggerResponse);
  if (!buildUrl) {
    buildUrl = await latestBuildUrl();
  }
  if (!buildUrl) {
    throw new Error(`ReadTheDocs accepted the build trigger but did not return a build URL: ${JSON.stringify(triggerResponse)}`);
  }

  console.log(`Triggered ReadTheDocs build for ${projectSlug}/${versionSlug}: ${buildUrl}`);

  const deadline = Date.now() + timeoutSeconds * 1000;
  while (Date.now() < deadline) {
    const build = await requestJson(buildUrl);
    const state = build.state || build.status || 'unknown';
    console.log(`ReadTheDocs build state: ${state}`);

    if (state === 'finished') {
      if (build.success === false || build.error) {
        throw new Error(`ReadTheDocs build finished with an error: ${build.error || JSON.stringify(build)}`);
      }
      console.log(`ReadTheDocs build finished successfully for ${projectSlug}/${versionSlug}.`);
      return;
    }
    if (state === 'cancelled') {
      throw new Error(`ReadTheDocs build was cancelled: ${JSON.stringify(build)}`);
    }

    await sleep(pollSeconds * 1000);
  }

  throw new Error(`Timed out after ${timeoutSeconds}s waiting for ReadTheDocs build ${buildUrl}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
