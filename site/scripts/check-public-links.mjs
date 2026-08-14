import { setTimeout as sleep } from 'node:timers/promises';
import process from 'node:process';

const baseUrl = new URL(process.env.PUBLIC_DOCS_BASE_URL || 'https://bloodraven.dev/');
const maxPages = Number.parseInt(process.env.PUBLIC_DOCS_MAX_PAGES || '500', 10);
const checkExternal = process.env.PUBLIC_DOCS_CHECK_EXTERNAL === 'true';
const requestTimeoutMs = Number.parseInt(process.env.PUBLIC_DOCS_REQUEST_TIMEOUT_MS || '20000', 10);
const userAgent = 'bloodraven-docs-link-check/1.0';

const htmlPages = new Set();
const checkedResources = new Set();
const queue = [baseUrl.href];
const failures = [];

function skipRawReference(raw) {
  return raw === '' ||
    raw.startsWith('#') ||
    raw.startsWith('mailto:') ||
    raw.startsWith('tel:') ||
    raw.startsWith('javascript:') ||
    raw.startsWith('data:');
}

function withoutHash(url) {
  const copy = new URL(url.href);
  copy.hash = '';
  return copy.href;
}

function underPublicBase(url) {
  return url.origin === baseUrl.origin && url.pathname.startsWith(baseUrl.pathname);
}

function isHtmlLike(url) {
  const pathname = url.pathname;
  const basename = pathname.split('/').pop() || '';
  return pathname.endsWith('/') || basename === '' || basename.endsWith('.html') || !basename.includes('.');
}

async function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
  try {
    return await fetch(url, {
      redirect: 'follow',
      ...options,
      signal: controller.signal,
      headers: {
        'user-agent': userAgent,
        ...(options.headers || {}),
      },
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function checkResource(url, source) {
  const normalized = withoutHash(url);
  if (checkedResources.has(normalized)) {
    return;
  }
  checkedResources.add(normalized);

  let response;
  try {
    response = await fetchWithTimeout(normalized, { method: 'HEAD' });
    if (response.status === 405 || response.status === 403) {
      response = await fetchWithTimeout(normalized, { method: 'GET' });
    }
  } catch (error) {
    failures.push(`${source} -> ${normalized}: ${error.message}`);
    return;
  }

  if (response.status < 200 || response.status >= 400) {
    failures.push(`${source} -> ${normalized}: HTTP ${response.status}`);
  }

  await sleep(25);
}

function extractReferences(html) {
  const refs = [];
  const attrPattern = /\b(?:href|src)\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'<>`]+))/gi;
  for (const match of html.matchAll(attrPattern)) {
    refs.push(match[1] || match[2] || match[3]);
  }

  const srcsetPattern = /\bsrcset\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'<>`]+))/gi;
  for (const match of html.matchAll(srcsetPattern)) {
    for (const candidate of (match[1] || match[2] || match[3]).split(',')) {
      const [url] = candidate.trim().split(/\s+/, 1);
      if (url) {
        refs.push(url);
      }
    }
  }
  return refs;
}

async function crawlPage(pageUrl) {
  let response;
  try {
    response = await fetchWithTimeout(pageUrl);
  } catch (error) {
    failures.push(`${pageUrl}: ${error.message}`);
    return;
  }

  if (response.status < 200 || response.status >= 400) {
    failures.push(`${pageUrl}: HTTP ${response.status}`);
    return;
  }

  const contentType = response.headers.get('content-type') || '';
  if (!contentType.includes('text/html')) {
    return;
  }

  const html = await response.text();
  for (const rawRef of extractReferences(html)) {
    if (skipRawReference(rawRef)) {
      continue;
    }

    let refUrl;
    try {
      refUrl = new URL(rawRef, pageUrl);
    } catch {
      failures.push(`${pageUrl} -> ${rawRef}: invalid URL`);
      continue;
    }

    if (refUrl.protocol !== 'http:' && refUrl.protocol !== 'https:') {
      continue;
    }

    if (underPublicBase(refUrl)) {
      const normalized = withoutHash(refUrl);
      if (isHtmlLike(refUrl)) {
        if (!htmlPages.has(normalized) && queue.length + htmlPages.size < maxPages) {
          queue.push(normalized);
        }
      } else {
        await checkResource(refUrl, pageUrl);
      }
      continue;
    }

    if (checkExternal) {
      await checkResource(refUrl, pageUrl);
    }
  }
}

async function main() {
  while (queue.length > 0) {
    const page = queue.shift();
    const normalized = withoutHash(new URL(page));
    if (htmlPages.has(normalized)) {
      continue;
    }
    if (htmlPages.size >= maxPages) {
      failures.push(`reached PUBLIC_DOCS_MAX_PAGES=${maxPages} before crawl completed`);
      break;
    }
    htmlPages.add(normalized);
    await crawlPage(normalized);
  }

  if (failures.length > 0) {
    console.error(`Public docs link check failed for ${baseUrl.href}:`);
    for (const failure of failures) {
      console.error(`- ${failure}`);
    }
    process.exit(1);
  }

  console.log(`Checked ${htmlPages.size} public docs pages and ${checkedResources.size} same-site resources from ${baseUrl.href}.`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
