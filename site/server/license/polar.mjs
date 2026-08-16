import { createHash, timingSafeEqual } from 'node:crypto';

export const POLAR_PRODUCTION_BASE = 'https://api.polar.sh';
export const POLAR_SANDBOX_BASE = 'https://sandbox-api.polar.sh';
export const ALLOWED_POLAR_BASES = new Set([POLAR_PRODUCTION_BASE, POLAR_SANDBOX_BASE]);

const EDITIONS = new Set(['production', 'organization']);
const SUBSCRIPTION_PERIOD_STATUSES = new Set(['active', 'canceled', 'past_due']);
const MAX_ORG_CHARS = 200;

export class PolarError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'PolarError';
    this.code = code;
  }
}

export function normalizePolarBase(raw) {
  const fallback = POLAR_PRODUCTION_BASE;
  if (raw == null || String(raw).trim() === '') {
    return fallback;
  }
  let value = String(raw).trim().replace(/\/+$/, '');
  if (value.endsWith('/v1')) {
    value = value.slice(0, -3);
  }
  if (!ALLOWED_POLAR_BASES.has(value)) {
    throw new PolarError('bad-base', 'POLAR_API_BASE is not an allowed Polar origin');
  }
  return value;
}

export function normalizeEmail(value) {
  return String(value ?? '').trim().toLowerCase();
}

export function emailsMatch(left, right) {
  const a = createHash('sha256').update(normalizeEmail(left)).digest();
  const b = createHash('sha256').update(normalizeEmail(right)).digest();
  return timingSafeEqual(a, b);
}

export function looksLikeOrderId(value) {
  return typeof value === 'string'
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value.trim());
}

function sanitizeOrg(value) {
  const cleaned = String(value)
    .replace(/[\u0000-\u001f\u007f]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) {
    return '';
  }
  return cleaned.length > MAX_ORG_CHARS ? cleaned.slice(0, MAX_ORG_CHARS) : cleaned;
}

export function orgFromOrder(order) {
  const candidates = [
    order?.customer?.name,
    order?.billing_name,
    order?.customer?.billing_name,
  ];
  for (const candidate of candidates) {
    const org = sanitizeOrg(candidate ?? '');
    if (org) {
      return org;
    }
  }
  return 'Individual';
}

export function editionFromProduct(product) {
  const raw = product?.metadata?.edition;
  if (typeof raw !== 'string') {
    return null;
  }
  const edition = raw.trim();
  return EDITIONS.has(edition) ? edition : null;
}

export function addCalendarMonthsUtc(date, months) {
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth();
  const day = date.getUTCDate();
  const hours = date.getUTCHours();
  const minutes = date.getUTCMinutes();
  const seconds = date.getUTCSeconds();
  const ms = date.getUTCMilliseconds();
  const endMonthIndex = month + months;
  const target = new Date(Date.UTC(year, endMonthIndex, 1, hours, minutes, seconds, ms));
  const lastDay = new Date(Date.UTC(target.getUTCFullYear(), target.getUTCMonth() + 1, 0)).getUTCDate();
  target.setUTCDate(Math.min(day, lastDay));
  return target;
}

export function updatesUntilUnix(order) {
  const periodEnd = order?.subscription?.current_period_end;
  const status = order?.subscription?.status;
  if (periodEnd && SUBSCRIPTION_PERIOD_STATUSES.has(status)) {
    const parsed = Date.parse(periodEnd);
    if (Number.isFinite(parsed)) {
      return Math.floor(parsed / 1000);
    }
  }
  const created = Date.parse(order?.created_at);
  if (!Number.isFinite(created)) {
    throw new PolarError('bad-order', 'order.created_at is missing');
  }
  return Math.floor(addCalendarMonthsUtc(new Date(created), 12).getTime() / 1000);
}

export function orderIsPaid(order) {
  return order?.paid === true
    && order?.status === 'paid'
    && (order?.refunded_amount ?? 0) === 0;
}

function abortError() {
  const error = new Error('aborted');
  error.name = 'AbortError';
  return error;
}

export async function fetchOrder({ base, token, orderId, timeoutMs = 8000, fetchImpl = fetch }) {
  const url = `${base}/v1/orders/${encodeURIComponent(orderId)}`;
  const controller = new AbortController();
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(abortError());
    }, timeoutMs);
  });
  try {
    const request = fetchImpl(url, {
      method: 'GET',
      headers: {
        authorization: `Bearer ${token}`,
        accept: 'application/json',
      },
      signal: controller.signal,
    });
    request.catch(() => {});
    const response = await Promise.race([request, timeout]);
    return response;
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new PolarError('timeout', 'Polar request timed out');
    }
    throw new PolarError('unavailable', 'Polar request failed');
  } finally {
    clearTimeout(timer);
  }
}

export async function readOrderResponse(response) {
  const status = response.status;
  if (status === 404 || status === 422) {
    return { kind: 'not-found' };
  }
  if (status === 401 || status === 403) {
    throw new PolarError('unauthorized', 'Polar API token is unauthorized or expired');
  }
  if (status === 429 || status >= 500) {
    throw new PolarError('unavailable', 'Polar is unavailable');
  }
  if (status !== 200) {
    throw new PolarError('unavailable', 'Polar returned an unexpected status');
  }
  let body;
  try {
    body = await response.json();
  } catch {
    throw new PolarError('unavailable', 'Polar returned invalid JSON');
  }
  if (!body || typeof body !== 'object' || typeof body.id !== 'string') {
    throw new PolarError('unavailable', 'Polar order payload is malformed');
  }
  return { kind: 'ok', order: body };
}
