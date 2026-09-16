import { createHash, timingSafeEqual } from 'node:crypto';
import { HTTPClient, Polar } from '@polar-sh/sdk';
import { ResourceNotFound } from '@polar-sh/sdk/models/errors/resourcenotfound.js';
import { HTTPValidationError } from '@polar-sh/sdk/models/errors/httpvalidationerror.js';

export const POLAR_PRODUCTION_BASE = 'https://api.polar.sh';
export const POLAR_SANDBOX_BASE = 'https://sandbox-api.polar.sh';
export const ALLOWED_POLAR_BASES = new Set([POLAR_PRODUCTION_BASE, POLAR_SANDBOX_BASE]);

const EDITIONS = new Set(['production', 'organization']);
export const LICENSE_KINDS = new Set(['base', 'renewal']);
const ORDER_PAGE_LIMIT = 100;
const MAX_ORDER_PAGES = 10;
const SUBSCRIPTION_PERIOD_STATUSES = new Set(['active', 'canceled', 'past_due', 'trialing']);
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

function pick(object, ...keys) {
  if (!object || typeof object !== 'object') {
    return undefined;
  }
  for (const key of keys) {
    if (object[key] != null) {
      return object[key];
    }
  }
  return undefined;
}

function toUnixMs(value) {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

export function orgFromOrder(order) {
  const candidates = [
    order?.customer?.name,
    pick(order, 'billingName', 'billing_name'),
    pick(order?.customer, 'billingName', 'billing_name'),
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

/**
 * `kind` separates a base license purchase from a discounted renewal of one.
 * A product with no `kind`, or an unknown one, is not treated as either:
 * guessing `base` would let a renewal SKU mint a full license.
 */
export function kindFromProduct(product) {
  const raw = product?.metadata?.kind;
  if (typeof raw !== 'string') {
    return null;
  }
  const kind = raw.trim();
  return LICENSE_KINDS.has(kind) ? kind : null;
}

export function customerIdFromOrder(order) {
  const id = pick(order, 'customerId', 'customer_id') ?? order?.customer?.id;
  return typeof id === 'string' && id ? id : '';
}

/**
 * True when `candidate` is a paid, unrefunded base purchase of `edition`,
 * made by the same customer no later than the renewal order.
 */
export function qualifiesAsPriorBase(candidate, { customerId, edition, renewalOrderId, renewalCreatedMs }) {
  if (!candidate || candidate.id === renewalOrderId) {
    return false;
  }
  if (customerIdFromOrder(candidate) !== customerId) {
    return false;
  }
  if (!orderIsPaid(candidate)) {
    return false;
  }
  if (kindFromProduct(candidate.product) !== 'base' || editionFromProduct(candidate.product) !== edition) {
    return false;
  }
  const createdMs = toUnixMs(pick(candidate, 'createdAt', 'created_at'));
  return Number.isFinite(createdMs) && Number.isFinite(renewalCreatedMs) && createdMs <= renewalCreatedMs;
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
  const periodEnd = pick(order?.subscription, 'currentPeriodEnd', 'current_period_end');
  const status = order?.subscription?.status;
  if (periodEnd && SUBSCRIPTION_PERIOD_STATUSES.has(status)) {
    const parsed = toUnixMs(periodEnd);
    if (Number.isFinite(parsed)) {
      return Math.floor(parsed / 1000);
    }
  }
  const created = pick(order, 'createdAt', 'created_at');
  const createdMs = toUnixMs(created);
  if (!Number.isFinite(createdMs)) {
    throw new PolarError('bad-order', 'order created_at is missing');
  }
  return Math.floor(addCalendarMonthsUtc(new Date(createdMs), 12).getTime() / 1000);
}

export function orderIsPaid(order) {
  const refunded = pick(order, 'refundedAmount', 'refunded_amount') ?? 0;
  return order?.paid === true
    && order?.status === 'paid'
    && refunded === 0;
}

export function createPolarClient({ token, base, timeoutMs = 8000, httpClient, fetchImpl } = {}) {
  const client = httpClient || (fetchImpl
    ? new HTTPClient({ fetcher: fetchImpl })
    : undefined);
  return new Polar({
    accessToken: token,
    serverURL: base,
    timeoutMs,
    httpClient: client,
  });
}

function statusOf(error) {
  return error && typeof error.statusCode === 'number' ? error.statusCode : 0;
}

function mapPolarError(error) {
  if (error instanceof PolarError && error.code) {
    return error;
  }
  const status = statusOf(error);
  if (status === 401 || status === 403) {
    return new PolarError('unauthorized', 'Polar API token is unauthorized or expired');
  }
  if (status === 429 || status >= 500) {
    return new PolarError('unavailable', 'Polar is unavailable');
  }
  if (error?.name === 'AbortError') {
    return new PolarError('timeout', 'Polar request timed out');
  }
  return new PolarError('unavailable', 'Polar request failed');
}

async function withTimeout(lookup, timeoutMs) {
  lookup.catch(() => {});
  let timer;
  try {
    return await Promise.race([
      lookup,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new PolarError('timeout', 'Polar request timed out'));
        }, timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export async function getOrder(client, orderId, { timeoutMs = 8000 } = {}) {
  const lookup = (async () => {
    try {
      const order = await client.orders.get({ id: orderId });
      if (!order || typeof order.id !== 'string') {
        throw new PolarError('unavailable', 'Polar order payload is malformed');
      }
      return { kind: 'ok', order };
    } catch (error) {
      if (error instanceof ResourceNotFound || error instanceof HTTPValidationError) {
        return { kind: 'not-found' };
      }
      const status = statusOf(error);
      if (status === 404 || status === 422) {
        return { kind: 'not-found' };
      }
      throw mapPolarError(error);
    }
  })();
  return withTimeout(lookup, timeoutMs);
}

/**
 * Looks through the renewal customer's Polar orders for a qualifying base
 * purchase of the same edition. Renewal orders must not mint a token without
 * one. Requires only `orders:read`.
 */
export async function hasPriorBaseOrder(client, renewalOrder, edition, { timeoutMs = 8000 } = {}) {
  const customerId = customerIdFromOrder(renewalOrder);
  if (!customerId) {
    return false;
  }
  const criteria = {
    customerId,
    edition,
    renewalOrderId: renewalOrder.id,
    renewalCreatedMs: toUnixMs(pick(renewalOrder, 'createdAt', 'created_at')),
  };
  const lookup = (async () => {
    try {
      for (let page = 1; page <= MAX_ORDER_PAGES; page += 1) {
        const response = await client.orders.list({
          customerId,
          page,
          limit: ORDER_PAGE_LIMIT,
          sorting: ['created_at'],
        });
        const result = response?.result;
        const items = Array.isArray(result?.items) ? result.items : null;
        if (!items) {
          throw new PolarError('unavailable', 'Polar order list payload is malformed');
        }
        if (items.some((candidate) => qualifiesAsPriorBase(candidate, criteria))) {
          return true;
        }
        const maxPage = Number(pick(result.pagination, 'maxPage', 'max_page')) || 1;
        if (page >= maxPage || items.length === 0) {
          return false;
        }
      }
      return false;
    } catch (error) {
      throw mapPolarError(error);
    }
  })();
  return withTimeout(lookup, timeoutMs);
}
