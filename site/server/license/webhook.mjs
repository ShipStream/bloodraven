import { Webhook } from 'standardwebhooks';
import { validateEvent } from '@polar-sh/sdk/webhooks';

const NOTABLE_TYPES = new Set(['order.refunded', 'subscription.revoked']);

function asString(value) {
  return typeof value === 'string' && value.length > 0 ? value : '';
}

function dataOf(source) {
  return source?.data && typeof source.data === 'object' ? source.data : {};
}

export function webhookIdentifiers(source) {
  const type = asString(source?.type) || 'unknown';
  const data = dataOf(source);
  const ids = { type };

  if (type.startsWith('order.')) {
    const orderId = asString(data.id);
    if (orderId) {
      ids.orderId = orderId;
    }
    const subscriptionId = asString(data.subscriptionId) || asString(data.subscription_id);
    if (subscriptionId) {
      ids.subscriptionId = subscriptionId;
    }
  } else if (type.startsWith('subscription.')) {
    const subscriptionId = asString(data.id);
    if (subscriptionId) {
      ids.subscriptionId = subscriptionId;
    }
  }

  const productId = asString(data.productId)
    || asString(data.product_id)
    || asString(data.product?.id);
  if (productId) {
    ids.productId = productId;
  }

  return ids;
}

export function identifiersFromRaw(rawBody) {
  try {
    const text = typeof rawBody === 'string' ? rawBody : Buffer.from(rawBody).toString('utf8');
    return webhookIdentifiers(JSON.parse(text));
  } catch {
    return { type: 'unknown' };
  }
}

export function webhookHeaderMap(headers) {
  const source = headers && typeof headers === 'object' ? headers : {};
  const read = (name) => {
    const value = source[name] ?? source[name.toLowerCase()];
    return typeof value === 'string' ? value : '';
  };
  return {
    'webhook-id': read('webhook-id'),
    'webhook-timestamp': read('webhook-timestamp'),
    'webhook-signature': read('webhook-signature'),
  };
}

function defaultLog(msg, fields = {}) {
  console.error(JSON.stringify({ msg, ...fields }));
}

export function receivePolarWebhook({ rawBody, headers, secret, log = defaultLog }) {
  if (!secret) {
    log('polar webhook secret is not configured');
    return { status: 503, body: { error: 'Webhook receiver is not configured.' } };
  }

  if (rawBody == null || rawBody.length === 0) {
    log('polar webhook signature rejected');
    return { status: 401, body: { error: 'Invalid signature.' } };
  }

  const headerMap = webhookHeaderMap(headers);

  // Authenticate first, as its own step. validateEvent both verifies the
  // signature and parses the event, and it throws a non-verification error
  // for an authentic delivery whose shape it does not recognise — so its
  // exception type cannot tell "forged" from "unrecognised". Deciding
  // authenticity here keeps that distinction explicit: anything that fails
  // this step is rejected, whatever it threw.
  try {
    new Webhook(Buffer.from(secret, 'utf-8').toString('base64')).verify(rawBody, headerMap);
  } catch {
    log('polar webhook signature rejected');
    return { status: 401, body: { error: 'Invalid signature.' } };
  }

  // Authentic from here. Parsing failures must NOT become 401: Polar retries
  // non-2xx, so rejecting a genuine delivery we simply cannot parse would
  // retry forever. Fall back to identifiers read from the raw body.
  let event;
  try {
    event = validateEvent(rawBody, headerMap, secret);
  } catch {
    const ids = identifiersFromRaw(rawBody);
    const msg = NOTABLE_TYPES.has(ids.type) ? 'polar webhook notable' : 'polar webhook received';
    log(msg, ids);
    return { status: 202, body: '' };
  }

  const ids = webhookIdentifiers(event);
  const msg = NOTABLE_TYPES.has(ids.type) ? 'polar webhook notable' : 'polar webhook received';
  log(msg, ids);
  return { status: 202, body: '' };
}
