import { getRequestIP } from 'h3'
import { ISSUER, signLicense } from '../license/sign.mjs'
import {
  PolarError,
  emailsMatch,
  editionFromProduct,
  fetchOrder,
  looksLikeOrderId,
  orderIsPaid,
  orgFromOrder,
  readOrderResponse,
  updatesUntilUnix,
} from '../license/polar.mjs'
import { createLimiter } from '../license/rate-limit.mjs'
import { missingSignerReason, readConfig, signerFromConfig } from '../license/config.mjs'

const limiter = createLimiter()
const DENIAL_PAD_MS = 300
const MAX_BODY_BYTES = 4096
const GENERIC_DENIAL = 'Could not issue a license for that order.'
const UNAVAILABLE = 'License signing is temporarily unavailable.'
const DUMMY_EMAIL = 'nobody@invalid.invalid'

type JsonBody = {
  orderId?: unknown
  email?: unknown
}

function clientIp(event: Parameters<typeof getRequestIP>[0]) {
  return getRequestIP(event, { xForwardedFor: true }) || 'unknown'
}

function jsonError(event: Parameters<typeof setResponseStatus>[0], status: number, message: string, extra: Record<string, string> = {}) {
  setResponseStatus(event, status)
  setHeader(event, 'cache-control', 'no-store')
  return { error: message, ...extra }
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function paddedStatus(
  event: Parameters<typeof setResponseStatus>[0],
  started: number,
  status: number,
  message: string,
) {
  const wait = DENIAL_PAD_MS - (Date.now() - started)
  if (wait > 0) {
    await sleep(wait)
  }
  return jsonError(event, status, message)
}

async function paddedDenial(event: Parameters<typeof setResponseStatus>[0], started: number) {
  return paddedStatus(event, started, 404, GENERIC_DENIAL)
}

function logOperator(msg: string, fields: Record<string, unknown> = {}) {
  const safe = { msg, ...fields }
  console.error(JSON.stringify(safe))
}

export default defineEventHandler(async (event) => {
  setHeader(event, 'cache-control', 'no-store')
  const started = Date.now()

  const limited = limiter.allow(clientIp(event))
  if (!limited.ok) {
    setHeader(event, 'retry-after', String(Math.max(1, Math.ceil((limited.resetMs - Date.now()) / 1000))))
    return jsonError(event, 429, 'Too many requests. Try again later.')
  }

  const length = Number(getHeader(event, 'content-length') || 0)
  if (Number.isFinite(length) && length > MAX_BODY_BYTES) {
    return jsonError(event, 400, 'orderId and email are required.')
  }

  let body: JsonBody
  try {
    body = await readBody<JsonBody>(event)
  } catch {
    return jsonError(event, 400, 'orderId and email are required.')
  }

  const orderId = typeof body?.orderId === 'string' ? body.orderId.trim() : ''
  const email = typeof body?.email === 'string' ? body.email.trim() : ''
  if (!orderId || !email || email.length > 254 || orderId.length > 80) {
    return jsonError(event, 400, 'orderId and email are required.')
  }

  const config = readConfig(process.env)
  const missing = missingSignerReason(config)
  if (missing) {
    logOperator('license signing is not configured', { reason: missing })
    return jsonError(event, 503, UNAVAILABLE)
  }

  const signer = signerFromConfig(config)
  if (!signer.ok) {
    logOperator('license signing key rejected', { reason: signer.reason })
    return jsonError(event, 503, UNAVAILABLE)
  }

  if (!looksLikeOrderId(orderId)) {
    emailsMatch(email, DUMMY_EMAIL)
    return paddedDenial(event, started)
  }

  let response
  try {
    response = await fetchOrder({
      base: config.polarBase,
      token: config.polarToken,
      orderId,
    })
  } catch (error) {
    const code = error instanceof PolarError ? error.code : 'unavailable'
    if (code === 'timeout') {
      logOperator('polar request timed out')
    } else {
      logOperator('polar request failed', { reason: code })
    }
    return jsonError(event, 503, UNAVAILABLE)
  }

  let parsed
  try {
    parsed = await readOrderResponse(response)
  } catch (error) {
    const code = error instanceof PolarError ? error.code : 'unavailable'
    if (code === 'unauthorized') {
      logOperator('polar api unauthorized', {
        hint: 'POLAR_API_TOKEN is missing, expired, or lacks orders:read',
      })
    } else {
      logOperator('polar response unusable', { reason: code })
    }
    return jsonError(event, 503, UNAVAILABLE)
  }

  if (parsed.kind !== 'ok') {
    emailsMatch(email, DUMMY_EMAIL)
    return paddedDenial(event, started)
  }

  const order = parsed.order
  const orderEmail = order.customer?.email
  if (!emailsMatch(email, orderEmail ?? '')) {
    return paddedDenial(event, started)
  }

  if (!orderIsPaid(order)) {
    return paddedStatus(event, started, 422, 'This order is not eligible for a license.')
  }

  const edition = editionFromProduct(order.product)
  if (!edition) {
    logOperator('order product has no license edition metadata', {
      issuedFor: order.id,
    })
    return paddedStatus(event, started, 422, 'This order is not a Bloodraven license product.')
  }

  let updatesUntil: number
  try {
    updatesUntil = updatesUntilUnix(order)
  } catch {
    logOperator('order is missing created_at', { issuedFor: order.id })
    return jsonError(event, 503, UNAVAILABLE)
  }

  const org = orgFromOrder(order)
  const iat = Math.floor(Date.now() / 1000)
  let token: string
  try {
    token = signLicense({
      privateKey: signer.privateKey,
      kid: signer.kid,
      claims: {
        iss: ISSUER,
        sub: String(order.customer_id || order.customer?.id || ''),
        org,
        edition,
        issuedFor: order.id,
        iat,
        updatesUntil,
      },
    })
  } catch (error) {
    const message = error instanceof Error ? error.message : 'sign failed'
    if (/seed|LICENSE_SIGNING|private key|base64/i.test(message)) {
      logOperator('license sign failed')
    } else {
      logOperator('license sign failed', { reason: message })
    }
    return jsonError(event, 503, UNAVAILABLE)
  }

  logOperator('license issued', {
    issuedFor: order.id,
    edition,
    kid: signer.kid,
  })

  return {
    token,
    edition,
    org,
    updatesUntil,
    issuedFor: order.id,
  }
})
