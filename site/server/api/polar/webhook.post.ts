import { receivePolarWebhook } from '../../license/webhook.mjs'

function logOperator(msg: string, fields: Record<string, unknown> = {}) {
  console.error(JSON.stringify({ msg, ...fields }))
}

export default defineEventHandler(async (event) => {
  setHeader(event, 'cache-control', 'no-store')

  // Signature is over the exact raw bytes. Do not readBody / JSON.parse first.
  const rawBody = await readRawBody(event)
  const result = receivePolarWebhook({
    rawBody: rawBody ?? '',
    headers: getHeaders(event),
    secret: typeof process.env.POLAR_WEBHOOK_SECRET === 'string'
      ? process.env.POLAR_WEBHOOK_SECRET.trim()
      : '',
    log: logOperator,
  })

  setResponseStatus(event, result.status)
  return result.body
})
