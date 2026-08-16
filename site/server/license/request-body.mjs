export class BodyLimitError extends Error {
  constructor(message = 'request body too large') {
    super(message);
    this.name = 'BodyLimitError';
  }
}

export async function readLimitedRaw(req, maxBytes) {
  if (!req || typeof req.on !== 'function') {
    throw new Error('request stream is unavailable');
  }
  return await new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let settled = false;

    const finish = (err, value) => {
      if (settled) {
        return;
      }
      settled = true;
      req.off('data', onData);
      req.off('end', onEnd);
      req.off('error', onError);
      if (err) {
        reject(err);
      } else {
        resolve(value);
      }
    };

    const onData = (chunk) => {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      size += buf.length;
      if (size > maxBytes) {
        req.destroy();
        finish(new BodyLimitError());
        return;
      }
      chunks.push(buf);
    };
    const onEnd = () => finish(null, Buffer.concat(chunks));
    const onError = (err) => finish(err);

    req.on('data', onData);
    req.on('end', onEnd);
    req.on('error', onError);
  });
}

export function parseJsonObject(raw) {
  const text = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw ?? '');
  if (text.trim() === '') {
    return {};
  }
  const parsed = JSON.parse(text);
  if (parsed == null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new SyntaxError('JSON body must be an object');
  }
  return parsed;
}
