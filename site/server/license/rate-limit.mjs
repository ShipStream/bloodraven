export const RATE_LIMIT_MAX = 10;
export const RATE_LIMIT_WINDOW_MS = 15 * 60 * 1000;
const MAX_KEYS = 10_000;

export function createLimiter({
  max = RATE_LIMIT_MAX,
  windowMs = RATE_LIMIT_WINDOW_MS,
  maxKeys = MAX_KEYS,
  now = () => Date.now(),
} = {}) {
  const hits = new Map();

  function evict(nowMs) {
    for (const [key, rec] of hits) {
      if (nowMs - rec.start >= windowMs) {
        hits.delete(key);
      }
    }
    if (hits.size < maxKeys) {
      return;
    }
    const oldest = hits.keys().next().value;
    if (oldest !== undefined) {
      hits.delete(oldest);
    }
  }

  return {
    allow(ip) {
      const nowMs = now();
      evict(nowMs);
      const key = ip || 'unknown';
      let rec = hits.get(key);
      if (!rec || nowMs - rec.start >= windowMs) {
        rec = { start: nowMs, count: 0 };
        hits.set(key, rec);
      }
      rec.count += 1;
      return {
        ok: rec.count <= max,
        remaining: Math.max(0, max - rec.count),
        resetMs: rec.start + windowMs,
      };
    },
    size() {
      return hits.size;
    },
  };
}
