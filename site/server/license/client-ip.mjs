// Railway's edge appends the connecting IP to X-Forwarded-For and overwrites
// X-Real-IP. The leftmost XFF value is caller-controlled.

export function trustedClientIp(headers = {}, fallback = '') {
  const real = String(headers['x-real-ip'] || '').trim();
  if (real && !real.includes(',')) {
    return real;
  }
  const forwarded = String(headers['x-forwarded-for'] || '');
  const hops = forwarded.split(',').map((part) => part.trim()).filter(Boolean);
  if (hops.length > 0) {
    return hops[hops.length - 1];
  }
  return fallback || 'unknown';
}
