/**
 * Returns the current UTC timestamp in ISO-8601 format with microsecond precision.
 */
export function utcNowMicroseconds(): string {
  const nowMs = performance.timeOrigin + performance.now();
  const wholeMs = Math.floor(nowMs);
  const subMsMicros = Math.floor((nowMs - wholeMs) * 1000)
    .toString()
    .padStart(3, '0');
  const iso = new Date(wholeMs).toISOString();
  return `${iso.slice(0, -1)}${subMsMicros}Z`;
}
