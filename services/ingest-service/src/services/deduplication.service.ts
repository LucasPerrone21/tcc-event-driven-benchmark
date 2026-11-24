import { Injectable } from '@nestjs/common';

@Injectable()
export class DeduplicationService {
  private readonly seen = new Map<string, number>();
  private readonly ttlMs = Number(process.env.DEDUP_TTL_MS ?? 60_000);
  private readonly maxEntries = Number(process.env.DEDUP_MAX_ENTRIES ?? 100_000);

  /**
   * Returns true if the key was already seen within the TTL window; otherwise records it and returns false.
   */
  isDuplicate(key: string, nowMs = Date.now()): boolean {
    const expiresAt = this.seen.get(key);
    if (expiresAt !== undefined && expiresAt > nowMs) {
      return true;
    }

    if (this.seen.size >= this.maxEntries) {
      this.evictExpired(nowMs);
      if (this.seen.size >= this.maxEntries) {
        const oldestKey = this.seen.keys().next().value;
        if (oldestKey !== undefined) {
          this.seen.delete(oldestKey);
        }
      }
    }

    this.seen.set(key, nowMs + this.ttlMs);
    return false;
  }

  private evictExpired(nowMs: number): void {
    for (const [k, exp] of this.seen.entries()) {
      if (exp <= nowMs) {
        this.seen.delete(k);
      }
    }
  }
}
