import { describe, it, expect } from 'vitest';
import { generateApiKey, hashApiKey } from './auth';

describe('api keys', () => {
  it('generates a 64-char hex key (32 random bytes)', () => {
    const key = generateApiKey();
    expect(key).toMatch(/^[0-9a-f]{64}$/);
    expect(generateApiKey()).not.toBe(key);
  });
  it('hash is deterministic and not the key itself', () => {
    const key = generateApiKey();
    expect(hashApiKey(key)).toBe(hashApiKey(key));
    expect(hashApiKey(key)).not.toBe(key);
  });
});
