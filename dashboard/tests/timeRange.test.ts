import { describe, it, expect } from 'vitest';
import { isValidTimeRange, normalizeTimeRange } from '../utils/timeRange';

describe('normalizeTimeRange', () => {
  const now = Date.UTC(2024, 0, 1, 0, 0, 0);

  it('converts preset ranges to start-end format', () => {
    expect(normalizeTimeRange('1h', now)).toBe(`${now - 3_600_000}-${now}`);
    expect(normalizeTimeRange('30m', now)).toBe(`${now - 1_800_000}-${now}`);
    expect(normalizeTimeRange('2d', now)).toBe(`${now - 172_800_000}-${now}`);
  });

  it('returns custom ranges unchanged', () => {
    expect(normalizeTimeRange('1000-2000', now)).toBe('1000-2000');
  });

  it('trims whitespace and defaults to 24 hours for invalid input', () => {
    expect(normalizeTimeRange(' 1h ', now)).toBe(`${now - 3_600_000}-${now}`);
    expect(normalizeTimeRange('foo', now)).toBe(`${now - 86_400_000}-${now}`);
  });

  it('accepts wider preset and custom ranges up to 30 days', () => {
    expect(isValidTimeRange('14d')).toBe(true);
    expect(isValidTimeRange('30d')).toBe(true);
    expect(isValidTimeRange(`${now}-${now + 30 * 24 * 60 * 60 * 1000}`)).toBe(
      true,
    );
    expect(isValidTimeRange('31d')).toBe(false);
  });
});
