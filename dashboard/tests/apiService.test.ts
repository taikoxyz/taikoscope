import { describe, it, expect, afterEach, beforeEach, vi } from 'vitest';
import * as toast from '../utils/toast';

import {
  fetchAvgProveTime,
  fetchL2BlockTimes,
  fetchBlockTransactions,
  fetchAvgL2Tps,
  fetchDashboardData,
} from '../services/apiService.ts';

const originalFetch = globalThis.fetch;

beforeEach(() => {
  vi.stubGlobal('window', {
    dispatchEvent: vi.fn(),
  });
});

// helper to create mock fetch response
function mockFetch(data: unknown, status = 200, ok = true) {
  return async () =>
    ({
      ok,
      status,
      json: async () => data,
    }) as Response;
}

describe('apiService', () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('fetchAvgProveTime succeeds', async () => {
    globalThis.fetch = mockFetch({ avg_prove_time_ms: 42 });
    const prove = await fetchAvgProveTime('1h');
    expect(prove.badRequest).toBe(false);
    expect(prove.error).toBeNull();
    expect(prove.data).toBe(42);
  });

  it('fetchAvgProveTime 15m succeeds', async () => {
    globalThis.fetch = mockFetch({ avg_prove_time_ms: 42 });
    const prove = await fetchAvgProveTime('15m');
    expect(prove.badRequest).toBe(false);
    expect(prove.error).toBeNull();
    expect(prove.data).toBe(42);
  });

  it('handles bad request for fetchAvgProveTime', async () => {
    globalThis.fetch = mockFetch({}, 400, false);
    const badProve = await fetchAvgProveTime('1h');
    expect(badProve.badRequest).toBe(true);
    expect(badProve.error).toStrictEqual({});
    expect(badProve.data).toBeNull();
  });

  it('transforms block times', async () => {
    globalThis.fetch = mockFetch({
      blocks: [
        { l2_block_number: 1, block_time: '1970-01-01T00:00:01Z', s_since_prev_block: 10 },
        { l2_block_number: 2, block_time: '1970-01-01T00:00:02Z', s_since_prev_block: 20 },
      ],
    });
    const blockTimes = await fetchL2BlockTimes('1h');
    expect(blockTimes.error).toBeNull();
    expect(blockTimes.data).toStrictEqual([
      { value: 2, timestamp: 20, blockTime: new Date('1970-01-01T00:00:02Z').getTime() },
    ]);
  });

  it('transforms block times for 15m', async () => {
    globalThis.fetch = mockFetch({
      blocks: [
        { l2_block_number: 1, block_time: '1970-01-01T00:00:01Z', s_since_prev_block: 10 },
        { l2_block_number: 2, block_time: '1970-01-01T00:00:02Z', s_since_prev_block: 20 },
      ],
    });
    const blockTimes = await fetchL2BlockTimes('15m');
    expect(blockTimes.error).toBeNull();
    expect(blockTimes.data).toStrictEqual([
      { value: 2, timestamp: 20, blockTime: new Date('1970-01-01T00:00:02Z').getTime() },
    ]);
  });

  it('transforms block transactions', async () => {
    globalThis.fetch = mockFetch({
      blocks: [{ block_number: 1, txs: 3, block_time: '1970-01-01T00:00:01Z' }],
    });
    const txs = await fetchBlockTransactions('1h');
    expect(txs.error).toBeNull();
    expect(txs.data).toStrictEqual([
      { block_number: 1, txs: 3, blockTime: new Date('1970-01-01T00:00:01Z').getTime() },
    ]);
  });

  it('transforms block transactions for 15m', async () => {
    globalThis.fetch = mockFetch({
      blocks: [{ block_number: 1, txs: 3, block_time: '1970-01-01T00:00:01Z' }],
    });
    const txs = await fetchBlockTransactions('15m');
    expect(txs.error).toBeNull();
    expect(txs.data).toStrictEqual([
      { block_number: 1, txs: 3, blockTime: new Date('1970-01-01T00:00:01Z').getTime() },
    ]);
  });

  it('fetchAvgL2Tps succeeds', async () => {
    globalThis.fetch = mockFetch({ avg_tps: 1.5 });
    const res = await fetchAvgL2Tps('1h');
    expect(res.badRequest).toBe(false);
    expect(res.error).toBeNull();
    expect(res.data).toBe(1.5);
  });

  it('fetchAvgL2Tps 15m succeeds', async () => {
    globalThis.fetch = mockFetch({ avg_tps: 1.5 });
    const res = await fetchAvgL2Tps('15m');
    expect(res.badRequest).toBe(false);
    expect(res.error).toBeNull();
    expect(res.data).toBe(1.5);
  });

  it('handles bad request for fetchAvgL2Tps', async () => {
    globalThis.fetch = mockFetch({}, 400, false);
    const res = await fetchAvgL2Tps('1h');
    expect(res.badRequest).toBe(true);
    expect(res.error).toStrictEqual({});
    expect(res.data).toBeNull();
  });

  it('fetchDashboardData returns data', async () => {
    globalThis.fetch = mockFetch({});
    const res = await fetchDashboardData('1h');
    expect(res.badRequest).toBe(false);
    expect(res.error).toBeNull();
    expect(res.data).toEqual({});
  });

  it('fetchDashboardData 15m returns data', async () => {
    globalThis.fetch = mockFetch({});
    const res = await fetchDashboardData('15m');
    expect(res.badRequest).toBe(false);
    expect(res.error).toBeNull();
    expect(res.data).toEqual({});
  });

  it('retries failed fetches and then throws', async () => {
    let attempts = 0;
    globalThis.fetch = vi.fn(async () => {
      attempts += 1;
      throw new Error('network');
    });

    await expect(fetchAvgProveTime('1h')).rejects.toThrow('network');
    expect(attempts).toBe(3); // initial try + two retries
  });

  it('shows toast on server error', async () => {
    const spy = vi.spyOn(toast, 'showToast').mockImplementation(() => { });
    globalThis.fetch = mockFetch({}, 500, false);
    const res = await fetchAvgProveTime('1h');
    expect(res.badRequest).toBe(false);
    expect(res.data).toBeNull();
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });

  it('shows toast on network error after retries', async () => {
    const spy = vi.spyOn(toast, 'showToast').mockImplementation(() => { });
    globalThis.fetch = vi.fn(async () => {
      throw new Error('offline');
    });
    await expect(fetchAvgProveTime('1h')).rejects.toThrow('offline');
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });

  it('waits when Retry-After header is provided', async () => {
    vi.useFakeTimers();
    let attempts = 0;
    const spy = vi.spyOn(toast, 'showToast').mockImplementation(() => { });
    globalThis.fetch = vi.fn(async () => {
      attempts += 1;
      if (attempts === 1) {
        return {
          ok: false,
          status: 429,
          headers: new Headers({ 'Retry-After': '2' }),
          json: async () => ({}),
        } as Response;
      }
      return {
        ok: true,
        status: 200,
        headers: new Headers(),
        json: async () => ({ avg_prove_time_ms: 7 }),
      } as Response;
    });

    const promise = fetchAvgProveTime('1h');
    await vi.advanceTimersByTimeAsync(2000);
    const res = await promise;
    expect(res.data).toBe(7);
    expect(attempts).toBe(2);
    expect(spy).toHaveBeenCalledWith('Too many requests, retrying in 2s.');
    spy.mockRestore();
    vi.useRealTimers();
  });
});
