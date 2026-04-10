const metaEnv = import.meta.env as any;
export const API_BASE: string =
  (metaEnv.VITE_API_BASE ?? metaEnv.API_BASE ?? '') + '/v1';

import { getSequencerName } from '../sequencerConfig';
import { showToast } from '../utils/toast';
import { timeRangeToQuery, rangeToQuery } from '../utils/timeRange';

import type {
  TimeSeriesData,
  L2ReorgEvent,
  SlashingEvent,
  ForcedInclusionEvent,
  FailedProposalEvent,
  ErrorResponse,
  TimeRange,
} from '../types';

export interface SequencerDistributionDataItem {
  name: string;
  address: string;
  value: number;
  batches: number;
  tps: number | null;
}

export interface RequestResult<T> {
  data: T | null;
  badRequest: boolean;
  error: ErrorResponse | null;
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface FetchOptions {
  retries?: number;
  retryDelay?: number;
  timeout?: number;
}

const fetchJson = async <T>(
  url: string,
  { retries = 2, retryDelay = 500, timeout = 10_000 }: FetchOptions = {},
): Promise<RequestResult<T>> => {
  const buildUrl = (u: string) => {
    if (/^https?:\/\//.test(u)) return u;
    // Node.js fetch (used in Vitest/Jest) requires absolute URLs.
    // When running in a non-browser env, prefix with localhost so relative paths work.
    if (typeof window === 'undefined') {
      return `http://localhost${u.startsWith('/') ? '' : '/'}${u}`;
    }
    return u;
  };

  const absoluteUrl = buildUrl(url);
  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
      const res = await fetch(absoluteUrl, { signal: controller.signal });
      clearTimeout(id);
      if (!res.ok) {
        if (res.status === 429) {
          const retryAfterHeader =
            res.headers.get('retry-after') ?? res.headers.get('RETRY_AFTER');
          if (retryAfterHeader) {
            const retrySecs = parseFloat(retryAfterHeader);
            if (!Number.isNaN(retrySecs) && retrySecs > 0 && attempt < retries) {
              showToast(`Too many requests, retrying in ${retrySecs}s.`);
              await wait(retrySecs * 1000);
              continue;
            }
          }
          showToast('Too many requests, please slow down.');
        } else if (res.status >= 500) {
          showToast('Server error, please try again later.');
        }
        let error: ErrorResponse | null = null;
        try {
          error = (await res.json()) as ErrorResponse;
        } catch {
          // ignore JSON parse errors
        }
        return { data: null, badRequest: res.status === 400, error };
      }
      return { data: (await res.json()) as T, badRequest: false, error: null };
    } catch (error: unknown) {
      clearTimeout(id);
      if (attempt < retries) {
        console.warn(`Failed to fetch ${url}, retrying...`, error);
        await wait(retryDelay * (attempt + 1));
        continue;
      }
      console.error(`Failed to fetch ${url}`, error);
      showToast('Network error, please check your connection.');
      throw error;
    }
  }
  throw new Error('unreachable');
};

export interface AvgTimeResponse {
  avg_prove_time_ms?: number;
}

export const fetchAvgProveTime = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/avg-prove-time?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ avg_prove_time_ms?: number }>(url);
  return {
    data: res.data?.avg_prove_time_ms ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL2BlockCadence = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<number>> => {
  const url =
    `${API_BASE}/l2-block-cadence?${timeRangeToQuery(range)}` +
    (address ? `&address=${address}` : '');
  const res = await fetchJson<{ l2_block_cadence_ms?: number }>(url);
  return {
    data: res.data?.l2_block_cadence_ms ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchBatchPostingCadence = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/batch-posting-cadence?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ batch_posting_cadence_ms?: number }>(url);
  return {
    data: res.data?.batch_posting_cadence_ms ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchActiveSequencerAddresses = async (
  range: TimeRange = '1h',
): Promise<RequestResult<string[]>> => {
  const res = await fetchPreconfData(range);
  return {
    data: res.data?.candidates ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL2Reorgs = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/reorgs?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ events: unknown[] }>(url);
  return {
    data: res.data ? res.data.events.length : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL2ReorgEvents = async (
  range: TimeRange,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<L2ReorgEvent[]>> => {
  // Always use explicit time bounds; paginate by shifting created window markers.
  const baseParams = new URLSearchParams(timeRangeToQuery(range));
  baseParams.set('limit', String(limit));

  if (startingAfter !== undefined) {
    // Move the upper bound down exclusively to avoid duplicates
    baseParams.delete('created[lte]');
    baseParams.set('created[lt]', String(startingAfter));
  } else if (endingBefore !== undefined) {
    // Move the lower bound up exclusively to avoid duplicates
    baseParams.delete('created[gt]');
    baseParams.set('created[gt]', String(endingBefore));
  }

  const url = `${API_BASE}/reorgs?${baseParams.toString()}`;
  const res = await fetchJson<{
    events: {
      from_block_number: number;
      to_block_number: number;
      depth: number;
      old_sequencer: string;
      new_sequencer: string;
      inserted_at: string;
    }[];
  }>(url);
  return {
    data: res.data?.events
      ? res.data.events.map((e) => ({
        from_block_number: e.from_block_number,
        to_block_number: e.to_block_number,
        depth: e.depth,
        old_sequencer: e.old_sequencer,
        new_sequencer: e.new_sequencer,
        timestamp: Date.parse(e.inserted_at),
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchSlashingEventCount = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/slashings?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ events: unknown[] }>(url);
  return {
    data: res.data ? res.data.events.length : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchForcedInclusionCount = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/forced-inclusions?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ events: unknown[] }>(url);
  return {
    data: res.data ? res.data.events.length : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchSlashingEvents = async (
  range: TimeRange,
): Promise<RequestResult<SlashingEvent[]>> => {
  const url = `${API_BASE}/slashings?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ events: SlashingEvent[] }>(url);
  return {
    data: res.data ? res.data.events : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchForcedInclusionEvents = async (
  range: TimeRange,
): Promise<RequestResult<ForcedInclusionEvent[]>> => {
  const url = `${API_BASE}/forced-inclusions?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ events: ForcedInclusionEvent[] }>(url);
  return {
    data: res.data ? res.data.events : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchFailedProposalCount = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/failed-proposals?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ events: unknown[] }>(url);
  return {
    data: res.data ? res.data.events.length : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchFailedProposalEvents = async (
  range: TimeRange,
  limit = 50,
  startingAfterTs?: number,
  endingBeforeTs?: number,
  startingAfterBatch?: number,
  endingBeforeBatch?: number,
): Promise<RequestResult<FailedProposalEvent[]>> => {
  // Stable pagination using (timestamp, batch_id) cursor.
  // Server supports combining time window with batch_id cursor for tie-breaks.
  const baseParams = new URLSearchParams(timeRangeToQuery(range));
  baseParams.set('limit', String(limit));

  if (startingAfterTs !== undefined) {
    // Include rows at the same timestamp by using lte bound and tie-break on batch_id
    baseParams.set('created[lte]', String(startingAfterTs));
    if (startingAfterBatch !== undefined) {
      baseParams.set('starting_after', String(startingAfterBatch));
    }
  } else if (endingBeforeTs !== undefined) {
    // For previous page, move lower bound just before the first row's timestamp (ts-1)
    const gt = Math.max(0, endingBeforeTs - 1);
    baseParams.set('created[gt]', String(gt));
    if (endingBeforeBatch !== undefined) {
      baseParams.set('ending_before', String(endingBeforeBatch));
    }
  }

  const url = `${API_BASE}/failed-proposals?${baseParams.toString()}`;
  const res = await fetchJson<{
    events: {
      batch_id: number;
      original_sequencer: string;
      proposer: string;
      l1_block_number: number;
      inserted_at: string;
    }[];
  }>(url);
  return {
    data: res.data?.events
      ? res.data.events.map((e) => ({
        batch_id: e.batch_id,
        original_sequencer: e.original_sequencer,
        proposer: e.proposer,
        l1_block_number: e.l1_block_number,
        timestamp: Date.parse(e.inserted_at),
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL2HeadBlock = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/l2-block-times?${timeRangeToQuery(range)}&limit=50`;
  const res = await fetchJson<{ blocks: { l2_block_number: number }[] }>(url);
  const value =
    res.data && res.data.blocks.length > 0
      ? res.data.blocks[res.data.blocks.length - 1].l2_block_number
      : null;
  return { data: value, badRequest: res.badRequest, error: res.error };
};

export const fetchL1HeadBlock = async (
  range: TimeRange,
): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/l1-block-times?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ blocks: { l1_block_number: number }[] }>(url);
  const value =
    res.data && res.data.blocks.length > 0
      ? res.data.blocks[res.data.blocks.length - 1].l1_block_number
      : null;
  return { data: value, badRequest: res.badRequest, error: res.error };
};

export interface PreconfData {
  candidates: string[];
  current_operator?: string;
  next_operator?: string;
}

export const fetchPreconfData = async (
  _range: TimeRange = '1h',
): Promise<RequestResult<PreconfData>> => {
  void _range;
  const url = `${API_BASE}/preconf-data`;
  const res = await fetchJson<PreconfData>(url);
  return {
    data: res.data ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL2HeadNumber = async (): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/l2-head-block`;
  const res = await fetchJson<{ l2_head_block?: number }>(url);
  return {
    data: res.data?.l2_head_block ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL1HeadNumber = async (): Promise<RequestResult<number>> => {
  const url = `${API_BASE}/l1-head-block`;
  const res = await fetchJson<{ l1_head_block?: number }>(url);
  return {
    data: res.data?.l1_head_block ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchProveTimes = async (
  range: TimeRange,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<TimeSeriesData[]>> => {
  let url = `${API_BASE}/prove-times?`;
  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }
  if (startingAfter !== undefined) {
    url += `&starting_after=${startingAfter}`;
  } else if (endingBefore !== undefined) {
    url += `&ending_before=${endingBefore}`;
  }
  const res = await fetchJson<{
    batches: { batch_id: number; seconds_to_prove: number }[];
  }>(url);
  return {
    data: res.data
      ? res.data.batches.map((b) => ({
        name: b.batch_id.toString(),
        value: b.seconds_to_prove,
        timestamp: 0,
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchProveTimesAggregated = async (
  range: TimeRange,
): Promise<RequestResult<TimeSeriesData[]>> => {
  const url = `${API_BASE}/prove-times?${timeRangeToQuery(range)}&aggregated`;
  const res = await fetchJson<{
    batches: { batch_id: number; seconds_to_prove: number }[];
  }>(url);
  return {
    data: res.data
      ? res.data.batches.map((b) => ({
        name: b.batch_id.toString(),
        value: b.seconds_to_prove,
        timestamp: 0,
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL1BlockTimes = async (
  range: TimeRange,
): Promise<RequestResult<TimeSeriesData[]>> => {
  const url = `${API_BASE}/l1-block-times?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{
    blocks: { minute: number; l1_block_number: number }[];
  }>(url);
  if (!res.data) {
    return { data: null, badRequest: res.badRequest, error: res.error };
  }

  const blocks = res.data.blocks.map((b) => ({
    ts: b.minute * 1000,
    block: b.l1_block_number,
  }));

  const data = blocks
    .slice(1)
    .map((b, i): TimeSeriesData | null => {
      const prev = blocks[i];
      const deltaBlocks = b.block - prev.block;
      if (deltaBlocks <= 0) {
        return null;
      }
      const deltaTimeMs = b.ts - prev.ts;
      const interval = deltaTimeMs / deltaBlocks;
      return { timestamp: interval, value: b.block };
    })
    .filter((d): d is TimeSeriesData => d !== null);

  return { data, badRequest: res.badRequest, error: res.error };
};

export const fetchL2BlockTimes = async (
  range: TimeRange,
  address?: string,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<TimeSeriesData[]>> => {
  let url = `${API_BASE}/l2-block-times?`;
  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }
  if (startingAfter !== undefined) {
    url += `&starting_after=${startingAfter}`;
  } else if (endingBefore !== undefined) {
    url += `&ending_before=${endingBefore}`;
  }
  if (address) {
    url += `&address=${address}`;
  }
  const res = await fetchJson<{
    blocks: {
      l2_block_number: number;
      block_time: string;
      s_since_prev_block: number;
    }[];
  }>(url);
  if (!res.data) {
    return { data: null, badRequest: res.badRequest, error: res.error };
  }

  // Only drop the first element on the first page (when no pagination params)
  // This ensures we don't have gaps in pagination
  const blocksToProcess = (startingAfter === undefined && endingBefore === undefined)
    ? res.data.blocks.slice(1)
    : res.data.blocks;

  const data = blocksToProcess.map((b) => ({
    value: b.l2_block_number,
    timestamp: b.s_since_prev_block,
    blockTime: new Date(b.block_time).getTime(),
  }));

  return { data, badRequest: res.badRequest, error: res.error };
};

export const fetchL2BlockTimesAggregated = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<TimeSeriesData[]>> => {
  const url =
    `${API_BASE}/l2-block-times?${timeRangeToQuery(range)}&aggregated` +
    (address ? `&address=${address}` : '');
  const res = await fetchJson<{
    blocks: {
      l2_block_number: number;
      block_time: string;
      s_since_prev_block: number;
    }[];
  }>(url);
  if (!res.data) {
    return { data: null, badRequest: res.badRequest, error: res.error };
  }

  const data = res.data.blocks.slice(1).map(
    (b): TimeSeriesData => ({
      value: b.l2_block_number,
      timestamp: b.s_since_prev_block,
      blockTime: new Date(b.block_time).getTime(),
    }),
  );

  return { data, badRequest: res.badRequest, error: res.error };
};

export const fetchBatchPostingTimes = async (
  range: TimeRange,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<TimeSeriesData[]>> => {
  let url = `${API_BASE}/batch-posting-times?`;
  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }
  if (startingAfter !== undefined) {
    url += `&starting_after=${startingAfter}`;
  } else if (endingBefore !== undefined) {
    url += `&ending_before=${endingBefore}`;
  }
  const res = await fetchJson<{
    batches: { batch_id: number; ms_since_prev_batch: number }[];
  }>(url);
  if (!res.data) {
    return { data: null, badRequest: res.badRequest, error: res.error };
  }
  const data = res.data.batches.map(
    (b): TimeSeriesData => ({
      value: b.batch_id,
      timestamp: b.ms_since_prev_batch / 1000,
    }),
  );
  return { data, badRequest: res.badRequest, error: res.error };
};

export const fetchL2GasUsed = async (
  range: TimeRange,
  address?: string,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<TimeSeriesData[]>> => {
  let url = `${API_BASE}/l2-gas-used?`;
  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }
  if (startingAfter !== undefined) {
    url += `&starting_after=${startingAfter}`;
  } else if (endingBefore !== undefined) {
    url += `&ending_before=${endingBefore}`;
  }
  if (address) {
    url += `&address=${address}`;
  }
  const res = await fetchJson<{
    blocks: { l2_block_number: number; block_time: string; gas_used: number }[];
  }>(url);
  return {
    data: res.data
      ? res.data.blocks.map((b) => ({
        value: b.l2_block_number,
        timestamp: b.gas_used,
        blockTime: new Date(b.block_time).getTime(),
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchL2GasUsedAggregated = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<TimeSeriesData[]>> => {
  const url =
    `${API_BASE}/l2-gas-used?${timeRangeToQuery(range)}&aggregated` +
    (address ? `&address=${address}` : '');
  const res = await fetchJson<{
    blocks: { l2_block_number: number; block_time: string; gas_used: number }[];
  }>(url);
  return {
    data: res.data
      ? res.data.blocks.map((b) => ({
        value: b.l2_block_number,
        timestamp: b.gas_used,
        blockTime: new Date(b.block_time).getTime(),
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchSequencerDistribution = async (
  range: TimeRange,
): Promise<RequestResult<SequencerDistributionDataItem[]>> => {
  const url = `${API_BASE}/sequencer-distribution?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{
    sequencers: { address: string; blocks: number; batches: number; tps: number | null }[];
  }>(url);
  return {
    data: res.data
      ? res.data.sequencers.map((s) => ({
        name: getSequencerName(s.address),
        address: s.address,
        value: s.blocks,
        batches: s.batches,
        tps: s.tps,
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchSequencerBlocks = async (
  range: TimeRange,
  address: string,
): Promise<RequestResult<number[]>> => {
  const url = `${API_BASE}/sequencer-blocks?${timeRangeToQuery(range)}&address=${address}`;
  const res = await fetchJson<{
    sequencers: { address: string; blocks: number[] }[];
  }>(url);
  const blocks = res.data?.sequencers.find(
    (s) => s.address.toLowerCase() === address.toLowerCase(),
  )?.blocks;
  return { data: blocks ?? null, badRequest: res.badRequest, error: res.error };
};

export interface BlockTransaction {
  block_number: number;
  txs: number;
  blockTime: number;
}

export const fetchBlockTransactions = async (
  range: TimeRange,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
  address?: string,
  unlimited = false,
): Promise<RequestResult<BlockTransaction[]>> => {
  let url = `${API_BASE}/block-transactions?`;

  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }

  // For unlimited fetching, we ignore pagination parameters to get all data
  if (!unlimited) {
    if (startingAfter !== undefined) {
      url += `&starting_after=${startingAfter}`;
    } else if (endingBefore !== undefined) {
      url += `&ending_before=${endingBefore}`;
    }
  }

  if (address) {
    url += `&address=${address}`;
  }
  const res = await fetchJson<{
    blocks: {
      block_number: number;
      txs: number;
      block_time: string;
    }[];
  }>(url);
  return {
    data: res.data?.blocks
      ? res.data.blocks.map((b) => ({
        block_number: b.block_number,
        txs: b.txs,
        blockTime: new Date(b.block_time).getTime(),
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

// New function specifically for fetching all block transactions in a time range
// This will be used by both charts and tables to ensure data consistency
export const fetchAllBlockTransactions = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<BlockTransaction[]>> => {
  return fetchBlockTransactions(
    range,
    undefined,
    undefined,
    undefined,
    address,
    true,
  );
};

export const fetchBlockTransactionsAggregated = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<BlockTransaction[]>> => {
  const url =
    `${API_BASE}/block-transactions?${timeRangeToQuery(range)}&aggregated` +
    (address ? `&address=${address}` : '');
  const res = await fetchJson<{
    blocks: {
      block_number: number;
      txs: number;
      block_time: string;
    }[];
  }>(url);
  return {
    data: res.data?.blocks
      ? res.data.blocks.map((b) => ({
        block_number: b.block_number,
        txs: b.txs,
        blockTime: new Date(b.block_time).getTime(),
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export interface BatchBlobCount {
  block_number: number;
  batch: number;
  blobs: number;
}

export const fetchBatchBlobCounts = async (
  range: TimeRange,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<BatchBlobCount[]>> => {
  let url = `${API_BASE}/blobs-per-batch?`;
  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }
  if (startingAfter !== undefined) {
    url += `&starting_after=${startingAfter}`;
  } else if (endingBefore !== undefined) {
    url += `&ending_before=${endingBefore}`;
  }
  const res = await fetchJson<{
    batches: {
      l1_block_number?: number;
      batch_id: number;
      blob_count: number;
    }[];
  }>(url);
  return {
    data: res.data
      ? res.data.batches.map((b) => ({
        block_number: b.l1_block_number ?? b.batch_id, // Fallback to batch_id for backward compatibility
        batch: b.batch_id,
        blobs: b.blob_count,
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchBatchBlobCountsAggregated = async (
  range: TimeRange,
): Promise<RequestResult<BatchBlobCount[]>> => {
  const url = `${API_BASE}/blobs-per-batch?${timeRangeToQuery(range)}&aggregated`;
  const res = await fetchJson<{
    batches: {
      l1_block_number?: number;
      batch_id: number;
      blob_count: number;
    }[];
  }>(url);
  return {
    data: res.data
      ? res.data.batches.map((b) => ({
        block_number: b.l1_block_number ?? b.batch_id, // Fallback to batch_id for backward compatibility
        batch: b.batch_id,
        blobs: b.blob_count,
      }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export const fetchAvgL2Tps = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<number>> => {
  const url =
    `${API_BASE}/avg-l2-tps?${timeRangeToQuery(range)}` +
    (address ? `&address=${address}` : '');
  const res = await fetchJson<{ avg_tps?: number }>(url);
  return {
    data: res.data?.avg_tps ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export interface SequencerFee {
  address: string;
  priority_fee: number;
  base_fee: number;
  l1_data_cost: number;
  prove_cost: number;
}

// Removed fetchL2Fees in favor of fetchL2FeesComponents

export interface FeeComponent {
  block_number: number;
  priority: number;
  base: number;
  l1Cost: number | null;
}

export interface L2FeesComponentsResponse {
  priority_fee: number | null;
  base_fee: number | null;
  l1_data_cost: number;
  prove_cost: number;
  sequencers: SequencerFee[];
  batches: {
    batch_id: number;
    l1_block_number: number;
    l1_tx_hash: string;
    sequencer: string;
    priority_fee: number;
    base_fee: number;
    l1_data_cost: number | null;
    prove_cost: number | null;
  }[];
}

export const fetchL2FeesComponents = async (
  range: TimeRange,
): Promise<RequestResult<L2FeesComponentsResponse>> => {
  const url = `${API_BASE}/l2-fees-components?${timeRangeToQuery(range)}`;
  const res = await fetchJson<L2FeesComponentsResponse>(url);
  return {
    data: res.data ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};

export interface L1DataCostItem {
  block_number: number;
  cost: number;
}

export interface SequencerCostItem {
  address: string;
  cost: number;
}

export const fetchL1DataCost = async (
  range: TimeRange,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<L1DataCostItem[]>> => {
  let url = `${API_BASE}/l1-data-cost?`;
  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }
  if (startingAfter !== undefined) {
    url += `&starting_after=${startingAfter}`;
  } else if (endingBefore !== undefined) {
    url += `&ending_before=${endingBefore}`;
  }
  const res = await fetchJson<{
    blocks: { l1_block_number: number; cost: number }[];
  }>(url);
  return {
    data: res.data
      ? res.data.blocks.map((b) => ({ block_number: b.l1_block_number, cost: b.cost }))
      : null,
    badRequest: res.badRequest,
    error: res.error,
  };
};


export const fetchProveCostsByProposer = async (
  range: TimeRange,
): Promise<RequestResult<SequencerCostItem[]>> => {
  const url = `${API_BASE}/prove-costs?${timeRangeToQuery(range)}`;
  const res = await fetchJson<{ proposers: SequencerCostItem[] }>(url);
  return {
    data: res.data?.proposers ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};



export const fetchL2Tps = async (
  range: TimeRange,
  address?: string,
  limit = 50,
  startingAfter?: number,
  endingBefore?: number,
): Promise<RequestResult<{ block_number: number; tps: number }[]>> => {
  let url = `${API_BASE}/l2-tps?`;
  if (startingAfter === undefined && endingBefore === undefined) {
    url += `${timeRangeToQuery(range)}&limit=${limit}`;
  } else {
    url += `${rangeToQuery(range)}&limit=${limit}`;
  }
  if (startingAfter !== undefined) {
    url += `&starting_after=${startingAfter}`;
  } else if (endingBefore !== undefined) {
    url += `&ending_before=${endingBefore}`;
  }
  if (address) {
    url += `&address=${address}`;
  }
  const res = await fetchJson<{
    blocks: { l2_block_number: number; tps: number }[];
  }>(url);

  if (!res.data) {
    return { data: null, badRequest: res.badRequest, error: res.error };
  }

  const data = res.data.blocks.map((b) => ({
    block_number: b.l2_block_number,
    tps: b.tps,
  }));

  return { data, badRequest: res.badRequest, error: res.error };
};

export const fetchL2TpsAggregated = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<{ block_number: number; tps: number }[]>> => {
  const url =
    `${API_BASE}/l2-tps?${timeRangeToQuery(range)}&aggregated` +
    (address ? `&address=${address}` : '');
  const res = await fetchJson<{
    blocks: { l2_block_number: number; tps: number }[];
  }>(url);

  if (!res.data) {
    return { data: null, badRequest: res.badRequest, error: res.error };
  }

  const data = res.data.blocks.map((b) => ({
    block_number: b.l2_block_number,
    tps: b.tps,
  }));

  return { data, badRequest: res.badRequest, error: res.error };
};

// removed: fetchBlockProfits (endpoint removed)

export interface DashboardDataResponse {
  l2_block_cadence_ms: number | null;
  batch_posting_cadence_ms: number | null;
  avg_prove_time_ms: number | null;
  avg_tps: number | null;
  preconf_data: PreconfData | null;
  l2_reorgs: number;
  slashings: number;
  forced_inclusions: number;
  failed_proposals: number;
  l2_head_block: number | null;
  l1_head_block: number | null;
}

export const fetchDashboardData = async (
  range: TimeRange,
  address?: string,
): Promise<RequestResult<DashboardDataResponse>> => {
  const url =
    `${API_BASE}/dashboard-data?${timeRangeToQuery(range)}` +
    (address ? `&address=${address}` : '');
  const res = await fetchJson<DashboardDataResponse>(url);
  return {
    data: res.data ?? null,
    badRequest: res.badRequest,
    error: res.error,
  };
};
