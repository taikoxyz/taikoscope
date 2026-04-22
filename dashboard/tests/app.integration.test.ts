import { it, expect, vi, afterAll } from 'vitest';
import {
  fetchAvgProveTime,
  fetchL2BlockCadence,
  fetchBatchPostingCadence,
  fetchPreconfData,
  fetchL2Reorgs,
  fetchL2ReorgEvents,
  fetchSlashingEventCount,
  fetchForcedInclusionCount,
  fetchSlashingEvents,
  fetchForcedInclusionEvents,
  fetchL2HeadBlock,
  fetchL1HeadBlock,
  fetchL2HeadNumber,
  fetchL1HeadNumber,
  fetchProveTimes,
  fetchL2BlockTimes,
  fetchL2GasUsed,
  fetchSequencerDistribution,
  fetchL2FeesComponents,
  fetchDashboardData,
} from '../services/apiService.ts';
import { createMetrics, hasBadRequest } from '../helpers';
import type { MetricData } from '../types';

vi.useFakeTimers();
vi.setSystemTime(0);

type TimeRange = string;

type State = {
  metrics: MetricData[];
  secondsToProveData: unknown[];
  l2BlockTimeData: unknown[];
  l2GasUsedData: unknown[];
  sequencerDistribution: unknown[];
  l2ReorgEvents: unknown[];
  slashingEvents: unknown[];
  forcedInclusionEvents: unknown[];
  l2HeadBlock: string;
  l1HeadBlock: string;
  errorMessage: string;
};

interface MockFetchResponse {
  ok: boolean;
  json: () => Promise<unknown>;
}

const q15m = 'created%5Bgt%5D=-900000&created%5Blte%5D=0';
const q1h = 'created%5Bgt%5D=-3600000&created%5Blte%5D=0';
const q24h = 'created%5Bgt%5D=-86400000&created%5Blte%5D=0';

const dashboardDataFixture = {
  l2_block_cadence_ms: 60000,
  batch_posting_cadence_ms: 120000,
  avg_prove_time_ms: 1500,
  avg_verify_time_ms: 1800,
  avg_tps: 1.5,
  l2_reorgs: 0,
  slashings: 0,
  forced_inclusions: 0,
  failed_proposals: 0,
  l2_head_block: 1,
  l1_head_block: 1,
  preconf_data: {
    candidates: ['gw1', 'gw2'],
    current_operator: '0xaaa',
    next_operator: '0xbbb',
  },
};

const responses: Record<string, Record<string, unknown>> = {
  // Post-shasta, the single-value routes (avg-prove-time, avg-verify-time,
  // l2-block-cadence, batch-posting-cadence) were consolidated into
  // /dashboard-data. The dashboard fetchers now all delegate there.
  // The $q15m fixture lives further down because it pre-existed and the
  // app-rendering assertions are keyed to its specific values.
  [`/v1/dashboard-data?${q1h}`]: dashboardDataFixture,
  '/v1/preconf-data': {
    current_operator: '0xaaa',
    next_operator: '0xbbb',
  },
  [`/v1/reorgs?${q1h}`]: {
    events: [
      { from_block_number: 11, to_block_number: 10, depth: 1, inserted_at: '1970-01-01T00:00:00Z' },
    ],
  },
  [`/v1/slashings?${q1h}`]: {
    events: [{ l1_block_number: 5, validator_addr: [1, 2] }],
  },
  [`/v1/forced-inclusions?${q1h}`]: { events: [{ blob_hash: [3, 4] }] },
  [`/v1/reorgs?${q15m}`]: {
    events: [
      { from_block_number: 11, to_block_number: 10, depth: 1, inserted_at: '1970-01-01T00:00:00Z' },
    ],
  },
  [`/v1/slashings?${q15m}`]: {
    events: [{ l1_block_number: 5, validator_addr: [1, 2] }],
  },
  [`/v1/forced-inclusions?${q15m}`]: { events: [{ blob_hash: [3, 4] }] },
  [`/v1/l2-block-times?${q1h}&limit=50`]: {
    blocks: [
      { l2_block_number: 1, block_time: '1970-01-01T00:00:01Z', s_since_prev_block: 1 },
      { l2_block_number: 2, block_time: '1970-01-01T00:00:02Z', s_since_prev_block: 2 },
    ],
  },
  [`/v1/l2-block-times?${q15m}&limit=50`]: {
    blocks: [
      { l2_block_number: 1, block_time: '1970-01-01T00:00:01Z', s_since_prev_block: 1 },
      { l2_block_number: 2, block_time: '1970-01-01T00:00:02Z', s_since_prev_block: 2 },
    ],
  },
  [`/v1/l1-block-times?${q1h}`]: {
    blocks: [
      { l1_block_number: 50, minute: 1 },
      { l1_block_number: 52, minute: 2 },
    ],
  },
  [`/v1/l1-block-times?${q15m}`]: {
    blocks: [
      { l1_block_number: 50, minute: 1 },
      { l1_block_number: 52, minute: 2 },
    ],
  },
  [`/v1/prove-times?${q1h}&limit=50`]: {
    batches: [
      { batch_id: 1, seconds_to_prove: 1 },
      { batch_id: 2, seconds_to_prove: 2 },
    ],
  },
  [`/v1/prove-times?${q15m}&limit=50`]: {
    batches: [
      { batch_id: 1, seconds_to_prove: 1 },
      { batch_id: 2, seconds_to_prove: 2 },
    ],
  },

  [`/v1/l2-gas-used?${q1h}&limit=50`]: {
    blocks: [
      { l2_block_number: 1, block_time: '1970-01-01T00:00:01Z', gas_used: 100 },
      { l2_block_number: 2, block_time: '1970-01-01T00:00:02Z', gas_used: 150 },
    ],
  },
  [`/v1/l2-gas-used?${q15m}&limit=50`]: {
    blocks: [
      { l2_block_number: 1, block_time: '1970-01-01T00:00:01Z', gas_used: 100 },
      { l2_block_number: 2, block_time: '1970-01-01T00:00:02Z', gas_used: 150 },
    ],
  },
  [`/v1/l2-fees-components?${q1h}`]: {
    priority_fee: 600,
    base_fee: 400,
    l1_data_cost: 0,
    prove_cost: 5,
    sequencers: [],
    batches: [],
  },
  [`/v1/l2-fees-components?${q15m}`]: {
    priority_fee: 600,
    base_fee: 400,
    l1_data_cost: 0,
    prove_cost: 5,
    sequencers: [],
    batches: [],
  },
  [`/v1/sequencer-distribution?${q1h}`]: {
    sequencers: [{ address: 'addr1', blocks: 10 }],
  },
  [`/v1/sequencer-distribution?${q15m}`]: {
    sequencers: [{ address: 'addr1', blocks: 10 }],
  },
  '/v1/l2-head-block': { l2_head_block: 123 },
  '/v1/l1-head-block': { l1_head_block: 456 },
  [`/v1/dashboard-data?${q15m}`]: {
    l2_block_cadence_ms: 1,
    batch_posting_cadence_ms: 2,
    avg_prove_time_ms: 3,
    avg_tps: 5,
    preconf_data: {},
    l2_reorgs: 6,
    slashings: 7,
    forced_inclusions: 8,
    failed_proposals: 9,
    l2_head_block: 9,
    l1_head_block: 10,
  },
  [`/v1/l2-fees-components?${q24h}`]: {
    priority_fee: 1200,
    base_fee: 800,
    l1_data_cost: 0,
    prove_cost: 10,
    sequencers: [],
    batches: [],
  },
};

(
  globalThis as {
    fetch?: (url: string) => Promise<MockFetchResponse>;
  }
).fetch = async (url: string): Promise<MockFetchResponse> => {
  const u = new URL(url, 'http://localhost');
  const key = u.pathname + (u.search ? `?${u.searchParams.toString()}` : '');
  return {
    ok: true,
    json: async () => responses[key] ?? {},
  };
};

interface IntervalId {
  fn: () => Promise<void> | void;
  ms: number;
}
let intervals: IntervalId[] = [];
(
  globalThis as unknown as {
    setInterval: (fn: () => Promise<void> | void, ms: number) => NodeJS.Timeout;
  }
).setInterval = (
  fn: () => Promise<void> | void,
  ms: number,
): NodeJS.Timeout => {
    const id: IntervalId = { fn, ms };
    intervals.push(id);
    return id as unknown as NodeJS.Timeout;
  };
(
  globalThis as unknown as { clearInterval: (id: NodeJS.Timeout) => void }
).clearInterval = (id: NodeJS.Timeout) => {
  intervals = intervals.filter((i) => i !== (id as unknown as IntervalId));
};

async function fetchData(range: TimeRange, state: State, economics = false) {
  if (economics) {
    const [l2FeesRes, l2BlockRes, l1BlockRes] = await Promise.all([
      fetchL2FeesComponents(range),
      fetchL2HeadBlock(range),
      fetchL1HeadBlock(range),
    ]);

    const l2FeeData = l2FeesRes.data;
    const priorityFee = l2FeeData?.priority_fee ?? null;
    const baseFee = l2FeeData?.base_fee ?? null;
    const l2Block = l2BlockRes.data;
    const l1Block = l1BlockRes.data;

    const anyBadRequest = hasBadRequest([l2FeesRes, l2BlockRes, l1BlockRes]);

    const currentMetrics: MetricData[] = createMetrics({
      avgTps: 3,
      l2Cadence: null,
      batchCadence: null,
      avgProve: null,
      currentOperator: null,
      nextOperator: null,
      l2Reorgs: null,
      slashings: null,
      forcedInclusions: null,
      failedProposals: null,
      priorityFee,
      baseFee,
      l2Block,
      l1Block,
    });

    state.metrics = currentMetrics;
    state.l2HeadBlock =
      currentMetrics.find((m) => m.title === 'L2 Head Block')?.value || 'N/A';
    state.l1HeadBlock =
      currentMetrics.find((m) => m.title === 'L1 Head Block')?.value || 'N/A';
    state.errorMessage = anyBadRequest
      ? 'Invalid parameters provided. Some data may not be available.'
      : '';
    return;
  }
  const [
    l2CadenceRes,
    batchCadenceRes,
    avgProveRes,
    preconfRes,
    l2ReorgsRes,
    l2ReorgEventsRes,
    slashingCountRes,
    forcedInclusionCountRes,
    slashingEventsRes,
    forcedInclusionEventsRes,
    l2BlockRes,
    l1BlockRes,
    proveTimesRes,
    l2TimesRes,
    l2GasUsedRes,
    sequencerDistRes,
    l2FeesRes,
  ] = await Promise.all([
    fetchL2BlockCadence(range, undefined),
    fetchBatchPostingCadence(range),
    fetchAvgProveTime(range),
    fetchPreconfData(range),
    fetchL2Reorgs(range),
    fetchL2ReorgEvents(range),
    fetchSlashingEventCount(range),
    fetchForcedInclusionCount(range),
    fetchSlashingEvents(range),
    fetchForcedInclusionEvents(range),
    fetchL2HeadBlock(range),
    fetchL1HeadBlock(range),
    fetchProveTimes(range),
    fetchL2BlockTimes(range, undefined),
    fetchL2GasUsed(range, undefined),
    fetchSequencerDistribution(range),
    fetchL2FeesComponents(range),
  ]);

  const l2Cadence = l2CadenceRes.data;
  const batchCadence = batchCadenceRes.data;
  const avgProve = avgProveRes.data;
  const preconfData = preconfRes.data;
  const currentOperator = preconfData?.current_operator ?? null;
  const nextOperator = preconfData?.next_operator ?? null;
  const l2Reorgs = l2ReorgsRes.data;
  const reorgEvents = l2ReorgEventsRes.data || [];
  const slashings = slashingCountRes.data;
  const forcedInclusions = forcedInclusionCountRes.data;
  const slashingEventsData = slashingEventsRes.data || [];
  const forcedInclusionEventsData = forcedInclusionEventsRes.data || [];
  const l2Block = l2BlockRes.data;
  const l1Block = l1BlockRes.data;
  const proveTimes = proveTimesRes.data || [];
  const l2Times = l2TimesRes.data || [];
  const l2Gas = l2GasUsedRes.data || [];
  const sequencerDist = sequencerDistRes.data || [];
  const l2FeeData = l2FeesRes.data;
  const priorityFee = l2FeeData?.priority_fee ?? null;
  const baseFee = l2FeeData?.base_fee ?? null;

  const anyBadRequest = hasBadRequest([
    l2CadenceRes,
    batchCadenceRes,
    avgProveRes,
    preconfRes,
    l2ReorgsRes,
    l2ReorgEventsRes,
    slashingCountRes,
    forcedInclusionCountRes,
    l2BlockRes,
    l1BlockRes,
    proveTimesRes,
    l2TimesRes,
    l2GasUsedRes,
    sequencerDistRes,
    l2FeesRes,
  ]);

  const currentMetrics: MetricData[] = createMetrics({
    avgTps: 3,
    l2Cadence,
    batchCadence,
    avgProve,
    currentOperator,
    nextOperator,
    l2Reorgs,
    slashings,
    forcedInclusions,
    failedProposals: null,
    priorityFee,
    baseFee,
    l2Block,
    l1Block,
  });

  state.metrics = currentMetrics;
  state.secondsToProveData = proveTimes;
  state.l2BlockTimeData = l2Times;
  state.l2GasUsedData = l2Gas;
  state.sequencerDistribution = sequencerDist;
  state.slashingEvents = slashingEventsData;
  state.forcedInclusionEvents = forcedInclusionEventsData;
  state.l2ReorgEvents = reorgEvents;
  state.l2HeadBlock =
    currentMetrics.find((m) => m.title === 'L2 Block')?.value || 'N/A';
  state.l1HeadBlock =
    currentMetrics.find((m) => m.title === 'L1 Block')?.value || 'N/A';
  if (anyBadRequest) {
    state.errorMessage =
      'Invalid parameters provided. Some data may not be available.';
  } else {
    state.errorMessage = '';
  }
}

async function updateHeads(state: State) {
  const [l1, l2] = await Promise.all([
    fetchL1HeadNumber(),
    fetchL2HeadNumber(),
  ]);
  if (l1.data !== null) {
    const value = l1.data.toLocaleString();
    state.l1HeadBlock = value;
    state.metrics = state.metrics.map((m) =>
      m.title === 'L1 Block' ? { ...m, value } : m,
    );
  }
  if (l2.data !== null) {
    const value = l2.data.toLocaleString();
    state.l2HeadBlock = value;
    state.metrics = state.metrics.map((m) =>
      m.title === 'L2 Block' ? { ...m, value } : m,
    );
  }
}

function setupPolling(state: State) {
  updateHeads(state);
  return setInterval(() => updateHeads(state), 60000);
}

it('app integration', async () => {
  const state: State = {
    metrics: [],
    secondsToProveData: [],
    l2BlockTimeData: [],
    l2GasUsedData: [],
    sequencerDistribution: [],
    l2ReorgEvents: [],
    slashingEvents: [],
    forcedInclusionEvents: [],
    l2HeadBlock: '0',
    l1HeadBlock: '0',
    errorMessage: '',
  };

  await fetchData('15m', state);
  expect(state.metrics.length > 0).toBe(true);

  await fetchData('1h', state);
  expect(state.metrics.length > 0).toBe(true);
  expect(state.secondsToProveData.length).toBe(2);
  expect(state.l2GasUsedData.length).toBe(2);

  await fetchData('15m', state, true);
  await fetchData('1h', state, true);
  const econMetric = state.metrics.find((m) => m.group === 'Network Economics');
  expect(econMetric).toBeDefined();

  setupPolling(state);
  expect(intervals.length).toBe(1);
  await intervals[0].fn();
  expect(state.l1HeadBlock).toBe('456');
  expect(state.l2HeadBlock).toBe('123');

  const grouped = state.metrics.reduce<Record<string, MetricData[]>>(
    (acc, m) => {
      const g = m.group ?? 'Other';
      if (!acc[g]) acc[g] = [];
      acc[g].push(m);
      return acc;
    },
    {},
  );
  const groupOrder = [
    'Network Performance',
    'Network Health',
    'Sequencers',
    'Other',
  ];
  const visible = groupOrder.filter((g) => grouped[g] && grouped[g].length > 0);
  const expected = ['Network Performance', 'Network Health', 'Sequencers'];
  expect(visible).toStrictEqual(expected);

  console.log('App integration tests passed.');
});

afterAll(() => {
  vi.useRealTimers();
});

it('fetches dashboard data correctly', async () => {
  const result = await fetchDashboardData('15m');
  expect(result.data).toEqual({
    l2_block_cadence_ms: 1,
    batch_posting_cadence_ms: 2,
    avg_prove_time_ms: 3,
    avg_tps: 5,
    preconf_data: {},
    l2_reorgs: 6,
    slashings: 7,
    forced_inclusions: 8,
    failed_proposals: 9,
    l2_head_block: 9,
    l1_head_block: 10,
  });
});

it('fetches prove times correctly', async () => {
  const result = await fetchProveTimes('15m');
  expect(result.data).toEqual([
    { name: '1', value: 1, timestamp: 0 },
    { name: '2', value: 2, timestamp: 0 },
  ]);
});
