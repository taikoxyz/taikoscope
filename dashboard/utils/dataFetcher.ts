import { TimeRange, type TimeSeriesData } from '../types';
import { getSequencerAddress } from '../sequencerConfig';
import { normalizeTimeRange } from './timeRange';
import {
  fetchDashboardData,
  fetchProveTimesAggregated,
  fetchL2BlockTimesAggregated,
  fetchL2GasUsedAggregated,
  fetchSequencerDistribution,
  fetchBlockTransactionsAggregated,
  fetchBatchBlobCountsAggregated,
  fetchL2FeesComponents,
  fetchL2HeadNumber,
  fetchL1HeadNumber,
  type PreconfData,
  type SequencerDistributionDataItem,
  type BlockTransaction,
  type BatchBlobCount,
  type RequestResult,
} from '../services/apiService';

export interface MainDashboardData {
  l2Cadence: number | null;
  batchCadence: number | null;
  avgProve: number | null;
  avgTps: number | null;
  preconfData: PreconfData | null;
  l2Reorgs: number | null;
  slashings: number | null;
  forcedInclusions: number | null;
  failedProposals: number | null;
  l2Block: number | null;
  l1Block: number | null;
  proveTimes: TimeSeriesData[];
  l2Times: TimeSeriesData[];
  l2Gas: TimeSeriesData[];
  sequencerDist: SequencerDistributionDataItem[];
  txPerBlock: BlockTransaction[];
  blobsPerBatch: BatchBlobCount[];
  badRequestResults: RequestResult<unknown>[];
}

export interface EconomicsData {
  priorityFee: number | null;
  baseFee: number | null;
  l1DataCost: number;
  proveCost: number;
  l2Block: number | null;
  l1Block: number | null;
  sequencerDist: SequencerDistributionDataItem[];
  badRequestResults: RequestResult<unknown>[];
}

export const fetchMainDashboardData = async (
  timeRange: TimeRange,
  selectedSequencer: string | null,
): Promise<MainDashboardData> => {
  const normalizedRange = normalizeTimeRange(timeRange);
  const address = selectedSequencer
    ? getSequencerAddress(selectedSequencer)
    : undefined;

  function safe<T>(pr: Promise<RequestResult<T>>): Promise<RequestResult<T>> {
    return pr.catch(() => ({ data: null, badRequest: false, error: null }));
  }

  const [
    dashboardRes,
    proveTimesRes,
    l2TimesRes,
    l2GasUsedRes,
    sequencerDistRes,
    blockTxRes,
    batchBlobCountsRes,
  ] = await Promise.all([
    safe(fetchDashboardData(normalizedRange, address)),
    safe(fetchProveTimesAggregated(normalizedRange)),
    safe(fetchL2BlockTimesAggregated(normalizedRange, address)),
    safe(fetchL2GasUsedAggregated(normalizedRange, address)),
    safe(fetchSequencerDistribution(normalizedRange)),
    safe(fetchBlockTransactionsAggregated(normalizedRange, address)),
    safe(fetchBatchBlobCountsAggregated(normalizedRange)),
  ]);

  const data = dashboardRes.data;

  const allResults = [
    dashboardRes,
    proveTimesRes,
    l2TimesRes,
    l2GasUsedRes,
    sequencerDistRes,
    blockTxRes,
    batchBlobCountsRes,
  ];

  return {
    l2Cadence: data?.l2_block_cadence_ms ?? null,
    batchCadence: data?.batch_posting_cadence_ms ?? null,
    avgProve: data?.avg_prove_time_ms ?? null,
    avgTps: data?.avg_tps ?? null,
    preconfData: data?.preconf_data ?? null,
    l2Reorgs: data?.l2_reorgs ?? null,
    slashings: data?.slashings ?? null,
    forcedInclusions: data?.forced_inclusions ?? null,
    failedProposals: data?.failed_proposals ?? null,
    l2Block: data?.l2_head_block ?? null,
    l1Block: data?.l1_head_block ?? null,
    proveTimes: proveTimesRes.data || [],
    l2Times: l2TimesRes.data || [],
    l2Gas: l2GasUsedRes.data || [],
    sequencerDist: sequencerDistRes.data || [],
    txPerBlock: blockTxRes.data || [],
    blobsPerBatch: batchBlobCountsRes.data || [],
    badRequestResults: allResults.slice(1),
  };
};

export const fetchEconomicsData = async (
  timeRange: TimeRange,
  _selectedSequencer: string | null,
): Promise<EconomicsData> => {
  void _selectedSequencer;
  const normalizedRange = normalizeTimeRange(timeRange);
  const [feesSummaryRes, l2BlockRes, l1BlockRes, distRes] = await Promise.all([
    fetchL2FeesComponents(normalizedRange),
    fetchL2HeadNumber(),
    fetchL1HeadNumber(),
    fetchSequencerDistribution(normalizedRange),
  ]);

  const sequencerDist = distRes.data
    ? distRes.data
    : [];

  return {
    priorityFee: feesSummaryRes.data?.priority_fee ?? null,
    baseFee: feesSummaryRes.data?.base_fee ?? null,
    l1DataCost: feesSummaryRes.data?.l1_data_cost ?? 0,
    proveCost: feesSummaryRes.data?.prove_cost ?? 0,

    l2Block: l2BlockRes.data,
    l1Block: l1BlockRes.data,
    sequencerDist,
    badRequestResults: [feesSummaryRes, l2BlockRes, l1BlockRes, distRes],
  };
};
