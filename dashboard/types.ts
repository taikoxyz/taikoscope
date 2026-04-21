export type TimeRange = string;

export interface TimeSeriesData {
  timestamp: number; // Unix timestamp (ms)
  value: number;
  blockTime?: number;
  name?: string; // For line charts with 'name' on x-axis (like batchId)
}

export interface PieChartDataItem {
  name: string;
  value: number;
  fill?: string; // Color fill is now optional, can be assigned by the chart component
}

import type { ReactNode } from 'react';
import type { BlockTransaction, BatchBlobCount } from './services/apiService';

export interface MetricData {
  title: ReactNode;
  value: string;
  link?: string;
  unit?: string; // e.g., '1h', '24h', or specific units like 'ms'
  description?: ReactNode;
  group?: string;
  tooltip?: string;
}

export interface L2ReorgEvent {
  from_block_number: number;
  to_block_number: number;
  depth: number;
  old_sequencer: string;
  new_sequencer: string;
  timestamp: number;
}

export interface MissedBlockProposal {
  slot: number;
}

export interface SlashingEvent {
  l1_block_number: number;
  validator_addr: number[];
}

export interface ForcedInclusionEvent {
  blob_hash: number[];
}

export interface FailedProposalEvent {
  batch_id: number;
  original_sequencer: string;
  proposer: string;
  l1_block_number: number;
  timestamp: number;
}

export interface ErrorResponse {
  type: string;
  title: string;
  status: number;
  detail: string;
}

export interface FeeComponent {
  block: number;
  priority: number;
  base: number;
  l1Cost: number | null;
}

export interface BatchFeeComponent {
  batch: number;
  txHash: string;
  sequencer: string;
  priority: number;
  base: number;
  l1Cost: number | null;
  proveCost: number | null;
}

export interface BlockProfit {
  block: number;
  profit: number;
}

export interface ChartsDataUpdate {
  proveTimes?: TimeSeriesData[];
  l2Times?: TimeSeriesData[];
  l2Gas?: TimeSeriesData[];
  txPerBlock?: BlockTransaction[];
  blobsPerBatch?: BatchBlobCount[];
  sequencerDist?: PieChartDataItem[];
}

export interface ChartsData {
  secondsToProveData: TimeSeriesData[];
  l2BlockTimeData: TimeSeriesData[];
  l2GasUsedData: TimeSeriesData[];
  blockTxData: BlockTransaction[];
  batchBlobCounts: BatchBlobCount[];
  sequencerDistribution: PieChartDataItem[];
  updateChartsData: (data: ChartsDataUpdate) => void;
}

export interface MetricsDataState {
  metrics: MetricData[];
  setMetrics: (metrics: MetricData[]) => void;
  loadingMetrics: boolean;
  setLoadingMetrics: (v: boolean) => void;
  errorMessage: string;
  setErrorMessage: (msg: string) => void;
  isEconomicsView: boolean;
}

export interface BlockDataState {
  l2HeadBlock: string;
  l1HeadBlock: string;
  updateBlockHeads: () => Promise<void>;
  updateMetricsWithBlockHeads: (metrics: MetricData[]) => MetricData[];
}

export interface RefreshTimerState {
  refreshRate: number;
  setRefreshRate: (rate: number) => void;
  lastRefresh: number;
  updateLastRefresh: () => void;
}
