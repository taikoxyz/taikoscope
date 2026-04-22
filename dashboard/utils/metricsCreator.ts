import { type MetricData } from '../types';
import {
  formatSeconds,
  formatSecondsWithDecimal,
  formatDecimal,
  formatEth,
  formatWithCommas,
  TAIKOSCAN_BASE,
} from '../utils';
import { getSequencerName } from '../sequencerConfig';

export interface MetricInputData {
  avgTps: number | null;
  l2Cadence: number | null;
  batchCadence: number | null;
  avgProve: number | null;
  currentOperator: string | null;
  nextOperator: string | null;
  l2Reorgs: number | null;
  slashings: number | null;
  forcedInclusions: number | null;
  failedProposals: number | null;
  l2Block: number | null;
  l1Block: number | null;
  priorityFee: number | null;
  baseFee: number | null;
  l1DataCost?: number | null;
  profit?: number | null;
  proveCost?: number | null;

}

export const createMetrics = (data: MetricInputData): MetricData[] => [
  {
    title: 'Avg. L2 TPS',
    value: data.avgTps != null ? formatDecimal(data.avgTps) : 'N/A',
    group: 'Network Performance',
  },
  {
    title: 'L2 Block Cadence',
    value:
      data.l2Cadence != null ? formatSecondsWithDecimal(data.l2Cadence / 1000) : 'N/A',
    group: 'Network Performance',
  },
  {
    title: 'Batch Posting Cadence',
    value:
      data.batchCadence != null
        ? formatSeconds(data.batchCadence / 1000)
        : 'N/A',
    group: 'Network Performance',
  },
  {
    title: 'Avg. Prove Time',
    value:
      data.avgProve != null && data.avgProve > 0
        ? formatSeconds(data.avgProve / 1000)
        : 'N/A',
    group: 'Network Health',
  },
  {
    title: 'Current Sequencer',
    value:
      data.currentOperator != null
        ? getSequencerName(data.currentOperator)
        : 'N/A',
    link:
      data.currentOperator != null
        ? `${TAIKOSCAN_BASE}/address/${data.currentOperator}`
        : undefined,
    group: 'Sequencers',
  },
  {
    title: 'Next Sequencer',
    value:
      data.nextOperator != null ? getSequencerName(data.nextOperator) : 'N/A',
    link:
      data.nextOperator != null
        ? `${TAIKOSCAN_BASE}/address/${data.nextOperator}`
        : undefined,
    group: 'Sequencers',
  },
  {
    title: 'L2 Reorgs',
    value: data.l2Reorgs != null ? formatWithCommas(data.l2Reorgs) : 'N/A',
    group: 'Network Health',
  },
  {
    title: 'Failed Proposals',
    value: data.failedProposals != null ? formatWithCommas(data.failedProposals) : 'N/A',
    group: 'Network Health',
  },
  {
    title: 'Slashing Events',
    value: data.slashings != null ? formatWithCommas(data.slashings) : 'N/A',
    group: 'Network Health',
  },
  {
    title: 'Forced Inclusions',
    value:
      data.forcedInclusions != null
        ? formatWithCommas(data.forcedInclusions)
        : 'N/A',
    group: 'Network Health',
  },
  {
    title: 'Net Sequencer Profit',
    value: data.profit != null ? formatEth(data.profit, 4) : 'N/A',
    group: 'Network Economics',
    tooltip: 'Sequencer profit minus subsidy.',
  },
  {
    title: 'Priority Fee',
    value: data.priorityFee != null ? formatEth(data.priorityFee, 4) : 'N/A',
    group: 'Network Economics',
  },
  {
    title: 'Base Fee',
    value: data.baseFee != null ? formatEth(data.baseFee, 4) : 'N/A',
    group: 'Network Economics',
  },
  {
    title: 'Proposing Cost',
    value: data.l1DataCost != null ? formatEth(data.l1DataCost, 4) : 'N/A',
    group: 'Network Economics',
  },
  {
    title: 'Prove Cost',
    value: data.proveCost != null ? formatEth(data.proveCost, 4) : 'N/A',
    group: 'Network Economics',
  },
  {
    title: 'L2 Block',
    value: data.l2Block != null ? data.l2Block.toLocaleString() : 'N/A',
    link:
      data.l2Block != null ? `${TAIKOSCAN_BASE}/block/${data.l2Block}` : undefined,
    group: 'Block Information',
  },
  {
    title: 'L1 Block',
    value: data.l1Block != null ? data.l1Block.toLocaleString() : 'N/A',
    link:
      data.l1Block != null ? `${TAIKOSCAN_BASE}/block/${data.l1Block}` : undefined,
    group: 'Block Information',
  },
];
