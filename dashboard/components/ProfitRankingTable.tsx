import React from 'react';
import useSWR from 'swr';
import { TimeRange } from '../types';
import {
  fetchSequencerDistribution,
  type L2FeesComponentsResponse,
  type SequencerDistributionDataItem,
  fetchL2FeesComponents,
} from '../services/apiService';
// removed unused namespace import
import { getSequencerAddress, getSequencerName } from '../sequencerConfig';
import { addressLink, formatEth, formatDecimal } from '../utils';
import { calculateProfit, SEQUENCER_BASE_FEE_RATIO } from '../utils/profit';
import { useEthPrice } from '../services/priceService';
import { rangeToHours } from '../utils/timeRange';

interface ProfitRankingTableProps {
  timeRange: TimeRange;
  cloudCost: number;
  proverCost: number;
  feesData?: L2FeesComponentsResponse | null;
  sequencers?: SequencerDistributionDataItem[];
}

const formatUsd = (value: number): string => {
  const abs = Math.abs(value);
  if (abs >= 1000) {
    return Math.trunc(value).toLocaleString();
  }
  return value.toLocaleString(undefined, {
    minimumFractionDigits: 3,
    maximumFractionDigits: 3,
  });
};

export const ProfitRankingTable: React.FC<ProfitRankingTableProps> = ({
  timeRange,
  cloudCost,
  proverCost,
  feesData,
  sequencers: sequencersProp,
}) => {

  const sequencers = React.useMemo(() => {
    if (Array.isArray(sequencersProp) && sequencersProp.length > 0) {
      return sequencersProp;
    }
    return undefined;
  }, [sequencersProp]);

  const { data: distRes } = useSWR(
    sequencers ? null : ['profitRankingSeq', timeRange],
    () => fetchSequencerDistribution(timeRange),
  );
  const sequencersFetched = distRes?.data ?? [];
  const sequencersEffective = sequencers ?? sequencersFetched;

  const { data: ethPrice = 0 } = useEthPrice();
  // Fallback fetch for fees data when not provided via props
  const { data: feesRes } = useSWR(
    feesData === undefined ? ['l2FeesComponents', timeRange] : null,
    () => fetchL2FeesComponents(timeRange),
  );
  const effectiveFees = feesData ?? feesRes?.data ?? null;

  // Aggregate fees and costs by sequencer name from batch components to align with batch tables
  const feeAggByName = React.useMemo(() => {
    type Agg = {
      priority_fee: number;
      base_fee: number;
      l1_data_cost: number;
      prove_cost: number;
    };

    const byName = new Map<string, Agg>();
    // Build an address->name map from the distribution response to keep keys consistent with table rows
    const addrToName = new Map<string, string>();
    sequencersEffective.forEach((s: SequencerDistributionDataItem) => {
      addrToName.set(s.address.toLowerCase(), s.name);
    });

    // Primary source: per-batch components within the selected range
    effectiveFees?.batches.forEach((b) => {
      const name = addrToName.get(b.sequencer.toLowerCase()) ?? getSequencerName(b.sequencer);
      const cur = byName.get(name) ?? {
        priority_fee: 0,
        base_fee: 0,
        l1_data_cost: 0,
        prove_cost: 0,
      };
      cur.priority_fee += b.priority_fee ?? 0;
      cur.base_fee += b.base_fee ?? 0;
      cur.l1_data_cost += b.l1_data_cost ?? 0;
      cur.prove_cost += b.prove_cost ?? 0;
      byName.set(name, cur);
    });

    // Fallback: if batch rows are missing fields (e.g., in tests or partial data),
    // fill zeros from per-sequencer aggregates without double counting
    effectiveFees?.sequencers.forEach((f) => {
      const name = addrToName.get(f.address.toLowerCase()) ?? getSequencerName(f.address);
      const cur = byName.get(name) ?? {
        priority_fee: 0,
        base_fee: 0,
        l1_data_cost: 0,
        prove_cost: 0,
      };
      if ((cur.priority_fee ?? 0) === 0) cur.priority_fee = (f.priority_fee ?? 0);
      if ((cur.base_fee ?? 0) === 0) cur.base_fee = (f.base_fee ?? 0);
      if ((cur.l1_data_cost ?? 0) === 0) cur.l1_data_cost = (f.l1_data_cost ?? 0);
      if ((cur.prove_cost ?? 0) === 0) cur.prove_cost = (f.prove_cost ?? 0);
      byName.set(name, cur);
    });

    return byName;
  }, [effectiveFees, sequencersEffective]);

  // Note: batchCounts now comes directly from sequencer distribution data

  const [sortBy, setSortBy] = React.useState<
    'name' | 'blocks' | 'batches' | 'revenue' | 'cost' | 'profit' | 'ratio'
  >('ratio');
  const [sortDirection, setSortDirection] = React.useState<'asc' | 'desc'>(
    'desc',
  );

  const hours = rangeToHours(timeRange);
  const MONTH_HOURS = 30 * 24;
  const costPerSeqUsd = ((cloudCost + proverCost) / MONTH_HOURS) * hours;
  const costPerSeqEth = ethPrice && ethPrice > 0 ? costPerSeqUsd / ethPrice : 0;

  // Group distribution by name so duplicates are merged (e.g., Gattaca addresses)
  const distByName = React.useMemo(() => {
    const map = new Map<string, { name: string; address: string; blocks: number; batches: number }>();
    sequencersEffective.forEach((seq: SequencerDistributionDataItem) => {
      const name = seq.name;
      const repAddr = getSequencerAddress(name) || seq.address || '';
      const prev = map.get(name);
      if (prev) {
        prev.blocks += seq.value;
        prev.batches += seq.batches;
      } else {
        map.set(name, {
          name,
          address: repAddr,
          blocks: seq.value,
          batches: seq.batches,
        });
      }
    });
    return Array.from(map.values());
  }, [sequencersEffective]);

  const rows = distByName.map((seq) => {
    const addr = seq.address;
    const batchCount = seq.batches;
    const fees = feeAggByName.get(seq.name);
    const proveEth = ((fees?.prove_cost ?? 0) / 1e9);
    const extraEth = proveEth;
    const extraUsd = extraEth * ethPrice;
    if (!fees) {
      // If no fee data exists for this name, show N/A unless there are operational costs
      const hasCosts = costPerSeqEth > 0 || extraEth > 0;
      return {
        name: seq.name,
        address: addr,
        blocks: seq.blocks,
        batches: batchCount,
        revenueEth: null as number | null,
        revenueUsd: null as number | null,
        costEth: hasCosts ? costPerSeqEth + extraEth : null,
        costUsd: hasCosts ? costPerSeqUsd + extraUsd : null,
        profitEth: null as number | null,
        profitUsd: null as number | null,
        ratio: null as number | null,
      };
    }
    const revenueEth =
      ((fees.priority_fee ?? 0) + (fees.base_fee ?? 0) * SEQUENCER_BASE_FEE_RATIO) / 1e9;
    const l1CostEth = (fees.l1_data_cost ?? 0) / 1e9;
    const revenueUsd = revenueEth * ethPrice;
    const l1CostUsd = l1CostEth * ethPrice;
    const costEth = costPerSeqEth + l1CostEth + extraEth;
    const costUsd = costPerSeqUsd + l1CostUsd + extraUsd;
    const { profitEth, profitUsd } = calculateProfit({
      priorityFee: fees.priority_fee,
      baseFee: fees.base_fee,
      l1DataCost: fees.l1_data_cost,
      proveCost: fees.prove_cost,

      hardwareCostUsd: costPerSeqUsd,
      ethPrice,
    });
    const ratio = costEth > 0 ? revenueEth / costEth : null;
    return {
      name: seq.name,
      address: addr,
      blocks: seq.blocks,
      batches: batchCount,
      revenueEth,
      revenueUsd,
      costEth,
      costUsd,
      profitEth,
      profitUsd,
      ratio,
    };
  });

  const sorted = React.useMemo(() => {
    const data = [...rows];
    data.sort((a, b) => {
      const key =
        sortBy === 'revenue'
          ? 'revenueEth'
          : sortBy === 'cost'
            ? 'costEth'
            : sortBy === 'profit'
              ? 'profitEth'
              : sortBy === 'ratio'
                ? 'ratio'
                : sortBy;
      const aVal = (a as any)[key];
      const bVal = (b as any)[key];
      let cmp = 0;
      // Handle numeric columns (blocks, revenue, cost, profit and ratio) including null values
      if (
        sortBy === 'blocks' ||
        sortBy === 'batches' ||
        sortBy === 'revenue' ||
        sortBy === 'cost' ||
        sortBy === 'profit' ||
        sortBy === 'ratio'
      ) {
        const safeA = (typeof aVal === 'number' ? aVal : null) ?? -Infinity;
        const safeB = (typeof bVal === 'number' ? bVal : null) ?? -Infinity;
        cmp = safeA - safeB;
      } else {
        cmp = String(aVal).localeCompare(String(bVal), undefined, {
          numeric: true,
        });
      }
      return sortDirection === 'desc' ? -cmp : cmp;
    });
    return data;
  }, [rows, sortBy, sortDirection]);

  if (!effectiveFees) {
    return (
      <div className="flex items-center justify-center h-20 text-gray-500 dark:text-gray-400">
        Loading...
      </div>
    );
  }

  const handleSort = (
    column:
      | 'name'
      | 'blocks'
      | 'batches'
      | 'revenue'
      | 'cost'
      | 'profit'
      | 'ratio',
  ) => {
    if (sortBy === column) {
      setSortDirection((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortBy(column);
      setSortDirection('desc');
    }
  };

  return (
    <div className="mt-6">
      <h3 className="text-lg font-semibold mb-2">Sequencer Profit Ranking</h3>
      <div className="overflow-x-auto">
        <table className="min-w-full border border-gray-100 dark:border-[#475569] divide-y divide-gray-100 dark:divide-[#475569] bg-card dark:bg-[rgba(30,41,59,0.85)] text-card-fg">
          <thead>
            <tr>
              <th
                className="px-2 py-1 text-left cursor-pointer select-none"
                onClick={() => handleSort('name')}
              >
                Sequencer
                {sortBy === 'name' && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
              <th
                className="px-2 py-1 text-left cursor-pointer select-none"
                onClick={() => handleSort('blocks')}
              >
                Blocks
                {sortBy === 'blocks' && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
              <th
                className="px-2 py-1 text-left cursor-pointer select-none"
                onClick={() => handleSort('batches')}
              >
                Batches
                {sortBy === 'batches' && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
              <th
                className="px-2 py-1 text-left cursor-pointer select-none"
                onClick={() => handleSort('revenue')}
              >
                Revenue
                {sortBy === 'revenue' && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
              <th
                className="px-2 py-1 text-left cursor-pointer select-none"
                onClick={() => handleSort('cost')}
              >
                Cost
                {sortBy === 'cost' && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
              <th
                className="px-2 py-1 text-left cursor-pointer select-none"
                onClick={() => handleSort('profit')}
              >
                Profit
                {sortBy === 'profit' && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
              <th
                className="px-2 py-1 text-left cursor-pointer select-none"
                onClick={() => handleSort('ratio')}
              >
                Revenue-to-Cost Ratio
                {sortBy === 'ratio' && (
                  <span className="ml-1">
                    {sortDirection === 'asc' ? '↑' : '↓'}
                  </span>
                )}
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((row) => (
              <tr
                key={row.address}
                className="border-t border-gray-100 dark:border-[#475569]"
              >
                <td className="px-2 py-1">
                  {addressLink(row.address, row.name)}
                </td>
                <td className="px-2 py-1">{row.blocks.toLocaleString()}</td>
                <td className="px-2 py-1">
                  {row.batches.toLocaleString()}
                </td>
                <td
                  className="px-2 py-1"
                  title={
                    row.revenueUsd != null
                      ? `$${formatUsd(row.revenueUsd)}`
                      : undefined
                  }
                >
                  {row.revenueEth != null
                    ? formatEth(row.revenueEth * 1e9, 4)
                    : 'N/A'}
                </td>
                <td
                  className="px-2 py-1"
                  title={row.costUsd != null ? `$${formatUsd(row.costUsd)}` : undefined}
                >
                  {row.costEth != null ? formatEth(row.costEth * 1e9, 4) : 'N/A'}
                </td>
                <td
                  className="px-2 py-1"
                  title={
                    row.profitUsd != null
                      ? `$${formatUsd(row.profitUsd)}`
                      : undefined
                  }
                >
                  {row.profitEth != null
                    ? formatEth(row.profitEth * 1e9, 4)
                    : 'N/A'}
                </td>
                <td className="px-2 py-1">
                  {row.ratio != null ? formatDecimal(row.ratio) : 'N/A'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
