import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import type { Payload } from 'recharts/types/component/DefaultTooltipContent';
import { useEthPrice } from '../services/priceService';
import useSWR from 'swr';
import { fetchL2FeesComponents, type L2FeesComponentsResponse } from '../services/apiService';
import { TimeRange, BatchFeeComponent } from '../types';
import { rangeToHours } from '../utils/timeRange';
import { formatEth } from '../utils';
import { getSequencerName } from '../sequencerConfig';
import { SEQUENCER_BASE_FEE_RATIO } from '../utils/profit';

interface EconomicsChartProps {
  timeRange: TimeRange;
  cloudCost: number;
  proverCost: number;
  address?: string;
  /** Total number of sequencers for scaling network-wide costs */
  totalSequencers?: number;
  /** Pre-fetched L2 fees + components data to avoid duplicate requests */
  feesData?: L2FeesComponentsResponse | null;
}

export const EconomicsChart: React.FC<EconomicsChartProps> = ({
  timeRange,
  cloudCost,
  proverCost,
  address,
  totalSequencers,
  feesData,
}) => {
  // Fallback fetch when feesData not provided via props
  const { data: feesRes } = useSWR(
    feesData === undefined ? ['l2FeesComponents', timeRange] : null,
    () => fetchL2FeesComponents(timeRange),
  );
  const effectiveFees = feesData ?? feesRes?.data ?? null;
  const feeData: BatchFeeComponent[] | null =
    effectiveFees?.batches
      ?.filter((b) => !address || b.sequencer === address)
      .map((b) => ({
        batch: b.batch_id,
        txHash: b.l1_tx_hash,
        sequencer: b.sequencer,
        priority: b.priority_fee,
        base: b.base_fee,
        l1Cost: b.l1_data_cost,
        proveCost: b.prove_cost,
      })) ?? null;
  const { data: ethPrice = 0 } = useEthPrice();

  if (!feeData || feeData.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-500">
        No data available
      </div>
    );
  }

  const hours = rangeToHours(timeRange);
  const HOURS_IN_MONTH = 30 * 24;
  const seqCount = address ? 1 : Math.max(1, totalSequencers ?? 1);
  const baseCostUsd = ((cloudCost + proverCost) * seqCount) / HOURS_IN_MONTH * hours;
  const baseCostPerBatchUsd = baseCostUsd / feeData.length;
  const baseCostPerBatchEth = ethPrice ? baseCostPerBatchUsd / ethPrice : 0;

  const data = feeData.map((b) => {
    const revenueEth = (b.priority + b.base * SEQUENCER_BASE_FEE_RATIO) / 1e9;
    const proveEth = (b.proveCost ?? 0) / 1e9;
    const costEth = baseCostPerBatchEth + proveEth + (b.l1Cost ?? 0) / 1e9;
    const profitEth = revenueEth - costEth;
    const revenueUsd = revenueEth * ethPrice;
    const costUsd = costEth * ethPrice;
    const profitUsd = profitEth * ethPrice;
    return {
      batch: b.batch,
      sequencer: b.sequencer,
      revenueEth,
      costEth,
      profitEth,
      revenueUsd,
      costUsd,
      profitUsd,
    };
  });

  return (
    <>
      {/* If ETH price is missing we still render using 0-derived ETH and USD-only tooltips */}
      <ResponsiveContainer width="100%" height={240}>
        <LineChart
          data={data}
          margin={{ top: 15, right: 20, left: 20, bottom: 20 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
          <XAxis
            dataKey="batch"
            stroke="#666666"
            fontSize={12}
            label={{
              value: 'Batch',
              position: 'insideBottom',
              offset: -10,
              fontSize: 10,
              fill: '#666666',
            }}
          />
          <YAxis
            stroke="#666666"
            fontSize={12}
            domain={['auto', 'auto']}
            tickFormatter={(v: number) => formatEth(v * 1e9, 4)}
            label={{
              value: 'ETH',
              angle: -90,
              position: 'insideLeft',
              offset: -16,
              fontSize: 10,
              fill: '#666666',
            }}
          />
          <Tooltip
            labelFormatter={(v: number, payload) => {
              const seq = payload?.[0]?.payload?.sequencer as string | undefined;
              const label = seq ? getSequencerName(seq) : undefined;
              return label ? `Batch ${v} - ${label}` : `Batch ${v}`;
            }}
            formatter={(value: number, name: string, { payload }: Payload<number, string>) => {
              if (name === 'Revenue')
                return [
                  `${formatEth(value * 1e9, 4)} ($${payload.revenueUsd.toFixed(3)})`,
                  name,
                ];
              if (name === 'Cost')
                return [
                  `${formatEth(value * 1e9, 4)} ($${payload.costUsd.toFixed(3)})`,
                  name,
                ];
              return [
                `${formatEth(value * 1e9, 4)} ($${payload.profitUsd.toFixed(3)})`,
                name,
              ];
            }}
            contentStyle={{
              backgroundColor: 'rgba(255,255,255,0.8)',
              borderColor: '#8884d8',
            }}
            labelStyle={{ color: '#333' }}
          />
          <Line
            type="monotone"
            dataKey="revenueEth"
            stroke="#4E79A7"
            strokeWidth={2}
            dot={false}
            name="Revenue"
          />
          <Line
            type="monotone"
            dataKey="costEth"
            stroke="#E573B5"
            strokeWidth={2}
            dot={false}
            name="Cost"
          />
          <Line
            type="monotone"
            dataKey="profitEth"
            stroke="#8884d8"
            strokeWidth={2}
            dot={false}
            name="Profit"
          />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
};
