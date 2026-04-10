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
import useSWR from 'swr';
import { fetchL2FeesComponents } from '../services/apiService';
import { TimeRange, BatchFeeComponent } from '../types';
import { rangeToHours } from '../utils/timeRange';
import { useEthPrice } from '../services/priceService';
import { formatEth } from '../utils';

interface CostChartProps {
  timeRange: TimeRange;
  cloudCost: number;
  proverCost: number;
  address?: string;
  /** Total number of sequencers for scaling network-wide costs */
  totalSequencers?: number;
}

export const CostChart: React.FC<CostChartProps> = ({
  timeRange,
  cloudCost,
  proverCost,
  address,
  totalSequencers,
}) => {
  const { data: feeRes } = useSWR(
    ['l2FeesComponents', timeRange, address],
    () => fetchL2FeesComponents(timeRange),
  );
  const { data: ethPrice = 0 } = useEthPrice();
  const feeData: BatchFeeComponent[] | null =
    feeRes?.data?.batches
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
  const baseCostEth = ethPrice ? baseCostUsd / ethPrice : 0;
  const baseCostPerBatchEth = baseCostEth / feeData.length;

  const data = feeData.map((b) => {
    const l1CostEth = (b.l1Cost ?? 0) / 1e9;
    const proveEth = (b.proveCost ?? 0) / 1e9;
    const costEth = baseCostPerBatchEth + proveEth + l1CostEth;
    const costUsd = costEth * ethPrice;
    return { batch: b.batch, costEth, costUsd };
  });

  return (
    <>
      {/* Continue rendering; USD values may be zero when price missing */}
      <ResponsiveContainer width="100%" height={240}>
        <LineChart
          data={data}
          margin={{ top: 5, right: 20, left: 20, bottom: 40 }}
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
            tickFormatter={(v: number) => formatEth(v * 1e9, 3)}
            label={{
              value: 'Cost (ETH)',
              angle: -90,
              position: 'insideLeft',
              offset: -16,
              fontSize: 10,
              fill: 'var(--chart-tick)',
            }}
          />
          <Tooltip
            labelFormatter={(v: number) => `Batch ${v}`}
            formatter={(value: number, _name: string, { payload }: Payload<number, 'Cost'>) =>
              [`${formatEth(value * 1e9, 3)} ($${payload.costUsd.toFixed(3)})`, 'Cost']
            }
            contentStyle={{
              backgroundColor: 'var(--chart-tooltip-bg)',
              borderColor: 'var(--color-brand)',
            }}
            labelStyle={{ color: 'var(--chart-tooltip-label)' }}
          />
          <Line
            type="monotone"
            dataKey="costEth"
            stroke="#E573B5"
            strokeWidth={2}
            dot={false}
            name="Cost"
          />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
};
