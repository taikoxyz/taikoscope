import { describe, it, expect } from 'vitest';
import { createMetrics } from '../utils/metricsCreator';

const addressA = '0x00a00800c28f2616360dcfadee02d761d14ad94e';
const addressB = '0x00b00194cdc219921784ab1eb4eaa9634fe1f1a8';

describe('metricsCreator', () => {
  it('creates properly formatted metrics', () => {
    const metrics = createMetrics({
      avgTps: 1.234,
      l2Cadence: 60000,
      batchCadence: 30000,
      avgProve: 2000,
      currentOperator: addressA,
      nextOperator: addressB,
      l2Reorgs: 1,
      slashings: 2,
      forcedInclusions: 3,
      failedProposals: 4,
      priorityFee: 40e9,
      baseFee: 2e9,
      proveCost: 5e9,

      l1DataCost: 3e9,
      profit: 39e9,
      l2Block: 100,
      l1Block: 50,
    });

    expect(metrics).toHaveLength(17);
    expect(metrics[0].value).toBe('1.2');
    expect(metrics.find((m) => m.title === 'Active Sequencers')).toBeUndefined();

    const proveMetric = metrics.find((m) => m.title === 'Avg. Prove Time');
    expect(proveMetric?.value).toBe('2s');

    const proveCostMetric = metrics.find((m) => m.title === 'Prove Cost');
    expect(proveCostMetric?.value).toBe('5 ETH');

    const profitMetric = metrics.find((m) => m.title === 'Net Sequencer Profit');
    expect(profitMetric?.value).toBe('39 ETH');
    expect(profitMetric?.tooltip).toBe('Sequencer profit minus subsidy.');

    const current = metrics.find((m) => m.title === 'Current Sequencer');
    const next = metrics.find((m) => m.title === 'Next Sequencer');
    expect(current?.value).toBe('Chainbound A');
    expect(current?.link).toContain('/address/');
    expect(next?.value).toBe('Chainbound B');
    expect(next?.link).toContain('/address/');

    const l2BlockMetric = metrics.find((m) => m.title === 'L2 Block');
    expect(l2BlockMetric?.link).toContain('/block/100');
    const l1BlockMetric = metrics.find((m) => m.title === 'L1 Block');
    expect(l1BlockMetric?.link).toContain('/block/50');
  });

  it('falls back to N/A for missing values', () => {
    const metrics = createMetrics({
      avgTps: null,
      l2Cadence: null,
      batchCadence: null,
      avgProve: null,
      currentOperator: null,
      nextOperator: null,
      l2Reorgs: null,
      slashings: null,
      forcedInclusions: null,
      failedProposals: null,
      priorityFee: null,
      baseFee: null,
      proveCost: null,

      l1DataCost: null,
      profit: null,
      l2Block: null,
      l1Block: null,
    });

    for (const metric of metrics) {
      expect(metric.value).toBe('N/A');
    }
  });
});
