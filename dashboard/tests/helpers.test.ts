import { describe, it, expect } from 'vitest';
import { createMetrics, hasBadRequest } from '../helpers';

const metrics = createMetrics({
  avgTps: 3,
  l2Cadence: 60000,
  batchCadence: null,
  avgProve: 1200,
  activeGateways: 2,
  currentOperator: '0xabc',
  nextOperator: null,
  l2Reorgs: 1,
  slashings: null,
  forcedInclusions: 0,
  failedProposals: 0,
  l2Block: 100,
  l1Block: 50,
  priorityFee: 41e9,
  baseFee: 1e9,
  proveCost: 9e9,

  l1DataCost: 2e9,
  profit: 40e9,
});

const results = [
  { badRequest: false, data: null, error: null },
  { badRequest: true, data: null, error: null },
];

const metricsAllNull = createMetrics({
  avgTps: null,
  l2Cadence: null,
  batchCadence: null,
  avgProve: null,
  activeGateways: null,
  l2Reorgs: null,
  slashings: null,
  forcedInclusions: null,
  failedProposals: null,
  l2Block: null,
  l1Block: null,
  currentOperator: null,
  nextOperator: null,
  priorityFee: null,
  baseFee: null,
  proveCost: null,

  l1DataCost: null,
  profit: null,
});

describe('helpers', () => {
  it('creates metrics correctly', () => {
    expect(metrics[0].value).toBe('3.0');
    expect(metrics[0].group).toBe('Network Performance');
    expect(metrics[1].value).toBe('60.0s');
    expect(metrics[1].group).toBe('Network Performance');
    expect(metrics[2].value).toBe('N/A');
    expect(metrics[2].group).toBe('Network Performance');
    expect(metrics[3].value).toBe('1s');
    expect(metrics[3].group).toBe('Network Health');
    expect(metrics[4].value).toBe('2');
    expect(metrics[4].group).toBe('Sequencers');
    expect(metrics[5].value).toBe('0xabc');
    expect(metrics[5].group).toBe('Sequencers');
    expect(metrics[6].value).toBe('N/A');
    expect(metrics[6].group).toBe('Sequencers');
    expect(metrics[7].value).toBe('1');
    expect(metrics[7].group).toBe('Network Health');
    expect(metrics[8].value).toBe('0');
    expect(metrics[8].group).toBe('Network Health');
    expect(metrics[9].value).toBe('N/A');
    expect(metrics[9].group).toBe('Network Health');
    expect(metrics[10].value).toBe('0');
    expect(metrics[10].group).toBe('Network Health');
    expect(metrics[11].value).toBe('40 ETH');
    expect(metrics[11].group).toBe('Network Economics');
    expect(metrics[12].value).toBe('41 ETH');
    expect(metrics[12].group).toBe('Network Economics');
    expect(metrics[13].value).toBe('1 ETH');
    expect(metrics[13].group).toBe('Network Economics');
    expect(metrics[14].value).toBe('2 ETH');
    expect(metrics[14].group).toBe('Network Economics');
    expect(metrics[15].value).toBe('9 ETH');
    expect(metrics[15].group).toBe('Network Economics');
    expect(metrics[16].value).toBe('100');
    expect(metrics[16].link).toContain('/block/100');
    expect(metrics[16].group).toBe('Block Information');
    expect(metrics[17].value).toBe('50');
    expect(metrics[17].link).toContain('/block/50');
    expect(metrics[17].group).toBe('Block Information');
  });

  it('detects bad requests', () => {
    expect(hasBadRequest(results)).toBe(true);
    expect(
      hasBadRequest([{ badRequest: false, data: null, error: null }]),
    ).toBe(false);
  });

  it('handles null metrics', () => {
    for (const metric of metricsAllNull) {
      expect(metric.value).toBe('N/A');
    }
    expect(metricsAllNull[0].group).toBe('Network Performance');
    expect(metricsAllNull[1].group).toBe('Network Performance');
    expect(metricsAllNull[2].group).toBe('Network Performance');
    expect(metricsAllNull[3].group).toBe('Network Health');
    expect(metricsAllNull[4].group).toBe('Sequencers');
    expect(metricsAllNull[5].group).toBe('Sequencers');
    expect(metricsAllNull[6].group).toBe('Sequencers');
    expect(metricsAllNull[7].group).toBe('Network Health');
    expect(metricsAllNull[8].group).toBe('Network Health');
    expect(metricsAllNull[9].group).toBe('Network Health');
    expect(metricsAllNull[10].group).toBe('Network Health');
    expect(metricsAllNull[11].group).toBe('Network Economics');
    expect(metricsAllNull[12].group).toBe('Network Economics');
    expect(metricsAllNull[13].group).toBe('Network Economics');
    expect(metricsAllNull[14].group).toBe('Network Economics');
    expect(metricsAllNull[15].group).toBe('Network Economics');
    expect(metricsAllNull[16].group).toBe('Block Information');
    expect(metricsAllNull[17].group).toBe('Block Information');
  });

  it('handles all successful requests', () => {
    expect(
      hasBadRequest([
        { badRequest: false, data: null, error: null },
        { badRequest: false, data: null, error: null },
      ]),
    ).toBe(false);
  });
});
