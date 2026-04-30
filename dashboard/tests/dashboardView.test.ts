import { describe, expect, it, vi } from 'vitest';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { MemoryRouter } from 'react-router-dom';
import { SWRConfig } from 'swr';
import { DashboardView } from '../components/views/DashboardView';
import { ThemeProvider } from '../contexts/ThemeContext';
import type { ChartsData, MetricData } from '../types';

vi.mock('../components/ChartCard', () => ({
  ChartCard: ({ title }: { title: string }) =>
    React.createElement('section', null, title),
}));

vi.mock('../services/priceService', () => ({
  useEthPrice: () => ({ data: 0 }),
}));

const metrics: MetricData[] = [
  {
    title: 'Avg. L2 TPS',
    value: '5',
    group: 'Network Performance',
  },
  {
    title: 'Avg. Prove Time',
    value: '3s',
    group: 'Network Health',
  },
  {
    title: 'L2 Reorgs',
    value: '0',
    group: 'Network Health',
  },
  {
    title: 'Current Sequencer',
    value: 'Nethermind A',
    group: 'Sequencers',
  },
  {
    title: 'Next Sequencer',
    value: 'Chainbound B',
    group: 'Sequencers',
  },
  {
    title: 'Net Sequencer Profit',
    value: '1 ETH',
    group: 'Network Economics',
  },
];

const chartsData: ChartsData = {
  secondsToProveData: [],
  l2BlockTimeData: [],
  l2GasUsedData: [],
  blockTxData: [],
  batchBlobCounts: [],
  sequencerDistribution: [
    { name: 'Nethermind A', value: 10, address: '0x123' } as any,
  ],
  updateChartsData: () => {},
};

const countText = (html: string, text: string): number =>
  html.split(text).length - 1;

const renderView = (view: string): string =>
  renderToStaticMarkup(
    React.createElement(
      ThemeProvider,
      null,
      React.createElement(
        SWRConfig,
        { value: { provider: () => new Map() } },
        React.createElement(
          MemoryRouter,
          { initialEntries: [`/?view=${view}`] },
          React.createElement(DashboardView, {
            timeRange: '24h',
            selectedSequencer: null,
            metricsData: {
              metrics,
              loadingMetrics: false,
              errorMessage: '',
              setErrorMessage: () => {},
            },
            chartsData,
            isLoadingData: false,
            isTimeRangeChanging: false,
            onOpenTable: () => {},
            onOpenTpsTable: () => {},
            onOpenSequencerDistributionTable: () => {},
          }),
        ),
      ),
    ),
  );

describe('DashboardView', () => {
  it('renders network performance and sequencer panels on the performance page', () => {
    const html = renderView('performance');

    expect(html).toContain('Network Performance');
    expect(html).toContain('Avg. L2 TPS');
    expect(html).toContain('Sequencers');
    expect(html).toContain('Current Sequencer');
    expect(html).toContain('Next Sequencer');
    expect(html).toContain('Sequencer Distribution');
    expect(html).not.toContain('Network Health');
    expect(html).not.toContain('Network Economics');
    expect(countText(html, 'Avg Gas Used Per Block')).toBe(1);
    expect(countText(html, 'Avg Tx Count Per L2 Block')).toBe(1);
    expect(countText(html, 'Sequencer Distribution')).toBe(1);
    expect(countText(html, 'Avg Prove Time')).toBe(0);
  });

  it('renders only network health panels on the health page', () => {
    const html = renderView('health');

    expect(html).toContain('Network Health');
    expect(html).toContain('Avg. Prove Time');
    expect(html).toContain('L2 Reorgs');
    expect(html).not.toContain('Network Performance');
    expect(html).not.toContain('Sequencers');
    expect(html).not.toContain('Network Economics');
    expect(countText(html, 'Avg Prove Time')).toBe(1);
    expect(countText(html, 'Avg Blobs per Batch')).toBe(1);
    expect(countText(html, 'L2 Block Time Distribution')).toBe(1);
    expect(countText(html, 'Avg Gas Used Per Block')).toBe(0);
    expect(countText(html, 'Sequencer Distribution')).toBe(0);
  });

  it('renders only economics panels on the economics page', () => {
    const html = renderView('economics');

    expect(html).toContain('Network Economics');
    expect(html).toContain('Net Sequencer Profit');
    expect(html).not.toContain('Network Performance');
    expect(html).not.toContain('Network Health');
    expect(html).not.toContain('Sequencers');
    expect(countText(html, 'Avg Gas Used Per Block')).toBe(0);
    expect(countText(html, 'Avg Prove Time')).toBe(0);
    expect(countText(html, 'Sequencer Distribution')).toBe(0);
  });
});
