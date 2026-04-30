import React, { useCallback, lazy, useState } from 'react';
import { ErrorDisplay } from '../layout/ErrorDisplay';
import { MetricsGrid } from '../layout/MetricsGrid';
import { ProfitCalculator } from '../ProfitCalculator';
import { EconomicsChart } from '../EconomicsChart';
import { ProfitRankingTable } from '../ProfitRankingTable';
import { BlockProfitTables } from '../BlockProfitTables';
import { FeeFlowChart } from '../FeeFlowChart';
import { ChartCard } from '../ChartCard';
// brand color exposed via chartTheme
import { chartSeries } from '../charts/chartTheme';
import { TimeRange, MetricData, ChartsData } from '../../types';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { DEFAULT_VIEW } from '../../constants';
import { useEthPrice } from '../../services/priceService';
import { rangeToHours } from '../../utils/timeRange';
import { calculateHardwareCost } from '../../utils/hardwareCost';
import { formatEth, parseEthValue } from '../../utils';
import useSWR from 'swr';
import { fetchL2FeesComponents } from '../../services/apiService';

const SequencerPieChart = lazy(() =>
  import('../SequencerPieChart').then((m) => ({
    default: m.SequencerPieChart,
  })),
);
const BlockTimeDistributionChart = lazy(() =>
  import('../BlockTimeDistributionChart').then((m) => ({
    default: m.BlockTimeDistributionChart,
  })),
);
const BatchProcessChart = lazy(() =>
  import('../BatchProcessChart').then((m) => ({
    default: m.BatchProcessChart,
  })),
);
const GasUsedChart = lazy(() =>
  import('../GasUsedChart').then((m) => ({
    default: m.GasUsedChart,
  })),
);
const BlockTxChart = lazy(() =>
  import('../BlockTxChart').then((m) => ({
    default: m.BlockTxChart,
  })),
);
const BlobsPerBatchChart = lazy(() =>
  import('../BlobsPerBatchChart').then((m) => ({
    default: m.BlobsPerBatchChart,
  })),
);

interface DashboardViewProps {
  timeRange: TimeRange;
  selectedSequencer: string | null;

  // Data hooks
  metricsData: {
    metrics: MetricData[];
    loadingMetrics: boolean;
    errorMessage: string;
    setErrorMessage: (msg: string) => void;
  };
  chartsData: ChartsData;

  // Loading states
  isLoadingData: boolean;
  isTimeRangeChanging: boolean;

  // Actions
  onOpenTable: (table: string, timeRange?: TimeRange) => void;
  onOpenTpsTable: () => void;
  onOpenSequencerDistributionTable: (
    timeRange: TimeRange,
    page: number,
  ) => void;
}

export const DashboardView: React.FC<DashboardViewProps> = ({
  timeRange,
  selectedSequencer,
  metricsData,
  chartsData,
  isLoadingData,
  isTimeRangeChanging,
  onOpenTable,
  onOpenTpsTable,
  onOpenSequencerDistributionTable,
}) => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const viewParam = searchParams.get('view') ?? DEFAULT_VIEW;
  const isEconomicsView = viewParam === 'economics';
  const isHealthView = viewParam === 'health';
  const isPerformanceView = viewParam === 'performance';
  const isMainDashboardView = isEconomicsView || isHealthView || isPerformanceView;
  // Default monthly costs in USD
  const [cloudCost, setCloudCost] = useState(0);
  const [proverCost, setProverCost] = useState(0);

  const { data: ethPrice = 0 } = useEthPrice();
  const { data: l2FeesRes } = useSWR(
    isEconomicsView ? ['l2FeesComponents', timeRange] : null,
    () => fetchL2FeesComponents(timeRange),
  );
  const feesData = l2FeesRes?.data ?? null;
  const metricsWithHardware = React.useMemo(() => {
    if (!isEconomicsView) return metricsData.metrics;
    const hours = rangeToHours(timeRange);
    const sequencerCount = chartsData.sequencerDistribution.length || 1;
    const { totalUsd: costUsd } = calculateHardwareCost(
      cloudCost,
      proverCost,
      sequencerCount,
      hours,
    );
    const costWei = ethPrice > 0 ? (costUsd / ethPrice) * 1e9 : null;
    const list = [...metricsData.metrics];

    if (costWei != null) {
      const profitIdx = list.findIndex(
        (m) => m.title === 'Net Sequencer Profit',
      );
      if (profitIdx >= 0) {
        const profitEth = parseEthValue(list[profitIdx].value);
        const profitWei = profitEth * 1e9;
        const newProfitWei = profitWei - costWei;
        list[profitIdx] = {
          ...list[profitIdx],
          value: formatEth(newProfitWei, 4),
        };
      }
    }

    return list;
  }, [metricsData.metrics, isEconomicsView, cloudCost, proverCost, ethPrice, timeRange]);

  const visibleMetrics = React.useMemo(
    () =>
      metricsWithHardware.filter((m) => {
        if (selectedSequencer && m.group === 'Sequencers') return false;
        if (isEconomicsView) return m.group === 'Network Economics';
        if (isHealthView) return m.group === 'Network Health';
        if (isPerformanceView) {
          return (
            m.group === 'Network Performance' ||
            m.group === 'Sequencers'
          );
        }
        return m.group !== 'Network Economics';
      }),
    [metricsWithHardware, selectedSequencer, isEconomicsView, isHealthView, isPerformanceView],
  );

  const groupedMetrics = React.useMemo(
    () =>
      visibleMetrics.reduce<Record<string, MetricData[]>>((acc, m) => {
        const group = m.group ?? 'Other';
        if (!acc[group]) acc[group] = [];
        acc[group].push(m);
        return acc;
      }, {}),
    [visibleMetrics],
  );

  const groupOrder = isEconomicsView
    ? ['Network Economics']
    : isHealthView
      ? ['Network Health']
      : isPerformanceView
        ? ['Network Performance', 'Sequencers']
        : ['Network Performance', 'Network Health', 'Sequencers', 'Other'];

  const skeletonGroupCounts: Record<string, number> = isEconomicsView
    ? { 'Network Economics': 6 }
    : isHealthView
      ? { 'Network Health': 6 }
      : isPerformanceView
        ? {
          'Network Performance': 3,
          Sequencers: 3,
        }
        : {
          'Network Performance': 3,
          'Network Health': 6,
          Sequencers: 3,
        };

  const displayGroupName = useCallback(
    (group: string): string => {
      if (!selectedSequencer) return group;
      if (group === 'Network Performance') return 'Sequencer Performance';
      if (group === 'Network Health') return 'Sequencer Health';
      return group;
    },
    [selectedSequencer],
  );

  const displayedGroupOrder = selectedSequencer
    ? groupOrder.filter((g) => g !== 'Sequencers')
    : groupOrder;

  const handleResetNavigation = useCallback(() => {
    navigate('/', { replace: true });
    metricsData.setErrorMessage('');
  }, [navigate, metricsData]);

  const handleClearError = useCallback(() => {
    metricsData.setErrorMessage('');
  }, [metricsData]);

  const getMetricAction = useCallback(
    (title: string) => {
      const actions: Record<string, () => void> = {
        'Avg. L2 TPS': onOpenTpsTable,
        'L2 Reorgs': () => onOpenTable('reorgs'),
        'Failed Proposals': () => onOpenTable('failed-proposals'),
        'Slashing Events': () => onOpenTable('slashings'),
        'Forced Inclusions': () => onOpenTable('forced-inclusions'),
        'Batch Posting Cadence': () => onOpenTable('batch-posting-cadence'),
        'Avg. Prove Time': () => onOpenTable('prove-times', timeRange),
        'Propose Batch Cost': () => onOpenTable('l1-data-cost', timeRange),
      };
      return actions[title];
    },
    [onOpenTable, onOpenTpsTable, timeRange],
  );

  const groupedCharts = React.useMemo(() => {
    if (isEconomicsView) return {} as Record<string, React.ReactNode[]>;

    const performance = [
      <ChartCard
        key="gas"
        title="Avg Gas Used Per Block"
        onMore={() => onOpenTable('l2-gas-used', timeRange)}
        loading={isLoadingData}
      >
        <GasUsedChart
          key={`${timeRange}-g`}
          data={chartsData.l2GasUsedData}
          lineColor={chartSeries.accent3}
        />
      </ChartCard>,
      <ChartCard
        key="tx"
        title="Avg Tx Count Per L2 Block"
        onMore={() => onOpenTable('block-tx', timeRange)}
        loading={isLoadingData}
      >
        <BlockTxChart
          key={`${timeRange}-t`}
          data={chartsData.blockTxData}
          lineColor={chartSeries.accent1}
        />
      </ChartCard>,
    ];

    const health = [
      <ChartCard
        key="prove"
        title="Avg Prove Time"
        onMore={() => onOpenTable('prove-times', timeRange)}
        loading={isLoadingData}
      >
        <BatchProcessChart
          key={timeRange}
          data={chartsData.secondsToProveData}
          lineColor={chartSeries.primary}
        />
      </ChartCard>,
      <ChartCard
        key="blobs"
        title="Avg Blobs per Batch"
        onMore={() => onOpenTable('blobs-per-batch', timeRange)}
        loading={isLoadingData}
      >
        <BlobsPerBatchChart
          key={`${timeRange}-b`}
          data={chartsData.batchBlobCounts}
          barColor="#A0CBE8"
        />
      </ChartCard>,
      <ChartCard
        key="block-times"
        title="L2 Block Time Distribution"
        loading={isLoadingData}
      >
        <BlockTimeDistributionChart
          key={`${timeRange}-d`}
          data={chartsData.l2BlockTimeData}
          barColor={chartSeries.accent5}
        />
      </ChartCard>,
    ];

    const groups: Record<string, React.ReactNode[]> = {};

    if (isPerformanceView || !isHealthView) {
      groups['Network Performance'] = performance;
    }

    if (isHealthView || !isPerformanceView) {
      groups['Network Health'] = health;
    }

    if (!selectedSequencer && isPerformanceView) {
      groups['Sequencers'] = [
        <ChartCard
          key="seq-dist"
          title="Sequencer Distribution"
          onMore={() => onOpenSequencerDistributionTable(timeRange, 0)}
          loading={isLoadingData}
        >
          <SequencerPieChart
            key={`${timeRange}-s`}
            data={chartsData.sequencerDistribution}
          />
        </ChartCard>,
      ];
    }

    return groups;
  }, [
    chartsData,
    timeRange,
    selectedSequencer,
    isLoadingData,
    isEconomicsView,
    isHealthView,
    isPerformanceView,
    onOpenTable,
    onOpenSequencerDistributionTable,
  ]);

  return (
    <div
      className={
        `bg-white dark:bg-gray-900 text-gray-800 dark:text-gray-100 ${isMainDashboardView ? 'pt-2 pb-4 px-4 md:pt-2 md:pb-6 md:px-6 lg:pt-2 lg:pb-8 lg:px-8' : 'p-4 md:p-6 lg:p-8'}`
      }
      style={{ fontFamily: "'Inter', sans-serif" }}
    >
      <ErrorDisplay
        errorMessage={metricsData.errorMessage}
        onResetNavigation={handleResetNavigation}
        onClearError={handleClearError}
      />

      <main className="mt-6">
        <MetricsGrid
          isLoading={metricsData.loadingMetrics}
          groupedMetrics={groupedMetrics}
          groupOrder={displayedGroupOrder}
          skeletonGroupCounts={skeletonGroupCounts}
          displayGroupName={displayGroupName}
          onMetricAction={getMetricAction}
          economicsView={isEconomicsView}
          groupedCharts={groupedCharts}
          isTimeRangeChanging={isTimeRangeChanging}
          timeRange={timeRange}
        />

        {isEconomicsView && (
          <>
            <ProfitCalculator
              timeRange={timeRange}
              cloudCost={cloudCost}
              proverCost={proverCost}
              onCloudCostChange={setCloudCost}
              onProverCostChange={setProverCost}
            />
            <FeeFlowChart
              timeRange={timeRange}
              cloudCost={cloudCost}
              proverCost={proverCost}
              address={selectedSequencer || undefined}
              height={400}
              totalSequencers={chartsData.sequencerDistribution.length}
              feesData={feesData}
            />
            <div className="mt-6">
              <h3 className="text-lg font-semibold mb-2">PnL Trend per Batch</h3>
              <EconomicsChart
                timeRange={timeRange}
                cloudCost={cloudCost}
                proverCost={proverCost}
                address={selectedSequencer || undefined}
                totalSequencers={chartsData.sequencerDistribution.length}
                feesData={feesData}
              />
            </div>
            <ProfitRankingTable
              timeRange={timeRange}
              cloudCost={cloudCost}
              proverCost={proverCost}
              feesData={feesData}
              sequencers={chartsData.sequencerDistribution as any}
            />
            <BlockProfitTables
              timeRange={timeRange}
              cloudCost={cloudCost}
              proverCost={proverCost}
              address={selectedSequencer || undefined}
              totalSequencers={chartsData.sequencerDistribution.length}
              feesData={feesData}
            />
          </>
        )}

        {/* Charts are now displayed within MetricsGrid groups */}
      </main>
    </div>
  );
};
