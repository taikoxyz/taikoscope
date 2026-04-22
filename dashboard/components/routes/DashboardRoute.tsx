import React from 'react';
import { useOutletContext } from 'react-router-dom';
import { DashboardView } from '../views/DashboardView';
import { DashboardHeader } from '../DashboardHeader';
import {
  TimeRange,
  ChartsData,
  MetricsDataState,
  RefreshTimerState,
} from '../../types';

interface DashboardContextType {
  timeRange: TimeRange;
  setTimeRange: (range: TimeRange) => void;
  selectedSequencer: string | null;
  chartsData: ChartsData;
  metricsData: MetricsDataState;
  refreshTimer: RefreshTimerState;
  isLoadingData: boolean;
  isTimeRangeChanging: boolean;
  handleManualRefresh: () => void;
  openGenericTable: (table: string, timeRange?: TimeRange) => void;
  openTpsTable: () => void;
  openSequencerDistributionTable: (timeRange: TimeRange, page: number) => void;
}

export const DashboardRoute: React.FC = () => {
  const {
    timeRange,
    setTimeRange,
    selectedSequencer,
    chartsData,
    metricsData,
    refreshTimer,
    isLoadingData,
    isTimeRangeChanging,
    handleManualRefresh,
    openGenericTable,
    openTpsTable,
    openSequencerDistributionTable,
  } = useOutletContext<DashboardContextType>();

  return (
    <>
      <DashboardHeader
        timeRange={timeRange}
        onTimeRangeChange={setTimeRange}
        refreshRate={refreshTimer.refreshRate}
        onRefreshRateChange={refreshTimer.setRefreshRate}
        lastRefresh={refreshTimer.lastRefresh}
        onManualRefresh={handleManualRefresh}
        isTimeRangeChanging={isTimeRangeChanging}
      />
      <DashboardView
        timeRange={timeRange}
        selectedSequencer={selectedSequencer}
        metricsData={metricsData}
        chartsData={chartsData}
        isLoadingData={isLoadingData}
        isTimeRangeChanging={isTimeRangeChanging}
        onOpenTable={openGenericTable}
        onOpenTpsTable={openTpsTable}
        onOpenSequencerDistributionTable={openSequencerDistributionTable}
      />
    </>
  );
};
