import { useCallback } from 'react';
import { useMetricsData } from './useMetricsData';
import { useChartsData } from './useChartsData';
import { useBlockData } from './useBlockData';
import { useRefreshTimer } from './useRefreshTimer';
import { useTableRouter } from './useTableRouter';
import { useNavigationHandler } from './useNavigationHandler';
import { useDataFetcher } from './useDataFetcher';
import { useSequencerHandler } from './useSequencerHandler';
import { useTableActions } from './useTableActions';
import { useTimeRangeSync } from './useTimeRangeSync';
import { useSearchParams } from 'react-router-dom';
import { useErrorHandler } from './useErrorHandler';

export const useDashboardController = () => {
  const { timeRange, setTimeRange } = useTimeRangeSync();
  const [searchParams] = useSearchParams();
  const { setErrorMessage } = useErrorHandler();

  // Data management hooks
  const metricsData = useMetricsData();
  const chartsData = useChartsData();
  const blockData = useBlockData();
  const refreshTimer = useRefreshTimer();

  // Sequencer handling
  const { selectedSequencer, setSelectedSequencer } =
    useSequencerHandler({ blockData, metricsData });


  // Table actions
  const {
    tableView,
    tableLoading,
    setTableView,
    setTableLoading,
    openGenericTable,
    openSequencerDistributionTable,
  } = useTableActions(timeRange, setTimeRange, selectedSequencer);

  // Data fetching coordination
  const { handleManualRefresh, isLoadingData, isTimeRangeChanging, hasData } =
    useDataFetcher({
      timeRange,
      selectedSequencer,
      tableView,
      updateChartsData: chartsData.updateChartsData,
      setMetrics: metricsData.setMetrics,
      setLoadingMetrics: metricsData.setLoadingMetrics,
      setErrorMessage: metricsData.setErrorMessage,
      isEconomicsView: metricsData.isEconomicsView,
      refreshRate: refreshTimer.refreshRate,
      updateLastRefresh: refreshTimer.updateLastRefresh,
    });

  // Navigation handling
  const { handleBack, handleSequencerChange } = useNavigationHandler({
    onError: setErrorMessage,
  });

  // Table routing
  useTableRouter({
    timeRange,
    setTableView,
    setTableLoading,
    tableView,
    openGenericTable,
    openSequencerDistributionTable,
    onError: setErrorMessage,
  });

  // Combined sequencer change handler
  const handleSequencerChangeWithState = (seq: string | null) => {
    setSelectedSequencer(seq);
    handleSequencerChange(seq);
  };

  // Create a TPS table opener that uses the generic table function
  const openTpsTable = useCallback(() => {
    openGenericTable('l2-tps');
  }, [openGenericTable]);

  return {
    // State
    timeRange,
    setTimeRange,
    selectedSequencer,

    // Data
    metricsData,
    chartsData,
    blockData,
    refreshTimer,
    searchParams,

    // Loading states
    isLoadingData,
    isTimeRangeChanging,
    hasData,

    // Table state
    tableView,
    tableLoading,

    // Handlers
    handleSequencerChangeWithState,
    handleBack,
    handleManualRefresh,

    // Table actions
    openGenericTable,
    openTpsTable,
    openSequencerDistributionTable,
  };
};
