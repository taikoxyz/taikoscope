import { useCallback, useEffect, useMemo } from 'react';
import useSWR from 'swr';
import { useSearchParams, useLocation } from 'react-router-dom';
import { DEFAULT_VIEW } from '../constants';
import { TimeRange, MetricData, ChartsDataUpdate } from '../types';
import { TableViewState } from './useTableActions';
import {
  fetchMainDashboardData,
  fetchEconomicsData,
} from '../utils/dataFetcher';
import { createMetrics, type MetricInputData } from '../utils/metricsCreator';
import { calculateNetProfit } from '../utils/profit';
import { hasBadRequest, getErrorMessage } from '../utils/errorHandler';

interface UseDataFetcherProps {
  timeRange: TimeRange;
  selectedSequencer: string | null;
  tableView: TableViewState | null;
  updateChartsData: (data: ChartsDataUpdate) => void;
  setMetrics: (metrics: MetricData[]) => void;
  setLoadingMetrics: (v: boolean) => void;
  setErrorMessage: (msg: string) => void;
  isEconomicsView: boolean;
  refreshRate: number;
  updateLastRefresh: () => void;
}

export const useDataFetcher = ({
  timeRange,
  selectedSequencer,
  tableView,
  updateChartsData,
  setMetrics,
  setLoadingMetrics,
  setErrorMessage,
  isEconomicsView,
  refreshRate,
  updateLastRefresh,
}: UseDataFetcherProps) => {
  const [searchParams] = useSearchParams();
  const location = useLocation();

  // Memoize the specific value we need to prevent infinite re-renders
  const viewParam = searchParams.get('view') ?? DEFAULT_VIEW;
  const isTableRoute = location.pathname.startsWith('/table/');
  const isTableView = useMemo(
    () => tableView || viewParam === 'table' || isTableRoute,
    [tableView, viewParam, isTableRoute],
  );

  const selectedSequencerForFetch = selectedSequencer;

  const fetchKey = isTableView
    ? null
    : ['metrics', timeRange, selectedSequencerForFetch, isEconomicsView];

  const fetcher = async () => {
    if (isEconomicsView) {
      const data = await fetchEconomicsData(
        timeRange,
        selectedSequencerForFetch,
      );
      const anyBadRequest = hasBadRequest(data.badRequestResults);

      const metricsInput: MetricInputData = {
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
      priorityFee: data.priorityFee,
        baseFee: data.baseFee,
        l1DataCost: data.l1DataCost,
        proveCost: data.proveCost,

        profit:
          data.priorityFee != null &&
            data.baseFee != null &&
            data.l1DataCost != null &&
            data.proveCost != null
            ? calculateNetProfit({
              priorityFee: data.priorityFee,
              baseFee: data.baseFee,
              l1DataCost: data.l1DataCost,
              proveCost: data.proveCost,
            })
            : null,
        l2Block: data.l2Block,
        l1Block: data.l1Block,
      };

      const metrics = createMetrics(metricsInput);

      return {
        metrics,
        chartData: { sequencerDist: data.sequencerDist },
        anyBadRequest,
      };
    }

    const data = await fetchMainDashboardData(
      timeRange,
      selectedSequencerForFetch,
    );

    const anyBadRequest = hasBadRequest(data.badRequestResults);
    const currentOperator = data.preconfData?.current_operator ?? null;
    const nextOperator = data.preconfData?.next_operator ?? null;

    const metricsInput: MetricInputData = {
      avgTps: data.avgTps,
      l2Cadence: data.l2Cadence,
      batchCadence: data.batchCadence,
      avgProve: data.avgProve,
      currentOperator,
      nextOperator,
      l2Reorgs: data.l2Reorgs,
      slashings: data.slashings,
      forcedInclusions: data.forcedInclusions,
      failedProposals: data.failedProposals,
      priorityFee: null,
      baseFee: null,
      proveCost: null,

      l1DataCost: null,
      profit: null,
      l2Block: data.l2Block,
      l1Block: data.l1Block,
    };

    const metrics = createMetrics(metricsInput);

    return {
      metrics,
      chartData: {
        proveTimes: data.proveTimes,
        l2Times: data.l2Times,
        l2Gas: data.l2Gas,
        txPerBlock: data.txPerBlock,
        blobsPerBatch: data.blobsPerBatch,
        sequencerDist: data.sequencerDist,
      },
      anyBadRequest,
    };
  };

  const { data, mutate, isLoading, isValidating } = useSWR(fetchKey, fetcher, {
    refreshInterval:
      refreshRate === 0 ? 0 : Math.max(refreshRate, 300_000),
    revalidateOnFocus: false,
    refreshWhenHidden: false,
    onError: () => {
      setErrorMessage('Failed to fetch dashboard data. Please try again.');
    },
  });

  const fetchData = useCallback(async () => {
    try {
      await mutate();
    } catch {
      setErrorMessage('Failed to fetch dashboard data. Please try again.');
    }
  }, [mutate, setErrorMessage]);

  const handleManualRefresh = useCallback(() => {
    if (tableView && tableView.onRefresh) {
      // If we're in a table view and it has a refresh function, use that
      tableView.onRefresh();
    } else {
      // Otherwise refresh the main dashboard data
      void fetchData();
    }
  }, [fetchData, tableView?.onRefresh]);

  useEffect(() => {
    if (!data) return;
    updateLastRefresh();
    setMetrics(data.metrics);
    setErrorMessage(getErrorMessage(data.anyBadRequest));
    if (data.chartData) {
      updateChartsData(data.chartData);
    }
  }, [data, updateChartsData, updateLastRefresh, setMetrics, setErrorMessage]);

  useEffect(() => {
    setLoadingMetrics(isLoading || isValidating);
  }, [isLoading, isValidating, setLoadingMetrics]);

  // Enhanced loading state that considers both SWR loading and time range changes
  const isLoadingData = isLoading || isValidating;

  return {
    fetchData,
    handleManualRefresh,
    isLoadingData,
    isTimeRangeChanging: isValidating,
    hasData: !!data,
  };
};
