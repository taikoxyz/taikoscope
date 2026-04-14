import React, { useEffect, useState, useCallback, useRef } from 'react';
import { useParams, useSearchParams, useOutletContext } from 'react-router-dom';
import { useRouterNavigation } from '../../hooks/useRouterNavigation';
import { TableView } from '../views/TableView';
import { DashboardHeader } from '../DashboardHeader';
import { TableViewState } from '../../hooks/useTableActions';
import {
  TimeRange,
  ChartsData,
  MetricsDataState,
  RefreshTimerState,
} from '../../types';
import { TABLE_CONFIGS } from '../../config/tableConfig';
import { getSequencerAddress } from '../../sequencerConfig';
import { useDataFetcher } from '../../hooks/useDataFetcher';

interface DashboardContextType {
  timeRange: TimeRange;
  setTimeRange: (range: TimeRange) => void;
  selectedSequencer: string | null;
  sequencerList: string[];
  chartsData: ChartsData;
  metricsData: MetricsDataState;
  refreshTimer: RefreshTimerState;
}

export const TableRoute: React.FC = () => {
  const { tableType } = useParams<{ tableType: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const { navigateToDashboard } = useRouterNavigation();

  const PAGE_LIMIT = 50;

  const {
    timeRange,
    setTimeRange,
    selectedSequencer,
    chartsData,
    metricsData,
    refreshTimer,
  } = useOutletContext<DashboardContextType>();

  // Override setTimeRange to update URL params for table route instead of navigating away
  const handleTimeRangeChange = useCallback(
    (newRange: TimeRange) => {
      const newParams = new URLSearchParams(searchParams);
      if (newRange === '24h') {
        newParams.delete('range');
      } else {
        newParams.set('range', newRange);
      }
      setSearchParams(newParams, { replace: true });
      setTimeRange(newRange);
    },
    [searchParams, setSearchParams, setTimeRange],
  );

  const [tableView, setTableView] = useState<TableViewState | undefined>(
    undefined,
  );
  const [tableLoading, setTableLoading] = useState(false);
  const fetchIdRef = useRef(0);

  // Get current time range from URL params, fallback to context
  const currentTimeRange =
    (searchParams.get('range') as TimeRange) || timeRange;

  const { handleManualRefresh } = useDataFetcher({
    timeRange: currentTimeRange,
    selectedSequencer,
    tableView: tableView || null,
    updateChartsData: chartsData.updateChartsData,
    setMetrics: metricsData.setMetrics,
    setLoadingMetrics: metricsData.setLoadingMetrics,
    setErrorMessage: metricsData.setErrorMessage,
    isEconomicsView: metricsData.isEconomicsView,
    refreshRate: refreshTimer.refreshRate,
    updateLastRefresh: refreshTimer.updateLastRefresh,
  });

  useEffect(() => {
    const currentFetchId = ++fetchIdRef.current;

    const loadTable = async () => {
      if (!tableType) return;

      setTableLoading(true);
      setTableView(undefined);

      try {
        const range = currentTimeRange;

        // Handle all tables using config
        const config = TABLE_CONFIGS[tableType];
        if (!config) {
          throw new Error(`Unknown table type: ${tableType}`);
        }

        const fetcherArgs: (string | number | undefined)[] = [];
        const extraParams: Record<string, string | number | undefined> = {};

        const pageStr = searchParams.get('page') ?? '0';
        const page = parseInt(pageStr, 10);
        const start = searchParams.get('start');
        const end = searchParams.get('end');
        const startBatch = searchParams.get('start_batch');
        const endBatch = searchParams.get('end_batch');
        const startingAfter = start ? Number(start) : undefined;
        const endingBefore = end ? Number(end) : undefined;
        const startingAfterBatch = startBatch ? Number(startBatch) : undefined;
        const endingBeforeBatch = endBatch ? Number(endBatch) : undefined;

        if (tableType === 'sequencer-blocks') {
          const address = searchParams.get('address');
          if (address) {
            fetcherArgs.push(address);
            extraParams.address = address;
          }
        } else if (['l2-gas-used', 'l2-tps'].includes(tableType)) {
          fetcherArgs.push(
            selectedSequencer
              ? getSequencerAddress(selectedSequencer)
              : undefined,
          );
        }

        let res;
        let aggRes;
        if (tableType === 'reorgs' || tableType === 'prove-times' ) {
          // For reorgs, fetch one extra item on the first page to detect if a next page exists
          const isFirstPage = startingAfter === undefined && endingBefore === undefined;
          const fetchLimit = tableType === 'reorgs' && isFirstPage ? PAGE_LIMIT + 1 : PAGE_LIMIT;
          if (config.aggregatedFetcher) {
            [res, aggRes] = await Promise.all([
              config.fetcher(range, fetchLimit, startingAfter, endingBefore),
              config.aggregatedFetcher(range),
            ]);
          } else {
            [res] = await Promise.all([
              config.fetcher(range, fetchLimit, startingAfter, endingBefore),
            ]);
          }
        } else if (config.supportsPagination) {
          // For paginated tables, determine if an address param is needed
          const isFirstPage = startingAfter === undefined && endingBefore === undefined;
          // Fetch +1 on the first failed-proposals page to precisely toggle Next
          const fetchLimit = tableType === 'failed-proposals' && isFirstPage
            ? PAGE_LIMIT + 1
            : PAGE_LIMIT;
          const needsAddress = ['l2-block-times', 'l2-gas-used', 'l2-tps', 'block-tx'].includes(tableType);
          const addressParam = needsAddress
            ? (selectedSequencer ? getSequencerAddress(selectedSequencer) : undefined)
            : undefined;
          if (tableType === 'failed-proposals') {
            // Pass tie-breaker cursors (timestamp + l2) for stable pagination
            [res] = await Promise.all([
              config.fetcher(
                range,
                fetchLimit,
                startingAfter,
                endingBefore,
                startingAfterBatch,
                endingBeforeBatch,
              ),
            ]);
          } else if (config.aggregatedFetcher) {
            [res, aggRes] = await Promise.all([
              needsAddress
                ? config.fetcher(range, addressParam, fetchLimit, startingAfter, endingBefore)
                : config.fetcher(range, fetchLimit, startingAfter, endingBefore),
              needsAddress
                ? config.aggregatedFetcher(range, addressParam)
                : config.aggregatedFetcher(range),
            ]);
          } else {
            [res] = await Promise.all([
              needsAddress
                ? config.fetcher(range, addressParam, fetchLimit, startingAfter, endingBefore)
                : config.fetcher(range, fetchLimit, startingAfter, endingBefore),
            ]);
          }
        } else {
          [res, aggRes] = await (config.aggregatedFetcher
            ? Promise.all([
              config.fetcher(range, ...fetcherArgs),
              config.aggregatedFetcher(range, ...fetcherArgs),
            ])
            : Promise.all([config.fetcher(range, ...fetcherArgs)]));
        }
        if (currentFetchId !== fetchIdRef.current) return;
        let data = res.data || [];
        const chartData = aggRes?.data || data;

        // For tables with aggregatedFetcher, use aggregated data on custom absolute time ranges
        const isCustomAbsoluteRange =
          typeof currentTimeRange === 'string' && /^\d+-\d+$/.test(currentTimeRange);
        if (config.aggregatedFetcher && isCustomAbsoluteRange) {
          data = chartData;
        }

        // Calculate pagination cursors from original data before reversing/slicing
        const originalData = data;

        const getCursor = (item: Record<string, unknown>) => {
          // For prove-times tables, the "name" field is the batch ID
          if (tableType === 'prove-times' ) {
            const name = (item as { name?: string }).name;
            return name !== undefined ? Number(name) : undefined;
          }
          // For reorgs, paginate by timestamp to respect time window exclusivity
          if (tableType === 'reorgs') {
            const ts = (item as { timestamp?: number }).timestamp;
            return ts !== undefined ? ts : undefined;
          }
          // For failed-proposals, also paginate by timestamp to keep within the same time window
          if (tableType === 'failed-proposals') {
            const ts = (item as { timestamp?: number }).timestamp;
            return ts !== undefined ? ts : undefined;
          }
          // Otherwise fall back to the numeric value or block/batch fields
          // Reorgs use l2_block_number internally for pagination, which maps to
          // the "to_block_number" field in the UI payload
          const reorgToBlock = (item as { to_block_number?: number }).to_block_number;
          if (reorgToBlock !== undefined) return reorgToBlock;
          return (
            (item as { value?: number }).value ??
            (item as { l2_block_number?: number }).l2_block_number ??
            (item as { block?: number }).block ??
            (item as { block_number?: number }).block_number ??
            (item as { batch?: number }).batch
          );
        };
        const isFirstReorgsPage = tableType === 'reorgs' && startingAfter === undefined && endingBefore === undefined;
        const isFirstFailedProposalsPage = tableType === 'failed-proposals' && startingAfter === undefined && endingBefore === undefined;
        const nextCursor = originalData.length > 0
          ? ((isFirstReorgsPage || isFirstFailedProposalsPage) && originalData.length > PAGE_LIMIT
            ? getCursor(originalData[PAGE_LIMIT - 1])
            : getCursor(originalData[originalData.length - 1]))
          : undefined;
        const prevCursor =
          originalData.length > 0 ? getCursor(originalData[0]) : undefined;
        // Compute L2 cursors for failed-proposals
        const getBatch = (item: any) => (item?.batch_id !== undefined ? Number(item.batch_id) : undefined);
        const nextCursorBatch =
          tableType === 'failed-proposals' && originalData.length > 0
            ? ((isFirstFailedProposalsPage && originalData.length > PAGE_LIMIT)
              ? getBatch(originalData[PAGE_LIMIT - 1])
              : getBatch(originalData[originalData.length - 1]))
            : undefined;
        const prevCursorBatch =
          tableType === 'failed-proposals' && originalData.length > 0
            ? getBatch(originalData[0])
            : undefined;

        if (config.reverseOrder) {
          data = [...data].reverse();
        }

        // If we fetched an extra item on the first page (reorgs or failed-proposals), trim to PAGE_LIMIT
        if ((isFirstReorgsPage || isFirstFailedProposalsPage) && originalData.length > PAGE_LIMIT) {
          data = data.slice(0, PAGE_LIMIT);
        }

        const title =
          typeof config.title === 'function'
            ? config.title(extraParams)
            : config.title;

        const mappedData = config.mapData
          ? config.mapData(data, extraParams)
          : data;
        const chart = config.chart ? config.chart(chartData) : undefined;

        if (currentFetchId === fetchIdRef.current) {
          const view: TableViewState = {
            title,
            description: config.description,
            columns: config.columns,
            rows: mappedData,
            chart,
          };
          // Only show pagination controls when NOT in a custom range on l2-tps
          if (config.supportsPagination && !(config.aggregatedFetcher && isCustomAbsoluteRange)) {
            const disablePrev = page === 0;
            const disableNext = (isFirstReorgsPage || isFirstFailedProposalsPage)
              ? originalData.length <= PAGE_LIMIT
              : originalData.length < PAGE_LIMIT;
            view.serverPagination = {
              page,
              onNext: () => {
                const params = new URLSearchParams(searchParams);
                params.set('page', String(page + 1));
                if (nextCursor !== undefined)
                  params.set('start', String(nextCursor));
                if (tableType === 'failed-proposals' && nextCursorBatch !== undefined) {
                  params.set('start_batch', String(nextCursorBatch));
                } else {
                  params.delete('start_batch');
                }
                params.delete('end');
                params.delete('end_batch');
                // Preserve explicit time range bounds for subsequent pages
                const rangeParam = params.get('range') || '24h';
                // Only preset ranges are supported here; they will be converted by the API service
                params.set('range', rangeParam);
                setSearchParams(params);
              },
              onPrev: () => {
                const params = new URLSearchParams(searchParams);
                const newPage = page - 1;
                params.set('page', String(newPage));
                if (newPage === 0) {
                  // On first page, clear all cursor params
                  params.delete('start');
                  params.delete('start_batch');
                  params.delete('end');
                  params.delete('end_batch');
                } else {
                  if (prevCursor !== undefined) params.set('end', String(prevCursor));
                  if (tableType === 'failed-proposals' && prevCursorBatch !== undefined) {
                    params.set('end_batch', String(prevCursorBatch));
                  } else {
                    params.delete('end_batch');
                  }
                  params.delete('start');
                  params.delete('start_batch');
                }
                // Preserve explicit time range bounds for previous pages as well
                const rangeParam = params.get('range') || '24h';
                params.set('range', rangeParam);
                setSearchParams(params);
              },
              disableNext,
              disablePrev,
            };
          }
          setTableView(view);
        }
      } catch (error) {
        console.error('Failed to load table:', error);
        metricsData.setErrorMessage(
          `Failed to load ${tableType} table. Please try again.`,
        );
      } finally {
        setTableLoading(false);
      }
    };

    loadTable();

    return () => {
      fetchIdRef.current++;
    };
  }, [
    tableType,
    searchParams,
    currentTimeRange,
    selectedSequencer,
    metricsData.setErrorMessage,
  ]);

  const handleBack = () => {
    navigateToDashboard(true);
  };

  if (!tableView && !tableLoading) {
    return <div>Table not found</div>;
  }

  return (
    <>
      <DashboardHeader
        timeRange={currentTimeRange}
        onTimeRangeChange={handleTimeRangeChange}
        refreshRate={refreshTimer.refreshRate}
        onRefreshRateChange={refreshTimer.setRefreshRate}
        lastRefresh={refreshTimer.lastRefresh}
        onManualRefresh={handleManualRefresh}
      />
      <TableView
        tableView={tableView}
        tableLoading={tableLoading}
        isNavigating={false}
        onBack={handleBack}
      />
    </>
  );
};
