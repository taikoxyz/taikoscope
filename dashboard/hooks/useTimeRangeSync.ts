import { useCallback, useEffect, useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { TimeRange } from '../types';
import { isValidTimeRange } from '../utils/timeRange';

const DEFAULT_TIME_RANGE: TimeRange = '24h';
const BATCH_DATA_DEFAULT_TIME_RANGE: TimeRange = '14d';

/**
 * Hook that synchronizes time range state with URL parameters to prevent navigation loops
 * and maintain consistent state across dashboard and table views.
 */
export const useTimeRangeSync = () => {
  const location = useLocation();
  const navigate = useNavigate();

  const getDefaultTimeRange = useCallback((): TimeRange => {
    const params = new URLSearchParams(location.search);
    const view = params.get('view');
    return view === 'economics' || view === 'health'
      ? BATCH_DATA_DEFAULT_TIME_RANGE
      : DEFAULT_TIME_RANGE;
  }, [location.search]);

  // Get initial time range from URL or use default
  const getInitialTimeRange = useCallback((): TimeRange => {
    const params = new URLSearchParams(location.search);
    const start = params.get('start');
    const end = params.get('end');
    if (start && end) {
      const s = parseInt(start, 10);
      const e = parseInt(end, 10);
      const custom = `${s}-${e}`;
      if (!isNaN(s) && !isNaN(e) && isValidTimeRange(custom)) {
        return custom;
      }
    }
    const urlRange = params.get('range');
    return urlRange && isValidTimeRange(urlRange)
      ? (urlRange as TimeRange)
      : getDefaultTimeRange();
  }, [getDefaultTimeRange, location.search]);

  const [timeRange, setTimeRangeState] =
    useState<TimeRange>(getInitialTimeRange);

  // Update time range and sync with URL
  const setTimeRange = useCallback(
    (newRange: TimeRange) => {
      if (!isValidTimeRange(newRange)) {
        console.warn('Invalid time range:', newRange);
        return;
      }

      setTimeRangeState(newRange);

      const newParams = new URLSearchParams(location.search);
      if (/^\d+-\d+$/.test(newRange)) {
        const [s, e] = newRange.split('-');
        newParams.set('start', s);
        newParams.set('end', e);
        newParams.delete('range');
      } else {
        if (newRange === getDefaultTimeRange()) {
          newParams.delete('range');
        } else {
          newParams.set('range', newRange);
        }
        newParams.delete('start');
        newParams.delete('end');
      }

      navigate(
        { search: newParams.toString() },
        { replace: true },
      );
    },
    [getDefaultTimeRange, location.search, navigate],
  );

  // Sync state when URL changes (e.g., browser back/forward)
  useEffect(() => {
    const urlRange = getInitialTimeRange();
    if (urlRange !== timeRange) {
      setTimeRangeState(urlRange);
    }
  }, [location.search, timeRange, getInitialTimeRange]);

  return {
    timeRange,
    setTimeRange,
  };
};
