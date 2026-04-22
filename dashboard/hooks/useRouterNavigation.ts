import { useCallback } from 'react';
import {
  useNavigate,
  useSearchParams,
  createSearchParams,
} from 'react-router-dom';
import { safeNavigate } from '../utils/navigationUtils';
import { TimeRange } from '../types';

export const useRouterNavigation = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const navigateToTable = useCallback(
    (
      tableType: string,
      params?: Record<string, string | number>,
      range?: TimeRange,
    ) => {
      const queryParams: Record<string, string> = {};

      // Preserve current dashboard view if present
      const currentView = searchParams.get('view');
      if (currentView) {
        queryParams.view = currentView;
      }

      if (range) {
        queryParams.range = range;
      }

      if (params) {
        Object.entries(params).forEach(([key, value]) => {
          queryParams[key] = String(value);
        });
      }

      const queryString = createSearchParams(queryParams).toString();
      const path = `/table/${tableType}${queryString ? `?${queryString}` : ''}`;
      safeNavigate(navigate, path);
    },
    [navigate, searchParams],
  );

  const navigateToDashboard = useCallback(
    (preserveParams = false) => {
      if (preserveParams) {
        const params: Record<string, string> = {};
        const sequencer = searchParams.get('sequencer');
        const range = searchParams.get('range');
        const view = searchParams.get('view');

        if (sequencer) params.sequencer = sequencer;
        if (range) params.range = range;
        if (view) params.view = view;

        const queryString = createSearchParams(params).toString();
        safeNavigate(navigate, `/${queryString ? `?${queryString}` : ''}`);
      } else {
        safeNavigate(navigate, '/');
      }
    },
    [navigate, searchParams],
  );

  const updateSearchParams = useCallback(
    (updates: Record<string, string | null>) => {
      const params: Record<string, string> = {};
      searchParams.forEach((value, key) => {
        params[key] = value;
      });

      Object.entries(updates).forEach(([key, value]) => {
        if (value === null) {
          delete params[key];
        } else {
          params[key] = value;
        }
      });

      const queryString = createSearchParams(params).toString();
      safeNavigate(navigate, `?${queryString}`, true);
    },
    [navigate, searchParams],
  );

  return {
    navigateToTable,
    navigateToDashboard,
    updateSearchParams,
  };
};
