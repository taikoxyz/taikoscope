import React from 'react';
import { TimeRange } from '../types';
import { RefreshCountdown } from './RefreshCountdown';
import { isValidRefreshRate } from '../utils';
import {
  isValidTimeRange,
  formatTimeRangeDisplay,
  MAX_TIME_RANGE_DAYS,
} from '../utils/timeRange';
import { useRouterNavigation } from '../hooks/useRouterNavigation';
import { DEFAULT_VIEW } from '../constants';
import { useErrorHandler } from '../hooks/useErrorHandler';
import { useSearchParams, useLocation } from 'react-router-dom';
import { showToast } from '../utils/toast';
import { DayPicker, DateRange } from 'react-day-picker';
import * as Popover from '@radix-ui/react-popover';
import { TabList, Tab } from './ui/Tabs';
import { Select } from './ui/Select';

interface ImportMetaEnv {
  readonly VITE_NETWORK_NAME?: string;
  readonly NETWORK_NAME?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

const metaEnv = (import.meta as ImportMeta).env;
const rawNetworkName =
  metaEnv?.VITE_NETWORK_NAME ?? metaEnv?.NETWORK_NAME ?? 'Hekla';
const NETWORK_NAME =
  rawNetworkName.charAt(0).toUpperCase() +
  rawNetworkName.slice(1).toLowerCase();
const DASHBOARD_TITLE = `Taikoscope ${NETWORK_NAME}`;
const MAX_CUSTOM_DAYS = MAX_TIME_RANGE_DAYS;
const CUSTOM_RANGE_LIMIT_MESSAGE = `Custom time range is limited to ${MAX_CUSTOM_DAYS} days.`;

interface DashboardHeaderProps {
  timeRange: TimeRange;
  onTimeRangeChange: (range: TimeRange) => void;
  refreshRate: number;
  onRefreshRateChange: (rate: number) => void;
  lastRefresh: number;
  onManualRefresh: () => void;
  isTimeRangeChanging?: boolean;
}

export const DashboardHeader: React.FC<DashboardHeaderProps> = ({
  timeRange,
  onTimeRangeChange,
  refreshRate,
  onRefreshRateChange,
  lastRefresh,
  onManualRefresh,
  isTimeRangeChanging,
}) => {
  const { updateSearchParams } = useRouterNavigation();
  const { errorMessage } = useErrorHandler();
  const [searchParams] = useSearchParams();
  const location = useLocation();
  const isDashboardRoute = location.pathname === '/';
  // If a view is present anywhere, use it for highlighting; otherwise default only on dashboard
  const rawView = searchParams.get('view');
  const viewParam = rawView ?? (isDashboardRoute ? DEFAULT_VIEW : null);
  React.useEffect(() => {
    if (errorMessage) {
      showToast(errorMessage);
    }
  }, [errorMessage]);
  return (
    <header className="flex flex-col md:flex-row justify-between items-center pb-4 border-b border-gray-200 dark:border-gray-700">
      <div className="flex items-baseline space-x-4">
          <h1 className="text-3xl font-bold" style={{ color: 'var(--color-brand)' }}>
          {DASHBOARD_TITLE}
        </h1>
      </div>
      <div className="flex flex-wrap items-center gap-2 mt-4 md:mt-0 justify-center md:justify-end">
        <TabList>
          {[
            { view: 'economics', label: 'Economics' },
            { view: 'performance', label: 'Performance' },
            { view: 'health', label: 'Health' },
          ].map((t) => (
            <Tab
              key={t.view}
              selected={Boolean(viewParam && viewParam === t.view)}
              onClick={() => updateSearchParams({ view: t.view, table: null })}
            >
              {t.label}
            </Tab>
          ))}
        </TabList>
        <a
          href="https://status.taiko.xyz/"
          target="_blank"
          rel="noopener noreferrer"
          className="text-sm hover:underline"
          style={{ color: 'var(--color-brand)' }}
        >
          Status Page
        </a>
        <TimeRangeSelector
          currentTimeRange={timeRange}
          onTimeRangeChange={onTimeRangeChange}
          isChanging={isTimeRangeChanging}
        />
        <RefreshRateInput
          refreshRate={refreshRate}
          onRefreshRateChange={onRefreshRateChange}
        />
        <RefreshCountdown
          refreshRate={refreshRate}
          lastRefresh={lastRefresh}
          onRefresh={onManualRefresh}
        />
        {/* Sequencer filter removed */}
        {/* Dark mode toggle removed as per request */}
      </div>
    </header>
  );
};

export interface TimeRangeSelectorProps {
  currentTimeRange: TimeRange;
  onTimeRangeChange: (range: TimeRange) => void;
  isChanging?: boolean;
}

export const TimeRangeSelector: React.FC<TimeRangeSelectorProps> = ({
  currentTimeRange,
  onTimeRangeChange,
  isChanging,
}) => {
  const { updateSearchParams } = useRouterNavigation();
  const presetRanges: TimeRange[] = [
    '15m',
    '1h',
    '3h',
    '6h',
    '12h',
    '24h',
    '7d',
    '14d',
    '30d',
  ];
  const isCustom = /^\d+-\d+$/.test(currentTimeRange);
  const [open, setOpen] = React.useState(false);
  const [dateRange, setDateRange] = React.useState<DateRange | undefined>(() => {
    if (!isCustom) return undefined;
    const [startMs, endMs] = currentTimeRange.split('-').map((t) => Number(t));
    if (Number.isNaN(startMs) || Number.isNaN(endMs)) return undefined;
    const start = new Date(startMs);
    const end = new Date(endMs);
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
      return undefined;
    }
    const from = new Date(start);
    from.setHours(0, 0, 0, 0);
    const to = new Date(end);
    to.setHours(0, 0, 0, 0);
    if (to.getTime() === from.getTime()) {
      return { from, to: undefined };
    }
    return { from, to };
  });
  const [fromTime, setFromTime] = React.useState('');
  const [toTime, setToTime] = React.useState('');

  const buttonLabel = React.useMemo(
    () =>
      isCustom ? formatTimeRangeDisplay(currentTimeRange) : currentTimeRange,
    [currentTimeRange, isCustom],
  );

  const customTooltip = React.useMemo(() => {
    if (!isCustom) return undefined;
    const [s, e] = currentTimeRange.split('-').map((t) => new Date(Number(t)));
    const fmt = (d: Date) =>
      `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(
        d.getDate(),
      ).padStart(2, '0')} ${d.getHours().toString().padStart(2, '0')}:${d
        .getMinutes()
        .toString()
        .padStart(2, '0')}`;
    return `From ${fmt(s)} to ${fmt(e)}`;
  }, [currentTimeRange, isCustom]);

  React.useEffect(() => {
    if (isCustom) {
      const [startValue, endValue] = currentTimeRange
        .split('-')
        .map((t) => Number(t));
      if (Number.isNaN(startValue) || Number.isNaN(endValue)) return;
      const start = new Date(startValue);
      const end = new Date(endValue);
      if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return;

      const fromDay = new Date(start);
      fromDay.setHours(0, 0, 0, 0);
      const toDay = new Date(end);
      toDay.setHours(0, 0, 0, 0);
      setDateRange({
        from: fromDay,
        to: toDay.getTime() === fromDay.getTime() ? undefined : toDay,
      });

      setFromTime(
        `${start.getHours().toString().padStart(2, '0')}:${start
          .getMinutes()
          .toString()
          .padStart(2, '0')}`,
      );
      setToTime(
        `${end.getHours().toString().padStart(2, '0')}:${end
          .getMinutes()
          .toString()
          .padStart(2, '0')}`,
      );
    }
    if (!isCustom) {
      setDateRange(undefined);
      setFromTime('');
      setToTime('');
    }
  }, [currentTimeRange, isCustom]);

  const handlePreset = (r: TimeRange) => {
    updateSearchParams({ start: null, end: null, range: r });
    onTimeRangeChange(r);
    setOpen(false);
  };

  const applyCustom = () => {
    if (!dateRange?.from) return;
    const from = fromTime || '00:00';
    const to = toTime || '23:59';
    const [fh, fm] = from.split(':').map(Number);
    const [th, tm] = to.split(':').map(Number);
    const start = new Date(dateRange.from);
    start.setHours(fh, fm, 0, 0);
    const rawEndDate = dateRange.to ?? dateRange.from;
    const end = new Date(rawEndDate);
    end.setHours(th, tm, 0, 0);
    if (!dateRange.to && end <= start) {
      end.setDate(end.getDate() + 1);
    }
    if (end.getTime() - start.getTime() > MAX_CUSTOM_DAYS * 24 * 60 * 60 * 1000) {
      showToast(CUSTOM_RANGE_LIMIT_MESSAGE);
      return;
    }
    const s = start.getTime();
    const e = end.getTime();
    const custom = `${s}-${e}`;
    if (isValidTimeRange(custom)) {
      updateSearchParams({ start: String(s), end: String(e), range: null });
      onTimeRangeChange(custom);
      setOpen(false);
    }
  };

  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger asChild>
        <button
          disabled={isChanging}
          className="p-1 border border-gray-300 dark:border-border rounded-md text-sm bg-card text-card-fg min-w-[3rem]"
          title={customTooltip}
        >
          {buttonLabel}
        </button>
      </Popover.Trigger>
      <Popover.Content
        side="bottom"
        align="end"
        className="bg-card text-card-fg border border-gray-100 dark:border-border rounded-md shadow-lg p-2 space-y-1 z-10"
      >
        {presetRanges.map((r) => {
          const isSelected = !isCustom && r === currentTimeRange;
          const base = 'block w-full text-left px-2 py-1 rounded';
          const hover = 'hover:bg-gray-100 dark:hover:bg-gray-700';
          const selected = 'bg-gray-200 dark:bg-gray-700 font-medium';
          return (
            <button
              key={r}
              onClick={() => handlePreset(r)}
              aria-selected={isSelected}
              className={`${base} ${isSelected ? selected : hover}`}
            >
              {r}
            </button>
          );
        })}
        <div className="pt-1 border-t border-gray-200 dark:border-gray-700 mt-1 space-y-1">
          <DayPicker
            mode="range"
            selected={dateRange}
            onSelect={(range) => {
              if (!range?.from) {
                setDateRange(undefined);
                return;
              }

              const normalizedFrom = new Date(range.from);
              normalizedFrom.setHours(0, 0, 0, 0);
              let normalizedTo = range.to ? new Date(range.to) : undefined;
              if (normalizedTo) {
                normalizedTo.setHours(0, 0, 0, 0);
              }

              const maxRangeEnd = new Date(normalizedFrom);
              // Cap custom selection to MAX_CUSTOM_DAYS (inclusive of the start day).
              maxRangeEnd.setDate(maxRangeEnd.getDate() + (MAX_CUSTOM_DAYS - 1));

              if (normalizedTo && normalizedTo > maxRangeEnd) {
                normalizedTo = maxRangeEnd;
                showToast(CUSTOM_RANGE_LIMIT_MESSAGE);
              }

              setDateRange({
                from: normalizedFrom,
                to:
                  normalizedTo && normalizedTo.getTime() === normalizedFrom.getTime()
                    ? undefined
                    : normalizedTo,
              });

              if (!fromTime && !toTime) {
                setFromTime('00:00');
                setToTime('23:59');
              }
            }}
            defaultMonth={dateRange?.from}
            style={{
              // React Day Picker v8 uses CSS variables for accents
              ['--rdp-accent-color' as unknown as keyof React.CSSProperties]:
                'var(--color-brand)',
              ['--rdp-today-color' as unknown as keyof React.CSSProperties]:
                'var(--color-brand)',
            }}
            styles={{
              nav_button: { color: 'var(--color-brand)' },
              caption_label: { color: 'var(--color-brand)', fontWeight: 600 },
              day_selected: { backgroundColor: 'var(--color-brand)', color: '#fff' },
              day_today: { color: 'var(--color-brand)' },
            }}
          />
          <div className="flex items-center space-x-2">
            <input
              type="time"
              step="900"
              placeholder="hh:mm"
              value={fromTime}
              onChange={(e) => setFromTime(e.target.value)}
              className="border border-gray-100 dark:border-border rounded p-1 text-sm bg-card text-card-fg"
            />
            <span className="text-sm">to</span>
            <input
              type="time"
              step="900"
              placeholder="hh:mm"
              value={toTime}
              onChange={(e) => setToTime(e.target.value)}
              className="border border-gray-100 dark:border-border rounded p-1 text-sm bg-card text-card-fg"
            />
          </div>
          <button
            onClick={applyCustom}
            disabled={isChanging || !dateRange?.from || !fromTime || !toTime}
            className="mt-1 px-2 py-1 text-sm rounded-md bg-gray-200 dark:bg-gray-700 w-full"
          >
            Apply
          </button>
        </div>
      </Popover.Content>
    </Popover.Root>
  );
};

export interface RefreshRateInputProps {
  refreshRate: number;
  onRefreshRateChange: (rate: number) => void;
}

export const RefreshRateInput: React.FC<RefreshRateInputProps> = ({
  refreshRate,
  onRefreshRateChange,
}) => {
  const selectRef = React.useRef<HTMLSelectElement>(null);
  const options = [
    { label: 'Off', value: 0 },
    { label: '5 min', value: 5 * 60_000 },
    { label: '10 min', value: 10 * 60_000 },
    { label: '1h', value: 60 * 60_000 },
  ];

  const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const value = Number(e.target.value);
    if (isValidRefreshRate(value)) {
      onRefreshRateChange(value);
      // Proactively remove focus so the select does not remain highlighted
      selectRef.current?.blur();
    }
  };

  return (
    <div className="flex items-center space-x-1">
      <label htmlFor="refreshRate" className="text-sm text-muted-fg">
        Refresh
      </label>
      <Select
        id="refreshRate"
        ref={selectRef}
        value={refreshRate}
        onChange={handleChange}
        className="text-sm text-center"
      >
        {options.map(({ label, value }) => (
          <option key={value} value={value}>
            {label}
          </option>
        ))}
      </Select>
    </div>
  );
};
