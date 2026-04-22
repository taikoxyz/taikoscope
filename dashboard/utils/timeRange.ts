export const MAX_TIME_RANGE_DAYS = 30;
const MAX_TIME_RANGE_MINUTES = MAX_TIME_RANGE_DAYS * 24 * 60;
const MAX_TIME_RANGE_MS = MAX_TIME_RANGE_DAYS * 24 * 60 * 60 * 1000;

export const isValidTimeRange = (range: string): boolean => {
  const trimmed = range.trim();
  const match = trimmed.match(/^(\d+)([mhd])$/i);
  if (match) {
    const value = parseInt(match[1], 10);
    if (value <= 0) return false;
    const unit = match[2].toLowerCase();
    const minutes =
      unit === 'h' ? value * 60 : unit === 'd' ? value * 24 * 60 : value;
    return minutes <= MAX_TIME_RANGE_MINUTES;
  }

  const custom = trimmed.match(/^(\d+)-(\d+)$/);
  if (custom) {
    const start = parseInt(custom[1], 10);
    const end = parseInt(custom[2], 10);
    if (isNaN(start) || isNaN(end) || end <= start) return false;
    return end - start <= MAX_TIME_RANGE_MS;
  }

  return false;
};

export const rangeToHours = (range: string): number => {
  const trimmed = range.trim();
  const match = trimmed.match(/^(\d+)([mhd])$/i);
  if (match) {
    const value = parseInt(match[1], 10);
    const unit = match[2].toLowerCase();
    if (unit === 'h') return value;
    if (unit === 'd') return value * 24;
    return value / 60;
  }

  const custom = trimmed.match(/^(\d+)-(\d+)$/);
  if (custom) {
    const start = parseInt(custom[1], 10);
    const end = parseInt(custom[2], 10);
    if (isNaN(start) || isNaN(end) || end <= start) return 24;
    return (end - start) / 3_600_000;
  }

  return 24;
};

export const timeRangeToQuery = (range: string): string => {
  const now = Date.now();
  let start = now - 86_400_000;
  let end = now;

  const trimmed = range.trim();
  const preset = trimmed.match(/^(\d+)([mhd])$/i);
  if (preset) {
    const value = parseInt(preset[1], 10);
    const ms =
      value *
      (preset[2].toLowerCase() === 'h'
        ? 3_600_000
        : preset[2].toLowerCase() === 'd'
        ? 86_400_000
        : 60_000);
    start = now - ms;
  } else {
    const custom = trimmed.match(/^(\d+)-(\d+)$/);
    if (custom) {
      start = parseInt(custom[1], 10);
      end = parseInt(custom[2], 10);
    }
  }

  const params = new URLSearchParams();
  params.set('created[gt]', String(start));
  params.set('created[lte]', String(end));
  return params.toString();
};

export const rangeToQuery = (range: string): string => {
  const params = new URLSearchParams();
  params.set('range', range.trim());
  return params.toString();
};

export const formatTimeRangeDisplay = (range: string): string => {
  const trimmed = range.trim();
  const preset = trimmed.match(/^(\d+)([mhd])$/i);
  if (preset) {
    const value = parseInt(preset[1], 10);
    const unit =
      preset[2].toLowerCase() === 'h'
        ? 'hour'
        : preset[2].toLowerCase() === 'd'
        ? 'day'
        : 'minute';
    const plural = value === 1 ? '' : 's';
    return `last ${value} ${unit}${plural}`;
  }

  const custom = trimmed.match(/^(\d+)-(\d+)$/);
  if (custom) {
    const start = parseInt(custom[1], 10);
    const end = parseInt(custom[2], 10);
    if (!Number.isNaN(start) && !Number.isNaN(end)) {
      const startDate = new Date(start);
      const endDate = new Date(end);
      const sameDay = startDate.toDateString() === endDate.toDateString();
      const fmt = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')} ${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
      if (sameDay) {
        const fmtTime = (d: Date) => `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
        return `${fmt(startDate)}-${fmtTime(endDate)}`;
      }
      return `${fmt(startDate)}-${fmt(endDate)}`;
    }
  }

  return trimmed;
};

export const normalizeTimeRange = (
  range: string,
  now: number = Date.now(),
): string => {
  let start = now - 86_400_000;
  let end = now;

  const trimmed = range.trim();
  const preset = trimmed.match(/^(\d+)([mhd])$/i);
  if (preset) {
    const value = parseInt(preset[1], 10);
    const ms =
      value *
      (preset[2].toLowerCase() === 'h'
        ? 3_600_000
        : preset[2].toLowerCase() === 'd'
        ? 86_400_000
        : 60_000);
    start = now - ms;
  } else {
    const custom = trimmed.match(/^(\d+)-(\d+)$/);
    if (custom) {
      start = parseInt(custom[1], 10);
      end = parseInt(custom[2], 10);
    }
  }

  return `${start}-${end}`;
};
