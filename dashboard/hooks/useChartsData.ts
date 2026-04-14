import { useState, useCallback, useMemo } from 'react';
import {
  TimeSeriesData,
  PieChartDataItem,
  ChartsData,
  ChartsDataUpdate,
} from '../types';
import type { BlockTransaction, BatchBlobCount } from '../services/apiService';

export const useChartsData = (): ChartsData => {
  const [secondsToProveData, setSecondsToProveData] = useState<
    TimeSeriesData[]
  >([]);
  const [l2BlockTimeData, setL2BlockTimeData] = useState<TimeSeriesData[]>([]);
  const [l2GasUsedData, setL2GasUsedData] = useState<TimeSeriesData[]>([]);
  const [blockTxData, setBlockTxData] = useState<BlockTransaction[]>([]);
  const [batchBlobCounts, setBatchBlobCounts] = useState<BatchBlobCount[]>([]);
  const [sequencerDistribution, setSequencerDistribution] = useState<
    PieChartDataItem[]
  >([]);
  const updateChartsData = useCallback(
    (data: ChartsDataUpdate) => {
      if (data.proveTimes) setSecondsToProveData([...data.proveTimes]);
      if (data.l2Times) setL2BlockTimeData([...data.l2Times]);
      if (data.l2Gas) setL2GasUsedData([...data.l2Gas]);
      if (data.txPerBlock) setBlockTxData([...data.txPerBlock]);
      if (data.blobsPerBatch) setBatchBlobCounts([...data.blobsPerBatch]);
      if (data.sequencerDist)
        setSequencerDistribution([...data.sequencerDist]);
    },
    [],
  );

  return useMemo(
    () => ({
      secondsToProveData,
      l2BlockTimeData,
      l2GasUsedData,
      blockTxData,
      batchBlobCounts,
      sequencerDistribution,
      updateChartsData,
    }),
    [
      secondsToProveData,
      l2BlockTimeData,
      l2GasUsedData,
      blockTxData,
      batchBlobCounts,
      sequencerDistribution,
      updateChartsData,
    ],
  );
};
