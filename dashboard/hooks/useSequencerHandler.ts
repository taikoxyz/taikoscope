import { useState, useEffect, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';
import type { MetricData } from '../types';

interface UseSequencerHandlerProps {
  blockData: {
    l1HeadBlock: string;
    l2HeadBlock: string;
    updateMetricsWithBlockHeads: (metrics: MetricData[]) => MetricData[];
  };
  metricsData: {
    metrics: MetricData[];
    setMetrics: (metrics: MetricData[]) => void;
  };
}

export const useSequencerHandler = ({ blockData, metricsData }: UseSequencerHandlerProps) => {
  const [searchParams] = useSearchParams();
  const [selectedSequencer, setSelectedSequencer] = useState<string | null>(
    searchParams.get('sequencer'),
  );

  // Sync with URL params - extract specific value to avoid object dependency
  const sequencerParam = searchParams.get('sequencer');
  useEffect(() => {
    setSelectedSequencer(sequencerParam ?? null);
  }, [sequencerParam]);

  // Update metrics with current block heads whenever they change
  const lastUpdateRef = useRef<string>('');

  useEffect(() => {
    const currentKey = `${blockData.l1HeadBlock}-${blockData.l2HeadBlock}`;

    if (
      metricsData.metrics.length > 0 &&
      lastUpdateRef.current !== currentKey
    ) {
      const updatedMetrics = blockData.updateMetricsWithBlockHeads(
        metricsData.metrics,
      );
      metricsData.setMetrics(updatedMetrics);
      lastUpdateRef.current = currentKey;
    }
  }, [blockData.l1HeadBlock, blockData.l2HeadBlock]);

  return {
    selectedSequencer,
    setSelectedSequencer,
  };
};
