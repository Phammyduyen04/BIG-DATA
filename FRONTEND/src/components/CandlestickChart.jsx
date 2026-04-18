import { useEffect, useRef, useCallback, useState } from 'react';
import { createChart, CrosshairMode, CandlestickSeries, HistogramSeries } from 'lightweight-charts';

/**
 * CandlestickChart — Canvas-based chart using TradingView Lightweight Charts v5
 *
 * Props:
 *   klines        — array of kline objects from API
 *   interval      — current interval code ('1m', '5m', …)
 *   onLoadMore    — callback(direction: 'left') called when user pans to data edge
 *   onIntervalChange — callback(newInterval) for zoom in/out buttons
 */
export default function CandlestickChart({
  klines,
  interval = '1h',
  onLoadMore,
  onIntervalChange,
}) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const candleSeriesRef = useRef(null);
  const volumeSeriesRef = useRef(null);
  const klinesLenRef = useRef(0);
  const isLoadingMoreRef = useRef(false);

  // Legend state (OHLCV display on hover)
  const [legend, setLegend] = useState(null);

  // ─── Create chart on mount ──────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: '#0b0e11' },
        textColor: '#848e9c',
        fontFamily: "'Inter', 'Segoe UI', sans-serif",
        fontSize: 11,
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: '#1e2329' },
        horzLines: { color: '#1e2329' },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: '#848e9c44', width: 1, style: 0 },
        horzLine: { color: '#848e9c44', width: 1, style: 0 },
      },
      rightPriceScale: {
        borderColor: '#2b3139',
        scaleMargins: { top: 0.05, bottom: 0.25 },
      },
      timeScale: {
        borderColor: '#2b3139',
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        barSpacing: 6,
        minBarSpacing: 2,
      },
      handleScroll: { vertTouchDrag: false },
      handleScale: { axisPressedMouseMove: true },
    });

    // Candlestick series (v5 API: chart.addSeries)
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#0ecb81',
      downColor: '#f6465d',
      borderUpColor: '#0ecb81',
      borderDownColor: '#f6465d',
      wickUpColor: '#0ecb81',
      wickDownColor: '#f6465d',
    });

    // Volume series (histogram overlay at bottom) (v5 API: chart.addSeries)
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
      drawTicks: false,
    });

    // Crosshair move → update legend
    chart.subscribeCrosshairMove((param) => {
      if (!param || !param.time) {
        setLegend(null);
        return;
      }
      const candleData = param.seriesData?.get(candleSeries);
      const volumeData = param.seriesData?.get(volumeSeries);
      if (candleData) {
        setLegend({
          open: candleData.open,
          high: candleData.high,
          low: candleData.low,
          close: candleData.close,
          volume: volumeData?.value ?? 0,
          isUp: candleData.close >= candleData.open,
        });
      }
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

    // ResizeObserver for responsive
    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        chart.applyOptions({ width, height });
      }
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, []);

  // ─── Update data when klines change ─────────────────────
  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current || !klines?.length) return;

    // Sort by open_time ascending (LW Charts requires sorted data)
    const sorted = [...klines].sort(
      (a, b) => new Date(a.open_time).getTime() - new Date(b.open_time).getTime()
    );

    const candleData = sorted.map((k) => ({
      time: Math.floor(new Date(k.open_time).getTime() / 1000),
      open: parseFloat(k.open),
      high: parseFloat(k.high),
      low: parseFloat(k.low),
      close: parseFloat(k.close),
    }));

    const volumeData = sorted.map((k) => {
      const o = parseFloat(k.open);
      const c = parseFloat(k.close);
      return {
        time: Math.floor(new Date(k.open_time).getTime() / 1000),
        value: parseFloat(k.volume_base) || 0,
        color: c >= o ? '#0ecb8140' : '#f6465d40',
      };
    });

    candleSeriesRef.current.setData(candleData);
    volumeSeriesRef.current.setData(volumeData);

    // Only fit content on initial load or when data count changes drastically (interval switch)
    const prevLen = klinesLenRef.current;
    const newLen = klines.length;
    if (prevLen === 0 || Math.abs(newLen - prevLen) > newLen * 0.3) {
      chartRef.current?.timeScale().fitContent();
    }
    klinesLenRef.current = newLen;
  }, [klines]);

  // ─── Lazy-load on pan to left edge ─────────────────────
  useEffect(() => {
    if (!chartRef.current || !onLoadMore) return;

    const handler = (newRange) => {
      if (!newRange || isLoadingMoreRef.current) return;
      // When user pans left and visible range starts near the beginning of data
      if (newRange.from !== null && newRange.from < 10) {
        isLoadingMoreRef.current = true;
        onLoadMore('left');
        // Reset after a delay to prevent rapid re-fetching
        setTimeout(() => {
          isLoadingMoreRef.current = false;
        }, 2000);
      }
    };

    chartRef.current.timeScale().subscribeVisibleLogicalRangeChange(handler);
    return () => {
      chartRef.current?.timeScale().unsubscribeVisibleLogicalRangeChange(handler);
    };
  }, [onLoadMore]);

  // ─── Format helpers ─────────────────────────────────────
  const fmt = (v) => {
    if (v == null) return '-';
    if (v >= 1) return v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return v.toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 8 });
  };

  const fmtVol = (v) => {
    if (v == null) return '-';
    if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
    if (v >= 1e3) return (v / 1e3).toFixed(2) + 'K';
    return v.toFixed(2);
  };

  // ─── Render ─────────────────────────────────────────────
  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      {/* OHLCV Legend Overlay */}
      <div
        style={{
          position: 'absolute',
          top: 8,
          left: 12,
          zIndex: 10,
          display: 'flex',
          gap: 12,
          fontSize: 11,
          fontFamily: "'Inter', 'Segoe UI', monospace",
          pointerEvents: 'none',
          color: '#848e9c',
        }}
      >
        <span style={{ color: '#d1d5db', fontWeight: 600 }}>{interval.toUpperCase()}</span>
        {legend && (
          <>
            <span>
              O <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.open)}</b>
            </span>
            <span>
              H <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.high)}</b>
            </span>
            <span>
              L <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.low)}</b>
            </span>
            <span>
              C <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.close)}</b>
            </span>
            <span>
              Vol <b style={{ color: '#848e9c' }}>{fmtVol(legend.volume)}</b>
            </span>
          </>
        )}
      </div>

      {/* Chart Container */}
      <div
        ref={containerRef}
        style={{ width: '100%', height: '100%' }}
      />

      {/* No data message */}
      {(!klines || klines.length === 0) && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: '#6b7280',
            fontSize: 14,
            pointerEvents: 'none',
          }}
        >
          Không có dữ liệu klines
        </div>
      )}
    </div>
  );
}
