import { useEffect, useRef, useCallback, useState } from 'react';
import { createChart, CrosshairMode, CandlestickSeries, HistogramSeries } from 'lightweight-charts';

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

  const [legend, setLegend] = useState(null);
  const [tooltip, setTooltip] = useState(null); // { x, y, data }

  // Range selection state
  const [rangeSelect, setRangeSelect] = useState(null); // { startX, endX } in px
  const isDraggingRef = useRef(false);
  const dragStartXRef = useRef(null);
  const dragStartLogicalRef = useRef(null);
  const dragEndLogicalRef = useRef(null);

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

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#0ecb81',
      downColor: '#f6465d',
      borderUpColor: '#0ecb81',
      borderDownColor: '#f6465d',
      wickUpColor: '#0ecb81',
      wickDownColor: '#f6465d',
    });

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
      drawTicks: false,
    });

    // Crosshair move → update legend + tooltip
    chart.subscribeCrosshairMove((param) => {
      if (!param || !param.time || !param.point) {
        setLegend(null);
        setTooltip(null);
        return;
      }
      const candleData = param.seriesData?.get(candleSeries);
      const volumeData = param.seriesData?.get(volumeSeries);
      if (candleData) {
        const legendData = {
          open: candleData.open,
          high: candleData.high,
          low: candleData.low,
          close: candleData.close,
          volume: volumeData?.value ?? 0,
          isUp: candleData.close >= candleData.open,
          time: param.time,
        };
        setLegend(legendData);

        // Tooltip position — keep inside container
        const container = containerRef.current;
        if (container) {
          const rect = container.getBoundingClientRect();
          const x = param.point.x;
          const y = param.point.y;
          const tooltipW = 160;
          const tooltipH = 110;
          const left = x + tooltipW + 20 > rect.width ? x - tooltipW - 10 : x + 16;
          const top = y + tooltipH + 20 > rect.height ? y - tooltipH - 10 : y + 12;
          setTooltip({ left, top, data: legendData });
        }
      }
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

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
      if (newRange.from !== null && newRange.from < 10) {
        isLoadingMoreRef.current = true;
        onLoadMore('left');
        setTimeout(() => { isLoadingMoreRef.current = false; }, 2000);
      }
    };

    chartRef.current.timeScale().subscribeVisibleLogicalRangeChange(handler);
    return () => {
      chartRef.current?.timeScale().unsubscribeVisibleLogicalRangeChange(handler);
    };
  }, [onLoadMore]);

  // ─── Range selection via document-level listeners (bypasses chart capture) ───
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const onDown = (e) => {
      if (!e.altKey) return;
      e.preventDefault();
      e.stopPropagation();
      const rect = container.getBoundingClientRect();
      const x = e.clientX - rect.left;
      isDraggingRef.current = true;
      dragStartXRef.current = x;
      dragStartLogicalRef.current = chartRef.current?.timeScale().coordinateToLogical(x);
      dragEndLogicalRef.current = dragStartLogicalRef.current;
      setRangeSelect({ startX: x, endX: x });
    };

    const onMove = (e) => {
      if (!isDraggingRef.current) return;
      const rect = container.getBoundingClientRect();
      // endX không được nhỏ hơn startX — chỉ mở rộng sang phải
      const rawX = e.clientX - rect.left;
      const x = Math.max(rawX, dragStartXRef.current);
      dragEndLogicalRef.current = chartRef.current?.timeScale().coordinateToLogical(x);
      setRangeSelect({ startX: dragStartXRef.current, endX: x });
    };

    const onUp = () => {
      if (!isDraggingRef.current) return;
      isDraggingRef.current = false;

      const start = dragStartLogicalRef.current;
      const end = dragEndLogicalRef.current;

      if (start != null && end != null && end - start > 2) {
        chartRef.current?.timeScale().setVisibleLogicalRange({ from: start, to: end });
        if (onIntervalChange) onIntervalChange('15m');
      }

      setRangeSelect(null);
      dragStartXRef.current = null;
      dragStartLogicalRef.current = null;
      dragEndLogicalRef.current = null;
    };

    container.addEventListener('mousedown', onDown);
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);

    return () => {
      container.removeEventListener('mousedown', onDown);
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
  }, [onIntervalChange]);

  const handleMouseLeave = useCallback(() => {
    setTooltip(null);
    setLegend(null);
  }, []);

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

  const fmtTime = (unixSec) => {
    if (!unixSec) return '';
    const d = new Date(unixSec * 1000);
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  };

  // ─── Render ─────────────────────────────────────────────
  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      {/* OHLCV Legend Overlay */}
      <div
        style={{
          position: 'absolute', top: 8, left: 12, zIndex: 10,
          display: 'flex', gap: 12, fontSize: 11,
          fontFamily: "'Inter', 'Segoe UI', monospace",
          pointerEvents: 'none', color: '#848e9c',
        }}
      >
        <span style={{ color: '#d1d5db', fontWeight: 600 }}>{interval.toUpperCase()}</span>
        {legend && (
          <>
            <span>O <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.open)}</b></span>
            <span>H <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.high)}</b></span>
            <span>L <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.low)}</b></span>
            <span>C <b style={{ color: legend.isUp ? '#0ecb81' : '#f6465d' }}>{fmt(legend.close)}</b></span>
            <span>Vol <b style={{ color: '#848e9c' }}>{fmtVol(legend.volume)}</b></span>
          </>
        )}
      </div>

      {/* Alt+drag hint */}
      <div style={{
        position: 'absolute', top: 8, right: 12, zIndex: 10,
        fontSize: 10, color: '#4b5563', pointerEvents: 'none',
        fontFamily: "'Inter', sans-serif",
      }}>
        Alt + kéo để chọn vùng
      </div>

      {/* Chart Container */}
      <div
        ref={containerRef}
        style={{ width: '100%', height: '100%', cursor: 'crosshair' }}
        onMouseLeave={handleMouseLeave}
      />

      {/* Range selection overlay */}
      {rangeSelect && (
        <div
          style={{
            position: 'absolute',
            top: 0,
            left: Math.min(rangeSelect.startX, rangeSelect.endX),
            width: Math.abs(rangeSelect.endX - rangeSelect.startX),
            height: '100%',
            background: '#f0b90b18',
            border: '1px solid #f0b90b55',
            pointerEvents: 'none',
            zIndex: 5,
          }}
        />
      )}

      {/* Tooltip */}
      {tooltip && (
        <div
          style={{
            position: 'absolute',
            left: tooltip.left,
            top: tooltip.top,
            zIndex: 20,
            background: '#1e2329ee',
            border: '1px solid #2b3139',
            borderRadius: 6,
            padding: '8px 12px',
            pointerEvents: 'none',
            minWidth: 150,
            fontFamily: "'Inter', 'Segoe UI', monospace",
            fontSize: 11,
            boxShadow: '0 4px 16px rgba(0,0,0,0.5)',
          }}
        >
          <div style={{ color: '#848e9c', marginBottom: 6, fontSize: 10 }}>
            {fmtTime(tooltip.data.time)}
          </div>
          {[
            ['Open',  tooltip.data.open],
            ['High',  tooltip.data.high],
            ['Low',   tooltip.data.low],
            ['Close', tooltip.data.close],
          ].map(([label, val]) => (
            <div key={label} style={{ display: 'flex', justifyContent: 'space-between', gap: 16, marginBottom: 2 }}>
              <span style={{ color: '#6b7280' }}>{label}</span>
              <span style={{ color: tooltip.data.isUp ? '#0ecb81' : '#f6465d', fontWeight: 600 }}>
                {fmt(val)}
              </span>
            </div>
          ))}
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, marginTop: 4, borderTop: '1px solid #2b3139', paddingTop: 4 }}>
            <span style={{ color: '#6b7280' }}>Vol</span>
            <span style={{ color: '#848e9c' }}>{fmtVol(tooltip.data.volume)}</span>
          </div>
        </div>
      )}

      {/* No data message */}
      {(!klines || klines.length === 0) && (
        <div style={{
          position: 'absolute', inset: 0, display: 'flex',
          alignItems: 'center', justifyContent: 'center',
          color: '#6b7280', fontSize: 14, pointerEvents: 'none',
        }}>
          Không có dữ liệu klines
        </div>
      )}
    </div>
  );
}
