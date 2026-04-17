import ReactApexChart from 'react-apexcharts';
import { useMemo, useCallback, useState, useEffect, useRef } from 'react';

const INTERVALS = ['1m', '5m', '15m', '30m', '1h', '4h', '1d'];

const DATETIME_FORMATTER = {
  '1m':  { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '5m':  { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '15m': { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '30m': { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '1h':  { hour: 'dd MMM HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '4h':  { hour: 'dd MMM HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '1d':  { day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
};

const IconCursor = ({ active }) => (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
    stroke={active ? '#f0b90b' : '#848e9c'} strokeWidth="1.8"
    strokeLinecap="round" strokeLinejoin="round">
    <path d="M5 3l14 9-7 1-4 7z"/>
  </svg>
);

const IconHand = ({ active }) => (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
    stroke={active ? '#f0b90b' : '#848e9c'} strokeWidth="1.8"
    strokeLinecap="round" strokeLinejoin="round">
    <path d="M18 11V8a2 2 0 0 0-4 0v3M14 11V6a2 2 0 0 0-4 0v5M10 11V8a2 2 0 0 0-4 0v6a8 8 0 0 0 16 0v-3a2 2 0 0 0-4 0"/>
  </svg>
);

const IconZoomIn = () => (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
    stroke="#848e9c" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="11" cy="11" r="7"/>
    <path d="m21 21-4.35-4.35M11 8v6M8 11h6"/>
  </svg>
);

const IconZoomOut = () => (
  <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
    stroke="#848e9c" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="11" cy="11" r="7"/>
    <path d="m21 21-4.35-4.35M8 11h6"/>
  </svg>
);

const Btn = ({ onClick, title, active, children }) => (
  <button
    onClick={onClick}
    title={title}
    style={{
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      width: 28, height: 28, borderRadius: 4, cursor: 'pointer',
      border: 'none', background: active ? '#2b3139' : 'transparent',
      transition: 'background 0.15s',
    }}
    onMouseEnter={e => e.currentTarget.style.background = '#2b3139'}
    onMouseLeave={e => e.currentTarget.style.background = active ? '#2b3139' : 'transparent'}
  >
    {children}
  </button>
);

// 'zoom' = cursor selection (default), 'pan' = hand drag
export default function CandlestickChart({ klines, interval = '1h', onZoom, onIntervalChange }) {
  const [toolMode, setToolMode] = useState(interval === '1m' ? 'pan' : 'zoom');
  const [chartKey, setChartKey] = useState(0);

  useEffect(() => {
    setToolMode(interval === '1m' ? 'pan' : 'zoom');
    setChartKey(k => k + 1);
  }, [interval]);

  const setMode = useCallback((mode) => {
    setToolMode(mode);
    setChartKey(k => k + 1);
  }, []);

  const handleZoomIn = useCallback(() => {
    const idx = INTERVALS.indexOf(interval);
    if (idx > 0) onIntervalChange?.(INTERVALS[idx - 1]);
  }, [interval, onIntervalChange]);

  const handleZoomOut = useCallback(() => {
    const idx = INTERVALS.indexOf(interval);
    if (idx < INTERVALS.length - 1) onIntervalChange?.(INTERVALS[idx + 1]);
  }, [interval, onIntervalChange]);

  const handleZoomOrScroll = useCallback((_, { xaxis }) => {
    if (onZoom && xaxis?.min != null && xaxis?.max != null) {
      onZoom(xaxis.max - xaxis.min);
    }
  }, [onZoom]);

  const series = useMemo(() => {
    if (!klines?.length) return [];
    const sorted = [...klines].sort((a, b) => new Date(a.open_time) - new Date(b.open_time));
    const candleData = sorted.map(k => ({
      x: new Date(k.open_time),
      y: [parseFloat(k.open), parseFloat(k.high), parseFloat(k.low), parseFloat(k.close)],
    }));
    const volumeData = sorted.map(k => ({
      x: new Date(k.open_time),
      y: parseFloat(k.volume_base) || 0,
      fillColor: parseFloat(k.close) >= parseFloat(k.open) ? '#0ecb8166' : '#f6465d66',
    }));
    return [
      { name: 'Candle', type: 'candlestick', data: candleData },
      { name: 'Volume', type: 'bar', data: volumeData },
    ];
  }, [klines]);

  const maxVolume = useMemo(() => {
    if (!klines?.length) return 1;
    return Math.max(...klines.map(k => parseFloat(k.volume_base) || 0));
  }, [klines]);

  const options = useMemo(() => ({
    chart: {
      type: 'candlestick',
      background: '#0b0e11',
      toolbar: { show: false, autoSelected: toolMode },
      zoom: { enabled: true, type: 'x' },
      animations: { enabled: false },
      events: { zoomed: handleZoomOrScroll, scrolled: handleZoomOrScroll },
    },
    theme: { mode: 'dark' },
    xaxis: {
      type: 'datetime',
      labels: {
        style: { colors: '#848e9c' },
        datetimeFormatter: DATETIME_FORMATTER[interval] ?? DATETIME_FORMATTER['1h'],
      },
      axisBorder: { color: '#2b3139' },
      axisTicks: { color: '#2b3139' },
    },
    yaxis: [
      {
        seriesName: 'Candle',
        tooltip: { enabled: true },
        labels: { style: { colors: '#848e9c' }, formatter: v => v?.toLocaleString() },
      },
      {
        seriesName: 'Volume',
        opposite: false, show: false,
        max: maxVolume * 5, min: 0,
        labels: { show: false },
      },
    ],
    grid: { borderColor: '#1e2329', strokeDashArray: 0 },
    plotOptions: {
      candlestick: {
        colors: { upward: '#0ecb81', downward: '#f6465d' },
        wick: { useFillColor: true },
      },
      bar: { columnWidth: '80%' },
    },
    stroke: { show: true, width: [1, 0], colors: ['transparent', 'transparent'] },
    tooltip: {
      shared: true, theme: 'dark',
      x: { format: 'dd MMM yyyy HH:mm' },
      y: [
        {
          formatter: (val, { dataPointIndex, w }) => {
            const d = w.config.series[0]?.data?.[dataPointIndex];
            if (!d) return '';
            const [o, h, l, c] = d.y;
            return `O: ${o?.toLocaleString()}  H: ${h?.toLocaleString()}  L: ${l?.toLocaleString()}  C: ${c?.toLocaleString()}`;
          },
        },
        {
          formatter: v => (v != null ? v.toLocaleString(undefined, { maximumFractionDigits: 4 }) : ''),
          title: { formatter: () => 'Vol: ' },
        },
      ],
    },
    legend: { show: false },
    dataLabels: { enabled: false },
  }), [interval, maxVolume, handleZoomOrScroll, toolMode]);

  if (!klines?.length) {
    return (
      <div style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280', fontSize: 14 }}>
        Không có dữ liệu klines
      </div>
    );
  }

  const canZoomIn  = INTERVALS.indexOf(interval) > 0;
  const canZoomOut = INTERVALS.indexOf(interval) < INTERVALS.length - 1;

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      {/* Custom toolbar */}
      <div style={{
        position: 'absolute', top: 8, right: 12, zIndex: 10,
        display: 'flex', gap: 2, background: '#161a1e',
        borderRadius: 6, padding: '2px 4px', border: '1px solid #2b3139',
      }}>
        <Btn onClick={() => setMode('zoom')} active={toolMode === 'zoom'}
          title="Cursor — kéo chọn vùng để phóng to (mặc định)">
          <IconCursor active={toolMode === 'zoom'} />
        </Btn>
        <Btn onClick={() => setMode('pan')} active={toolMode === 'pan'}
          title="Pan — nhấn giữ để kéo trái/phải">
          <IconHand active={toolMode === 'pan'} />
        </Btn>
        <Btn onClick={handleZoomIn} active={false}
          title={canZoomIn ? `Zoom in → ${INTERVALS[INTERVALS.indexOf(interval) - 1]}` : 'Đã ở interval nhỏ nhất'}>
          <IconZoomIn />
        </Btn>
        <Btn onClick={handleZoomOut} active={false}
          title={canZoomOut ? `Zoom out → ${INTERVALS[INTERVALS.indexOf(interval) + 1]}` : 'Đã ở interval lớn nhất'}>
          <IconZoomOut />
        </Btn>
      </div>

      <ReactApexChart
        key={chartKey}
        options={options}
        series={series}
        type="candlestick"
        width="100%"
        height="100%"
      />
    </div>
  );
}
