import ReactApexChart from 'react-apexcharts';
import { useMemo, useCallback } from 'react';

const DATETIME_FORMATTER = {
  '1m':  { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '5m':  { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '15m': { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '30m': { hour: 'HH:mm', minute: 'HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '1h':  { hour: 'dd MMM HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '4h':  { hour: 'dd MMM HH:mm', day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
  '1d':  { day: 'dd MMM', month: "MMM 'yy", year: 'yyyy' },
};

export default function CandlestickChart({ klines, interval = '1h', onZoom }) {
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

  const handleZoomOrScroll = useCallback((_, { xaxis }) => {
    if (onZoom && xaxis?.min != null && xaxis?.max != null) {
      onZoom(xaxis.max - xaxis.min);
    }
  }, [onZoom]);

  const options = useMemo(() => ({
    chart: {
      type: 'candlestick',
      background: '#0b0e11',
      toolbar: { show: false, autoSelected: 'pan' },
      zoom: { enabled: true, type: 'x' },
      animations: { enabled: false },
      events: {
        zoomed: handleZoomOrScroll,
        scrolled: handleZoomOrScroll,
      },
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
        labels: {
          style: { colors: '#848e9c' },
          formatter: v => v?.toLocaleString(),
        },
      },
      {
        seriesName: 'Volume',
        opposite: false,
        show: false,
        max: maxVolume * 5,
        min: 0,
        labels: { show: false },
      },
    ],
    grid: {
      borderColor: '#1e2329',
      strokeDashArray: 0,
    },
    plotOptions: {
      candlestick: {
        colors: { upward: '#0ecb81', downward: '#f6465d' },
        wick: { useFillColor: true },
      },
      bar: { columnWidth: '80%' },
    },
    stroke: {
      show: true,
      width: [1, 0],
      colors: ['transparent', 'transparent'],
    },
    tooltip: {
      shared: true,
      theme: 'dark',
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
  }), [interval, maxVolume, handleZoomOrScroll]);

  if (!klines?.length) {
    return (
      <div style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6b7280', fontSize: 14 }}>
        Không có dữ liệu klines
      </div>
    );
  }

  return (
    <div style={{ width: '100%', height: '100%' }}>
      <ReactApexChart
        options={options}
        series={series}
        type="candlestick"
        width="100%"
        height="100%"
      />
    </div>
  );
}
