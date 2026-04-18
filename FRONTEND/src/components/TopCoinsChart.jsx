import { useEffect, useState, useCallback, useRef } from 'react';
import ReactApexChart from 'react-apexcharts';
import { getTopCoins, getTopCoinDetail } from '../api/marketApi';

const SIGNAL_COLOR = {
  ACCUMULATING: '#0ecb81',
  DISTRIBUTING: '#f6465d',
  NEUTRAL:      '#f0b90b',
};

const fmt = (v, decimals = 2) =>
  v == null ? '—' : Number(v).toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });

const fmtPct = (v) => v == null ? '—' : `${fmt(Number(v) * 100)}%`;

const fmtLarge = (v) => {
  if (v == null) return '—';
  const n = Number(v);
  if (n >= 1e9) return `$${fmt(n / 1e9)}B`;
  if (n >= 1e6) return `$${fmt(n / 1e6)}M`;
  return `$${fmt(n)}`;
};

const DETAIL_FIELDS = [
  { key: 'total_volume_quote',  label: 'Total Volume Quote',   format: fmtLarge },
  { key: 'avg_taker_buy_ratio', label: 'Avg Taker Buy Ratio',  format: fmtPct },
  { key: 'bullish_ratio',       label: 'Bullish Ratio',        format: fmtPct },
  { key: 'volume_trend',        label: 'Volume Trend',         format: (v) => fmt(v, 4) },
  { key: 'money_flow',          label: 'Money Flow',           format: fmtLarge },
];

export default function TopCoinsChart() {
  const [coins, setCoins]                 = useState([]);
  const [selected, setSelected]           = useState(null);
  const [detail, setDetail]               = useState(null);
  const [loading, setLoading]             = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError]                 = useState(null);
  const containerRef                      = useRef(null);

  useEffect(() => {
    setLoading(true);
    getTopCoins()
      .then(data => { setCoins(data ?? []); setError(null); })
      .catch(() => setError('Không thể tải dữ liệu Top Coins'))
      .finally(() => setLoading(false));
  }, []);

  // Scroll up to dismiss detail panel
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const onWheel = (e) => {
      if (e.deltaY < 0 && selected) {
        setSelected(null);
        setDetail(null);
      }
    };
    el.addEventListener('wheel', onWheel, { passive: true });
    return () => el.removeEventListener('wheel', onWheel);
  }, [selected]);

  const handleBarClick = useCallback(async (symbolCode) => {
    if (selected === symbolCode) {
      setSelected(null);
      setDetail(null);
      return;
    }
    setSelected(symbolCode);
    setDetailLoading(true);
    try {
      const d = await getTopCoinDetail(symbolCode);
      setDetail(d);
    } catch {
      setDetail(null);
    } finally {
      setDetailLoading(false);
    }
  }, [selected]);

  // --- Derived data ---
  const detailVisible = !!selected;

  const categories = coins.map(c => c.symbol_code.replace('USDT', ''));
  const scores     = coins.map(c => +(Number(c.long_term_score) * 100).toFixed(2));
  // Khi có cột được chọn: cột được chọn giữ màu, cột còn lại mờ đi
  const colors = coins.map(c => {
    const base = SIGNAL_COLOR[c.signal] ?? '#848e9c';
    if (!selected) return base;
    return c.symbol_code === selected ? base : base + '55';
  });

  const options = {
    chart: {
      type: 'bar',
      background: '#0b0e11',
      toolbar: { show: false },
      animations: { enabled: true, speed: 400 },
      events: {
        dataPointSelection: (_, __, config) => {
          const sym = coins[config.dataPointIndex]?.symbol_code;
          if (sym) handleBarClick(sym);
        },
      },
    },
    theme: { mode: 'dark' },
    plotOptions: {
      bar: {
        distributed: true,
        borderRadius: 4,
        columnWidth: '55%',
        dataLabels: { position: 'top' },
      },
    },
    colors,
    dataLabels: {
      enabled: true,
      formatter: v => `${v}%`,
      style: { fontSize: '11px', colors: ['#eaecef'] },
      offsetY: -20,
    },
    legend: { show: false },
    xaxis: {
      categories,
      labels: { style: { colors: '#848e9c', fontSize: '12px' } },
      axisBorder: { color: '#2b3139' },
      axisTicks: { color: '#2b3139' },
    },
    yaxis: {
      title: { text: 'Score (%)', style: { color: '#848e9c', fontSize: '12px' } },
      labels: {
        formatter: v => `${v}%`,
        style: { colors: '#848e9c' },
      },
      max: 100,
      min: 0,
    },
    grid: { borderColor: '#1e2329', strokeDashArray: 3 },
    tooltip: {
      theme: 'dark',
      custom: ({ dataPointIndex }) => {
        const c = coins[dataPointIndex];
        if (!c) return '';
        const signalColor = SIGNAL_COLOR[c.signal] ?? '#848e9c';
        return `
          <div style="padding:10px 14px;background:#1e2329;border:1px solid #2b3139;border-radius:6px;font-size:13px;line-height:1.8">
            <div style="font-weight:600;color:#eaecef;margin-bottom:4px">${c.symbol_code}</div>
            <div>Score: <b style="color:#f0b90b">${(Number(c.long_term_score)*100).toFixed(2)}%</b></div>
            <div>Signal: <b style="color:${signalColor}">${c.signal}</b></div>
            <div>Total Trades: <b style="color:#eaecef">${Number(c.total_num_trades).toLocaleString()}</b></div>
          </div>`;
      },
    },
    // Tắt hoàn toàn filter active của ApexCharts — màu cột được kiểm soát bằng colors array
    states: {
      active: { filter: { type: 'none' } },
    },
  };

  const series = [{ name: 'Long-term Score', data: scores }];

  // --- Render ---
  if (loading) return (
    <div className="flex items-center justify-center h-full text-gray-500 text-sm">
      Đang tải dữ liệu...
    </div>
  );

  if (error) return (
    <div className="flex items-center justify-center h-full text-red-400 text-sm">{error}</div>
  );

  if (!coins.length) return (
    <div className="flex items-center justify-center h-full text-gray-500 text-sm">
      Không có dữ liệu Top Coins
    </div>
  );

  return (
    <div ref={containerRef} className="flex flex-col h-full" style={{ padding: '16px 20px', gap: 16, overflow: 'hidden' }}>
      {/* Header */}
      <div style={{ flexShrink: 0 }}>
        <h2 className="text-base font-semibold text-white">Top {coins.length} Coin</h2>
        <p className="text-xs text-gray-500 mt-0.5">
          Xếp hạng theo Long-term Score · Click vào cột để xem chi tiết · Scroll lên để đóng
        </p>
      </div>

      {/* Chart */}
      <div style={{ flex: detailVisible ? '0 0 55%' : '1 1 auto', minHeight: 0 }}>
        <ReactApexChart
          options={options}
          series={series}
          type="bar"
          width="100%"
          height="100%"
        />
      </div>

      {/* Detail panel */}
      {selected && (
        <div style={{
          background: '#161a1e',
          border: '1px solid #2b3139',
          borderRadius: 8,
          padding: '14px 20px',
          flexShrink: 0,
        }}>
          {detailLoading ? (
            <p className="text-gray-500 text-sm">Đang tải chi tiết...</p>
          ) : detail ? (
            <>
              <div className="flex items-center gap-3 mb-3">
                <span className="text-white font-semibold text-sm">{detail.symbol_code}</span>
                <span style={{
                  fontSize: 11, padding: '2px 8px', borderRadius: 4,
                  background: (SIGNAL_COLOR[detail.signal] ?? '#848e9c') + '22',
                  color: SIGNAL_COLOR[detail.signal] ?? '#848e9c',
                  border: `1px solid ${SIGNAL_COLOR[detail.signal] ?? '#848e9c'}55`,
                  fontWeight: 600,
                }}>
                  {detail.signal}
                </span>
                <span className="text-xs text-gray-500 ml-auto">
                  Rank #{detail.rank}
                </span>
              </div>
              <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(5, 1fr)',
                gap: '10px 16px',
              }}>
                {DETAIL_FIELDS.map(({ key, label, format }) => (
                  <div key={key} style={{
                    background: '#0b0e11',
                    borderRadius: 6,
                    padding: '10px 12px',
                    border: '1px solid #2b3139',
                  }}>
                    <div className="text-gray-500" style={{ fontSize: 11, marginBottom: 4 }}>{label}</div>
                    <div className="text-white font-semibold" style={{ fontSize: 14 }}>
                      {format(detail[key])}
                    </div>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <p className="text-red-400 text-sm">Không tải được chi tiết</p>
          )}
        </div>
      )}
    </div>
  );
}
