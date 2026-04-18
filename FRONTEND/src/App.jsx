import { useState, useEffect, useCallback, useRef } from 'react';
import { getSymbols, getTicker24h, getKlines, getTrades, getKlineLatestTime } from './api/marketApi';
import Ticker24h from './components/Ticker24h';
import CandlestickChart from './components/CandlestickChart';
import IntervalSelector from './components/IntervalSelector';
import TradesList from './components/TradesList';
import CoinFilter from './components/CoinFilter';
import TopCoinsChart from './components/TopCoinsChart';

const TABS = ['Hiển thị', 'Bài toán'];

const RANGE_MS = {
  '1d':  1 * 24 * 60 * 60 * 1000,
  '5d':  5 * 24 * 60 * 60 * 1000,
  '10d': 10 * 24 * 60 * 60 * 1000,
  '1mo': 30 * 24 * 60 * 60 * 1000,
  '3mo': 90 * 24 * 60 * 60 * 1000,
};

// Visible range buttons (subset shown in UI)
const DISPLAY_RANGES = ['10d', '1mo', '3mo'];

function findClosestRange(visibleMs) {
  return DISPLAY_RANGES.reduce((best, r) =>
    Math.abs(RANGE_MS[r] - visibleMs) < Math.abs(RANGE_MS[best] - visibleMs) ? r : best
  );
}

const INTERVAL_DEFAULT_RANGE = {
  '1m':  '10d',
  '5m':  '10d',
  '15m': '10d',
  '30m': '1mo',
  '1h':  '1mo',
  '4h':  '3mo',
  '1d':  '3mo',
};

// Milliseconds per interval — used to compute how many candles to fetch for lazy loading
const INTERVAL_MS = {
  '1m':  60_000,
  '5m':  300_000,
  '15m': 900_000,
  '30m': 1_800_000,
  '1h':  3_600_000,
  '4h':  14_400_000,
  '1d':  86_400_000,
};

// How many extra candles to fetch when panning to edge
const LOAD_MORE_CANDLES = 300;

export default function App() {
  const [activeTab, setActiveTab] = useState(0);
  const [symbols, setSymbols] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState('BTCUSDT');
  const [selectedInterval, setSelectedInterval] = useState('1h');
  const [selectedRange, setSelectedRange] = useState(INTERVAL_DEFAULT_RANGE['1h']);
  const [activeRange, setActiveRange] = useState(INTERVAL_DEFAULT_RANGE['1h']);
  const [ticker, setTicker] = useState(null);
  const [klines, setKlines] = useState([]);
  const [trades, setTrades] = useState([]);
  const [error, setError] = useState(null);

  // Track the earliest kline time we've loaded (for lazy-loading older data)
  const earliestTimeRef = useRef(null);

  useEffect(() => {
    getSymbols()
      .then(data => {
        setSymbols(data);
        if (data.length > 0 && !data.find(s => s.symbol_code === 'BTCUSDT')) {
          setSelectedSymbol(data[0].symbol_code);
        }
      })
      .catch(e => setError('Không thể kết nối backend: ' + e.message));
  }, []);

  const handleIntervalChange = useCallback(iv => {
    setSelectedInterval(iv);
    const r = INTERVAL_DEFAULT_RANGE[iv];
    setSelectedRange(r);
    setActiveRange(r);
    // Reset klines + earliest time when interval changes
    setKlines([]);
    earliestTimeRef.current = null;
  }, []);

  const handleRangeChange = useCallback(r => {
    setSelectedRange(r);
    setActiveRange(r);
    // Reset klines + earliest time when range changes
    setKlines([]);
    earliestTimeRef.current = null;
  }, []);

  const handleChartZoom = useCallback(visibleMs => {
    setActiveRange(findClosestRange(visibleMs));
  }, []);

  const fetchTickerAndTrades = useCallback(async () => {
    if (!selectedSymbol) return;
    try {
      const [tickerData, tradesData] = await Promise.all([
        getTicker24h(selectedSymbol),
        getTrades(selectedSymbol, 50),
      ]);
      setTicker(tickerData);
      setTrades(tradesData);
      setError(null);
    } catch (e) {
      setError('Lỗi khi tải dữ liệu: ' + e.message);
    }
  }, [selectedSymbol]);

  // ─── Initial klines fetch ───────────────────────────────
  const fetchKlines = useCallback(async () => {
    if (!selectedSymbol) return;
    try {
      // Anchor range to latest available data time (not `now`) to handle historical datasets
      const latestTime = await getKlineLatestTime(selectedSymbol, selectedInterval);
      const end = latestTime ? new Date(latestTime) : new Date();
      const start = new Date(end.getTime() - (RANGE_MS[selectedRange] ?? RANGE_MS['1mo']));
      const data = await getKlines(selectedSymbol, selectedInterval, start.toISOString(), end.toISOString());

      setKlines(data);
      // Track earliest time for lazy loading
      if (data.length > 0) {
        const times = data.map(k => new Date(k.open_time).getTime());
        earliestTimeRef.current = new Date(Math.min(...times));
      }
    } catch (e) {
      console.error('Klines error:', e);
    }
  }, [selectedSymbol, selectedInterval, selectedRange]);

  // ─── Lazy load more data when user pans to edge ────────
  const handleLoadMore = useCallback(async (direction) => {
    if (direction !== 'left' || !selectedSymbol || !earliestTimeRef.current) return;

    try {
      const intervalMs = INTERVAL_MS[selectedInterval] || 60_000;
      const loadMs = LOAD_MORE_CANDLES * intervalMs;

      const end = new Date(earliestTimeRef.current.getTime());
      const start = new Date(end.getTime() - loadMs);

      const moreData = await getKlines(
        selectedSymbol,
        selectedInterval,
        start.toISOString(),
        end.toISOString()
      );

      if (moreData.length > 0) {
        // Update earliest time
        const times = moreData.map(k => new Date(k.open_time).getTime());
        earliestTimeRef.current = new Date(Math.min(...times));

        // Merge & deduplicate by open_time
        setKlines(prev => {
          const existingTimes = new Set(prev.map(k => k.open_time));
          const newItems = moreData.filter(k => !existingTimes.has(k.open_time));
          return [...newItems, ...prev];
        });
      }
    } catch (e) {
      console.error('Load more klines error:', e);
    }
  }, [selectedSymbol, selectedInterval]);

  useEffect(() => { fetchTickerAndTrades(); }, [fetchTickerAndTrades]);
  useEffect(() => { fetchKlines(); }, [fetchKlines]);

  useEffect(() => {
    const id = window.setInterval(fetchTickerAndTrades, 10000);
    return () => window.clearInterval(id);
  }, [fetchTickerAndTrades]);

  return (
    <div className="flex flex-col bg-[#0b0e11] text-white" style={{ height: '100vh', overflow: 'hidden' }}>

      {/* Tab Bar */}
      <div className="flex border-b border-[#2b3139] bg-[#161a1e]" style={{ flexShrink: 0 }}>
        {TABS.map((tab, i) => (
          <button
            key={tab}
            onClick={() => setActiveTab(i)}
            className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
              activeTab === i
                ? 'border-[#f0b90b] text-[#f0b90b]'
                : 'border-transparent text-gray-400 hover:text-white'
            }`}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Error Banner */}
      {error && (
        <div className="bg-red-900 text-red-200 text-xs px-4 py-2" style={{ flexShrink: 0 }}>
          {error}
        </div>
      )}

      {/* Tab: Hiển thị */}
      {activeTab === 0 && (
        <div className="flex" style={{ flex: 1, minHeight: 0 }}>

          {/* LEFT */}
          <div className="flex flex-col border-r border-[#2b3139]" style={{ flex: 1, minWidth: 0 }}>
            <Ticker24h
              ticker={ticker}
              symbols={symbols}
              selectedSymbol={selectedSymbol}
              onSymbolChange={setSelectedSymbol}
            />
            <div style={{ flex: 1, minHeight: 0, background: '#0b0e11' }}>
              <CandlestickChart
                klines={klines}
                interval={selectedInterval}
                onLoadMore={handleLoadMore}
                onIntervalChange={handleIntervalChange}
              />
            </div>
            <IntervalSelector
              selected={selectedInterval}
              onChange={handleIntervalChange}
            />
          </div>

          {/* RIGHT */}
          <div className="flex flex-col" style={{ width: 256, flexShrink: 0 }}>
            <div className="border-b border-[#2b3139]" style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>
              <TradesList trades={trades} />
            </div>
            <div style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>
              <CoinFilter symbols={symbols} onSymbolChange={setSelectedSymbol} />
            </div>
          </div>
        </div>
      )}

      {/* Tab: Bài toán */}
      {activeTab === 1 && (
        <div style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>
          <TopCoinsChart />
        </div>
      )}
    </div>
  );
}
