import SymbolSelector from './SymbolSelector';

export default function Ticker24h({ ticker, symbols, selectedSymbol, onSymbolChange }) {
  if (!ticker) return (
    <div className="flex items-center gap-4 px-4 py-3 bg-[#161a1e] border-b border-[#2b3139]">
      <SymbolSelector symbols={symbols} selected={selectedSymbol} onChange={onSymbolChange} />
      <span className="text-gray-500 text-sm">Đang tải...</span>
    </div>
  );

  const isUp = parseFloat(ticker.price_change_percent) >= 0;
  const color = isUp ? 'text-[#0ecb81]' : 'text-[#f6465d]';

  return (
    <div className="flex flex-wrap items-center gap-4 px-4 py-3 bg-[#161a1e] border-b border-[#2b3139]">
      <SymbolSelector symbols={symbols} selected={selectedSymbol} onChange={onSymbolChange} />

      <span className={`text-xl font-bold ${color}`}>
        {parseFloat(ticker.last_price).toLocaleString()}
      </span>

      <div className="flex flex-wrap gap-4 text-sm text-gray-400">
        <div>
          <span className="text-gray-500 mr-1">24h Change</span>
          <span className={color}>
            {isUp ? '+' : ''}{parseFloat(ticker.price_change_percent).toFixed(2)}%
          </span>
        </div>
        <div>
          <span className="text-gray-500 mr-1">H:</span>
          <span className="text-white">{parseFloat(ticker.high_price).toLocaleString()}</span>
        </div>
        <div>
          <span className="text-gray-500 mr-1">L:</span>
          <span className="text-white">{parseFloat(ticker.low_price).toLocaleString()}</span>
        </div>
        <div>
          <span className="text-gray-500 mr-1">Vol(24h):</span>
          <span className="text-white">{parseFloat(ticker.volume_base_24h).toLocaleString()}</span>
        </div>
        <div>
          <span className="text-gray-500 mr-1">Trades:</span>
          <span className="text-white">{Number(ticker.trade_count).toLocaleString()}</span>
        </div>
      </div>
    </div>
  );
}
