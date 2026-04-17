import { useState } from 'react';

export default function CoinFilter({ symbols, trades, onSymbolChange }) {
  const [search, setSearch] = useState('');

  const filtered = symbols.filter(s =>
    s.symbol_code.toLowerCase().includes(search.toLowerCase())
  );

  // Tính giá gần nhất từ trades (nếu có)
  const latestPrice = trades.length > 0 ? parseFloat(trades[0].price) : null;

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-[#2b3139]">
        <h3 className="text-sm font-semibold text-white">Lọc các đồng coin</h3>
      </div>

      <div className="px-3 py-2 border-b border-[#2b3139]">
        <input
          type="text"
          placeholder="Tìm kiếm..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="w-full bg-[#1e2329] text-white text-xs px-2 py-1 rounded border border-[#2b3139] focus:outline-none focus:border-[#f0b90b] placeholder-gray-500"
        />
      </div>

      <div className="grid grid-cols-3 px-3 py-1 text-xs text-gray-500 border-b border-[#2b3139]">
        <span>Symbol</span>
        <span className="text-right">Price</span>
        <span className="text-right">Base</span>
      </div>

      <div className="flex-1 overflow-y-auto">
        {filtered.map(s => (
          <div
            key={s.symbol_code}
            onClick={() => onSymbolChange(s.symbol_code)}
            className="grid grid-cols-3 px-3 py-[5px] text-xs hover:bg-[#1e2329] cursor-pointer transition-colors"
          >
            <span className="text-white font-medium">{s.symbol_code}</span>
            <span className="text-right text-gray-300">
              {latestPrice ? latestPrice.toLocaleString() : '—'}
            </span>
            <span className="text-right text-gray-500">{s.base_asset}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
