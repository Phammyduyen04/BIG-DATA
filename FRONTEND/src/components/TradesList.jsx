export default function TradesList({ trades }) {
  const fmt = (time) => {
    const d = new Date(time);
    return d.toLocaleTimeString('vi-VN', { hour12: false });
  };

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-[#2b3139]">
        <h3 className="text-sm font-semibold text-white">Các giao dịch gần đây nhất</h3>
        <p className="text-xs text-gray-500">Trade recently</p>
      </div>

      <div className="grid grid-cols-3 px-3 py-1 text-xs text-gray-500 border-b border-[#2b3139]">
        <span>Time</span>
        <span className="text-right">Price</span>
        <span className="text-right">Qty</span>
      </div>

      <div className="flex-1 overflow-y-auto">
        {trades.length === 0 ? (
          <div className="flex items-center justify-center h-full text-gray-500 text-xs">
            Không có dữ liệu
          </div>
        ) : (
          trades.map((t, i) => {
            const isBuy = !t.is_buyer_maker;
            return (
              <div
                key={i}
                className="grid grid-cols-3 px-3 py-[3px] text-xs hover:bg-[#1e2329] transition-colors"
              >
                <span className="text-gray-400">{fmt(t.trade_time)}</span>
                <span className={`text-right ${isBuy ? 'text-[#0ecb81]' : 'text-[#f6465d]'}`}>
                  {parseFloat(t.price).toLocaleString()}
                </span>
                <span className="text-right text-gray-300">
                  {parseFloat(t.qty_base).toFixed(4)}
                </span>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
