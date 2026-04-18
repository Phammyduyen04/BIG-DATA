const INTERVALS = ['1m', '5m', '15m', '30m', '1h', '4h', '1d'];

export default function IntervalSelector({ selected, onChange }) {
  return (
    <div className="flex items-center gap-3 px-4 py-2 bg-[#161a1e] border-t border-[#2b3139]">
      <div className="flex gap-1">
        {INTERVALS.map(iv => (
          <button
            key={iv}
            onClick={() => onChange(iv)}
            className={`px-3 py-1 text-xs rounded font-medium transition-colors
              ${selected === iv
                ? 'bg-[#f0b90b] text-black'
                : 'text-gray-400 hover:text-white hover:bg-[#2b3139]'
              }`}
          >
            {iv}
          </button>
        ))}
      </div>
    </div>
  );
}
