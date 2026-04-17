export default function SymbolSelector({ symbols, selected, onChange }) {
  return (
    <select
      value={selected}
      onChange={e => onChange(e.target.value)}
      className="bg-[#1e2329] text-white font-bold text-base px-3 py-1 rounded border border-[#2b3139] focus:outline-none cursor-pointer"
    >
      {symbols.map(s => (
        <option key={s.symbol_code} value={s.symbol_code}>
          {s.symbol_code}
        </option>
      ))}
    </select>
  );
}
