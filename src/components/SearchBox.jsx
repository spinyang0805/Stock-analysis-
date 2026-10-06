import React, { useEffect, useId, useState } from "react";
import { Search } from "lucide-react";
import { loadStockList, searchStockList } from "../lib/data.js";

export default function SearchBox({ onSelect, placeholder = "代號或名稱，例：2330、台積電" }) {
  const [q, setQ] = useState("");
  const [list, setList] = useState([]);
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const id = useId();

  useEffect(() => {
    if (!q.trim()) { setList([]); return undefined; }
    const t = setTimeout(async () => setList(searchStockList(await loadStockList(), q)), 120);
    return () => clearTimeout(t);
  }, [q]);

  function pick(item) {
    if (!item) return;
    onSelect(item);
    setQ(""); setOpen(false); setActive(0);
  }

  function onKey(e) {
    if (e.key === "ArrowDown") { e.preventDefault(); setActive((a) => Math.min(a + 1, list.length - 1)); setOpen(true); }
    else if (e.key === "ArrowUp") { e.preventDefault(); setActive((a) => Math.max(a - 1, 0)); }
    else if (e.key === "Enter") {
      e.preventDefault();
      if (list[active]) pick(list[active]);
      else if (/^[0-9A-Z]{4,6}$/i.test(q.trim())) pick({ code: q.trim().toUpperCase(), name: q.trim().toUpperCase() });
    } else if (e.key === "Escape") setOpen(false);
  }

  return (
    <div className="searchbox">
      <Search size={16} className="icon" aria-hidden="true" />
      <input
        className="input" value={q} placeholder={placeholder} aria-label="搜尋股票"
        role="combobox" aria-expanded={open && list.length > 0} aria-controls={id} aria-autocomplete="list"
        aria-activedescendant={open && list[active] ? `${id}-${active}` : undefined}
        onChange={(e) => { setQ(e.target.value); setOpen(true); setActive(0); }}
        onFocus={() => setOpen(true)} onBlur={() => setTimeout(() => setOpen(false), 150)} onKeyDown={onKey}
      />
      {open && list.length > 0 && (
        <div className="suggest" role="listbox" id={id}>
          {list.map((item, i) => (
            <div key={item.code} id={`${id}-${i}`} role="option" aria-selected={i === active}
              onMouseDown={(e) => { e.preventDefault(); pick(item); }} onMouseEnter={() => setActive(i)}>
              <b className="num" style={{ color: "var(--gold)" }}>{item.code}</b>
              <span>{item.name}</span>
              <span className="dim" style={{ marginLeft: "auto" }}>{item.market}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
