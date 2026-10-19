// 可點表頭排序：useSort 管排序狀態，SortTh 畫表頭按鈕（aria-sort、箭頭）
import React, { useMemo, useState } from "react";
import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";

// columns: { [key]: (row) => 排序值（數字／字串；null、NaN 一律排最後） }
// init: { key, dir } 預設排序
export function useSort(rows, columns, init) {
  const [sort, setSort] = useState(init);
  const sorted = useMemo(() => {
    const get = columns[sort.key];
    if (!get) return rows;
    const sign = sort.dir === "asc" ? 1 : -1;
    const empty = (v) => v == null || v === "" || (typeof v === "number" && !Number.isFinite(v));
    return rows.map((r, i) => [r, get(r), i]).sort((a, b) => {
      if (empty(a[1]) || empty(b[1])) return empty(a[1]) - empty(b[1]) || a[2] - b[2];
      const c = typeof a[1] === "number" && typeof b[1] === "number" ? a[1] - b[1] : String(a[1]).localeCompare(String(b[1]), "zh-Hant");
      return c * sign || a[2] - b[2];
    }).map((x) => x[0]);
  }, [rows, columns, sort]);
  // 點同一欄切換升降冪；換欄時文字欄先升冪、數字欄先降冪
  const toggle = (key, firstDir = "desc") => setSort((s) => (s.key === key ? { key, dir: s.dir === "asc" ? "desc" : "asc" } : { key, dir: firstDir }));
  return { sorted, sort, toggle };
}

export function SortTh({ k, label, sort, onSort, left = false, firstDir }) {
  const on = sort.key === k;
  const Icon = !on ? ChevronsUpDown : sort.dir === "asc" ? ArrowUp : ArrowDown;
  return (
    <th className={left ? "left" : undefined} aria-sort={on ? (sort.dir === "asc" ? "ascending" : "descending") : "none"}>
      <button type="button" className={`sort-btn ${on ? "on" : ""}`} onClick={() => onSort(k, firstDir ?? (left ? "asc" : "desc"))} title={`依「${label}」排序`}>
        {label}<Icon size={12} aria-hidden="true" />
      </button>
    </th>
  );
}
