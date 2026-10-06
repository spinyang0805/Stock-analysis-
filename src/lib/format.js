// 數字格式與台股漲跌色（紅漲綠跌）

export const UP = "#ef4444";
export const DOWN = "#22c55e";
export const FLAT = "#94a3b8";

export function num(...vs) {
  for (const v of vs) {
    if (v === null || v === undefined || v === "") continue;
    const n = Number(String(v).replace(/,/g, ""));
    if (Number.isFinite(n)) return n;
  }
  return NaN;
}

export function fmt(v, d = 2) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toLocaleString("zh-TW", { maximumFractionDigits: d }) : "--";
}

export function fmtInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n).toLocaleString("zh-TW") : "--";
}

// 帶正負號與 ▲▼，不只靠顏色表達漲跌
export function signed(v, d = 2, suffix = "") {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  const arrow = n > 0 ? "▲" : n < 0 ? "▼" : "";
  return `${arrow}${n > 0 ? "+" : n < 0 ? "−" : ""}${Math.abs(n).toLocaleString("zh-TW", { maximumFractionDigits: d })}${suffix}`;
}

export function trendColor(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n === 0) return FLAT;
  return n > 0 ? UP : DOWN;
}

// 大金額：萬 / 億
export function fmtMoney(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "--";
  const a = Math.abs(n);
  if (a >= 1e8) return `${(n / 1e8).toFixed(2)} 億`;
  if (a >= 1e4) return `${(n / 1e4).toFixed(1)} 萬`;
  return fmtInt(n);
}

export function toISODate(d) {
  const s = String(d ?? "").trim();
  if (/^\d{7}$/.test(s)) return `${Number(s.slice(0, 3)) + 1911}-${s.slice(3, 5)}-${s.slice(5, 7)}`; // 民國 1151001
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  if (/^\d{4}\/\d{1,2}\/\d{1,2}/.test(s)) {
    const [y, m, dd] = s.split(/[/\s]/);
    return `${y}-${m.padStart(2, "0")}-${dd.padStart(2, "0")}`;
  }
  // 民國年 113/01/05
  if (/^\d{2,3}\/\d{1,2}\/\d{1,2}/.test(s)) {
    const [y, m, dd] = s.split(/[/\s]/);
    return `${Number(y) + 1911}-${m.padStart(2, "0")}-${dd.padStart(2, "0")}`;
  }
  return null;
}

export function taipeiNow() {
  return new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
}

export function todayISO() {
  const n = taipeiNow();
  return `${n.getFullYear()}-${String(n.getMonth() + 1).padStart(2, "0")}-${String(n.getDate()).padStart(2, "0")}`;
}

export function isTradingSession() {
  const now = taipeiNow();
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  const m = now.getHours() * 60 + now.getMinutes();
  return m >= 525 && m <= 815; // 08:45 試撮 ~ 13:35
}
