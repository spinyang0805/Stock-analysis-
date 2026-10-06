// 靜態資料層：GH Actions 每日產生 public/data/*.json，隨前端一起部署
import { num, toISODate } from "./format.js";

const DATA = `${import.meta.env.BASE_URL}data`.replace(/\/{2,}/g, "/");

export async function fetchStaticJson(path) {
  try {
    const res = await fetch(`${DATA}${path}`);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// 個股 bundle：{ code, name, market, updated_at, kline, chip, analysis, fundamentals, financials }
const bundleCache = new Map();
export function fetchStockBundle(code) {
  if (!bundleCache.has(code)) {
    bundleCache.set(code, fetchStaticJson(`/stocks/${encodeURIComponent(code)}.json`).then((b) => {
      if (!b) bundleCache.delete(code);
      return b;
    }));
  }
  return bundleCache.get(code);
}

let stockListPromise = null;
export async function loadStockList() {
  if (!stockListPromise) stockListPromise = fetchStaticJson("/stocklist.json");
  const list = await stockListPromise;
  return Array.isArray(list) ? list : [];
}

export async function findStock(code) {
  const list = await loadStockList();
  return list.find((s) => String(s.code) === String(code)) || null;
}

let pricesPromise = null;
export function loadLatestPrices() {
  if (!pricesPromise) pricesPromise = fetchStaticJson("/prices.json").then((p) => p || {});
  return pricesPromise;
}

// 完全相符 > 代號開頭 > 名稱/代號包含
export function searchStockList(list, query, limit = 8) {
  const q = String(query || "").trim().toLowerCase();
  if (!q) return [];
  const exact = [], prefix = [], includes = [];
  for (const item of list) {
    const code = String(item.code || "").toLowerCase();
    const name = String(item.name || "").toLowerCase();
    if (q === code || q === name) exact.push(item);
    else if (code.startsWith(q)) prefix.push(item);
    else if (name.includes(q) || code.includes(q)) includes.push(item);
  }
  return [...exact, ...prefix, ...includes].slice(0, limit);
}

// 靜態 kline → 標準 bar：{ time:'YYYY-MM-DD', open, high, low, close, volume(股), ...原指標 }
export function normalizeRows(payload) {
  const src = Array.isArray(payload) ? payload : Array.isArray(payload?.data) ? payload.data : [];
  const seen = new Set();
  return src
    .map((r) => ({
      ...r,
      time: toISODate(r.date ?? r.Date),
      open: num(r.open, r.Open), high: num(r.high, r.High),
      low: num(r.low, r.Low), close: num(r.close, r.Close),
      volume: num(r.volume, r.Volume, 0),
      ma5: num(r.ma5), ma10: num(r.ma10), ma20: num(r.ma20), ma60: num(r.ma60),
      bb_upper: num(r.bb_upper), bb_lower: num(r.bb_lower), bb_width: num(r.bb_width),
      rsi14: num(r.rsi14), kd_k: num(r.kd_k), kd_d: num(r.kd_d),
    }))
    .filter((r) => r.time && [r.open, r.high, r.low, r.close].every(Number.isFinite))
    .sort((a, b) => a.time.localeCompare(b.time))
    .filter((r) => (seen.has(r.time) ? false : (seen.add(r.time), true)));
}
