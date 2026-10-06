// 盤中即時報價 / 分時 / 長期日K
//   即時、分時：Supabase RPC（DB 端以 http extension 轉發 TWSE MIS / Yahoo，見 supabase/migrations）
//   長期日K：FinMind 公開 API（支援 CORS，瀏覽器直接抓，上市櫃皆有）
import { supabase } from "./supabase.js";
import { num } from "./format.js";

function splitLevels(prices, vols) {
  const p = String(prices || "").split("_").filter(Boolean).map(Number);
  const v = String(vols || "").split("_").filter(Boolean).map(Number);
  return p.map((price, i) => ({ price, qty: v[i] ?? null })).filter((x) => Number.isFinite(x.price) && x.price > 0);
}

function parseQuote(q) {
  const bids = splitLevels(q.bids, q.bidVols);
  const asks = splitLevels(q.asks, q.askVols);
  const prev = num(q.prev);
  // MIS 最近 5 秒沒成交時 z 為 "-"，以最佳買價近似
  const price = num(q.price, bids[0]?.price, prev);
  return {
    code: q.code, name: q.name, market: q.market,
    price, prev,
    open: num(q.open), high: num(q.high), low: num(q.low),
    volumeLots: num(q.vol, 0),
    change: Number.isFinite(price) && Number.isFinite(prev) ? price - prev : NaN,
    changePct: Number.isFinite(price) && prev ? ((price - prev) / prev) * 100 : NaN,
    date: q.date, time: q.time,
    bids, asks,
  };
}

// 回傳 { source, fetchedAt, quotes: { [code]: quote } }
export async function fetchQuotes(codes) {
  const list = [...new Set((codes || []).filter(Boolean).map(String))];
  if (!list.length) return { source: null, quotes: {} };
  const { data, error } = await supabase.rpc("market_quote", { codes: list });
  if (error || !data) throw new Error(error?.message || "報價服務無回應");
  const quotes = {};
  for (const raw of data.quotes || []) {
    const q = parseQuote(raw);
    if (!q.code) continue;
    // 同代號 tse/otc 都查，保留有價格的那筆
    if (!quotes[q.code] || (!Number.isFinite(quotes[q.code].price) && Number.isFinite(q.price))) quotes[q.code] = q;
  }
  return { source: data.source, fetchedAt: data.fetched_at, quotes };
}

// 分時：interval 1m/5m/15m/60m，range 1d/5d/1mo → bars（time = unix 秒，圖表以台北時間顯示）
export async function fetchIntraday(code, interval = "1m", range = "1d", market = null) {
  const { data, error } = await supabase.rpc("market_intraday", { code, intv: interval, rng: range, mkt: market });
  if (error) throw new Error(error.message);
  if (!data?.t?.length) return { prevClose: NaN, bars: [] };
  const bars = [];
  for (let i = 0; i < data.t.length; i++) {
    const o = data.o?.[i], h = data.h?.[i], l = data.l?.[i], c = data.c?.[i];
    if (![o, h, l, c].every((x) => Number.isFinite(x))) continue;
    bars.push({ time: data.t[i], open: o, high: h, low: l, close: c, volume: data.v?.[i] || 0 });
  }
  return { prevClose: num(data.prev_close), price: num(data.price), bars };
}

// FinMind 長期日K（free 層級：每 IP 每小時 300 次，個人使用足夠）；結果快取 30 分鐘
const historyCache = new Map();
export async function fetchLongHistory(code, startDate = "2010-01-01") {
  const key = `${code}:${startDate}`;
  const hit = historyCache.get(key);
  if (hit && Date.now() - hit.at < 30 * 60 * 1000) return hit.bars;
  const url = `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPrice&data_id=${encodeURIComponent(code)}&start_date=${startDate}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`FinMind ${res.status}`);
  const json = await res.json();
  if (json.status !== 200) throw new Error(json.msg || "FinMind 查詢失敗");
  const bars = (json.data || [])
    .filter((r) => r.open > 0 && r.close > 0)
    .map((r) => ({ time: r.date, open: r.open, high: r.max, low: r.min, close: r.close, volume: r.Trading_Volume }));
  historyCache.set(key, { at: Date.now(), bars });
  return bars;
}

// 現金股利歷史（FinMind TaiwanStockDividend），供存股估價與帳本股利提示
export async function fetchDividends(code) {
  const url = `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockDividend&data_id=${encodeURIComponent(code)}&start_date=2015-01-01`;
  try {
    const res = await fetch(url);
    const json = await res.json();
    if (json.status !== 200) return [];
    // 除息日（現金）與除權日（股票）可能不同 → 拆成兩個事件
    const out = [];
    for (const r of json.data || []) {
      const cash = num(r.CashEarningsDistribution, 0) + num(r.CashStatutorySurplus, 0);
      const stock = num(r.StockEarningsDistribution, 0) + num(r.StockStatutorySurplus, 0);
      const cashEx = r.CashExDividendTradingDate || null, stockEx = r.StockExDividendTradingDate || null;
      const payDate = r.CashDividendPaymentDate || null;
      if (cash > 0 && stock > 0 && cashEx && stockEx && cashEx !== stockEx) {
        out.push({ year: r.year, cash, stock: 0, exDate: cashEx, payDate });
        out.push({ year: r.year, cash: 0, stock, exDate: stockEx, payDate: null });
      } else {
        out.push({ year: r.year, cash, stock, exDate: cashEx || stockEx, payDate });
      }
    }
    return out;
  } catch {
    return [];
  }
}

// 季 EPS（FinMind TaiwanStockFinancialStatements type=EPS）
export async function fetchQuarterlyEps(code) {
  const url = `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockFinancialStatements&data_id=${encodeURIComponent(code)}&start_date=2016-01-01`;
  try {
    const res = await fetch(url);
    const json = await res.json();
    if (json.status !== 200) return [];
    return (json.data || []).filter((r) => r.type === "EPS").map((r) => ({ date: r.date, eps: Number(r.value) }));
  } catch {
    return [];
  }
}

export async function explainWithAI(prompt) {
  const { data, error } = await supabase.rpc("ai_explain", { prompt });
  if (error) return { error: error.message };
  return data || { error: "AI 無回應" };
}
