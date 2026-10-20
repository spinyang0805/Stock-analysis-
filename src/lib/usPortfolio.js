// 美股（國泰複委託）持股計算：與 portfolio.js 同樣的移動平均成本法，金額以美元計
// 純函式、無副作用，方便測試（tests/us_loan.test.mjs）
import { toISODate } from "./format.js";

export const US_ACCOUNT = "國泰複委託（美股）";
export const US_HEADERS = ["交易日期", "商品代碼", "交易種類", "股數", "價格"];

const SIDE = { 買進: "buy", 賣出: "sell", 除息: "cash_dividend" };
const numOf = (v) => {
  const s = String(v ?? "").replace(/[,\s$]/g, "");
  if (!s || s === "-") return NaN;
  const neg = /^\(.*\)$/.test(s);
  const n = Number(neg ? s.slice(1, -1) : s);
  return Number.isFinite(n) ? (neg ? -n : n) : NaN;
};

// 表頭有「交易幣別」「商品代碼」「交易種類」就是美股對帳單；回傳表頭列索引，不是則 -1
export function findUsHeader(rows) {
  const idx = rows.slice(0, 15).findIndex((r) => ["交易幣別", "商品代碼", "交易種類", "股數"].every((h) => r.some((c) => String(c).trim() === h)));
  return idx;
}

// 對帳單二維陣列 → 可預覽列 [{ ok, errors, tx, raw, line }]；tx 對應 us_transactions 欄位
export function parseUsRows(rows) {
  const h = findUsHeader(rows);
  if (h < 0) return [];
  const header = rows[h].map((c) => String(c).trim());
  const col = (name) => header.indexOf(name);
  const get = (r, name) => (col(name) >= 0 ? r[col(name)] : "");
  const seen = new Map();
  return rows.slice(h + 1).map((r, i) => {
    const line = h + i + 2;
    const errors = [];
    const date = toISODate(get(r, "交易日期"));
    if (!date) errors.push("日期無法辨識");
    const ticker = String(get(r, "商品代碼")).trim().toUpperCase();
    if (!ticker) errors.push("商品代碼空白");
    const side = SIDE[String(get(r, "交易種類")).trim()];
    if (!side) errors.push(`交易種類「${get(r, "交易種類")}」無法辨識`);
    const shares = Math.abs(numOf(get(r, "股數")));
    const price = Math.abs(numOf(get(r, "價格")));
    if (!(shares > 0)) errors.push("股數無效");
    if (!(price >= 0)) errors.push("價格無效");
    if (errors.length) return { ok: false, errors, tx: null, raw: r, line };
    const fee = Math.abs(numOf(get(r, "手續費"))) || 0;
    const otherFee = Math.abs(numOf(get(r, "其他費用"))) || 0;
    const grossN = Math.abs(numOf(get(r, "成交金額")));
    const amountN = numOf(get(r, "應收/付(-)金額"));
    const fx = numOf(get(r, "匯率"));
    const amount = Number.isFinite(amountN) ? amountN : null;
    const base = [date, ticker, side, shares, price, amount ?? ""].join("|");
    const n = (seen.get(base) || 0) + 1;
    seen.set(base, n);
    const tx = {
      trade_date: date, ticker, name: String(get(r, "商品名稱")).trim() || null, side, shares, price, fee, other_fee: otherFee,
      gross: Number.isFinite(grossN) ? grossN : shares * price, amount,
      fx: fx > 0 ? fx : 1, currency: String(get(r, "交易幣別")).trim() || "USD",
      import_hash: n > 1 ? `${base}|#${n}` : base,
    };
    return { ok: true, errors: [], tx, raw: r, line };
  });
}

const sortTx = (txs) => [...txs].sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)) || (a.side === "sell") - (b.side === "sell"));

// 依交易算出各檔部位（美元）。成本含買進手續費；賣出已實現 = 賣出金額 − 手續費與其他費用 − 平均成本；股利用實收淨額
export function computeUsPositions(txs) {
  const map = new Map();
  for (const t of sortTx(txs)) {
    const code = String(t.ticker).toUpperCase();
    if (!map.has(code)) map.set(code, { code, name: t.name || "", shares: 0, cost: 0, realized: 0, dividends: 0, fees: 0 });
    const p = map.get(code);
    if (!p.name && t.name) p.name = t.name;
    const s = Number(t.shares) || 0, px = Number(t.price) || 0;
    const fee = (Number(t.fee) || 0) + (Number(t.other_fee) || 0);
    if (t.side === "buy") {
      p.cost += s * px + fee; p.shares += s; p.fees += fee;
    } else if (t.side === "sell") {
      const qty = Math.min(s, p.shares);
      const avg = p.shares > 0 ? p.cost / p.shares : 0;
      p.realized += qty * px - fee - avg * qty;
      p.cost -= avg * qty; p.shares -= qty; p.fees += fee;
      if (p.shares <= 1e-9) { p.shares = 0; p.cost = 0; }
    } else if (t.side === "cash_dividend") {
      const a = Number(t.amount);
      p.dividends += Number.isFinite(a) && t.amount !== null && t.amount !== "" ? a : s * px - fee;
    }
  }
  const list = [...map.values()];
  for (const p of list) p.avgCost = p.shares > 0 ? p.cost / p.shares : 0;
  return list;
}

export const fxSourceLabel = (fx) => (fx?.source === "bot" ? "臺灣銀行牌告（即期買入）" : "參考匯率（Yahoo，台銀資料暫缺）");

// us_prices 資料列 → { prices:{ticker:{close,date}}, fx:{close,date}|null }，各取日期最新的一筆
export function latestUsPrices(rows) {
  const prices = {};
  let fx = null;
  for (const r of rows || []) {
    const close = Number(r.close);
    if (!Number.isFinite(close) || close <= 0) continue;
    const cur = { close, date: String(r.date), source: r.source || null };
    if (r.ticker === "USDTWD") { if (!fx || cur.date > fx.date) fx = cur; }
    else if (!prices[r.ticker] || cur.date > prices[r.ticker].date) prices[r.ticker] = cur;
  }
  return { prices, fx };
}

// 加上現價與匯率 → 美元與台幣的市值／損益。沒報價的部位 priced=false（市值、未實現為 NaN，不算進合計）
export function valueUsPositions(positions, prices, fx) {
  const rate = Number(fx?.close);
  const hasFx = Number.isFinite(rate) && rate > 0;
  const rows = positions.map((p) => {
    const px = Number(prices?.[p.code]?.close);
    const priced = Number.isFinite(px) && p.shares > 0;
    const mvUsd = priced ? p.shares * px : NaN;
    const unrealizedUsd = priced ? mvUsd - p.cost : NaN;
    const totalUsd = (priced ? unrealizedUsd : 0) + p.realized + p.dividends;
    const twd = (v) => (hasFx && Number.isFinite(v) ? v * rate : NaN);
    return {
      ...p, price: priced ? px : NaN, priceDate: prices?.[p.code]?.date || null, priced,
      mvUsd, unrealizedUsd, unrealizedPct: priced && p.cost > 0 ? (unrealizedUsd / p.cost) * 100 : NaN,
      totalUsd, mvTwd: twd(mvUsd), costTwd: twd(p.cost), unrealizedTwd: twd(unrealizedUsd),
      realizedTwd: twd(p.realized), dividendsTwd: twd(p.dividends), totalTwd: twd(totalUsd),
    };
  });
  const held = rows.filter((r) => r.shares > 0);
  const sum = (arr, k) => arr.reduce((s, r) => s + (Number.isFinite(r[k]) ? r[k] : 0), 0);
  const priced = held.filter((r) => r.priced);
  const totals = {
    mvUsd: sum(priced, "mvUsd"), costUsd: sum(held, "cost"), unrealizedUsd: sum(priced, "unrealizedUsd"),
    realizedUsd: sum(rows, "realized"), dividendsUsd: sum(rows, "dividends"),
    mvTwd: sum(priced, "mvTwd"), costTwd: sum(held, "costTwd"), unrealizedTwd: sum(priced, "unrealizedTwd"),
    realizedTwd: sum(rows, "realizedTwd"), dividendsTwd: sum(rows, "dividendsTwd"),
    unpriced: held.length - priced.length,
  };
  totals.totalUsd = totals.unrealizedUsd + totals.realizedUsd + totals.dividendsUsd;
  totals.totalTwd = totals.unrealizedTwd + totals.realizedTwd + totals.dividendsTwd;
  return { rows, held, totals, fx: hasFx ? fx : null };
}
