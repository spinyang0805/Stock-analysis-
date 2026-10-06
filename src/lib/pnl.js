// 損益分析：自動配股配息 + 含息／不含息兩本帳 + 年／月拆解
// 純函式（tests/lib.test.mjs 有測），資料來源見 PnlPage.jsx
import { accountOf, computePositions, equityCurve } from "./portfolio.js";

const DAY = 86400000;
const addDays = (iso, n) => new Date(Date.parse(iso) + n * DAY).toISOString().slice(0, 10);

/* ── 依除權息日持股自動產生股利交易 ──────────────────────────────
   dividendsByCode: { code: [{ exDate, cash, stock, payDate }] }（stock 為每股股票股利「元」，配股數 = 持股 × stock ÷ 10）
   若使用者已自行記錄（對帳單匯入）同類股利，且日期落在除息日 −5 ～ +90 天內 → 不重複產生 */
export function autoDividendTx(transactions, dividendsByCode) {
  const out = [];
  const byCode = new Map();
  for (const t of transactions) {
    const c = String(t.stock_id);
    if (!byCode.has(c)) byCode.set(c, []);
    byCode.get(c).push(t);
  }
  for (const [code, txs] of byCode) {
    const events = (dividendsByCode?.[code] || [])
      .filter((d) => d.exDate && (d.cash > 0 || d.stock > 0))
      .sort((a, b) => a.exDate.localeCompare(b.exDate));
    const first = txs.reduce((m, t) => (t.trade_date < m ? t.trade_date : m), "9999");
    const generated = [];
    for (const ev of events) {
      if (ev.exDate <= first) continue;
      // 除息日前一天收盤時的持股（含先前自動產生的配股）
      const before = [...txs, ...generated].filter((t) => t.trade_date < ev.exDate);
      const held = computePositions(before, { byAccount: true }).filter((p) => p.shares > 0);
      for (const p of held) {
        const recorded = (side) => txs.some((t) => t.side === side && accountOf(t) === p.account
          && t.trade_date >= addDays(ev.exDate, -5) && t.trade_date <= addDays(ev.exDate, 90));
        const name = txs.find((t) => t.stock_name)?.stock_name || null;
        if (ev.cash > 0 && !recorded("cash_dividend")) {
          const tx = {
            id: `auto-cash-${code}-${ev.exDate}-${p.account}`, trade_date: ev.exDate, stock_id: code, stock_name: name,
            side: "cash_dividend", shares: p.shares, price: ev.cash, amount: Math.round(p.shares * ev.cash), fee: 0, tax: 0,
            broker: p.account, source: "auto", payDate: ev.payDate || null,
          };
          out.push(tx); generated.push(tx);
        }
        const add = Math.floor((p.shares * ev.stock) / 10);
        if (ev.stock > 0 && add > 0 && !recorded("stock_dividend")) {
          const tx = {
            id: `auto-stock-${code}-${ev.exDate}-${p.account}`, trade_date: ev.exDate, stock_id: code, stock_name: name,
            side: "stock_dividend", shares: add, price: 0, fee: 0, tax: 0, perShare: ev.stock,
            broker: p.account, source: "auto",
          };
          out.push(tx); generated.push(tx);
        }
      }
    }
  }
  return out;
}

const monthKey = (d) => d.slice(0, 7);

// 配股價值 = 配股股數 × 除權當天收盤價（當天無行情則取之後第一個交易日）
export function stockDividendValue(tx, priceHistory) {
  const bars = priceHistory?.[String(tx.stock_id)] || [];
  const bar = bars.find((b) => b.time >= tx.trade_date);
  return bar ? { price: bar.close, value: Number(tx.shares) * bar.close, date: bar.time } : { price: NaN, value: 0, date: null };
}

/* ── 期間拆解 ─────────────────────────────────────────────────────
   withDiv：含股利交易的帳；noDiv：去掉所有股利交易的帳（價差報酬）
   回傳 { months: [...], years: [...], total } 每列：
   { key, realized, unrealized, cashDiv, pnlNoDiv, pnlWithDiv, avgCost, retNoDiv, retWithDiv, byCode:{code:{...}} } */
export function periodBreakdown(transactions, priceHistory, { livePrices = null, today = null } = {}) {
  const noDivTx = transactions.filter((t) => t.side !== "cash_dividend" && t.side !== "stock_dividend");
  const A = equityCurve(transactions, priceHistory, { livePrices, today });
  const B = equityCurve(noDivTx, priceHistory, { livePrices, today });
  if (!A.length) return { months: [], years: [], total: null };
  const bByDate = new Map(B.map((p) => [p.date, p]));
  const stockDivs = transactions.filter((t) => t.side === "stock_dividend")
    .map((t) => ({ date: t.trade_date, code: String(t.stock_id), value: stockDividendValue(t, priceHistory).value }))
    .sort((a, b) => a.date.localeCompare(b.date));

  // 每日：含息與不含息狀態並列，並累計到當日為止的配股價值
  let si = 0;
  const sdCum = { total: 0, byCode: {} };
  const days = A.map((a) => {
    const b = bByDate.get(a.date) || { pnl: 0, byCode: {} };
    while (si < stockDivs.length && stockDivs[si].date <= a.date) {
      const e = stockDivs[si++];
      sdCum.total += e.value;
      sdCum.byCode[e.code] = (sdCum.byCode[e.code] || 0) + e.value;
    }
    return { date: a.date, a, b, sd: { total: sdCum.total, byCode: { ...sdCum.byCode } } };
  });

  const snap = (d) => {
    const codes = {};
    for (const [c, v] of Object.entries(d.a.byCode)) {
      const bv = d.b.byCode[c] || { mv: 0, cost: 0, realized: 0 };
      const sd = d.sd.byCode[c] || 0;
      codes[c] = {
        name: v.name, realized: v.realized, unrealized: v.mv - v.cost - sd, cashDiv: v.dividends, stockDiv: sd,
        pnlWithDiv: v.mv - v.cost + v.realized + v.dividends,
        pnlNoDiv: bv.mv - bv.cost + bv.realized,
      };
    }
    return {
      realized: d.a.realized, unrealized: d.a.marketValue - d.a.cost - d.sd.total, cashDiv: d.a.dividends, stockDiv: d.sd.total,
      pnlWithDiv: d.a.pnl, pnlNoDiv: d.b.pnl, byCode: codes,
    };
  };
  const zero = { realized: 0, unrealized: 0, cashDiv: 0, stockDiv: 0, pnlWithDiv: 0, pnlNoDiv: 0, byCode: {} };
  const FIELDS = ["realized", "unrealized", "cashDiv", "stockDiv", "pnlWithDiv", "pnlNoDiv"];
  const diff = (end, start) => {
    const r = {};
    for (const f of FIELDS) r[f] = end[f] - start[f];
    r.byCode = {};
    for (const c of new Set([...Object.keys(end.byCode), ...Object.keys(start.byCode)])) {
      const e = end.byCode[c] || { ...zero }, s = start.byCode[c] || { ...zero };
      const row = { name: e.name || s.name };
      for (const f of FIELDS) row[f] = (e[f] || 0) - (s[f] || 0);
      if (FIELDS.some((f) => Math.abs(row[f]) > 0.5)) r.byCode[c] = row;
    }
    return r;
  };

  const group = (keyFn) => {
    const out = [];
    let prevEnd = zero;
    let cur = null;
    for (const d of days) {
      const k = keyFn(d.date);
      if (!cur || cur.key !== k) {
        if (cur) { out.push(finish(cur, prevEnd)); prevEnd = cur.end; }
        cur = { key: k, costSum: 0, n: 0, end: null, from: d.date };
      }
      cur.costSum += d.a.cost; cur.n += 1; cur.end = snap(d); cur.to = d.date;
    }
    if (cur) out.push(finish(cur, prevEnd));
    return out;
  };
  const finish = (cur, prevEnd) => {
    const r = diff(cur.end, prevEnd);
    const avgCost = cur.costSum / Math.max(1, cur.n);
    return {
      key: cur.key, from: cur.from, to: cur.to, ...r, avgCost,
      retNoDiv: avgCost > 0 ? (r.pnlNoDiv / avgCost) * 100 : NaN,
      retWithDiv: avgCost > 0 ? (r.pnlWithDiv / avgCost) * 100 : NaN,
    };
  };

  const months = group(monthKey);
  const years = group((d) => d.slice(0, 4));
  const last = snap(days[days.length - 1]);
  const avgAll = days.reduce((s, d) => s + d.a.cost, 0) / days.length;
  const total = {
    ...diff(last, zero), avgCost: avgAll, from: days[0].date, to: days[days.length - 1].date,
    retNoDiv: avgAll > 0 ? (last.pnlNoDiv / avgAll) * 100 : NaN,
    retWithDiv: avgAll > 0 ? (last.pnlWithDiv / avgAll) * 100 : NaN,
  };
  return { months, years, total };
}
