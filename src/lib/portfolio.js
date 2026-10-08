// 持股帳本計算：移動平均成本法（含手續費），已實現損益扣手續費與交易稅
// 純函式、無副作用，方便測試（tests/portfolio.test.mjs）

export const SIDE_LABEL = { buy: "買進", sell: "賣出", cash_dividend: "現金股利", stock_dividend: "股票股利" };

// 股票分割／反分割：以 stock_dividend（只加股數、成本不變）+ 備註「股票分割」記錄，免改資料庫 side 限制
export const isSplit = (t) => t.side === "stock_dividend" && /股票分割/.test(t.note || "");
// 真正的股利收入（現金股利、配股）；分割不是股利，不能計入股利統計
export const isDividendTx = (t) => t.side === "cash_dividend" || (t.side === "stock_dividend" && !isSplit(t));
export const sideLabel = (t) => (isSplit(t) ? "股票分割" : SIDE_LABEL[t.side]);

function sortTx(txs) {
  return [...txs].sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)) || (a.side === "sell") - (b.side === "sell"));
}

export const NO_ACCOUNT = "未指定帳戶";
export const accountOf = (t) => (t.broker && String(t.broker).trim()) || NO_ACCOUNT;

export function listAccounts(transactions) {
  return [...new Set(transactions.map(accountOf))].sort((a, b) => (a === NO_ACCOUNT) - (b === NO_ACCOUNT) || a.localeCompare(b, "zh-Hant"));
}

function emptyPos(code, name, account = null) {
  return { code, name, account, shares: 0, cost: 0, realized: 0, dividends: 0, fees: 0, buyCount: 0, firstDate: null };
}

export function cashDividendAmount(t) {
  const amt = Number(t.amount);
  return Number.isFinite(amt) && amt !== 0 ? Math.abs(amt) : Number(t.shares) * Number(t.price);
}

function apply(pos, t) {
  const s = Number(t.shares) || 0, p = Number(t.price) || 0;
  const fee = Number(t.fee) || 0, tax = Number(t.tax) || 0;
  if (!pos.name && t.stock_name) pos.name = t.stock_name;
  if (t.side === "buy") {
    pos.cost += s * p + fee;
    pos.shares += s;
    pos.fees += fee;
    pos.buyCount += 1;
    if (!pos.firstDate) pos.firstDate = t.trade_date;
  } else if (t.side === "sell") {
    const qty = Math.min(s, pos.shares);
    const avg = pos.shares > 0 ? pos.cost / pos.shares : 0;
    pos.realized += qty * p - fee - tax - avg * qty;
    pos.cost -= avg * qty;
    pos.shares -= qty;
    pos.fees += fee + tax;
    if (pos.shares <= 0) { pos.shares = 0; pos.cost = 0; }
  } else if (t.side === "cash_dividend") {
    pos.dividends += cashDividendAmount(t);
  } else if (t.side === "stock_dividend") {
    pos.shares += s;
  }
}

// 依交易算出各股部位。成本一律「先分帳戶各自計算」（每個帳戶的賣出只扣該帳戶成本，與券商對帳單一致）
//   byAccount=true  → 每個 (帳戶, 股票) 一列
//   byAccount=false → 同一檔跨帳戶加總：股數、成本相加，合併均價 = 總成本 ÷ 總股數
export function computePositions(transactions, { byAccount = false } = {}) {
  const map = new Map();
  for (const t of sortTx(transactions)) {
    const code = String(t.stock_id), acc = accountOf(t);
    const key = `${code}|${acc}`;
    if (!map.has(key)) map.set(key, emptyPos(code, t.stock_name, acc));
    apply(map.get(key), t);
  }
  let list = [...map.values()];
  if (!byAccount) {
    const merged = new Map();
    for (const p of list) {
      const m = merged.get(p.code) || { ...emptyPos(p.code, p.name), accounts: [] };
      m.shares += p.shares; m.cost += p.cost; m.realized += p.realized; m.dividends += p.dividends;
      m.fees += p.fees; m.buyCount += p.buyCount;
      if (p.firstDate && (!m.firstDate || p.firstDate < m.firstDate)) m.firstDate = p.firstDate;
      if (!m.name && p.name) m.name = p.name;
      if (p.shares > 0 || p.buyCount) m.accounts.push(p.account);
      merged.set(p.code, m);
    }
    list = [...merged.values()];
  }
  for (const p of list) p.avgCost = p.shares > 0 ? p.cost / p.shares : 0;
  return list;
}

// 加上現價 → 市值、未實現損益、總損益、報酬率
export function valuePositions(positions, prices) {
  const rows = positions.map((p) => {
    const price = Number(prices?.[p.code]);
    const mv = Number.isFinite(price) ? p.shares * price : NaN;
    const unrealized = Number.isFinite(mv) ? mv - p.cost : NaN;
    const total = (Number.isFinite(unrealized) ? unrealized : 0) + p.realized + p.dividends;
    return { ...p, price, marketValue: mv, unrealized, unrealizedPct: p.cost > 0 && Number.isFinite(unrealized) ? (unrealized / p.cost) * 100 : NaN, totalPnl: total };
  });
  const held = rows.filter((r) => r.shares > 0);
  const sum = (arr, k) => arr.reduce((s, r) => s + (Number.isFinite(r[k]) ? r[k] : 0), 0);
  const totals = {
    marketValue: sum(held, "marketValue"),
    cost: sum(held, "cost"),
    unrealized: sum(held, "unrealized"),
    realized: sum(rows, "realized"),
    dividends: sum(rows, "dividends"),
  };
  totals.totalPnl = totals.unrealized + totals.realized + totals.dividends;
  const pricedCost = sum(held.filter((r) => Number.isFinite(r.marketValue)), "cost");
  totals.unrealizedPct = pricedCost > 0 ? (totals.unrealized / pricedCost) * 100 : NaN;
  for (const r of held) r.weight = totals.marketValue > 0 && Number.isFinite(r.marketValue) ? (r.marketValue / totals.marketValue) * 100 : NaN;
  return { rows, held, totals };
}

// 每日資產曲線
// priceHistory: { [code]: [{ time:'YYYY-MM-DD', close }] }（已排序）
// 回傳 [{ date, marketValue, cost, pnl, realized, dividends, byCode:{code:{mv,cost}} }]
export function equityCurve(transactions, priceHistory, { onlyCode = null, livePrices = null, today = null } = {}) {
  const txs = sortTx(transactions.filter((t) => !onlyCode || String(t.stock_id) === String(onlyCode)));
  if (!txs.length) return [];
  const start = txs[0].trade_date;
  const dateSet = new Set();
  for (const [code, bars] of Object.entries(priceHistory || {})) {
    if (onlyCode && code !== String(onlyCode)) continue;
    for (const b of bars) if (b.time >= start) dateSet.add(b.time);
  }
  for (const t of txs) dateSet.add(t.trade_date);
  if (today && livePrices) dateSet.add(today);
  const dates = [...dateSet].sort();

  const cursors = {}, lastClose = {};
  const positions = new Map();
  let ti = 0;
  const out = [];
  for (const date of dates) {
    while (ti < txs.length && txs[ti].trade_date <= date) {
      const t = txs[ti++];
      const key = `${t.stock_id}|${accountOf(t)}`;
      if (!positions.has(key)) positions.set(key, emptyPos(String(t.stock_id), t.stock_name, accountOf(t)));
      apply(positions.get(key), t);
    }
    let mv = 0, cost = 0, realized = 0, dividends = 0;
    const byCode = {};
    for (const pos of positions.values()) {
      const code = pos.code;
      const bars = priceHistory?.[code] || [];
      let i = cursors[code] ?? 0;
      while (i < bars.length && bars[i].time <= date) { lastClose[code] = bars[i].close; i++; }
      cursors[code] = i;
      let px = lastClose[code];
      if (date === today && livePrices && Number.isFinite(livePrices[code])) px = livePrices[code];
      if (!Number.isFinite(px) && pos.shares > 0) px = pos.cost / pos.shares; // 尚無行情時以成本估
      const m = pos.shares > 0 ? pos.shares * px : 0;
      mv += m; cost += pos.cost; realized += pos.realized; dividends += pos.dividends;
      const bc = byCode[code] || (byCode[code] = { mv: 0, cost: 0, realized: 0, dividends: 0, name: pos.name });
      bc.mv += m; bc.cost += pos.cost; bc.realized += pos.realized; bc.dividends += pos.dividends;
    }
    out.push({ date, marketValue: mv, cost, realized, dividends, pnl: mv - cost + realized + dividends, byCode });
  }
  return out;
}

// 期間報酬：以「總損益變化 ÷ 期初投入成本」近似
export function periodChange(curve, days) {
  if (curve.length < 2) return null;
  const end = curve[curve.length - 1];
  const startIdx = Math.max(0, curve.length - 1 - days);
  const s = curve[startIdx];
  const diff = end.pnl - s.pnl;
  const base = s.cost || end.cost;
  return { diff, pct: base > 0 ? (diff / base) * 100 : NaN, from: s.date };
}
