// 進出場規則引擎：存股（估值區間）＋ 波段（趨勢 + ATR 停損停利）
// 全部規則可解釋、可回測；AI 只負責把結果寫成白話。
import { getChipAnalysis, getMaStatus } from "./analysis.js";

const last = (a) => a[a.length - 1];
const finite = (v) => Number.isFinite(v);
const round = (v) => (finite(v) ? Math.round(v * 100) / 100 : NaN);

/* ── 年度現金股利（以除息日年份彙整，季配息會加總） ─────────────── */
export function annualCashDividends(dividends, financials) {
  const byYear = new Map();
  for (const d of dividends || []) {
    const y = Number(String(d.exDate || "").slice(0, 4));
    if (!y || !(d.cash > 0)) continue;
    byYear.set(y, (byYear.get(y) || 0) + d.cash);
  }
  if (!byYear.size) {
    for (const y of financials?.years || []) if (y.dividend > 0) byYear.set(Number(y.year), y.dividend);
  }
  return [...byYear.entries()].sort((a, b) => a[0] - b[0]).map(([year, cash]) => ({ year, cash }));
}

const percentile = (arr, p) => {
  const s = [...arr].sort((a, b) => a - b);
  const idx = (p / 100) * (s.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return s[lo] + (s[hi] - s[lo]) * (idx - lo);
};

// 季 EPS → 近四季累計（以季底日期 + 45 天公告延遲為生效日）
export function ttmEps(quarters) {
  const q = [...(quarters || [])].filter((x) => finite(x.eps)).sort((a, b) => a.date.localeCompare(b.date));
  const out = [];
  for (let i = 3; i < q.length; i++) {
    const eff = new Date(Date.parse(q[i].date) + 45 * 86400000).toISOString().slice(0, 10);
    out.push({ date: eff, ttm: q[i].eps + q[i - 1].eps + q[i - 2].eps + q[i - 3].eps });
  }
  return out;
}

/* ── 存股：估值區間 ─────────────────────────────────────────────── */
export function valuationBands({ price, fundamentals, financials, dividends, longBars, epsQuarters }) {
  const methods = [];
  const thisYear = new Date().getFullYear();

  // 1) 股利法：近 3 個完整年度平均現金股利 × 15 / 20 / 30（殖利率 6.7% / 5% / 3.3%）
  const annual = annualCashDividends(dividends, financials).filter((d) => d.year < thisYear).slice(-3);
  if (annual.length) {
    const avgDiv = annual.reduce((s, d) => s + d.cash, 0) / annual.length;
    methods.push({
      name: "股利法",
      basis: `近 ${annual.length} 年平均現金股利 ${avgDiv.toFixed(2)} 元 × 15／20／30`,
      cheap: avgDiv * 15, fair: avgDiv * 20, expensive: avgDiv * 30, avgDiv,
    });
  }

  // 2) 本益比法：近 5 年每週「收盤 ÷ 近四季 EPS」的第 10／50／90 百分位 × 目前近四季 EPS
  const ttm = ttmEps(epsQuarters);
  const pe = Number(fundamentals?.pe_ratio);
  const epsNow = ttm.length ? ttm[ttm.length - 1].ttm : pe > 0 && price > 0 ? price / pe : Number(fundamentals?.eps) || NaN;
  if (finite(epsNow) && epsNow > 0) {
    const pes = [];
    if (ttm.length >= 4 && longBars?.length) {
      const fromDate = `${thisYear - 5}${new Date().toISOString().slice(4, 10)}`;
      let j = 0;
      longBars.forEach((b, i) => {
        if (b.time < fromDate || i % 5) return;
        while (j + 1 < ttm.length && ttm[j + 1].date <= b.time) j++;
        if (ttm[j].date <= b.time && ttm[j].ttm > 0) pes.push(b.close / ttm[j].ttm);
      });
    }
    if (pes.length >= 50) {
      const lo = percentile(pes, 10), mid = percentile(pes, 50), hi = percentile(pes, 90);
      methods.push({
        name: "本益比法",
        basis: `近 5 年本益比區間 ${lo.toFixed(1)}／${mid.toFixed(1)}／${hi.toFixed(1)} 倍（10／50／90 百分位）× 近四季 EPS ${epsNow.toFixed(2)}`,
        cheap: epsNow * lo, fair: epsNow * mid, expensive: epsNow * hi, peNow: price / epsNow,
      });
    } else {
      methods.push({
        name: "本益比法",
        basis: `歷史本益比資料不足，用通用 12／15／20 倍 × EPS ${epsNow.toFixed(2)}`,
        cheap: epsNow * 12, fair: epsNow * 15, expensive: epsNow * 20,
      });
    }
  }

  if (!methods.length) return null;
  // 配息率低（成長股）時股利法會嚴重低估 → 只列參考、不納入平均
  const div = methods.find((m) => m.name === "股利法");
  const peM = methods.find((m) => m.name === "本益比法");
  if (div && peM && finite(epsNow) && epsNow > 0 && div.avgDiv / epsNow < 0.4) {
    div.excluded = true;
    div.basis += `（配息率 ${((div.avgDiv / epsNow) * 100).toFixed(0)}% 偏低，僅供參考、不納入估價）`;
  }
  const used = methods.filter((m) => !m.excluded);
  const avg = (k) => used.reduce((s, m) => s + m[k], 0) / used.length;
  return { cheap: round(avg("cheap")), fair: round(avg("fair")), expensive: round(avg("expensive")), methods };
}

/* ── 波段：條件檢查 ─────────────────────────────────────────────── */
function swingConditions(rows, chipData) {
  const b = last(rows);
  const p5 = rows[rows.length - 6] || {};
  const p1 = rows[rows.length - 2] || {};
  const chip = getChipAnalysis(chipData);
  const inst5 = (chip.foreign5d || 0) + (chip.trust5d || 0);
  return [
    { key: "trend", label: "站上月線且月線上揚", ok: b.close > b.ma20 && b.ma20 > p5.ma20, detail: `收盤 ${b.close}／MA20 ${round(b.ma20)}` },
    { key: "align", label: "均線多頭排列（5 > 20 > 60）", ok: b.ma5 > b.ma20 && b.ma20 > b.ma60, detail: `MA5 ${round(b.ma5)}／MA20 ${round(b.ma20)}／MA60 ${round(b.ma60)}` },
    { key: "momentum", label: "動能轉強（MACD 柱 > 0 或 KD 金叉）", ok: b.macd_hist > 0 || (b.kd_k > b.kd_d && p1.kd_k <= p1.kd_d), detail: `MACD 柱 ${round(b.macd_hist)}／K ${round(b.kd_k)} D ${round(b.kd_d)}` },
    { key: "chip", label: "法人近 5 日買超", ok: inst5 > 0, detail: `外資＋投信 ${(inst5 / 1000).toFixed(0)} 千張` },
    { key: "volume", label: "量能放大（> 5 日均量 1.2 倍）", ok: b.volume > (b.volume_ma5 || Infinity) * 1.2, detail: `量比 ${finite(b.volume_ma5) && b.volume_ma5 ? (b.volume / b.volume_ma5).toFixed(2) : "--"}` },
    { key: "rsi", label: "RSI 在 50～75（強而不過熱）", ok: b.rsi14 >= 50 && b.rsi14 <= 75, detail: `RSI ${round(b.rsi14)}` },
  ];
}

/* ── 主函式 ──────────────────────────────────────────────────────── */
// rows：已 computeIndicators 的日K；position：{ shares, avgCost } 或 null
export function buildStrategy({ rows, chipData, fundamentals, financials, dividends, longBars, epsQuarters, position, style = "存股", livePrice }) {
  if (!rows?.length || rows.length < 30) return null;
  const b = last(rows);
  const price = finite(livePrice) ? livePrice : b.close;
  const atr = b.atr14;
  const holding = position && position.shares > 0;
  const ma = getMaStatus(rows);

  /* 波段 */
  const recent = rows.slice(-21, -1);
  const high20 = Math.max(...recent.map((r) => r.high));
  const low20 = Math.min(...recent.map((r) => r.low));
  const highestClose22 = Math.max(...rows.slice(-22).map((r) => r.close));
  const conds = swingConditions(rows, chipData);
  const score = conds.filter((c) => c.ok).length;
  const pullbackLo = b.ma20, pullbackHi = b.ma20 + 0.5 * atr;
  const breakout = high20 + 0.1 * atr;
  const refEntry = holding ? position.avgCost : score >= 4 ? price : pullbackHi;
  const initialStop = refEntry - 2 * atr;
  const trailStop = highestClose22 - 3 * atr;
  const stop = holding ? Math.max(initialStop, trailStop) : initialStop;
  const target1 = refEntry + 3 * atr, target2 = refEntry + 6 * atr;

  let swingAction;
  if (holding && price < stop) swingAction = { label: "出場", tone: "down", why: `跌破停損 ${round(stop)}（${price < trailStop ? "移動停利線" : "成本 − 2ATR"}）` };
  else if (holding && price < b.ma20 && b.ma20 < (rows[rows.length - 6]?.ma20 ?? Infinity)) swingAction = { label: "減碼", tone: "down", why: "跌破月線且月線下彎，波段轉弱" };
  else if (score >= 4) swingAction = holding ? { label: "續抱，可加碼", tone: "up", why: `${score}/6 條件成立，趨勢延續` } : { label: "可進場", tone: "up", why: `${score}/6 條件成立` };
  else if (score >= 2) swingAction = holding ? { label: "續抱觀察", tone: "flat", why: `${score}/6 條件成立，未轉弱也未轉強` } : { label: "觀望", tone: "flat", why: `只有 ${score}/6 條件成立，等拉回月線或突破 ${round(breakout)}` };
  else swingAction = holding ? { label: "減碼", tone: "down", why: `僅 ${score}/6 條件成立，趨勢偏弱` } : { label: "不進場", tone: "down", why: `僅 ${score}/6 條件成立` };

  const swing = {
    atr: round(atr), score, conditions: conds,
    pullback: [round(pullbackLo), round(pullbackHi)], breakout: round(breakout),
    stop: round(stop), initialStop: round(initialStop), trailStop: round(trailStop),
    target1: round(target1), target2: round(target2), high20: round(high20), low20: round(low20),
    action: swingAction,
  };

  /* 存股 */
  const val = valuationBands({ price, fundamentals, financials, dividends, longBars, epsQuarters });
  let valueAction = null, batches = [];
  if (val) {
    const bearTrend = ma.label === "空頭排列" || (b.ma60 && price < b.ma60);
    if (price <= val.cheap) valueAction = { label: "分批加碼", tone: "up", why: `股價 ≤ 便宜價 ${val.cheap}` };
    else if (price <= val.fair) valueAction = { label: "可逢低買進", tone: "up", why: `股價介於便宜價與合理價之間` };
    else if (price < val.expensive) valueAction = { label: "持有，不追高", tone: "flat", why: `股價高於合理價 ${val.fair}` };
    else valueAction = { label: "分批減碼", tone: "down", why: `股價 ≥ 昂貴價 ${val.expensive}` };
    if (bearTrend && valueAction.tone === "up") valueAction.why += "；但趨勢偏空，拉長間距、分 3 批";
    const step = finite(atr) ? 2 * atr : val.cheap * 0.05;
    batches = [
      { label: "第 1 批", price: round(Math.min(val.fair, price)) },
      { label: "第 2 批", price: round(val.cheap) },
      { label: "第 3 批", price: round(val.cheap - step) },
    ];
    const avgDiv = val.methods.find((m) => m.avgDiv)?.avgDiv;
    val.yieldNow = avgDiv ? (avgDiv / price) * 100 : null;
    val.yieldOnCost = avgDiv && holding ? (avgDiv / position.avgCost) * 100 : null;
  }
  const value = val ? { ...val, action: valueAction, batches, reduce: [round(val.expensive), round(val.expensive * 1.1)] } : null;

  /* 主建議依使用者標記 */
  const primary = style === "波段" ? swing.action : value?.action || swing.action;

  /* 畫在 K 線上的價位 */
  const levels = [];
  if (holding) levels.push({ price: position.avgCost, label: "成本", color: "#e2e8f0", style: 0 });
  if (style === "波段") {
    levels.push({ price: swing.stop, label: "停損", color: "#22c55e", style: 2 });
    levels.push({ price: swing.target1, label: "目標1", color: "#f97316", style: 2 });
    if (!holding) levels.push({ price: swing.breakout, label: "突破", color: "#a78bfa", style: 1 });
  } else if (value) {
    levels.push({ price: value.cheap, label: "便宜", color: "#38bdf8", style: 2 });
    levels.push({ price: value.fair, label: "合理", color: "#facc15", style: 2 });
    levels.push({ price: value.expensive, label: "昂貴", color: "#f97316", style: 2 });
  }

  return { style, price, holding, maLabel: ma.label, primary, swing, value, levels };
}

/* ── 波段規則回測（日K，收盤進出） ───────────────────────────────
   進場：「收盤 > MA20、MA20 上揚、MA5 > MA20」由不成立轉為成立的那天
   出場：收盤跌破 max(進場價 − 2ATR, 進場後最高收盤 − 3ATR) */
export function backtestSwing(rows) {
  const trades = [];
  let pos = null;
  for (let i = 25; i < rows.length; i++) {
    const r = rows[i], p = rows[i - 1], p5 = rows[i - 5];
    if (![r.ma20, p.ma20, p5.ma20, r.ma5, r.atr14].every(finite)) continue;
    if (!pos) {
      const p6 = rows[i - 6];
      const okNow = r.close > r.ma20 && r.ma20 > p5.ma20 && r.ma5 > r.ma20;
      const okPrev = p.close > p.ma20 && p.ma20 > (p6?.ma20 ?? Infinity) && p.ma5 > p.ma20;
      if (okNow && !okPrev) {
        pos = { entryDate: r.time, entry: r.close, atr: r.atr14, peak: r.close };
      }
    } else {
      pos.peak = Math.max(pos.peak, r.close);
      const stop = Math.max(pos.entry - 2 * pos.atr, pos.peak - 3 * r.atr14);
      if (r.close < stop || i === rows.length - 1) {
        trades.push({ entryDate: pos.entryDate, exitDate: r.time, entry: pos.entry, exit: r.close, ret: (r.close / pos.entry - 1) * 100, open: i === rows.length - 1 && r.close >= stop });
        pos = null;
      }
    }
  }
  const closed = trades.filter((t) => !t.open);
  const wins = closed.filter((t) => t.ret > 0);
  const total = closed.reduce((acc, t) => acc * (1 + t.ret / 100), 1);
  const bh = rows.length > 25 ? (last(rows).close / rows[25].close - 1) * 100 : NaN;
  return {
    trades,
    count: closed.length,
    winRate: closed.length ? (wins.length / closed.length) * 100 : NaN,
    avgRet: closed.length ? closed.reduce((s, t) => s + t.ret, 0) / closed.length : NaN,
    totalRet: (total - 1) * 100,
    buyHold: bh,
    from: rows[25]?.time, to: last(rows)?.time,
  };
}

/* ── 規則結果 → 白話（不靠 AI 也能看懂） ─────────────────────────── */
export function strategyNarrative(s, name) {
  if (!s) return "";
  const lines = [];
  lines.push(`${name}目前股價 ${s.price}，均線狀態「${s.maLabel}」。你標記為「${s.style}」，主建議：${s.primary.label}（${s.primary.why}）。`);
  if (s.value) {
    lines.push(`存股角度：便宜價 ${s.value.cheap}、合理價 ${s.value.fair}、昂貴價 ${s.value.expensive}（${s.value.methods.filter((m) => !m.excluded).map((m) => m.name).join("＋")}${s.value.methods.filter((m) => !m.excluded).length > 1 ? "平均" : ""}）。${s.value.yieldNow ? `以目前股價計，殖利率約 ${s.value.yieldNow.toFixed(2)}%。` : ""}${s.value.yieldOnCost ? `以你的成本計，殖利率 ${s.value.yieldOnCost.toFixed(2)}%。` : ""}`);
  }
  lines.push(`波段角度：${s.swing.score}/6 條件成立；ATR ${s.swing.atr}。拉回買點 ${s.swing.pullback[0]}～${s.swing.pullback[1]}，突破買點 ${s.swing.breakout}；停損 ${s.swing.stop}，目標 ${s.swing.target1}／${s.swing.target2}。`);
  return lines.join("\n");
}
