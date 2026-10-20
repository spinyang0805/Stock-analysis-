// 借貸試算：用 0050 還原總報酬月資料，回測「借錢投資、每月還款」在每一個歷史起點的結果
// 資料：public/data/etf_history.json（{ months, tr }）。所有輸入只存在 React state + 本機 localStorage，不會送出。
// 模型與 .omc/plans/loan/loan_sim.py 的 sim() 一致（再加上配息、薪水、多檔 ETF、可選賣出順序）
import React, { useEffect, useMemo, useState } from "react";
import { ArrowDown, ArrowUp, Landmark, LineChart, RotateCcw, SlidersHorizontal } from "lucide-react";
import { Card } from "../components/ui.jsx";
import { fetchStaticJson } from "../lib/data.js";

const KEY = "loanSim";
const money = (v) => Math.round(v).toLocaleString("zh-TW");
const signedMoney = (v) => (v < 0 ? "−" : "") + money(Math.abs(v));

// ==MODEL-START==（純函式；scratch 驗證會直接截取這一段）
const ETF_KEYS = ["e0050", "e006208", "e009816", "e00919"];
const CASH_KEYS = ["deduct", "reserve", "other"];
const DIV_MONTHS = [3, 6, 9, 12];
const NHI_RATE = 0.0211, NHI_LIMIT = 20000;

// rates below ~0.0001%/yr are treated as 0 so (1 − (1+i)^−n) never rounds to zero
const monthlyRate = (annualPct) => (Math.abs(annualPct) < 1e-6 ? 0 : annualPct / 100 / 12);

function loanPayment(P, annualPct, n) {
  const i = monthlyRate(annualPct);
  return i === 0 ? P / n : (P * i) / (1 - Math.pow(1 + i, -n));
}

function loanBalance(P, annualPct, n, k) {
  const i = monthlyRate(annualPct), pay = loanPayment(P, annualPct, n);
  if (i === 0) return Math.max(0, P - pay * k);
  return Math.max(0, P * Math.pow(1 + i, k) - (pay * (Math.pow(1 + i, k) - 1)) / i);
}

// px：start..start+n 的 0050 總報酬指數；labels：同長度的 "YYYY-MM"
function simulate(px, labels, p, detail) {
  const n = p.n, pay = loanPayment(p.loan, p.rate, n);
  const fb = (p.feePct * p.discount) / 100 + p.slipPct / 100, fs = fb + p.taxPct / 100;
  const cr = p.cashRate / 100 / 12;
  const c = { deduct: p.amt.deduct, reserve: p.amt.reserve, other: p.amt.other, wait: 0 };
  const u = { e0050: 0, e006208: 0, e009816: 0, e00919: 0 };
  const T = ETF_KEYS.reduce((a, k) => a + p.amt[k], 0);
  c.wait = T;                                            // 準備投入的錢先放在「其他現金」
  const cashSum = () => c.deduct + c.reserve + c.other + c.wait;
  const stockVal = (x) => ETF_KEYS.reduce((a, k) => a + u[k] * x, 0);
  const takeCash = (amt, order) => {
    let left = amt;
    for (const k of order) { const t = Math.min(c[k], left); c[k] -= t; left -= t; }
  };
  let high = px[0], invested = 0, short = 0, ruin = false, salaryPaid = 0, cumDiv = 0, cumNhi = 0;
  let lowNet = Infinity, lowNetK = 0, lowPot = Infinity;
  const series = [], rows = [];
  for (let k = 0; k <= n; k += 1) {
    const x = px[k];
    if (k) for (const key in c) c[key] *= 1 + cr;
    high = Math.max(high, x);
    // 進場
    let want = 0;
    if (p.entry === "lump") want = k === 0 ? T : 0;
    else if (p.entry === "dca") want = k < p.dcaN ? T / p.dcaN : 0;
    else if (k <= p.dipM) {
      let f = 1 / 3 + (x <= (1 - p.dipX / 100) * high ? 1 / 3 : 0) + (x <= (1 - p.dipY / 100) * high ? 1 / 3 : 0);
      if (k >= p.dipM) f = 1;
      want = Math.max(0, f * T - invested);
    }
    if (want > 0 && T > 0) {
      const buy = Math.min(want, c.wait);
      for (const key of ETF_KEYS) u[key] += (buy * (p.amt[key] / T) * (1 - fb)) / x;
      c.wait -= buy; invested += buy;
    }
    // 00919 季配息（由市值扣除；總報酬不變）
    if (k && u.e00919 > 0 && DIV_MONTHS.includes(Number(labels[k].slice(5)))) {
      const payout = u.e00919 * x * (p.yield919 / 100 / 4);
      const nhi = payout >= NHI_LIMIT ? payout * NHI_RATE : 0, net = payout - nhi;
      u.e00919 -= payout / x;
      cumDiv += payout; cumNhi += nhi;
      if (p.divDeposit) c.deduct += net; else u.e00919 += (net * (1 - fb)) / x;
    }
    // 還款
    if (k) {
      let need = pay;
      const sal = Math.min(p.salary, need); need -= sal; salaryPaid += sal;
      for (const key of p.order) {
        if (need <= 1e-6) break;
        if (key === "other") { for (const kk of ["other", "wait"]) { const t = Math.min(c[kk], need); c[kk] -= t; need -= t; } }
        else if (CASH_KEYS.includes(key)) { const t = Math.min(c[key], need); c[key] -= t; need -= t; }
        else {
          const got = Math.min(u[key] * x * (1 - fs), need);   // 不會賣超過持有
          u[key] -= got / (x * (1 - fs)); need -= got;
        }
      }
      if (need > 1e-6) { short += need; if (k < n) ruin = true; }
    }
    // 每季再平衡到目標股票比例
    const sv0 = stockVal(x);
    if (p.rebalance && k && k % 3 === 0 && sv0 > 0 && T > 0) {
      const w = p.rebTarget / 100, tot = sv0 + cashSum(), gap = w * tot - sv0;
      if (gap > 0) {
        const bx = Math.min(gap / (1 - fb + w * fb), cashSum());
        takeCash(bx, ["wait", "other", "reserve", "deduct"]);
        for (const key of ETF_KEYS) u[key] += (bx * (p.amt[key] / T) * (1 - fb)) / x;
      } else {
        const sx = -gap / (1 - w * fs);
        for (const key of ETF_KEYS) u[key] *= 1 - sx / sv0;
        c.other += sx * (1 - fs);
      }
    }
    const sv = stockVal(x), cash = cashSum(), pot = sv + cash, bal = loanBalance(p.loan, p.rate, n, k);
    lowPot = Math.min(lowPot, pot);
    if (pot - bal < lowNet) { lowNet = pot - bal; lowNetK = k; }
    if (detail) {
      series.push({ k, pot, bal });
      if (k > 0 && (k % 12 === 0 || k === n)) rows.push({ k, label: labels[k], sv, cash, cumDiv, cumNhi, short, bal });
    }
  }
  const final = stockVal(px[n]) + cashSum() - short - salaryPaid;
  return { final, lowNet, lowNetK, lowPot, ruin, short, salaryPaid, cumDiv, cumNhi, series, rows, pay };
}
// ==MODEL-END==

function quantile(sorted, q) {
  if (!sorted.length) return NaN;
  const pos = (sorted.length - 1) * q, lo = Math.floor(pos), hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

const SOURCE_LABELS = {
  deduct: "扣款戶現金", reserve: "預備金", other: "其他現金",
  e0050: "賣 0050", e006208: "賣 006208", e009816: "賣 009816", e00919: "賣 00919",
};

const DEFAULTS = {
  loan: 2000000, rate: 2.9, n: 120, salary: 0,
  amt: { deduct: 200000, reserve: 0, other: 0, e009816: 1500000, e00919: 300000, e0050: 0, e006208: 0 },
  yield919: 9, divDeposit: false, cashRate: 1.5,
  feePct: 0.1425, discount: 0.6, slipPct: 0.1, taxPct: 0.1,
  order: ["deduct", "reserve", "other", "e0050", "e006208", "e009816", "e00919"],
  entry: "lump", dcaN: 12, dipX: 10, dipY: 20, dipM: 12,
  rebalance: false, rebTarget: 50,
  firstMonth: "2004-01", sel: "",
};

// 所有數值都限制在有限、合理的範圍；無效或 NaN 用預設值。輸入與讀回 localStorage 都走這裡
const num = (v, lo, hi, def) => {
  const x = typeof v === "string" && v.trim() === "" ? NaN : Number(v);
  return Number.isFinite(x) ? Math.min(hi, Math.max(lo, x)) : def;
};
const int = (v, lo, hi, def) => Math.round(num(v, lo, hi, def));

function sanitize(o) {
  const d = DEFAULTS, amt = {};
  for (const k of Object.keys(d.amt)) amt[k] = num(o.amt?.[k], 0, 1e9, d.amt[k]);
  const order = Array.isArray(o.order) && o.order.length === 7 && Object.keys(SOURCE_LABELS).every((k) => o.order.includes(k)) ? o.order : d.order;
  const dipX = int(o.dipX, 0, 89, d.dipX);
  const dipY = Math.min(90, Math.max(dipX + 1, int(o.dipY, 1, 90, d.dipY)));
  return {
    loan: num(o.loan, 0, 1e9, d.loan), rate: num(o.rate, 0, 30, d.rate), n: int(o.n, 1, 480, d.n), salary: num(o.salary, 0, 1e9, d.salary),
    amt, yield919: num(o.yield919, 0, 30, d.yield919), divDeposit: !!o.divDeposit, cashRate: num(o.cashRate, 0, 30, d.cashRate),
    feePct: num(o.feePct, 0, 5, d.feePct), discount: num(o.discount, 0, 1, d.discount), slipPct: num(o.slipPct, 0, 5, d.slipPct), taxPct: num(o.taxPct, 0, 5, d.taxPct),
    order, entry: ["lump", "dca", "dip"].includes(o.entry) ? o.entry : d.entry,
    dcaN: int(o.dcaN, 1, 24, d.dcaN), dipX, dipY, dipM: int(o.dipM, 1, 36, d.dipM),
    rebalance: !!o.rebalance, rebTarget: num(o.rebTarget, 0, 100, d.rebTarget),
    firstMonth: typeof o.firstMonth === "string" && /^(19|20)\d{2}-(0[1-9]|1[0-2])$/.test(o.firstMonth) ? o.firstMonth : d.firstMonth,
    sel: typeof o.sel === "string" ? o.sel : "",
  };
}

function loadParams() {
  try {
    const o = JSON.parse(localStorage.getItem(KEY) || "null");
    return o && typeof o === "object" ? sanitize(o) : DEFAULTS;
  } catch { return DEFAULTS; }
}

// 數字欄 + 滑桿；一改動就即時重算
function Field({ label, value, onChange, min = 0, max, step = 1, suffix, hint }) {
  const set = (v) => onChange(v === "" ? 0 : Number(v));
  return (
    <label style={{ display: "block", minWidth: 200, flex: "1 1 220px" }}>
      <span style={{ fontSize: 13 }}>{label}{suffix ? `（${suffix}）` : ""}</span>
      <div style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 2 }}>
        {max != null && <input type="range" min={min} max={max} step={step} value={Math.min(max, Math.max(min, value))} onChange={(e) => set(e.target.value)} style={{ flex: 1, minWidth: 80 }} />}
        <input className="input num" type="number" min={min} step={step} value={Number.isFinite(value) ? value : 0} onChange={(e) => set(e.target.value)} style={{ width: max != null ? 120 : "100%" }} />
      </div>
      {hint && <span className="dim">{hint}</span>}
    </label>
  );
}

const Wrap = ({ children }) => <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>{children}</div>;

function Kpi({ label, value, sub }) {
  return <div className="kpi"><div className="label">{label}</div><div className="value num">{value}</div>{sub && <div className="sub dim">{sub}</div>}</div>;
}

// 各起點最終淨值（依時間排列）；紅色 = 該起點途中需自掏腰包；點柱子可切換指定起點
function ResultBars({ results, selIdx, onPick }) {
  const W = 720, H = 220, L = 56, B = 22, T = 8;
  const vals = results.map((r) => r.final);
  const hi = Math.max(...vals, 0), lo = Math.min(...vals, 0);
  const y = (v) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - B - T);
  const bw = (W - L - 6) / results.length;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto" }} role="img" aria-label="各起點最終淨值">
      <line x1={L} x2={W - 6} y1={y(0)} y2={y(0)} stroke="currentColor" strokeOpacity="0.35" />
      <text x={4} y={y(hi) + 4} fontSize="11" fill="currentColor" opacity="0.6">{money(hi / 10000)}萬</text>
      <text x={4} y={y(0) + 4} fontSize="11" fill="currentColor" opacity="0.6">0</text>
      {lo < 0 && <text x={4} y={y(lo) + 4} fontSize="11" fill="currentColor" opacity="0.6">{signedMoney(lo / 10000)}萬</text>}
      {results.map((r, i) => (
        <rect key={r.month} x={L + i * bw} width={Math.max(bw - 0.4, 0.5)} y={Math.min(y(r.final), y(0))} height={Math.abs(y(r.final) - y(0)) || 0.5}
          fill={r.ruin ? "#dc2626" : r.final < 0 ? "#f59e0b" : "#2563eb"} opacity={i === selIdx ? 1 : 0.6} style={{ cursor: "pointer" }} onClick={() => onPick(r.month)}>
          <title>{`${r.month} 起：${signedMoney(r.final)} 元${r.ruin ? "（途中需自掏腰包）" : ""}`}</title>
        </rect>
      ))}
      <text x={L} y={H - 6} fontSize="11" fill="currentColor" opacity="0.6">{results[0]?.month}</text>
      <text x={W - 6} y={H - 6} fontSize="11" textAnchor="end" fill="currentColor" opacity="0.6">{results[results.length - 1]?.month}</text>
    </svg>
  );
}

function PotChart({ series }) {
  const W = 720, H = 220, L = 56, B = 22, T = 8;
  const hi = Math.max(...series.map((s) => Math.max(s.pot, s.bal)), 1), lo = Math.min(...series.map((s) => s.pot), 0);
  const x = (k) => L + (k / (series.length - 1 || 1)) * (W - L - 8);
  const y = (v) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - B - T);
  const line = (f) => series.map((s) => `${x(s.k).toFixed(1)},${y(f(s)).toFixed(1)}`).join(" ");
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto" }} role="img" aria-label="資金池與剩餘貸款">
      <text x={4} y={y(hi) + 4} fontSize="11" fill="currentColor" opacity="0.6">{money(hi / 10000)}萬</text>
      <text x={4} y={y(lo) + 4} fontSize="11" fill="currentColor" opacity="0.6">{signedMoney(lo / 10000)}萬</text>
      <polyline fill="none" stroke="#2563eb" strokeWidth="2" points={line((s) => s.pot)} />
      <polyline fill="none" stroke="#dc2626" strokeWidth="2" strokeDasharray="5 3" points={line((s) => s.bal)} />
      <text x={L + 6} y={H - 6} fontSize="11" fill="#2563eb">藍：資金池（股票＋現金）</text>
      <text x={L + 190} y={H - 6} fontSize="11" fill="#dc2626">紅虛線：剩餘貸款</text>
    </svg>
  );
}

export default function LoanSimPage() {
  const [hist, setHist] = useState(undefined);
  const [p, setP] = useState(loadParams);
  useEffect(() => { fetchStaticJson("/etf_history.json").then(setHist); }, []);
  useEffect(() => { try { localStorage.setItem(KEY, JSON.stringify(p)); } catch { /* ignore */ } }, [p]);

  const set = (patch) => setP((o) => sanitize({ ...o, ...patch }));
  const setAmt = (k, v) => setP((o) => sanitize({ ...o, amt: { ...o.amt, [k]: v } }));
  const move = (i, d) => setP((o) => {
    const a = [...o.order], j = i + d;
    if (j < 0 || j >= a.length) return o;
    [a[i], a[j]] = [a[j], a[i]];
    return { ...o, order: a };
  });

  const n = p.n;
  const model = p;
  const months = hist?.months, tr = hist?.tr;

  const out = useMemo(() => {
    if (!months || !tr) return null;
    const starts = [];
    for (let i = 0; i + n < months.length; i += 1) if (months[i] >= p.firstMonth) starts.push(i);
    if (!starts.length) return { starts, results: [] };
    const results = starts.map((i) => ({ month: months[i], idx: i, ...simulate(tr.slice(i, i + n + 1), months.slice(i, i + n + 1), model, false) }));
    return { starts, results };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [months, tr, JSON.stringify(model)]);

  if (hist === undefined) return <div className="page dim">載入中…</div>;
  if (!hist) return <div className="page"><div className="notice warn">讀不到 etf_history.json，無法試算。</div></div>;

  const results = out.results;
  const amtSum = ETF_KEYS.reduce((a, k) => a + p.amt[k], 0) + CASH_KEYS.reduce((a, k) => a + p.amt[k], 0);
  const pay = loanPayment(p.loan, p.rate, n);
  const totalPaid = pay * n;
  const finals = results.map((r) => r.final).sort((a, b) => a - b);
  const worst = results.reduce((a, r) => (!a || r.final < a.final ? r : a), null);
  const best = results.reduce((a, r) => (!a || r.final > a.final ? r : a), null);
  const ruinN = results.filter((r) => r.ruin).length, anyN = results.filter((r) => r.short > 1e-6).length;
  const maxShort = Math.max(0, ...results.map((r) => r.short));
  const lowNetRes = results.reduce((a, r) => (!a || r.lowNet < a.lowNet ? r : a), null);
  const selMonth = results.some((r) => r.month === p.sel) ? p.sel : worst?.month;
  const selRes = results.find((r) => r.month === selMonth);
  const detail = selRes ? simulate(tr.slice(selRes.idx, selRes.idx + n + 1), months.slice(selRes.idx, selRes.idx + n + 1), model, true) : null;
  const selIdx = results.findIndex((r) => r.month === selMonth);
  const pct = (a, b) => (b ? `${((a / b) * 100).toFixed(1)}%` : "–");

  return (
    <div className="page">
      <h2 style={{ display: "flex", gap: 8, alignItems: "center" }}><Landmark size={22} />借貸試算</h2>
      <div className="dim" style={{ marginBottom: 8 }}>每個欄位都能改，結果即時重算。輸入只存在這台瀏覽器（localStorage），不會送出。</div>

      <div className="grid two">
        <Card title="貸款" icon={Landmark}>
          <Wrap>
            <Field label="貸款金額" suffix="元" value={p.loan} onChange={(v) => set({ loan: v })} max={10000000} step={50000} />
            <Field label="年利率" suffix="%" value={p.rate} onChange={(v) => set({ rate: v })} max={10} step={0.05} />
            <Field label="期數" suffix="月" value={p.n} onChange={(v) => set({ n: v })} min={1} max={360} step={1} />
            <Field label="每月薪水支付" suffix="元" value={p.salary} onChange={(v) => set({ salary: v })} max={100000} step={1000} hint="每月先用薪水付款，不夠才動用下面的資金；計入淨值扣除" />
          </Wrap>
          <div style={{ marginTop: 8, lineHeight: 1.8 }}>
            每月還款 <b className="num">{money(pay)}</b> 元；總還款 <b className="num">{money(totalPaid)}</b> 元；總利息 <b className="num">{money(totalPaid - p.loan)}</b> 元
          </div>
        </Card>

        <Card title="資金配置（各帳戶金額）" icon={SlidersHorizontal}>
          <Wrap>
            <Field label="扣款戶現金" suffix="元" value={p.amt.deduct} onChange={(v) => setAmt("deduct", v)} max={3000000} step={10000} />
            <Field label="預備金現金" suffix="元" value={p.amt.reserve} onChange={(v) => setAmt("reserve", v)} max={3000000} step={10000} />
            <Field label="其他現金" suffix="元" value={p.amt.other} onChange={(v) => setAmt("other", v)} max={3000000} step={10000} />
            <Field label="009816" suffix="元" value={p.amt.e009816} onChange={(v) => setAmt("e009816", v)} max={5000000} step={10000} />
            <Field label="00919" suffix="元" value={p.amt.e00919} onChange={(v) => setAmt("e00919", v)} max={5000000} step={10000} />
            <Field label="0050" suffix="元" value={p.amt.e0050} onChange={(v) => setAmt("e0050", v)} max={5000000} step={10000} />
            <Field label="006208" suffix="元" value={p.amt.e006208} onChange={(v) => setAmt("e006208", v)} max={5000000} step={10000} />
          </Wrap>
          <div style={{ marginTop: 8 }}>
            合計 <b className="num">{money(amtSum)}</b> 元，貸款 <b className="num">{money(p.loan)}</b> 元
            {Math.abs(amtSum - p.loan) > 0.5 && (
              <div className="notice warn" style={{ marginTop: 6 }}>
                {amtSum > p.loan ? `合計比貸款多 ${money(amtSum - p.loan)} 元（視為另外投入的自有資金，結果沒有扣掉這筆本金）。` : `合計比貸款少 ${money(p.loan - amtSum)} 元（這部分貸款資金沒有被算進去）。`}
              </div>
            )}
          </div>
          <div className="dim" style={{ marginTop: 6 }}>所有股票 ETF 都用 0050 還原總報酬歷史代替（009816、006208 追蹤相近的台灣前 50 大指數）。</div>
        </Card>
      </div>

      <div className="grid two" style={{ marginTop: 12 }}>
        <Card title="00919 配息、現金利率與成本" icon={SlidersHorizontal}>
          <Wrap>
            <Field label="00919 現金殖利率" suffix="%／年" value={p.yield919} onChange={(v) => set({ yield919: v })} max={20} step={0.1} hint="每年 3、6、9、12 月各配 1/4，從市值扣除（總報酬與 0050 相同）" />
            <Field label="現金利率" suffix="%／年" value={p.cashRate} onChange={(v) => set({ cashRate: v })} max={6} step={0.05} />
          </Wrap>
          <label style={{ display: "block", marginTop: 8 }}>
            <input type="checkbox" checked={p.divDeposit} onChange={(e) => set({ divDeposit: e.target.checked })} /> 配息存入扣款戶（預設不勾＝配息扣健保費後再投入 00919，含買進成本）
          </label>
          <div className="dim" style={{ marginTop: 4 }}>單筆配息 ≥ 20,000 元會扣二代健保 2.11%；所得稅依個人級距而定，這裡沒有計入。</div>
          <details style={{ marginTop: 8 }}>
            <summary>進階：交易成本</summary>
            <Wrap>
              <Field label="手續費率" suffix="%" value={p.feePct} onChange={(v) => set({ feePct: v })} max={0.5} step={0.0025} />
              <Field label="手續費折扣" suffix="倍" value={p.discount} onChange={(v) => set({ discount: v })} max={1} step={0.05} />
              <Field label="滑價" suffix="%" value={p.slipPct} onChange={(v) => set({ slipPct: v })} max={1} step={0.01} />
              <Field label="證交稅（賣出）" suffix="%" value={p.taxPct} onChange={(v) => set({ taxPct: v })} max={1} step={0.01} />
            </Wrap>
          </details>
        </Card>

        <Card title="還款資金來源順序" icon={SlidersHorizontal}>
          <div className="dim" style={{ marginBottom: 6 }}>由上而下依序動用；股票最多賣到持有為止，仍不夠就算「自掏腰包」。</div>
          {p.order.map((k, i) => (
            <div key={k} style={{ display: "flex", alignItems: "center", gap: 6, padding: "2px 0" }}>
              <span className="num" style={{ width: 18 }}>{i + 1}</span>
              <span style={{ flex: 1 }}>{SOURCE_LABELS[k]}</span>
              <button type="button" className="btn sm" aria-label="上移" disabled={i === 0} onClick={() => move(i, -1)}><ArrowUp size={14} /></button>
              <button type="button" className="btn sm" aria-label="下移" disabled={i === p.order.length - 1} onClick={() => move(i, 1)}><ArrowDown size={14} /></button>
            </div>
          ))}
        </Card>
      </div>

      <Card title="進場方式與再平衡" icon={SlidersHorizontal} style={{ marginTop: 12 }}>
        <div style={{ display: "flex", gap: 14, flexWrap: "wrap", marginBottom: 8 }}>
          {[["lump", "一次投入"], ["dca", "分月平均投入"], ["dip", "跌深加碼"]].map(([v, t]) => (
            <label key={v}><input type="radio" name="entry" checked={p.entry === v} onChange={() => set({ entry: v })} /> {t}</label>
          ))}
        </div>
        {p.entry === "dca" && <Wrap><Field label="分幾個月投入" suffix="月" value={p.dcaN} onChange={(v) => set({ dcaN: v })} min={1} max={24} /></Wrap>}
        {p.entry === "dip" && (
          <Wrap>
            <Field label="先投 1/3，從高點跌 X% 再投 1/3" suffix="%" value={p.dipX} onChange={(v) => set({ dipX: v })} max={50} />
            <Field label="跌 Y% 再投最後 1/3" suffix="%" value={p.dipY} onChange={(v) => set({ dipY: v })} max={60} />
            <Field label="最晚第 M 個月全部投入" suffix="月" value={p.dipM} onChange={(v) => set({ dipM: v })} max={36} />
          </Wrap>
        )}
        <div className="dim" style={{ margin: "6px 0" }}>還沒投入的錢放在「其他現金」賺現金利率，必要時也會拿來還款。投入金額 = 上面各 ETF 欄位的金額。</div>
        <label><input type="checkbox" checked={p.rebalance} onChange={(e) => set({ rebalance: e.target.checked })} /> 每季再平衡到目標股票比例</label>
        <div className="dim">再平衡從上面輸入的金額開始，第一次調整在第 3 個月。</div>
        {p.rebalance && <Wrap><Field label="目標股票比例" suffix="%" value={p.rebTarget} onChange={(v) => set({ rebTarget: v })} max={100} /></Wrap>}
      </Card>

      <Card title="起點範圍" style={{ marginTop: 12 }}>
        <Wrap>
          <label>最早起點月份<br />
            <select className="input" value={p.firstMonth} onChange={(e) => set({ firstMonth: e.target.value })}>
              {months.slice(0, months.length - n).map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          </label>
        </Wrap>
        <div className="dim" style={{ marginTop: 4 }}>
          資料 {months[0]}～{months[months.length - 1]}；共 {results.length} 個完整 {n} 個月的起點。預設從 2004-01 起（和先前研究一致）。
        </div>
      </Card>

      {results.length === 0 ? <div className="notice warn" style={{ marginTop: 12 }}>沒有足夠長的歷史可以跑這個期數／起點。</div> : (
        <>
          <Card title={`全部起點的結果（${results.length} 個）`} icon={LineChart} style={{ marginTop: 12 }}>
            <div className="kpis">
              <Kpi label="最終淨值 中位數" value={money(quantile(finals, 0.5))} sub="還清貸款後所有資產，扣自掏腰包與薪水" />
              <Kpi label="p10（較差 10%）" value={signedMoney(quantile(finals, 0.1))} />
              <Kpi label="p90（較好 10%）" value={money(quantile(finals, 0.9))} />
              <Kpi label="最差" value={signedMoney(finals[0])} sub={`${worst.month} 起`} />
              <Kpi label="最好" value={money(finals[finals.length - 1])} sub={`${best.month} 起`} />
              <Kpi label={`第 ${n} 期前需自掏腰包`} value={`${ruinN} / ${results.length}`} sub={`${pct(ruinN, results.length)}；含最後一期共 ${anyN} 個`} />
              <Kpi label="自掏腰包最多" value={money(maxShort)} />
              <Kpi label="途中最低淨值" value={signedMoney(lowNetRes.lowNet)} sub={`資金池減剩餘貸款；${lowNetRes.month} 起、第 ${lowNetRes.lowNetK} 個月`} />
            </div>
            <div style={{ marginTop: 12 }}><ResultBars results={results} selIdx={selIdx} onPick={(m) => set({ sel: m })} /></div>
            <div className="dim">每根柱子 = 一個起點月份的最終淨值（藍：正常、紅：途中需自掏腰包、橘：期末為負）；點柱子可切換下面的指定起點。</div>
          </Card>

          {detail && (
            <Card title="指定起點" icon={LineChart} style={{ marginTop: 12 }}>
              <label>起點月份
                <select className="input" style={{ width: "auto" }} value={selMonth} onChange={(e) => set({ sel: e.target.value })}>
                  {results.map((r) => <option key={r.month} value={r.month}>{r.month}</option>)}
                </select>
              </label>
              <div className="dim" style={{ margin: "6px 0" }}>預設顯示最差起點。最終淨值 <b className="num">{signedMoney(selRes.final)}</b> 元；累計配息 {money(detail.cumDiv)}、健保補充保費 {money(detail.cumNhi)}、薪水支付 {money(detail.salaryPaid)}、自掏腰包 {money(detail.short)}。</div>
              <PotChart series={detail.series} />
              <div className="table-wrap" style={{ marginTop: 8 }}>
                <table className="table">
                  <thead><tr><th>期末</th><th>股票市值</th><th>現金</th><th>累計配息</th><th>累計健保費</th><th>累計自掏腰包</th><th>剩餘貸款</th></tr></thead>
                  <tbody>
                    {detail.rows.map((r) => (
                      <tr key={r.k}>
                        <td>第 {r.k} 月（{r.label}）</td>
                        <td className="num">{money(r.sv)}</td><td className="num">{money(r.cash)}</td>
                        <td className="num">{money(r.cumDiv)}</td><td className="num">{money(r.cumNhi)}</td>
                        <td className="num">{money(r.short)}</td><td className="num">{money(r.bal)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </>
      )}

      <Card title="注意事項" icon={RotateCcw} style={{ marginTop: 12 }}>
        <ul style={{ margin: 0, paddingLeft: 18, lineHeight: 1.8 }}>
          <li>歷史不代表未來；這只是把過去的路徑套進來看，不是預測。</li>
          <li>相鄰起點的視窗高度重疊，不是獨立樣本；{results.length} 個起點不等於 {results.length} 次獨立實驗。</li>
          <li>009816、006208、00919 都用 0050 的歷史代替（00919 另外扣掉你設定的配息）；實際走勢會不同。</li>
          <li>所得稅依級距而定，這裡只算二代健保；期末資產以市價計、不再扣賣出成本。</li>
          <li>非投資建議。借錢投資有本金損失與還款壓力的風險。</li>
        </ul>
        <button type="button" className="btn sm" style={{ marginTop: 8 }} onClick={() => setP(DEFAULTS)}><RotateCcw size={14} />回復預設值</button>
      </Card>
    </div>
  );
}
