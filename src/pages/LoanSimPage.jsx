// 借貸試算：用 0050 還原總報酬月資料，回測「借錢投資、每月還款」在每一個歷史起點的結果
// 資料：public/data/etf_history.json（{ months, tr }）。所有輸入只存在 React state + 本機 localStorage，不會送出。
// 模型與 .omc/plans/loan/loan_sim.py 的 sim() 一致（再加上配息、薪水、多檔 ETF、可選賣出順序）
import React, { useEffect, useMemo, useRef, useState } from "react";
import { ArrowDown, ArrowUp, Landmark, LineChart, RotateCcw, SlidersHorizontal } from "lucide-react";
import { Card } from "../components/ui.jsx";
import { fetchStaticJson } from "../lib/data.js";
import {
  ETF_KEYS, CASH_KEYS, loanPayment, simulate, quantile, SCEN_GROUPS, SCEN_DEFS, DEFAULT_SCEN_ON,
  nextMonthLabel, futureLabels, runScenarios, runActual, computeFuture,
} from "../lib/loanModel.js";

const KEY = "loanSim";
const SNAP_KEY = "loanSimSnaps";
const money = (v) => Math.round(v).toLocaleString("zh-TW");
const signedMoney = (v) => (v < 0 ? "−" : "") + money(Math.abs(v));



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
  tab: "future", scen: "base", retMkt: 8, ret919: 7, volMkt: 20, vol919: 15, method: "boot", origin: nextMonthLabel(),
};

const SCENARIOS = { cons: ["保守", 4, 4], base: ["基準", 8, 7], opt: ["樂觀", 11, 9] };
const PATHS = 2000;

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
    tab: o.tab === "hist" ? "hist" : "future",
    scen: o.scen === "custom" || SCENARIOS[o.scen] ? o.scen : d.scen,
    retMkt: num(o.retMkt, -20, 40, d.retMkt), ret919: num(o.ret919, -20, 40, d.ret919),
    volMkt: num(o.volMkt, 1, 80, d.volMkt), vol919: num(o.vol919, 1, 80, d.vol919),
    method: o.method === "normal" ? "normal" : "boot",
    origin: typeof o.origin === "string" && /^(19|20)\d{2}-(0[1-9]|1[0-2])$/.test(o.origin) ? o.origin : d.origin,
  };
}

function loadParams() {
  try {
    const o = JSON.parse(localStorage.getItem(KEY) || "null");
    return o && typeof o === "object" ? sanitize(o) : DEFAULTS;
  } catch { return DEFAULTS; }
}

function useDebounced(v, ms) {
  const [d, setD] = useState(v);
  useEffect(() => { const t = setTimeout(() => setD(v), ms); return () => clearTimeout(t); }, [v, ms]);
  return d;
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

// 淨資產扇形圖：p10–p90 範圍、中位數、剩餘貸款
function FanChart({ fan, lines, actual }) {
  const W = 720, H = 260, L = 56, B = 22, T = 8;
  const all = [...lines.flatMap((l) => l.net), ...(actual ? actual.net : [])];
  const hi = Math.max(...fan.map((s) => Math.max(s.p90, s.bal)), ...all, 1), lo = Math.min(...fan.map((s) => s.p10), ...all, 0);
  const x = (k) => L + (k / (fan.length - 1 || 1)) * (W - L - 8);
  const y = (v) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - B - T);
  const pts = (f) => fan.map((s) => `${x(s.k).toFixed(1)},${y(f(s)).toFixed(1)}`);
  const seg = (net, a, b) => net.slice(a, b + 1).map((v, i) => `${x(a + i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const band = [...pts((s) => s.p90), ...pts((s) => s.p10).reverse()].join(" ");
  const ticks = fan.filter((s) => s.k > 0 && (s.label.endsWith("-12") || s.k === fan.length - 1));
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto" }} role="img" aria-label="未來淨資產範圍、歷史劇本與剩餘貸款">
      <line x1={L} x2={W - 8} y1={y(0)} y2={y(0)} stroke="currentColor" strokeOpacity="0.35" />
      <text x={4} y={y(hi) + 4} fontSize="11" fill="currentColor" opacity="0.6">{money(hi / 10000)}萬</text>
      <text x={4} y={y(0) + 4} fontSize="11" fill="currentColor" opacity="0.6">0</text>
      {lo < 0 && <text x={4} y={y(lo) + 4} fontSize="11" fill="currentColor" opacity="0.6">{signedMoney(lo / 10000)}萬</text>}
      <polygon points={band} fill="#2563eb" opacity="0.18" />
      <polyline fill="none" stroke="#2563eb" strokeWidth="2" points={pts((s) => s.p50).join(" ")} />
      <polyline fill="none" stroke="#dc2626" strokeWidth="2" strokeDasharray="5 3" points={pts((s) => s.bal).join(" ")} />
      {lines.map((l) => (
        <g key={l.id}>
          <polyline fill="none" stroke={l.color} strokeWidth="1.6" points={seg(l.net, 0, l.histMonths)} />
          {l.histMonths < l.net.length - 1 && <polyline fill="none" stroke={l.color} strokeWidth="1.6" strokeDasharray="4 3" points={seg(l.net, l.histMonths, l.net.length - 1)} />}
        </g>
      ))}
      {actual && actual.net.length > 1 && <polyline fill="none" stroke="currentColor" strokeWidth="3" points={seg(actual.net, 0, actual.net.length - 1)} />}
      {ticks.map((s) => <text key={s.k} x={x(s.k)} y={H - 8} fontSize="10" textAnchor="middle" fill="currentColor" opacity="0.6">{s.label.slice(0, 4)}</text>)}
      <text x={L + 6} y={T + 12} fontSize="11" fill="#2563eb">藍線：模擬淨資產中位數；淺藍帶：較差 10% 到較好 10% 之間</text>
      <text x={L + 6} y={T + 26} fontSize="11" fill="#dc2626">紅虛線：剩餘貸款</text>
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
  const isFuture = p.tab === "future";
  const [seed, setSeed] = useState(20261101);
  const [scenOn, setScenOn] = useState(DEFAULT_SCEN_ON);
  const [custom, setCustom] = useState([]);
  const [pick, setPick] = useState("");
  const [snaps, setSnaps] = useState(() => {
    try {
      const a = JSON.parse(localStorage.getItem(SNAP_KEY) || "[]");
      return Array.isArray(a) ? a.filter((x) => x && typeof x.date === "string" && x.spec && typeof x.spec === "object" && x.fan && Array.isArray(x.fan.p50) && Array.isArray(x.scen)).slice(0, 20) : [];
    } catch { return []; }
  });
  useEffect(() => { try { localStorage.setItem(SNAP_KEY, JSON.stringify(snaps)); } catch { /* ignore */ } }, [snaps]);
  const dp = useDebounced(p, 300);                       // 未來預估算得比較久，等輸入停下來再算

  // 蒙地卡羅與「每個歷史月份進場」摘要放在背景執行緒；輸入一變就丟掉還沒算完的舊工作。
  // 背景執行緒不能用（或出錯）時，改在主畫面算一次（較慢），並可按「重試」
  const [job, setJob] = useState({ mc: null, sumry: null, key: "", busy: false, err: false, fallback: false });
  const [retry, setRetry] = useState(0);
  const noWorker = useRef(false);
  const dpKey = JSON.stringify({ ...dp, sel: 0, tab: 0, firstMonth: 0 });
  const curKey = `${dpKey}|${seed}`;
  useEffect(() => {
    if (!isFuture || !hist) return undefined;
    let dead = false, w = null;
    setJob((j) => ({ ...j, busy: true, err: false }));
    const input = JSON.parse(dpKey);
    const runHere = () => {                                // 主畫面計算：先讓「計算中…」畫出來
      noWorker.current = true;
      setTimeout(() => {
        if (dead) return;
        try {
          const r = computeFuture(input, hist.months, hist.tr, seed, PATHS);
          setJob({ mc: r.mc, sumry: r.sumry, key: curKey, busy: false, err: false, fallback: true });
        } catch { setJob((j) => ({ ...j, busy: false, err: true, fallback: true })); }
      }, 30);
    };
    if (noWorker.current || typeof Worker === "undefined") runHere();
    else {
      try {
        w = new Worker(new URL("../lib/loanWorker.js", import.meta.url), { type: "module" });
        w.onmessage = (e) => { if (!dead) setJob({ mc: e.data.mc, sumry: e.data.sumry, key: curKey, busy: false, err: false, fallback: false }); };
        w.onerror = () => { if (!dead) runHere(); };
        w.postMessage({ id: 1, p: input, months: hist.months, tr: hist.tr, seed, paths: PATHS });
      } catch { runHere(); }
    }
    return () => { dead = true; if (w) w.terminate(); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isFuture, hist, curKey, retry]);
  const mc = job.mc, sumry = job.sumry;

  const sc = useMemo(() => {
    if (!hist || !isFuture) return null;
    return runScenarios(dp, hist, futureLabels(dp.origin, dp.n), custom);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hist, isFuture, custom, JSON.stringify({ ...dp, sel: 0, tab: 0, firstMonth: 0, method: 0 })]);

  // 每個存檔用它自己當時的完整設定與起算月份，重算實際走勢
  const snapActual = useMemo(() => (hist ? snaps.map((sn) => {
    try { const sp = sanitize(sn.spec); return runActual(sp, hist, futureLabels(sp.origin, sp.n)); } catch { return null; }
  }) : []), [snaps, hist]);

  const out = useMemo(() => {
    if (!months || !tr || isFuture) return null;
    const starts = [];
    for (let i = 0; i + n < months.length; i += 1) if (months[i] >= p.firstMonth) starts.push(i);
    if (!starts.length) return { starts, results: [] };
    const results = starts.map((i) => ({ month: months[i], idx: i, ...simulate(tr.slice(i, i + n + 1), months.slice(i, i + n + 1), model, false) }));
    return { starts, results };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [months, tr, isFuture, JSON.stringify(model)]);

  if (hist === undefined) return <div className="page dim">載入中…</div>;
  if (!hist) return <div className="page"><div className="notice warn">讀不到 etf_history.json，無法試算。</div></div>;

  const results = out?.results ?? [];
  const amtSum = ETF_KEYS.reduce((a, k) => a + p.amt[k], 0) + CASH_KEYS.reduce((a, k) => a + p.amt[k], 0);
  const pay = loanPayment(p.loan, p.rate, n);
  const totalPaid = pay * n;
  const finals = results.map((r) => r.final).sort((a, b) => a - b);
  const worst = results.reduce((a, r) => (!a || r.final < a.final ? r : a), null);
  const best = results.reduce((a, r) => (!a || r.final > a.final ? r : a), null);
  const ruinN = results.filter((r) => r.ruin).length, anyN = results.filter((r) => r.short > 1e-6).length;
  const maxShort = Math.max(0, ...results.map((r) => r.short));
  const lowNetRes = results.reduce((a, r) => (!a || r.lowNet < a.lowNet ? r : a), null);
  const pending = isFuture && (job.busy || !mc || !sc || dp !== p);
  const canSave = !pending && !job.err && job.key === curKey;      // 結果必須是用現在這組設定與種子算出來的
  const allLines = sc ? sc.list.map((l, i) => ({ ...l, color: `hsl(${(i * 47) % 360} 70% 42%)` })) : [];
  const lines = allLines.filter((l) => scenOn[l.id]);
  const shown = lines;
  const customOpts = hist && hist.taiex_months ? [
    ...hist.taiex_months.filter((m) => m < hist.months[0]).map((m) => ({ v: `taiex|${m}`, src: "taiex", start: m, t: `${m}（加權指數＋3% 配息估算）` })),
    ...hist.months.slice(0, -1).map((m) => ({ v: `tr|${m}`, src: "tr", start: m, t: m })),
  ] : [];
  const addCustom = () => {
    const o = customOpts.find((x) => x.v === pick);
    if (!o) return;
    const preset = SCEN_DEFS.find((d) => d.src === o.src && d.start === o.start);   // 已經是內建劇本：直接打開它
    const id = preset ? preset.id : `c:${o.v}`;
    if (!preset) setCustom((a) => (a.some((x) => x.id === id) ? a : [...a, { id, group: "custom", name: `自選 ${o.start}${o.src === "taiex" ? "*" : ""}`, start: o.start, src: o.src }]));
    setScenOn((m) => ({ ...m, [id]: true }));
  };
  const saveSnap = () => {
    if (!canSave) return;
    const d = new Date(), date = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    const r = (v) => Math.round(v);
    const snap = {
      date, start: mc.fan[0].label, n: p.n, seed,
      spec: JSON.parse(JSON.stringify(dp)),                // 完整模擬設定（含還款順序、進場、成本、利率、方法、起算月份）
      head: { net: mc.head.net.map(r), needP: mc.head.needP },
      fan: { p10: mc.fan.map((s) => r(s.p10)), p50: mc.fan.map((s) => r(s.p50)), p90: mc.fan.map((s) => r(s.p90)) },
      scen: lines.map((l) => ({ name: l.name, final: r(l.final), needs: l.needs, net: l.net.map(r) })),
    };
    setSnaps((a) => [snap, ...a].slice(0, 20));
  };
  const selMonth = results.some((r) => r.month === p.sel) ? p.sel : worst?.month;
  const selRes = results.find((r) => r.month === selMonth);
  const detail = selRes ? simulate(tr.slice(selRes.idx, selRes.idx + n + 1), months.slice(selRes.idx, selRes.idx + n + 1), model, true) : null;
  const selIdx = results.findIndex((r) => r.month === selMonth);
  const pct = (a, b) => (b ? `${((a / b) * 100).toFixed(1)}%` : "–");

  return (
    <div className="page">
      <h2 style={{ display: "flex", gap: 8, alignItems: "center" }}><Landmark size={22} />借貸試算</h2>
      <div className="dim" style={{ marginBottom: 8 }}>每個欄位都能改，結果即時重算。輸入只存在這台瀏覽器（localStorage），不會送出。</div>
      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        {[["future", "未來預估"], ["hist", "用過去歷史驗證"]].map(([v, t]) => (
          <button key={v} type="button" className={`btn${p.tab === v ? " primary" : ""}`} aria-pressed={p.tab === v} onClick={() => set({ tab: v })}>{t}</button>
        ))}
      </div>

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
          <Wrap>
            <Field label="銀行存款利率" suffix="扣款戶、預備金、其他現金" value={p.cashRate} onChange={(v) => set({ cashRate: v })} max={6} step={0.05} hint="現金放在銀行每年賺的利息，和 00919 的配息無關" />
          </Wrap>
          <div style={{ marginTop: 8 }}>
            合計 <b className="num">{money(amtSum)}</b> 元，貸款 <b className="num">{money(p.loan)}</b> 元
            {Math.abs(amtSum - p.loan) > 0.5 && (
              <div className="notice warn" style={{ marginTop: 6 }}>
                {amtSum > p.loan ? `合計比貸款多 ${money(amtSum - p.loan)} 元（視為另外投入的自有資金，結果沒有扣掉這筆本金）。` : `合計比貸款少 ${money(p.loan - amtSum)} 元（這部分貸款資金沒有被算進去）。`}
              </div>
            )}
          </div>
          <div className="dim" style={{ marginTop: 6 }}>{isFuture ? "009816、0050、006208 用同一組報酬假設（追蹤相近的台灣前 50 大指數）。" : "所有股票 ETF 都用 0050 還原總報酬歷史代替（009816、006208 追蹤相近的台灣前 50 大指數）。"}</div>
        </Card>
      </div>

      {isFuture && (
        <Card title="未來報酬假設" icon={LineChart} style={{ marginTop: 12 }}>
          <div style={{ display: "flex", gap: 14, flexWrap: "wrap", marginBottom: 8 }}>
            <span>情境：</span>
            {[...Object.entries(SCENARIOS).map(([v, a]) => [v, a[0]]), ["custom", "自訂"]].map(([v, t]) => (
              <label key={v}><input type="radio" name="scen" checked={p.scen === v} onChange={() => set(SCENARIOS[v] ? { scen: v, retMkt: SCENARIOS[v][1], ret919: SCENARIOS[v][2] } : { scen: v })} /> {t}</label>
            ))}
          </div>
          <Wrap>
            <label style={{ display: "block", minWidth: 200, flex: "1 1 220px" }}>
              <span style={{ fontSize: 13 }}>預估起算月份</span>
              <div style={{ marginTop: 2 }}><input className="input" type="month" value={p.origin} onChange={(e) => e.target.value && set({ origin: e.target.value })} /></div>
              <span className="dim">從這個月起算；以後資料庫有了這些月份，會自動補上實際走勢</span>
            </label>
            <Field label="009816／0050／006208 年化報酬（複利／CAGR，約為中位數情況）" suffix="%／年" value={p.retMkt} onChange={(v) => set({ retMkt: v, scen: "custom" })} min={-20} max={30} step={0.5} hint="含配息的總報酬，每年複利成長的速度；例：8 代表 10 年約變 2.16 倍。有漲有跌時，平均值會比這個數字略高" />
            <Field label="00919 年化報酬（複利／CAGR，約為中位數情況）" suffix="%／年" value={p.ret919} onChange={(v) => set({ ret919: v, scen: "custom" })} min={-20} max={30} step={0.5} hint="含配息的全部報酬；上面的配息率是從這裡面領出來的" />
            <Field label="009816／0050／006208 年波動" suffix="%" value={p.volMkt} onChange={(v) => set({ volMkt: v })} min={1} max={60} step={1} hint="數字越大，每年漲跌越劇烈" />
            <Field label="00919 年波動" suffix="%" value={p.vol919} onChange={(v) => set({ vol919: v })} min={1} max={60} step={1} />
          </Wrap>
          <div style={{ display: "flex", gap: 14, flexWrap: "wrap", margin: "10px 0 4px", alignItems: "center" }}>
            <span>模擬方式：</span>
            {[["boot", "歷史抽樣（保留大跌）"], ["normal", "常態分布"]].map(([v, t]) => (
              <label key={v}><input type="radio" name="method" checked={p.method === v} onChange={() => set({ method: v })} /> {t}</label>
            ))}
            <button type="button" className="btn sm" onClick={() => setSeed((x) => x + 1)}><RotateCcw size={14} />重新抽樣</button>
          </div>
          <div className="dim">歷史抽樣：把 0050 過去每 12 個月的漲跌節奏隨機拼起來，再調成你設定的年化報酬與波動，會保留 2008、2020 那種大跌。常態分布：沒有歷史上大跌會連著發生的特性，但還是可能出現大幅虧損。共模擬 {PATHS.toLocaleString("zh-TW")} 條路徑；00919 與其他 ETF 的相關度設為 0.7。結果不會因為改數字而亂跳，想換一批隨機路徑就按「重新抽樣」。</div>
        </Card>
      )}

      <div className="grid two" style={{ marginTop: 12 }}>
        <Card title="00919 配息與交易成本" icon={SlidersHorizontal}>
          <Wrap>
            <Field label="00919 每年配息率" suffix="%／年，配息佔市值" value={p.yield919} onChange={(v) => set({ yield919: v })} max={20} step={0.1} hint={isFuture ? "每年 3、6、9、12 月各領 1/4；配息從總報酬裡面拿出來，不是額外多賺" : "每年 3、6、9、12 月各配 1/4，從市值扣除（總報酬與 0050 相同）"} />
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

      {isFuture && job.err && (
        <div className="notice warn" style={{ marginTop: 12 }}>
          計算失敗。<button type="button" className="btn sm" style={{ marginLeft: 8 }} onClick={() => { noWorker.current = false; setRetry((x) => x + 1); }}>重試</button>
        </div>
      )}
      {isFuture && !job.err && job.fallback && <div className="dim" style={{ marginTop: 12 }}>瀏覽器不支援背景計算，改用一般計算，可能稍慢。<button type="button" className="btn sm" style={{ marginLeft: 8 }} onClick={() => { noWorker.current = false; setRetry((x) => x + 1); }}>重試背景計算</button></div>}
      {isFuture && (!mc ? (!job.err && <div className="dim" style={{ marginTop: 12 }}>計算中…</div>) : (
        <div style={{ opacity: pending ? 0.5 : 1 }}>
          {pending && <div className="notice" style={{ marginTop: 12 }}>計算中…（下面是上一次的結果）</div>}
          <Card title={`未來預估：${mc.years[0].year}～${mc.years[mc.years.length - 1].year} 年（從 ${dp.origin} 開始，共 ${n} 個月）`} icon={LineChart} style={{ marginTop: 12 }}>
            <div className="kpis">
              <Kpi label="貸款還清時的淨資產 中位數" value={signedMoney(mc.head.net[1])} sub="一半的情況比這好、一半比這差" />
              <Kpi label="悲觀 10%" value={signedMoney(mc.head.net[0])} sub="只有 10% 的情況比這更差" />
              <Kpi label="樂觀 90%" value={signedMoney(mc.head.net[2])} sub="只有 10% 的情況比這更好" />
              <Kpi label="需要自掏腰包的機率" value={`${(mc.head.needP * 100).toFixed(1)}%`} sub={mc.head.needP > 0 ? `需要時：中位數 ${money(mc.head.needMed)} 元、最多 ${money(mc.head.needMax)} 元` : "模擬中沒有出現"} />
              <Kpi label="累計可領配息（扣健保費後）中位數" value={money(mc.head.cumDiv)} sub={`${n} 個月內 00919 配息合計`} />
              <Kpi label="扣款戶用完的月份 中位數" value={mc.head.deductShare > 0 ? `第 ${Math.round(mc.head.deductMed)} 個月` : "不會用完"} sub={mc.head.deductShare > 0 ? `${(mc.head.deductShare * 100).toFixed(0)}% 的情況會用完（還款先從扣款戶扣）` : undefined} />
            </div>
            <div style={{ marginTop: 12 }}><FanChart fan={mc.fan} lines={shown} actual={sc?.actual} /></div>
            {sc && (
              <>
                <div style={{ marginTop: 8, fontSize: 13 }}>
                  {SCEN_GROUPS.map(([g, gt]) => {
                    const items = allLines.filter((l) => l.group === g);
                    if (g === "custom" && !items.length) return null;
                    const setAll = (on) => setScenOn((m) => ({ ...m, ...Object.fromEntries(items.map((l) => [l.id, on])) }));
                    return (
                      <div key={g} style={{ margin: "6px 0" }}>
                        <b>{gt}</b>{" "}
                        <button type="button" className="btn sm" onClick={() => setAll(true)}>全選</button>{" "}
                        <button type="button" className="btn sm" onClick={() => setAll(false)}>全不選</button>
                        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 4 }}>
                          {items.map((l) => (
                            <label key={l.id} style={{ color: l.color }}>
                              <input type="checkbox" checked={!!scenOn[l.id]} onChange={(e) => setScenOn((m) => ({ ...m, [l.id]: e.target.checked }))} /> {l.name}
                              <span className="dim">（歷史 {l.histMonths} 個月、年化 {(l.cagr * 100).toFixed(1)}%、最大跌幅 {(l.maxDD * 100).toFixed(0)}%）</span>
                              {g === "custom" && <button type="button" className="btn sm" style={{ marginLeft: 4 }} onClick={() => { setCustom((a) => a.filter((x) => x.id !== l.id)); }}>移除</button>}
                            </label>
                          ))}
                        </div>
                      </div>
                    );
                  })}
                  <div style={{ marginTop: 8 }}>
                    <label>自選任何月份當劇本：{" "}
                      <select className="input" style={{ width: "auto" }} value={pick} onChange={(e) => setPick(e.target.value)}>
                        <option value="">選一個月份…</option>
                        {customOpts.map((o) => <option key={o.v} value={o.v}>{o.t}</option>)}
                      </select>
                    </label>{" "}
                    <button type="button" className="btn sm" onClick={addCustom} disabled={!pick}>加入</button>
                  </div>
                  <div style={{ marginTop: 6 }}><b>黑粗線</b>：實際走勢（* = 加權指數加每年 3% 配息估算，不是 0050）</div>
                </div>
                <div className="dim" style={{ marginTop: 4 }}>
                  歷史劇本線 = 把過去那段 0050 的真實漲跌，從 {dp.origin} 起套在你的方案上；虛線是歷史用完之後「用基準報酬接續」（目前 {p.retMkt}%／年）。
                  {" "}{sc.actual && sc.actual.histMonths > 0 ? `實際走勢已有 ${sc.actual.histMonths} 個月。` : `實際走勢：從 ${dp.origin} 起每月自動加上實際走勢（資料更新後這裡就會出現黑線）。`}
                </div>
              </>
            )}
          </Card>
          {sc && lines.length > 0 && (
            <Card title="歷史劇本比較：如果接下來走得像過去那一段" icon={LineChart} style={{ marginTop: 12 }}>
              <div className="dim" style={{ marginBottom: 6 }}>
                現在 0050 剛好在歷史高點附近，這裡把過去各種走法（空頭、溫和、多頭）接在現在讓你對照；表格列出上面勾選的劇本。有「*」的是用加權指數（不含配息），每年加 3% 配息估算總報酬，只是近似。00919 和其他 ETF 走同一條路徑，不另外加隨機波動。金額單位：元。
              </div>
              <div className="table-wrap">
                <table className="table">
                  <thead><tr><th>劇本</th><th>歷史實際月數</th><th>年化／最大跌幅</th><th>要自掏腰包嗎</th>{mc.years.map((y) => <th key={y.year}>{y.year} 年底淨資產</th>)}</tr></thead>
                  <tbody>
                    {lines.map((l) => (
                      <tr key={l.id}>
                        <td style={{ color: l.color }}>{l.name}</td>
                        <td className="num">{l.histMonths}／{n}{l.histMonths < n ? "（之後用基準報酬接續）" : ""}</td>
                        <td className="num">{(l.cagr * 100).toFixed(1)}%／{(l.maxDD * 100).toFixed(0)}%</td>
                        <td>{l.needs ? `要，共 ${money(l.short)}` : "不用"}</td>
                        {l.yearNet.map((v, i) => <td key={i} className="num">{signedMoney(v)}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="table-wrap" style={{ marginTop: 10 }}>
                <table className="table">
                  <thead><tr><th>劇本</th>{mc.years.map((y) => <th key={y.year}>{y.year} 年 00919 可領配息</th>)}</tr></thead>
                  <tbody>
                    {lines.map((l) => (
                      <tr key={l.id}><td style={{ color: l.color }}>{l.name}</td>{l.yearDiv.map((v, i) => <td key={i} className="num">{money(v)}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
          {sumry && sumry.total > 0 && (
            <Card title="如果每個過去的月份都當成今天進場" icon={LineChart} style={{ marginTop: 12 }}>
              <div className="kpis">
                {[["淨資產 < 0（虧損）", 0], ["0 ～ 100 萬", 1], ["100 萬 ～ 300 萬", 2], ["> 300 萬", 3]].map(([t, i]) => (
                  <Kpi key={t} label={t} value={`${((sumry.b[i] / sumry.total) * 100).toFixed(0)}%`} sub={`${sumry.b[i]} / ${sumry.total} 個月份`} />
                ))}
              </div>
              <div className="dim">用你目前的設定，把 0050 歷史上每一個月份都當成進場點、跑滿 {n} 個月的結果。相鄰月份的走勢高度重疊，不是獨立樣本；只是讓你知道過去各種走法大致落在哪一段。</div>
            </Card>
          )}
          <Card title="驗證：存下今天的預估，之後對照實際" icon={LineChart} style={{ marginTop: 12 }}>
            <div className="dim" style={{ marginBottom: 6 }}>把你現在的設定和上面的預估範圍、歷史劇本結果存在這台瀏覽器。之後資料更新、出現實際走勢，就能看實際有沒有落在當時預估的範圍裡。</div>
            <button type="button" className="btn primary sm" onClick={saveSnap} disabled={!canSave}>儲存今天的預估</button>
            {snaps.length > 0 && (
              <div className="table-wrap" style={{ marginTop: 8 }}>
                <table className="table">
                  <thead><tr><th>儲存日期</th><th>當時設定</th><th>當時預估到期淨資產（悲觀／中位／樂觀）</th><th>實際走勢對照</th><th /></tr></thead>
                  <tbody>
                    {snaps.map((sn, i) => {
                      const ac = snapActual[i];
                      const ak = ac && ac.histMonths > 0 ? Math.min(ac.histMonths, sn.fan.p50.length - 1) : 0;
                      const act = ak > 0 ? ac.net[ak] : null;
                      const sp = sn.spec;
                      return (
                        <tr key={`${sn.date}-${i}`}>
                          <td>{sn.date}</td>
                          <td>起算 {sp.origin}、貸款 {money(sp.loan ?? 0)}、{sp.n} 個月、年化報酬 {sp.retMkt}%／{sp.ret919}%、{sp.method === "normal" ? "常態分布" : "歷史抽樣"}</td>
                          <td className="num">{signedMoney(sn.head?.net?.[0] ?? 0)}／{signedMoney(sn.head?.net?.[1] ?? 0)}／{signedMoney(sn.head?.net?.[2] ?? 0)}</td>
                          <td>{act == null ? "還沒有實際資料" : `第 ${ak} 個月：實際（用當時的設定算）${signedMoney(act)}；當時預估 ${signedMoney(sn.fan.p10[ak])}～${signedMoney(sn.fan.p90[ak])}，中位 ${signedMoney(sn.fan.p50[ak])}`}</td>
                          <td><button type="button" className="btn sm" onClick={() => setSnaps((a) => a.filter((_, j) => j !== i))}>刪除</button></td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
          <Card title="每年的樣子（每年底）" icon={LineChart} style={{ marginTop: 12 }}>
            <div className="dim" style={{ marginBottom: 6 }}>中位數 = 一半的情況比這好；悲觀 = 較差 10% 的門檻；樂觀 = 較好 10% 的門檻。金額單位：元。</div>
            <div className="table-wrap">
              <table className="table">
                <thead><tr>
                  <th>年度</th><th>00919 每年可領配息（中位數）</th><th>配息範圍（悲觀～樂觀）</th><th>二代健保費</th><th>每月還款</th><th>剩餘貸款</th>
                  <th>總資產 中位數</th><th>總資產 悲觀</th><th>總資產 樂觀</th><th>淨資產 中位數</th><th>淨資產 悲觀</th><th>淨資產 樂觀</th><th>賣股票還款的金額（中位數）</th>
                </tr></thead>
                <tbody>
                  {mc.years.map((r) => (
                    <tr key={r.year}>
                      <td>{r.year}{r.months < 12 ? `（${r.months} 個月）` : ""}</td>
                      <td className="num">{money(r.divNet[1])}</td><td className="num">{money(r.divNet[0])}～{money(r.divNet[2])}</td>
                      <td className="num">{money(r.nhi)}</td><td className="num">{money(mc.pay)}</td><td className="num">{money(r.bal)}</td>
                      <td className="num">{money(r.pot[1])}</td><td className="num">{money(r.pot[0])}</td><td className="num">{money(r.pot[2])}</td>
                      <td className="num">{signedMoney(r.net[1])}</td><td className="num">{signedMoney(r.net[0])}</td><td className="num">{signedMoney(r.net[2])}</td>
                      <td className="num">{money(r.sold)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      ))}
      {!isFuture && (
        <details style={{ marginTop: 12 }}>
          <summary>進階：歷史資料從哪一年開始算</summary>
          <label>歷史資料從哪一年開始算（進階）<br />
            <select className="input" value={p.firstMonth} onChange={(e) => set({ firstMonth: e.target.value })}>
              {months.slice(0, months.length - n).map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          </label>
          <div className="dim" style={{ marginTop: 4 }}>資料 {months[0]}～{months[months.length - 1]}；共 {results.length} 個完整 {n} 個月的進場月份。預設從 2004-01 起（和先前研究一致）。</div>
        </details>
      )}

      {!isFuture && (results.length === 0 ? <div className="notice warn" style={{ marginTop: 12 }}>沒有足夠長的歷史可以跑這個期數／進場月份。</div> : (
        <>
          <Card title={`每個月份進場的結果（${results.length} 個）`} icon={LineChart} style={{ marginTop: 12 }}>
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
            <div className="dim">每根柱子 = 一個進場月份的最終淨值（藍：正常、紅：途中需自掏腰包、橘：期末為負）；點柱子可切換下面的詳細走勢。</div>
          </Card>

          {detail && (
            <Card title="看某一個月份進場的詳細走勢" icon={LineChart} style={{ marginTop: 12 }}>
              <label>進場月份
                <select className="input" style={{ width: "auto" }} value={selMonth} onChange={(e) => set({ sel: e.target.value })}>
                  {results.map((r) => <option key={r.month} value={r.month}>{r.month}</option>)}
                </select>
              </label>
              <div className="dim" style={{ margin: "6px 0 0" }}>上面的統計用了每一個月份當進場點；這裡挑其中一個月份看它每年的變化。</div>
              <div className="dim" style={{ margin: "6px 0" }}>預設顯示最差的月份。最終淨值 <b className="num">{signedMoney(selRes.final)}</b> 元；累計配息 {money(detail.cumDiv)}、健保補充保費 {money(detail.cumNhi)}、薪水支付 {money(detail.salaryPaid)}、自掏腰包 {money(detail.short)}。</div>
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
      ))}

      <Card title="注意事項" icon={RotateCcw} style={{ marginTop: 12 }}>
        <ul style={{ margin: 0, paddingLeft: 18, lineHeight: 1.8 }}>
          {isFuture ? (
            <>
              <li>這是根據你設定的報酬假設算出來的可能範圍，不是預測；報酬假設一改，結果就跟著變。</li>
              <li>009816、00919 上市時間短，沒有足夠歷史，所以用 0050 過去的漲跌節奏來模擬。</li>
              <li>「淨資產」= 總資產 − 剩餘貸款，再扣掉累計自掏腰包與薪水支付；單筆配息未達 20,000 元不扣二代健保。</li>
            </>
          ) : (
            <>
              <li>歷史不代表未來；這只是把過去的路徑套進來看，不是預測。</li>
              <li>相鄰月份進場的結果高度重疊，不是獨立樣本；{results.length} 個月份不等於 {results.length} 次獨立實驗。</li>
              <li>009816、006208、00919 都用 0050 的歷史代替（00919 另外扣掉你設定的配息）；實際走勢會不同。</li>
            </>
          )}
          <li>所得稅依級距而定，這裡只算二代健保；期末資產以市價計、不再扣賣出成本。</li>
          <li>非投資建議。借錢投資有本金損失與還款壓力的風險。</li>
        </ul>
        <button type="button" className="btn sm" style={{ marginTop: 8 }} onClick={() => setP({ ...DEFAULTS, origin: p.origin })}><RotateCcw size={14} />回復預設值</button>
      </Card>
    </div>
  );
}
