// 借貸試算：用 0050 還原總報酬月資料，回測「借錢投資、每月還款」在每一個歷史起點的結果
// 資料：public/data/etf_history.json（{ months, tr }）。所有輸入只存在 React state + 本機 localStorage，不會送出。
// 模型與 .omc/plans/loan/loan_sim.py 的 sim() 一致（再加上配息、薪水、多檔 ETF、可選賣出順序）
import React, { useEffect, useId, useMemo, useRef, useState } from "react";
import { ArrowDown, ArrowUp, Landmark, LineChart, RotateCcw, SlidersHorizontal } from "lucide-react";
import { Card } from "../components/ui.jsx";
import "./LoanSimPage.css";
import LoanStatus from "./LoanStatus.jsx";
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
  tab: "status", scen: "base", retMkt: 8, ret919: 7, volMkt: 20, vol919: 15, method: "boot", origin: nextMonthLabel(),
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
    tab: o.tab === "hist" ? "hist" : o.tab === "future" ? "future" : "status",
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
  const id = useId();
  const set = (v) => onChange(v === "" ? 0 : Number(v));
  return (
    <div className="loan-field">
      <label htmlFor={id} className="loan-field-label">{label}{suffix ? <span className="loan-field-unit">（{suffix}）</span> : null}</label>
      <div className="loan-field-row">
        {max != null && <input type="range" aria-label={`${label}（滑桿）`} min={min} max={max} step={step} value={Math.min(max, Math.max(min, value))} onChange={(e) => set(e.target.value)} />}
        <input id={id} className="input num" type="number" inputMode="decimal" min={min} step={step} value={Number.isFinite(value) ? value : 0} onChange={(e) => set(e.target.value)} />
      </div>
      {hint && <span className="loan-hint">{hint}</span>}
    </div>
  );
}

const Wrap = ({ children }) => <div className="loan-fields">{children}</div>;

// 設定分組：標題 + 一行目前設定摘要，點開才看到欄位
function Group({ title, summary, defaultOpen = false, children }) {
  return (
    <details className="loan-group" open={defaultOpen}>
      <summary>
        <span className="loan-group-title">{title}</span>
        {summary ? <span className="loan-group-sum">{summary}</span> : null}
      </summary>
      <div className="loan-group-body">{children}</div>
    </details>
  );
}

// 單選：膠囊按鈕（仍是原生 radio，鍵盤可操作）
function Choice({ name, value, options, onChange }) {
  return (
    <div className="loan-choice" role="radiogroup">
      {options.map(([v, t]) => (
        <label key={v} className={value === v ? "on" : ""}>
          <input type="radio" name={name} checked={value === v} onChange={() => onChange(v)} /> {t}
        </label>
      ))}
    </div>
  );
}

// 摺疊區塊：次要資訊預設收起
function Fold({ title, hint, children, defaultOpen = false }) {
  return (
    <details className="loan-fold" open={defaultOpen}>
      <summary><span className="loan-fold-title">{title}</span>{hint ? <span className="loan-fold-hint">{hint}</span> : null}</summary>
      <div className="loan-fold-body">{children}</div>
    </details>
  );
}

// 主要數字：一個大數字 + 一句白話
function Stat({ label, value, sentence, tone }) {
  return (
    <div className={`loan-stat${tone ? ` ${tone}` : ""}`}>
      <div className="loan-stat-label">{label}</div>
      <div className="loan-stat-value num">{value}</div>
      <div className="loan-stat-sentence">{sentence}</div>
    </div>
  );
}

function Mini({ label, value, sentence }) {
  return (
    <div className="loan-mini">
      <div className="loan-mini-label">{label}</div>
      <div className="loan-mini-value num">{value}</div>
      <div className="loan-mini-sentence">{sentence}</div>
    </div>
  );
}

// 圖例小圖示：同時用線型／形狀區分，不只靠顏色
function Swatch({ kind, color = "currentColor" }) {
  return (
    <svg width="28" height="12" viewBox="0 0 28 12" aria-hidden="true" focusable="false">
      {kind === "line" && <line x1="1" x2="27" y1="6" y2="6" stroke={color} strokeWidth="2.5" />}
      {kind === "dash" && <line x1="1" x2="27" y1="6" y2="6" stroke={color} strokeWidth="2.5" strokeDasharray="5 3" />}
      {kind === "thin" && <line x1="1" x2="27" y1="6" y2="6" stroke={color} strokeWidth="1.5" />}
      {kind === "thick" && <line x1="1" x2="27" y1="6" y2="6" stroke={color} strokeWidth="4.5" />}
      {kind === "band" && <rect x="1" y="1" width="26" height="10" rx="2" fill={color} opacity="0.3" />}
      {kind === "bar" && <rect x="7" y="1" width="14" height="10" rx="1.5" fill={color} />}
      {kind === "hatch" && <><rect x="7" y="1" width="14" height="10" rx="1.5" fill={color} opacity="0.35" stroke={color} /><path d="M7 11 L21 1 M7 6 L14 1 M14 11 L21 6" stroke={color} strokeWidth="1.2" /></>}
    </svg>
  );
}
const Legend = ({ items }) => (
  <ul className="loan-legend" aria-label="圖例">
    {items.map(([kind, color, text]) => <li key={text}><Swatch kind={kind} color={color} /><span>{text}</span></li>)}
  </ul>
);

// 各起點最終淨值（依時間排列）；斜線柱 = 該起點途中需自掏腰包；點柱子可切換指定起點
function ResultBars({ results, selIdx, onPick }) {
  const W = 720, H = 220, L = 60, B = 24, T = 8;
  const vals = results.map((r) => r.final);
  const hi = Math.max(...vals, 0), lo = Math.min(...vals, 0);
  const y = (v) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - B - T);
  const bw = (W - L - 6) / results.length;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="loan-svg" role="img" aria-label="各進場月份的最終淨值">
      <defs>
        <pattern id="loan-hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <rect width="6" height="6" fill="#dc2626" opacity="0.45" /><line x1="0" y1="0" x2="0" y2="6" stroke="#dc2626" strokeWidth="3" />
        </pattern>
      </defs>
      <line x1={L} x2={W - 6} y1={y(0)} y2={y(0)} stroke="currentColor" strokeOpacity="0.35" />
      <text x={4} y={y(hi) + 8} fontSize="14" fill="currentColor" opacity="0.7">{money(hi / 10000)}萬</text>
      <text x={4} y={y(0) + 4} fontSize="14" fill="currentColor" opacity="0.7">0</text>
      {lo < 0 && <text x={4} y={y(lo) + 4} fontSize="14" fill="currentColor" opacity="0.7">{signedMoney(lo / 10000)}萬</text>}
      {results.map((r, i) => (
        <rect key={r.month} x={L + i * bw} width={Math.max(bw - 0.4, 0.5)} y={Math.min(y(r.final), y(0))} height={Math.abs(y(r.final) - y(0)) || 0.5}
          fill={r.ruin ? "url(#loan-hatch)" : r.final < 0 ? "#f59e0b" : "#2563eb"} opacity={i === selIdx ? 1 : 0.65} stroke={i === selIdx ? "currentColor" : "none"} style={{ cursor: "pointer" }} onClick={() => onPick(r.month)}>
          <title>{`${r.month} 起：${signedMoney(r.final)} 元${r.ruin ? "（途中需自掏腰包）" : ""}`}</title>
        </rect>
      ))}
      <text x={L} y={H - 6} fontSize="14" fill="currentColor" opacity="0.7">{results[0]?.month}</text>
      <text x={W - 6} y={H - 6} fontSize="14" textAnchor="end" fill="currentColor" opacity="0.7">{results[results.length - 1]?.month}</text>
    </svg>
  );
}

function PotChart({ series }) {
  const W = 720, H = 220, L = 60, B = 12, T = 8;
  const hi = Math.max(...series.map((s) => Math.max(s.pot, s.bal)), 1), lo = Math.min(...series.map((s) => s.pot), 0);
  const x = (k) => L + (k / (series.length - 1 || 1)) * (W - L - 8);
  const y = (v) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - B - T);
  const line = (f) => series.map((s) => `${x(s.k).toFixed(1)},${y(f(s)).toFixed(1)}`).join(" ");
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="loan-svg" role="img" aria-label="資金池與剩餘貸款">
      <text x={4} y={y(hi) + 8} fontSize="14" fill="currentColor" opacity="0.7">{money(hi / 10000)}萬</text>
      <text x={4} y={y(lo) + 4} fontSize="14" fill="currentColor" opacity="0.7">{signedMoney(lo / 10000)}萬</text>
      <polyline fill="none" stroke="#2563eb" strokeWidth="2.5" points={line((s) => s.pot)} />
      <polyline fill="none" stroke="#dc2626" strokeWidth="2.5" strokeDasharray="5 3" points={line((s) => s.bal)} />
    </svg>
  );
}

// 淨資產扇形圖：較差 10%～較好 10% 範圍、中位數、剩餘貸款（說明改放 HTML 圖例，不擠在圖裡）
function FanChart({ fan, lines, actual }) {
  const W = 720, H = 260, L = 60, B = 24, T = 8;
  const all = [...lines.flatMap((l) => l.net), ...(actual ? actual.net : [])];
  const hi = Math.max(...fan.map((s) => Math.max(s.p90, s.bal)), ...all, 1), lo = Math.min(...fan.map((s) => s.p10), ...all, 0);
  const x = (k) => L + (k / (fan.length - 1 || 1)) * (W - L - 8);
  const y = (v) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - B - T);
  const pts = (f) => fan.map((s) => `${x(s.k).toFixed(1)},${y(f(s)).toFixed(1)}`);
  const seg = (net, a, b) => net.slice(a, b + 1).map((v, i) => `${x(a + i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const band = [...pts((s) => s.p90), ...pts((s) => s.p10).reverse()].join(" ");
  const ticks = fan.filter((s) => s.k > 0 && (s.label.endsWith("-12") || s.k === fan.length - 1));
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="loan-svg" role="img" aria-label="未來淨資產範圍、歷史劇本與剩餘貸款">
      <line x1={L} x2={W - 8} y1={y(0)} y2={y(0)} stroke="currentColor" strokeOpacity="0.35" />
      <text x={4} y={y(hi) + 8} fontSize="14" fill="currentColor" opacity="0.7">{money(hi / 10000)}萬</text>
      <text x={4} y={y(0) + 4} fontSize="14" fill="currentColor" opacity="0.7">0</text>
      {lo < 0 && <text x={4} y={y(lo) + 4} fontSize="14" fill="currentColor" opacity="0.7">{signedMoney(lo / 10000)}萬</text>}
      <polygon points={band} fill="#2563eb" opacity="0.18" />
      <polyline fill="none" stroke="#2563eb" strokeWidth="2.5" points={pts((s) => s.p50).join(" ")} />
      <polyline fill="none" stroke="#dc2626" strokeWidth="2.5" strokeDasharray="5 3" points={pts((s) => s.bal).join(" ")} />
      {lines.map((l) => (
        <g key={l.id}>
          <polyline fill="none" stroke={l.color} strokeWidth="1.6" points={seg(l.net, 0, l.histMonths)} />
          {l.histMonths < l.net.length - 1 && <polyline fill="none" stroke={l.color} strokeWidth="1.6" strokeDasharray="4 3" points={seg(l.net, l.histMonths, l.net.length - 1)} />}
        </g>
      ))}
      {actual && actual.net.length > 1 && <polyline fill="none" stroke="currentColor" strokeWidth="3.5" points={seg(actual.net, 0, actual.net.length - 1)} />}
      {ticks.map((s) => <text key={s.k} x={x(s.k)} y={H - 6} fontSize="13" textAnchor="middle" fill="currentColor" opacity="0.7">{s.label.slice(0, 4)}</text>)}
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
  const isHist = p.tab === "hist";
  const [seed, setSeed] = useState(20261101);
  const [scenOn, setScenOn] = useState(DEFAULT_SCEN_ON);
  const [custom, setCustom] = useState([]);
  const [pick, setPick] = useState("");
  const [scenSort, setScenSort] = useState("def");
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
  const scNow = useMemo(() => {
    if (!hist || !isFuture) return null;
    return runScenarios(dp, hist, futureLabels(dp.origin, dp.n), custom);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hist, isFuture, custom, JSON.stringify({ ...dp, sel: 0, tab: 0, firstMonth: 0, method: 0 })]);

  // 單一資料來源：mc／sumry／sc／所用設定 一起成套更新。
  // 只有「worker 結果的 key 與目前設定+種子相同」且劇本也算好時才換成新的一整套；重算中就整套維持上一次，絕不混用新舊數字
  const live = !!(job.mc && scNow && job.key === curKey);
  const lastRef = useRef(null);
  if (live) lastRef.current = { mc: job.mc, sumry: job.sumry, sc: scNow, dp };
  const V = lastRef.current;
  const mc = V ? V.mc : null, sumry = V ? V.sumry : null, sc = V ? V.sc : null;
  const sdp = V ? V.dp : dp, sn = sdp.n;

  // 每個存檔用它自己當時的完整設定與起算月份，重算實際走勢
  const snapActual = useMemo(() => (hist ? snaps.map((sn) => {
    try { const sp = sanitize(sn.spec); return runActual(sp, hist, futureLabels(sp.origin, sp.n)); } catch { return null; }
  }) : []), [snaps, hist]);

  const out = useMemo(() => {
    if (!months || !tr || !isHist) return null;
    const starts = [];
    for (let i = 0; i + n < months.length; i += 1) if (months[i] >= p.firstMonth) starts.push(i);
    if (!starts.length) return { starts, results: [] };
    const results = starts.map((i) => ({ month: months[i], idx: i, ...simulate(tr.slice(i, i + n + 1), months.slice(i, i + n + 1), model, false) }));
    return { starts, results };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [months, tr, isHist, JSON.stringify(model)]);

  const isStatus = p.tab === "status";
  const headEl = (
    <header className="loan-head">
        <h2><Landmark size={22} aria-hidden="true" />借貸試算</h2>
        <p className="loan-lead">{isStatus ? "追蹤這筆貸款還了多少、還剩多少，以及貸款買的股票現在是賺還是賠。" : "如果借一筆錢去買 ETF、每月還款，最後大概能賺多少、會不會不夠還？輸入只存在這台瀏覽器，不會送出。"}</p>
      </header>
  );
  const tabsEl = (
    <div className="loan-tabs" role="group" aria-label="試算方式">
        {[["status", "目前狀況", "還了多少、現在賺賠"], ["future", "未來預估", "模擬 2,000 種可能的未來"], ["hist", "歷史驗證", "套用過去真實的漲跌"]].map(([v, t, d]) => (
          <button key={v} type="button" className={`loan-tab${p.tab === v ? " on" : ""}`} aria-pressed={p.tab === v} onClick={() => set({ tab: v })}>
            <span className="loan-tab-t">{t}</span><span className="loan-tab-d">{d}</span>
          </button>
        ))}
      </div>
  );
  if (isStatus) return <div className="page loan-page">{headEl}{tabsEl}<LoanStatus /></div>;

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
  const pending = isFuture && (!live || job.busy || dp !== p);
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
      date, start: mc.fan[0].label, n: sn, seed,
      spec: JSON.parse(JSON.stringify(sdp)),                // 完整模擬設定（含還款順序、進場、成本、利率、方法、起算月份）
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

  const groupName = Object.fromEntries(SCEN_GROUPS);
  const sortedLines = scenSort === "hi" ? [...allLines].sort((a, b) => b.final - a.final) : scenSort === "lo" ? [...allLines].sort((a, b) => a.final - b.final) : allLines;
  const setScenAll = (on, ids) => setScenOn((m) => ({ ...m, ...Object.fromEntries(ids.map((id) => [id, on])) }));
  const needTone = (pr) => (pr < 0.05 ? "good" : pr < 0.2 ? "warn" : "bad");
  const needWord = (pr) => (pr < 0.05 ? "風險低" : pr < 0.2 ? "要留意" : "風險高");
  const histNeedPr = results.length ? ruinN / results.length : 0;
  const amtMismatch = Math.abs(amtSum - p.loan) > 0.5;

  // ===== 調整假設（所有欄位集中、分組、預設收起）=====
  const assumptions = (
    <details className="loan-assume">
      <summary>
        <span className="loan-assume-title"><SlidersHorizontal size={18} aria-hidden="true" />調整假設</span>
        <span className="loan-assume-sum">
          貸 {money(p.loan / 10000)} 萬・年利率 {p.rate}%・{n} 個月・每月還 {money(pay)} 元
          {amtMismatch ? "・注意：資金配置合計與貸款金額不同" : ""}
        </span>
      </summary>
      <div className="loan-assume-body">
        <Group title="1. 貸款條件" summary={`${money(p.loan)} 元、${p.rate}%、${n} 個月`} defaultOpen>
          <Wrap>
            <Field label="貸款金額" suffix="元" value={p.loan} onChange={(v) => set({ loan: v })} max={10000000} step={50000} />
            <Field label="年利率" suffix="%" value={p.rate} onChange={(v) => set({ rate: v })} max={10} step={0.05} />
            <Field label="還款期數" suffix="月" value={p.n} onChange={(v) => set({ n: v })} min={1} max={360} step={1} />
            <Field label="每月用薪水支付的金額" suffix="元" value={p.salary} onChange={(v) => set({ salary: v })} max={100000} step={1000} hint="每月先用薪水還款，不夠才動用下面的資金；這筆錢會從淨資產扣掉" />
          </Wrap>
          <p className="loan-note">每月還款 <b className="num">{money(pay)}</b> 元；總還款 <b className="num">{money(totalPaid)}</b> 元；其中利息 <b className="num">{money(totalPaid - p.loan)}</b> 元。</p>
        </Group>

        <Group title="2. 借來的錢放在哪裡" summary={`合計 ${money(amtSum)} 元`}>
          <Wrap>
            <Field label="扣款戶現金" suffix="元" value={p.amt.deduct} onChange={(v) => setAmt("deduct", v)} max={3000000} step={10000} />
            <Field label="預備金現金" suffix="元" value={p.amt.reserve} onChange={(v) => setAmt("reserve", v)} max={3000000} step={10000} />
            <Field label="其他現金" suffix="元" value={p.amt.other} onChange={(v) => setAmt("other", v)} max={3000000} step={10000} />
            <Field label="009816" suffix="元" value={p.amt.e009816} onChange={(v) => setAmt("e009816", v)} max={5000000} step={10000} />
            <Field label="00919" suffix="元" value={p.amt.e00919} onChange={(v) => setAmt("e00919", v)} max={5000000} step={10000} />
            <Field label="0050" suffix="元" value={p.amt.e0050} onChange={(v) => setAmt("e0050", v)} max={5000000} step={10000} />
            <Field label="006208" suffix="元" value={p.amt.e006208} onChange={(v) => setAmt("e006208", v)} max={5000000} step={10000} />
            <Field label="銀行存款利率" suffix="%／年" value={p.cashRate} onChange={(v) => set({ cashRate: v })} max={6} step={0.05} hint="現金放銀行每年賺的利息（扣款戶、預備金、其他現金適用），和 00919 配息無關" />
          </Wrap>
          <p className="loan-note">合計 <b className="num">{money(amtSum)}</b> 元，貸款 <b className="num">{money(p.loan)}</b> 元。</p>
          {amtMismatch && (
            <div className="notice warn">
              {amtSum > p.loan ? `合計比貸款多 ${money(amtSum - p.loan)} 元（視為另外投入的自有資金，結果沒有扣掉這筆本金）。` : `合計比貸款少 ${money(p.loan - amtSum)} 元（這部分貸款資金沒有被算進去）。`}
            </div>
          )}
          <p className="loan-hint">{isFuture ? "009816、0050、006208 用同一組報酬假設（追蹤相近的台灣前 50 大指數）。" : "所有股票 ETF 都用 0050 含配息的歷史報酬代替（009816、006208 追蹤相近的台灣前 50 大指數）。"}</p>
        </Group>

        {isFuture && (
          <Group title="3. 未來每年報酬" summary={`${p.scen === "custom" ? "自訂" : SCENARIOS[p.scen][0]}：${p.retMkt}%／${p.ret919}%、起算 ${p.origin}`}>
            <div className="loan-row-label">快速選擇</div>
            <Choice name="scen" value={p.scen} options={[...Object.entries(SCENARIOS).map(([v, a]) => [v, `${a[0]}（${a[1]}%／${a[2]}%）`]), ["custom", "自訂"]]}
              onChange={(v) => set(SCENARIOS[v] ? { scen: v, retMkt: SCENARIOS[v][1], ret919: SCENARIOS[v][2] } : { scen: v })} />
            <Wrap>
              <div className="loan-field">
                <label htmlFor="loan-origin" className="loan-field-label">預估起算月份</label>
                <div className="loan-field-row"><input id="loan-origin" className="input" type="month" value={p.origin} onChange={(e) => e.target.value && set({ origin: e.target.value })} /></div>
                <span className="loan-hint">從這個月起算；之後資料庫有了這些月份，會自動補上實際走勢</span>
              </div>
              <Field label="009816／0050／006208 每年平均成長" suffix="%" value={p.retMkt} onChange={(v) => set({ retMkt: v, scen: "custom" })} min={-20} max={30} step={0.5} hint="含配息的總報酬，每年成長幾 %（一般情況）。例：8 代表 10 年後約變 2.16 倍" />
              <Field label="00919 每年平均成長" suffix="%" value={p.ret919} onChange={(v) => set({ ret919: v, scen: "custom" })} min={-20} max={30} step={0.5} hint="含配息的總報酬；下面的配息率是從這裡面領出來的" />
              <Field label="009816／0050／006208 每年漲跌幅度" suffix="%" value={p.volMkt} onChange={(v) => set({ volMkt: v })} min={1} max={60} step={1} hint="數字越大，每年漲跌越劇烈" />
              <Field label="00919 每年漲跌幅度" suffix="%" value={p.vol919} onChange={(v) => set({ vol919: v })} min={1} max={60} step={1} />
            </Wrap>
            <div className="loan-row-label">模擬方式</div>
            <div className="loan-inline">
              <Choice name="method" value={p.method} options={[["boot", "歷史抽樣（保留大跌）"], ["normal", "常態分布"]]} onChange={(v) => set({ method: v })} />
              <button type="button" className="btn sm" onClick={() => setSeed((x) => x + 1)}><RotateCcw size={14} />換一批隨機路徑</button>
            </div>
            <p className="loan-hint">歷史抽樣：把 0050 過去每 12 個月的漲跌節奏隨機拼起來，再調成你設定的成長與漲跌幅度，會保留 2008、2020 那種大跌。常態分布：沒有「大跌連續發生」的特性，但仍可能出現大幅虧損。共模擬 {PATHS.toLocaleString("zh-TW")} 條路徑；00919 與其他 ETF 的連動程度設為 0.7。改數字結果不會亂跳，想看另一批隨機結果再按按鈕。</p>
          </Group>
        )}

        <Group title={`${isFuture ? "4" : "3"}. 00919 配息與交易成本`} summary={`配息率 ${p.yield919}%／年`}>
          <Wrap>
            <Field label="00919 每年配息率" suffix="%／年，佔市值" value={p.yield919} onChange={(v) => set({ yield919: v })} max={20} step={0.1} hint={isFuture ? "每年 3、6、9、12 月各領 1/4；配息從總報酬裡面拿出來，不是額外多賺" : "每年 3、6、9、12 月各配 1/4，從市值扣除（總報酬與 0050 相同）"} />
          </Wrap>
          <label className="loan-check">
            <input type="checkbox" checked={p.divDeposit} onChange={(e) => set({ divDeposit: e.target.checked })} />
            <span>配息存入扣款戶<span className="loan-hint">不勾（預設）＝配息扣健保費後再買進 00919，含買進成本</span></span>
          </label>
          <p className="loan-hint">單筆配息 ≥ 20,000 元會扣二代健保 2.11%；所得稅依個人級距而定，這裡沒有計入。</p>
          <div className="loan-row-label">交易成本（一般不用改）</div>
          <Wrap>
            <Field label="手續費率" suffix="%" value={p.feePct} onChange={(v) => set({ feePct: v })} max={0.5} step={0.0025} />
            <Field label="手續費折扣" suffix="倍，0.6＝6 折" value={p.discount} onChange={(v) => set({ discount: v })} max={1} step={0.05} />
            <Field label="買賣價差損失" suffix="%" value={p.slipPct} onChange={(v) => set({ slipPct: v })} max={1} step={0.01} hint="實際成交價和預期價的差距" />
            <Field label="證交稅（賣出時）" suffix="%" value={p.taxPct} onChange={(v) => set({ taxPct: v })} max={1} step={0.01} />
          </Wrap>
        </Group>

        <Group title={`${isFuture ? "5" : "4"}. 每月還款的錢從哪裡拿`} summary={`第一順位：${SOURCE_LABELS[p.order[0]]}`}>
          <p className="loan-hint">由上而下依序動用；股票最多賣到持有為止，仍不夠就算「自掏腰包」。</p>
          <ol className="loan-order">
            {p.order.map((k, i) => (
              <li key={k}>
                <span className="num loan-order-n">{i + 1}</span>
                <span className="loan-order-name">{SOURCE_LABELS[k]}</span>
                <button type="button" className="btn sm" aria-label={`${SOURCE_LABELS[k]} 上移`} disabled={i === 0} onClick={() => move(i, -1)}><ArrowUp size={14} /></button>
                <button type="button" className="btn sm" aria-label={`${SOURCE_LABELS[k]} 下移`} disabled={i === p.order.length - 1} onClick={() => move(i, 1)}><ArrowDown size={14} /></button>
              </li>
            ))}
          </ol>
        </Group>

        <Group title={`${isFuture ? "6" : "5"}. 怎麼把錢投進去`} summary={`${{ lump: "一次投入", dca: "分月平均投入", dip: "跌深加碼" }[p.entry]}${p.rebalance ? "・每季調整比例" : ""}`}>
          <Choice name="entry" value={p.entry} options={[["lump", "一次投入"], ["dca", "分月平均投入"], ["dip", "跌深加碼"]]} onChange={(v) => set({ entry: v })} />
          {p.entry === "dca" && <Wrap><Field label="分幾個月投入" suffix="月" value={p.dcaN} onChange={(v) => set({ dcaN: v })} min={1} max={24} /></Wrap>}
          {p.entry === "dip" && (
            <Wrap>
              <Field label="先投 1/3，從高點跌幾 % 再投第二個 1/3" suffix="%" value={p.dipX} onChange={(v) => set({ dipX: v })} max={50} />
              <Field label="跌幾 % 再投最後 1/3" suffix="%" value={p.dipY} onChange={(v) => set({ dipY: v })} max={60} />
              <Field label="最晚第幾個月全部投入" suffix="月" value={p.dipM} onChange={(v) => set({ dipM: v })} max={36} />
            </Wrap>
          )}
          <p className="loan-hint">還沒投入的錢放在「其他現金」賺存款利息，必要時也會拿來還款。投入金額 = 上面各 ETF 欄位的金額。</p>
          <label className="loan-check">
            <input type="checkbox" checked={p.rebalance} onChange={(e) => set({ rebalance: e.target.checked })} />
            <span>每季把股票比例調回目標值<span className="loan-hint">從第 3 個月開始，每季賣高買低一次，回到你設定的股票佔比</span></span>
          </label>
          {p.rebalance && <Wrap><Field label="目標股票比例" suffix="%" value={p.rebTarget} onChange={(v) => set({ rebTarget: v })} max={100} /></Wrap>}
        </Group>

        {isHist && (
          <Group title="5. 歷史資料起點" summary={`從 ${p.firstMonth} 起`}>
            <div className="loan-field">
              <label htmlFor="loan-first" className="loan-field-label">歷史資料從哪個月開始算</label>
              <select id="loan-first" className="input" value={p.firstMonth} onChange={(e) => set({ firstMonth: e.target.value })}>
                {months.slice(0, months.length - n).map((m) => <option key={m} value={m}>{m}</option>)}
              </select>
            </div>
            <p className="loan-hint">資料 {months[0]}～{months[months.length - 1]}；共 {results.length} 個完整 {n} 個月的進場月份。預設從 2004-01 起（和先前研究一致）。</p>
          </Group>
        )}

        <div className="loan-assume-foot">
          <button type="button" className="btn sm" onClick={() => setP({ ...DEFAULTS, origin: p.origin })}><RotateCcw size={14} />全部回復預設值</button>
        </div>
      </div>
    </details>
  );

  return (
    <div className="page loan-page">
      {headEl}
      {tabsEl}

      {isFuture && job.err && (
        <div className="notice warn loan-gap">
          計算失敗。<button type="button" className="btn sm" style={{ marginLeft: 8 }} onClick={() => { noWorker.current = false; setRetry((x) => x + 1); }}>重試</button>
        </div>
      )}
      {isFuture && !job.err && job.fallback && <div className="dim loan-gap">瀏覽器不支援背景計算，改用一般計算，可能稍慢。<button type="button" className="btn sm" style={{ marginLeft: 8 }} onClick={() => { noWorker.current = false; setRetry((x) => x + 1); }}>重試背景計算</button></div>}

      {/* ===== 未來預估 ===== */}
      {isFuture && (!mc ? (<>{!job.err && <div className="loan-hero loan-pending" role="status">計算中…</div>}{assumptions}</>) : (
        <div className={pending ? "loan-stale" : ""}>
          {pending && <div className="notice loan-gap" role="status">重新計算中…（下面是上一次的結果）</div>}

          <section className={`loan-hero ${needTone(mc.head.needP)}`} aria-labelledby="loan-hero-h">
            <h3 id="loan-hero-h" className="loan-hero-q">先看結論：{sn} 個月後，貸款還清時</h3>
            <p className="loan-hero-a">
              最可能{mc.head.net[1] >= 0 ? "淨賺" : "淨虧"}約 <b className="num">{money(Math.abs(mc.head.net[1]))}</b> 元；
              {mc.head.needP > 0
                ? <>有 <b className="num">{(mc.head.needP * 100).toFixed(1)}%</b> 的機率中途資金不夠、得自己掏錢補。</>
                : <>模擬中沒有出現資金不夠、需要自己掏錢補的情況。</>}
            </p>
            <div className="loan-stats">
              <Stat label="最可能的結果" value={signedMoney(mc.head.net[1])} sentence="中位數：模擬的未來裡，一半比這好、一半比這差（單位：元）" />
              <Stat tone={needTone(mc.head.needP)} label={`要自掏腰包的機率（${needWord(mc.head.needP)}）`} value={`${(mc.head.needP * 100).toFixed(1)}%`}
                sentence={mc.head.needP > 0 ? `若發生：通常補 ${money(mc.head.needMed)} 元、最多 ${money(mc.head.needMax)} 元` : "每月還款的資金一直夠用"} />
              <Stat label="運氣不好時（較差 10%）" value={signedMoney(mc.head.net[0])} sentence="10 次裡有 1 次會比這更差（單位：元）" />
            </div>
            <div className="loan-minis">
              <Mini label="運氣好時（較好 10%）" value={signedMoney(mc.head.net[2])} sentence="10 次裡有 1 次會比這更好" />
              <Mini label="00919 累計可領配息" value={money(mc.head.cumDiv)} sentence={`${sn} 個月內合計，已扣健保費（中位數）`} />
              <Mini label="扣款戶撐到第幾個月" value={mc.head.deductShare > 0 ? `第 ${Math.round(mc.head.deductMed)} 個月` : "不會用完"} sentence={mc.head.deductShare > 0 ? `${(mc.head.deductShare * 100).toFixed(0)}% 的情況會用完（還款先從扣款戶扣）` : "還款期間扣款戶都有錢"} />
            </div>
            <p className="loan-fine">「淨資產」＝ 股票與現金總額 − 還沒還的貸款，再扣掉自掏腰包與薪水支付。這是依你的假設算出的可能範圍，不是預測。</p>
          </section>

          {assumptions}

          <Card title={`未來淨資產走勢：${mc.years[0].year}～${mc.years[mc.years.length - 1].year} 年（從 ${sdp.origin} 起，共 ${sn} 個月）`} icon={LineChart} className="loan-card">
            <Legend items={[
              ["line", "#2563eb", "模擬的中間值（一半情況比這好）"],
              ["band", "#2563eb", "較差 10% 到較好 10% 的範圍"],
              ["dash", "#dc2626", "還沒還的貸款"],
              ...(lines.length ? [["thin", "currentColor", "歷史劇本（實線＝歷史真實走勢，虛線＝之後用基準報酬接續）"]] : []),
              ...(sc?.actual && sc.actual.net.length > 1 ? [["thick", "currentColor", "實際走勢"]] : []),
            ]} />
            <FanChart fan={mc.fan} lines={shown} actual={sc?.actual} />
            <p className="loan-hint">
              {sc?.actual && sc.actual.histMonths > 0 ? `實際走勢已有 ${sc.actual.histMonths} 個月（黑粗線）。` : `實際走勢：從 ${sdp.origin} 起每月自動加上，資料更新後這裡會出現黑粗線。`}
              歷史劇本線 = 把過去那段真實漲跌，從 {sdp.origin} 起套在你的方案上（基準 {sdp.retMkt}%／年）。
            </p>

            {sc && (
              <Fold title={`比較過去 ${allLines.length} 種真實走法`} hint={`目前圖上顯示 ${lines.length} 條；點開可挑選、排序`}>
                <div className="loan-toolbar">
                  <label className="loan-toolbar-item">排序
                    <select className="input" value={scenSort} onChange={(e) => setScenSort(e.target.value)}>
                      <option value="def">預設分組</option>
                      <option value="hi">到期淨資產：高到低</option>
                      <option value="lo">到期淨資產：低到高</option>
                    </select>
                  </label>
                  <button type="button" className="btn sm" onClick={() => setScenAll(true, allLines.map((l) => l.id))}>全部顯示</button>
                  <button type="button" className="btn sm" onClick={() => setScenAll(false, allLines.map((l) => l.id))}>全部隱藏</button>
                  {SCEN_GROUPS.map(([g, gt]) => {
                    const ids = allLines.filter((l) => l.group === g).map((l) => l.id);
                    if (!ids.length) return null;
                    const allOn = ids.every((id) => scenOn[id]);
                    return <button key={g} type="button" className="btn sm" aria-pressed={allOn} onClick={() => setScenAll(!allOn, ids)}>{allOn ? "隱藏" : "顯示"}「{gt}」</button>;
                  })}
                </div>
                <ul className="loan-scen">
                  {sortedLines.map((l) => (
                    <li key={l.id} className={scenOn[l.id] ? "on" : ""}>
                      <label>
                        <input type="checkbox" checked={!!scenOn[l.id]} onChange={(e) => setScenOn((m) => ({ ...m, [l.id]: e.target.checked }))} />
                        <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="6" fill={l.color} /></svg>
                        <span className="loan-scen-name">{l.name}<span className="loan-scen-tag">{groupName[l.group]}</span></span>
                      </label>
                      <dl className="loan-scen-nums">
                        <div><dt>到期淨資產</dt><dd className="num">{signedMoney(l.final)}</dd></div>
                        <div><dt>每年平均成長</dt><dd className="num">{(l.cagr * 100).toFixed(1)}%</dd></div>
                        <div><dt>最深跌幅</dt><dd className="num">{(l.maxDD * 100).toFixed(0)}%</dd></div>
                        <div><dt>自掏腰包</dt><dd>{l.needs ? `要，${money(l.short)}` : "不用"}</dd></div>
                      </dl>
                      {l.group === "custom" && <button type="button" className="btn sm" onClick={() => setCustom((a) => a.filter((x) => x.id !== l.id))}>移除</button>}
                    </li>
                  ))}
                </ul>
                <div className="loan-toolbar">
                  <label className="loan-toolbar-item">自選任何月份當起點
                    <select className="input" value={pick} onChange={(e) => setPick(e.target.value)}>
                      <option value="">選一個月份…</option>
                      {customOpts.map((o) => <option key={o.v} value={o.v}>{o.t}</option>)}
                    </select>
                  </label>
                  <button type="button" className="btn sm" onClick={addCustom} disabled={!pick}>加入</button>
                </div>
                <p className="loan-hint">「年化」在這裡指每年平均成長率；有「*」的是用加權指數（不含配息）每年加 3% 估算，只是近似。現在 0050 剛好在歷史高點附近，所以把過去空頭、溫和、多頭的走法接在現在讓你對照。</p>
              </Fold>
            )}
          </Card>

          {sc && lines.length > 0 && (
            <Fold title="各歷史劇本：每年年底淨資產與配息" hint="表格，點開查看">
              <table className="loan-table stack">
                <caption>淨資產（元）</caption>
                <thead><tr><th>劇本</th><th>要自掏腰包嗎</th>{mc.years.map((y) => <th key={y.year}>{y.year} 年底</th>)}</tr></thead>
                <tbody>
                  {lines.map((l) => (
                    <tr key={l.id}>
                      <th scope="row" style={{ color: l.color }}>{l.name}</th>
                      <td data-label="要自掏腰包嗎">{l.needs ? `要，共 ${money(l.short)}` : "不用"}</td>
                      {l.yearNet.map((v, i) => <td key={i} className="num" data-label={`${mc.years[i].year} 年底`}>{signedMoney(v)}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
              <table className="loan-table stack">
                <caption>00919 每年可領配息（元）</caption>
                <thead><tr><th>劇本</th>{mc.years.map((y) => <th key={y.year}>{y.year} 年</th>)}</tr></thead>
                <tbody>
                  {lines.map((l) => (
                    <tr key={l.id}><th scope="row" style={{ color: l.color }}>{l.name}</th>{l.yearDiv.map((v, i) => <td key={i} className="num" data-label={`${mc.years[i].year} 年`}>{money(v)}</td>)}</tr>
                  ))}
                </tbody>
              </table>
            </Fold>
          )}

          <Fold title="每年的樣子" hint="每年年底的配息、貸款、資產（表格）">
            <p className="loan-hint">「中位數」＝ 一半的情況比這好；「悲觀」＝ 較差 10% 的門檻；「樂觀」＝ 較好 10% 的門檻。金額單位：元。</p>
            <table className="loan-table stack">
              <thead><tr>
                <th>年度</th><th>00919 配息（中位數）</th><th>配息範圍（悲觀～樂觀）</th><th>二代健保費</th><th>每月還款</th><th>剩餘貸款</th>
                <th>總資產 中位數</th><th>總資產 悲觀</th><th>總資產 樂觀</th><th>淨資產 中位數</th><th>淨資產 悲觀</th><th>淨資產 樂觀</th><th>賣股票還款（中位數）</th>
              </tr></thead>
              <tbody>
                {mc.years.map((r) => (
                  <tr key={r.year}>
                    <th scope="row">{r.year}{r.months < 12 ? `（${r.months} 個月）` : ""}</th>
                    <td className="num" data-label="00919 配息（中位數）">{money(r.divNet[1])}</td><td className="num" data-label="配息範圍">{money(r.divNet[0])}～{money(r.divNet[2])}</td>
                    <td className="num" data-label="二代健保費">{money(r.nhi)}</td><td className="num" data-label="每月還款">{money(mc.pay)}</td><td className="num" data-label="剩餘貸款">{money(r.bal)}</td>
                    <td className="num" data-label="總資產 中位數">{money(r.pot[1])}</td><td className="num" data-label="總資產 悲觀">{money(r.pot[0])}</td><td className="num" data-label="總資產 樂觀">{money(r.pot[2])}</td>
                    <td className="num" data-label="淨資產 中位數">{signedMoney(r.net[1])}</td><td className="num" data-label="淨資產 悲觀">{signedMoney(r.net[0])}</td><td className="num" data-label="淨資產 樂觀">{signedMoney(r.net[2])}</td>
                    <td className="num" data-label="賣股票還款">{money(r.sold)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Fold>

          {sumry && sumry.total > 0 && (
            <Fold title="如果過去每個月都當成今天進場" hint="用你現在的設定回頭看，結果落在哪一段">
              <div className="loan-minis four">
                {[["虧損（淨資產 < 0）", 0], ["0 ～ 100 萬", 1], ["100 萬 ～ 300 萬", 2], ["超過 300 萬", 3]].map(([t, i]) => (
                  <Mini key={t} label={t} value={`${((sumry.b[i] / sumry.total) * 100).toFixed(0)}%`} sentence={`${sumry.b[i]} / ${sumry.total} 個月份`} />
                ))}
              </div>
              <p className="loan-hint">把 0050 歷史上每一個月份都當成進場點、跑滿 {sn} 個月。相鄰月份的走勢高度重疊，不是獨立樣本；只是讓你知道過去各種走法大致落在哪一段。</p>
            </Fold>
          )}

          <Fold title="存檔對照：之後檢查預估準不準" hint={snaps.length ? `已存 ${snaps.length} 筆` : "把今天的預估存起來"}>
            <p className="loan-hint">把你現在的設定、預估範圍和歷史劇本結果存在這台瀏覽器。之後出現實際走勢，就能看實際有沒有落在當時預估的範圍裡。</p>
            <button type="button" className="btn primary sm" onClick={saveSnap} disabled={!canSave}>儲存今天的預估</button>
            {snaps.length > 0 && (
              <ul className="loan-snaps">
                {snaps.map((sn, i) => {
                  const ac = snapActual[i];
                  const ak = ac && ac.histMonths > 0 ? Math.min(ac.histMonths, sn.fan.p50.length - 1) : 0;
                  const act = ak > 0 ? ac.net[ak] : null;
                  const sp = sn.spec;
                  return (
                    <li key={`${sn.date}-${i}`}>
                      <div className="loan-snap-head"><b>{sn.date}</b><button type="button" className="btn sm" onClick={() => setSnaps((a) => a.filter((_, j) => j !== i))}>刪除</button></div>
                      <div className="loan-hint">起算 {sp.origin}、貸款 {money(sp.loan ?? 0)}、{sp.n} 個月、年成長 {sp.retMkt}%／{sp.ret919}%、{sp.method === "normal" ? "常態分布" : "歷史抽樣"}</div>
                      <div>當時預估到期淨資產：較差 10% <b className="num">{signedMoney(sn.head?.net?.[0] ?? 0)}</b>／中間值 <b className="num">{signedMoney(sn.head?.net?.[1] ?? 0)}</b>／較好 10% <b className="num">{signedMoney(sn.head?.net?.[2] ?? 0)}</b></div>
                      <div>{act == null ? "實際走勢：還沒有實際資料" : `實際走勢（第 ${ak} 個月，用當時設定算）：${signedMoney(act)}；當時預估 ${signedMoney(sn.fan.p10[ak])}～${signedMoney(sn.fan.p90[ak])}，中間值 ${signedMoney(sn.fan.p50[ak])}`}</div>
                    </li>
                  );
                })}
              </ul>
            )}
          </Fold>
        </div>
      ))}

      {/* ===== 歷史驗證 ===== */}
      {isHist && (results.length === 0 ? (
        <>
          <div className="notice warn loan-gap">沒有足夠長的歷史可以跑這個期數／進場月份。</div>
          {assumptions}
        </>
      ) : (
        <>
          <section className={`loan-hero ${needTone(histNeedPr)}`} aria-labelledby="loan-hero-h">
            <h3 id="loan-hero-h" className="loan-hero-q">先看結論：過去 {results.length} 個月份進場，各跑滿 {n} 個月</h3>
            <p className="loan-hero-a">
              中間水準最後{quantile(finals, 0.5) >= 0 ? "淨賺" : "淨虧"}約 <b className="num">{money(Math.abs(quantile(finals, 0.5)))}</b> 元；
              {ruinN > 0 ? <>其中 <b className="num">{ruinN}</b> 個月份（{pct(ruinN, results.length)}）途中資金不夠、得自掏腰包。</> : <>沒有任何月份出現資金不夠、得自掏腰包的情況。</>}
            </p>
            <div className="loan-stats">
              <Stat label="中間水準的結果" value={signedMoney(quantile(finals, 0.5))} sentence="中位數：一半進場月份比這好、一半比這差（單位：元）" />
              <Stat tone={needTone(histNeedPr)} label={`途中要自掏腰包的比例（${needWord(histNeedPr)}）`} value={pct(ruinN, results.length)} sentence={`${ruinN} / ${results.length} 個進場月份；含最後一期共 ${anyN} 個`} />
              <Stat label="最倒楣的進場時間" value={signedMoney(finals[0])} sentence={`${worst.month} 進場；自掏腰包最多 ${money(maxShort)} 元`} />
            </div>
            <div className="loan-minis">
              <Mini label="運氣不好時（較差 10%）" value={signedMoney(quantile(finals, 0.1))} sentence="10 個月份裡有 1 個比這更差" />
              <Mini label="運氣好時（較好 10%）" value={signedMoney(quantile(finals, 0.9))} sentence="10 個月份裡有 1 個比這更好" />
              <Mini label="最幸運的進場時間" value={signedMoney(finals[finals.length - 1])} sentence={`${best.month} 進場`} />
            </div>
            <p className="loan-fine">「最終淨值」＝ 還清貸款後所有資產，已扣自掏腰包與薪水。途中最低淨值 {signedMoney(lowNetRes.lowNet)} 元（{lowNetRes.month} 進場、第 {lowNetRes.lowNetK} 個月，＝資金池減剩餘貸款）。歷史不代表未來。</p>
          </section>

          {assumptions}

          <Card title="每個月份進場的最終淨值" icon={LineChart} className="loan-card">
            <Legend items={[["bar", "#2563eb", "正常（最終為正）"], ["hatch", "#dc2626", "斜線：途中需自掏腰包"], ["bar", "#f59e0b", "期末淨值為負"]]} />
            <ResultBars results={results} selIdx={selIdx} onPick={(m) => set({ sel: m })} />
            <p className="loan-hint">每根柱子 = 一個進場月份；點柱子可切換下面的詳細走勢。</p>
          </Card>

          {detail && (
            <Card title="看某一個月份進場的詳細走勢" icon={LineChart} className="loan-card">
              <div className="loan-toolbar">
                <label className="loan-toolbar-item">進場月份
                  <select className="input" value={selMonth} onChange={(e) => set({ sel: e.target.value })}>
                    {results.map((r) => <option key={r.month} value={r.month}>{r.month}</option>)}
                  </select>
                </label>
              </div>
              <p className="loan-note">預設顯示最差的月份。最終淨值 <b className="num">{signedMoney(selRes.final)}</b> 元；累計配息 {money(detail.cumDiv)}、健保補充保費 {money(detail.cumNhi)}、薪水支付 {money(detail.salaryPaid)}、自掏腰包 {money(detail.short)}（單位：元）。</p>
              <Legend items={[["line", "#2563eb", "資金池（股票＋現金）"], ["dash", "#dc2626", "還沒還的貸款"]]} />
              <PotChart series={detail.series} />
              <Fold title="逐月明細" hint="表格">
                <table className="loan-table stack">
                  <thead><tr><th>期末</th><th>股票市值</th><th>現金</th><th>累計配息</th><th>累計健保費</th><th>累計自掏腰包</th><th>剩餘貸款</th></tr></thead>
                  <tbody>
                    {detail.rows.map((r) => (
                      <tr key={r.k}>
                        <th scope="row">第 {r.k} 月（{r.label}）</th>
                        <td className="num" data-label="股票市值">{money(r.sv)}</td><td className="num" data-label="現金">{money(r.cash)}</td>
                        <td className="num" data-label="累計配息">{money(r.cumDiv)}</td><td className="num" data-label="累計健保費">{money(r.cumNhi)}</td>
                        <td className="num" data-label="累計自掏腰包">{money(r.short)}</td><td className="num" data-label="剩餘貸款">{money(r.bal)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Fold>
            </Card>
          )}
        </>
      ))}

      <Fold title="注意事項" hint="看完再決定">
        <ul className="loan-notes">
          {isFuture ? (
            <>
              <li>這是根據你設定的報酬假設算出來的可能範圍，不是預測；報酬假設一改，結果就跟著變。</li>
              <li>009816、00919 上市時間短，沒有足夠歷史，所以用 0050 過去的漲跌節奏來模擬。</li>
              <li>單筆配息未達 20,000 元不扣二代健保。</li>
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
      </Fold>
    </div>
  );
}
