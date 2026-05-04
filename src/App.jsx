import { useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries, LineSeries, HistogramSeries } = LightweightCharts;
const API = import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-api-ihun.onrender.com";

const STOCKS = [
  { code: "2330", name: "台積電", market: "上市", industry: "半導體" },
  { code: "3702", name: "大聯大", market: "上市", industry: "電子通路" },
  { code: "2317", name: "鴻海", market: "上市", industry: "其他電子" },
  { code: "2454", name: "聯發科", market: "上市", industry: "半導體" },
  { code: "2382", name: "廣達", market: "上市", industry: "電腦及週邊" },
  { code: "3231", name: "緯創", market: "上市", industry: "電腦及週邊" },
  { code: "2324", name: "仁寶", market: "上市", industry: "電腦及週邊" },
  { code: "2308", name: "台達電", market: "上市", industry: "電子零組件" },
  { code: "2357", name: "華碩", market: "上市", industry: "電腦及週邊" },
  { code: "2412", name: "中華電", market: "上市", industry: "通信網路" },
  { code: "2881", name: "富邦金", market: "上市", industry: "金融保險" },
  { code: "2882", name: "國泰金", market: "上市", industry: "金融保險" },
  { code: "2303", name: "聯電", market: "上市", industry: "半導體" },
  { code: "3711", name: "日月光投控", market: "上市", industry: "半導體" },
  { code: "2618", name: "長榮航", market: "上市", industry: "航運" },
  { code: "2603", name: "長榮", market: "上市", industry: "航運" },
  { code: "3037", name: "欣興", market: "上市", industry: "電子零組件" },
  { code: "3583", name: "辛耘", market: "上櫃", industry: "半導體設備" },
];

const PERSPECTIVE_FALLBACK = [
  { category: "trend", title: "趨勢面", status: "等待資料", level: "neutral", meaning: "依照 MA5、MA20、MA60 與四線多排判斷股票趨勢強弱。", logic: "MA5 > MA20 > MA60 或 MA5 > MA10 > MA20 > MA60" },
  { category: "volume_price", title: "量價面", status: "等待資料", level: "neutral", meaning: "觀察成交量與價格同步或背離，用來判斷攻擊、出貨或洗盤。", logic: "Volume vs Volume_MA5 + Price_Change" },
  { category: "chip", title: "籌碼面", status: "等待資料", level: "neutral", meaning: "觀察外資、投信、自營商與大戶籌碼是否集中。", logic: "Foreign / Trust / Dealer rolling sum and streak" },
  { category: "credit", title: "信用交易", status: "等待資料", level: "neutral", meaning: "觀察融資、融券與券資比，辨識軋空或斷頭風險。", logic: "Short_Margin_Ratio、Margin_Ratio、Close vs MA60" },
  { category: "risk", title: "風險面", status: "等待資料", level: "neutral", meaning: "整合融資過高、正乖離過大、跌破季線等風險訊號。", logic: "Margin_Ratio > 60% OR Bias > 15% OR Close < MA60" },
];

function cleanCode(q) { return String(q || "").trim().split(/\s+/)[0].replace(".TW", "").replace(".TWO", ""); }
function findStock(q) {
  const s = String(q || "").trim().toLowerCase();
  if (!s) return [];
  return STOCKS.filter(x => x.code.includes(s) || x.name.toLowerCase().includes(s)).slice(0, 8);
}
function resolveStock(q) {
  const s = cleanCode(q || "2330");
  const found = STOCKS.find(x => x.code === s || x.name === s);
  return found || { code: s, name: s, market: "--", industry: "--" };
}
function fmt(v, d = 2) { return v == null || Number.isNaN(Number(v)) ? "--" : Number(v).toFixed(d); }
function displayValue(v) { return v == null || v === "" ? "待資料" : v; }
function series(chart, Type, options, fallback) { return typeof chart.addSeries === "function" && Type ? chart.addSeries(Type, options) : chart[fallback](options); }
function normalizeKline(payload) { return Array.isArray(payload?.data) ? payload.data.filter(x => x?.time && x.open != null && x.close != null) : []; }
function val(data, key) { return data.filter(x => x[key] != null).map(x => ({ time: x.time, value: Number(x[key]) })); }
function volume(data) { return data.map(x => ({ time: x.time, value: Number(x.volume || 0), color: x.close >= x.open ? "rgba(34,197,94,.55)" : "rgba(239,68,68,.55)" })); }
function scoreColor(s) { return s >= 20 ? "#22c55e" : s <= -15 ? "#ef4444" : "#f59e0b"; }
function levelColor(level) {
  const key = String(level || "neutral").toLowerCase();
  if (key.includes("bull")) return "#22c55e";
  if (key.includes("bear") || key.includes("risk") || key.includes("warning")) return "#ef4444";
  if (key.includes("strong")) return "#14b8a6";
  return "#f59e0b";
}
function Card({ title, children }) { return <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 16, padding: 16 }}><h3 style={{ margin: "0 0 12px" }}>{title}</h3>{children}</section>; }
function Row({ label, value, color }) { return <div style={{ display: "flex", justifyContent: "space-between", borderBottom: "1px solid rgba(148,163,184,.14)", padding: "6px 0", gap: 12 }}><span style={{ color: "#94a3b8" }}>{label}</span><b style={{ color: color || "#e2e8f0" }}>{value ?? "--"}</b></div>; }

function normalizePerspectives(analysis, dashboard) {
  const sources = [
    analysis?.perspective_cards,
    analysis?.analysis?.perspective_cards,
    dashboard?.analysis?.perspective_cards,
    dashboard?.dashboard?.analysis?.perspective_cards,
    dashboard?.decision?.perspective_cards,
  ];
  const cards = sources.find(Array.isArray);
  if (!cards || cards.length === 0) return PERSPECTIVE_FALLBACK;
  return cards.map((item, idx) => ({
    category: item.category || `perspective_${idx}`,
    title: item.title || item.category || "分析面向",
    status: item.status || "待判斷",
    level: item.level || "neutral",
    meaning: item.meaning || item.description || "此面向資料建立中。",
    logic: item.logic || item.condition || "後端尚未提供判斷邏輯",
  }));
}

function AnalysisPerspectiveCard({ perspectives }) {
  return <Card title="個股多面向分析">
    <div style={{ display: "grid", gap: 10 }}>
      {perspectives.map(item => <div key={item.category} style={{ border: "1px solid rgba(148,163,184,.18)", borderRadius: 14, padding: 12, background: "rgba(15,23,42,.7)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
          <b>{item.title}</b>
          <span style={{ color: levelColor(item.level), border: `1px solid ${levelColor(item.level)}`, borderRadius: 999, padding: "3px 9px", fontSize: 12, whiteSpace: "nowrap" }}>{item.status}</span>
        </div>
        <p style={{ color: "#cbd5e1", margin: "8px 0 6px", lineHeight: 1.5 }}>{item.meaning}</p>
        <div style={{ color: "#64748b", fontSize: 12 }}>判斷邏輯：{item.logic}</div>
      </div>)}
    </div>
  </Card>;
}

function localAnalysis(code, k) {
  const last = k.at(-1) || {};
  const ma5 = last.ma5 ?? last.close;
  const ma20 = last.ma20 ?? last.close;
  const ma60 = last.ma60 ?? last.close;
  const score = Math.round((last.close > ma5 ? 12 : -8) + (ma5 > ma20 ? 18 : -10));
  const trend = score >= 20 ? "偏多" : score <= -10 ? "偏空" : "整理";
  const perspective_cards = [
    { category: "trend", title: "趨勢面", status: ma5 > ma20 && ma20 > ma60 ? "多頭排列" : ma5 < ma20 && ma20 < ma60 ? "空頭排列" : "均線整理", level: ma5 > ma20 && ma20 > ma60 ? "bullish" : ma5 < ma20 && ma20 < ma60 ? "bearish" : "neutral", meaning: ma5 > ma20 && ma20 > ma60 ? "短中長期均線依序向上，代表趨勢偏強。" : "均線尚未形成明確多頭排列，需等待方向確認。", logic: "MA5 > MA20 > MA60" },
    { category: "volume_price", title: "量價面", status: "待後端量價矩陣", level: "neutral", meaning: "需由後端依成交量均線與漲跌幅判斷量增價漲、量增價跌、量縮價漲或量縮價跌。", logic: "Volume > Volume_MA5 AND Price_Change threshold" },
    { category: "chip", title: "籌碼面", status: "等待 chip_data", level: "neutral", meaning: "需由 Firebase chip_data 彙總外資、投信、自營商與大戶籌碼。", logic: "foreign_5d_sum / trust_buy_streak / dealer_5d_sum" },
    { category: "credit", title: "信用交易", status: "等待信用資料", level: "neutral", meaning: "需由融資、融券與券資比判斷軋空或斷頭風險。", logic: "Short_Margin_Ratio > 30% AND Close > Price_20D_Max" },
    { category: "risk", title: "風險面", status: last.close < ma60 ? "跌破季線" : "待完整風險資料", level: last.close < ma60 ? "warning" : "neutral", meaning: last.close < ma60 ? "股價跌破 MA60，需留意中期趨勢轉弱。" : "尚需後端提供融資使用率與乖離率以完成風險判斷。", logic: "Close < MA60 OR Margin_Ratio > 60% OR Bias > 15%" },
  ];
  return { score, trend, rating: score >= 20 ? "Bullish" : score <= -10 ? "Bearish" : "Neutral", summary: `${code} 由前端備援分析判斷為「${trend}」。`, indicators: last, signals: [], perspective_cards };
}

export default function App() {
  const priceRef = useRef(null), volRef = useRef(null), rsiRef = useRef(null), macdRef = useRef(null);
  const chartRef = useRef([]), sRef = useRef({});
  const [input, setInput] = useState("2330");
  const [stock, setStock] = useState(resolveStock("2330"));
  const [openSuggest, setOpenSuggest] = useState(false);
  const [klinePayload, setKlinePayload] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [dashboard, setDashboard] = useState(null);
  const [status, setStatus] = useState("初始化中");
  const [showMA, setShowMA] = useState(true);
  const [showBB, setShowBB] = useState(true);
  const suggestions = useMemo(() => findStock(input), [input]);

  useEffect(() => {
    const base = (el, h) => createChart(el, { width: el.clientWidth || 900, height: h, layout: { background: { color: "#0f172a" }, textColor: "#dbeafe" }, grid: { vertLines: { color: "#1e293b" }, horzLines: { color: "#1e293b" } }, timeScale: { timeVisible: true, borderColor: "#334155" }, rightPriceScale: { borderColor: "#334155" } });
    const c1 = base(priceRef.current, 420), c2 = base(volRef.current, 130), c3 = base(rsiRef.current, 130), c4 = base(macdRef.current, 150);
    chartRef.current = [c1, c2, c3, c4];
    sRef.current = {
      candle: series(c1, CandlestickSeries, { upColor: "#22c55e", downColor: "#ef4444", borderUpColor: "#22c55e", borderDownColor: "#ef4444", wickUpColor: "#22c55e", wickDownColor: "#ef4444" }, "addCandlestickSeries"),
      ma5: series(c1, LineSeries, { color: "#facc15", lineWidth: 1 }, "addLineSeries"),
      ma20: series(c1, LineSeries, { color: "#38bdf8", lineWidth: 1 }, "addLineSeries"),
      ma60: series(c1, LineSeries, { color: "#a78bfa", lineWidth: 1 }, "addLineSeries"),
      bbU: series(c1, LineSeries, { color: "rgba(148,163,184,.75)", lineWidth: 1 }, "addLineSeries"),
      bbL: series(c1, LineSeries, { color: "rgba(148,163,184,.75)", lineWidth: 1 }, "addLineSeries"),
      volume: series(c2, HistogramSeries, { priceFormat: { type: "volume" } }, "addHistogramSeries"),
      rsi: series(c3, LineSeries, { color: "#f59e0b", lineWidth: 2 }, "addLineSeries"),
      macd: series(c4, HistogramSeries, {}, "addHistogramSeries"),
    };
    const resize = () => chartRef.current.forEach(c => c.applyOptions({ width: priceRef.current?.clientWidth || 900 }));
    window.addEventListener("resize", resize);
    return () => { window.removeEventListener("resize", resize); chartRef.current.forEach(c => c.remove()); };
  }, []);

  useEffect(() => {
    let dead = false;
    async function load() {
      setStatus("讀取資料中...");
      const code = stock.code;
      try {
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), 6500);
        const [kr, ar, dr] = await Promise.allSettled([
          fetch(`${API}/api/kline/${code}`, { signal: ctrl.signal }),
          fetch(`${API}/api/analysis/${code}`, { signal: ctrl.signal }),
          fetch(`${API}/api/dashboard/${code}`, { signal: ctrl.signal }),
        ]);
        clearTimeout(timer);
        const kp = kr.status === "fulfilled" && kr.value.ok ? await kr.value.json() : null;
        const ap = ar.status === "fulfilled" && ar.value.ok ? await ar.value.json() : null;
        const dp = dr.status === "fulfilled" && dr.value.ok ? await dr.value.json() : null;
        const k = normalizeKline(kp);
        if (!dead) {
          const s = sRef.current;
          s.candle?.setData(k); s.volume?.setData(volume(k)); s.rsi?.setData(val(k, "rsi14")); s.macd?.setData(val(k, "macd_hist"));
          s.ma5?.setData(showMA ? val(k, "ma5") : []); s.ma20?.setData(showMA ? val(k, "ma20") : []); s.ma60?.setData(showMA ? val(k, "ma60") : []);
          s.bbU?.setData(showBB ? val(k, "bb_upper") : []); s.bbL?.setData(showBB ? val(k, "bb_lower") : []);
          chartRef.current.forEach(c => c.timeScale().fitContent());
          setKlinePayload(kp); setAnalysis(ap || localAnalysis(code, k)); setDashboard(dp); setStatus(kp?.status === "loading" ? "資料導入中，請稍後重新查詢" : "已連接 API，資料已載入");
        }
      } catch (e) { if (!dead) setStatus(`API 連線失敗：${e.message}`); }
    }
    load(); return () => { dead = true; };
  }, [stock.code, showMA, showBB]);

  function choose(x) { setStock(x); setInput(x.code); setOpenSuggest(false); }
  function submit() { const s = suggestions[0] || resolveStock(input); choose(s); }

  const meta = { ...stock, ...(klinePayload?.meta || {}), ...(analysis?.meta || {}), ...(dashboard?.basic || {}) };
  const latest = analysis?.indicators || {};
  const board = dashboard?.dashboard || {};
  const chip = board.chip || dashboard?.chip || dashboard?.basic?.chip || {};
  const order = dashboard?.basic || {};
  const perspectives = normalizePerspectives(analysis, dashboard);
  const pc = (meta.change ?? 0) >= 0 ? "#22c55e" : "#ef4444";
  const titleCode = meta.code || stock.code;
  const titleName = meta.name && meta.name !== titleCode ? meta.name : stock.name;

  return <div style={{ minHeight: "100vh", background: "#020617", color: "white", fontFamily: "Arial, sans-serif" }}>
    <header style={{ padding: 24, borderBottom: "1px solid #1e293b", background: "#0f172a" }}>
      <div style={{ color: "#38bdf8", letterSpacing: 1 }}>TW STOCK DECISION SYSTEM</div>
      <h1>券商等級交易決策 Dashboard</h1>
      <div style={{ fontSize: 26, fontWeight: 800 }}><span style={{ color: "#facc15" }}>{titleCode}</span> {titleName} <span style={{ color: "#94a3b8", fontSize: 15 }}>{meta.market || stock.market}・{meta.industry || stock.industry}</span></div>
      <div style={{ fontSize: 44, fontWeight: 900, color: pc }}>{fmt(meta.price)}</div>
      <div style={{ color: pc }}>{fmt(meta.change)} ({fmt(meta.change_pct)}%)　開 {fmt(meta.open)}　高 {fmt(meta.high)}　低 {fmt(meta.low)}　收 {fmt(meta.close)}</div>
      <div style={{ marginTop: 18, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ position: "relative" }}>
          <input value={input} onFocus={() => setOpenSuggest(true)} onChange={e => { setInput(e.target.value); setOpenSuggest(true); }} onKeyDown={e => e.key === "Enter" && submit()} placeholder="輸入公司或股號，例如 大聯 / 3702" style={{ padding: "12px 14px", borderRadius: 10, border: "1px solid #334155", background: "#020617", color: "white", minWidth: 320 }} />
          {openSuggest && suggestions.length > 0 && <div style={{ position: "absolute", top: 48, left: 0, right: 0, background: "#0f172a", border: "1px solid #334155", borderRadius: 12, zIndex: 10, overflow: "hidden" }}>{suggestions.map(x => <div key={x.code} onMouseDown={() => choose(x)} style={{ padding: "10px 12px", cursor: "pointer", borderBottom: "1px solid rgba(148,163,184,.15)" }}><b style={{ color: "#facc15" }}>{x.code}</b> {x.name}<span style={{ color: "#94a3b8", marginLeft: 8 }}>{x.market}・{x.industry}</span></div>)}</div>}
        </div>
        <button onClick={submit} style={{ padding: "12px 18px", borderRadius: 10, border: 0, background: "#2563eb", color: "white", fontWeight: 700 }}>查詢分析</button>
        <label><input type="checkbox" checked={showMA} onChange={e => setShowMA(e.target.checked)} /> MA</label>
        <label><input type="checkbox" checked={showBB} onChange={e => setShowBB(e.target.checked)} /> 布林</label>
        <span style={{ color: "#22c55e" }}>{status}</span>
      </div>
    </header>
    <main style={{ padding: 18, display: "grid", gridTemplateColumns: "minmax(0,2fr) minmax(360px,.9fr)", gap: 18 }}>
      <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 }}><h2>{titleCode} {titleName} K線 + 均線 + 布林</h2><div ref={priceRef} /><h3>成交量</h3><div ref={volRef} /><h3>RSI14</h3><div ref={rsiRef} /><h3>MACD</h3><div ref={macdRef} /></section>
      <aside style={{ display: "grid", gap: 12, alignContent: "start" }}>
        <Card title="Decision Score"><div style={{ fontSize: 46, color: scoreColor(analysis?.score ?? 0), fontWeight: 900 }}>{analysis?.score ?? "--"}</div><b>{analysis?.trend}</b><p style={{ color: "#94a3b8" }}>{analysis?.summary}</p></Card>
        <AnalysisPerspectiveCard perspectives={perspectives} />
        <Card title="原本技術分析總覽"><Row label="MA5" value={fmt(latest.ma5)} color="#facc15"/><Row label="MA20" value={fmt(latest.ma20)} color="#38bdf8"/><Row label="MA60" value={fmt(latest.ma60)} color="#a78bfa"/><Row label="RSI14" value={fmt(latest.rsi14)}/><Row label="MACD" value={fmt(latest.macd)}/><Row label="布林上軌" value={fmt(latest.bb_upper)}/><Row label="布林下軌" value={fmt(latest.bb_lower)}/></Card>
        <Card title="籌碼分析"><Row label="外資" value={displayValue(chip.foreign)}/><Row label="投信" value={displayValue(chip.investment_trust)}/><Row label="自營商" value={displayValue(chip.dealer)}/><Row label="融資餘額" value={displayValue(chip.margin_balance ?? chip.margin)}/><Row label="融券餘額" value={displayValue(chip.short_balance ?? chip.short)}/><Row label="資料來源" value={chip.source || "等待 Firebase chip_data"}/></Card>
        <Card title="五檔委買委賣"><div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}><div><b style={{color:"#22c55e"}}>買</b>{(order.bids||[]).map((b,i)=><Row key={i} label={b.qty} value={fmt(b.price)} color="#22c55e" />)}</div><div><b style={{color:"#ef4444"}}>賣</b>{(order.asks||[]).map((a,i)=><Row key={i} label={fmt(a.price)} value={a.qty} color="#ef4444" />)}</div></div></Card>
        <Card title="操作劇本"><Row label="突破" value={board.scenario?.breakout || "資料建立中"}/><Row label="回檔" value={board.scenario?.pullback || "資料建立中"}/><Row label="風險" value={board.scenario?.risk || "資料建立中"}/></Card>
      </aside>
    </main>
  </div>;
}
