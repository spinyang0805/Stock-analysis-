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

function findStock(q) {
  const s = String(q || "").trim().toLowerCase();
  if (!s) return [];
  return STOCKS.filter(x => x.code.includes(s) || x.name.toLowerCase().includes(s)).slice(0, 8);
}
function resolveStock(q) {
  const s = String(q || "2330").trim();
  return STOCKS.find(x => x.code === s || x.name === s) || { code: s.replace(".TW", ""), name: s, market: "--", industry: "--" };
}
function fmt(v, d = 2) { return v == null || Number.isNaN(Number(v)) ? "--" : Number(v).toFixed(d); }
function series(chart, Type, options, fallback) { return typeof chart.addSeries === "function" && Type ? chart.addSeries(Type, options) : chart[fallback](options); }
function normalizeKline(payload) { return Array.isArray(payload?.data) ? payload.data.filter(x => x?.time && x.open != null && x.close != null) : []; }
function val(data, key) { return data.filter(x => x[key] != null).map(x => ({ time: x.time, value: Number(x[key]) })); }
function volume(data) { return data.map(x => ({ time: x.time, value: Number(x.volume || 0), color: x.close >= x.open ? "rgba(34,197,94,.55)" : "rgba(239,68,68,.55)" })); }
function scoreColor(s) { return s >= 20 ? "#22c55e" : s <= -15 ? "#ef4444" : "#f59e0b"; }
function Card({ title, children }) { return <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 16, padding: 16 }}><h3 style={{ margin: "0 0 12px" }}>{title}</h3>{children}</section>; }
function Row({ label, value, color }) { return <div style={{ display: "flex", justifyContent: "space-between", borderBottom: "1px solid rgba(148,163,184,.14)", padding: "6px 0", gap: 12 }}><span style={{ color: "#94a3b8" }}>{label}</span><b style={{ color: color || "#e2e8f0" }}>{value ?? "--"}</b></div>; }

function localAnalysis(code, k) {
  const last = k.at(-1) || {};
  const ma5 = last.ma5 ?? last.close;
  const ma20 = last.ma20 ?? last.close;
  const score = Math.round((last.close > ma5 ? 12 : -8) + (ma5 > ma20 ? 18 : -10));
  const trend = score >= 20 ? "偏多" : score <= -10 ? "偏空" : "整理";
  return { score, trend, rating: score >= 20 ? "Bullish" : score <= -10 ? "Bearish" : "Neutral", summary: `${code} 由前端備援分析判斷為「${trend}」。`, indicators: last, signals: [] };
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
          setKlinePayload(kp); setAnalysis(ap || localAnalysis(code, k)); setDashboard(dp); setStatus("已連接 API，資料已載入");
        }
      } catch (e) { if (!dead) setStatus(`API 連線失敗：${e.message}`); }
    }
    load(); return () => { dead = true; };
  }, [stock.code, showMA, showBB]);

  function choose(x) { setStock(x); setInput(`${x.code} ${x.name}`); setOpenSuggest(false); }
  function submit() { const s = suggestions[0] || resolveStock(input); choose(s); }

  const meta = { ...stock, ...(klinePayload?.meta || {}), ...(analysis?.meta || {}), ...(dashboard?.basic || {}) };
  const latest = analysis?.indicators || {};
  const board = dashboard?.dashboard || {};
  const chip = board.chip || {};
  const order = dashboard?.basic || {};
  const pc = (meta.change ?? 0) >= 0 ? "#22c55e" : "#ef4444";

  return <div style={{ minHeight: "100vh", background: "#020617", color: "white", fontFamily: "Arial, sans-serif" }}>
    <header style={{ padding: 24, borderBottom: "1px solid #1e293b", background: "#0f172a" }}>
      <div style={{ color: "#38bdf8", letterSpacing: 1 }}>TW STOCK DECISION SYSTEM</div>
      <h1>券商等級交易決策 Dashboard</h1>
      <div style={{ fontSize: 26, fontWeight: 800 }}>{meta.name || stock.name} <span style={{ color: "#facc15" }}>{meta.code || stock.code}</span> <span style={{ color: "#94a3b8", fontSize: 15 }}>{meta.market || stock.market}・{meta.industry || stock.industry}</span></div>
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
      <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 }}><h2>{meta.code || stock.code} {meta.name || stock.name} K線 + 均線 + 布林</h2><div ref={priceRef} /><h3>成交量</h3><div ref={volRef} /><h3>RSI14</h3><div ref={rsiRef} /><h3>MACD</h3><div ref={macdRef} /></section>
      <aside style={{ display: "grid", gap: 12, alignContent: "start" }}>
        <Card title="Decision Score"><div style={{ fontSize: 46, color: scoreColor(analysis?.score ?? 0), fontWeight: 900 }}>{analysis?.score ?? "--"}</div><b>{analysis?.trend}</b><p style={{ color: "#94a3b8" }}>{analysis?.summary}</p></Card>
        <Card title="原本技術分析總覽"><Row label="MA5" value={fmt(latest.ma5)} color="#facc15"/><Row label="MA20" value={fmt(latest.ma20)} color="#38bdf8"/><Row label="MA60" value={fmt(latest.ma60)} color="#a78bfa"/><Row label="RSI14" value={fmt(latest.rsi14)}/><Row label="MACD" value={fmt(latest.macd)}/><Row label="布林上軌" value={fmt(latest.bb_upper)}/><Row label="布林下軌" value={fmt(latest.bb_lower)}/></Card>
        <Card title="籌碼分析"><Row label="外資" value={chip.foreign ?? "待資料"}/><Row label="投信" value={chip.investment_trust ?? "待資料"}/><Row label="自營商" value={chip.dealer ?? "待資料"}/><Row label="融資餘額" value={chip.margin_balance ?? "待資料"}/><Row label="融券餘額" value={chip.short_balance ?? "待資料"}/></Card>
        <Card title="五檔委買委賣"><div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}><div><b style={{color:"#22c55e"}}>買</b>{(order.bids||[]).map((b,i)=><Row key={i} label={b.qty} value={fmt(b.price)} color="#22c55e" />)}</div><div><b style={{color:"#ef4444"}}>賣</b>{(order.asks||[]).map((a,i)=><Row key={i} label={fmt(a.price)} value={a.qty} color="#ef4444" />)}</div></div></Card>
        <Card title="操作劇本"><Row label="突破" value={board.scenario?.breakout || "資料建立中"}/><Row label="回檔" value={board.scenario?.pullback || "資料建立中"}/><Row label="風險" value={board.scenario?.risk || "資料建立中"}/></Card>
      </aside>
    </main>
  </div>;
}
