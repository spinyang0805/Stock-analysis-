import { useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries, LineSeries, HistogramSeries } = LightweightCharts;
const API = import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-api-ihun.onrender.com";
const APP_VERSION = "v14";
const BUILD_LABEL = "2026-05-14 21:28";
const COMMIT_LABEL = "front-chart-flow-fix";

const STOCKS = [
  { code: "2330", name: "台積電", market: "上市", industry: "半導體" },
  { code: "2408", name: "南亞科", market: "上市", industry: "半導體" },
  { code: "3702", name: "大聯大", market: "上市", industry: "電子通路" },
  { code: "2317", name: "鴻海", market: "上市", industry: "其他電子" },
  { code: "2454", name: "聯發科", market: "上市", industry: "半導體" },
  { code: "2308", name: "台達電", market: "上市", industry: "電子零組件" },
  { code: "2382", name: "廣達", market: "上市", industry: "電腦及週邊" },
  { code: "0050", name: "元大台灣50", market: "上市", industry: "ETF" },
  { code: "00981A", name: "主動式ETF", market: "上市", industry: "ETF" },
  { code: "00679B", name: "元大美債20年", market: "上市", industry: "債券ETF" },
];

function cleanCode(value) {
  return String(value || "2330").trim().replace(".TW", "").replace(".TWO", "").split(/\s+/)[0].toUpperCase();
}

function findStock(q) {
  const s = String(q || "").trim().toLowerCase();
  if (!s) return [];
  return STOCKS.filter((x) => x.code.toLowerCase().includes(s) || x.name.toLowerCase().includes(s)).slice(0, 8);
}

function resolveStock(q) {
  const s = String(q || "2330").trim();
  const found = STOCKS.find((x) => x.code === s.toUpperCase() || x.name === s);
  return found || { code: cleanCode(s), name: s || cleanCode(s), market: "--", industry: "--" };
}

function fmt(v, d = 2) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(d) : "--";
}

function addSeries(chart, Type, options, fallback) {
  return typeof chart.addSeries === "function" && Type ? chart.addSeries(Type, options) : chart[fallback](options);
}

function toChartTime(row) {
  const date = String(row?.date || "").trim();
  if (/^\d{8}$/.test(date)) return `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`;
  if (typeof row?.time === "number" && Number.isFinite(row.time)) return Math.floor(row.time);
  return null;
}

function normalizeRows(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  const used = new Set();
  return rows
    .map((r) => ({
      ...r,
      time: toChartTime(r),
      open: Number(r.open),
      high: Number(r.high),
      low: Number(r.low),
      close: Number(r.close),
      volume: Number(r.volume || 0),
    }))
    .filter((r) => r.time && [r.open, r.high, r.low, r.close].every(Number.isFinite))
    .sort((a, b) => String(a.time).localeCompare(String(b.time)))
    .filter((r) => {
      if (used.has(r.time)) return false;
      used.add(r.time);
      return true;
    });
}

function line(rows, key) {
  return rows
    .filter((r) => r[key] !== null && r[key] !== undefined && Number.isFinite(Number(r[key])))
    .map((r) => ({ time: r.time, value: Number(r[key]) }));
}

function volumeRows(rows) {
  return rows.map((r) => ({
    time: r.time,
    value: Number(r.volume || 0),
    color: r.close >= r.open ? "rgba(34,197,94,.55)" : "rgba(239,68,68,.55)",
  }));
}

function Card({ title, children }) {
  return <section style={cardStyle}><h3 style={{ marginTop: 0 }}>{title}</h3>{children}</section>;
}

function Row({ label, value }) {
  return <div style={{ display: "flex", justifyContent: "space-between", gap: 12, borderBottom: "1px solid rgba(148,163,184,.16)", padding: "6px 0" }}><span style={{ color: "#94a3b8" }}>{label}</span><b>{value}</b></div>;
}

export default function App() {
  const priceRef = useRef(null);
  const volRef = useRef(null);
  const rsiRef = useRef(null);
  const macdRef = useRef(null);
  const charts = useRef([]);
  const series = useRef({});

  const [input, setInput] = useState("2330");
  const [stock, setStock] = useState(resolveStock("2330"));
  const [openSuggest, setOpenSuggest] = useState(false);
  const [payload, setPayload] = useState(null);
  const [rows, setRows] = useState([]);
  const [analysis, setAnalysis] = useState(null);
  const [status, setStatus] = useState("初始化中");
  const [showMA, setShowMA] = useState(true);
  const [showBB, setShowBB] = useState(true);

  const suggestions = useMemo(() => findStock(input), [input]);

  useEffect(() => {
    const makeChart = (el, height) => createChart(el, {
      width: el.clientWidth || 900,
      height,
      layout: { background: { color: "#0f172a" }, textColor: "#dbeafe" },
      grid: { vertLines: { color: "#1e293b" }, horzLines: { color: "#1e293b" } },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#334155" },
      rightPriceScale: { borderColor: "#334155" },
    });
    const c1 = makeChart(priceRef.current, 420);
    const c2 = makeChart(volRef.current, 130);
    const c3 = makeChart(rsiRef.current, 130);
    const c4 = makeChart(macdRef.current, 150);
    charts.current = [c1, c2, c3, c4];
    series.current = {
      candle: addSeries(c1, CandlestickSeries, { upColor: "#22c55e", downColor: "#ef4444", borderUpColor: "#22c55e", borderDownColor: "#ef4444", wickUpColor: "#22c55e", wickDownColor: "#ef4444" }, "addCandlestickSeries"),
      ma5: addSeries(c1, LineSeries, { color: "#facc15", lineWidth: 1 }, "addLineSeries"),
      ma20: addSeries(c1, LineSeries, { color: "#38bdf8", lineWidth: 1 }, "addLineSeries"),
      ma60: addSeries(c1, LineSeries, { color: "#a78bfa", lineWidth: 1 }, "addLineSeries"),
      bbUpper: addSeries(c1, LineSeries, { color: "rgba(148,163,184,.75)", lineWidth: 1 }, "addLineSeries"),
      bbLower: addSeries(c1, LineSeries, { color: "rgba(148,163,184,.75)", lineWidth: 1 }, "addLineSeries"),
      volume: addSeries(c2, HistogramSeries, { priceFormat: { type: "volume" } }, "addHistogramSeries"),
      rsi: addSeries(c3, LineSeries, { color: "#f59e0b", lineWidth: 2 }, "addLineSeries"),
      macd: addSeries(c4, HistogramSeries, {}, "addHistogramSeries"),
    };
    const resize = () => charts.current.forEach((c) => c.applyOptions({ width: priceRef.current?.clientWidth || 900 }));
    window.addEventListener("resize", resize);
    return () => {
      window.removeEventListener("resize", resize);
      charts.current.forEach((c) => c.remove());
    };
  }, []);

  useEffect(() => {
    const s = series.current;
    if (!s.candle) return;
    s.candle.setData(rows.map((r) => ({ time: r.time, open: r.open, high: r.high, low: r.low, close: r.close })));
    s.volume.setData(volumeRows(rows));
    s.rsi.setData(line(rows, "rsi14"));
    s.macd.setData(line(rows, "macd_hist"));
    s.ma5.setData(showMA ? line(rows, "ma5") : []);
    s.ma20.setData(showMA ? line(rows, "ma20") : []);
    s.ma60.setData(showMA ? line(rows, "ma60") : []);
    s.bbUpper.setData(showBB ? line(rows, "bb_upper") : []);
    s.bbLower.setData(showBB ? line(rows, "bb_lower") : []);
    charts.current.forEach((c) => c.timeScale().fitContent());
  }, [rows, showMA, showBB]);

  useEffect(() => {
    let alive = true;
    async function load() {
      const code = stock.code;
      setStatus(`讀取 ${code} K線中...`);
      setRows([]);
      setPayload(null);
      try {
        const kRes = await fetch(`${API}/api/kline/${code}`);
        const kJson = await kRes.json();
        if (!kRes.ok) throw new Error(kJson?.detail || kJson?.error || `HTTP ${kRes.status}`);
        const nextRows = normalizeRows(kJson);
        if (!alive) return;
        setPayload(kJson);
        setRows(nextRows);
        setStatus(nextRows.length ? `已載入 ${nextRows.length} 筆K線資料（${APP_VERSION}）` : `API回傳0筆K線（${APP_VERSION}）`);

        fetch(`${API}/api/analysis/${code}`)
          .then((r) => r.ok ? r.json() : null)
          .then((j) => { if (alive) setAnalysis(j); })
          .catch(() => {});
      } catch (e) {
        if (alive) setStatus(`讀取失敗：${e.message}（${APP_VERSION}）`);
      }
    }
    load();
    return () => { alive = false; };
  }, [stock.code]);

  function submit() {
    const target = suggestions[0] || resolveStock(input);
    setStock(target);
    setInput(target.code);
    setOpenSuggest(false);
  }

  const meta = { ...stock, ...(payload?.meta || {}), ...(analysis?.meta || {}) };
  const latest = rows.at(-1) || {};
  const pc = Number(meta.change || 0) >= 0 ? "#22c55e" : "#ef4444";

  return <div style={{ minHeight: "100vh", background: "#020617", color: "white", fontFamily: "Arial, sans-serif" }}>
    <header style={{ padding: 24, borderBottom: "1px solid #1e293b", background: "#0f172a" }}>
      <div style={{ color: "#38bdf8", letterSpacing: 1, fontWeight: 800 }}>TW STOCK DECISION SYSTEM {APP_VERSION}</div>
      <div style={{ color: "#64748b", fontSize: 12, marginTop: 4 }}>Build: {BUILD_LABEL} | Commit: {COMMIT_LABEL}</div>
      <h1>券商等級交易決策 Dashboard</h1>
      <div style={{ fontSize: 26, fontWeight: 800 }}><span style={{ color: "#facc15" }}>{meta.code || stock.code}</span> {meta.name || stock.name} <span style={{ color: "#94a3b8", fontSize: 15 }}>{meta.market || stock.market}・{meta.industry || stock.industry}</span></div>
      <div style={{ fontSize: 44, fontWeight: 900, color: pc }}>{fmt(meta.price ?? latest.close)}</div>
      <div style={{ color: pc }}>{fmt(meta.change)} ({fmt(meta.change_pct)}%)　開 {fmt(meta.open ?? latest.open)}　高 {fmt(meta.high ?? latest.high)}　低 {fmt(meta.low ?? latest.low)}　收 {fmt(meta.close ?? latest.close)}</div>
      <div style={{ marginTop: 18, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ position: "relative" }}>
          <input value={input} onFocus={() => setOpenSuggest(true)} onChange={(e) => { setInput(e.target.value); setOpenSuggest(true); }} onKeyDown={(e) => e.key === "Enter" && submit()} placeholder="輸入公司或股號，例如 2330 / 南亞科" style={inputStyle} />
          {openSuggest && suggestions.length > 0 && <div style={suggestStyle}>{suggestions.map((x) => <div key={x.code} onMouseDown={() => { setStock(x); setInput(x.code); setOpenSuggest(false); }} style={suggestItemStyle}><b style={{ color: "#facc15" }}>{x.code}</b> {x.name}<span style={{ color: "#94a3b8", marginLeft: 8 }}>{x.market}・{x.industry}</span></div>)}</div>}
        </div>
        <button onClick={submit} style={btnStyle}>查詢分析</button>
        <label><input type="checkbox" checked={showMA} onChange={(e) => setShowMA(e.target.checked)} /> MA</label>
        <label><input type="checkbox" checked={showBB} onChange={(e) => setShowBB(e.target.checked)} /> 布林</label>
        <span style={{ color: rows.length ? "#22c55e" : "#f59e0b" }}>{status}</span>
      </div>
    </header>

    <main style={{ padding: 18, display: "grid", gridTemplateColumns: "minmax(0,2fr) minmax(340px,.9fr)", gap: 18 }}>
      <section style={cardStyle}>
        <h2>{meta.code || stock.code} {meta.name || stock.name} K線 + 均線 + 布林</h2>
        <div ref={priceRef} />
        <h3>成交量</h3><div ref={volRef} />
        <h3>RSI14</h3><div ref={rsiRef} />
        <h3>MACD</h3><div ref={macdRef} />
      </section>
      <aside style={{ display: "grid", gap: 12, alignContent: "start" }}>
        <Card title="資料狀態"><Row label="版本" value={APP_VERSION} /><Row label="K線筆數" value={rows.length} /><Row label="API狀態" value={payload?.status || "--"} /><Row label="資料日期" value={meta.data_date || latest.date || "--"} /></Card>
        <Card title="Decision Score"><div style={{ fontSize: 46, color: Number(analysis?.score || 0) <= -15 ? "#ef4444" : "#22c55e", fontWeight: 900 }}>{analysis?.score ?? "--"}</div><b>{analysis?.trend || "等待分析"}</b><p style={{ color: "#94a3b8" }}>{analysis?.summary || "K線資料載入後會顯示分析結果。"}</p></Card>
        <Card title="技術指標"><Row label="MA5" value={fmt(latest.ma5)} /><Row label="MA20" value={fmt(latest.ma20)} /><Row label="MA60" value={fmt(latest.ma60)} /><Row label="RSI14" value={fmt(latest.rsi14)} /><Row label="MACD" value={fmt(latest.macd)} /></Card>
      </aside>
    </main>
  </div>;
}

const inputStyle = { padding: "12px 14px", borderRadius: 10, border: "1px solid #334155", background: "#020617", color: "white", minWidth: 320 };
const btnStyle = { padding: "12px 18px", borderRadius: 10, border: 0, background: "#2563eb", color: "white", fontWeight: 700 };
const cardStyle = { background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 };
const suggestStyle = { position: "absolute", top: 48, left: 0, right: 0, background: "#0f172a", border: "1px solid #334155", borderRadius: 12, zIndex: 10, overflow: "hidden" };
const suggestItemStyle = { padding: "10px 12px", cursor: "pointer", borderBottom: "1px solid rgba(148,163,184,.15)" };
