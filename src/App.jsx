import { useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries, LineSeries, HistogramSeries } = LightweightCharts;
const DEFAULT_API = "https://stock-analysis-api-ihun.onrender.com";
const RAW_API = import.meta.env.VITE_API_BASE_URL || DEFAULT_API;
const API = String(RAW_API).includes("stock-analysis-api-ihun") ? String(RAW_API).replace(/\/$/, "") : DEFAULT_API;
const APP_VERSION = "v16";
const BUILD_LABEL = "2026-05-14 21:45";
const COMMIT_LABEL = "chip-card-ui";

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
  const raw = String(q || "2330").trim();
  const found = STOCKS.find((x) => x.code === raw.toUpperCase() || x.name === raw);
  return found || { code: cleanCode(raw), name: raw || cleanCode(raw), market: "--", industry: "--" };
}
function fmt(v, d = 2) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(d) : "--";
}
function pickRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.rows)) return payload.rows;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.kline)) return payload.kline;
  if (Array.isArray(payload?.kline_data)) return payload.kline_data;
  if (Array.isArray(payload?.daily)) return payload.daily;
  if (Array.isArray(payload?.result?.data)) return payload.result.data;
  if (Array.isArray(payload?.result?.rows)) return payload.result.rows;
  if (Array.isArray(payload?.basic?.data)) return payload.basic.data;
  return [];
}
function toChartTime(row) {
  const date = String(row?.date || row?.Date || row?.data_date || "").trim();
  if (/^\d{8}$/.test(date)) return `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(date)) return date;
  if (typeof row?.time === "number" && Number.isFinite(row.time)) return Math.floor(row.time);
  if (typeof row?.timestamp === "number" && Number.isFinite(row.timestamp)) return Math.floor(row.timestamp);
  return null;
}
function n(...values) {
  for (const v of values) {
    const x = Number(v);
    if (Number.isFinite(x)) return x;
  }
  return NaN;
}
function normalizeRows(payload) {
  const rows = pickRows(payload);
  const used = new Set();
  return rows
    .map((r) => ({
      ...r,
      time: toChartTime(r),
      open: n(r.open, r.Open, r.o),
      high: n(r.high, r.High, r.h),
      low: n(r.low, r.Low, r.l),
      close: n(r.close, r.Close, r.c),
      volume: n(r.volume, r.Volume, r.vol, 0),
      ma5: n(r.ma5, r.MA5),
      ma20: n(r.ma20, r.MA20),
      ma60: n(r.ma60, r.MA60),
      bb_upper: n(r.bb_upper, r.BB_UPPER),
      bb_lower: n(r.bb_lower, r.BB_LOWER),
      rsi14: n(r.rsi14, r.RSI14),
      macd_hist: n(r.macd_hist, r.MACD_HIST),
      macd: n(r.macd, r.MACD),
    }))
    .filter((r) => r.time && [r.open, r.high, r.low, r.close].every(Number.isFinite))
    .sort((a, b) => String(a.time).localeCompare(String(b.time)))
    .filter((r) => { if (used.has(r.time)) return false; used.add(r.time); return true; });
}
function line(rows, key) {
  return rows.filter((r) => Number.isFinite(Number(r[key]))).map((r) => ({ time: r.time, value: Number(r[key]) }));
}
function volumeRows(rows) {
  return rows.map((r) => ({ time: r.time, value: Number(r.volume || 0), color: r.close >= r.open ? "rgba(34,197,94,.55)" : "rgba(239,68,68,.55)" }));
}
function addSeries(chart, Type, options, fallback) {
  return typeof chart.addSeries === "function" && Type ? chart.addSeries(Type, options) : chart[fallback](options);
}
function Card({ title, children }) {
  return <section style={cardStyle}><h3 style={{ marginTop: 0 }}>{title}</h3>{children}</section>;
}
function Row({ label, value }) {
  return <div style={{ display: "flex", justifyContent: "space-between", gap: 12, borderBottom: "1px solid rgba(148,163,184,.16)", padding: "6px 0" }}><span style={{ color: "#94a3b8" }}>{label}</span><b>{value}</b></div>;
}
function levelColor(level) {
  if (["strong_bullish", "bullish"].includes(level)) return "#22c55e";
  if (["bearish", "warning"].includes(level)) return "#f59e0b";
  if (["strong_bearish"].includes(level)) return "#ef4444";
  return "#94a3b8";
}
function MiniCard({ card }) {
  return <div style={{ border: `1px solid ${levelColor(card?.level)}66`, background: "#020617", borderRadius: 12, padding: 12 }}>
    <div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}>
      <b>{card?.title || card?.category || "Card"}</b>
      <span style={{ color: levelColor(card?.level), fontWeight: 800 }}>{card?.status || card?.level || "--"}</span>
    </div>
    <p style={{ color: "#cbd5e1", margin: "8px 0 0", lineHeight: 1.45 }}>{card?.meaning || "--"}</p>
    {card?.logic && <div style={{ color: "#64748b", fontSize: 12, marginTop: 8 }}>{card.logic}</div>}
  </div>;
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
  const [chip, setChip] = useState(null);
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
    return () => { window.removeEventListener("resize", resize); charts.current.forEach((c) => c.remove()); };
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
      setStatus(`讀取 ${code} K線中... API=${API}`);
      setRows([]);
      setPayload(null);
      setAnalysis(null);
      setChip(null);
      try {
        const kUrl = `${API}/api/kline/${code}`;
        const kRes = await fetch(kUrl, { cache: "no-store" });
        const kJson = await kRes.json();
        if (!kRes.ok) throw new Error(`${kJson?.detail || kJson?.error || `HTTP ${kRes.status}`} @ ${kUrl}`);
        const nextRows = normalizeRows(kJson);
        if (!alive) return;
        setPayload(kJson);
        setRows(nextRows);
        const rawCount = pickRows(kJson).length;
        setStatus(nextRows.length ? `已載入 ${nextRows.length} 筆K線資料（raw:${rawCount} / ${APP_VERSION}）` : `API回傳0筆可畫K線（raw:${rawCount} / status:${kJson?.status || "--"} / ${APP_VERSION}）`);
        fetch(`${API}/api/analysis/${code}`, { cache: "no-store" })
          .then((r) => r.ok ? r.json() : null)
          .then((j) => { if (alive) setAnalysis(j); })
          .catch(() => {});
        fetch(`${API}/api/chip/${code}?auto_init=false`, { cache: "no-store" })
          .then((r) => r.ok ? r.json() : null)
          .then((j) => { if (alive) setChip(j); })
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
  const rawRows = pickRows(payload).length;
  const pc = Number(meta.change || 0) >= 0 ? "#22c55e" : "#ef4444";
  const perspectiveCards = Array.isArray(analysis?.perspective_cards) ? analysis.perspective_cards : [];
  const chipPerspective = perspectiveCards.find((x) => x.category === "chip");
  const chipAnalysis = chip?.analysis || {};
  const chipLatest = chip?.latest_chip || {};
  const chipMetrics = chipAnalysis.metrics || {};

  return <div style={{ minHeight: "100vh", background: "#020617", color: "white", fontFamily: "Arial, sans-serif" }}>
    <header style={{ padding: 24, borderBottom: "1px solid #1e293b", background: "#0f172a" }}>
      <div style={{ color: "#38bdf8", letterSpacing: 1, fontWeight: 800 }}>TW STOCK DECISION SYSTEM {APP_VERSION}</div>
      <div style={{ color: "#64748b", fontSize: 12, marginTop: 4 }}>Build: {BUILD_LABEL} | Commit: {COMMIT_LABEL} | API: {API}</div>
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
        <Card title="資料狀態"><Row label="版本" value={APP_VERSION} /><Row label="API" value={API} /><Row label="Raw筆數" value={rawRows} /><Row label="K線筆數" value={rows.length} /><Row label="API狀態" value={payload?.status || "--"} /><Row label="資料日期" value={meta.data_date || latest.date || "--"} /></Card>
        <Card title="Decision Score"><div style={{ fontSize: 46, color: Number(analysis?.score || 0) <= -15 ? "#ef4444" : "#22c55e", fontWeight: 900 }}>{analysis?.score ?? "--"}</div><b>{analysis?.trend || "等待分析"}</b><p style={{ color: "#94a3b8" }}>{analysis?.summary || "K線資料載入後會顯示分析結果。"}</p></Card>
        <Card title="籌碼卡片">
          {chipPerspective ? <MiniCard card={chipPerspective} /> : <p style={{ color: "#94a3b8" }}>尚未取得籌碼觀點卡。</p>}
          <div style={{ marginTop: 12 }}>
            <Row label="籌碼分數" value={chipAnalysis.score ?? "--"} />
            <Row label="籌碼狀態" value={chipAnalysis.status || "--"} />
            <Row label="資料日期" value={chipLatest.date || chipLatest.chip_date || chipLatest.margin_date || "--"} />
            <Row label="來源" value={chipLatest.source || chipLatest.source_t86 || chipLatest.source_margin || chipLatest._collection || "--"} />
            <Row label="筆數" value={chip?.row_count ?? "--"} />
          </div>
          {Array.isArray(chipAnalysis.reasons) && chipAnalysis.reasons.length > 0 && <div style={{ marginTop: 10, color: "#cbd5e1", fontSize: 13 }}>
            {chipAnalysis.reasons.slice(0, 3).map((reason, index) => <div key={index} style={{ padding: "3px 0" }}>• {reason}</div>)}
          </div>}
        </Card>
        <Card title="法人 / 信用">
          <Row label="外資近5日" value={fmt(chipMetrics.foreign_5d_sum, 0)} />
          <Row label="投信近5日" value={fmt(chipMetrics.investment_trust_5d_sum, 0)} />
          <Row label="自營商近5日" value={fmt(chipMetrics.dealer_5d_sum, 0)} />
          <Row label="融資餘額" value={fmt(chipMetrics.margin_balance ?? chipLatest.margin_balance, 0)} />
          <Row label="融券餘額" value={fmt(chipMetrics.short_balance ?? chipLatest.short_balance, 0)} />
          <Row label="券資比" value={`${fmt(chipMetrics.short_margin_ratio, 2)}%`} />
        </Card>
        {perspectiveCards.length > 0 && <Card title="五面向觀點">
          <div style={{ display: "grid", gap: 10 }}>
            {perspectiveCards.filter((x) => x.category !== "chip").map((card, index) => <MiniCard key={`${card.category}-${index}`} card={card} />)}
          </div>
        </Card>}
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
