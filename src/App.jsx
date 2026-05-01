import { useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries, LineSeries, HistogramSeries } = LightweightCharts;
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-api-ihun.onrender.com";

function buildDemoKline() {
  const now = Math.floor(Date.now() / 1000);
  let price = 80;
  return Array.from({ length: 160 }, (_, i) => {
    const open = price + (Math.random() - 0.5) * 1.8;
    const close = open + (Math.random() - 0.5) * 3.2;
    const high = Math.max(open, close) + Math.random() * 1.8;
    const low = Math.min(open, close) - Math.random() * 1.8;
    const volume = Math.round(3000 + Math.random() * 70000);
    price = close;
    return { time: now - (160 - i) * 86400, open: +open.toFixed(2), high: +high.toFixed(2), low: +low.toFixed(2), close: +close.toFixed(2), volume };
  });
}

function addLine(chart, options) { return typeof chart.addSeries === "function" && LineSeries ? chart.addSeries(LineSeries, options) : chart.addLineSeries(options); }
function addHistogram(chart, options) { return typeof chart.addSeries === "function" && HistogramSeries ? chart.addSeries(HistogramSeries, options) : chart.addHistogramSeries(options); }
function addCandles(chart, options) { return typeof chart.addSeries === "function" && CandlestickSeries ? chart.addSeries(CandlestickSeries, options) : chart.addCandlestickSeries(options); }

function normalizeKline(payload) {
  const raw = Array.isArray(payload) ? payload : payload?.data;
  if (!Array.isArray(raw) || raw.length === 0) return buildDemoKline();
  return raw.filter(x => x && x.time && x.open != null && x.high != null && x.low != null && x.close != null);
}
function valueLine(data, key) { return data.filter(x => x[key] != null).map(x => ({ time: x.time, value: Number(x[key]) })); }
function volumeBars(data) { return data.filter(x => x.volume != null).map(x => ({ time: x.time, value: Number(x.volume), color: x.close >= x.open ? "rgba(34,197,94,.55)" : "rgba(239,68,68,.55)" })); }
function macdBars(data) { return data.filter(x => x.macd_hist != null).map(x => ({ time: x.time, value: Number(x.macd_hist), color: x.macd_hist >= 0 ? "rgba(34,197,94,.65)" : "rgba(239,68,68,.65)" })); }
function fmt(v, digits = 2) { return v == null || Number.isNaN(Number(v)) ? "--" : Number(v).toFixed(digits); }
function scoreColor(score) { return score >= 20 ? "#22c55e" : score <= -15 ? "#ef4444" : "#f59e0b"; }
function signalColor(score) { return score > 0 ? "#064e3b" : score < 0 ? "#7f1d1d" : "#334155"; }
function Card({ title, children }) { return <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 16, padding: 16 }}><h3 style={{ margin: "0 0 12px", color: "#e2e8f0" }}>{title}</h3>{children}</section>; }
function Row({ label, value, color }) { return <div style={{ display: "flex", justifyContent: "space-between", gap: 12, padding: "5px 0", borderBottom: "1px solid rgba(148,163,184,.12)" }}><span style={{ color: "#94a3b8" }}>{label}</span><b style={{ color: color || "#e2e8f0" }}>{value ?? "--"}</b></div>; }

function buildAnalysisFromKline(symbol, kline, note = "後端分析資料不完整，已由前端依照 K 線強制產生決策分析。") {
  const data = normalizeKline(kline);
  const first = data[0]?.close ?? 0;
  const last = data[data.length - 1]?.close ?? first;
  const recent = data.slice(-20);
  const ma5 = data.slice(-5).reduce((s, x) => s + x.close, 0) / Math.max(1, data.slice(-5).length);
  const ma20 = recent.reduce((s, x) => s + x.close, 0) / Math.max(1, recent.length);
  const rsi = data[data.length - 1]?.rsi14;
  const macd = data[data.length - 1]?.macd;
  const macdSignal = data[data.length - 1]?.macd_signal;
  const changePct = first ? ((last - first) / first) * 100 : 0;
  const score = Math.round((last > ma5 ? 12 : -8) + (ma5 > ma20 ? 18 : -12) + Math.max(-20, Math.min(20, changePct)));
  const trend = score >= 25 ? "強勢偏多" : score >= 10 ? "偏多" : score <= -20 ? "偏空" : "盤整觀望";
  return {
    stock: symbol, trend, score, rating: score >= 25 ? "Bullish" : score <= -20 ? "Bearish" : "Neutral",
    summary: `${symbol} 目前由前端決策引擎判定為「${trend}」，綜合分數 ${score}。${note}`,
    indicators: { close: last, ma5, ma20, rsi14: rsi, macd, macd_signal: macdSignal, change_pct: changePct },
    missing_data: [note],
    signals: [
      { title: ma5 > ma20 ? "短均線站上月線" : "短均線跌破月線", message: `MA5 ${ma5 > ma20 ? "高於" : "低於"} MA20，短線動能${ma5 > ma20 ? "偏強" : "偏弱"}。`, score: ma5 > ma20 ? 18 : -12 },
      { title: last > ma5 ? "收盤站上短均線" : "收盤低於短均線", message: `最新收盤 ${last.toFixed(2)}，MA5 約 ${ma5.toFixed(2)}。`, score: last > ma5 ? 12 : -8 },
      { title: "區間漲跌幅", message: `目前區間漲跌幅約 ${changePct.toFixed(2)}%。`, score: Math.round(Math.max(-20, Math.min(20, changePct))) },
    ],
  };
}
function normalizeAnalysis(symbol, raw, kline) {
  if (!raw || typeof raw !== "object") return buildAnalysisFromKline(symbol, kline, "後端未回傳有效分析物件，已由前端強制產生。 ");
  if (Array.isArray(raw.signals) && raw.signals.length > 0 && typeof raw.score === "number" && raw.trend && raw.summary) return raw;
  const forced = buildAnalysisFromKline(symbol, kline, "後端分析資料不完整，已由前端補足決策分數與訊號。 ");
  return { ...forced, meta: raw.meta || forced.meta, source: raw.source || forced.source, missing_data: [...(raw.missing_data || []), ...(forced.missing_data || [])] };
}

export default function App() {
  const priceRef = useRef(null), volumeRef = useRef(null), rsiRef = useRef(null), macdRef = useRef(null);
  const chartsRef = useRef([]), seriesRef = useRef({});
  const [input, setInput] = useState("2330");
  const [symbol, setSymbol] = useState("2330");
  const [analysis, setAnalysis] = useState(null);
  const [dashboard, setDashboard] = useState(null);
  const [status, setStatus] = useState("初始化中");
  const [showMA, setShowMA] = useState(true);
  const [showBB, setShowBB] = useState(true);
  const apiReady = useMemo(() => API_BASE_URL?.startsWith("https://"), []);

  useEffect(() => {
    const chartBase = (el, height) => createChart(el, { width: el.clientWidth || 900, height, layout: { background: { color: "#0f172a" }, textColor: "#dbeafe" }, grid: { vertLines: { color: "#1e293b" }, horzLines: { color: "#1e293b" } }, rightPriceScale: { borderColor: "#334155" }, timeScale: { borderColor: "#334155", timeVisible: true } });
    const priceChart = chartBase(priceRef.current, 420), volumeChart = chartBase(volumeRef.current, 150), rsiChart = chartBase(rsiRef.current, 150), macdChart = chartBase(macdRef.current, 170);
    chartsRef.current = [priceChart, volumeChart, rsiChart, macdChart];
    seriesRef.current = {
      candle: addCandles(priceChart, { upColor: "#22c55e", downColor: "#ef4444", borderUpColor: "#22c55e", borderDownColor: "#ef4444", wickUpColor: "#22c55e", wickDownColor: "#ef4444" }),
      ma5: addLine(priceChart, { color: "#facc15", lineWidth: 1, title: "MA5" }), ma20: addLine(priceChart, { color: "#38bdf8", lineWidth: 1, title: "MA20" }), ma60: addLine(priceChart, { color: "#a78bfa", lineWidth: 1, title: "MA60" }),
      bbUpper: addLine(priceChart, { color: "rgba(148,163,184,.75)", lineWidth: 1, title: "BB Upper" }), bbMid: addLine(priceChart, { color: "rgba(148,163,184,.45)", lineWidth: 1, title: "BB Mid" }), bbLower: addLine(priceChart, { color: "rgba(148,163,184,.75)", lineWidth: 1, title: "BB Lower" }),
      volume: addHistogram(volumeChart, { priceFormat: { type: "volume" }, priceScaleId: "" }), rsi: addLine(rsiChart, { color: "#f59e0b", lineWidth: 2, title: "RSI14" }), macd: addHistogram(macdChart, { priceScaleId: "" }), macdLine: addLine(macdChart, { color: "#38bdf8", lineWidth: 1, title: "MACD" }), signalLine: addLine(macdChart, { color: "#f97316", lineWidth: 1, title: "Signal" })
    };
    const resize = () => chartsRef.current.forEach(c => c.applyOptions({ width: priceRef.current?.clientWidth || 900 }));
    window.addEventListener("resize", resize);
    return () => { window.removeEventListener("resize", resize); chartsRef.current.forEach(c => c.remove()); };
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setStatus("讀取資料中...");
      let kline = [], rawAnalysis = null, rawDashboard = null, warnings = [];
      try {
        if (apiReady) {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 6500);
          const [klineRes, analysisRes, dashboardRes] = await Promise.allSettled([
            fetch(`${API_BASE_URL}/api/kline/${symbol}`, { signal: controller.signal }),
            fetch(`${API_BASE_URL}/api/analysis/${symbol}`, { signal: controller.signal }),
            fetch(`${API_BASE_URL}/api/dashboard/${symbol}`, { signal: controller.signal }),
          ]);
          clearTimeout(timeout);
          if (klineRes.status === "fulfilled" && klineRes.value.ok) kline = await klineRes.value.json(); else warnings.push("K線 API 回應異常，已使用前端備援 K 線。");
          if (analysisRes.status === "fulfilled" && analysisRes.value.ok) rawAnalysis = await analysisRes.value.json(); else warnings.push("分析 API 回應異常，已由前端強制產生分析。");
          if (dashboardRes.status === "fulfilled" && dashboardRes.value.ok) rawDashboard = await dashboardRes.value.json(); else warnings.push("Dashboard API 尚未完成或回應逾時，卡片以既有技術分析顯示。");
        }
        kline = normalizeKline(kline);
        const nextAnalysis = normalizeAnalysis(symbol, rawAnalysis, kline);
        if (warnings.length) nextAnalysis.missing_data = [...(nextAnalysis.missing_data || []), ...warnings];
        if (cancelled) return;
        const s = seriesRef.current;
        s.candle?.setData(kline); s.volume?.setData(volumeBars(kline)); s.rsi?.setData(valueLine(kline, "rsi14")); s.macd?.setData(macdBars(kline)); s.macdLine?.setData(valueLine(kline, "macd")); s.signalLine?.setData(valueLine(kline, "macd_signal"));
        s.ma5?.setData(showMA ? valueLine(kline, "ma5") : []); s.ma20?.setData(showMA ? valueLine(kline, "ma20") : []); s.ma60?.setData(showMA ? valueLine(kline, "ma60") : []);
        s.bbUpper?.setData(showBB ? valueLine(kline, "bb_upper") : []); s.bbMid?.setData(showBB ? valueLine(kline, "bb_mid") : []); s.bbLower?.setData(showBB ? valueLine(kline, "bb_lower") : []);
        chartsRef.current.forEach(c => c.timeScale().fitContent());
        setAnalysis(nextAnalysis); setDashboard(rawDashboard);
        setStatus(`已連接 API，技術分析與券商卡片已載入`);
      } catch (error) {
        if (cancelled) return;
        kline = buildDemoKline(); seriesRef.current.candle?.setData(kline);
        setAnalysis(buildAnalysisFromKline(symbol, kline, `API 連線失敗，已由前端強制產生分析：${error.message}`));
        setDashboard(null); setStatus(`API 連線失敗：${error.message}`);
      }
    }
    load(); return () => { cancelled = true; };
  }, [symbol, apiReady, showMA, showBB]);

  const meta = dashboard?.basic || analysis?.meta || {};
  const latest = analysis?.indicators || {};
  const tech = dashboard?.dashboard?.technical || {};
  const chip = dashboard?.dashboard?.chip || {};
  const scenario = dashboard?.dashboard?.scenario || {};
  const order = dashboard?.basic || {};
  const priceColor = (meta.change ?? 0) >= 0 ? "#22c55e" : "#ef4444";

  return <div style={{ minHeight: "100vh", background: "#020617", color: "white", fontFamily: "Arial, sans-serif" }}>
    <header style={{ padding: "22px 28px", borderBottom: "1px solid #1e293b", background: "#0f172a" }}>
      <div style={{ color: "#38bdf8", fontSize: 13, letterSpacing: 1 }}>TW STOCK DECISION SYSTEM</div>
      <h1 style={{ margin: "6px 0 12px", fontSize: 28 }}>券商等級交易決策 Dashboard</h1>
      <div style={{ display: "grid", gridTemplateColumns: "minmax(220px, 1fr) auto", gap: 18, alignItems: "end" }}>
        <div><div style={{ fontSize: 24, fontWeight: 800 }}>{meta.name || symbol} <span style={{ color: "#facc15" }}>{meta.code || symbol}</span> <span style={{ fontSize: 14, color: "#94a3b8" }}>{meta.market || "--"}・{meta.industry || "--"}</span></div><div style={{ fontSize: 44, fontWeight: 900, color: priceColor }}>{fmt(meta.price)}</div><div style={{ color: priceColor }}>{fmt(meta.change)} ({fmt(meta.change_pct)}%)　開 {fmt(meta.open)}　高 {fmt(meta.high)}　低 {fmt(meta.low)}　收 {fmt(meta.close)}</div></div>
        <div style={{ color: "#22c55e", fontSize: 14 }}>{status}</div>
      </div>
      <div style={{ marginTop: 18, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}><input value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === "Enter" && setSymbol(input.trim() || "2330")} placeholder="輸入股票代號或名稱，例如 大聯大 / 3702" style={{ padding: "12px 14px", borderRadius: 10, border: "1px solid #334155", background: "#020617", color: "white", minWidth: 280 }} /><button onClick={() => setSymbol(input.trim() || "2330")} style={{ padding: "12px 18px", borderRadius: 10, border: 0, background: "#2563eb", color: "white", fontWeight: 700 }}>查詢分析</button><label><input type="checkbox" checked={showMA} onChange={e => setShowMA(e.target.checked)} /> MA</label><label><input type="checkbox" checked={showBB} onChange={e => setShowBB(e.target.checked)} /> 布林</label></div>
    </header>
    <main style={{ padding: 18, display: "grid", gridTemplateColumns: "minmax(0, 2fr) minmax(360px, .9fr)", gap: 18 }}>
      <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 }}><h2 style={{ marginTop: 0 }}>{meta.code || symbol} K線 + 均線 + 布林</h2><div ref={priceRef} style={{ width: "100%" }} /><h3>成交量 Volume</h3><div ref={volumeRef} style={{ width: "100%" }} /><h3>RSI14</h3><div ref={rsiRef} style={{ width: "100%" }} /><h3>MACD</h3><div ref={macdRef} style={{ width: "100%" }} /></section>
      <aside style={{ display: "grid", gap: 12, alignContent: "start" }}>
        <Card title="Decision Score"><div style={{ display: "flex", alignItems: "center", gap: 14 }}><div style={{ fontSize: 46, fontWeight: 900, color: scoreColor(analysis?.score ?? 0) }}>{analysis?.score ?? "--"}</div><div><div style={{ fontSize: 24, fontWeight: 800 }}>{analysis?.trend ?? "分析中"}</div><div style={{ color: "#94a3b8" }}>{analysis?.rating ?? ""}</div></div></div></Card>
        <Card title="原本技術分析總覽"><Row label="MA5" value={fmt(latest.ma5)} color="#facc15" /><Row label="MA20" value={fmt(latest.ma20)} color="#38bdf8" /><Row label="RSI14" value={fmt(latest.rsi14)} /><Row label="MACD" value={fmt(latest.macd)} /><Row label="布林上軌" value={fmt(latest.bb_upper)} /><Row label="布林下軌" value={fmt(latest.bb_lower)} /><Row label="技術趨勢" value={tech.trend_direction || analysis?.trend} /></Card>
        <Card title="籌碼分析"><Row label="外資" value={chip.foreign ?? "待資料"} /><Row label="投信" value={chip.investment_trust ?? "待資料"} /><Row label="自營商" value={chip.dealer ?? "待資料"} /><Row label="融資餘額" value={chip.margin_balance ?? "待資料"} /><Row label="融券餘額" value={chip.short_balance ?? "待資料"} /><Row label="籌碼分數" value={chip.chip_score ?? "--"} /></Card>
        <Card title="五檔委買委賣"><div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}><div><b style={{ color: "#22c55e" }}>買</b>{(order.bids || []).map((b, i) => <Row key={i} label={b.qty} value={fmt(b.price)} color="#22c55e" />)}</div><div><b style={{ color: "#ef4444" }}>賣</b>{(order.asks || []).map((a, i) => <Row key={i} label={fmt(a.price)} value={a.qty} color="#ef4444" />)}</div></div></Card>
        <Card title="操作劇本"><Row label="突破" value={scenario.breakout || "資料建立中"} /><Row label="回檔" value={scenario.pullback || "資料建立中"} /><Row label="風險" value={scenario.risk || "資料建立中"} /></Card>
        <Card title="原本決策訊號">{(analysis?.signals || []).map((s, i) => <div key={i} style={{ background: signalColor(s.score), borderRadius: 12, padding: 12, marginBottom: 8 }}><b>{s.title} {s.score > 0 ? "+" : ""}{s.score}</b><p style={{ margin: "6px 0 0", color: "#e2e8f0" }}>{s.message}</p></div>)}</Card>
      </aside>
    </main>
  </div>;
}
