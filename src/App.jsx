import { useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries } = LightweightCharts;
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-api-ihun.onrender.com";

function buildDemoKline() {
  const now = Math.floor(Date.now() / 1000);
  let price = 600;
  return Array.from({ length: 90 }, (_, i) => {
    const open = price + (Math.random() - 0.5) * 10;
    const close = open + (Math.random() - 0.5) * 18;
    const high = Math.max(open, close) + Math.random() * 10;
    const low = Math.min(open, close) - Math.random() * 10;
    price = close;
    return { time: now - (90 - i) * 86400, open: Number(open.toFixed(2)), high: Number(high.toFixed(2)), low: Number(low.toFixed(2)), close: Number(close.toFixed(2)) };
  });
}

function normalizeKline(kline) {
  if (Array.isArray(kline) && kline.length > 0) return kline;
  return buildDemoKline();
}

function buildAnalysisFromKline(symbol, kline, note = "後端分析資料不完整，已由前端依照 K 線強制產生決策分析。") {
  const data = normalizeKline(kline);
  const first = data[0]?.close ?? 0;
  const last = data[data.length - 1]?.close ?? first;
  const recent = data.slice(-20);
  const ma5 = data.slice(-5).reduce((s, x) => s + x.close, 0) / Math.max(1, data.slice(-5).length);
  const ma20 = recent.reduce((s, x) => s + x.close, 0) / Math.max(1, recent.length);
  const changePct = first ? ((last - first) / first) * 100 : 0;
  const score = Math.round((last > ma5 ? 12 : -8) + (ma5 > ma20 ? 18 : -12) + Math.max(-20, Math.min(20, changePct)));
  const trend = score >= 25 ? "強勢偏多" : score >= 10 ? "偏多" : score <= -20 ? "偏空" : "盤整觀望";
  const signals = [
    { level: ma5 > ma20 ? "bullish" : "bearish", title: ma5 > ma20 ? "短均線站上月線" : "短均線跌破月線", message: `MA5 ${ma5 > ma20 ? "高於" : "低於"} MA20，短線動能${ma5 > ma20 ? "偏強" : "偏弱"}。`, score: ma5 > ma20 ? 18 : -12, category: "trend" },
    { level: last > ma5 ? "bullish" : "warning", title: last > ma5 ? "收盤站上短均線" : "收盤低於短均線", message: `最新收盤 ${last.toFixed(2)}，MA5 約 ${ma5.toFixed(2)}。`, score: last > ma5 ? 12 : -8, category: "price" },
    { level: changePct >= 0 ? "bullish" : "bearish", title: "區間漲跌幅", message: `目前區間漲跌幅約 ${changePct.toFixed(2)}%。`, score: Math.round(Math.max(-20, Math.min(20, changePct))), category: "momentum" },
  ];
  return {
    stock: symbol,
    trend,
    score,
    rating: score >= 25 ? "Bullish" : score <= -20 ? "Bearish" : "Neutral",
    summary: `${symbol} 目前由前端決策引擎判定為「${trend}」，綜合分數 ${score}。${note}`,
    indicators: { close: last, ma5, ma20, change_pct: changePct },
    missing_data: [note],
    signals,
    source: "frontend-force-analysis",
  };
}

function normalizeAnalysis(symbol, raw, kline) {
  if (!raw || typeof raw !== "object") return buildAnalysisFromKline(symbol, kline, "後端未回傳有效分析物件，已由前端強制產生。 ");
  const hasSignals = Array.isArray(raw.signals) && raw.signals.length > 0;
  const hasScore = typeof raw.score === "number" && Number.isFinite(raw.score);
  if (hasSignals && hasScore && raw.trend && raw.summary) return raw;
  const forced = buildAnalysisFromKline(symbol, kline, "後端分析資料不完整，已由前端補足決策分數與訊號。 ");
  return {
    ...forced,
    source: raw.source || forced.source,
    missing_data: [...(raw.missing_data || []), ...(forced.missing_data || [])],
  };
}

function demoAnalysis(symbol, message = "API 暫時無法連線，系統目前使用示範 K 線。") {
  return buildAnalysisFromKline(symbol, buildDemoKline(), message);
}

function scoreColor(score) {
  if (score >= 20) return "#16a34a";
  if (score <= -15) return "#dc2626";
  return "#f59e0b";
}

function signalColor(score) {
  if (score > 0) return "#064e3b";
  if (score < 0) return "#7f1d1d";
  return "#334155";
}

export default function App() {
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const candleRef = useRef(null);
  const [input, setInput] = useState("2330");
  const [symbol, setSymbol] = useState("2330");
  const [analysis, setAnalysis] = useState(null);
  const [status, setStatus] = useState("初始化中");

  const apiReady = useMemo(() => API_BASE_URL && API_BASE_URL.startsWith("https://"), []);

  useEffect(() => {
    try {
      if (!chartContainerRef.current) return;
      const chart = createChart(chartContainerRef.current, {
        width: chartContainerRef.current.clientWidth || 900,
        height: 430,
        layout: { background: { color: "#0f172a" }, textColor: "#dbeafe" },
        grid: { vertLines: { color: "#1e293b" }, horzLines: { color: "#1e293b" } },
        rightPriceScale: { borderColor: "#334155" },
        timeScale: { borderColor: "#334155", timeVisible: true },
      });
      const options = { upColor: "#22c55e", downColor: "#ef4444", borderUpColor: "#22c55e", borderDownColor: "#ef4444", wickUpColor: "#22c55e", wickDownColor: "#ef4444" };
      const candle = typeof chart.addSeries === "function" && CandlestickSeries ? chart.addSeries(CandlestickSeries, options) : chart.addCandlestickSeries(options);
      chartRef.current = chart;
      candleRef.current = candle;
      const resize = () => chart.applyOptions({ width: chartContainerRef.current?.clientWidth || 900 });
      window.addEventListener("resize", resize);
      return () => { window.removeEventListener("resize", resize); chart.remove(); };
    } catch (error) {
      setStatus(`圖表初始化失敗：${error.message}`);
      setAnalysis(demoAnalysis(symbol, `圖表初始化失敗：${error.message}`));
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setStatus("讀取資料中...");
      let kline = [];
      let rawAnalysis = null;
      const warnings = [];
      try {
        if (apiReady) {
          const [klineRes, analysisRes] = await Promise.allSettled([
            fetch(`${API_BASE_URL}/api/kline/${symbol}`),
            fetch(`${API_BASE_URL}/api/analysis/${symbol}`),
          ]);

          if (klineRes.status === "fulfilled" && klineRes.value.ok) {
            kline = await klineRes.value.json();
          } else {
            warnings.push("K線 API 回應異常，已使用前端備援 K 線。");
          }

          if (analysisRes.status === "fulfilled" && analysisRes.value.ok) {
            rawAnalysis = await analysisRes.value.json();
          } else {
            warnings.push("分析 API 回應異常，已由前端強制產生分析。");
          }
        } else {
          warnings.push("尚未設定後端 API，使用前端備援資料。");
        }

        kline = normalizeKline(kline);
        const nextAnalysis = normalizeAnalysis(symbol, rawAnalysis, kline);
        if (warnings.length) nextAnalysis.missing_data = [...(nextAnalysis.missing_data || []), ...warnings];

        if (cancelled) return;
        candleRef.current?.setData(kline);
        chartRef.current?.timeScale().fitContent();
        setAnalysis(nextAnalysis);
        setStatus(apiReady ? `已連接 API，資料已完成正規化：${API_BASE_URL}` : "示範模式：尚未設定後端 API");
      } catch (error) {
        if (cancelled) return;
        kline = buildDemoKline();
        candleRef.current?.setData(kline);
        setAnalysis(buildAnalysisFromKline(symbol, kline, `API 連線失敗，已由前端強制產生分析：${error.message}`));
        setStatus(`API 連線失敗，已由前端強制產生分析：${error.message}`);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [symbol, apiReady]);

  return (
    <div style={{ minHeight: "100vh", background: "#020617", color: "white", fontFamily: "Arial, sans-serif" }}>
      <header style={{ padding: "22px 28px", borderBottom: "1px solid #1e293b", background: "#0f172a" }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "center", flexWrap: "wrap" }}>
          <div><div style={{ color: "#38bdf8", fontSize: 13, letterSpacing: 1 }}>TW STOCK DECISION SYSTEM</div><h1 style={{ margin: "6px 0 0", fontSize: 28 }}>交易決策系統</h1></div>
          <div style={{ color: apiReady ? "#22c55e" : "#f59e0b" }}>{status}</div>
        </div>
        <div style={{ marginTop: 18, display: "flex", gap: 10, flexWrap: "wrap" }}>
          <input value={input} onChange={(e) => setInput(e.target.value)} onKeyDown={(e) => e.key === "Enter" && setSymbol(input.trim() || "2330")} placeholder="輸入股票代號，例如 2330" style={{ padding: "12px 14px", borderRadius: 10, border: "1px solid #334155", background: "#020617", color: "white", minWidth: 260 }} />
          <button onClick={() => setSymbol(input.trim() || "2330")} style={{ padding: "12px 18px", borderRadius: 10, border: 0, background: "#2563eb", color: "white", fontWeight: 700 }}>查詢分析</button>
        </div>
      </header>
      <main style={{ padding: 24, display: "grid", gridTemplateColumns: "minmax(0, 2fr) minmax(320px, 0.9fr)", gap: 18 }}>
        <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}><h2 style={{ margin: 0 }}>{symbol} K 線決策圖</h2><span style={{ color: "#94a3b8" }}>可拖曳 / 縮放</span></div>
          <div ref={chartContainerRef} style={{ width: "100%" }} />
        </section>
        <aside style={{ display: "grid", gap: 14 }}>
          <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 }}>
            <div style={{ color: "#94a3b8", fontSize: 13 }}>Decision Score</div>
            <div style={{ display: "flex", alignItems: "center", gap: 14, marginTop: 8 }}><div style={{ fontSize: 44, fontWeight: 800, color: scoreColor(analysis?.score ?? 0) }}>{analysis?.score ?? "--"}</div><div><div style={{ fontSize: 22, fontWeight: 700 }}>{analysis?.trend ?? "分析中"}</div><div style={{ color: "#94a3b8" }}>{analysis?.rating ?? ""}</div></div></div>
          </section>
          <section style={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 }}><h3 style={{ marginTop: 0 }}>決策摘要</h3><p style={{ lineHeight: 1.7, color: "#cbd5e1" }}>{analysis?.summary ?? "正在建立分析..."}</p></section>
        </aside>
        <section style={{ gridColumn: "1 / -1", background: "#0f172a", border: "1px solid #1e293b", borderRadius: 18, padding: 18 }}>
          <h3 style={{ marginTop: 0 }}>決策訊號</h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 12 }}>
            {(analysis?.signals || []).map((s, i) => (<div key={i} style={{ background: signalColor(s.score), border: "1px solid rgba(255,255,255,.08)", borderRadius: 14, padding: 14 }}><div style={{ display: "flex", justifyContent: "space-between", gap: 10 }}><b>{s.title}</b><span>{s.score > 0 ? "+" : ""}{s.score}</span></div><p style={{ color: "#e2e8f0", lineHeight: 1.6, marginBottom: 0 }}>{s.message}</p></div>))}
          </div>
        </section>
        {analysis?.missing_data?.length > 0 && (<section style={{ gridColumn: "1 / -1", background: "#422006", border: "1px solid #92400e", borderRadius: 18, padding: 18 }}><h3 style={{ marginTop: 0 }}>資料限制 / 待串接</h3><ul>{analysis.missing_data.map((m, i) => <li key={i}>{m}</li>)}</ul></section>)}
      </main>
    </div>
  );
}
