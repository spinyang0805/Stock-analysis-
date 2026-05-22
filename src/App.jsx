import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries, LineSeries, HistogramSeries } = LightweightCharts;
const API = (import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-tw.fly.dev").replace(/\/$/, "");
const APP_VERSION = "v18-realtime";
const POLL_INTERVAL_MS = 10_000;

const STOCKS = [
  { code: "2330", name: "台積電", market: "上市", industry: "半導體" },
  { code: "1402", name: "遠東新", market: "上市", industry: "紡織" },
  { code: "2408", name: "南亞科", market: "上市", industry: "半導體" },
  { code: "3702", name: "大聯大", market: "上市", industry: "電子通路" },
  { code: "2317", name: "鴻海", market: "上市", industry: "電子" },
  { code: "2454", name: "聯發科", market: "上市", industry: "半導體" },
  { code: "2308", name: "台達電", market: "上市", industry: "電子零組件" },
  { code: "2382", name: "廣達", market: "上市", industry: "電腦周邊" },
  { code: "0050", name: "元大台灣50", market: "上市", industry: "ETF" },
  { code: "00981A", name: "主動統一台股增長", market: "上市", industry: "ETF" },
  { code: "00679B", name: "元大美債20年", market: "上市", industry: "債券ETF" },
];

function isTradingSession() {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  const mins = now.getHours() * 60 + now.getMinutes();
  return mins >= 9 * 60 && mins <= 13 * 60 + 30;
}

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
  return found || { code: cleanCode(raw), name: cleanCode(raw), market: "--", industry: "--" };
}
function fmt(value, digits = 2) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: digits }) : "--";
}
function pickRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.rows)) return payload.rows;
  if (Array.isArray(payload?.items)) return payload.items;
  return [];
}
function toChartTime(row) {
  const date = String(row?.date || row?.Date || row?.data_date || "").trim();
  if (/^\d{8}$/.test(date)) return `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(date)) return date;
  if (typeof row?.time === "number" && Number.isFinite(row.time)) return Math.floor(row.time);
  return null;
}
function num(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return NaN;
}
function normalizeRows(payload) {
  const used = new Set();
  return pickRows(payload)
    .map((row) => ({
      ...row,
      time: toChartTime(row),
      open: num(row.open, row.Open, row.o),
      high: num(row.high, row.High, row.h),
      low: num(row.low, row.Low, row.l),
      close: num(row.close, row.Close, row.c),
      volume: num(row.volume, row.Volume, row.vol, 0),
      ma5: num(row.ma5, row.MA5),
      ma20: num(row.ma20, row.MA20),
      ma60: num(row.ma60, row.MA60),
      rsi14: num(row.rsi14, row.RSI14),
      macd_hist: num(row.macd_hist, row.MACD_HIST),
    }))
    .filter((row) => row.time && [row.open, row.high, row.low, row.close].every(Number.isFinite))
    .sort((a, b) => String(a.time).localeCompare(String(b.time)))
    .filter((row) => { if (used.has(row.time)) return false; used.add(row.time); return true; });
}
function addSeries(chart, SeriesType, options, fallback) {
  return typeof chart.addSeries === "function" && SeriesType ? chart.addSeries(SeriesType, options) : chart[fallback](options);
}
function line(rows, key) {
  return rows.filter((row) => Number.isFinite(Number(row[key]))).map((row) => ({ time: row.time, value: Number(row[key]) }));
}
function volumeRows(rows) {
  return rows.map((row) => ({
    time: row.time, value: Number(row.volume || 0),
    color: row.close >= row.open ? "rgba(239,68,68,.55)" : "rgba(34,197,94,.55)",
  }));
}

function Row({ label, value, color }) {
  return (
    <div style={rowStyle}>
      <span style={{ color: "#94a3b8" }}>{label}</span>
      <b style={color ? { color } : undefined}>{value}</b>
    </div>
  );
}
function Card({ title, badge, children }) {
  return (
    <section style={cardStyle}>
      <h3 style={{ marginTop: 0, display: "flex", alignItems: "center", gap: 8 }}>
        {title}
        {badge}
      </h3>
      {children}
    </section>
  );
}

function LiveBadge() {
  return (
    <span style={{ background: "#ef4444", color: "white", borderRadius: 4, padding: "2px 7px", fontSize: 11, fontWeight: 800, letterSpacing: 1, animation: "pulse 1.5s infinite" }}>
      ● LIVE
    </span>
  );
}

function OrderBook({ bids = [], asks = [] }) {
  const rows = Math.max(bids.length, asks.length, 1);
  return (
    <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
      <thead>
        <tr>
          <th style={{ color: "#22c55e", textAlign: "right", paddingRight: 8 }}>委買量</th>
          <th style={{ color: "#22c55e", textAlign: "right", paddingRight: 8 }}>買價</th>
          <th style={{ color: "#ef4444", textAlign: "left", paddingLeft: 8 }}>賣價</th>
          <th style={{ color: "#ef4444", textAlign: "left", paddingLeft: 8 }}>委賣量</th>
        </tr>
      </thead>
      <tbody>
        {Array.from({ length: rows }).map((_, i) => {
          const bid = bids[i] || {};
          const ask = asks[i] || {};
          return (
            <tr key={i} style={{ borderBottom: "1px solid rgba(148,163,184,.1)" }}>
              <td style={{ textAlign: "right", paddingRight: 8, color: "#22c55e", padding: "4px 8px 4px 0" }}>
                {bid.qty != null ? bid.qty.toLocaleString() : ""}
              </td>
              <td style={{ textAlign: "right", paddingRight: 8, color: "#22c55e", fontWeight: 700 }}>
                {bid.price != null ? fmt(bid.price) : ""}
              </td>
              <td style={{ textAlign: "left", paddingLeft: 8, color: "#ef4444", fontWeight: 700 }}>
                {ask.price != null ? fmt(ask.price) : ""}
              </td>
              <td style={{ textAlign: "left", paddingLeft: 8, color: "#ef4444" }}>
                {ask.qty != null ? ask.qty.toLocaleString() : ""}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
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
  const [realtime, setRealtime] = useState(null);
  const [chip, setChip] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [status, setStatus] = useState("尚未查詢");
  const [lastRefresh, setLastRefresh] = useState(null);
  const [isLive, setIsLive] = useState(isTradingSession());
  const suggestions = useMemo(() => findStock(input), [input]);

  // Chart setup
  useEffect(() => {
    if (!priceRef.current || !volRef.current || !rsiRef.current || !macdRef.current) return undefined;
    const makeChart = (el, height) => createChart(el, {
      width: el.clientWidth || 900, height,
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
      candle: addSeries(c1, CandlestickSeries, { upColor: "#ef4444", downColor: "#22c55e", borderUpColor: "#ef4444", borderDownColor: "#22c55e", wickUpColor: "#ef4444", wickDownColor: "#22c55e" }, "addCandlestickSeries"),
      ma5: addSeries(c1, LineSeries, { color: "#facc15", lineWidth: 1 }, "addLineSeries"),
      ma20: addSeries(c1, LineSeries, { color: "#38bdf8", lineWidth: 1 }, "addLineSeries"),
      ma60: addSeries(c1, LineSeries, { color: "#a78bfa", lineWidth: 1 }, "addLineSeries"),
      volume: addSeries(c2, HistogramSeries, { priceFormat: { type: "volume" } }, "addHistogramSeries"),
      rsi: addSeries(c3, LineSeries, { color: "#f59e0b", lineWidth: 2 }, "addLineSeries"),
      macd: addSeries(c4, HistogramSeries, {}, "addHistogramSeries"),
    };
    const resize = () => charts.current.forEach((c) => c.applyOptions({ width: priceRef.current?.clientWidth || 900 }));
    window.addEventListener("resize", resize);
    return () => {
      window.removeEventListener("resize", resize);
      charts.current.forEach((c) => c.remove());
      charts.current = [];
    };
  }, []);

  // Chart data update
  useEffect(() => {
    const s = series.current;
    if (!s.candle) return;
    s.candle.setData(rows.map((r) => ({ time: r.time, open: r.open, high: r.high, low: r.low, close: r.close })));
    s.volume.setData(volumeRows(rows));
    s.rsi.setData(line(rows, "rsi14"));
    s.macd.setData(line(rows, "macd_hist"));
    s.ma5.setData(line(rows, "ma5"));
    s.ma20.setData(line(rows, "ma20"));
    s.ma60.setData(line(rows, "ma60"));
    charts.current.forEach((c) => c.timeScale().fitContent());
  }, [rows]);

  // Fetch kline (called on load + polling)
  const fetchKline = useCallback(async (code, isRefresh = false) => {
    try {
      const res = await fetch(`${API}/api/kline/${encodeURIComponent(code)}`, { cache: "no-store" });
      if (!res.ok) return;
      const json = await res.json();
      const nextRows = normalizeRows(json);
      setPayload(json);
      setRows(nextRows);
      setRealtime(json.realtime || null);
      setLastRefresh(new Date());
      if (!isRefresh) {
        setStatus(nextRows.length ? `已載入 ${nextRows.length} 筆股價資料` : "API 已回應，但沒有股價資料");
      }
    } catch (e) {
      if (!isRefresh) setStatus(`查詢失敗：${e?.message || e}`);
    }
  }, []);

  // Initial full load (kline + chip + analysis)
  useEffect(() => {
    let alive = true;
    const code = stock.code;
    setStatus(`查詢 ${code} 中...`);
    setRows([]);
    setPayload(null);
    setChip(null);
    setAnalysis(null);
    setRealtime(null);

    fetchKline(code);

    Promise.allSettled([
      fetch(`${API}/api/chip/${encodeURIComponent(code)}?auto_init=false`, { cache: "no-store" }),
      fetch(`${API}/api/analysis/${encodeURIComponent(code)}`, { cache: "no-store" }),
    ]).then(([chipRes, analysisRes]) => {
      if (!alive) return;
      if (chipRes.status === "fulfilled" && chipRes.value.ok)
        chipRes.value.json().then((j) => { if (alive) setChip(j); }).catch(() => {});
      if (analysisRes.status === "fulfilled" && analysisRes.value.ok)
        analysisRes.value.json().then((j) => { if (alive) setAnalysis(j); }).catch(() => {});
    });

    return () => { alive = false; };
  }, [stock.code, fetchKline]);

  // Polling during trading hours
  useEffect(() => {
    const tick = () => {
      const live = isTradingSession();
      setIsLive(live);
      if (live) fetchKline(stock.code, true);
    };
    const id = setInterval(tick, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [stock.code, fetchKline]);

  function submit() {
    const target = suggestions[0] || resolveStock(input);
    setStock(target);
    setInput(target.code);
    setOpenSuggest(false);
  }

  const meta = { ...stock, ...(payload?.meta || {}), ...(analysis?.meta || {}) };
  const latest = rows.at(-1) || {};
  const chipLatest = chip?.latest_chip || {};
  const chipAnalysis = chip?.analysis || {};
  const chipMetrics = chipAnalysis.metrics || {};
  const change = Number(meta.change ?? (latest.close - latest.open));
  const priceColor = change >= 0 ? "#ef4444" : "#22c55e";
  const livePrice = realtime?.price ?? meta.price ?? latest.close;
  const bids = realtime?.bids || [];
  const asks = realtime?.asks || [];

  return (
    <div style={pageStyle}>
      <style>{`@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }`}</style>
      <header style={headerStyle}>
        <div style={eyebrowStyle}>TW STOCK DECISION SYSTEM {APP_VERSION}</div>
        <div style={subtleStyle}>API: {API}</div>
        <h1 style={titleStyle}>個股儀表板</h1>
        <div style={stockTitleStyle}>
          <span style={{ color: "#facc15" }}>{meta.code || stock.code}</span>{" "}
          {meta.name || stock.name}
          <span style={metaStyle}>{meta.market || stock.market} / {meta.industry || stock.industry}</span>
          {isLive && <LiveBadge />}
        </div>
        <div style={{ fontSize: 44, fontWeight: 900, color: priceColor }}>{fmt(livePrice)}</div>
        <div style={{ color: priceColor }}>
          漲跌 {fmt(meta.change ?? change)} ({fmt(meta.change_pct, 2)}%)&nbsp;&nbsp;
          開 {fmt(meta.open ?? latest.open)} / 高 {fmt(meta.high ?? latest.high)} / 低 {fmt(meta.low ?? latest.low)}
        </div>
        {isLive && realtime?.time && (
          <div style={{ color: "#94a3b8", fontSize: 13, marginTop: 4 }}>
            即時報價時間 {realtime.time}・每 {POLL_INTERVAL_MS / 1000} 秒更新・成交量 {realtime.volume_lot ? realtime.volume_lot.toLocaleString() : "--"} 張
          </div>
        )}
        {lastRefresh && (
          <div style={{ color: "#475569", fontSize: 12, marginTop: 2 }}>
            最後更新 {lastRefresh.toLocaleTimeString("zh-TW")}
          </div>
        )}
        <div style={toolbarStyle}>
          <div style={{ position: "relative" }}>
            <input
              value={input}
              onFocus={() => setOpenSuggest(true)}
              onChange={(e) => { setInput(e.target.value); setOpenSuggest(true); }}
              onKeyDown={(e) => { if (e.key === "Enter") submit(); }}
              placeholder="輸入股票代號，例如 1402"
              style={inputStyle}
            />
            {openSuggest && suggestions.length > 0 && (
              <div style={suggestStyle}>
                {suggestions.map((item) => (
                  <div key={item.code} onMouseDown={() => { setStock(item); setInput(item.code); setOpenSuggest(false); }} style={suggestItemStyle}>
                    <b style={{ color: "#facc15" }}>{item.code}</b> {item.name}
                    <span style={{ color: "#94a3b8", marginLeft: 8 }}>{item.market} / {item.industry}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
          <button type="button" onClick={submit} style={buttonStyle}>查詢</button>
          <span style={{ color: rows.length ? "#22c55e" : "#f59e0b" }}>{status}</span>
        </div>
      </header>

      <main style={mainStyle}>
        <section style={cardStyle}>
          <h2 style={{ marginTop: 0 }}>{meta.code || stock.code} 股價走勢</h2>
          <div ref={priceRef} />
          <h3>成交量</h3><div ref={volRef} />
          <h3>RSI14</h3><div ref={rsiRef} />
          <h3>MACD</h3><div ref={macdRef} />
        </section>

        <aside style={asideStyle}>
          {isLive && (
            <Card title="即時委買委賣" badge={<LiveBadge />}>
              {bids.length > 0 || asks.length > 0
                ? <OrderBook bids={bids} asks={asks} />
                : <div style={{ color: "#64748b", fontSize: 13 }}>等待揭示資料中...</div>
              }
              <div style={{ color: "#475569", fontSize: 11, marginTop: 8 }}>
                來源：TWSE MIS・每 {POLL_INTERVAL_MS / 1000} 秒更新
              </div>
            </Card>
          )}

          <Card title="資料狀態">
            <Row label="版本" value={APP_VERSION} />
            <Row label="資料來源" value={isLive ? "歷史 + 即時" : "歷史"} color={isLive ? "#22c55e" : undefined} />
            <Row label="股價筆數" value={rows.length} />
            <Row label="股價 API" value={payload?.status || "--"} />
            <Row label="籌碼筆數" value={chip?.row_count ?? "--"} />
            <Row label="三大法人" value={chip?.has_institutional_data === true ? "有" : chip ? "無" : "--"} />
          </Card>

          <Card title="籌碼摘要">
            <Row label="日期" value={chipLatest.date || chipLatest.chip_date || "--"} />
            <Row label="外資買賣超" value={fmt(chipLatest.foreign_buy ?? chipLatest.foreign, 0)} />
            <Row label="投信買賣超" value={fmt(chipLatest.investment_trust_buy ?? chipLatest.investment_trust, 0)} />
            <Row label="自營商買賣超" value={fmt(chipLatest.dealer_buy ?? chipLatest.dealer, 0)} />
            <Row label="融資餘額" value={fmt(chipMetrics.margin_balance ?? chipLatest.margin_balance, 0)} />
            <Row label="融券餘額" value={fmt(chipMetrics.short_balance ?? chipLatest.short_balance, 0)} />
          </Card>

          <Card title="技術指標">
            <Row label="MA5" value={fmt(latest.ma5)} />
            <Row label="MA20" value={fmt(latest.ma20)} />
            <Row label="MA60" value={fmt(latest.ma60)} />
            <Row label="RSI14" value={fmt(latest.rsi14)} />
            <Row label="MACD Hist" value={fmt(latest.macd_hist)} />
          </Card>

          <Card title="決策摘要">
            <div style={{ fontSize: 42, color: Number(analysis?.score || 0) < 0 ? "#22c55e" : "#ef4444", fontWeight: 900 }}>{analysis?.score ?? "--"}</div>
            <b>{analysis?.trend || "尚無分析資料"}</b>
            <p style={{ color: "#94a3b8", lineHeight: 1.5 }}>{analysis?.summary || "股價與籌碼資料會分開載入。"}</p>
          </Card>
        </aside>
      </main>
    </div>
  );
}

const pageStyle = { minHeight: "100vh", background: "#020617", color: "white", fontFamily: "Arial, sans-serif" };
const headerStyle = { padding: 24, borderBottom: "1px solid #1e293b", background: "#0f172a" };
const eyebrowStyle = { color: "#38bdf8", letterSpacing: 1, fontWeight: 800 };
const subtleStyle = { color: "#64748b", fontSize: 12, marginTop: 4 };
const titleStyle = { marginBottom: 8 };
const stockTitleStyle = { fontSize: 26, fontWeight: 800, display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" };
const metaStyle = { color: "#94a3b8", fontSize: 15 };
const toolbarStyle = { marginTop: 18, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" };
const inputStyle = { padding: "12px 14px", borderRadius: 8, border: "1px solid #334155", background: "#020617", color: "white", minWidth: 300 };
const buttonStyle = { padding: "12px 18px", borderRadius: 8, border: 0, background: "#2563eb", color: "white", fontWeight: 700, cursor: "pointer" };
const mainStyle = { padding: 18, display: "grid", gridTemplateColumns: "minmax(0,2fr) minmax(320px,.9fr)", gap: 18 };
const asideStyle = { display: "grid", gap: 12, alignContent: "start" };
const cardStyle = { background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8, padding: 18 };
const rowStyle = { display: "flex", justifyContent: "space-between", gap: 12, borderBottom: "1px solid rgba(148,163,184,.16)", padding: "6px 0" };
const suggestStyle = { position: "absolute", top: 48, left: 0, right: 0, background: "#0f172a", border: "1px solid #334155", borderRadius: 8, zIndex: 10, overflow: "hidden" };
const suggestItemStyle = { padding: "10px 12px", cursor: "pointer", borderBottom: "1px solid rgba(148,163,184,.15)" };
