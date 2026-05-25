import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries, LineSeries, HistogramSeries } = LightweightCharts;
const API = "https://stock-analysis-tw.fly.dev";
const APP_VERSION = "v20-stable";
const POLL_MS = 10_000;

/* ── Stock list ──────────────────────────────────────────────────── */
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

/* ── Helpers ─────────────────────────────────────────────────────── */
function isTradingSession() {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  const m = now.getHours() * 60 + now.getMinutes();
  return m >= 540 && m <= 810;
}
function cleanCode(v) { return String(v || "2330").trim().replace(/\.(TW|TWO)$/i, "").split(/\s+/)[0].toUpperCase(); }
function findStock(q) {
  const s = String(q || "").trim().toLowerCase();
  if (!s) return [];
  return STOCKS.filter(x => x.code.toLowerCase().includes(s) || x.name.toLowerCase().includes(s)).slice(0, 8);
}
function resolveStock(q) {
  const raw = String(q || "2330").trim();
  return STOCKS.find(x => x.code === raw.toUpperCase() || x.name === raw) || { code: cleanCode(raw), name: cleanCode(raw), market: "--", industry: "--" };
}
function fmt(v, d = 2) { const n = Number(v); return Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: d }) : "--"; }
function num(...vs) { for (const v of vs) { const n = Number(v); if (Number.isFinite(n)) return n; } return NaN; }

function toChartTime(row) {
  const d = String(row?.date || row?.Date || "").trim();
  if (/^\d{8}$/.test(d)) return `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  if (typeof row?.time === "number" && Number.isFinite(row.time)) return Math.floor(row.time);
  return null;
}
function normalizeRows(payload) {
  const src = Array.isArray(payload) ? payload : Array.isArray(payload?.data) ? payload.data : [];
  const used = new Set();
  return src
    .map(r => ({
      ...r,
      time: toChartTime(r),
      open: num(r.open, r.Open), high: num(r.high, r.High),
      low: num(r.low, r.Low),   close: num(r.close, r.Close),
      volume: num(r.volume, r.Volume, 0),
      ma5: num(r.ma5, r.MA5),   ma10: num(r.ma10, r.MA10),
      ma20: num(r.ma20, r.MA20), ma60: num(r.ma60, r.MA60),
      bb_upper: num(r.bb_upper), bb_lower: num(r.bb_lower),
      rsi14: num(r.rsi14), macd_hist: num(r.macd_hist),
      bb_width: num(r.bb_width),
    }))
    .filter(r => r.time && [r.open, r.high, r.low, r.close].every(Number.isFinite))
    .sort((a, b) => String(a.time).localeCompare(String(b.time)))
    .filter(r => { if (used.has(r.time)) return false; used.add(r.time); return true; });
}
function pickLine(rows, key) {
  return rows.filter(r => Number.isFinite(r[key])).map(r => ({ time: r.time, value: r[key] }));
}

/* ── AI Analysis Engine (based on 📈 股票技術與籌碼分析.docx) ───── */
function analyzeStock(rows, chipData) {
  if (!rows || rows.length < 10) return null;
  const latest = rows.at(-1);
  const prev = rows.at(-2) || {};
  const recent5 = rows.slice(-5);

  // 1. MA arrangement
  const { ma5, ma10, ma20, ma60 } = latest;
  const maStatus = (ma5 && ma20 && ma60)
    ? (ma5 > ma20 && ma20 > ma60 ? "四線多排" : ma5 > ma20 ? "多頭排列" : ma5 < ma20 && ma20 < ma60 ? "空頭排列" : "均線糾結")
    : "均線資料不足";
  const goldenCross = prev.ma5 && prev.ma20 && prev.ma5 < prev.ma20 && ma5 > ma20;
  const deathCross = prev.ma5 && prev.ma20 && prev.ma5 > prev.ma20 && ma5 < ma20;

  // 2. Volume-price matrix
  const avgVol5 = recent5.reduce((s, r) => s + (r.volume || 0), 0) / recent5.length;
  const volRatio = avgVol5 > 0 ? (latest.volume || 0) / avgVol5 : 1;
  const isUp = latest.close >= latest.open;
  const volPrice = volRatio > 1.3 && isUp ? "量增價漲"
    : volRatio > 1.3 && !isUp ? "量增價跌"
    : volRatio < 0.7 && isUp ? "量縮價漲"
    : volRatio < 0.7 && !isUp ? "量縮價跌"
    : "量能正常";

  // 3. RSI
  const rsi = latest.rsi14;
  const rsiStatus = !rsi ? "N/A"
    : rsi >= 80 ? "超買警戒（過熱）"
    : rsi >= 70 ? "偏強偏熱"
    : rsi <= 20 ? "深度超賣（底部機會）"
    : rsi <= 30 ? "超賣"
    : rsi <= 40 ? "偏弱"
    : "正常區間";

  // 4. MACD
  const hist = latest.macd_hist, prevHist = prev.macd_hist;
  const macdStatus = !Number.isFinite(hist) ? "N/A"
    : hist > 0 && prevHist <= 0 ? "MACD 翻多（金叉）"
    : hist < 0 && prevHist >= 0 ? "MACD 翻空（死叉）"
    : hist > 0 && hist > prevHist ? "多頭動能擴張"
    : hist > 0 ? "多頭動能收斂"
    : hist < 0 && hist < prevHist ? "空頭動能擴張"
    : "空頭動能收斂";

  // 5. Bollinger
  const bbW = latest.bb_width;
  const bbStatus = !bbW ? "N/A"
    : bbW > 0.08 ? "大幅開口（高波動）"
    : bbW < 0.02 ? "極度收縮（蓄勢待發）"
    : bbW < 0.04 ? "收縮（蓄勢中）"
    : "正常";

  // 6. Chip analysis
  const metrics = chipData?.analysis?.metrics || {};
  const chipScore = chipData?.analysis?.score ?? 50;
  const chipStatus = chipData?.analysis?.status || "無籌碼資料";
  const foreign5d = metrics.foreign_5d_sum || 0;
  const trust5d = metrics.investment_trust_5d_sum || 0;
  const shortRatio = metrics.short_margin_ratio;
  const foreignStreak = metrics.foreign_buy_streak || 0;
  const trustStreak = metrics.investment_trust_buy_streak || 0;

  // 7. Signals
  const signals = [];
  if (maStatus === "四線多排") signals.push({ t: "bull", icon: "🔥", text: "四線多排 — 最強波段多頭格局" });
  if (goldenCross) signals.push({ t: "bull", icon: "✅", text: "MA5 穿越 MA20 黃金交叉（中線買訊）" });
  if (volPrice === "量增價漲") signals.push({ t: "bull", icon: "📈", text: `量增價漲（量比 ${volRatio.toFixed(1)}x）— 買盤積極` });
  if (rsi <= 30) signals.push({ t: "bull", icon: "💡", text: `RSI ${fmt(rsi, 1)} 超賣 — 底部反彈機會` });
  if (hist > 0 && prevHist <= 0) signals.push({ t: "bull", icon: "⚡", text: "MACD 金叉翻多" });
  if (foreign5d > 0 && trust5d > 0) signals.push({ t: "bull", icon: "🏦", text: `外資投信雙買（外資近5日 ${(foreign5d / 1000).toFixed(0)}千張）` });
  if (foreignStreak >= 3) signals.push({ t: "bull", icon: "🏦", text: `外資連買 ${foreignStreak} 天（持續買超）` });
  if (trustStreak >= 3) signals.push({ t: "bull", icon: "🏦", text: `投信連買 ${trustStreak} 天（中期支撐）` });
  if (shortRatio > 30) signals.push({ t: "special", icon: "🎯", text: `券資比 ${shortRatio.toFixed(1)}% 偏高 — 潛在軋空行情` });
  if (bbW < 0.02) signals.push({ t: "neutral", icon: "🗜️", text: "布林極度收縮 — 即將大波動，方向待確認" });
  if (maStatus === "空頭排列") signals.push({ t: "bear", icon: "⚠️", text: "空頭排列 — 趨勢偏弱，建議觀望" });
  if (deathCross) signals.push({ t: "bear", icon: "❌", text: "MA5 跌破 MA20 死亡交叉（轉弱訊號）" });
  if (volPrice === "量增價跌") signals.push({ t: "bear", icon: "📉", text: `量增價跌（量比 ${volRatio.toFixed(1)}x）— 賣壓沉重` });
  if (rsi >= 80) signals.push({ t: "bear", icon: "🔴", text: `RSI ${fmt(rsi, 1)} 超買 — 短線注意過熱回測` });
  if (hist < 0 && prevHist >= 0) signals.push({ t: "bear", icon: "⚡", text: "MACD 死叉翻空" });
  if (foreign5d < 0 && trust5d < 0) signals.push({ t: "bear", icon: "🏦", text: "外資投信雙賣 — 法人出場，籌碼轉弱" });

  // 8. Score
  let score = 50;
  if (maStatus === "四線多排") score += 20;
  else if (maStatus === "多頭排列") score += 10;
  else if (maStatus === "空頭排列") score -= 15;
  if (volPrice === "量增價漲") score += 8;
  if (volPrice === "量增價跌") score -= 10;
  if (rsi && rsi > 70) score -= 5;
  if (rsi && rsi < 30) score += 5;
  if (Number.isFinite(hist)) score += hist > 0 ? 5 : -5;
  score += foreign5d > 0 ? 8 : foreign5d < 0 ? -8 : 0;
  score += trust5d > 0 ? 5 : trust5d < 0 ? -5 : 0;
  score = Math.max(0, Math.min(100, Math.round(score)));

  const trend = score >= 75 ? "強勢多頭" : score >= 60 ? "偏多觀察" : score >= 45 ? "中性盤整" : score >= 30 ? "偏空觀望" : "強勢空頭";

  return { score, trend, maStatus, volPrice, volRatio, rsiStatus, rsi, macdStatus, bbStatus, bbW, chipStatus, chipScore, foreign5d, trust5d, shortRatio, foreignStreak, trustStreak, signals };
}

/* ── Small UI components ─────────────────────────────────────────── */
function Row({ label, value, color }) {
  return (
    <div style={rowStyle}>
      <span style={{ color: "#94a3b8" }}>{label}</span>
      <b style={color ? { color } : undefined}>{value}</b>
    </div>
  );
}
function Card({ title, children, style: extraStyle }) {
  return <section style={{ ...cardStyle, ...extraStyle }}><h3 style={{ marginTop: 0 }}>{title}</h3>{children}</section>;
}
function LiveBadge() {
  return <span style={{ background: "#ef4444", color: "#fff", borderRadius: 4, padding: "2px 7px", fontSize: 11, fontWeight: 800, animation: "pulse 1.5s infinite" }}>● LIVE</span>;
}
function OrderBook({ bids = [], asks = [] }) {
  const n = Math.max(bids.length, asks.length, 1);
  return (
    <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
      <thead><tr>
        <th style={{ color: "#22c55e", textAlign: "right", paddingRight: 6 }}>委買量</th>
        <th style={{ color: "#22c55e", textAlign: "right", paddingRight: 6 }}>買價</th>
        <th style={{ color: "#ef4444", textAlign: "left", paddingLeft: 6 }}>賣價</th>
        <th style={{ color: "#ef4444", textAlign: "left", paddingLeft: 6 }}>委賣量</th>
      </tr></thead>
      <tbody>{Array.from({ length: n }).map((_, i) => {
        const b = bids[i] || {}, a = asks[i] || {};
        return (
          <tr key={i} style={{ borderBottom: "1px solid rgba(148,163,184,.08)" }}>
            <td style={{ textAlign: "right", padding: "3px 6px 3px 0", color: "#22c55e" }}>{b.qty != null ? b.qty.toLocaleString() : ""}</td>
            <td style={{ textAlign: "right", paddingRight: 6, color: "#22c55e", fontWeight: 700 }}>{b.price != null ? fmt(b.price) : ""}</td>
            <td style={{ textAlign: "left", paddingLeft: 6, color: "#ef4444", fontWeight: 700 }}>{a.price != null ? fmt(a.price) : ""}</td>
            <td style={{ textAlign: "left", paddingLeft: 6, color: "#ef4444" }}>{a.qty != null ? a.qty.toLocaleString() : ""}</td>
          </tr>
        );
      })}</tbody>
    </table>
  );
}

/* ── AI Card ─────────────────────────────────────────────────────── */
function AiCard({ rows, chipData, stockCode }) {
  const ai = useMemo(() => analyzeStock(rows, chipData), [rows, chipData]);
  const [groqText, setGroqText] = useState("");
  const [groqLoading, setGroqLoading] = useState(false);
  const [groqError, setGroqError] = useState("");
  const [groqMeta, setGroqMeta] = useState(null);

  async function runGroqAnalysis() {
    if (!stockCode || groqLoading) return;
    setGroqLoading(true);
    setGroqText("");
    setGroqError("");
    setGroqMeta(null);
    try {
      const res = await fetch(`${API}/api/ai/groq/${encodeURIComponent(stockCode)}`, { cache: "no-store" });
      const json = await res.json();
      if (json.error) { setGroqError(json.error); return; }
      setGroqText(json.analysis || "");
      setGroqMeta({ model: json.model, tokens: json.tokens_used, rows: json.data_rows });
    } catch (e) {
      setGroqError(e.message || "連線失敗");
    } finally {
      setGroqLoading(false);
    }
  }

  if (!ai) return <Card title="🤖 AI 趨勢分析"><p style={{ color: "#64748b" }}>資料載入中...</p></Card>;
  const scoreColor = ai.score >= 65 ? "#ef4444" : ai.score >= 45 ? "#f59e0b" : "#22c55e";
  const sigColor = { bull: "#ef4444", bear: "#22c55e", special: "#f59e0b", neutral: "#94a3b8" };
  const sigBg = { bull: "rgba(239,68,68,.08)", bear: "rgba(34,197,94,.08)", special: "rgba(251,191,36,.1)", neutral: "rgba(148,163,184,.08)" };

  return (
    <Card title="🤖 AI 趨勢分析">
      <div style={{ display: "flex", alignItems: "baseline", gap: 14, marginBottom: 12 }}>
        <div style={{ fontSize: 54, fontWeight: 900, color: scoreColor, lineHeight: 1 }}>{ai.score}</div>
        <div>
          <div style={{ fontSize: 20, fontWeight: 800, color: scoreColor }}>{ai.trend}</div>
          <div style={{ color: "#64748b", fontSize: 12 }}>技術 + 籌碼綜合評分</div>
        </div>
      </div>

      {ai.signals.length > 0 && (
        <div style={{ marginBottom: 14 }}>
          {ai.signals.map((sig, i) => (
            <div key={i} style={{ padding: "5px 10px", marginBottom: 4, borderRadius: 4, fontSize: 13, background: sigBg[sig.t], borderLeft: `3px solid ${sigColor[sig.t]}` }}>
              {sig.icon} {sig.text}
            </div>
          ))}
        </div>
      )}

      <div style={{ fontSize: 13, display: "grid", gap: 6, marginBottom: 14 }}>
        {[
          ["均線", ai.maStatus],
          ["量價矩陣", `${ai.volPrice}（量比 ${ai.volRatio.toFixed(1)}x）`],
          ["RSI14", `${ai.rsiStatus}（${fmt(ai.rsi, 1)}）`],
          ["MACD", ai.macdStatus],
          ["布林通道", ai.bbStatus],
          ["籌碼狀態", `${ai.chipStatus}（籌碼分 ${ai.chipScore}）`],
          ai.shortRatio != null ? ["券資比", `${ai.shortRatio.toFixed(1)}%${ai.shortRatio > 30 ? " ⚠️ 偏高" : ""}`] : null,
          ai.foreignStreak > 0 ? ["外資", `連買 ${ai.foreignStreak} 天`] : ai.foreign5d !== 0 ? ["外資近5日", `${ai.foreign5d > 0 ? "+" : ""}${(ai.foreign5d / 1000).toFixed(0)} 千張`] : null,
        ].filter(Boolean).map(([label, value]) => (
          <div key={label} style={{ display: "flex", justifyContent: "space-between", borderBottom: "1px solid rgba(148,163,184,.12)", paddingBottom: 5 }}>
            <span style={{ color: "#94a3b8" }}>{label}</span>
            <span style={{ textAlign: "right", maxWidth: "60%" }}>{value}</span>
          </div>
        ))}
      </div>

      {/* Groq deep analysis */}
      <button
        type="button"
        onClick={runGroqAnalysis}
        disabled={groqLoading}
        style={{ width: "100%", padding: "10px 0", borderRadius: 6, border: 0, background: groqLoading ? "#1e293b" : "linear-gradient(90deg,#7c3aed,#2563eb)", color: "white", fontWeight: 700, cursor: groqLoading ? "default" : "pointer", fontSize: 14, marginBottom: 10 }}
      >
        {groqLoading ? "⏳ Groq 分析中..." : "⚡ Groq 深度分析（llama-3.3-70b）"}
      </button>

      {groqError && (
        <div style={{ padding: 10, borderRadius: 6, background: "rgba(239,68,68,.1)", color: "#fca5a5", fontSize: 13, marginBottom: 10 }}>
          ❌ {groqError}
        </div>
      )}

      {groqText && (
        <div style={{ padding: 12, borderRadius: 6, background: "rgba(124,58,237,.08)", border: "1px solid rgba(124,58,237,.25)", fontSize: 13, lineHeight: 1.8, whiteSpace: "pre-wrap", marginBottom: 8 }}>
          {groqText}
        </div>
      )}

      {groqMeta && (
        <div style={{ color: "#475569", fontSize: 11 }}>
          模型：{groqMeta.model}・Token：{groqMeta.tokens}・資料筆數：{groqMeta.rows}
        </div>
      )}

      <div style={{ marginTop: 10, padding: 8, borderRadius: 4, background: "rgba(148,163,184,.06)", color: "#64748b", fontSize: 11, lineHeight: 1.6 }}>
        ⚠️ 本分析為量化指標與 AI 參考，不構成投資建議。操作前請自行研判風險。
      </div>
    </Card>
  );
}

/* ── Main App ────────────────────────────────────────────────────── */
function addSeries(chart, Type, opts, fallback) {
  return typeof chart.addSeries === "function" && Type ? chart.addSeries(Type, opts) : chart[fallback](opts);
}

export default function App() {
  const mainRef = useRef(null);
  const rsiRef = useRef(null);
  const macdRef = useRef(null);
  const chartsRef = useRef({});
  const seriesRef = useRef({});
  const syncingRef = useRef(false);

  const [input, setInput] = useState("2330");
  const [stock, setStock] = useState(resolveStock("2330"));
  const [loadKey, setLoadKey] = useState(0);
  const [openSuggest, setOpenSuggest] = useState(false);
  const [payload, setPayload] = useState(null);
  const [rows, setRows] = useState([]);
  const [chip, setChip] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [realtime, setRealtime] = useState(null);
  const [status, setStatus] = useState("載入中...");
  const [lastRefresh, setLastRefresh] = useState(null);
  const [isLive, setIsLive] = useState(isTradingSession);
  const [hovered, setHovered] = useState(null);
  const [suggestions, setSuggestions] = useState([]);
  const [backfillAttempt, setBackfillAttempt] = useState(0);
  const searchTimerRef = useRef(null);

  /* ── Chart init ──────────────────────────────────────────────── */
  useEffect(() => {
    if (!mainRef.current || !rsiRef.current || !macdRef.current) return;
    const theme = {
      layout: { background: { color: "#0f172a" }, textColor: "#dbeafe" },
      grid: { vertLines: { color: "#1e293b" }, horzLines: { color: "#1e293b" } },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#334155" },
      rightPriceScale: { borderColor: "#334155" },
      crosshair: { mode: 1 },
      autoSize: true,
    };

    const main = createChart(mainRef.current, { ...theme, height: 520 });
    const rsi  = createChart(rsiRef.current,  { ...theme, height: 130 });
    const macd = createChart(macdRef.current, { ...theme, height: 150 });
    chartsRef.current = { main, rsi, macd };

    const candle = addSeries(main, CandlestickSeries, {
      upColor: "#ef4444", downColor: "#22c55e",
      borderUpColor: "#ef4444", borderDownColor: "#22c55e",
      wickUpColor: "#ef4444", wickDownColor: "#22c55e",
    }, "addCandlestickSeries");

    const volume = addSeries(main, HistogramSeries, {
      priceFormat: { type: "volume" }, priceScaleId: "vol",
    }, "addHistogramSeries");
    try {
      main.priceScale("vol").applyOptions({ scaleMargins: { top: 0.78, bottom: 0 }, visible: false });
    } catch (_) {}

    const ma5s  = addSeries(main, LineSeries, { color: "#facc15", lineWidth: 1, lastValueVisible: false, priceLineVisible: false }, "addLineSeries");
    const ma10s = addSeries(main, LineSeries, { color: "#fb923c", lineWidth: 1, lastValueVisible: false, priceLineVisible: false }, "addLineSeries");
    const ma20s = addSeries(main, LineSeries, { color: "#38bdf8", lineWidth: 1, lastValueVisible: false, priceLineVisible: false }, "addLineSeries");
    const ma60s = addSeries(main, LineSeries, { color: "#a78bfa", lineWidth: 1, lastValueVisible: false, priceLineVisible: false }, "addLineSeries");
    const bbUs  = addSeries(main, LineSeries, { color: "rgba(148,163,184,.3)", lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false }, "addLineSeries");
    const bbLs  = addSeries(main, LineSeries, { color: "rgba(148,163,184,.3)", lineWidth: 1, lineStyle: 2, lastValueVisible: false, priceLineVisible: false }, "addLineSeries");
    const rsiS  = addSeries(rsi,  LineSeries, { color: "#f59e0b", lineWidth: 2 }, "addLineSeries");
    const macdS = addSeries(macd, HistogramSeries, {}, "addHistogramSeries");

    seriesRef.current = { candle, volume, ma5s, ma10s, ma20s, ma60s, bbUs, bbLs, rsiS, macdS };

    main.subscribeCrosshairMove(param => {
      if (!param.point || !param.time) { setHovered(null); return; }
      const c = param.seriesData?.get(candle);
      const v = param.seriesData?.get(volume);
      if (c) setHovered({ time: param.time, open: c.open, high: c.high, low: c.low, close: c.close, volume: v?.value });
    });

    const syncRange = (src, targets) => {
      src.timeScale().subscribeVisibleLogicalRangeChange(range => {
        if (syncingRef.current || !range) return;
        syncingRef.current = true;
        targets.forEach(t => { try { t.timeScale().setVisibleLogicalRange(range); } catch (_) {} });
        syncingRef.current = false;
      });
    };
    syncRange(main, [rsi, macd]);
    syncRange(rsi,  [main, macd]);
    syncRange(macd, [main, rsi]);

    return () => {
      Object.values(chartsRef.current).forEach(c => { try { c.remove(); } catch (_) {} });
      chartsRef.current = {};
    };
  }, []);

  /* ── Search API (全市場股票名稱/代號搜尋) ───────────────────── */
  useEffect(() => {
    const q = input.trim();
    if (!q) { setSuggestions([]); return; }
    clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(async () => {
      try {
        const res = await fetch(`${API}/api/search?q=${encodeURIComponent(q)}`, { cache: "no-store" });
        if (res.ok) {
          const json = await res.json();
          setSuggestions(Array.isArray(json) ? json.slice(0, 8) : []);
        }
      } catch { setSuggestions([]); }
    }, 250);
    return () => clearTimeout(searchTimerRef.current);
  }, [input]);

  /* ── Feed chart data ─────────────────────────────────────────── */
  useEffect(() => {
    const s = seriesRef.current;
    if (!s.candle || !rows.length) return;
    s.candle.setData(rows.map(r => ({ time: r.time, open: r.open, high: r.high, low: r.low, close: r.close })));
    s.volume.setData(rows.map(r => ({ time: r.time, value: r.volume || 0, color: r.close >= r.open ? "rgba(239,68,68,.5)" : "rgba(34,197,94,.5)" })));
    s.ma5s.setData(pickLine(rows, "ma5"));
    s.ma10s.setData(pickLine(rows, "ma10"));
    s.ma20s.setData(pickLine(rows, "ma20"));
    s.ma60s.setData(pickLine(rows, "ma60"));
    s.bbUs.setData(pickLine(rows, "bb_upper"));
    s.bbLs.setData(pickLine(rows, "bb_lower"));
    s.rsiS.setData(pickLine(rows, "rsi14"));
    s.macdS.setData(rows.filter(r => Number.isFinite(r.macd_hist)).map(r => ({
      time: r.time, value: r.macd_hist,
      color: r.macd_hist >= 0 ? "rgba(239,68,68,.8)" : "rgba(34,197,94,.8)",
    })));
    Object.values(chartsRef.current).forEach(c => c.timeScale().fitContent());
  }, [rows]);

  /* ── Fetch kline ─────────────────────────────────────────────── */
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
      if (nextRows.length > 0) {
        if (!isRefresh) setStatus(`已載入 ${nextRows.length} 筆資料`);
        setBackfillAttempt(0);
      } else if (!isRefresh) {
        // 無資料時後端已自動啟動 backfill，前端輪詢等待資料進來
        setBackfillAttempt(1);
        setStatus("⏳ 首次查詢，正在補回歷史資料（約 30～60 秒）...");
      }
    } catch (e) {
      if (!isRefresh) setStatus(`查詢失敗：${e?.message}`);
    }
  }, []);

  /* ── Initial full load (triggers on stock change OR manual refresh) */
  useEffect(() => {
    let alive = true;
    setStatus(`⏳ 查詢 ${stock.code} 中...`);
    setRows([]); setPayload(null); setChip(null); setAnalysis(null); setRealtime(null); setHovered(null); setBackfillAttempt(0);
    fetchKline(stock.code);
    Promise.allSettled([
      fetch(`${API}/api/chip/${encodeURIComponent(stock.code)}?auto_init=false`, { cache: "no-store" }),
      fetch(`${API}/api/analysis/${encodeURIComponent(stock.code)}`, { cache: "no-store" }),
    ]).then(([chipRes, anaRes]) => {
      if (!alive) return;
      if (chipRes.status === "fulfilled" && chipRes.value.ok) chipRes.value.json().then(j => { if (alive) setChip(j); }).catch(() => {});
      if (anaRes.status === "fulfilled" && anaRes.value.ok) anaRes.value.json().then(j => { if (alive) setAnalysis(j); }).catch(() => {});
    });
    return () => { alive = false; };
  }, [stock.code, loadKey, fetchKline]);

  /* ── Live polling ────────────────────────────────────────────── */
  useEffect(() => {
    const id = setInterval(() => {
      const live = isTradingSession();
      setIsLive(live);
      if (live) fetchKline(stock.code, true);
    }, POLL_MS);
    return () => clearInterval(id);
  }, [stock.code, fetchKline]);

  /* ── 手動觸發 backfill ───────────────────────────────────────── */
  async function triggerBackfill() {
    const code = stock.code;
    setStatus(`⏳ 手動補資料中（${code}）...`);
    try {
      await fetch(`${API}/api/job/backfill/${encodeURIComponent(code)}`, { cache: "no-store" });
      setBackfillAttempt(1);
    } catch (e) {
      setStatus(`補資料失敗：${e?.message}`);
    }
  }

  /* ── Backfill 輪詢：空資料時每 5 秒重查直到資料進來（最多 30 次）── */
  useEffect(() => {
    if (backfillAttempt <= 0 || backfillAttempt > 30) return;
    const code = stock.code;
    const timer = setTimeout(async () => {
      try {
        const res = await fetch(`${API}/api/kline/${encodeURIComponent(code)}`, { cache: "no-store" });
        if (!res.ok) { setBackfillAttempt(a => a + 1); return; }
        const json = await res.json();
        const nextRows = normalizeRows(json);
        if (nextRows.length > 0) {
          setPayload(json);
          setRows(nextRows);
          setRealtime(json.realtime || null);
          setLastRefresh(new Date());
          setStatus(`✅ 歷史資料已補回（${nextRows.length} 筆）`);
          setBackfillAttempt(0);
        } else {
          setStatus(`⏳ 補資料中，第 ${backfillAttempt} 次確認...`);
          setBackfillAttempt(a => a + 1);
        }
      } catch { setBackfillAttempt(a => a + 1); }
    }, 5000);
    return () => clearTimeout(timer);
  }, [backfillAttempt, stock.code]);

  function submit() {
    const t = suggestions[0] || resolveStock(input);
    setStock(t);
    setInput(t.code);
    setOpenSuggest(false);
    setLoadKey(k => k + 1);  // always force a fresh load
  }

  const meta = { ...stock, ...(payload?.meta || {}), ...(analysis?.meta || {}) };
  const displayBar = hovered || rows.at(-1) || {};
  const change = Number(meta.change ?? (displayBar.close - displayBar.open));
  const priceColor = change >= 0 ? "#ef4444" : "#22c55e";
  const livePrice = (hovered ? hovered.close : null) ?? realtime?.price ?? meta.price ?? displayBar.close;

  return (
    <div style={pageStyle}>
      <style>{`@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}} * { box-sizing: border-box; }`}</style>

      {/* Header */}
      <header style={headerStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 12 }}>
          <div>
            <div style={eyebrowStyle}>TW STOCK DECISION SYSTEM {APP_VERSION}</div>
        <div style={{ color: "#475569", fontSize: 11, marginTop: 2 }}>API: {API}</div>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", marginTop: 6 }}>
              <span style={{ fontSize: 28, fontWeight: 900, color: "#facc15" }}>{meta.code || stock.code}</span>
              <span style={{ fontSize: 22, fontWeight: 700 }}>{meta.name || stock.name}</span>
              <span style={{ color: "#64748b", fontSize: 14 }}>{meta.market} / {meta.industry}</span>
              {isLive && <LiveBadge />}
            </div>
          </div>
          <div style={{ textAlign: "right" }}>
            <div style={{ fontSize: 42, fontWeight: 900, color: priceColor }}>{fmt(livePrice)}</div>
            <div style={{ color: priceColor, fontSize: 14 }}>
              漲跌 {fmt(meta.change ?? change)}（{fmt(meta.change_pct, 2)}%）
            </div>
          </div>
        </div>

        {/* OHLCV bar — updates with crosshair */}
        <div style={{ marginTop: 8, display: "flex", gap: 20, fontSize: 14, flexWrap: "wrap" }}>
          {[
            ["開盤", displayBar.open],
            ["最高", displayBar.high, "#ef4444"],
            ["最低", displayBar.low, "#22c55e"],
            ["收盤", displayBar.close, priceColor],
            ["成交量", displayBar.volume ? (displayBar.volume / 1000).toFixed(0) + "千張" : "--"],
          ].map(([label, val, color]) => (
            <span key={label}>
              <span style={{ color: "#64748b" }}>{label} </span>
              <b style={color ? { color } : undefined}>{typeof val === "string" ? val : fmt(val)}</b>
            </span>
          ))}
          {hovered && <span style={{ color: "#475569", fontSize: 12 }}>📅 {String(hovered.time)}</span>}
          {!hovered && lastRefresh && <span style={{ color: "#475569", fontSize: 12 }}>更新 {lastRefresh.toLocaleTimeString("zh-TW")}</span>}
        </div>

        {/* Toolbar */}
        <div style={toolbarStyle}>
          <div style={{ position: "relative" }}>
            <input value={input} onFocus={() => setOpenSuggest(true)}
              onChange={e => { setInput(e.target.value); setOpenSuggest(true); }}
              onKeyDown={e => { if (e.key === "Enter") submit(); }}
              placeholder="輸入股票代號或名稱" style={inputStyle} />
            {openSuggest && suggestions.length > 0 && (
              <div style={suggestStyle}>
                {suggestions.map(item => (
                  <div key={item.code} onMouseDown={() => { setStock(item); setInput(item.code); setOpenSuggest(false); }} style={suggestItemStyle}>
                    <b style={{ color: "#facc15" }}>{item.code}</b> {item.name}
                    <span style={{ color: "#94a3b8", marginLeft: 8 }}>{item.market}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
          <button type="button" onClick={submit} style={buttonStyle}>查詢</button>
          {!rows.length && (
            <button type="button" onClick={triggerBackfill} style={{ ...buttonStyle, background: "#7c3aed", fontSize: 13, padding: "10px 14px" }}>
              ⚡ 手動補資料
            </button>
          )}
          <span style={{ color: rows.length ? "#22c55e" : backfillAttempt > 0 ? "#f97316" : "#f59e0b", fontSize: 13 }}>{status}</span>
          {isLive && <span style={{ color: "#94a3b8", fontSize: 12 }}>每 {POLL_MS / 1000}s 自動更新</span>}
        </div>
      </header>

      {/* Main layout */}
      <main style={mainStyle}>
        {/* Chart column */}
        <div>
          <div style={cardStyle}>
            <div style={{ display: "flex", gap: 14, marginBottom: 6, fontSize: 12, flexWrap: "wrap" }}>
              {[["■", "#facc15", "MA5"], ["■", "#fb923c", "MA10"], ["■", "#38bdf8", "MA20"], ["■", "#a78bfa", "MA60"], ["╌", "rgba(148,163,184,.6)", "布林帶"]].map(([sym, color, label]) => (
                <span key={label}><span style={{ color }}>{sym}</span> {label}</span>
              ))}
              <span style={{ color: "#94a3b8" }}>│ 下方柱狀為成交量</span>
            </div>
            <div ref={mainRef} />
          </div>
          <div style={{ ...cardStyle, marginTop: 8 }}>
            <div style={{ color: "#f59e0b", fontSize: 12, marginBottom: 4 }}>RSI14 &nbsp;
              {Number.isFinite(displayBar.rsi14) && <span style={{ color: displayBar.rsi14 > 70 ? "#ef4444" : displayBar.rsi14 < 30 ? "#22c55e" : "#f59e0b", fontWeight: 700 }}>{fmt(displayBar.rsi14, 1)}</span>}
            </div>
            <div ref={rsiRef} />
          </div>
          <div style={{ ...cardStyle, marginTop: 8 }}>
            <div style={{ color: "#94a3b8", fontSize: 12, marginBottom: 4 }}>MACD Histogram &nbsp;
              {Number.isFinite(displayBar.macd_hist) && <span style={{ color: displayBar.macd_hist >= 0 ? "#ef4444" : "#22c55e", fontWeight: 700 }}>{fmt(displayBar.macd_hist, 3)}</span>}
            </div>
            <div ref={macdRef} />
          </div>
        </div>

        {/* Sidebar */}
        <aside style={asideStyle}>
          <AiCard rows={rows} chipData={chip} stockCode={stock.code} />

          {isLive && (
            <Card title={<>即時委買委賣 <LiveBadge /></>}>
              {realtime?.bids?.length ? <OrderBook bids={realtime.bids} asks={realtime.asks} /> : <div style={{ color: "#64748b", fontSize: 13 }}>等待揭示...</div>}
              {realtime?.time && <div style={{ color: "#475569", fontSize: 11, marginTop: 6 }}>成交時間 {realtime.time}・量 {realtime.volume_lot?.toLocaleString() || "--"} 張</div>}
            </Card>
          )}

          <Card title="籌碼摘要">
            {(() => {
              const cl = chip?.latest_chip || {};
              const cm = chip?.analysis?.metrics || {};
              return <>
                <Row label="資料日期" value={cl.date || "--"} />
                <Row label="外資買賣超" value={fmt(cl.foreign_buy, 0)} color={Number(cl.foreign_buy) > 0 ? "#ef4444" : "#22c55e"} />
                <Row label="投信買賣超" value={fmt(cl.investment_trust_buy, 0)} color={Number(cl.investment_trust_buy) > 0 ? "#ef4444" : "#22c55e"} />
                <Row label="自營商買賣超" value={fmt(cl.dealer_buy, 0)} color={Number(cl.dealer_buy) > 0 ? "#ef4444" : "#22c55e"} />
                <Row label="融資餘額" value={fmt(cm.margin_balance || cl.margin_balance, 0)} />
                <Row label="融券餘額" value={fmt(cm.short_balance || cl.short_balance, 0)} />
                <Row label="籌碼評分" value={`${chip?.analysis?.score ?? "--"} / 100`} />
                <Row label="籌碼狀態" value={chip?.analysis?.status || "--"} />
              </>;
            })()}
          </Card>

          <Card title="技術指標（最新 / 游標）">
            <Row label="MA5" value={fmt(displayBar.ma5)} />
            <Row label="MA20" value={fmt(displayBar.ma20)} />
            <Row label="MA60" value={fmt(displayBar.ma60)} />
            <Row label="RSI14" value={fmt(displayBar.rsi14, 1)} />
            <Row label="MACD Hist" value={fmt(displayBar.macd_hist, 3)} />
            <Row label="布林寬度" value={fmt(displayBar.bb_width, 4)} />
          </Card>
        </aside>
      </main>
    </div>
  );
}

/* ── Styles ──────────────────────────────────────────────────────── */
const pageStyle = { minHeight: "100vh", background: "#020617", color: "#f1f5f9", fontFamily: "Arial, sans-serif" };
const headerStyle = { padding: "20px 24px 16px", borderBottom: "1px solid #1e293b", background: "#0f172a" };
const eyebrowStyle = { color: "#38bdf8", letterSpacing: 1, fontWeight: 800, fontSize: 12 };
const toolbarStyle = { marginTop: 14, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" };
const inputStyle = { padding: "10px 14px", borderRadius: 8, border: "1px solid #334155", background: "#020617", color: "white", minWidth: 260, fontSize: 14 };
const buttonStyle = { padding: "10px 18px", borderRadius: 8, border: 0, background: "#2563eb", color: "white", fontWeight: 700, cursor: "pointer" };
const mainStyle = { padding: 16, display: "grid", gridTemplateColumns: "minmax(0,2.2fr) minmax(300px,.8fr)", gap: 16 };
const asideStyle = { display: "grid", gap: 12, alignContent: "start" };
const cardStyle = { background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8, padding: 16 };
const rowStyle = { display: "flex", justifyContent: "space-between", gap: 8, borderBottom: "1px solid rgba(148,163,184,.12)", padding: "5px 0", fontSize: 13 };
const suggestStyle = { position: "absolute", top: 44, left: 0, right: 0, background: "#0f172a", border: "1px solid #334155", borderRadius: 8, zIndex: 10, overflow: "hidden" };
const suggestItemStyle = { padding: "9px 12px", cursor: "pointer", borderBottom: "1px solid rgba(148,163,184,.1)", fontSize: 14 };
