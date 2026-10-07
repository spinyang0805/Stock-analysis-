// 看盤圖：分時 / 5分 / 日K / 週K / 月K，區間切換、副圖（量 / KD / RSI / MACD）、十字線逐筆資料、逐日資料表
// lightweight-charts 固定 v4.2.3（v5 API 不相容）；主圖與副圖兩個 chart 同步時間軸與十字線
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createChart, CrosshairMode, LineStyle } from "lightweight-charts";
import { Table2, RefreshCw } from "lucide-react";
import { Seg } from "./ui.jsx";
import { aggregateBars, computeIndicators } from "../lib/indicators.js";
import { fetchIntraday } from "../lib/market.js";
import { supabaseConfigured } from "../lib/supabase.js";
import { DOWN, UP, fmt, isTradingSession, signed, todayISO, trendColor } from "../lib/format.js";

const TZ_SHIFT = 8 * 3600; // 分時時間戳轉台北時間顯示
const PERIODS = [
  { value: "1m", label: "分時" },
  { value: "5m", label: "5分" },
  { value: "D", label: "日K" },
  { value: "W", label: "週K" },
  { value: "M", label: "月K" },
];
const RANGES = [
  { value: "3M", label: "3月", days: 92 },
  { value: "6M", label: "6月", days: 183 },
  { value: "1Y", label: "1年", days: 366 },
  { value: "3Y", label: "3年", days: 1096 },
  { value: "5Y", label: "5年", days: 1827 },
  { value: "ALL", label: "全部", days: Infinity },
];
const SUBS = [
  { value: "VOL", label: "成交量" },
  { value: "KD", label: "KD" },
  { value: "RSI", label: "RSI" },
  { value: "MACD", label: "MACD" },
];
const MA_COLORS = { ma5: "#facc15", ma10: "#fb923c", ma20: "#38bdf8", ma60: "#a78bfa" };

const THEME = {
  layout: { background: { color: "#0e1223" }, textColor: "#cbd5e1", fontFamily: "Fira Code, ui-monospace, monospace" },
  grid: { vertLines: { color: "#18213a" }, horzLines: { color: "#18213a" } },
  rightPriceScale: { borderColor: "#334155" },
  timeScale: { borderColor: "#334155", rightOffset: 4 },
  crosshair: { mode: CrosshairMode.Normal },
  autoSize: true,
};

function loadPref(key, def) {
  try { return localStorage.getItem(`chart.${key}`) || def; } catch { return def; }
}
function savePref(key, v) {
  try { localStorage.setItem(`chart.${key}`, v); } catch { /* 無痕模式 */ }
}

function fmtTime(t, intraday) {
  if (typeof t === "number") {
    const d = new Date(t * 1000);
    return `${String(d.getUTCMonth() + 1).padStart(2, "0")}/${String(d.getUTCDate()).padStart(2, "0")} ${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
  }
  return intraday ? t : String(t);
}

export default function StockChart({ code, market, dailyRows, longBars, longLoading, quote, levels = [], prevClose }) {
  const mainEl = useRef(null), subEl = useRef(null);
  const charts = useRef({ main: null, sub: null });
  const series = useRef({});
  const priceLines = useRef([]);
  const [period, setPeriod] = useState(() => loadPref("period", "D"));
  const [range, setRange] = useState(() => loadPref("range", "6M"));
  const [sub, setSub] = useState(() => loadPref("sub", "VOL"));
  const [showBB, setShowBB] = useState(() => loadPref("bb", "0") === "1");
  const [logScale, setLogScale] = useState(() => loadPref("log", "0") === "1");
  const [hoverTime, setHoverTime] = useState(null); // 十字線所在 K 棒時間（分時有補空白點，不能用邏輯索引）
  const [intra, setIntra] = useState({ bars: [], prevClose: NaN, loading: false, error: "" });
  const [showTable, setShowTable] = useState(false);

  useEffect(() => savePref("period", period), [period]);
  useEffect(() => savePref("range", range), [range]);
  useEffect(() => savePref("sub", sub), [sub]);
  useEffect(() => savePref("bb", showBB ? "1" : "0"), [showBB]);
  useEffect(() => {
    savePref("log", logScale ? "1" : "0");
    charts.current.main?.priceScale("right").applyOptions({ mode: logScale && period !== "1m" && period !== "5m" ? 1 : 0 });
  }, [logScale, period]);

  const intraday = period === "1m" || period === "5m";

  /* ── 分時資料：開盤中每 30 秒更新 ─────────────────────────────── */
  const loadIntraday = useCallback(async () => {
    if (!intraday || !code) return;
    if (!supabaseConfigured) {
      setIntra({ bars: [], prevClose: NaN, loading: false, error: "分時／5分需要即時服務：網站尚未設定 Supabase 金鑰（VITE_SUPABASE_ANON_KEY）。日K／週K／月K 可正常使用。" });
      return;
    }
    setIntra((s) => ({ ...s, loading: true, error: "" }));
    try {
      const r = period === "1m" ? await fetchIntraday(code, "1m", "1d", market) : await fetchIntraday(code, "5m", "5d", market);
      setIntra({ bars: r.bars.map((b) => ({ ...b, time: b.time + TZ_SHIFT })), prevClose: r.prevClose, loading: false, error: r.bars.length ? "" : "目前沒有分時資料" });
    } catch (e) {
      setIntra({ bars: [], prevClose: NaN, loading: false, error: `分時資料讀取失敗：${e.message}` });
    }
  }, [code, period, intraday, market]);

  useEffect(() => {
    setIntra({ bars: [], prevClose: NaN, loading: false, error: "" });
    if (!intraday) return undefined;
    loadIntraday();
    const id = setInterval(() => { if (isTradingSession() && !document.hidden) loadIntraday(); }, 30000);
    return () => clearInterval(id);
  }, [intraday, loadIntraday]);

  /* ── 日/週/月：長期歷史 + 靜態近一年 + 盤中即時合併成今日K ───────── */
  const bars = useMemo(() => {
    if (intraday) {
      const list = intra.bars;
      if (period === "1m") {
        // 分時均價線（累計成交金額 ÷ 累計量）
        let pv = 0, v = 0;
        return list.map((b) => { pv += b.close * (b.volume || 0); v += b.volume || 0; return { ...b, avg: v ? pv / v : b.close }; });
      }
      return computeIndicators(list);
    }
    const map = new Map();
    for (const b of longBars || []) map.set(b.time, { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume });
    for (const b of dailyRows || []) map.set(b.time, { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume });
    const today = todayISO();
    if (quote && Number.isFinite(quote.price) && quote.date && `${quote.date.slice(0, 4)}-${quote.date.slice(4, 6)}-${quote.date.slice(6, 8)}` === today) {
      const prev = map.get(today);
      map.set(today, {
        time: today,
        open: Number.isFinite(quote.open) ? quote.open : prev?.open ?? quote.price,
        high: Math.max(quote.high || quote.price, quote.price),
        low: Math.min(Number.isFinite(quote.low) ? quote.low : quote.price, quote.price),
        close: quote.price,
        volume: Number.isFinite(quote.volumeLots) && quote.volumeLots > 0 ? quote.volumeLots * 1000 : prev?.volume || 0,
      });
    }
    const daily = [...map.values()].sort((a, b) => a.time.localeCompare(b.time));
    return computeIndicators(aggregateBars(daily, period));
  }, [intraday, intra.bars, period, longBars, dailyRows, quote]);

  /* ── 建立圖表（一次） ───────────────────────────────────────────── */
  useEffect(() => {
    const main = createChart(mainEl.current, { ...THEME, timeScale: { ...THEME.timeScale, timeVisible: true, secondsVisible: false } });
    const subc = createChart(subEl.current, { ...THEME, timeScale: { ...THEME.timeScale, visible: false } });
    charts.current = { main, sub: subc };
    let syncing = false;
    const sync = (src, dst) => src.timeScale().subscribeVisibleLogicalRangeChange((r) => {
      if (syncing || !r) return;
      syncing = true;
      try { dst.timeScale().setVisibleLogicalRange(r); } catch { /* 尚無資料 */ }
      syncing = false;
    });
    sync(main, subc); sync(subc, main);
    const onMove = (src) => (param) => {
      if (!param.point || param.time === undefined) { setHoverTime(null); return; }
      setHoverTime(param.time);
      const other = src === main ? subc : main;
      const s = src === main ? series.current.sub0 : series.current.main0;
      if (other.setCrosshairPosition && s) {
        try { other.setCrosshairPosition(NaN, param.time, s); } catch { /* ignore */ }
      }
    };
    main.subscribeCrosshairMove(onMove(main));
    subc.subscribeCrosshairMove(onMove(subc));
    return () => { main.remove(); subc.remove(); charts.current = { main: null, sub: null }; series.current = {}; };
  }, []);

  /* ── 序列結構：只在週期 / 副圖 / 布林切換時重建（報價更新不重建，保留使用者縮放） ── */
  const [structVer, setStructVer] = useState(0);
  const rangeKey = useRef("");
  const prevLine = useRef(null);
  useEffect(() => {
    const { main, sub: subc } = charts.current;
    if (!main) return;
    for (const s of Object.values(series.current)) { try { s.chart.removeSeries(s.api); } catch { /* removed */ } }
    series.current = {};
    priceLines.current = [];
    prevLine.current = null;
    const add = (chart, key, type, opts) => {
      const api = chart[type](opts);
      series.current[key] = { api, chart };
      return api;
    };
    const hideLast = { lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false };
    main.applyOptions({ timeScale: { timeVisible: intraday, secondsVisible: false } });
    main.priceScale("right").applyOptions({ mode: logScale && !intraday ? 1 : 0 });
    if (period === "1m") {
      add(main, "price", "addBaselineSeries", {
        baseValue: { type: "price", price: 0 },
        topLineColor: UP, topFillColor1: "rgba(239,68,68,.25)", topFillColor2: "rgba(239,68,68,.02)",
        bottomLineColor: DOWN, bottomFillColor1: "rgba(34,197,94,.02)", bottomFillColor2: "rgba(34,197,94,.25)",
        lineWidth: 2,
      });
      add(main, "avg", "addLineSeries", { color: "#facc15", lineWidth: 1, ...hideLast });
    } else {
      add(main, "candle", "addCandlestickSeries", { upColor: UP, downColor: DOWN, borderUpColor: UP, borderDownColor: DOWN, wickUpColor: UP, wickDownColor: DOWN });
      for (const [k, color] of Object.entries(MA_COLORS)) add(main, k, "addLineSeries", { color, lineWidth: 1, ...hideLast });
      if (showBB) for (const k of ["bb_upper", "bb_lower"]) add(main, k, "addLineSeries", { color: "rgba(148,163,184,.55)", lineWidth: 1, lineStyle: LineStyle.Dashed, ...hideLast });
    }
    const subKind = period === "1m" ? "VOL" : sub;
    if (subKind === "VOL") {
      add(subc, "vol", "addHistogramSeries", { priceFormat: { type: "volume" }, lastValueVisible: false, priceLineVisible: false });
      if (!intraday) add(subc, "volume_ma5", "addLineSeries", { color: "#facc15", lineWidth: 1, ...hideLast });
    } else if (subKind === "KD") {
      const k = add(subc, "kd_k", "addLineSeries", { color: "#f97316", lineWidth: 2, ...hideLast });
      add(subc, "kd_d", "addLineSeries", { color: "#38bdf8", lineWidth: 2, ...hideLast });
      for (const lv of [80, 20]) k.createPriceLine({ price: lv, color: "#475569", lineStyle: LineStyle.Dotted, lineWidth: 1, axisLabelVisible: false });
    } else if (subKind === "RSI") {
      const r = add(subc, "rsi14", "addLineSeries", { color: "#f59e0b", lineWidth: 2, ...hideLast });
      for (const lv of [70, 30]) r.createPriceLine({ price: lv, color: "#475569", lineStyle: LineStyle.Dotted, lineWidth: 1, axisLabelVisible: false });
    } else {
      add(subc, "macd_hist", "addHistogramSeries", { lastValueVisible: false, priceLineVisible: false });
      add(subc, "macd", "addLineSeries", { color: "#facc15", lineWidth: 1, ...hideLast });
      add(subc, "macd_signal", "addLineSeries", { color: "#38bdf8", lineWidth: 1, ...hideLast });
    }
    const S = series.current;
    S.main0 = (S.candle || S.price).api;
    S.sub0 = (S.vol || S.kd_k || S.rsi14 || S.macd_hist).api;
    rangeKey.current = "";
    setStructVer((v) => v + 1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [period, sub, showBB, intraday]);

  /* ── 餵資料：缺值用 whitespace 點（只給 time），主副圖 K 棒索引才會一致 ── */
  useEffect(() => {
    const S = series.current;
    const { main, sub: subc } = charts.current;
    if (!main || !S.main0) return;
    const line = (k, scale = 1) => bars.map((b) => (Number.isFinite(b[k]) ? { time: b.time, value: b[k] / scale } : { time: b.time }));
    if (S.price) {
      const base = Number.isFinite(intra.prevClose) ? intra.prevClose : bars[0]?.open;
      S.price.api.applyOptions({ baseValue: { type: "price", price: base || 0 } });
      // 像看盤軟體一樣固定顯示整個交易時段 09:00～13:30（沒資料的分鐘補空白點）
      const pad = [];
      if (bars.length) {
        const day = Math.floor(bars[0].time / 86400) * 86400;
        const have = new Set(bars.map((b) => b.time));
        for (let t = day + 9 * 3600; t <= day + 13.5 * 3600; t += 60) if (!have.has(t)) pad.push({ time: t });
      }
      const withPad = (arr) => [...arr, ...pad].sort((a, b) => a.time - b.time);
      S.price.api.setData(withPad(bars.map((b) => ({ time: b.time, value: b.close }))));
      S.avg.api.setData(withPad(line("avg")));
      if (prevLine.current) { try { S.price.api.removePriceLine(prevLine.current); } catch { /* ignore */ } }
      prevLine.current = base ? S.price.api.createPriceLine({ price: base, color: "#64748b", lineStyle: LineStyle.Dashed, lineWidth: 1, axisLabelVisible: true, title: "昨收" }) : null;
    } else {
      S.candle.api.setData(bars.map((b) => ({ time: b.time, open: b.open, high: b.high, low: b.low, close: b.close })));
      for (const k of [...Object.keys(MA_COLORS), "bb_upper", "bb_lower"]) S[k]?.api.setData(line(k));
    }
    if (S.vol) {
      const padVol = period === "1m" && bars.length ? (() => {
        const day = Math.floor(bars[0].time / 86400) * 86400, have = new Set(bars.map((b) => b.time)), out = [];
        for (let t = day + 9 * 3600; t <= day + 13.5 * 3600; t += 60) if (!have.has(t)) out.push({ time: t });
        return out;
      })() : [];
      S.vol.api.setData([...padVol, ...bars.map((b, i) => ({
        time: b.time, value: (b.volume || 0) / 1000,
        color: (period === "1m" ? b.close >= (bars[i - 1]?.close ?? b.open) : b.close >= b.open) ? "rgba(239,68,68,.6)" : "rgba(34,197,94,.6)",
      }))].sort((a, b) => a.time - b.time));
      S.volume_ma5?.api.setData(line("volume_ma5", 1000));
    }
    for (const k of ["kd_k", "kd_d", "rsi14", "macd", "macd_signal"]) S[k]?.api.setData(line(k));
    S.macd_hist?.api.setData(bars.map((b) => (Number.isFinite(b.macd_hist) ? { time: b.time, value: b.macd_hist, color: b.macd_hist >= 0 ? "rgba(239,68,68,.6)" : "rgba(34,197,94,.6)" } : { time: b.time })));

    // 可見區間：只在換股票 / 週期 / 區間（或第一次有資料）時設定，之後的報價更新不動使用者的縮放
    const n = bars.length;
    // 長期歷史晚到時 K 棒會往前補，邏輯索引位移 → 重新套用區間
    const key = `${code}|${period}|${range}|${structVer}|${longBars?.length ? "L" : "S"}`;
    if (!n || rangeKey.current === key) return;
    rangeKey.current = key;
    if (intraday) main.timeScale().fitContent();
    else {
      const days = RANGES.find((r) => r.value === range)?.days ?? 183;
      const per = period === "D" ? 1 : period === "W" ? 7 : 30.4;
      const count = days === Infinity ? n : Math.min(n, Math.max(10, Math.round((days / per) * (period === "D" ? 250 / 365 : 1))));
      main.timeScale().setVisibleLogicalRange({ from: n - count - 0.5, to: n - 1 + 3 });
    }
    try { subc.timeScale().setVisibleLogicalRange(main.timeScale().getVisibleLogicalRange()); } catch { /* ignore */ }
  }, [bars, structVer, range, code, period, intraday, intra.prevClose, longBars]);

  /* ── 成本 / 策略價位線 ───────────────────────────────────────────── */
  useEffect(() => {
    const s = series.current.candle?.api;
    for (const pl of priceLines.current) { try { pl.s.removePriceLine(pl.line); } catch { /* removed */ } }
    priceLines.current = [];
    if (!s) return;
    for (const lv of levels) {
      if (!Number.isFinite(lv.price)) continue;
      const line = s.createPriceLine({ price: lv.price, color: lv.color, lineWidth: 1, lineStyle: lv.style ?? LineStyle.Dashed, axisLabelVisible: true, title: lv.label });
      priceLines.current.push({ s, line });
    }
  }, [levels, structVer]);

  /* ── 十字線所在 K 棒 ───────────────────────────────────────────── */
  const timeIndex = useMemo(() => new Map(bars.map((b, i) => [b.time, i])), [bars]);
  const idx = hoverTime != null && timeIndex.has(hoverTime) ? timeIndex.get(hoverTime) : bars.length - 1;
  const bar = bars[idx];
  const prevBar = bars[idx - 1];
  const ref = period === "1m" ? (Number.isFinite(intra.prevClose) ? intra.prevClose : prevClose) : prevBar?.close;
  const chg = bar && Number.isFinite(ref) ? bar.close - ref : NaN;

  const tableRows = useMemo(() => [...bars].reverse(), [bars]);
  const [tablePage, setTablePage] = useState(0);
  const [jumpDate, setJumpDate] = useState("");
  useEffect(() => setTablePage(0), [period, code]);
  const PAGE = 20;

  function jumpTo(date) {
    setJumpDate(date);
    if (!date || intraday) return;
    const i = bars.findIndex((b) => b.time >= date);
    if (i < 0) return;
    const main = charts.current.main;
    main?.timeScale().setVisibleLogicalRange({ from: Math.max(0, i - 30), to: Math.min(bars.length + 3, i + 30) });
    setHoverTime(bars[i].time);
    const ti = tableRows.findIndex((b) => b.time <= date);
    if (ti >= 0) setTablePage(Math.floor(ti / PAGE));
  }

  const emptyMsg = intraday ? (intra.loading ? "分時資料載入中…" : intra.error) : !bars.length ? "K 線資料載入中…" : "";

  return (
    <section className="card chart-card" aria-label="看盤圖">
      <div className="chart-toolbar">
        <Seg label="週期" options={PERIODS} value={period} onChange={setPeriod} />
        {!intraday && <Seg label="區間" options={RANGES.map(({ value, label }) => ({ value, label }))} value={range} onChange={setRange} />}
        {period !== "1m" && <Seg label="副圖指標" options={SUBS} value={sub} onChange={setSub} />}
        <span className="spacer" />
        {!intraday && (
          <>
            <button type="button" className="btn sm" aria-pressed={showBB} onClick={() => setShowBB((v) => !v)}>布林</button>
            <button type="button" className="btn sm" aria-pressed={logScale} onClick={() => setLogScale((v) => !v)} title="長期走勢建議用對數座標">對數</button>
          </>
        )}
        {intraday && (
          <button type="button" className="btn sm" onClick={loadIntraday} aria-label="重新整理分時"><RefreshCw size={14} /></button>
        )}
        <button type="button" className="btn sm" aria-pressed={showTable} onClick={() => setShowTable((v) => !v)}><Table2 size={14} />逐筆資料</button>
      </div>

      <div className="chart-legend num" aria-live="off">
        {bar ? (
          <>
            <span>{fmtTime(bar.time, intraday)}</span>
            {period !== "1m" && <span>開 <b>{fmt(bar.open)}</b></span>}
            {period !== "1m" && <span>高 <b className="up">{fmt(bar.high)}</b></span>}
            {period !== "1m" && <span>低 <b className="down">{fmt(bar.low)}</b></span>}
            <span>{period === "1m" ? "價" : "收"} <b style={{ color: trendColor(chg) }}>{fmt(bar.close)}</b></span>
            <span style={{ color: trendColor(chg) }}>{signed(chg)}（{signed(Number.isFinite(ref) && ref ? (chg / ref) * 100 : NaN, 2, "%")}）</span>
            <span>量 <b>{fmt((bar.volume || 0) / 1000, 0)}</b> 張</span>
            {period === "1m" && <span style={{ color: "#facc15" }}>均價 <b>{fmt(bar.avg)}</b></span>}
            {period !== "1m" && Object.entries(MA_COLORS).map(([k, c]) => Number.isFinite(bar[k]) && (
              <span key={k} style={{ color: c }}>{k.toUpperCase()} {fmt(bar[k])}</span>
            ))}
          </>
        ) : <span>{longLoading ? "長期資料載入中…" : ""}</span>}
      </div>

      <div className="chart-main">
        <div ref={mainEl} style={{ position: "absolute", inset: 0 }} />
        {emptyMsg && <div className="chart-empty">{emptyMsg}</div>}
      </div>
      <div className="chart-sub-label num">
        {(period === "1m" || sub === "VOL") && bar && <span>量 {fmt((bar.volume || 0) / 1000, 0)} 張{Number.isFinite(bar.volume_ma5) ? `／5日均 ${fmt(bar.volume_ma5 / 1000, 0)}` : ""}</span>}
        {period !== "1m" && sub === "KD" && bar && <><span style={{ color: "#f97316" }}>K {fmt(bar.kd_k, 1)}</span><span style={{ color: "#38bdf8" }}>D {fmt(bar.kd_d, 1)}</span></>}
        {period !== "1m" && sub === "RSI" && bar && <span style={{ color: "#f59e0b" }}>RSI14 {fmt(bar.rsi14, 1)}</span>}
        {period !== "1m" && sub === "MACD" && bar && <><span style={{ color: "#facc15" }}>DIF {fmt(bar.macd, 2)}</span><span style={{ color: "#38bdf8" }}>MACD {fmt(bar.macd_signal, 2)}</span><span style={{ color: trendColor(bar.macd_hist) }}>柱 {fmt(bar.macd_hist, 2)}</span></>}
        {!intraday && longLoading && <span className="muted">（長期歷史載入中，週K/月K 稍後完整）</span>}
      </div>
      <div className="chart-sub"><div ref={subEl} style={{ position: "absolute", inset: 0 }} /></div>

      {showTable && (
        <div style={{ marginTop: 12 }}>
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 8 }}>
            <b style={{ fontSize: 14 }}>{PERIODS.find((p) => p.value === period)?.label} 逐筆資料（新到舊）</b>
            {!intraday && (
              <label className="field" style={{ gridAutoFlow: "column", alignItems: "center", gap: 6 }}>
                <span className="label">跳到日期</span>
                <input type="date" className="input" style={{ minHeight: 34, padding: "4px 8px", fontSize: 14 }} value={jumpDate} onChange={(e) => jumpTo(e.target.value)} />
              </label>
            )}
            <span className="spacer" style={{ flex: 1 }} />
            <button type="button" className="btn sm" disabled={tablePage === 0} onClick={() => setTablePage((p) => p - 1)}>較新</button>
            <span className="dim num">{tablePage + 1} / {Math.max(1, Math.ceil(tableRows.length / PAGE))}</span>
            <button type="button" className="btn sm" disabled={(tablePage + 1) * PAGE >= tableRows.length} onClick={() => setTablePage((p) => p + 1)}>較舊</button>
          </div>
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th className="left">時間</th>
                  {period !== "1m" && <><th>開</th><th>高</th><th>低</th></>}
                  <th>收</th><th>漲跌</th><th>漲跌%</th><th>量(張)</th>
                  {period !== "1m" && <><th>MA5</th><th>MA20</th><th>MA60</th><th>K</th><th>D</th><th>RSI</th></>}
                </tr>
              </thead>
              <tbody>
                {tableRows.slice(tablePage * PAGE, tablePage * PAGE + PAGE).map((b) => {
                  const i = bars.indexOf(b);
                  const p = period === "1m" && i === 0 ? intra.prevClose : bars[i - 1]?.close;
                  const c = Number.isFinite(p) ? b.close - p : NaN;
                  return (
                    <tr key={b.time} className="clickable" onClick={() => setHoverTime(b.time)} style={i === idx ? { outline: "1px solid var(--accent)" } : undefined}>
                      <td className="left num">{fmtTime(b.time, intraday)}</td>
                      {period !== "1m" && <><td>{fmt(b.open)}</td><td className="up">{fmt(b.high)}</td><td className="down">{fmt(b.low)}</td></>}
                      <td style={{ color: trendColor(c), fontWeight: 700 }}>{fmt(b.close)}</td>
                      <td style={{ color: trendColor(c) }}>{signed(c)}</td>
                      <td style={{ color: trendColor(c) }}>{signed(Number.isFinite(p) && p ? (c / p) * 100 : NaN, 2, "%")}</td>
                      <td>{fmt((b.volume || 0) / 1000, 0)}</td>
                      {period !== "1m" && <><td>{fmt(b.ma5)}</td><td>{fmt(b.ma20)}</td><td>{fmt(b.ma60)}</td><td>{fmt(b.kd_k, 1)}</td><td>{fmt(b.kd_d, 1)}</td><td>{fmt(b.rsi14, 1)}</td></>}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
}
