import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import * as LightweightCharts from "lightweight-charts";

const { createChart, CandlestickSeries, LineSeries, HistogramSeries } = LightweightCharts;
const API = "https://stock-analysis-tw.fly.dev";
const APP_VERSION = "v21-dashboard";
const POLL_MS = 10_000;

/* ── Helpers ─────────────────────────────────────────────────────── */
function isTradingSession() {
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const day = now.getDay();
  if (day === 0 || day === 6) return false;
  const m = now.getHours() * 60 + now.getMinutes();
  return m >= 540 && m <= 810;
}
function cleanCode(v) { return String(v || "2330").trim().replace(/\.(TW|TWO)$/i, "").split(/\s+/)[0].toUpperCase(); }
function resolveStock(q) {
  const raw = String(q || "2330").trim();
  return { code: cleanCode(raw), name: cleanCode(raw), market: "--", industry: "--" };
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

/* ══════════════════════════════════════════════════════════════════
   Analysis Engine — 📈 股票技術與籌碼分析.docx
   ══════════════════════════════════════════════════════════════════ */

function getMaStatus(rows) {
  if (!rows?.length) return { label: "資料不足", color: "#94a3b8", conclusion: "資料不足，無法評估均線狀態。" };
  const latest = rows.at(-1);
  const prev = rows.at(-2) || {};
  const { ma5, ma10, ma20, ma60 } = latest;
  if (!ma5 || !ma20 || !ma60) return { label: "均線計算中", color: "#94a3b8", conclusion: "均線資料累積中，請稍後再查。", ma5, ma10, ma20, ma60 };
  const slope60 = prev.ma60 ? ma60 - prev.ma60 : 0;
  const slope5 = prev.ma5 ? ma5 - prev.ma5 : 0;
  const goldenCross = prev.ma5 && prev.ma20 && prev.ma5 < prev.ma20 && ma5 > ma20;
  const deathCross  = prev.ma5 && prev.ma20 && prev.ma5 > prev.ma20 && ma5 < ma20;
  const turnBear = latest.close < ma5 && latest.close < (ma10 || ma5) && slope5 < 0;
  const isBullPerfect = ma5 > ma10 && ma10 > ma20 && ma20 > ma60 && slope60 > 0;
  const isBull = ma5 > ma20;
  const isBear = ma5 < ma20 && ma20 < ma60;
  let label, color, conclusion;
  if (isBullPerfect)  { label="四線多排"; color="#ef4444"; conclusion="均線完美多頭排列且MA60向上，波段起漲點確立，可積極持多。"; }
  else if (goldenCross){ label="黃金交叉"; color="#f97316"; conclusion="MA5穿越MA20黃金交叉，中線買訊出現，建議逢回加碼。"; }
  else if (deathCross) { label="死亡交叉"; color="#22c55e"; conclusion="MA5跌破MA20死亡交叉，中線轉弱，建議分批減碼。"; }
  else if (turnBear)   { label="轉空警訊"; color="#16a34a"; conclusion="收盤跌破MA5/MA10且均線下彎，短線動能消失，宜出場觀望。"; }
  else if (isBull)     { label="多頭排列"; color="#f59e0b"; conclusion="均線多頭排列，趨勢偏多，持股待漲為主。"; }
  else if (isBear)     { label="空頭排列"; color="#64748b"; conclusion="均線空頭排列，趨勢偏空，空手觀望為宜。"; }
  else                 { label="均線糾結"; color="#94a3b8"; conclusion="多空均線糾結，方向未明，等待一方突破再行動。"; }
  return { label, color, conclusion, ma5, ma10, ma20, ma60, slope60, slope5, goldenCross, deathCross, turnBear, isBullPerfect };
}

function getVolPriceMatrix(rows) {
  if (!rows?.length) return { type:"資料不足", color:"#94a3b8", score:0, volRatio:1, conclusion:"資料不足。" };
  const recent5 = rows.slice(-5);
  const latest = rows.at(-1);
  const avgVol = recent5.reduce((s, r) => s + (r.volume || 0), 0) / recent5.length || 1;
  const volRatio = (latest.volume || 0) / avgVol;
  const isUp = latest.close >= latest.open;
  let type, color, score, conclusion;
  if (volRatio > 1.3 && isUp)   { type="量增價漲"; color="#ef4444"; score=2;  conclusion=`量增（${volRatio.toFixed(1)}x）價漲，買盤積極入場，短線做多訊號明確。`; }
  else if (volRatio > 1.3)      { type="量增價跌"; color="#22c55e"; score=-2; conclusion=`量增（${volRatio.toFixed(1)}x）價跌，賣壓沉重，宜立即止損出場。`; }
  else if (volRatio < 0.7 && isUp){ type="量縮價漲"; color="#f59e0b"; score=1;  conclusion=`量縮（${volRatio.toFixed(1)}x）價漲，無量上攻，等待放量突破再加碼。`; }
  else if (volRatio < 0.7)      { type="量縮價跌"; color="#94a3b8"; score=-1; conclusion=`量縮（${volRatio.toFixed(1)}x）價跌，賣盤無力，惜售支撐，尚無系統風險。`; }
  else                          { type="量能正常"; color="#94a3b8"; score=0;  conclusion=`量能正常（${volRatio.toFixed(1)}x），觀察是否有放量突破機會。`; }
  return { type, color, score, volRatio, conclusion };
}

function detectPatterns(rows) {
  if (!rows || rows.length < 20) return { patterns:[{ type:"neutral", label:"📊 資料不足", desc:"需20筆以上才能偵測型態" }], bullCount:0, bearCount:0, conclusion:"資料累積中，暫無型態判斷。" };
  const latest = rows.at(-1);
  const recent20 = rows.slice(-20);
  const recent5  = rows.slice(-5);
  const box20High = Math.max(...recent20.slice(0,-1).map(r => r.high));
  const box20Low  = Math.min(...recent20.slice(0,-1).map(r => r.low));
  const avgVol5 = recent5.reduce((s,r) => s+(r.volume||0), 0) / 5 || 1;
  const volRatio = (latest.volume||0) / avgVol5;
  const isBoxBreak = latest.close > box20High && volRatio > 1.5;
  const isBreakDown = latest.close < box20Low;
  const isNearHigh = latest.close > box20High * 0.97 && !isBoxBreak;
  // W-bottom simplified
  const lows = recent20.map(r => r.low);
  const minLow = Math.min(...lows);
  const firstIdx = lows.indexOf(minLow);
  let isWBottom = false;
  if (firstIdx >= 2 && firstIdx < lows.length - 4) {
    const secondTrough = Math.min(...lows.slice(firstIdx + 3));
    const neckLine = Math.max(...recent20.slice(firstIdx, firstIdx+3).map(r => r.high));
    isWBottom = secondTrough > minLow * 1.005 && latest.close > neckLine * 0.98;
  }
  const patterns = [];
  if (isBoxBreak)   patterns.push({ type:"bull", label:"🚀 帶量突破", desc:`突破近20日高點，量比 ${volRatio.toFixed(1)}x，新趨勢啟動` });
  if (isWBottom)    patterns.push({ type:"bull", label:"📐 W底確立", desc:"雙底型態成立，底部支撐確認" });
  if (isNearHigh)   patterns.push({ type:"bull", label:"📈 逼近壓力區", desc:"接近近20日高點，突破機率增加" });
  if (isBreakDown)  patterns.push({ type:"bear", label:"⚠️ 跌破支撐", desc:"有效跌破近20日低點，建議止損" });
  if (!patterns.length) patterns.push({ type:"neutral", label:"📊 區間整理", desc:"股價在近期區間內整理，等待方向訊號" });
  const bullCount = patterns.filter(p=>p.type==="bull").length;
  const bearCount = patterns.filter(p=>p.type==="bear").length;
  const conclusion = bullCount > 0 && bearCount === 0
    ? `偵測到 ${bullCount} 個多頭型態，技術面偏多，注意放量確認。`
    : bearCount > 0
    ? "偵測到跌破支撐型態，注意下行風險，嚴守停損。"
    : "股價於區間整理，等待量能放大的突破方向訊號。";
  return { patterns, bullCount, bearCount, conclusion };
}

function getChipAnalysis(chipData) {
  if (!chipData) return { foreign5d:0, trust5d:0, foreignStreak:0, trustStreak:0, score:50, status:"無資料", conclusion:"籌碼資料未取得，無法評估機構動向。", metrics:{}, latestChip:{} };
  const metrics = chipData.analysis?.metrics || {};
  const cl = chipData.latest_chip || {};
  const foreign5d  = metrics.foreign_5d_sum || 0;
  const trust5d    = metrics.investment_trust_5d_sum || 0;
  const foreignStreak = metrics.foreign_buy_streak || 0;
  const trustStreak   = metrics.investment_trust_buy_streak || 0;
  const score  = chipData.analysis?.score ?? 50;
  const status = chipData.analysis?.status || "中性";
  let conclusion;
  if (foreign5d > 0 && trust5d > 0)
    conclusion = `外資投信雙買（近5日合計 ${((foreign5d+trust5d)/1000).toFixed(0)}千張），法人積極佈局，籌碼健康。`;
  else if (foreign5d > 0)
    conclusion = `外資近5日買超 ${(foreign5d/1000).toFixed(0)}千張，主力偏多，持股續抱為宜。`;
  else if (trust5d > 0)
    conclusion = `投信連買 ${trustStreak||"多"} 天，中期支撐明顯，觀察外資是否跟進。`;
  else if (foreign5d < 0 && trust5d < 0)
    conclusion = "外資投信雙賣，法人持續出場，籌碼轉弱建議觀望。";
  else
    conclusion = `法人籌碼中性（${status}），無明確方向，等待法人明確表態。`;
  return { foreign5d, trust5d, foreignStreak, trustStreak, score, status, conclusion, metrics, latestChip:cl };
}

function detectBlackCandleAccum(rows, chipData) {
  if (!rows?.length) return { signal:"無資料", color:"#94a3b8", isAccum:false, isBlack:false, closeAbovePrev:false, instBuy:false, changeRate:0, conclusion:"資料不足。" };
  const latest = rows.at(-1);
  const prev = rows.at(-2) || {};
  const cl = chipData?.latest_chip || {};
  const instBuy = Number(cl.institution_total_buy || 0) > 0;
  const isBlack = latest.close < latest.open;
  const closeAbovePrev = latest.close > (prev.close || 0);
  const isAccum = isBlack && closeAbovePrev && instBuy;
  let signal, color, conclusion;
  if (isAccum)          { signal="法人黑K吸籌"; color="#f59e0b"; conclusion="外表下跌、收高於昨收且法人買超，是主力洗盤吸籌的典型訊號，可適度跟進。"; }
  else if (isBlack && instBuy){ signal="法人逆勢買入"; color="#f97316"; conclusion="黑K但法人逆勢買超，籌碼流向機構，視為中線偏多訊號。"; }
  else if (isBlack)     { signal="一般下跌"; color="#22c55e"; conclusion="一般性下跌，無法人護盤訊號，謹慎操作，等待止跌訊號。"; }
  else                  { signal="紅K上漲"; color="#ef4444"; conclusion=instBuy?"收紅K且法人買超，量價齊揚，多頭訊號強烈。":"收紅K上漲，觀察法人是否跟進確認多頭格局。"; }
  const changeRate = latest.open > 0 ? (latest.close - latest.open) / latest.open * 100 : 0;
  return { signal, color, isAccum, isBlack, closeAbovePrev, instBuy, changeRate, conclusion };
}

function getRiskMetrics(rows, chipData) {
  if (!rows?.length) return { isLongRisk:false, isShortSqueeze:false, marginBalance:0, shortBalance:0, shortRatio:null, belowMa60:false, nearHigh:false, conclusion:"資料不足。" };
  const latest = rows.at(-1);
  const metrics = chipData?.analysis?.metrics || {};
  const cl = chipData?.latest_chip || {};
  const marginBalance = Number(metrics.margin_balance || cl.margin_balance || 0);
  const shortBalance  = Number(metrics.short_balance  || cl.short_balance  || 0);
  const shortRatio    = metrics.short_margin_ratio;
  const belowMa60 = !!(latest.ma60 && latest.close < latest.ma60);
  const high20 = Math.max(...rows.slice(-20).map(r => r.high));
  const nearHigh = latest.close > high20 * 0.95;
  const isLongRisk    = belowMa60 && marginBalance > 10000;
  const isShortSqueeze = shortRatio != null && shortRatio > 30 && nearHigh;
  let conclusion;
  if (isLongRisk && isShortSqueeze) conclusion = "同時出現斷頭風險與軋空預兆，多空交戰激烈，波動將加劇，謹慎操作。";
  else if (isLongRisk)   conclusion = "融資部位高且跌破MA60，有系統性斷頭崩盤風險，建議立即迴避。";
  else if (isShortSqueeze) conclusion = `券資比 ${shortRatio.toFixed(1)}% 偏高且接近20日高點，空頭回補行情可期，可積極追多。`;
  else conclusion = "融資融券無異常風險，市場相對健康，可依技術面操作。";
  return { marginBalance, shortBalance, shortRatio, belowMa60, nearHigh, isLongRisk, isShortSqueeze, conclusion };
}

function getRsiAnalysis(rows) {
  if (!rows?.length) return { rsi:null, status:"N/A", color:"#94a3b8", conclusion:"資料不足。" };
  const rsi = rows.at(-1).rsi14;
  if (!Number.isFinite(rsi)) return { rsi:null, status:"計算中", color:"#94a3b8", conclusion:"RSI資料累積中。" };
  let status, color, conclusion;
  if (rsi >= 80)      { status="嚴重超買"; color="#ef4444"; conclusion=`RSI ${rsi.toFixed(1)} 嚴重超買，短線過熱，考慮部分獲利了結。`; }
  else if (rsi >= 70) { status="超買偏熱"; color="#f97316"; conclusion=`RSI ${rsi.toFixed(1)} 進入超買區，追高風險增加，持股者注意停利。`; }
  else if (rsi <= 20) { status="深度超賣"; color="#22c55e"; conclusion=`RSI ${rsi.toFixed(1)} 深度超賣，底部反彈機率極高，可小量試探佈局。`; }
  else if (rsi <= 30) { status="超賣";     color="#34d399"; conclusion=`RSI ${rsi.toFixed(1)} 超賣區，短線反彈可期，搭配型態確認再進場。`; }
  else if (rsi >= 50) { status="偏強";     color="#f59e0b"; conclusion=`RSI ${rsi.toFixed(1)} 在多頭強勢區，趨勢維持中，持股不必急賣。`; }
  else                { status="偏弱";     color="#64748b"; conclusion=`RSI ${rsi.toFixed(1)} 在弱勢區，多頭動能不足，觀望為宜。`; }
  return { rsi, status, color, conclusion };
}

function getMacdAnalysis(rows) {
  if (!rows?.length) return { status:"N/A", color:"#94a3b8", hist:null, conclusion:"資料不足。" };
  const latest = rows.at(-1);
  const prev   = rows.at(-2) || {};
  const hist = latest.macd_hist, prevHist = prev.macd_hist;
  if (!Number.isFinite(hist)) return { status:"計算中", color:"#94a3b8", hist:null, conclusion:"MACD資料累積中。" };
  let status, color, conclusion;
  if (hist > 0 && prevHist <= 0)       { status="金叉翻多"; color="#ef4444"; conclusion="MACD柱翻正金叉，中線多頭訊號，可積極建立多頭部位。"; }
  else if (hist < 0 && prevHist >= 0)  { status="死叉翻空"; color="#22c55e"; conclusion="MACD柱翻負死叉，中線空頭訊號，建議減碼降低持倉。"; }
  else if (hist > 0 && hist > prevHist){ status="多頭擴張"; color="#f97316"; conclusion="多頭動能持續擴張，趨勢強勁，順勢持有。"; }
  else if (hist > 0)                   { status="多頭收斂"; color="#f59e0b"; conclusion="多頭動能開始收斂，注意是否轉折，可考慮部分獲利。"; }
  else if (hist < prevHist)            { status="空頭擴張"; color="#16a34a"; conclusion="空頭動能持續擴大，下跌趨勢確立，不宜搶反彈。"; }
  else                                 { status="空頭收斂"; color="#86efac"; conclusion="空頭動能收斂，跌勢趨緩，等待金叉翻多確認後再進場。"; }
  return { hist, prevHist, status, color, conclusion };
}

function getScenarios(rows, chipData) {
  if (!rows?.length) return { bull:33, bear:33, neutral:34, conclusion:"資料不足。" };
  const ma   = getMaStatus(rows);
  const vol  = getVolPriceMatrix(rows);
  const chip = getChipAnalysis(chipData);
  let bull=30, bear=25, neutral=45;
  const maAdd = {"四線多排":20,"多頭排列":12,"黃金交叉":10,"均線糾結":0,"死亡交叉":-12,"空頭排列":-15,"轉空警訊":-10};
  bull += maAdd[ma.label] ?? 0; bear -= (maAdd[ma.label] ?? 0) * 0.5; neutral -= (maAdd[ma.label] ?? 0) * 0.5;
  bull += vol.score*5; bear -= vol.score*3; neutral -= vol.score*2;
  if (chip.foreign5d > 0 && chip.trust5d > 0) { bull+=12; bear-=8; neutral-=4; }
  else if (chip.foreign5d < 0 && chip.trust5d < 0) { bear+=12; bull-=8; neutral-=4; }
  const total = bull+bear+neutral;
  bull    = Math.max(5, Math.round(bull/total*100));
  bear    = Math.max(5, Math.round(bear/total*100));
  neutral = Math.max(5, 100-bull-bear);
  let conclusion;
  if (bull >= 50)      conclusion=`多頭情境機率最高（${bull}%），技術與籌碼共同支持，可積極佈局多方。`;
  else if (bear >= 40) conclusion=`空頭情境機率偏高（${bear}%），謹慎看待，等待空頭確認再佈局。`;
  else                 conclusion="情境機率分散，市場方向未定，縮小倉位等待突破確認。";
  return { bull, bear, neutral, conclusion };
}

function getOverallScore(rows, chipData) {
  if (!rows?.length) return 50;
  const ma   = getMaStatus(rows);
  const vol  = getVolPriceMatrix(rows);
  const chip = getChipAnalysis(chipData);
  const rsi  = getRsiAnalysis(rows);
  const macd = getMacdAnalysis(rows);
  let score = 50;
  const maAdd = {"四線多排":20,"多頭排列":10,"黃金交叉":8,"均線糾結":0,"死亡交叉":-10,"空頭排列":-15,"轉空警訊":-12};
  score += maAdd[ma.label] ?? 0;
  score += vol.score * 5;
  if (rsi.rsi) { if (rsi.rsi > 70) score -= 5; if (rsi.rsi < 30) score += 5; }
  if (macd.hist != null) score += macd.hist > 0 ? 5 : -5;
  score += chip.foreign5d > 0 ? 8 : chip.foreign5d < 0 ? -8 : 0;
  score += chip.trust5d > 0 ? 5 : chip.trust5d < 0 ? -5 : 0;
  return Math.max(0, Math.min(100, Math.round(score)));
}

function getTechRadar(rows, chipData) {
  if (!rows?.length) return { dims:[] };
  const ma   = getMaStatus(rows);
  const vol  = getVolPriceMatrix(rows);
  const chip = getChipAnalysis(chipData);
  const rsi  = getRsiAnalysis(rows);
  const macd = getMacdAnalysis(rows);
  const pat  = detectPatterns(rows);
  const maScore  = {"四線多排":95,"多頭排列":75,"黃金交叉":70,"均線糾結":50,"死亡交叉":30,"空頭排列":20,"轉空警訊":15}[ma.label] ?? 50;
  const volScore = {2:85,1:65,0:50,"-1":40,"-2":15}[String(vol.score)] ?? 50;
  const rsiScore = rsi.rsi ? Math.min(100, Math.max(0, rsi.rsi)) : 50;
  const macdScore = macd.hist != null ? Math.min(90, Math.max(10, 50 + macd.hist * 300)) : 50;
  const breakoutScore = Math.min(90, Math.max(10, 40 + pat.bullCount*20 - pat.bearCount*15));
  return {
    dims: [
      { label:"趨勢強度", value:maScore,    color:ma.color },
      { label:"量價配合", value:volScore,   color:vol.color },
      { label:"RSI動能",  value:rsiScore,   color:rsi.color },
      { label:"MACD訊號", value:macdScore,  color:macd.color },
      { label:"籌碼健康", value:chip.score, color:chip.score>60?"#ef4444":chip.score<40?"#22c55e":"#f59e0b" },
      { label:"突破潛力", value:breakoutScore, color:breakoutScore>60?"#ef4444":"#94a3b8" },
    ],
  };
}

/* ══════════════════════════════════════════════════════════════════
   UI Components
   ══════════════════════════════════════════════════════════════════ */

function ConclusionLine({ text, color }) {
  return (
    <div style={{ marginTop:10, padding:"8px 10px", borderRadius:4, background:"rgba(148,163,184,.07)", borderLeft:`3px solid ${color||"#94a3b8"}`, fontSize:12, color:"#cbd5e1", lineHeight:1.7 }}>
      💡 {text}
    </div>
  );
}

function Row({ label, value, color }) {
  return (
    <div style={rowStyle}>
      <span style={{ color:"#94a3b8" }}>{label}</span>
      <b style={color?{color}:undefined}>{value}</b>
    </div>
  );
}

function Card({ title, icon, children, style:extraStyle }) {
  return (
    <section style={{ ...cardStyle, ...extraStyle }}>
      <h3 style={{ margin:"0 0 12px 0", fontSize:13, color:"#94a3b8", letterSpacing:0.5, fontWeight:700 }}>
        {icon&&<span style={{ marginRight:5 }}>{icon}</span>}{title}
      </h3>
      {children}
    </section>
  );
}

function LiveBadge() {
  return <span style={{ background:"#ef4444", color:"#fff", borderRadius:4, padding:"2px 7px", fontSize:11, fontWeight:800, animation:"pulse 1.5s infinite" }}>● LIVE</span>;
}

function ScoreBadge({ score }) {
  const color = score >= 65 ? "#ef4444" : score >= 45 ? "#f59e0b" : "#22c55e";
  const label = score >= 75 ? "強勢多頭" : score >= 60 ? "偏多觀察" : score >= 45 ? "中性盤整" : score >= 30 ? "偏空觀望" : "強勢空頭";
  return (
    <div style={{ display:"flex", alignItems:"center", gap:12, marginBottom:12 }}>
      <div style={{ fontSize:48, fontWeight:900, color, lineHeight:1 }}>{score}</div>
      <div>
        <div style={{ fontSize:17, fontWeight:800, color }}>{label}</div>
        <div style={{ color:"#64748b", fontSize:11, marginTop:2 }}>技術 + 籌碼綜合評分</div>
        <div style={{ marginTop:5, width:120, height:6, borderRadius:3, overflow:"hidden", background:"#1e293b" }}>
          <div style={{ height:"100%", width:`${score}%`, borderRadius:3, background:`linear-gradient(90deg,#22c55e,#f59e0b,#ef4444)`, backgroundSize:"200% 100%", backgroundPosition:`${score}% 50%` }} />
        </div>
      </div>
    </div>
  );
}

/* ── Card: 技術強度雷達 ───────────────────────────────────────────── */
function TechRadarCard({ rows, chipData }) {
  const radar = useMemo(() => getTechRadar(rows, chipData), [rows, chipData]);
  const score = useMemo(() => getOverallScore(rows, chipData), [rows, chipData]);
  const trend = score>=75?"強勢多頭":score>=60?"偏多觀察":score>=45?"中性盤整":score>=30?"偏空觀望":"強勢空頭";
  const tc = score>=60?"#ef4444":score>=45?"#f59e0b":"#22c55e";
  if (!radar.dims.length) return null;
  return (
    <Card title="技術強度雷達" icon="📡">
      <ScoreBadge score={score} />
      <div style={{ display:"grid", gap:7 }}>
        {radar.dims.map(d => (
          <div key={d.label}>
            <div style={{ display:"flex", justifyContent:"space-between", fontSize:12, marginBottom:3 }}>
              <span style={{ color:"#94a3b8" }}>{d.label}</span>
              <span style={{ color:d.color, fontWeight:700 }}>{Math.round(d.value)}</span>
            </div>
            <div style={{ height:5, borderRadius:3, background:"#1e293b" }}>
              <div style={{ height:"100%", borderRadius:3, width:`${Math.min(100,Math.max(3,d.value))}%`, background:d.color, transition:"width .5s" }} />
            </div>
          </div>
        ))}
      </div>
      <ConclusionLine text={`綜合評分 ${score} 分，當前研判：${trend}，${score>=60?"技術面偏多，可積極持股。":score>=45?"多空均衡，等待突破。":"技術面偏空，建議降低持倉。"}`} color={tc} />
    </Card>
  );
}

/* ── Card: 均線排列 ───────────────────────────────────────────────── */
function MaStatusCard({ rows }) {
  const ma = useMemo(() => getMaStatus(rows), [rows]);
  const latest = rows?.at(-1) || {};
  return (
    <Card title="均線排列" icon="📊">
      <div style={{ textAlign:"center", marginBottom:10 }}>
        <span style={{ fontSize:20, fontWeight:900, color:ma.color, padding:"4px 14px", borderRadius:6, background:ma.color+"22" }}>{ma.label}</span>
      </div>
      {[["MA5","ma5","#facc15"],["MA10","ma10","#fb923c"],["MA20","ma20","#38bdf8"],["MA60","ma60","#a78bfa"]].map(([label,key,c])=>(
        <Row key={label} label={label} value={fmt(ma[key])} color={c} />
      ))}
      {ma.ma20 && <Row label="收盤 vs MA20" value={`${((latest.close/ma.ma20-1)*100).toFixed(1)}%`} color={latest.close>ma.ma20?"#ef4444":"#22c55e"} />}
      {ma.goldenCross && <Row label="訊號" value="✅ 黃金交叉" color="#f59e0b" />}
      {ma.deathCross  && <Row label="訊號" value="❌ 死亡交叉" color="#22c55e" />}
      <ConclusionLine text={ma.conclusion} color={ma.color} />
    </Card>
  );
}

/* ── Card: 量價矩陣 ───────────────────────────────────────────────── */
function VolPriceCard({ rows }) {
  const vol = useMemo(() => getVolPriceMatrix(rows), [rows]);
  const latest = rows?.at(-1) || {};
  const prev   = rows?.at(-2) || {};
  const chg = prev.close ? (latest.close-prev.close)/prev.close*100 : 0;
  const isNormal = vol.type === "量能正常";
  const matrix = [
    { label:"量增價漲", color:"#ef4444", bg:"rgba(239,68,68,.18)", active:vol.type==="量增價漲", desc:"強勢買盤" },
    { label:"量增價跌", color:"#22c55e", bg:"rgba(34,197,94,.18)",  active:vol.type==="量增價跌", desc:"賣壓沉重" },
    { label:"量縮價漲", color:"#f59e0b", bg:"rgba(245,158,11,.18)", active:vol.type==="量縮價漲", desc:"謹慎無量" },
    { label:"量縮價跌", color:"#64748b", bg:"rgba(100,116,139,.18)",active:vol.type==="量縮價跌", desc:"無力下跌" },
  ];
  return (
    <Card title="量價矩陣" icon="📦">
      {/* Current state badge */}
      <div style={{ textAlign:"center", marginBottom:8 }}>
        <span style={{ fontSize:16, fontWeight:900, color:vol.color, padding:"3px 16px", borderRadius:6, background:vol.color+"22", border:`1px solid ${vol.color}55` }}>
          {vol.type}
        </span>
      </div>
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:5, marginBottom:10 }}>
        {matrix.map(m => (
          <div key={m.label} style={{ padding:"7px 8px", borderRadius:5, textAlign:"center",
            background: m.active ? m.bg : "rgba(30,41,59,.6)",
            border: m.active ? `2px solid ${m.color}` : "1px solid rgba(148,163,184,.08)",
            opacity: m.active ? 1 : (isNormal ? 0.35 : 0.28),
          }}>
            <div style={{ fontSize:11, fontWeight:m.active?800:400, color:m.active?m.color:"#475569" }}>{m.label}</div>
            <div style={{ fontSize:10, color:m.active?m.color+"cc":"#334155", marginTop:1 }}>{m.desc}</div>
          </div>
        ))}
      </div>
      <Row label="量比（今/5日均）" value={`${vol.volRatio.toFixed(2)}x`} color={vol.volRatio>1.3?"#ef4444":vol.volRatio<0.7?"#22c55e":"#94a3b8"} />
      <Row label="今日漲跌" value={`${chg>=0?"+":""}${chg.toFixed(2)}%`} color={chg>=0?"#ef4444":"#22c55e"} />
      <ConclusionLine text={vol.conclusion} color={vol.color} />
    </Card>
  );
}

/* ── Card: 型態偵測 ───────────────────────────────────────────────── */
function PatternCard({ rows }) {
  const pat = useMemo(() => detectPatterns(rows), [rows]);
  const typeColor = { bull:"#ef4444", bear:"#22c55e", neutral:"#94a3b8" };
  const typeBg    = { bull:"rgba(239,68,68,.08)", bear:"rgba(34,197,94,.08)", neutral:"rgba(148,163,184,.06)" };
  const cc = pat.bullCount>0&&pat.bearCount===0?"#ef4444":pat.bearCount>0?"#22c55e":"#94a3b8";
  return (
    <Card title="型態偵測" icon="🔍">
      <div style={{ display:"grid", gap:5, marginBottom:6 }}>
        {pat.patterns.map((p,i) => (
          <div key={i} style={{ padding:"7px 10px", borderRadius:4, background:typeBg[p.type], borderLeft:`3px solid ${typeColor[p.type]}` }}>
            <div style={{ fontWeight:700, color:typeColor[p.type], fontSize:13 }}>{p.label}</div>
            <div style={{ color:"#94a3b8", fontSize:11, marginTop:2 }}>{p.desc}</div>
          </div>
        ))}
      </div>
      <ConclusionLine text={pat.conclusion} color={cc} />
    </Card>
  );
}

/* ── Card: 籌碼透視 ───────────────────────────────────────────────── */
function ChipXrayCard({ chipData }) {
  const chip = useMemo(() => getChipAnalysis(chipData), [chipData]);
  const cc = chip.foreign5d>0&&chip.trust5d>0?"#ef4444":chip.foreign5d<0&&chip.trust5d<0?"#22c55e":"#94a3b8";
  return (
    <Card title="籌碼透視" icon="🏦">
      <div style={{ display:"flex", gap:8, marginBottom:10 }}>
        <div style={{ flex:1, textAlign:"center", padding:"8px 4px", borderRadius:6, background:chip.score>60?"rgba(239,68,68,.1)":chip.score<40?"rgba(34,197,94,.1)":"rgba(148,163,184,.08)" }}>
          <div style={{ fontSize:26, fontWeight:900, color:chip.score>60?"#ef4444":chip.score<40?"#22c55e":"#f59e0b" }}>{chip.score}</div>
          <div style={{ fontSize:10, color:"#64748b" }}>籌碼評分</div>
        </div>
        <div style={{ flex:2 }}>
          <Row label="外資連續" value={chip.foreignStreak>0?`連買${chip.foreignStreak}天`:chip.foreignStreak<0?`連賣${-chip.foreignStreak}天`:"中性"} color={chip.foreignStreak>0?"#ef4444":chip.foreignStreak<0?"#22c55e":"#94a3b8"} />
          <Row label="投信連續" value={chip.trustStreak>0?`連買${chip.trustStreak}天`:"中性"} color={chip.trustStreak>0?"#ef4444":"#94a3b8"} />
          <Row label="狀態"     value={chip.status} />
        </div>
      </div>
      {[["外資近5日", chip.foreign5d],["投信近5日", chip.trust5d]].map(([label,val])=>(
        <div key={label} style={{ marginBottom:7 }}>
          <div style={{ display:"flex", justifyContent:"space-between", fontSize:12, marginBottom:2 }}>
            <span style={{ color:"#94a3b8" }}>{label}</span>
            <span style={{ color:val>0?"#ef4444":"#22c55e", fontWeight:700 }}>{val>=0?"+":""}{(val/1000).toFixed(0)}千張</span>
          </div>
          <div style={{ height:5, borderRadius:3, background:"#1e293b" }}>
            <div style={{ height:"100%", borderRadius:3, width:`${Math.min(100,Math.abs(val)/500*100)}%`, background:val>0?"#ef4444":"#22c55e" }} />
          </div>
        </div>
      ))}
      <Row label="融資餘額" value={fmt(chip.latestChip.margin_balance,0)} />
      <Row label="融券餘額" value={fmt(chip.latestChip.short_balance,0)} />
      <ConclusionLine text={chip.conclusion} color={cc} />
    </Card>
  );
}

/* ── Card: 法人黑K偵測 ────────────────────────────────────────────── */
function BlackCandleCard({ rows, chipData }) {
  const bc = useMemo(() => detectBlackCandleAccum(rows, chipData), [rows, chipData]);
  return (
    <Card title="法人黑K偵測" icon="🕯️">
      <div style={{ textAlign:"center", marginBottom:10 }}>
        <span style={{ fontSize:17, fontWeight:800, color:bc.color, padding:"4px 12px", borderRadius:6, background:bc.color+"22" }}>{bc.signal}</span>
      </div>
      <Row label="K棒形態"     value={bc.isBlack?"黑K（收跌）":"紅K（收漲）"} color={bc.isBlack?"#22c55e":"#ef4444"} />
      <Row label="收盤 vs 昨收" value={bc.closeAbovePrev?"↑ 高於昨收":"↓ 低於昨收"} color={bc.closeAbovePrev?"#ef4444":"#22c55e"} />
      <Row label="法人買超"    value={bc.instBuy?"✅ 是":"❌ 否"} color={bc.instBuy?"#ef4444":"#94a3b8"} />
      <Row label="當日漲跌幅"  value={`${bc.changeRate>=0?"+":""}${bc.changeRate.toFixed(2)}%`} color={bc.changeRate>=0?"#ef4444":"#22c55e"} />
      {bc.isAccum && (
        <div style={{ padding:"6px 10px", borderRadius:4, background:"rgba(245,158,11,.12)", border:"1px solid rgba(245,158,11,.3)", color:"#fbbf24", fontSize:12, marginTop:8 }}>
          ⭐ 發現「法人黑K吸籌」訊號 — 主力洗盤吸籌的典型形態
        </div>
      )}
      <ConclusionLine text={bc.conclusion} color={bc.color} />
    </Card>
  );
}

/* ── Card: 動能指標（RSI + MACD + 布林）─────────────────────────────── */
function MomentumCard({ rows }) {
  const rsi  = useMemo(() => getRsiAnalysis(rows),  [rows]);
  const macd = useMemo(() => getMacdAnalysis(rows), [rows]);
  const latest = rows?.at(-1) || {};
  return (
    <Card title="動能指標" icon="⚡">
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:6, marginBottom:10 }}>
        <div style={{ padding:"8px 6px", borderRadius:6, background:"rgba(148,163,184,.06)", textAlign:"center" }}>
          <div style={{ fontSize:10, color:"#64748b" }}>RSI14</div>
          <div style={{ fontSize:26, fontWeight:900, color:rsi.color }}>{rsi.rsi?rsi.rsi.toFixed(1):"--"}</div>
          <div style={{ fontSize:11, color:rsi.color }}>{rsi.status}</div>
          {rsi.rsi&&<div style={{ marginTop:4, height:4, borderRadius:2, background:"#1e293b" }}><div style={{ height:"100%", width:`${rsi.rsi}%`, background:rsi.color, borderRadius:2 }} /></div>}
        </div>
        <div style={{ padding:"8px 6px", borderRadius:6, background:"rgba(148,163,184,.06)", textAlign:"center" }}>
          <div style={{ fontSize:10, color:"#64748b" }}>MACD柱</div>
          <div style={{ fontSize:22, fontWeight:900, color:macd.color }}>{macd.hist!=null?fmt(macd.hist,3):"--"}</div>
          <div style={{ fontSize:11, color:macd.color }}>{macd.status}</div>
        </div>
      </div>
      <Row label="布林寬度" value={fmt(latest.bb_width,4)} color={latest.bb_width<0.02?"#f59e0b":"#94a3b8"} />
      {Number.isFinite(latest.bb_width) && latest.bb_width < 0.02 && <Row label="布林狀態" value="🗜️ 極度收縮（大波動蓄勢）" color="#f59e0b" />}
      <div style={{ marginTop:6 }}>
        <div style={{ fontSize:10, color:"#64748b", marginBottom:3 }}>RSI</div>
        <ConclusionLine text={rsi.conclusion} color={rsi.color} />
      </div>
      <div style={{ marginTop:4 }}>
        <div style={{ fontSize:10, color:"#64748b", marginBottom:3 }}>MACD</div>
        <ConclusionLine text={macd.conclusion} color={macd.color} />
      </div>
    </Card>
  );
}

/* ── Card: 風險雙鏡（斷頭 / 軋空）────────────────────────────────── */
function RiskMirrorCard({ rows, chipData }) {
  const risk = useMemo(() => getRiskMetrics(rows, chipData), [rows, chipData]);
  const cc = risk.isLongRisk?"#ef4444":risk.isShortSqueeze?"#f59e0b":"#22c55e";
  return (
    <Card title="風險雙鏡" icon="⚠️">
      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:6, marginBottom:10 }}>
        <div style={{ padding:"10px 6px", borderRadius:6, textAlign:"center", background:risk.isLongRisk?"rgba(239,68,68,.12)":"rgba(34,197,94,.07)", border:`1px solid ${risk.isLongRisk?"#ef444455":"#22c55e44"}` }}>
          <div style={{ fontSize:10, color:"#64748b", marginBottom:3 }}>融資斷頭</div>
          <div style={{ fontSize:13, fontWeight:800, color:risk.isLongRisk?"#ef4444":"#22c55e" }}>{risk.isLongRisk?"⚠️ 警戒":"✅ 安全"}</div>
        </div>
        <div style={{ padding:"10px 6px", borderRadius:6, textAlign:"center", background:risk.isShortSqueeze?"rgba(245,158,11,.12)":"rgba(34,197,94,.07)", border:`1px solid ${risk.isShortSqueeze?"#f59e0b55":"#22c55e44"}` }}>
          <div style={{ fontSize:10, color:"#64748b", marginBottom:3 }}>軋空預兆</div>
          <div style={{ fontSize:13, fontWeight:800, color:risk.isShortSqueeze?"#f59e0b":"#22c55e" }}>{risk.isShortSqueeze?"🎯 偵測到":"✅ 無"}</div>
        </div>
      </div>
      <Row label="融資餘額" value={fmt(risk.marginBalance,0)} />
      <Row label="融券餘額" value={fmt(risk.shortBalance,0)} />
      {risk.shortRatio!=null && <Row label="券資比" value={`${risk.shortRatio.toFixed(1)}%`} color={risk.shortRatio>30?"#f59e0b":"#94a3b8"} />}
      <Row label="跌破MA60" value={risk.belowMa60?"是 ⚠️":"否"} color={risk.belowMa60?"#ef4444":"#22c55e"} />
      <ConclusionLine text={risk.conclusion} color={cc} />
    </Card>
  );
}

/* ── Card: 情境機率 ───────────────────────────────────────────────── */
function ScenarioCard({ rows, chipData }) {
  const sc = useMemo(() => getScenarios(rows, chipData), [rows, chipData]);
  const cc = sc.bull>sc.bear?"#ef4444":sc.bear>sc.bull+10?"#22c55e":"#f59e0b";
  return (
    <Card title="情境機率" icon="🎲">
      <div style={{ marginBottom:10 }}>
        <div style={{ display:"flex", justifyContent:"space-between", fontSize:11, color:"#64748b", marginBottom:3 }}>
          <span>空頭 {sc.bear}%</span>
          <span>中性 {sc.neutral}%</span>
          <span>多頭 {sc.bull}%</span>
        </div>
        <div style={{ height:10, borderRadius:6, overflow:"hidden", display:"flex" }}>
          <div style={{ width:`${sc.bear}%`,    background:"#22c55e" }} />
          <div style={{ width:`${sc.neutral}%`, background:"#475569" }} />
          <div style={{ width:`${sc.bull}%`,    background:"#ef4444" }} />
        </div>
      </div>
      {[
        { label:"多頭走強", prob:sc.bull,    color:"#ef4444", bg:"rgba(239,68,68,.07)",  desc:"有效突破，法人加碼，放量上攻" },
        { label:"中性整理", prob:sc.neutral, color:"#f59e0b", bg:"rgba(245,158,11,.07)", desc:"均線纏繞，量能萎縮，等待方向" },
        { label:"空頭走弱", prob:sc.bear,    color:"#22c55e", bg:"rgba(34,197,94,.07)",  desc:"跌破支撐，法人出場，量增下跌" },
      ].map(s => (
        <div key={s.label} style={{ display:"flex", alignItems:"center", gap:8, marginBottom:5, padding:"6px 8px", borderRadius:4, background:s.bg }}>
          <div style={{ width:42, textAlign:"center", fontWeight:900, fontSize:17, color:s.color }}>{s.prob}%</div>
          <div>
            <div style={{ fontSize:12, fontWeight:700, color:s.color }}>{s.label}</div>
            <div style={{ fontSize:10, color:"#64748b" }}>{s.desc}</div>
          </div>
        </div>
      ))}
      <ConclusionLine text={sc.conclusion} color={cc} />
    </Card>
  );
}

/* ── Card: Groq AI 總結 ────────────────────────────────────────────── */
function GroqSummaryCard({ stockCode, rows, chipData }) {
  const [groqText, setGroqText] = useState("");
  const [groqLoading, setGroqLoading] = useState(false);
  const [groqError, setGroqError] = useState("");
  const [groqMeta, setGroqMeta] = useState(null);
  const score = useMemo(() => getOverallScore(rows, chipData), [rows, chipData]);
  const ma    = useMemo(() => getMaStatus(rows), [rows]);
  const vol   = useMemo(() => getVolPriceMatrix(rows), [rows]);
  const chip  = useMemo(() => getChipAnalysis(chipData), [chipData]);
  const pat   = useMemo(() => detectPatterns(rows), [rows]);
  const risk  = useMemo(() => getRiskMetrics(rows, chipData), [rows, chipData]);
  const sc    = useMemo(() => getScenarios(rows, chipData), [rows, chipData]);
  const scoreColor = score>=65?"#ef4444":score>=45?"#f59e0b":"#22c55e";

  async function runGroq() {
    if (!stockCode || groqLoading || !rows.length) return;
    setGroqLoading(true); setGroqText(""); setGroqError(""); setGroqMeta(null);
    try {
      const res = await fetch(`${API}/api/ai/groq/${encodeURIComponent(stockCode)}`, { cache:"no-store" });
      const json = await res.json();
      if (json.error) { setGroqError(json.error); return; }
      setGroqText(json.analysis||"");
      setGroqMeta({ model:json.model, tokens:json.tokens_used, rows:json.data_rows });
    } catch (e) { setGroqError(e.message||"連線失敗"); }
    finally { setGroqLoading(false); }
  }

  const summary = [
    { label:"綜合評分",  value:`${score} 分`,             color:scoreColor },
    { label:"均線狀態",  value:ma.label,                  color:ma.color },
    { label:"量價矩陣",  value:vol.type,                  color:vol.color },
    { label:"籌碼健康",  value:`${chip.score}分 ${chip.status}`, color:chip.score>60?"#ef4444":"#22c55e" },
    { label:"型態偵測",  value:pat.bullCount>0?`多頭×${pat.bullCount}`:pat.bearCount>0?`空頭×${pat.bearCount}`:"區間整理", color:pat.bullCount>0?"#ef4444":pat.bearCount>0?"#22c55e":"#94a3b8" },
    { label:"多頭機率",  value:`${sc.bull}%`,             color:"#ef4444" },
    { label:"空頭機率",  value:`${sc.bear}%`,             color:"#22c55e" },
    { label:"風險警示",  value:risk.isLongRisk?"斷頭警戒":risk.isShortSqueeze?"軋空預兆":"無異常", color:risk.isLongRisk?"#ef4444":risk.isShortSqueeze?"#f59e0b":"#22c55e" },
  ];

  return (
    <Card title="Groq AI 總結分析" icon="🤖">
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:6, marginBottom:14 }}>
        {summary.map(item => (
          <div key={item.label} style={{ padding:"7px 10px", borderRadius:4, background:"rgba(148,163,184,.06)", display:"flex", justifyContent:"space-between" }}>
            <span style={{ color:"#64748b", fontSize:12 }}>{item.label}</span>
            <b style={{ color:item.color, fontSize:12 }}>{item.value}</b>
          </div>
        ))}
      </div>
      <button type="button" onClick={runGroq} disabled={groqLoading||!rows.length}
        style={{ width:"100%", padding:"12px 0", borderRadius:6, border:0, background:groqLoading?"#1e293b":"linear-gradient(90deg,#7c3aed,#2563eb)", color:"white", fontWeight:700, cursor:groqLoading||!rows.length?"default":"pointer", fontSize:15, marginBottom:12 }}>
        {groqLoading?"⏳ Groq AI 分析中...":"⚡ 執行 Groq AI 綜合分析（llama-3.3-70b）"}
      </button>
      {groqError && <div style={{ padding:10, borderRadius:6, background:"rgba(239,68,68,.1)", color:"#fca5a5", fontSize:13, marginBottom:10 }}>❌ {groqError}</div>}
      {groqText && (
        <div style={{ padding:14, borderRadius:6, background:"rgba(124,58,237,.08)", border:"1px solid rgba(124,58,237,.25)", fontSize:13, lineHeight:1.9, whiteSpace:"pre-wrap" }}>
          {groqText}
        </div>
      )}
      {groqMeta && <div style={{ color:"#475569", fontSize:11, marginTop:8 }}>模型：{groqMeta.model}・Token：{groqMeta.tokens}・資料筆數：{groqMeta.rows}</div>}
      <div style={{ marginTop:10, padding:8, borderRadius:4, background:"rgba(148,163,184,.06)", color:"#64748b", fontSize:11, lineHeight:1.6 }}>
        ⚠️ 本分析為量化指標與 AI 參考，不構成投資建議。操作前請自行研判風險。
      </div>
    </Card>
  );
}

/* ── OrderBook ────────────────────────────────────────────────────── */
function OrderBook({ bids=[], asks=[] }) {
  const n = Math.max(bids.length, asks.length, 1);
  return (
    <table style={{ width:"100%", fontSize:13, borderCollapse:"collapse" }}>
      <thead><tr>
        <th style={{ color:"#22c55e", textAlign:"right", paddingRight:6 }}>委買量</th>
        <th style={{ color:"#22c55e", textAlign:"right", paddingRight:6 }}>買價</th>
        <th style={{ color:"#ef4444", textAlign:"left",  paddingLeft:6  }}>賣價</th>
        <th style={{ color:"#ef4444", textAlign:"left",  paddingLeft:6  }}>委賣量</th>
      </tr></thead>
      <tbody>{Array.from({length:n}).map((_,i)=>{
        const b=bids[i]||{}, a=asks[i]||{};
        return (
          <tr key={i} style={{ borderBottom:"1px solid rgba(148,163,184,.08)" }}>
            <td style={{ textAlign:"right", padding:"3px 6px 3px 0", color:"#22c55e" }}>{b.qty!=null?b.qty.toLocaleString():""}</td>
            <td style={{ textAlign:"right", paddingRight:6, color:"#22c55e", fontWeight:700 }}>{b.price!=null?fmt(b.price):""}</td>
            <td style={{ textAlign:"left",  paddingLeft:6,  color:"#ef4444", fontWeight:700 }}>{a.price!=null?fmt(a.price):""}</td>
            <td style={{ textAlign:"left",  paddingLeft:6,  color:"#ef4444" }}>{a.qty!=null?a.qty.toLocaleString():""}</td>
          </tr>
        );
      })}</tbody>
    </table>
  );
}

/* ══════════════════════════════════════════════════════════════════
   SideNav
   ══════════════════════════════════════════════════════════════════ */
function SideNav({ page, setPage, open, setOpen }) {
  const items = [
    { id:"dashboard", icon:"📈", label:"個股分析" },
    { id:"ai",        icon:"🤖", label:"AI 選股" },
  ];
  return (
    <div style={{ width:open?190:52, minHeight:"100vh", background:"#0f172a", borderRight:"1px solid #1e293b",
      flexShrink:0, transition:"width .2s", overflow:"hidden", display:"flex", flexDirection:"column", position:"sticky", top:0 }}>
      <div onClick={()=>setOpen(o=>!o)}
        style={{ padding:"14px 0", textAlign:"center", cursor:"pointer", color:"#475569", fontSize:15,
          borderBottom:"1px solid #1e293b", userSelect:"none", flexShrink:0 }}>
        {open?"◀":"▶"}
      </div>
      {items.map(it=>(
        <div key={it.id} onClick={()=>setPage(it.id)}
          style={{ display:"flex", alignItems:"center", gap:10, padding:"13px 14px", cursor:"pointer",
            background:page===it.id?"rgba(37,99,235,.15)":"transparent",
            borderLeft:page===it.id?"3px solid #2563eb":"3px solid transparent",
            transition:"background .15s" }}>
          <span style={{ fontSize:20, flexShrink:0 }}>{it.icon}</span>
          {open&&<span style={{ fontSize:13, fontWeight:600, color:"#f1f5f9", whiteSpace:"nowrap" }}>{it.label}</span>}
        </div>
      ))}
    </div>
  );
}

/* ══════════════════════════════════════════════════════════════════
   AIChatPage
   ══════════════════════════════════════════════════════════════════ */
function AIChatPage() {
  const [messages, setMessages] = useState([{
    role:"assistant",
    content:"您好！我是 AI 選股助理。\n請描述您想找的股票條件，例如：\n• 找近期突破月線的強勢股\n• 推薦技術面黃金交叉的股票\n• 哪些股票量增價漲且籌碼集中？"
  }]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  const quickActions = [
    "找近期突破月線的強勢股",
    "推薦技術面黃金交叉的股票",
    "哪些股票量增價漲？",
    "找 RSI 超賣可能反彈的股票",
  ];

  useEffect(()=>{ bottomRef.current?.scrollIntoView({behavior:"smooth"}); },[messages, loading]);

  function compressHistory(msgs) {
    if (msgs.length <= 8) return msgs;
    const old = msgs.slice(0, -8).filter(m=>m.role!=="system");
    const recent = msgs.slice(-8);
    const summary = old.map(m=>`[${m.role}] ${String(m.content).slice(0,100)}`).join(" | ");
    return [{ role:"system", content:`Prior conversation (compressed): ${summary}` }, ...recent];
  }

  async function send(text) {
    const content = (text||input).trim();
    if (!content||loading) return;
    setInput("");
    const newMsgs = [...messages, {role:"user", content}];
    setMessages(newMsgs);
    setLoading(true);
    try {
      const res = await fetch(`${API}/api/ai/stock-picker`, {
        method:"POST", headers:{"Content-Type":"application/json"},
        body:JSON.stringify({messages:compressHistory(newMsgs)}), cache:"no-store",
      });
      const json = await res.json();
      const reply = json.reply ?? json.error ?? "";
      setMessages(prev=>[...prev,{role:"assistant", content:reply||"AI 未回傳內容，請再試一次"}]);
    } catch(e) {
      setMessages(prev=>[...prev,{role:"assistant", content:`連線失敗：${e.message}`}]);
    }
    setLoading(false);
  }

  return (
    <div style={{ display:"flex", flexDirection:"column", height:"100vh", background:"#020617", color:"#f1f5f9" }}>
      <div style={{ padding:"14px 20px", borderBottom:"1px solid #1e293b", background:"#0f172a", flexShrink:0 }}>
        <div style={{ color:"#38bdf8", fontSize:11, fontWeight:800, letterSpacing:1 }}>TW STOCK DECISION SYSTEM</div>
        <div style={{ fontSize:18, fontWeight:900, marginTop:4 }}>🤖 AI 選股助理</div>
      </div>
      <div style={{ padding:"8px 14px", display:"flex", gap:8, flexWrap:"wrap", borderBottom:"1px solid #1e293b", flexShrink:0 }}>
        {quickActions.map(q=>(
          <button key={q} onClick={()=>send(q)} disabled={loading}
            style={{ padding:"5px 12px", borderRadius:20, border:"1px solid #334155", background:"#1e293b",
              color:"#94a3b8", cursor:"pointer", fontSize:12, whiteSpace:"nowrap" }}>{q}</button>
        ))}
      </div>
      <div style={{ flex:1, overflow:"auto", padding:"12px 16px" }}>
        {messages.map((m,i)=>(
          <div key={i} style={{ margin:"10px 0", display:"flex", justifyContent:m.role==="user"?"flex-end":"flex-start" }}>
            <div style={{ maxWidth:"82%", padding:"10px 14px", borderRadius:12,
              background:m.role==="user"?"#1d4ed8":"#1e293b", color:"#f1f5f9", fontSize:13,
              lineHeight:1.8, whiteSpace:"pre-wrap" }}>
              {m.role==="assistant"&&<div style={{ color:"#38bdf8", fontSize:11, marginBottom:4, fontWeight:700 }}>🤖 AI 助理</div>}
              {m.content}
            </div>
          </div>
        ))}
        {loading&&(
          <div style={{ display:"flex", justifyContent:"flex-start", margin:"10px 0" }}>
            <div style={{ padding:"10px 14px", borderRadius:12, background:"#1e293b", color:"#64748b", fontSize:13 }}>
              ⏳ AI 分析中，正在查詢資料庫...
            </div>
          </div>
        )}
        <div ref={bottomRef}/>
      </div>
      <div style={{ padding:"12px 16px", borderTop:"1px solid #1e293b", background:"#0f172a",
        display:"flex", gap:8, flexShrink:0 }}>
        <input value={input} onChange={e=>setInput(e.target.value)}
          onKeyDown={e=>e.key==="Enter"&&!e.shiftKey&&send()} disabled={loading}
          placeholder="問 AI 推薦適合的股票… (Enter 送出)"
          style={{ flex:1, padding:"10px 14px", borderRadius:8, border:"1px solid #334155",
            background:"#020617", color:"white", fontSize:14 }} />
        <button onClick={()=>send()} disabled={loading||!input.trim()}
          style={{ padding:"10px 20px", borderRadius:8, background:loading?"#1e293b":"#2563eb",
            color:"white", border:0, cursor:loading?"default":"pointer", fontWeight:700, fontSize:14 }}>
          送出
        </button>
        {messages.length>2&&(
          <button onClick={()=>setMessages([messages[0]])}
            style={{ padding:"10px 14px", borderRadius:8, border:"1px solid #334155",
              background:"transparent", color:"#475569", cursor:"pointer", fontSize:12 }}>
            清除
          </button>
        )}
      </div>
    </div>
  );
}

/* ══════════════════════════════════════════════════════════════════
   FundamentalsCard
   ══════════════════════════════════════════════════════════════════ */
function FundamentalsCard({ stockCode }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(()=>{
    if (!stockCode) return;
    setData(null); setLoading(true);
    fetch(`${API}/api/fundamentals/${encodeURIComponent(stockCode)}`,{cache:"no-store"})
      .then(r=>r.json()).then(j=>setData(j)).catch(()=>{}).finally(()=>setLoading(false));
  },[stockCode]);

  const pct=(v,d=1)=>v!=null?`${v>=0?"+":""}${fmt(v,d)}%`:"--";

  const items = data ? [
    {label:"本益比 PE",    value:data.pe_ratio!=null?fmt(data.pe_ratio,1):"--",
      color:data.pe_ratio&&data.pe_ratio<15?"#22c55e":data.pe_ratio&&data.pe_ratio>30?"#ef4444":"#f1f5f9"},
    {label:"殖利率",       value:data.dividend_yield!=null?`${fmt(data.dividend_yield,2)}%`:"--", color:"#f59e0b"},
    {label:"股價淨值比 PB",value:data.pb_ratio!=null?fmt(data.pb_ratio,2):"--"},
    {label:"EPS 估算",     value:data.eps_est!=null?fmt(data.eps_est,2):"--"},
    {label:"月營收 YOY",   value:pct(data.revenue_yoy), color:data.revenue_yoy>0?"#ef4444":data.revenue_yoy<0?"#22c55e":"#f1f5f9"},
    {label:"月營收 MOM",   value:pct(data.revenue_mom), color:data.revenue_mom>0?"#ef4444":data.revenue_mom<0?"#22c55e":"#f1f5f9"},
  ] : [];

  return (
    <Card title="個股基本面" icon="📊">
      {loading&&<div style={{color:"#64748b",fontSize:13}}>載入中...</div>}
      {!loading&&(!data||data.error)&&<div style={{color:"#475569",fontSize:12}}>{data?.error||"基本面資料暫無（僅支援上市股票）"}</div>}
      {!loading&&data&&!data.error&&(
        <>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginBottom:8}}>
            {items.map(it=>(
              <div key={it.label} style={{padding:"8px 10px",borderRadius:4,background:"rgba(148,163,184,.06)"}}>
                <div style={{color:"#64748b",fontSize:11,marginBottom:2}}>{it.label}</div>
                <div style={{color:it.color||"#f1f5f9",fontWeight:700,fontSize:15}}>{it.value}</div>
              </div>
            ))}
          </div>
          <div style={{color:"#475569",fontSize:11}}>資料來源：TWSE・{data.data_date||"--"}</div>
        </>
      )}
    </Card>
  );
}

/* ══════════════════════════════════════════════════════════════════
   Main App
   ══════════════════════════════════════════════════════════════════ */
function addSeries(chart, Type, opts, fallback) {
  return typeof chart.addSeries==="function"&&Type ? chart.addSeries(Type,opts) : chart[fallback](opts);
}

export default function App() {
  const mainRef = useRef(null);
  const rsiRef  = useRef(null);
  const macdRef = useRef(null);
  const chartsRef  = useRef({});
  const seriesRef  = useRef({});
  const syncingRef = useRef(false);

  const [page,            setPage]           = useState("dashboard");
  const [sideOpen,        setSideOpen]       = useState(true);
  const [input,           setInput]          = useState("2330");
  const [stock,           setStock]          = useState(resolveStock("2330"));
  const [loadKey,         setLoadKey]        = useState(0);
  const [openSuggest,     setOpenSuggest]    = useState(false);
  const [payload,         setPayload]        = useState(null);
  const [rows,            setRows]           = useState([]);
  const [chip,            setChip]           = useState(null);
  const [analysis,        setAnalysis]       = useState(null);
  const [realtime,        setRealtime]       = useState(null);
  const [status,          setStatus]         = useState("載入中...");
  const [lastRefresh,     setLastRefresh]    = useState(null);
  const [isLive,          setIsLive]         = useState(isTradingSession);
  const [hovered,         setHovered]        = useState(null);
  const [suggestions,     setSuggestions]    = useState([]);
  const [backfillAttempt, setBackfillAttempt]= useState(0);
  const searchTimerRef = useRef(null);

  /* ── Chart init ────────────────────────────────────────────────── */
  useEffect(() => {
    if (!mainRef.current||!rsiRef.current||!macdRef.current) return;
    const theme = {
      layout:       { background:{color:"#0f172a"}, textColor:"#dbeafe" },
      grid:         { vertLines:{color:"#1e293b"}, horzLines:{color:"#1e293b"} },
      timeScale:    { timeVisible:false, secondsVisible:false, borderColor:"#334155" },
      rightPriceScale:{ borderColor:"#334155" },
      crosshair:    { mode:1 },
      autoSize:     true,
    };
    const main = createChart(mainRef.current, { ...theme, height:150 });
    const rsi  = createChart(rsiRef.current,  { ...theme, height:70  });
    const macd = createChart(macdRef.current, { ...theme, height:70  });
    chartsRef.current = { main, rsi, macd };

    const candle = addSeries(main, CandlestickSeries, { upColor:"#ef4444", downColor:"#22c55e", borderUpColor:"#ef4444", borderDownColor:"#22c55e", wickUpColor:"#ef4444", wickDownColor:"#22c55e" }, "addCandlestickSeries");
    const volume = addSeries(main, HistogramSeries, { priceFormat:{type:"volume"}, priceScaleId:"vol" }, "addHistogramSeries");
    try { main.priceScale("vol").applyOptions({ scaleMargins:{top:0.78,bottom:0}, visible:false }); } catch(_) {}
    const ma5s  = addSeries(main, LineSeries, { color:"#facc15", lineWidth:1, lastValueVisible:false, priceLineVisible:false }, "addLineSeries");
    const ma10s = addSeries(main, LineSeries, { color:"#fb923c", lineWidth:1, lastValueVisible:false, priceLineVisible:false }, "addLineSeries");
    const ma20s = addSeries(main, LineSeries, { color:"#38bdf8", lineWidth:1, lastValueVisible:false, priceLineVisible:false }, "addLineSeries");
    const ma60s = addSeries(main, LineSeries, { color:"#a78bfa", lineWidth:1, lastValueVisible:false, priceLineVisible:false }, "addLineSeries");
    const bbUs  = addSeries(main, LineSeries, { color:"rgba(148,163,184,.3)", lineWidth:1, lineStyle:2, lastValueVisible:false, priceLineVisible:false }, "addLineSeries");
    const bbLs  = addSeries(main, LineSeries, { color:"rgba(148,163,184,.3)", lineWidth:1, lineStyle:2, lastValueVisible:false, priceLineVisible:false }, "addLineSeries");
    const rsiS  = addSeries(rsi,  LineSeries, { color:"#f59e0b", lineWidth:2 }, "addLineSeries");
    const macdS = addSeries(macd, HistogramSeries, {}, "addHistogramSeries");
    seriesRef.current = { candle, volume, ma5s, ma10s, ma20s, ma60s, bbUs, bbLs, rsiS, macdS };

    main.subscribeCrosshairMove(param => {
      if (!param.point||!param.time) { setHovered(null); return; }
      const c=param.seriesData?.get(candle), v=param.seriesData?.get(volume);
      if (c) setHovered({ time:param.time, open:c.open, high:c.high, low:c.low, close:c.close, volume:v?.value });
    });

    const syncRange=(src,targets)=>src.timeScale().subscribeVisibleLogicalRangeChange(range=>{
      if (syncingRef.current||!range) return;
      syncingRef.current=true;
      targets.forEach(t=>{try{t.timeScale().setVisibleLogicalRange(range);}catch(_){}});
      syncingRef.current=false;
    });
    syncRange(main,[rsi,macd]); syncRange(rsi,[main,macd]); syncRange(macd,[main,rsi]);

    return () => { Object.values(chartsRef.current).forEach(c=>{try{c.remove();}catch(_){}}); chartsRef.current={}; };
  }, []);

  /* ── Search ────────────────────────────────────────────────────── */
  useEffect(() => {
    const q = input.trim();
    if (!q) { setSuggestions([]); return; }
    clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(async () => {
      try {
        const res = await fetch(`${API}/api/search?q=${encodeURIComponent(q)}`, { cache:"no-store" });
        if (res.ok) { const j=await res.json(); setSuggestions(Array.isArray(j)?j.slice(0,8):[]); }
      } catch { setSuggestions([]); }
    }, 250);
    return () => clearTimeout(searchTimerRef.current);
  }, [input]);

  /* ── Feed chart data ───────────────────────────────────────────── */
  useEffect(() => {
    const s=seriesRef.current;
    if (!s.candle||!rows.length) return;
    s.candle.setData(rows.map(r=>({time:r.time,open:r.open,high:r.high,low:r.low,close:r.close})));
    s.volume.setData(rows.map(r=>({time:r.time,value:r.volume||0,color:r.close>=r.open?"rgba(239,68,68,.5)":"rgba(34,197,94,.5)"})));
    s.ma5s.setData(pickLine(rows,"ma5")); s.ma10s.setData(pickLine(rows,"ma10"));
    s.ma20s.setData(pickLine(rows,"ma20")); s.ma60s.setData(pickLine(rows,"ma60"));
    s.bbUs.setData(pickLine(rows,"bb_upper")); s.bbLs.setData(pickLine(rows,"bb_lower"));
    s.rsiS.setData(pickLine(rows,"rsi14"));
    s.macdS.setData(rows.filter(r=>Number.isFinite(r.macd_hist)).map(r=>({time:r.time,value:r.macd_hist,color:r.macd_hist>=0?"rgba(239,68,68,.8)":"rgba(34,197,94,.8)"})));
    Object.values(chartsRef.current).forEach(c=>c.timeScale().fitContent());
  }, [rows]);

  /* ── Live candle update ────────────────────────────────────────── */
  const fetchRealtimeDirect = useCallback(async (code) => {
    for (const prefix of ["tse","otc"]) {
      try {
        const url=`https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=${prefix}_${code}.tw&json=1&delay=0&_=${Date.now()}`;
        const res=await fetch(url,{headers:{Referer:`https://mis.twse.com.tw/stock/fibest.jsp?stock=${code}`},cache:"no-store"});
        const data=await res.json();
        const q=(data.msgArray||[])[0];
        if (!q) continue;
        const toNum=v=>{const n=parseFloat(String(v||"").replace(/,/g,"")); return isFinite(n)?n:null;};
        const price=toNum(q.z)??toNum(q.y);
        if (!price) continue;
        return { price, close:price, open:toNum(q.o)??price, high:toNum(q.h)??price, low:toNum(q.l)??price,
                 previous_close:toNum(q.y), volume_lot:parseInt(q.v)||0, time:q.t, name:q.n, source:"TWSE MIS (browser)" };
      } catch { continue; }
    }
    return null;
  }, []);

  useEffect(() => {
    if (!isLive||!rows.length) return;
    const code=stock.code, s=seriesRef.current;
    if (!s.candle) return;
    async function updateLiveCandle() {
      const rt=await fetchRealtimeDirect(code);
      if (!rt?.price) return;
      setRealtime(rt);
      const now=new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Taipei"}));
      const dateStr=`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
      try { s.candle.update({time:dateStr,open:rt.open,high:rt.high,low:rt.low,close:rt.price}); } catch {}
      try { s.volume.update({time:dateStr,value:(rt.volume_lot||0)*1000,color:rt.price>=rt.open?"rgba(239,68,68,.5)":"rgba(34,197,94,.5)"}); } catch {}
    }
    updateLiveCandle();
    const id=setInterval(updateLiveCandle, POLL_MS);
    return ()=>clearInterval(id);
  }, [isLive, rows.length, stock.code, fetchRealtimeDirect]);

  /* ── Fetch kline ───────────────────────────────────────────────── */
  const fetchKline = useCallback(async (code, isRefresh=false) => {
    try {
      const res=await fetch(`${API}/api/kline/${encodeURIComponent(code)}`, {cache:"no-store"});
      if (!res.ok) return;
      const json=await res.json();
      const nextRows=normalizeRows(json);
      setPayload(json); setRows(nextRows); setRealtime(json.realtime||null); setLastRefresh(new Date());
      if (nextRows.length>0) {
        if (!isRefresh) setStatus(`已載入 ${nextRows.length} 筆資料`);
        setBackfillAttempt(0);
      } else if (!isRefresh) {
        setBackfillAttempt(1);
        setStatus("⏳ 首次查詢，正在補回歷史資料（約 30～60 秒）...");
      }
    } catch(e) { if(!isRefresh) setStatus(`查詢失敗：${e?.message}`); }
  }, []);

  /* ── Initial load ──────────────────────────────────────────────── */
  useEffect(() => {
    let alive=true;
    setStatus(`⏳ 查詢 ${stock.code} 中...`);
    setRows([]); setPayload(null); setChip(null); setAnalysis(null); setRealtime(null); setHovered(null); setBackfillAttempt(0);
    fetchKline(stock.code);
    Promise.allSettled([
      fetch(`${API}/api/chip/${encodeURIComponent(stock.code)}?auto_init=false`, {cache:"no-store"}),
      fetch(`${API}/api/analysis/${encodeURIComponent(stock.code)}`, {cache:"no-store"}),
    ]).then(([chipRes,anaRes])=>{
      if (!alive) return;
      if (chipRes.status==="fulfilled"&&chipRes.value.ok) chipRes.value.json().then(j=>{if(alive)setChip(j);}).catch(()=>{});
      if (anaRes.status==="fulfilled"&&anaRes.value.ok) anaRes.value.json().then(j=>{if(alive)setAnalysis(j);}).catch(()=>{});
    });
    return ()=>{alive=false;};
  }, [stock.code, loadKey, fetchKline]);

  /* ── isLive poll ───────────────────────────────────────────────── */
  useEffect(() => {
    const id=setInterval(()=>setIsLive(isTradingSession()), POLL_MS);
    return ()=>clearInterval(id);
  }, []);

  /* ── Backfill ──────────────────────────────────────────────────── */
  async function triggerBackfill() {
    const code=stock.code;
    setStatus(`⏳ 手動補資料中（${code}）...`);
    try { await fetch(`${API}/api/job/backfill/${encodeURIComponent(code)}`,{cache:"no-store"}); setBackfillAttempt(1); }
    catch(e) { setStatus(`補資料失敗：${e?.message}`); }
  }

  useEffect(() => {
    if (backfillAttempt<=0||backfillAttempt>72) return;  // 72×5s = 6 min max
    const code=stock.code;
    const timer=setTimeout(async ()=>{
      try {
        const res=await fetch(`${API}/api/kline/${encodeURIComponent(code)}`,{cache:"no-store"});
        if (!res.ok) { setBackfillAttempt(a=>a+1); return; }
        const json=await res.json();
        const nextRows=normalizeRows(json);
        // Always update chart with whatever data has arrived so far
        if (nextRows.length>0) {
          setPayload(json); setRows(nextRows); setRealtime(json.realtime||null); setLastRefresh(new Date());
        }
        if (nextRows.length>=90) {
          setStatus(`✅ 歷史資料已補回（${nextRows.length} 筆）`); setBackfillAttempt(0);
        } else if (nextRows.length>0) {
          setStatus(`⏳ 補充歷史中（已有 ${nextRows.length} 筆，等待更多...）`); setBackfillAttempt(a=>a+1);
        } else {
          setStatus(`⏳ 補資料中（${backfillAttempt}/72）...`); setBackfillAttempt(a=>a+1);
        }
      } catch { setBackfillAttempt(a=>a+1); }
    }, 5000);
    return ()=>clearTimeout(timer);
  }, [backfillAttempt, stock.code]);

  function submit() {
    const t=suggestions[0]||resolveStock(input);
    setStock(t); setInput(t.code); setOpenSuggest(false); setLoadKey(k=>k+1);
  }

  const meta = { ...stock, ...(payload?.meta||{}), ...(analysis?.meta||{}) };
  const displayBar = hovered || rows.at(-1) || {};
  // Compute change from last two K-line closes (reliable even when backend meta is null)
  const _last = rows.at(-1) || {}, _prev = rows.at(-2) || {};
  const rowChange = (_last.close && _prev.close) ? _last.close - _prev.close : null;
  const rowChangePct = (rowChange != null && _prev.close) ? rowChange / _prev.close * 100 : null;
  const changeNum = meta.change ?? rowChange;
  const changePctNum = meta.change_pct ?? rowChangePct;
  const priceColor = (changeNum ?? 0) >= 0 ? "#ef4444" : "#22c55e";
  const livePrice = (hovered?hovered.close:null)??realtime?.price??meta.price??displayBar.close;

  /* ── Render ────────────────────────────────────────────────────── */
  return (
    <div style={{ display:"flex", minHeight:"100vh", background:"#020617" }}>
      <style>{`@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}} *{box-sizing:border-box;}`}</style>
      <SideNav page={page} setPage={setPage} open={sideOpen} setOpen={setSideOpen} />
      {page==="ai" ? <AIChatPage /> : (
      <div style={{ flex:1, minWidth:0, overflow:"auto", ...pageStyle }}>

      {/* ── Header ── */}
      <header style={headerStyle}>
        <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", flexWrap:"wrap", gap:12 }}>
          <div>
            <div style={eyebrowStyle}>TW STOCK DECISION SYSTEM {APP_VERSION}</div>
            <div style={{ display:"flex", alignItems:"center", gap:10, flexWrap:"wrap", marginTop:6 }}>
              <span style={{ fontSize:26, fontWeight:900, color:"#facc15" }}>{meta.code||stock.code}</span>
              <span style={{ fontSize:20, fontWeight:700 }}>{meta.name||stock.name}</span>
              <span style={{ color:"#64748b", fontSize:13 }}>{meta.market} / {meta.industry}</span>
              {isLive&&<LiveBadge />}
            </div>
          </div>
          <div style={{ textAlign:"right" }}>
            <div style={{ fontSize:38, fontWeight:900, color:priceColor, lineHeight:1 }}>{fmt(livePrice)}</div>
            <div style={{ color:priceColor, fontSize:13, marginTop:4 }}>漲跌 {changeNum!=null?(changeNum>=0?"+":"")+fmt(changeNum):"--"}（{changePctNum!=null?fmt(changePctNum,2):"--"}%）</div>
            <div style={{ marginTop:6, display:"flex", gap:14, fontSize:12, justifyContent:"flex-end", flexWrap:"wrap" }}>
              {[["開",displayBar.open],["高",displayBar.high,"#ef4444"],["低",displayBar.low,"#22c55e"],["量",displayBar.volume?(displayBar.volume/1000).toFixed(0)+"K":"--"]].map(([label,val,color])=>(
                <span key={label}><span style={{ color:"#64748b" }}>{label} </span><b style={color?{color}:undefined}>{typeof val==="string"?val:fmt(val)}</b></span>
              ))}
              {hovered&&<span style={{ color:"#475569" }}>📅 {String(hovered.time)}</span>}
              {!hovered&&lastRefresh&&<span style={{ color:"#475569" }}>更新 {lastRefresh.toLocaleTimeString("zh-TW")}</span>}
            </div>
          </div>
        </div>

        <div style={toolbarStyle}>
          <div style={{ position:"relative" }}>
            <input value={input} onFocus={()=>setOpenSuggest(true)}
              onChange={e=>{setInput(e.target.value); setOpenSuggest(true);}}
              onKeyDown={e=>{if(e.key==="Enter")submit();}}
              placeholder="輸入股票代號或名稱" style={inputStyle} />
            {openSuggest&&suggestions.length>0&&(
              <div style={suggestStyle}>
                {suggestions.map(item=>(
                  <div key={item.code} onMouseDown={()=>{setStock(item);setInput(item.code);setOpenSuggest(false);}} style={suggestItemStyle}>
                    <b style={{ color:"#facc15" }}>{item.code}</b> {item.name}
                    <span style={{ color:"#94a3b8", marginLeft:8 }}>{item.market}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
          <button type="button" onClick={submit} style={buttonStyle}>查詢</button>
          <span style={{ color:rows.length?"#22c55e":backfillAttempt>0?"#f97316":"#f59e0b", fontSize:13 }}>{status}</span>
          {isLive&&<span style={{ color:"#94a3b8", fontSize:12 }}>每 {POLL_MS/1000}s 更新</span>}
        </div>
      </header>

      {/* ── Charts Section（全寬，不影響分析卡排列）── */}
      <div style={{ padding:"12px 16px 0" }}>
        {/* K-line */}
        <div style={cardStyle}>
          <div style={{ display:"flex", gap:12, marginBottom:4, fontSize:11, flexWrap:"wrap" }}>
            {[["■","#facc15","MA5"],["■","#fb923c","MA10"],["■","#38bdf8","MA20"],["■","#a78bfa","MA60"],["╌","rgba(148,163,184,.6)","布林帶"]].map(([sym,color,label])=>(
              <span key={label}><span style={{ color }}>{sym}</span> {label}</span>
            ))}
            {hovered&&<span style={{ color:"#475569", marginLeft:"auto" }}>📅 {String(hovered.time)}</span>}
          </div>
          <div ref={mainRef} style={{height:150}} />
        </div>
        {/* RSI + MACD 並排 */}
        <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:8, marginTop:8 }}>
          <div style={cardStyle}>
            <div style={{ color:"#f59e0b", fontSize:11, marginBottom:2 }}>
              RSI14 {Number.isFinite(displayBar.rsi14)&&<b style={{ color:displayBar.rsi14>70?"#ef4444":displayBar.rsi14<30?"#22c55e":"#f59e0b" }}>{fmt(displayBar.rsi14,1)}</b>}
            </div>
            <div ref={rsiRef} style={{height:70}} />
          </div>
          <div style={cardStyle}>
            <div style={{ color:"#94a3b8", fontSize:11, marginBottom:2 }}>
              MACD {Number.isFinite(displayBar.macd_hist)&&<b style={{ color:displayBar.macd_hist>=0?"#ef4444":"#22c55e" }}>{fmt(displayBar.macd_hist,3)}</b>}
            </div>
            <div ref={macdRef} style={{height:70}} />
          </div>
        </div>
        {/* Live order book（開盤時顯示） */}
        {isLive&&(
          <div style={{ ...cardStyle, marginTop:8 }}>
            <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:8, fontSize:13, fontWeight:700, color:"#94a3b8" }}>
              即時委買委賣 <LiveBadge />
            </div>
            {realtime?.bids?.length?<OrderBook bids={realtime.bids} asks={realtime.asks}/>:<div style={{ color:"#64748b", fontSize:13 }}>等待揭示...</div>}
          </div>
        )}
      </div>

      {/* ── 分析卡片 auto-fill（自動換行，不受任何欄高度影響）── */}
      <div style={{ padding:"12px 16px", display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))", gap:12 }}>
        <FundamentalsCard stockCode={stock.code} />
        <TechRadarCard rows={rows} chipData={chip} />
        <MaStatusCard rows={rows} />
        <VolPriceCard rows={rows} />
        <PatternCard rows={rows} />
        <ChipXrayCard chipData={chip} />
        <BlackCandleCard rows={rows} chipData={chip} />
        <MomentumCard rows={rows} />
        <RiskMirrorCard rows={rows} chipData={chip} />
        <ScenarioCard rows={rows} chipData={chip} />
      </div>

      {/* ── Groq AI Card（全寬）── */}
      <div style={{ padding:"0 16px 28px" }}>
        <GroqSummaryCard stockCode={stock.code} rows={rows} chipData={chip} />
      </div>
      </div>
      )}
    </div>
  );
}

/* ── Styles ──────────────────────────────────────────────────────── */
const pageStyle        = { minHeight:"100vh", background:"#020617", color:"#f1f5f9", fontFamily:"Arial, sans-serif" };
const headerStyle      = { padding:"14px 20px 12px", borderBottom:"1px solid #1e293b", background:"#0f172a" };
const eyebrowStyle     = { color:"#38bdf8", letterSpacing:1, fontWeight:800, fontSize:11 };
const toolbarStyle     = { marginTop:10, display:"flex", gap:8, flexWrap:"wrap", alignItems:"center" };
const inputStyle       = { padding:"8px 12px", borderRadius:8, border:"1px solid #334155", background:"#020617", color:"white", minWidth:220, fontSize:14 };
const buttonStyle      = { padding:"8px 16px", borderRadius:8, border:0, background:"#2563eb", color:"white", fontWeight:700, cursor:"pointer" };
const cardStyle        = { background:"#0f172a", border:"1px solid #1e293b", borderRadius:8, padding:14 };
const rowStyle         = { display:"flex", justifyContent:"space-between", gap:8, borderBottom:"1px solid rgba(148,163,184,.1)", padding:"5px 0", fontSize:13 };
const suggestStyle     = { position:"absolute", top:42, left:0, right:0, background:"#0f172a", border:"1px solid #334155", borderRadius:8, zIndex:10, overflow:"hidden" };
const suggestItemStyle = { padding:"9px 12px", cursor:"pointer", borderBottom:"1px solid rgba(148,163,184,.1)", fontSize:14 };
