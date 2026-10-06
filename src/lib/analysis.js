// 既有指標分析引擎（原 App.jsx，依《📈 股票技術與籌碼分析.docx》），算法不變

export function getMaStatus(rows) {
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

export function getVolPriceMatrix(rows) {
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

export function detectPatterns(rows) {
  if (!rows || rows.length < 20) return { patterns:[{ type:"neutral", label:"資料不足", desc:"需20筆以上才能偵測型態" }], bullCount:0, bearCount:0, conclusion:"資料累積中，暫無型態判斷。" };
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
  if (isBoxBreak)   patterns.push({ type:"bull", label:"帶量突破", desc:`突破近20日高點，量比 ${volRatio.toFixed(1)}x，新趨勢啟動` });
  if (isWBottom)    patterns.push({ type:"bull", label:"W底確立", desc:"雙底型態成立，底部支撐確認" });
  if (isNearHigh)   patterns.push({ type:"bull", label:"逼近壓力區", desc:"接近近20日高點，突破機率增加" });
  if (isBreakDown)  patterns.push({ type:"bear", label:"跌破支撐", desc:"有效跌破近20日低點，建議止損" });
  if (!patterns.length) patterns.push({ type:"neutral", label:"區間整理", desc:"股價在近期區間內整理，等待方向訊號" });
  const bullCount = patterns.filter(p=>p.type==="bull").length;
  const bearCount = patterns.filter(p=>p.type==="bear").length;
  const conclusion = bullCount > 0 && bearCount === 0
    ? `偵測到 ${bullCount} 個多頭型態，技術面偏多，注意放量確認。`
    : bearCount > 0
    ? "偵測到跌破支撐型態，注意下行風險，嚴守停損。"
    : "股價於區間整理，等待量能放大的突破方向訊號。";
  return { patterns, bullCount, bearCount, conclusion };
}

export function getChipAnalysis(chipData) {
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

export function detectBlackCandleAccum(rows, chipData) {
  if (!rows?.length) return { signal:"無資料", color:"#94a3b8", isAccum:false, isBlack:false, closeAbovePrev:false, instBuy:false, changeRate:0, conclusion:"資料不足。" };
  const latest = rows.at(-1);
  const prev = rows.at(-2) || {};
  const cl = chipData?.latest_chip || {};
  const instBuy = (Number(cl.foreign_buy || 0) + Number(cl.investment_trust_buy || 0) + Number(cl.dealer_buy || 0)) > 0;
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

export function getRiskMetrics(rows, chipData) {
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

export function getRsiAnalysis(rows) {
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

export function getKdAnalysis(rows) {
  if (!rows?.length) return { status:"N/A", color:"#94a3b8", k:null, d:null, conclusion:"資料不足。" };
  const latest = rows.at(-1);
  const prev   = rows.at(-2) || {};
  const k = latest.kd_k, d = latest.kd_d;
  const pk = prev.kd_k,  pd = prev.kd_d;
  if (!Number.isFinite(k)) return { status:"計算中", color:"#94a3b8", k:null, d:null, conclusion:"KD資料累積中。" };
  let status, color, conclusion;
  if (k >= 80)                          { status="超買"; color="#ef4444"; conclusion=`K值 ${k.toFixed(1)} 進入超買區（≥80），短線過熱，留意回檔風險。`; }
  else if (k <= 20)                     { status="超賣"; color="#22c55e"; conclusion=`K值 ${k.toFixed(1)} 深入超賣區（≤20），逢低布局機會，等反彈確認。`; }
  else if (k > d && Number.isFinite(pk) && pk <= pd) { status="KD金叉"; color="#f97316"; conclusion="KD形成黃金交叉，短線買進訊號，動能轉多。"; }
  else if (k < d && Number.isFinite(pk) && pk >= pd) { status="KD死叉"; color="#38bdf8"; conclusion="KD形成死亡交叉，短線賣出訊號，動能轉空。"; }
  else if (k > d)                       { status="偏多"; color="#f59e0b"; conclusion=`K(${k.toFixed(1)}) > D(${d.toFixed(1)})，動能偏多，趨勢延續中。`; }
  else                                  { status="偏空"; color="#94a3b8"; conclusion=`K(${k.toFixed(1)}) < D(${d.toFixed(1)})，動能偏空，持觀望態度。`; }
  return { k, d, pk, pd, status, color, conclusion };
}

export function getScenarios(rows, chipData) {
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

export function getOverallScore(rows, chipData) {
  if (!rows?.length) return 50;
  const ma   = getMaStatus(rows);
  const vol  = getVolPriceMatrix(rows);
  const chip = getChipAnalysis(chipData);
  const rsi  = getRsiAnalysis(rows);
  const kd = getKdAnalysis(rows);
  let score = 50;
  const maAdd = {"四線多排":20,"多頭排列":10,"黃金交叉":8,"均線糾結":0,"死亡交叉":-10,"空頭排列":-15,"轉空警訊":-12};
  score += maAdd[ma.label] ?? 0;
  score += vol.score * 5;
  if (rsi.rsi) { if (rsi.rsi > 70) score -= 5; if (rsi.rsi < 30) score += 5; }
  if (kd.k != null) score += kd.k > kd.d ? 5 : -5;
  score += chip.foreign5d > 0 ? 8 : chip.foreign5d < 0 ? -8 : 0;
  score += chip.trust5d > 0 ? 5 : chip.trust5d < 0 ? -5 : 0;
  return Math.max(0, Math.min(100, Math.round(score)));
}

export function getTechRadar(rows, chipData) {
  if (!rows?.length) return { dims:[] };
  const ma   = getMaStatus(rows);
  const vol  = getVolPriceMatrix(rows);
  const chip = getChipAnalysis(chipData);
  const rsi  = getRsiAnalysis(rows);
  const kd   = getKdAnalysis(rows);
  const pat  = detectPatterns(rows);
  const maScore  = {"四線多排":95,"多頭排列":75,"黃金交叉":70,"均線糾結":50,"死亡交叉":30,"空頭排列":20,"轉空警訊":15}[ma.label] ?? 50;
  const volScore = {2:85,1:65,0:50,"-1":40,"-2":15}[String(vol.score)] ?? 50;
  const rsiScore = rsi.rsi ? Math.min(100, Math.max(0, rsi.rsi)) : 50;
  const kdScore  = kd.k != null ? Math.min(90, Math.max(10, kd.k)) : 50;
  const breakoutScore = Math.min(90, Math.max(10, 40 + pat.bullCount*20 - pat.bearCount*15));
  return {
    dims: [
      { label:"趨勢強度", value:maScore,    color:ma.color },
      { label:"量價配合", value:volScore,   color:vol.color },
      { label:"RSI動能",  value:rsiScore,   color:rsi.color },
      { label:"KD動能",   value:kdScore,    color:kd.color },
      { label:"籌碼健康", value:chip.score, color:chip.score>60?"#ef4444":chip.score<40?"#22c55e":"#f59e0b" },
      { label:"突破潛力", value:breakoutScore, color:breakoutScore>60?"#ef4444":"#94a3b8" },
    ],
  };
}
