// 技術指標（前端計算，供日/週/月/分時任一週期的圖表使用）
// 參數與台股看盤軟體慣例一致：MA 5/10/20/60、BB(20,2)、RSI14、KD(9,3,3)、MACD(12,26,9)、ATR14

function sma(values, n) {
  const out = new Array(values.length).fill(NaN);
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= n) sum -= values[i - n];
    if (i >= n - 1) out[i] = sum / n;
  }
  return out;
}

function ema(values, n) {
  const out = new Array(values.length).fill(NaN);
  const k = 2 / (n + 1);
  let prev = NaN;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (!Number.isFinite(v)) continue;
    prev = Number.isFinite(prev) ? v * k + prev * (1 - k) : v;
    out[i] = prev;
  }
  return out;
}

export function computeIndicators(bars) {
  const n = bars.length;
  if (!n) return [];
  const close = bars.map((b) => b.close);
  const vol = bars.map((b) => b.volume || 0);
  const ma5 = sma(close, 5), ma10 = sma(close, 10), ma20 = sma(close, 20), ma60 = sma(close, 60);
  const volMa5 = sma(vol, 5);

  // 布林 20,2
  const bbU = new Array(n).fill(NaN), bbL = new Array(n).fill(NaN);
  for (let i = 19; i < n; i++) {
    const m = ma20[i];
    let s = 0;
    for (let j = i - 19; j <= i; j++) s += (close[j] - m) ** 2;
    const sd = Math.sqrt(s / 20);
    bbU[i] = m + 2 * sd;
    bbL[i] = m - 2 * sd;
  }

  // RSI14（Wilder）
  const rsi = new Array(n).fill(NaN);
  let ag = 0, al = 0;
  for (let i = 1; i < n; i++) {
    const d = close[i] - close[i - 1];
    const g = Math.max(d, 0), l = Math.max(-d, 0);
    if (i <= 14) {
      ag += g / 14; al += l / 14;
      if (i === 14) rsi[i] = al === 0 ? 100 : 100 - 100 / (1 + ag / al);
    } else {
      ag = (ag * 13 + g) / 14; al = (al * 13 + l) / 14;
      rsi[i] = al === 0 ? 100 : 100 - 100 / (1 + ag / al);
    }
  }

  // KD 9,3,3
  const kArr = new Array(n).fill(NaN), dArr = new Array(n).fill(NaN);
  let k = 50, d = 50;
  for (let i = 0; i < n; i++) {
    if (i < 8) continue;
    let hi = -Infinity, lo = Infinity;
    for (let j = i - 8; j <= i; j++) { hi = Math.max(hi, bars[j].high); lo = Math.min(lo, bars[j].low); }
    const rsv = hi === lo ? 50 : ((close[i] - lo) / (hi - lo)) * 100;
    k = (2 / 3) * k + rsv / 3;
    d = (2 / 3) * d + k / 3;
    kArr[i] = k; dArr[i] = d;
  }

  // MACD 12,26,9
  const e12 = ema(close, 12), e26 = ema(close, 26);
  const dif = close.map((_, i) => (i >= 25 ? e12[i] - e26[i] : NaN));
  const sig = ema(dif, 9);
  // ATR14
  const atr = new Array(n).fill(NaN);
  let a = NaN;
  for (let i = 1; i < n; i++) {
    const tr = Math.max(bars[i].high - bars[i].low, Math.abs(bars[i].high - close[i - 1]), Math.abs(bars[i].low - close[i - 1]));
    if (i < 14) { a = Number.isFinite(a) ? a + tr : tr; if (i === 13) a /= 13; continue; }
    a = (a * 13 + tr) / 14;
    atr[i] = a;
  }

  return bars.map((b, i) => ({
    ...b,
    ma5: ma5[i], ma10: ma10[i], ma20: ma20[i], ma60: ma60[i],
    volume_ma5: volMa5[i],
    bb_upper: bbU[i], bb_lower: bbL[i], bb_width: Number.isFinite(bbU[i]) ? (bbU[i] - bbL[i]) / ma20[i] : NaN,
    rsi14: rsi[i], kd_k: kArr[i], kd_d: dArr[i],
    macd: dif[i], macd_signal: sig[i], macd_hist: Number.isFinite(dif[i]) && Number.isFinite(sig[i]) ? dif[i] - sig[i] : NaN,
    atr14: atr[i],
  }));
}

// 日K → 週K / 月K（週以週一為鍵）
export function aggregateBars(bars, period) {
  if (period === "D") return bars;
  const groups = new Map();
  for (const b of bars) {
    const dt = new Date(`${b.time}T00:00:00Z`);
    let key;
    if (period === "W") {
      const dow = (dt.getUTCDay() + 6) % 7;
      const mon = new Date(dt.getTime() - dow * 86400000);
      key = mon.toISOString().slice(0, 10);
    } else {
      key = `${b.time.slice(0, 7)}-01`;
    }
    const g = groups.get(key);
    if (!g) groups.set(key, { time: key, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume || 0, last: b.time });
    else {
      g.high = Math.max(g.high, b.high);
      g.low = Math.min(g.low, b.low);
      g.close = b.close;
      g.volume += b.volume || 0;
      g.last = b.time;
    }
  }
  return [...groups.values()];
}
