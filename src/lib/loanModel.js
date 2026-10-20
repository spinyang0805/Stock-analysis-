// 借貸試算的純計算模型（頁面與 Web Worker 共用；沒有任何 React / DOM 相依）

export function quantile(sorted, q) {
  if (!sorted.length) return NaN;
  const pos = (sorted.length - 1) * q, lo = Math.floor(pos), hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

// ==MODEL-START==（純函式；scratch 驗證會直接截取這一段）
export const ETF_KEYS = ["e0050", "e006208", "e009816", "e00919"];
export const CASH_KEYS = ["deduct", "reserve", "other"];
const DIV_MONTHS = [3, 6, 9, 12];
const NHI_RATE = 0.0211, NHI_LIMIT = 20000;

// rates below ~0.0001%/yr are treated as 0 so (1 − (1+i)^−n) never rounds to zero
const monthlyRate = (annualPct) => (Math.abs(annualPct) < 1e-6 ? 0 : annualPct / 100 / 12);

export function loanPayment(P, annualPct, n) {
  const i = monthlyRate(annualPct);
  return i === 0 ? P / n : (P * i) / (1 - Math.pow(1 + i, -n));
}

export function loanBalance(P, annualPct, n, k) {
  const i = monthlyRate(annualPct), pay = loanPayment(P, annualPct, n);
  if (i === 0) return Math.max(0, P - pay * k);
  return Math.max(0, P * Math.pow(1 + i, k) - (pay * (Math.pow(1 + i, k) - 1)) / i);
}

// px：start..start+n 的 0050 總報酬指數；labels：同長度的 "YYYY-MM"
// px919：00919 自己的總報酬指數（未給就和 px 相同）；trk：選填，逐月記錄用的陣列（未來預估用）
export function simulate(px, labels, p, detail, px919, trk) {
  const n = p.n, pay = loanPayment(p.loan, p.rate, n);
  const fb = (p.feePct * p.discount) / 100 + p.slipPct / 100, fs = fb + p.taxPct / 100;
  const cr = p.cashRate / 100 / 12;
  const c = { deduct: p.amt.deduct, reserve: p.amt.reserve, other: p.amt.other, wait: 0 };
  const u = { e0050: 0, e006208: 0, e009816: 0, e00919: 0 };
  const T = ETF_KEYS.reduce((a, k) => a + p.amt[k], 0);
  c.wait = T;                                            // 準備投入的錢先放在「其他現金」
  const cashSum = () => c.deduct + c.reserve + c.other + c.wait;
  const stockVal = (x, x9) => ETF_KEYS.reduce((a, k) => a + u[k] * (k === "e00919" ? x9 : x), 0);
  const takeCash = (amt, order) => {
    let left = amt;
    for (const k of order) { const t = Math.min(c[k], left); c[k] -= t; left -= t; }
  };
  let high = px[0], invested = 0, short = 0, ruin = false, salaryPaid = 0, cumDiv = 0, cumNhi = 0, cumSold = 0, deductOutK = -1;
  let lowNet = Infinity, lowNetK = 0, lowPot = Infinity;
  const series = [], rows = [];
  for (let k = 0; k <= n; k += 1) {
    const x = px[k], x9 = px919 ? px919[k] : x;
    if (k) for (const key in c) c[key] *= 1 + cr;
    high = Math.max(high, x);
    // 進場
    let want = 0;
    if (p.entry === "lump") want = k === 0 ? T : 0;
    else if (p.entry === "dca") want = k < p.dcaN ? T / p.dcaN : 0;
    else if (k <= p.dipM) {
      let f = 1 / 3 + (x <= (1 - p.dipX / 100) * high ? 1 / 3 : 0) + (x <= (1 - p.dipY / 100) * high ? 1 / 3 : 0);
      if (k >= p.dipM) f = 1;
      want = Math.max(0, f * T - invested);
    }
    if (want > 0 && T > 0) {
      const buy = Math.min(want, c.wait);
      for (const key of ETF_KEYS) u[key] += (buy * (p.amt[key] / T) * (1 - fb)) / (key === "e00919" ? x9 : x);
      c.wait -= buy; invested += buy;
    }
    // 00919 季配息（由市值扣除；總報酬不變）
    if (k && u.e00919 > 0 && DIV_MONTHS.includes(Number(labels[k].slice(5)))) {
      const payout = u.e00919 * x9 * (p.yield919 / 100 / 4);
      const nhi = payout >= NHI_LIMIT ? payout * NHI_RATE : 0, net = payout - nhi;
      u.e00919 -= payout / x9;
      cumDiv += payout; cumNhi += nhi;
      if (p.divDeposit) c.deduct += net; else u.e00919 += (net * (1 - fb)) / x9;
    }
    // 還款
    if (k) {
      let need = pay;
      const sal = Math.min(p.salary, need); need -= sal; salaryPaid += sal;
      for (const key of p.order) {
        if (need <= 1e-6) break;
        if (key === "other") { for (const kk of ["other", "wait"]) { const t = Math.min(c[kk], need); c[kk] -= t; need -= t; } }
        else if (CASH_KEYS.includes(key)) { const t = Math.min(c[key], need); c[key] -= t; need -= t; }
        else {
          const xx = key === "e00919" ? x9 : x;
          const got = Math.min(u[key] * xx * (1 - fs), need);   // 不會賣超過持有
          u[key] -= got / (xx * (1 - fs)); need -= got; cumSold += got;
        }
      }
      if (need > 1e-6) { short += need; if (k < n) ruin = true; }
    }
    // 每季再平衡到目標股票比例
    const sv0 = stockVal(x, x9);
    if (p.rebalance && k && k % 3 === 0 && sv0 > 0 && T > 0) {
      const w = p.rebTarget / 100, tot = sv0 + cashSum(), gap = w * tot - sv0;
      if (gap > 0) {
        const bx = Math.min(gap / (1 - fb + w * fb), cashSum());
        takeCash(bx, ["wait", "other", "reserve", "deduct"]);
        for (const key of ETF_KEYS) u[key] += (bx * (p.amt[key] / T) * (1 - fb)) / (key === "e00919" ? x9 : x);
      } else {
        const sx = -gap / (1 - w * fs);
        for (const key of ETF_KEYS) u[key] *= 1 - sx / sv0;
        c.other += sx * (1 - fs);
      }
    }
    const sv = stockVal(x, x9), cash = cashSum(), pot = sv + cash, bal = loanBalance(p.loan, p.rate, n, k);
    if (deductOutK < 0 && k && p.amt.deduct > 0 && c.deduct <= 1) deductOutK = k;
    if (trk) { trk.pot[k] = pot; trk.net[k] = pot - bal - short - salaryPaid; trk.div[k] = cumDiv; trk.nhi[k] = cumNhi; trk.short[k] = short; trk.sold[k] = cumSold; }
    lowPot = Math.min(lowPot, pot);
    if (pot - bal < lowNet) { lowNet = pot - bal; lowNetK = k; }
    if (detail) {
      series.push({ k, pot, bal });
      if (k > 0 && (k % 12 === 0 || k === n)) rows.push({ k, label: labels[k], sv, cash, cumDiv, cumNhi, short, bal });
    }
  }
  const final = stockVal(px[n], px919 ? px919[n] : px[n]) + cashSum() - short - salaryPaid;
  return { final, deductOutK, lowNet, lowNetK, lowPot, ruin, short, salaryPaid, cumDiv, cumNhi, series, rows, pay };
}
// ==MODEL-END==

// ==MC-START==（未來預估：蒙地卡羅；純函式）
const RHO = 0.7;
export function mulberry32(a) {
  return () => {
    a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// 0050 月對數報酬，標準化成平均 0、標準差 1
export function histZ(tr) {
  const r = [];
  for (let i = 0; i + 1 < tr.length; i += 1) r.push(Math.log(tr[i + 1] / tr[i]));
  const m = r.reduce((a, b) => a + b, 0) / r.length;
  const sd = Math.sqrt(r.reduce((a, b) => a + (b - m) * (b - m), 0) / r.length) || 1;
  return r.map((x) => (x - m) / sd);
}

// 每條路徑每個月兩個標準化衝擊：[大盤, 00919]。只和方法、種子、期數有關，改報酬假設不會重抽（結果不會亂跳）
// 抽完之後用全部路徑的實際平均與標準差校正成平均 0、標準差 1（循環區塊 + 經驗校正，避免區塊邊界造成偏差）
export function makeShocks(method, zh, P, n, seed) {
  const rnd = mulberry32(seed), L = zh.length, zm = new Float64Array(P * n), zi = new Float64Array(P * n);
  const gauss = () => Math.sqrt(-2 * Math.log(1 - rnd())) * Math.cos(2 * Math.PI * rnd());
  for (const arr of [zm, zi]) {
    for (let i = 0; i < P; i += 1) {
      if (method === "normal") { for (let k = 0; k < n; k += 1) arr[i * n + k] = gauss(); continue; }
      for (let k = 0; k < n;) {                          // 12 個月一塊（頭尾相接循環），整塊抽樣，保留大跌的連續性
        const s = Math.floor(rnd() * L);
        for (let j = 0; j < 12 && k < n; j += 1, k += 1) arr[i * n + k] = zh[(s + j) % L];
      }
    }
  }
  const std = (arr) => {
    let m = 0;
    for (let i = 0; i < arr.length; i += 1) m += arr[i];
    m /= arr.length;
    let v = 0;
    for (let i = 0; i < arr.length; i += 1) v += (arr[i] - m) * (arr[i] - m);
    const sd = Math.sqrt(v / arr.length) || 1;
    for (let i = 0; i < arr.length; i += 1) arr[i] = (arr[i] - m) / sd;
  };
  std(zm); std(zi);
  const rs = Math.sqrt(1 - RHO * RHO), z9 = new Float64Array(P * n);
  for (let i = 0; i < z9.length; i += 1) z9[i] = RHO * zm[i] + rs * zi[i];
  std(z9);
  const out = new Float32Array(P * n * 2);
  for (let i = 0; i < P * n; i += 1) { out[i * 2] = zm[i]; out[i * 2 + 1] = z9[i]; }
  return out;
}

function qs3(tmp, len) {
  const s = tmp.subarray(0, len).sort();
  return [quantile(s, 0.1), quantile(s, 0.5), quantile(s, 0.9)];
}

export function runMC(p, shocks, P, labels) {
  const n = p.n, N = n + 1;
  const sm = p.volMkt / 100, s9 = p.vol919 / 100;
  const mm = Math.log(1 + p.retMkt / 100) / 12, m9 = Math.log(1 + p.ret919 / 100) / 12;   // 年化報酬 = 複利（CAGR），每月對數漂移 = ln(1+R)/12
  const km = sm / Math.sqrt(12), k9 = s9 / Math.sqrt(12);
  const px = new Float64Array(N), px9 = new Float64Array(N);
  const A = {};
  for (const key of ["pot", "net", "div", "nhi", "short", "sold"]) A[key] = new Float64Array(P * N);
  const fin = [], deductK = [];
  let needN = 0;
  const needAmt = [];
  for (let i = 0; i < P; i += 1) {
    px[0] = px9[0] = 100;
    let a = 0, b = 0;
    for (let k = 1; k <= n; k += 1) {
      const zm = shocks[(i * n + k - 1) * 2], z9 = shocks[(i * n + k - 1) * 2 + 1];
      a += mm + km * zm; b += m9 + k9 * z9;
      px[k] = 100 * Math.exp(a); px9[k] = 100 * Math.exp(b);
    }
    const trk = {};
    for (const key in A) trk[key] = A[key].subarray(i * N, (i + 1) * N);
    const r = simulate(px, labels, p, false, px9, trk);
    fin.push(r.final);
    if (r.short > 1e-6) { needN += 1; needAmt.push(r.short); }
    if (r.deductOutK >= 0) deductK.push(r.deductOutK);
  }
  const tmp = new Float64Array(P), col = (arr, k) => { for (let i = 0; i < P; i += 1) tmp[i] = arr[i * N + k]; return qs3(tmp, P); };
  const diffCol = (arr, k0, k1) => { for (let i = 0; i < P; i += 1) tmp[i] = arr[i * N + k1] - arr[i * N + k0]; return qs3(tmp, P); };
  const fan = [];
  for (let k = 0; k <= n; k += 1) { const [lo, mid, hi] = col(A.net, k); fan.push({ k, label: labels[k], p10: lo, p50: mid, p90: hi, bal: loanBalance(p.loan, p.rate, n, k) }); }
  const years = [];
  let k0 = 0;
  for (let k = 1; k <= n; k += 1) {
    const y = labels[k].slice(0, 4);
    if (k < n && labels[k + 1].slice(0, 4) === y) continue;
    const dv = new Float64Array(P);
    for (let i = 0; i < P; i += 1) dv[i] = A.div[i * N + k] - A.nhi[i * N + k] - (A.div[i * N + k0] - A.nhi[i * N + k0]);
    years.push({ year: y, k1: k, months: k - k0, pay: loanPayment(p.loan, p.rate, n) * (k - k0), bal: loanBalance(p.loan, p.rate, n, k),
      divNet: qs3(dv, P), nhi: diffCol(A.nhi, k0, k)[1], sold: diffCol(A.sold, k0, k)[1], pot: col(A.pot, k), net: col(A.net, k) });
    k0 = k;
  }
  const f = Float64Array.from(fin).sort(), dn = new Float64Array(P);
  for (let i = 0; i < P; i += 1) dn[i] = A.div[i * N + n] - A.nhi[i * N + n];
  needAmt.sort((x, y) => x - y); deductK.sort((x, y) => x - y);
  return {
    years, fan, pay: loanPayment(p.loan, p.rate, n),
    head: { net: [quantile(f, 0.1), quantile(f, 0.5), quantile(f, 0.9)], needP: needN / P, needMed: quantile(needAmt, 0.5), needMax: needAmt.length ? needAmt[needAmt.length - 1] : 0,
      cumDiv: qs3(dn, P)[1], deductShare: deductK.length / P, deductMed: quantile(deductK, 0.5) },
  };
}
// 歷史劇本：把過去某個高點之後的月報酬套到使用者的方案上；歷史不夠長的部分用「基準報酬」接續
export const SCEN_GROUPS = [["bear", "空頭／崩盤型"], ["mild", "溫和成長型"], ["bull", "多頭型"], ["custom", "自選月份"]];
export const SCEN_DEFS = [
  { id: "s2000", group: "bear", name: "2000 網路泡沫型*", start: "2000-02", src: "taiex" },
  { id: "s2007", group: "bear", name: "2007 金融海嘯型", start: "2007-10", src: "tr" },
  { id: "s2015", group: "bear", name: "2015 盤整修正型", start: "2015-04", src: "tr" },
  { id: "s2018", group: "bear", name: "2018 貿易戰型", start: "2018-01", src: "tr" },
  { id: "s2021", group: "bear", name: "2022 升息空頭型", start: "2021-12", src: "tr" },
  { id: "s2024", group: "bear", name: "2024 高點後關稅震盪型", start: "2024-07", src: "tr" },
  { id: "s2003", group: "mild", name: "2003 SARS 後慢牛", start: "2003-06", src: "tr" },
  { id: "s2010", group: "mild", name: "2010 歐債前後", start: "2010-07", src: "tr" },
  { id: "s2013", group: "mild", name: "2013 穩步上漲", start: "2013-01", src: "tr" },
  { id: "s2009", group: "bull", name: "2009 海嘯後反彈", start: "2009-03", src: "tr" },
  { id: "s2012", group: "bull", name: "2012 長多", start: "2012-01", src: "tr" },
  { id: "s2016", group: "bull", name: "2016 台積電長多", start: "2016-06", src: "tr" },
  { id: "s2019", group: "bull", name: "2019 半導體多頭", start: "2019-01", src: "tr" },
  { id: "s2020", group: "bull", name: "2020 疫情後 V 轉", start: "2020-03", src: "tr" },
  { id: "s2023", group: "bull", name: "2023 AI 多頭", start: "2023-01", src: "tr" },
];
export const DEFAULT_SCEN_ON = { s2007: true, s2013: true, s2016: true };

// 起算月份（預設：下個月）與之後 n 個月的 "YYYY-MM"
export function nextMonthLabel() {
  const now = new Date();
  let y = now.getFullYear(), m = now.getMonth() + 2;
  if (m > 12) { m = 1; y += 1; }
  return `${y}-${String(m).padStart(2, "0")}`;
}
export function futureLabels(origin, n) {
  let y = Number(origin.slice(0, 4)), m = Number(origin.slice(5));
  const out = [];
  for (let k = 0; k <= n; k += 1) { out.push(`${y}-${String(m).padStart(2, "0")}`); m += 1; if (m > 12) { m = 1; y += 1; } }
  return out;
}

// 每個日曆年的最後一個月
function yearEnds(labels, n) {
  const ye = [];
  for (let k = 1; k <= n; k += 1) if (k === n || labels[k + 1].slice(0, 4) !== labels[k].slice(0, 4)) ye.push(k);
  return ye;
}

// 實際走勢：從起算月份起，etf_history.json 裡已經有的月份；沒有就回傳 null
export function runActual(p, hist, labels) {
  const n = p.n, N = n + 1, a0 = hist.months.indexOf(labels[0]);
  if (a0 < 0) return null;
  const am = Math.min(n, hist.months.length - 1 - a0), px = new Float64Array(N);
  px[0] = 100;
  for (let k = 1; k <= n; k += 1) px[k] = k <= am ? (100 * hist.tr[a0 + k]) / hist.tr[a0] : px[k - 1];
  return simOne(p, labels, "actual", "實際走勢", px, am, am);
}

function simOne(p, labels, id, name, px, histMonths, keep, group) {
  const n = p.n, N = n + 1, ye = yearEnds(labels, n);
  const trk = {};
  for (const key of ["pot", "net", "div", "nhi", "short", "sold"]) trk[key] = new Float64Array(N);
  const r = simulate(px, labels, p, false, undefined, trk);
  const ok = (k) => k <= keep;
  return {
    id, name, group, histMonths, keep,
    net: Array.from(trk.net.subarray(0, keep + 1)),
    yearNet: ye.map((k) => (ok(k) ? trk.net[k] : null)),
    yearDiv: ye.map((k, i) => (ok(k) ? trk.div[k] - trk.nhi[k] - (i ? trk.div[ye[i - 1]] - trk.nhi[ye[i - 1]] : 0) : null)),
    needs: trk.short[keep] > 1e-6, short: trk.short[keep], final: trk.net[keep], pay: r.pay,
  };
}

export function runScenarios(p, hist, labels, custom) {
  const n = p.n, N = n + 1, tail = Math.pow(1 + p.retMkt / 100, 1 / 12), dv = Math.pow(1.03, 1 / 12);
  const one = (id, name, px, hm, keep, group) => simOne(p, labels, id, name, px, hm, keep, group);
  const list = [];
  for (const d of SCEN_DEFS.concat(custom || [])) {
    const taiex = d.src === "taiex", months = taiex ? hist.taiex_months : hist.months, ser = taiex ? hist.taiex_price : hist.tr;
    const i0 = months && ser ? months.indexOf(d.start) : -1;
    if (i0 < 0) continue;
    const hm = Math.min(n, months.length - 1 - i0), px = new Float64Array(N);
    px[0] = 100;
    for (let k = 1; k <= n; k += 1) px[k] = px[k - 1] * (k <= hm ? (ser[i0 + k] / ser[i0 + k - 1]) * (taiex ? dv : 1) : tail);
    const r = one(d.id, d.name, px, hm, n, d.group);
    let peak = px[0], dd = 0;
    for (let k = 1; k <= hm; k += 1) { peak = Math.max(peak, px[k]); dd = Math.min(dd, px[k] / peak - 1); }
    r.maxDD = dd; r.cagr = hm >= 1 ? Math.pow(px[hm] / px[0], 12 / hm) - 1 : 0;
    list.push(r);
  }
  return { list, actual: runActual(p, hist, labels) };
}
// ==MC-END==

// 未來預估的重計算（背景執行緒與主畫面備援共用）：蒙地卡羅 + 「每個歷史月份進場」摘要
export function computeFuture(p, months, tr, seed, paths) {
  const mc = runMC(p, makeShocks(p.method, histZ(tr), paths, p.n, seed), paths, futureLabels(p.origin, p.n));
  const b = [0, 0, 0, 0];
  let total = 0;
  for (let i = 0; i + p.n < months.length; i += 1) {
    const v = simulate(tr.slice(i, i + p.n + 1), months.slice(i, i + p.n + 1), p, false).final;
    b[v < 0 ? 0 : v < 1e6 ? 1 : v < 3e6 ? 2 : 3] += 1; total += 1;
  }
  return { mc, sumry: { b, total } };
}
