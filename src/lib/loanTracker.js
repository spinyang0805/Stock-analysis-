// 台新信貸還款追蹤：已還幾期、剩餘本金、淨值（資產 − 剩餘本金）。純函式，方便測試
import { loanBalance, loanPayment } from "./loanModel.js";
import { toISODate } from "./format.js";

export const ACCOUNT_DEFS = [
  { key: "deduct", label: "扣款戶（台幣）", currency: "TWD" },
  { key: "richart", label: "Richart 子帳戶", currency: "TWD" },
  { key: "cathayTwd", label: "國泰銀行 台幣", currency: "TWD" },
  { key: "cathayUsd", label: "國泰銀行 美金", currency: "USD" },
];

export const DEFAULT_TRACKER = {
  loan: 0, rate: 0, months: 120, firstPayment: "", paidOverride: null, startEquity: 0,
  accounts: ACCOUNT_DEFS.map((a) => ({ ...a, amount: 0 })),
};

const finite = (v, def = 0) => (Number.isFinite(Number(v)) && v !== "" && v !== null ? Number(v) : def);

export function sanitizeTracker(o) {
  const d = DEFAULT_TRACKER, src = o && typeof o === "object" ? o : {};
  const fp = typeof src.firstPayment === "string" && /^\d{4}-\d{2}-\d{2}$/.test(src.firstPayment) ? src.firstPayment : "";
  const po = src.paidOverride === null || src.paidOverride === undefined || src.paidOverride === "" ? null : Math.max(0, Math.round(finite(src.paidOverride, 0)));
  return {
    loan: Math.max(0, finite(src.loan, d.loan)), rate: Math.max(0, finite(src.rate, d.rate)),
    months: Math.min(480, Math.max(1, Math.round(finite(src.months, d.months)))),
    firstPayment: fp, paidOverride: po, startEquity: finite(src.startEquity, 0),
    accounts: ACCOUNT_DEFS.map((a) => ({ ...a, amount: Math.max(0, finite((Array.isArray(src.accounts) ? src.accounts : []).find((x) => x?.key === a.key)?.amount, 0)) })),
  };
}

// 第 i 期（0 起算）的還款日：每月同一天，遇到短月份就取該月最後一天
export function paymentDate(firstPayment, i) {
  const [y, m, d] = firstPayment.split("-").map(Number);
  const total = m - 1 + i, yy = y + Math.floor(total / 12), mm = ((total % 12) + 12) % 12;
  const last = new Date(Date.UTC(yy, mm + 1, 0)).getUTCDate();
  return `${yy}-${String(mm + 1).padStart(2, "0")}-${String(Math.min(d, last)).padStart(2, "0")}`;
}

// today：'YYYY-MM-DD'。回傳所有「由同一組輸入算出」的還款數字
export function loanStatus(t, today) {
  const { loan, rate, months, firstPayment, paidOverride } = t;
  const payment = loan > 0 ? loanPayment(loan, rate, months) : 0;
  let due = 0;
  if (firstPayment) while (due < months && paymentDate(firstPayment, due) <= today) due += 1;
  const paid = Math.min(months, paidOverride ?? due);
  const remaining = loan > 0 ? loanBalance(loan, rate, months, paid) : 0;
  const principalPaid = loan - remaining;
  const totalPaid = payment * paid;
  return {
    payment, due, paid, months, remaining, principalPaid, totalPaid,
    interestPaid: Math.max(0, totalPaid - principalPaid),
    nextDate: firstPayment && paid < months ? paymentDate(firstPayment, paid) : null,
    progress: months > 0 ? paid / months : 0,
  };
}

// 淨值 = 總資產 − 剩餘本金 − 起始自有資金。所有數字都由輸入 inp 一次算出
export function loanOverview(t, today, inp) {
  const st = loanStatus(t, today);
  const fx = Number(inp.fx);
  const amt = (k) => Number(t.accounts.find((a) => a.key === k)?.amount) || 0;
  const usdTwd = Number.isFinite(fx) && fx > 0 ? amt("cathayUsd") * fx : NaN;
  const bank = amt("deduct") + amt("richart") + amt("cathayTwd") + (Number.isFinite(usdTwd) ? usdTwd : 0);
  const tw = Number(inp.twStockTwd) || 0, us = Number(inp.usStockTwd) || 0;
  const assets = bank + tw + us;
  const net = assets - st.remaining - t.startEquity;
  return { ...st, usdTwd, bank, twStock: tw, usStock: us, assets, net };
}

const CN = { 萬: 1e4, 億: 1e8 };
function parseAmount(s) {
  const m = String(s).replace(/,/g, "").match(/(-?\d+(?:\.\d+)?)\s*([萬億])?/);
  return m ? Number(m[1]) * (CN[m[2]] || 1) : NaN;
}
function parseRate(v) {
  const s = String(v ?? "").trim();
  const m = s.match(/-?\d+(?:\.\d+)?/);
  if (!m) return NaN;
  const n = Number(m[0]);
  return s.includes("%") || n >= 1 ? n : n * 100;
}

// 台新借貸內容.xlsx（無表頭）→ 部分的 tracker 設定；認不得的列就略過
export function parseLoanXlsxRows(rows) {
  const out = { accounts: [] };
  for (const r of rows) {
    const cells = r.map((c) => String(c ?? "").trim());
    const label = cells[0] || "";
    if (/信貸|借信貸|台新/.test(label) && !/帳戶|存款/.test(label)) {
      const amt = parseAmount(cells[1]);
      if (Number.isFinite(amt)) out.loan = amt;
      const yrs = cells.slice(1).find((c) => /年期?/.test(c) && !/年息/.test(c));
      const mo = cells.slice(1).find((c) => /個?月期?/.test(c));
      if (yrs) out.months = Math.round(parseAmount(yrs) * 12);
      else if (mo) out.months = Math.round(parseAmount(mo));
      const ri = cells.findIndex((c) => /利率/.test(c));
      if (ri >= 0) { const rt = parseRate(cells[ri + 1]); if (Number.isFinite(rt)) out.rate = rt; }
    } else if (/第一期/.test(label)) {
      const d = cells.slice(1).map((c) => toISODate(c)).find(Boolean);
      if (d) out.firstPayment = d;
    } else if (/帳戶|存款|Richart/i.test(label)) {
      const usd = /美金|美元|USD/.test(label) || /美金|美元|USD/.test(cells[1] || "");
      let key = null;
      if (/Richart/i.test(label)) key = "richart";
      else if (/國泰/.test(label)) key = usd ? "cathayUsd" : "cathayTwd";
      else if (/扣款|銀行存款/.test(label)) key = "deduct";
      const amt = parseAmount(cells[1]);
      if (key && Number.isFinite(amt)) out.accounts.push({ key, amount: amt });
    }
  }
  return out;
}

// 把 parseLoanXlsxRows 的結果套到現有設定上
export function applyLoanImport(t, parsed) {
  const next = { ...t, accounts: t.accounts.map((a) => ({ ...a })) };
  for (const k of ["loan", "months", "rate", "firstPayment"]) if (parsed[k] !== undefined) next[k] = parsed[k];
  for (const a of parsed.accounts || []) {
    const hit = next.accounts.find((x) => x.key === a.key);
    if (hit) hit.amount = a.amount;
  }
  return sanitizeTracker(next);
}
