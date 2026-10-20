// node --test tests/   （美股持股與信貸追蹤；全部是虛構的範例數字）
import test from "node:test";
import assert from "node:assert/strict";
import { parseUsRows, computeUsPositions, valueUsPositions, latestUsPrices, findUsHeader } from "../src/lib/usPortfolio.js";
import { loanStatus, loanOverview, parseLoanXlsxRows, applyLoanImport, sanitizeTracker, paymentDate, DEFAULT_TRACKER } from "../src/lib/loanTracker.js";

const H = ["交易日期", "商品代碼", "商品名稱", "交易市場", "交易種類", "交易幣別", "交割幣別", "股數", "價格", "匯率", "成交金額", "手續費", "其他費用", "應收/付(-)金額"];
const rows = [
  H,
  ["2026-01-05", "ABC", "範例公司", "美國", "買進", "USD", "USD", "10", "100", "1", "1000", "5", "0", "-1005"],
  ["2026-01-05", "ABC", "範例公司", "美國", "買進", "USD", "USD", "10", "100", "1", "1000", "5", "0", "-1005"],
  ["2026-02-01", "ABC", "範例公司", "美國", "賣出", "USD", "USD", "10", "120", "1", "1200", "5", "1", "1194"],
  ["2026-03-01", "ABC", "範例公司", "美國", "除息", "USD", "USD", "10", "1", "1", "10", "0", "3", "7"],
];

test("美股對帳單解析：偵測表頭、重複列加序號、雜湊穩定", () => {
  assert.equal(findUsHeader(rows), 0);
  const r = parseUsRows(rows);
  assert.equal(r.length, 4);
  assert.ok(r.every((x) => x.ok));
  assert.notEqual(r[0].tx.import_hash, r[1].tx.import_hash);
  assert.equal(r[3].tx.side, "cash_dividend");
  assert.equal(r[3].tx.other_fee, 3);
  assert.equal(parseUsRows(rows)[0].tx.import_hash, r[0].tx.import_hash);
});

test("美股部位：平均成本含手續費、賣出扣費、股利用淨額", () => {
  const [p] = computeUsPositions(parseUsRows(rows).map((x) => x.tx));
  // 買 20 股成本 2010；均價 100.5；賣 10 股：1200-6-1005 = 189
  assert.equal(p.shares, 10);
  assert.ok(Math.abs(p.cost - 1005) < 1e-9);
  assert.ok(Math.abs(p.realized - 189) < 1e-9);
  assert.equal(p.dividends, 7);
});

test("美股估值：最新報價與匯率；沒報價不算 NaN 進合計", () => {
  const pos = computeUsPositions(parseUsRows(rows).map((x) => x.tx));
  const { prices, fx } = latestUsPrices([
    { ticker: "ABC", date: "2026-10-01", close: 110 }, { ticker: "ABC", date: "2026-10-02", close: 130 },
    { ticker: "USDTWD", date: "2026-10-02", close: 30 },
  ]);
  const v = valueUsPositions(pos, prices, fx);
  assert.equal(v.rows[0].mvUsd, 1300);
  assert.equal(v.rows[0].mvTwd, 39000);
  assert.ok(Math.abs(v.totals.unrealizedUsd - 295) < 1e-9);
  assert.ok(Math.abs(v.totals.totalUsd - (295 + 189 + 7)) < 1e-9);
  const none = valueUsPositions(pos, {}, null);
  assert.equal(none.totals.mvUsd, 0);
  assert.equal(none.totals.unpriced, 1);
  assert.ok(Number.isNaN(none.rows[0].mvTwd));
  assert.ok(Number.isFinite(none.totals.totalTwd));
});

test("貸款攤還：200 萬、2.9%、120 期", () => {
  const t = { ...DEFAULT_TRACKER, loan: 2000000, rate: 2.9, months: 120, firstPayment: "2026-01-31" };
  const s0 = loanStatus(t, "2026-01-30");
  assert.ok(Math.abs(s0.payment - 19220) < 5, String(s0.payment));
  assert.equal(s0.paid, 0);
  assert.ok(Math.abs(s0.remaining - 2000000) < 1e-6);
  const s3 = loanStatus(t, "2026-03-31");
  assert.equal(s3.paid, 3);
  assert.equal(paymentDate("2026-01-31", 1), "2026-02-28");
  assert.ok(Math.abs(s3.principalPaid + s3.interestPaid - s3.totalPaid) < 1e-6);
  assert.ok(loanStatus({ ...t, paidOverride: 120 }, "2026-03-31").remaining < 1);
  assert.equal(loanStatus({ ...t, paidOverride: 5 }, "2030-01-01").paid, 5);
  assert.equal(loanStatus(t, "2099-01-01").paid, 120);
});

test("淨值：資產 − 剩餘本金 − 起始自有資金", () => {
  const t = sanitizeTracker({ loan: 100000, rate: 0, months: 10, firstPayment: "2026-01-10", startEquity: 1000,
    accounts: [{ key: "deduct", amount: 20000 }, { key: "cathayUsd", amount: 100 }] });
  const o = loanOverview(t, "2026-02-15", { fx: 30, twStockTwd: 50000, usStockTwd: 10000 });
  assert.equal(o.paid, 2);
  assert.equal(o.remaining, 80000);
  assert.equal(o.assets, 20000 + 3000 + 60000);
  assert.equal(o.net, 83000 - 80000 - 1000);
  assert.ok(Number.isNaN(loanOverview(t, "2026-02-15", {}).usdTwd));
});

test("台新借貸內容.xlsx 解析（虛構數字）", () => {
  const p = parseLoanXlsxRows([
    ["台新借信貸", "200萬", "10年期", "利率", "0.029"],
    ["第一期還款", "2026-11-15"],
    ["銀行存款 台幣(信貸扣款帳戶)", "124684", "年息", "0.015"],
    ["Richart子帳戶", "50000", "年息", "0.02"],
    ["國泰銀行 美金帳戶", "(美金)904.32"],
    ["國泰銀行 台幣帳戶", "30000"],
  ]);
  assert.equal(p.loan, 2000000);
  assert.equal(p.months, 120);
  assert.ok(Math.abs(p.rate - 2.9) < 1e-9);
  assert.equal(p.firstPayment, "2026-11-15");
  const t = applyLoanImport(DEFAULT_TRACKER, p);
  const a = Object.fromEntries(t.accounts.map((x) => [x.key, x.amount]));
  assert.deepEqual(a, { deduct: 124684, richart: 50000, cathayTwd: 30000, cathayUsd: 904.32 });
});

test("匯率來源標籤", async () => {
  const { fxSourceLabel, latestUsPrices: l } = await import("../src/lib/usPortfolio.js");
  assert.equal(fxSourceLabel(l([{ ticker: "USDTWD", date: "2026-10-02", close: 30, source: "bot" }]).fx), "臺灣銀行牌告（即期買入）");
  assert.match(fxSourceLabel({ source: "yahoo" }), /Yahoo/);
});

// ── 報酬率與預測（虛構數字）────────────────────────────────────────────
import { xirr, loanPerformance, projectToMaturity, disburseDateOf, loanStatus as _ls } from "../src/lib/loanTracker.js";

test("xirr: 投入 100、一年後拿回 110 ≈ 10%", () => {
  const r = xirr([{ date: "2025-01-01", amount: -100 }, { date: "2026-01-01", amount: 110 }]);
  assert.ok(Math.abs(r - 0.10) < 1e-6, String(r));
});

test("撥款日預設為第一期前一個月", () => {
  assert.equal(disburseDateOf({ firstPayment: "2026-05-10", disburseDate: "" }), "2026-04-10");
  assert.equal(disburseDateOf({ firstPayment: "2026-05-10", disburseDate: "2026-04-08" }), "2026-04-08");
});

test("資產剛好以信貸利率成長 → 年化 ≈ 利率、還完剩 ≈ 0", () => {
  const t = { loan: 1000000, rate: 3, months: 60, firstPayment: "2026-02-01", disburseDate: "2026-01-01", startEquity: 0, paidOverride: null };
  const st = _ls(t, "2027-01-15");             // 12 期
  // 資產每月以 3%/12 成長、每期拿出月付 → 剩下的就是剩餘本金
  const ov = { ...st, assets: st.remaining };
  const p = loanPerformance(t, ov, "2027-01-01");
  assert.ok(Math.abs(p.annual - 0.0304) < 0.002, String(p.annual));   // 月複利 3% ≈ 年化 3.04%
  const end = projectToMaturity(t, ov, (1 + 0.03 / 12) ** 12 - 1);
  assert.ok(Math.abs(end.endAssets) < 50, String(end.endAssets));
});

test("投資賺得比利率多 → 價差為正、到期有剩", () => {
  const t = { loan: 1000000, rate: 3, months: 60, firstPayment: "2026-02-01", disburseDate: "2026-01-01", startEquity: 0, paidOverride: null };
  const st = _ls(t, "2027-01-15");
  const ov = { ...st, assets: st.remaining + 50000 };
  const p = loanPerformance(t, ov, "2027-01-01");
  assert.ok(p.spread > 0 && p.gain > 0, JSON.stringify(p));
  assert.ok(projectToMaturity(t, ov, 0.08).endAssets > 0);
});
