// node --test tests/   （純函式單元測試，不需瀏覽器）
import test from "node:test";
import assert from "node:assert/strict";
import { computePositions, valuePositions, equityCurve } from "../src/lib/portfolio.js";
import { parseCSV, detectHeader, guessMapping, convertRows, decodeBytes, parseSide } from "../src/lib/csvImport.js";
import { computeIndicators, aggregateBars } from "../src/lib/indicators.js";
import { toISODate } from "../src/lib/format.js";
import { backtestSwing, annualCashDividends } from "../src/lib/strategy.js";

const tx = (trade_date, side, shares, price, fee = 0, tax = 0, extra = {}) =>
  ({ trade_date, stock_id: "2330", side, shares, price, fee, tax, ...extra });

test("移動平均成本：兩次買進後賣出一半", () => {
  const [p] = computePositions([
    tx("2026-01-02", "buy", 1000, 100, 20),
    tx("2026-01-05", "buy", 1000, 120, 20),
    tx("2026-02-01", "sell", 1000, 130, 20, 390),
  ]);
  // 成本 (100000+20+120000+20)=220040，均價 110.02；賣 1000 股：130000-20-390-110020 = 19570
  assert.equal(p.shares, 1000);
  assert.ok(Math.abs(p.cost - 110020) < 1e-6);
  assert.ok(Math.abs(p.realized - 19570) < 1e-6);
  assert.ok(Math.abs(p.avgCost - 110.02) < 1e-9);
});

test("全部賣出後成本歸零；現金股利以 amount 優先", () => {
  const [p] = computePositions([
    tx("2026-01-02", "buy", 2000, 50),
    tx("2026-03-01", "cash_dividend", 2000, 2.5, 0, 0, { amount: 4990 }),
    tx("2026-04-01", "sell", 2000, 55),
  ]);
  assert.equal(p.shares, 0);
  assert.equal(p.cost, 0);
  assert.equal(p.dividends, 4990);
  assert.equal(p.realized, 10000);
});

test("valuePositions 總計與權重", () => {
  const { totals, held } = valuePositions(computePositions([tx("2026-01-02", "buy", 1000, 100)]), { 2330: 110 });
  assert.equal(totals.marketValue, 110000);
  assert.equal(totals.unrealized, 10000);
  assert.equal(held[0].weight, 100);
});

test("equityCurve 逐日市值、用最近收盤補非交易日", () => {
  const curve = equityCurve(
    [tx("2026-01-02", "buy", 1000, 100), tx("2026-01-06", "sell", 500, 110)],
    { 2330: [{ time: "2026-01-02", close: 101 }, { time: "2026-01-05", close: 105 }, { time: "2026-01-06", close: 110 }] },
  );
  assert.deepEqual(curve.map((c) => c.date), ["2026-01-02", "2026-01-05", "2026-01-06"]);
  assert.equal(curve[0].marketValue, 101000);
  assert.equal(curve[1].pnl, 5000);
  assert.equal(curve[2].marketValue, 55000);
  assert.equal(curve[2].realized, 5000);
  assert.equal(curve[2].pnl, 10000);
});

test("CSV：找表頭、自動對應、張數換算、民國日期、重複列各自保留", () => {
  const csv = `國泰證券 對帳單\n帳號,123\n成交日期,股票代號,股票名稱,買賣別,成交張數,成交價,手續費,交易稅,淨收付金額\n115/10/01,2330 台積電,台積電,現股買進,1,"1,000",1425,0,"-1,001,425"\n115/10/01,2330 台積電,台積電,現股買進,1,"1,000",1425,0,"-1,001,425"\n1151002,0056,元大高股息,現股賣出,2,38.5,109,231,76660\n`;
  const rows = parseCSV(csv);
  const h = detectHeader(rows);
  assert.equal(h, 2);
  const header = rows[h];
  const mapping = guessMapping(header);
  assert.equal(mapping.code, "股票代號");
  assert.equal(mapping.lots, "成交張數");
  assert.equal(mapping.amount, "淨收付金額");
  const out = convertRows(header, rows.slice(h + 1), mapping, { broker: "國泰" });
  assert.ok(out.every((r) => r.ok), JSON.stringify(out.map((r) => r.errors)));
  assert.equal(out[0].tx.trade_date, "2026-10-01");
  assert.equal(out[0].tx.shares, 1000);
  assert.equal(out[0].tx.price, 1000);
  assert.notEqual(out[0].tx.import_hash, out[1].tx.import_hash);
  assert.equal(out[2].tx.side, "sell");
  assert.equal(out[2].tx.trade_date, "2026-10-02");
  assert.equal(out[2].tx.stock_id, "0056");
});

test("CSV：錯誤列附原因", () => {
  const out = convertRows(["日期", "代號", "買賣", "股數", "價格"], [["abc", "XX", "?", "0", "0"]],
    { date: "日期", code: "代號", side: "買賣", shares: "股數", price: "價格" });
  assert.equal(out[0].ok, false);
  assert.ok(out[0].errors.length >= 3);
});

test("Big5 解碼", () => {
  const big5 = new Uint8Array([0xb6, 0x52, 0xbd, 0xe6]); // 「買賣」
  assert.equal(decodeBytes(big5.buffer), "買賣");
});

test("parseSide", () => {
  assert.equal(parseSide("融資買進"), "buy");
  assert.equal(parseSide("現股賣出"), "sell");
  assert.equal(parseSide("現金股利"), "cash_dividend");
  assert.equal(parseSide("配股"), "stock_dividend");
});

test("toISODate 各種格式", () => {
  assert.equal(toISODate("20261006"), "2026-10-06");
  assert.equal(toISODate("2026/1/5"), "2026-01-05");
  assert.equal(toISODate("115/1/5"), "2026-01-05");
  assert.equal(toISODate("1150105"), "2026-01-05");
});

function synthBars(n, f) {
  const out = [];
  const d0 = Date.UTC(2024, 0, 1);
  for (let i = 0; i < n; i++) {
    const c = f(i);
    out.push({ time: new Date(d0 + i * 86400000).toISOString().slice(0, 10), open: c, high: c * 1.01, low: c * 0.99, close: c, volume: 1000 });
  }
  return out;
}

test("指標：常數價格 → MA 等於價格、RSI 有值、KD 50", () => {
  const rows = computeIndicators(synthBars(80, () => 100));
  const b = rows.at(-1);
  assert.equal(b.ma20, 100);
  assert.equal(b.ma60, 100);
  assert.ok(Math.abs(b.kd_k - 50) < 1e-9);
  assert.ok(Number.isFinite(b.atr14));
});

test("週K / 月K 彙整", () => {
  const bars = synthBars(31, (i) => 100 + i); // 2024-01-01(一) ~ 01-31
  const w = aggregateBars(bars, "W");
  assert.equal(w[0].time, "2024-01-01");
  assert.equal(w[0].open, 100);
  assert.equal(w[0].close, 106);
  const m = aggregateBars(bars, "M");
  assert.equal(m.length, 1);
  assert.equal(m[0].high, 130 * 1.01);
});

test("波段回測：上升趨勢至少一筆交易", () => {
  const rows = computeIndicators(synthBars(200, (i) => 100 + 10 * Math.sin(i / 15) + i * 0.2));
  const bt = backtestSwing(rows);
  assert.ok(bt.trades.length >= 1);
});

test("年度現金股利：季配息加總", () => {
  const a = annualCashDividends([
    { exDate: "2025-03-01", cash: 4 }, { exDate: "2025-06-01", cash: 4.5 }, { exDate: "2024-06-01", cash: 3 },
  ]);
  assert.deepEqual(a, [{ year: 2024, cash: 3 }, { year: 2025, cash: 8.5 }]);
});

import { ttmEps, valuationBands } from "../src/lib/strategy.js";
test("TTM EPS 與本益比百分位估價", () => {
  const q = [];
  for (let y = 2019; y <= 2026; y++) for (const m of ["03-31", "06-30", "09-30", "12-31"]) q.push({ date: `${y}-${m}`, eps: 2 });
  const t = ttmEps(q);
  assert.equal(t[0].ttm, 8);
  const bars = [];
  const d0 = Date.UTC(2020, 0, 1);
  for (let i = 0; i < 2400; i++) bars.push({ time: new Date(d0 + i * 86400000).toISOString().slice(0, 10), close: 80 + (i % 100) });
  const v = valuationBands({ price: 120, fundamentals: {}, financials: {}, dividends: [], longBars: bars, epsQuarters: q });
  const pe = v.methods.find((m) => m.name === "本益比法");
  assert.ok(pe.cheap < pe.fair && pe.fair < pe.expensive);
  assert.ok(pe.basis.includes("百分位"));
});

test("parseSide 不把英文字裡的 s/b 誤判", () => {
  assert.equal(parseSide("Cash dividend"), "cash_dividend");
  assert.equal(parseSide("Subscription"), null);
  assert.equal(parseSide("S"), "sell");
  assert.equal(parseSide("Buy"), "buy");
});

import { listAccounts } from "../src/lib/portfolio.js";
test("多帳戶：分帳戶成本各自計算，合併 = 加總", () => {
  const txs = [
    tx("2026-01-02", "buy", 1000, 100, 0, 0, { broker: "國泰證券" }),
    tx("2026-01-03", "buy", 1000, 200, 0, 0, { broker: "亞東證券" }),
    tx("2026-02-01", "sell", 500, 150, 0, 0, { broker: "國泰證券" }),
  ];
  const split = computePositions(txs, { byAccount: true });
  const cathay = split.find((p) => p.account === "國泰證券");
  const ya = split.find((p) => p.account === "亞東證券");
  assert.equal(cathay.shares, 500); assert.equal(cathay.avgCost, 100); assert.equal(cathay.realized, 25000);
  assert.equal(ya.shares, 1000); assert.equal(ya.avgCost, 200);
  const [all] = computePositions(txs);
  // 合併：1500 股，成本 50000 + 200000，均價 166.67；已實現以國泰成本 100 計
  assert.equal(all.shares, 1500);
  assert.equal(all.cost, 250000);
  assert.ok(Math.abs(all.avgCost - 166.6667) < 1e-3);
  assert.equal(all.realized, 25000);
  assert.deepEqual(all.accounts.sort(), ["亞東證券", "國泰證券"].sort());
  assert.deepEqual(listAccounts([...txs, tx("2026-03-01", "buy", 1, 1)]), ["亞東證券", "國泰證券", "未指定帳戶"]);
  const curve = equityCurve(txs, { 2330: [{ time: "2026-02-01", close: 150 }] });
  assert.equal(curve.at(-1).cost, 250000);
  assert.equal(curve.at(-1).realized, 25000);
});
