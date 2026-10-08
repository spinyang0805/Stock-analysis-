// 券商對帳單 CSV 匯入：解碼（UTF-8 / Big5）→ 找表頭 → 欄位對應 → 轉成 transactions 列
// 國泰、亞東等券商格式不同：先用關鍵字自動對應，使用者可手動改，並存成預設（import_presets）
import { toISODate } from "./format.js";

export const FIELDS = [
  { key: "date", label: "成交日期", required: true, words: ["成交日期", "交易日期", "成交日", "交易日", "日期"] },
  { key: "code", label: "股票代號", required: true, words: ["股票代號", "證券代號", "商品代號", "代號", "股票代碼", "代碼", "商品"] },
  { key: "name", label: "股票名稱", words: ["股票名稱", "證券名稱", "商品名稱", "名稱", "股名"] },
  { key: "side", label: "買賣別", required: true, words: ["買賣別", "交易類別", "交易別", "買賣", "種類", "類別", "委託別"] },
  { key: "shares", label: "成交股數", words: ["成交股數", "股數", "成交數量", "數量"] },
  { key: "lots", label: "成交張數", words: ["成交張數", "張數"] },
  { key: "price", label: "成交價", required: true, words: ["成交價格", "成交均價", "成交價", "成交單價", "單價", "價格"] },
  { key: "fee", label: "手續費", words: ["手續費"] },
  { key: "tax", label: "交易稅", words: ["證交稅", "交易稅", "稅額", "交易稅額"] },
  { key: "amount", label: "淨收付金額", words: ["淨收付金額", "應收付金額", "淨收付", "應收付", "收付金額", "淨額", "金額"] },
  { key: "note", label: "備註", words: ["備註", "說明"] },
];

// 位元組 → 文字：UTF-8 有亂碼就改用 Big5（台灣券商常見）
export function decodeBytes(buf) {
  const utf8 = new TextDecoder("utf-8").decode(buf);
  if (!utf8.includes("�")) return utf8.replace(/^﻿/, "");
  try {
    return new TextDecoder("big5").decode(buf);
  } catch {
    return utf8;
  }
}

// xlsx 對帳單 → 字串二維陣列（與 parseCSV 輸出同形，後續流程共用）。只讀第一個工作表
export async function parseXlsx(buf) {
  const { default: readXlsxFile } = await import("read-excel-file/browser");
  const sheets = await readXlsxFile(buf);
  const cell = (v) => {
    if (v instanceof Date) return v.toISOString().slice(0, 10);
    return v === null || v === undefined ? "" : String(v).trim();
  };
  return (sheets[0]?.data || []).map((r) => r.map(cell)).filter((r) => r.some((c) => c !== ""));
}

export function parseCSV(text) {
  const rows = [];
  let row = [], cell = "", q = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (q) {
      if (ch === '"') {
        if (text[i + 1] === '"') { cell += '"'; i++; } else q = false;
      } else cell += ch;
    } else if (ch === '"') q = true;
    else if (ch === "," || ch === "\t") { row.push(cell); cell = ""; }
    else if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && text[i + 1] === "\n") i++;
      row.push(cell); rows.push(row); row = []; cell = "";
    } else cell += ch;
  }
  if (cell !== "" || row.length) { row.push(cell); rows.push(row); }
  return rows.map((r) => r.map((c) => c.trim())).filter((r) => r.some((c) => c !== ""));
}

const norm = (s) => String(s || "").replace(/\s|　|\(.*?\)|（.*?）/g, "");

// 找表頭列：前 15 列中命中最多欄位關鍵字的那一列
export function detectHeader(rows) {
  let best = { idx: 0, hits: -1 };
  rows.slice(0, 15).forEach((r, idx) => {
    const hits = r.filter((c) => FIELDS.some((f) => f.words.some((w) => norm(c).includes(w)))).length;
    if (hits > best.hits) best = { idx, hits };
  });
  return best.idx;
}

// 自動對應：每個欄位取最長相符關鍵字的表頭
export function guessMapping(header) {
  const mapping = {};
  const used = new Set();
  for (const f of FIELDS) {
    let pick = null, score = 0;
    header.forEach((h, i) => {
      if (used.has(i)) return;
      const n = norm(h);
      for (const w of f.words) {
        if (n === w && w.length + 100 > score) { pick = i; score = w.length + 100; }
        else if (n.includes(w) && w.length > score) { pick = i; score = w.length; }
      }
    });
    if (pick !== null) { mapping[f.key] = header[pick]; used.add(pick); }
  }
  // 亞東、國泰台股把代號放在名稱欄的括號裡，例如「台積電(2330)」
  if (!mapping.code && mapping.name) mapping.code = mapping.name;
  return mapping;
}

export function toNumber(v) {
  let s = String(v ?? "").replace(/[,\s$元]/g, "");
  if (!s || s === "-") return NaN;
  let neg = false;
  if (/^\(.*\)$/.test(s)) { neg = true; s = s.slice(1, -1); }
  const n = Number(s);
  return Number.isFinite(n) ? (neg ? -n : n) : NaN;
}

export function parseSide(v) {
  const s = String(v || "");
  if (/現金增資|現金增股|現增|增資/.test(s)) return "buy"; // 現金增資認購：出資買新股，不是股利
  if (/股票股利|配股|無償|stock\s*div/i.test(s)) return "stock_dividend";
  if (/股息|股利|配息|現金|dividend/i.test(s)) return "cash_dividend";
  if ((/賣/.test(s) && !/買/.test(s)) || /^\s*(S|SELL)\s*$/i.test(s)) return "sell";
  if (/買/.test(s) || /^\s*(B|BUY)\s*$/i.test(s)) return "buy";
  return null;
}

export function parseCode(v) {
  const s = String(v || "");
  const paren = s.match(/[(（]\s*([0-9]{4,6}[A-Z]?)\s*[)）]/); // 名稱(代號) 優先，避免名稱內的數字被誤抓
  if (paren) return paren[1];
  const m = s.match(/([0-9]{4,6}[A-Z]?)/);
  return m ? m[1] : null;
}

// rows：去掉表頭後的資料列；header：表頭陣列；mapping：{ fieldKey: 表頭文字 }
// 回傳 [{ ok, errors[], tx, raw, line }]
export function convertRows(header, rows, mapping, { broker = null, headerLine = 0 } = {}) {
  const col = (key) => (mapping[key] ? header.indexOf(mapping[key]) : -1);
  const idx = Object.fromEntries(FIELDS.map((f) => [f.key, col(f.key)]));
  const get = (r, key) => (idx[key] >= 0 ? r[idx[key]] : "");
  const seen = new Map();
  // 價格 0 的賣出 = 股票分割／減資換股的一半：同檔的 0 元買進不能當配股，兩邊都擋下請手動處理
  const zeroSell = new Set(rows.filter((r) => parseSide(get(r, "side")) === "sell" && !(Math.abs(toNumber(get(r, "price"))) > 0)).map((r) => parseCode(get(r, "code"))));
  return rows.map((r, i) => {
    const errors = [];
    const date = toISODate(get(r, "date"));
    if (!date) errors.push("日期無法辨識");
    const code = parseCode(get(r, "code"));
    if (!code) errors.push("股票代號無法辨識");
    let side = parseSide(get(r, "side"));
    // 價格與金額都是 0 的「買進」= 配股（券商常把配股記成現股買進）
    if (side === "buy" && !(Math.abs(toNumber(get(r, "price"))) > 0) && !(Math.abs(toNumber(get(r, "amount"))) > 0) && !zeroSell.has(code)) side = "stock_dividend";
    if (!side) errors.push(`買賣別「${get(r, "side")}」無法辨識`);
    let shares = toNumber(get(r, "shares"));
    if (!Number.isFinite(shares) && idx.lots >= 0) shares = toNumber(get(r, "lots")) * 1000;
    shares = Math.abs(shares);
    const price = Math.abs(toNumber(get(r, "price")));
    if (side === "buy" || side === "sell") {
      if (!(shares > 0)) errors.push("股數無效");
      if (!(price > 0)) errors.push("成交價無效");
    }
    const fee = Math.abs(toNumber(get(r, "fee"))) || 0;
    const tax = Math.abs(toNumber(get(r, "tax"))) || 0;
    const amount = toNumber(get(r, "amount"));
    if (side === "cash_dividend" && !(Math.abs(amount) > 0) && !(shares > 0 && price > 0)) errors.push("股利金額無效");
    const base = [date, code, side, shares, price, fee, tax].join("|");
    const n = (seen.get(base) || 0) + 1;
    seen.set(base, n);
    const tx = errors.length ? null : {
      trade_date: date, stock_id: code, stock_name: String(get(r, "name") || "").replace(/\s*[(（][0-9A-Z]{4,6}[)）]\s*$/, "") || null, side,
      shares: Number.isFinite(shares) ? shares : 0, price: Number.isFinite(price) ? price : 0,
      fee, tax, amount: Number.isFinite(amount) ? amount : null,
      broker, source: "csv", note: get(r, "note") || null,
      import_hash: `${base}|#${n}`,
    };
    return { ok: !errors.length, errors, tx, raw: r, line: headerLine + i + 2 };
  });
}
