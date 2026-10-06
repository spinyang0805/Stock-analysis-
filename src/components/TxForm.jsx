// 新增 / 編輯一筆交易（手動輸入）
import React, { useMemo, useState } from "react";
import { Modal } from "./ui.jsx";
import { supabase } from "../lib/supabase.js";
import { useApp } from "../lib/appState.jsx";
import { todayISO } from "../lib/format.js";
import { NO_ACCOUNT, listAccounts } from "../lib/portfolio.js";

function lastAccount() {
  try { return localStorage.getItem("tx.lastAccount") || ""; } catch { return ""; }
}

// 台股預設：手續費 0.1425%（最低 20 元）、證交稅 賣出 0.3%（ETF 0.1%）
function estimate(side, shares, price, code) {
  const amt = shares * price;
  if (!(amt > 0)) return { fee: 0, tax: 0 };
  const fee = side === "buy" || side === "sell" ? Math.max(20, Math.floor(amt * 0.001425)) : 0;
  const etf = /^00/.test(code || "");
  const tax = side === "sell" ? Math.floor(amt * (etf ? 0.001 : 0.003)) : 0;
  return { fee, tax };
}

export default function TxForm({ initial, stock, onClose }) {
  const app = useApp();
  const accounts = useMemo(() => listAccounts(app.transactions).filter((a) => a !== NO_ACCOUNT), [app.transactions]);
  const [f, setF] = useState(() => ({
    broker: initial ? initial.broker || "" : lastAccount() || accounts[0] || "",
    trade_date: initial?.trade_date || todayISO(),
    stock_id: initial?.stock_id || stock?.code || "",
    stock_name: initial?.stock_name || stock?.name || "",
    side: initial?.side || "buy",
    unit: "lots",
    qty: initial ? String(initial.shares) : "1",
    price: initial ? String(initial.price) : stock?.price ? String(stock.price) : "",
    fee: initial ? String(initial.fee) : "",
    tax: initial ? String(initial.tax) : "",
    note: initial?.note || "",
  }));
  const [errors, setErrors] = useState({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const set = (k) => (e) => setF((s) => ({ ...s, [k]: e.target.value }));
  const shares = (Number(f.qty) || 0) * (initial ? 1 : f.unit === "lots" ? 1000 : 1);
  const est = useMemo(() => estimate(f.side, shares, Number(f.price), f.stock_id), [f.side, shares, f.price, f.stock_id]);
  const isDiv = f.side === "cash_dividend";

  async function save() {
    const e = {};
    if (!f.trade_date) e.trade_date = "請選日期";
    if (!/^[0-9A-Z]{4,6}$/i.test(f.stock_id.trim())) e.stock_id = "代號格式不正確";
    if (!(shares > 0)) e.qty = "數量需大於 0";
    if (!(Number(f.price) > 0)) e.price = isDiv ? "請填每股配息" : "請填成交價";
    setErrors(e);
    if (Object.keys(e).length) return;
    setSaving(true); setMsg("");
    const row = {
      trade_date: f.trade_date, stock_id: f.stock_id.trim().toUpperCase(), stock_name: f.stock_name.trim() || null,
      side: f.side, shares, price: Number(f.price),
      fee: f.fee === "" ? est.fee : Number(f.fee), tax: f.tax === "" ? est.tax : Number(f.tax),
      note: f.note.trim() || null,
      broker: f.broker.trim() || null,
    };
    try { if (row.broker) localStorage.setItem("tx.lastAccount", row.broker); } catch { /* ignore */ }
    const q = initial ? supabase.from("transactions").update(row).eq("id", initial.id) : supabase.from("transactions").insert({ ...row, source: "manual" });
    const { error } = await q;
    setSaving(false);
    if (error) { setMsg(`儲存失敗：${error.message}`); return; }
    await app.reload();
    onClose(true);
  }

  return (
    <Modal title={initial ? "編輯交易" : "記一筆交易"} onClose={() => onClose(false)} actions={<>
      <button type="button" className="btn" onClick={() => onClose(false)}>取消</button>
      <button type="button" className="btn primary" onClick={save} disabled={saving}>{saving ? "儲存中…" : "儲存"}</button>
    </>}>
      <div className="form-grid">
        <div className="field">
          <label htmlFor="tx-side">類別</label>
          <select id="tx-side" className="input" value={f.side} onChange={set("side")}>
            <option value="buy">買進</option><option value="sell">賣出</option>
            <option value="cash_dividend">現金股利</option><option value="stock_dividend">股票股利</option>
          </select>
        </div>
        <div className="field">
          <label htmlFor="tx-broker">帳戶</label>
          <input id="tx-broker" className="input" list="tx-broker-list" value={f.broker} onChange={set("broker")} placeholder="例：國泰證券" />
          <datalist id="tx-broker-list">
            {[...new Set([...accounts, "國泰證券", "亞東證券"])].map((a) => <option key={a} value={a} />)}
          </datalist>
        </div>
        <div className="field">
          <label htmlFor="tx-date">日期</label>
          <input id="tx-date" type="date" className="input" value={f.trade_date} onChange={set("trade_date")} />
          {errors.trade_date && <span className="err">{errors.trade_date}</span>}
        </div>
        <div className="field">
          <label htmlFor="tx-code">股票代號</label>
          <input id="tx-code" className="input" value={f.stock_id} onChange={set("stock_id")} inputMode="text" autoCapitalize="characters" />
          {errors.stock_id && <span className="err">{errors.stock_id}</span>}
        </div>
        <div className="field">
          <label htmlFor="tx-name">名稱（選填）</label>
          <input id="tx-name" className="input" value={f.stock_name} onChange={set("stock_name")} />
        </div>
        <div className="field">
          <label htmlFor="tx-qty">{isDiv ? "持有股數" : "數量"}</label>
          <div style={{ display: "flex", gap: 6 }}>
            <input id="tx-qty" className="input" inputMode="decimal" value={f.qty} onChange={set("qty")} style={{ minWidth: 0 }} />
            {!initial && (
              <select className="input" aria-label="數量單位" value={f.unit} onChange={set("unit")} style={{ width: 76 }}>
                <option value="lots">張</option><option value="shares">股</option>
              </select>
            )}
          </div>
          {errors.qty && <span className="err">{errors.qty}</span>}
          <span className="dim">= {shares.toLocaleString()} 股</span>
        </div>
        <div className="field">
          <label htmlFor="tx-price">{isDiv ? "每股配息（元）" : f.side === "stock_dividend" ? "（不影響成本，可填 0.01）" : "成交價"}</label>
          <input id="tx-price" className="input" inputMode="decimal" value={f.price} onChange={set("price")} />
          {errors.price && <span className="err">{errors.price}</span>}
        </div>
        {!isDiv && f.side !== "stock_dividend" && (
          <>
            <div className="field">
              <label htmlFor="tx-fee">手續費</label>
              <input id="tx-fee" className="input" inputMode="decimal" value={f.fee} placeholder={`自動 ${est.fee}`} onChange={set("fee")} />
            </div>
            <div className="field">
              <label htmlFor="tx-tax">交易稅</label>
              <input id="tx-tax" className="input" inputMode="decimal" value={f.tax} placeholder={`自動 ${est.tax}`} onChange={set("tax")} />
            </div>
          </>
        )}
        <div className="field" style={{ gridColumn: "1 / -1" }}>
          <label htmlFor="tx-note">備註</label>
          <input id="tx-note" className="input" value={f.note} onChange={set("note")} />
        </div>
      </div>
      <div className="dim" style={{ marginTop: 8 }}>手續費與交易稅留空會自動以 0.1425%（最低 20 元）與 0.3%（ETF 0.1%）估算；有券商折扣請直接填實際金額。</div>
      {msg && <div className="notice err" style={{ marginTop: 10 }}>{msg}</div>}
    </Modal>
  );
}
