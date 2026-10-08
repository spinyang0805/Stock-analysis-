// 交易紀錄：清單（篩選、編輯、刪除）、手動新增、CSV 匯入
import React, { useMemo, useState } from "react";
import { FileUp, ListOrdered, Pencil, Plus, Trash2 } from "lucide-react";
import CsvImport from "../components/CsvImport.jsx";
import TxForm from "../components/TxForm.jsx";
import { Card, Empty } from "../components/ui.jsx";
import { useApp } from "../lib/appState.jsx";
import { navigate, useRoute } from "../lib/router.js";
import { supabase } from "../lib/supabase.js";
import { SIDE_LABEL, sideLabel, accountOf, cashDividendAmount, listAccounts } from "../lib/portfolio.js";
import { fmt, fmtInt } from "../lib/format.js";

export default function TransactionsPage() {
  const app = useApp();
  const route = useRoute();
  const showImport = route.query.import === "1";
  const [filter, setFilter] = useState("");
  const [side, setSide] = useState("");
  const [acc, setAcc] = useState("");
  const accounts = useMemo(() => listAccounts(app.transactions), [app.transactions]);
  const [modal, setModal] = useState(null);
  const [selected, setSelected] = useState(new Set());
  const [msg, setMsg] = useState("");

  const list = useMemo(() => {
    const f = filter.trim().toLowerCase();
    return [...app.transactions].reverse().filter((t) =>
      (!f || t.stock_id.toLowerCase().includes(f) || String(t.stock_name || "").toLowerCase().includes(f)) && (!side || t.side === side) && (!acc || accountOf(t) === acc));
  }, [app.transactions, filter, side, acc]);

  if (!app.user) {
    return <div className="page"><Card><Empty icon={ListOrdered} title="登入後管理交易紀錄"><button type="button" className="btn primary" onClick={app.signIn}>Google 登入</button></Empty></Card></div>;
  }

  async function removeSelected() {
    if (!selected.size || !window.confirm(`刪除選取的 ${selected.size} 筆交易？此動作無法復原。`)) return;
    const { error } = await supabase.from("transactions").delete().in("id", [...selected]);
    if (error) { setMsg(`刪除失敗：${error.message}`); return; }
    setMsg("");
    setSelected(new Set());
    app.reload();
  }

  const toggle = (id) => setSelected((s) => { const n = new Set(s); if (n.has(id)) n.delete(id); else n.add(id); return n; });

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">TRANSACTIONS</div>
          <h1 className="page-title">交易紀錄</h1>
          <div className="dim">{app.transactions.length} 筆</div>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button type="button" className="btn" aria-pressed={showImport} onClick={() => route.setQuery({ import: showImport ? "" : "1" })}><FileUp size={16} />匯入 CSV</button>
          <button type="button" className="btn primary" onClick={() => setModal({})}><Plus size={16} />記一筆</button>
        </div>
      </div>

      {msg && <div className="notice err" style={{ marginBottom: 12 }}>{msg}</div>}
      {showImport && <div style={{ marginBottom: 12 }}><CsvImport onDone={() => navigate("/portfolio")} /></div>}

      <Card>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 10, alignItems: "center" }}>
          <input className="input" style={{ maxWidth: 220 }} placeholder="篩選代號或名稱" aria-label="篩選代號或名稱" value={filter} onChange={(e) => setFilter(e.target.value)} />
          <select className="input" style={{ maxWidth: 150 }} aria-label="類別" value={side} onChange={(e) => setSide(e.target.value)}>
            <option value="">全部類別</option>
            {Object.entries(SIDE_LABEL).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
          </select>
          {accounts.length > 1 && (
            <select className="input" style={{ maxWidth: 180 }} aria-label="帳戶" value={acc} onChange={(e) => setAcc(e.target.value)}>
              <option value="">全部帳戶</option>
              {accounts.map((a) => <option key={a} value={a}>{a}</option>)}
            </select>
          )}
          {selected.size > 0 && <button type="button" className="btn danger" onClick={removeSelected}><Trash2 size={16} />刪除 {selected.size} 筆</button>}
        </div>
        {!list.length ? <Empty icon={ListOrdered} title="沒有符合的交易" /> : (
          <div className="table-wrap">
            <table className="data">
              <thead>
                <tr>
                  <th className="left"><input type="checkbox" aria-label="全選" checked={selected.size === list.length} onChange={(e) => setSelected(e.target.checked ? new Set(list.map((t) => t.id)) : new Set())} /></th>
                  <th className="left">日期</th><th className="left">股票</th><th className="left">類別</th><th>股數</th><th>價格</th><th>手續費</th><th>稅</th><th>金額</th><th className="left">帳戶</th><th aria-label="操作" />
                </tr>
              </thead>
              <tbody>
                {list.slice(0, 500).map((t) => {
                  const gross = t.side === "cash_dividend" ? cashDividendAmount(t) : t.shares * t.price;
                  return (
                    <tr key={t.id}>
                      <td className="left"><input type="checkbox" aria-label={`選取 ${t.trade_date} ${t.stock_id}`} checked={selected.has(t.id)} onChange={() => toggle(t.id)} /></td>
                      <td className="left num">{t.trade_date}</td>
                      <td className="left"><a href={`#/stock/${t.stock_id}?tab=position`}><b className="num">{t.stock_id}</b></a> {t.stock_name}</td>
                      <td className="left"><span className={t.side === "buy" ? "up" : t.side === "sell" ? "down" : ""}>{sideLabel(t)}</span></td>
                      <td>{fmtInt(t.shares)}</td><td>{fmt(t.price)}</td><td>{fmtInt(t.fee)}</td><td>{fmtInt(t.tax)}</td>
                      <td>{fmtInt(gross)}</td>
                      <td className="left dim">{accountOf(t)}</td>
                      <td>
                        <button type="button" className="btn ghost sm" aria-label="編輯" onClick={() => setModal({ initial: t })}><Pencil size={14} /></button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {list.length > 500 && <div className="dim">只顯示最新 500 筆，請用篩選縮小範圍。</div>}
          </div>
        )}
      </Card>
      {modal && <TxForm initial={modal.initial} onClose={() => setModal(null)} />}
    </div>
  );
}
