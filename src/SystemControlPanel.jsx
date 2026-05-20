import React, { useState } from "react";

const API = "https://stock-analysis-api-ihun.onrender.com";

export default function SystemControlPanel() {
  const [result, setResult] = useState(null);
  const [log, setLog] = useState([]);
  const [busy, setBusy] = useState(false);
  const [stock, setStock] = useState("2330");
  const [auditStocks, setAuditStocks] = useState(500);
  const [auditRows, setAuditRows] = useState(30);

  const addLog = (text) => setLog((prev) => [`${new Date().toLocaleTimeString()} ${text}`, ...prev].slice(0, 24));

  async function call(path, label) {
    setBusy(true);
    addLog(`開始：${label}`);
    try {
      const res = await fetch(API + path);
      const json = await res.json();
      setResult(json);
      addLog(`完成：${label}`);
    } catch (e) {
      const message = String(e.message || e);
      setResult({ error: message });
      addLog(`失敗：${label} - ${message}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <section style={pageStyle}>
      <div style={boxStyle}>
        <div style={eyebrowStyle}>系統控制</div>
        <h2 style={titleStyle}>唯讀系統檢查</h2>
        <p style={mutedStyle}>會清除或重建資料庫的功能已移到「資料庫維護」頁籤。</p>

        <div style={buttonRowStyle}>
          <button disabled={busy} onClick={() => call("/", "API 健康檢查")} style={primaryButtonStyle}>API 健康檢查</button>
          <button disabled={busy} onClick={() => call("/api/firebase/test", "Firebase 連線檢查")} style={primaryButtonStyle}>Firebase 連線檢查</button>
          <button disabled={busy} onClick={() => call(`/api/cache/status/${encodeURIComponent(stock)}`, `${stock} 快取狀態`)} style={primaryButtonStyle}>快取狀態</button>
          <button disabled={busy} onClick={() => call(`/api/firebase/audit_all?limit_stocks=${auditStocks}&limit_per_stock=${auditRows}`, "stock_daily 資料稽核")} style={primaryButtonStyle}>stock_daily 資料稽核</button>
        </div>

        <div style={controlRowStyle}>
          <label style={labelStyle}>
            股票代號
            <input value={stock} onChange={(e) => setStock(e.target.value)} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            稽核股票數
            <input type="number" value={auditStocks} onChange={(e) => setAuditStocks(Number(e.target.value || 500))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            每檔檢查筆數
            <input type="number" value={auditRows} onChange={(e) => setAuditRows(Number(e.target.value || 30))} style={inputStyle} />
          </label>
        </div>

        <div style={gridStyle}>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>API 回應</h3>
            <pre style={preStyle}>{JSON.stringify(result || {}, null, 2)}</pre>
          </div>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>操作紀錄</h3>
            {log.map((x, i) => (
              <div key={i} style={logLineStyle}>{x}</div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

const pageStyle = { background: "#020617", color: "white", padding: 18, fontFamily: "Arial, sans-serif" };
const boxStyle = { background: "#0f172a", border: "1px solid #334155", borderRadius: 12, padding: 18 };
const eyebrowStyle = { color: "#38bdf8", fontWeight: 800, letterSpacing: 1 };
const titleStyle = { margin: "8px 0" };
const mutedStyle = { color: "#94a3b8" };
const controlRowStyle = { display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center", marginTop: 12 };
const buttonRowStyle = { display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center", marginTop: 12 };
const labelStyle = { display: "grid", gap: 6, color: "#cbd5e1", fontSize: 13 };
const inputStyle = { width: 110, padding: 10, borderRadius: 10, background: "#020617", color: "white", border: "1px solid #334155" };
const primaryButtonStyle = { padding: "10px 12px", borderRadius: 10, border: 0, background: "#2563eb", color: "white", fontWeight: 800, cursor: "pointer" };
const gridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))", gap: 12, marginTop: 14 };
const panelStyle = { border: "1px solid #1e293b", borderRadius: 12, padding: 12, background: "rgba(15,23,42,.75)" };
const panelTitleStyle = { marginTop: 0 };
const preStyle = { maxHeight: 320, overflow: "auto", color: "#cbd5e1", background: "#020617", padding: 12, borderRadius: 10, fontSize: 12 };
const logLineStyle = { color: "#cbd5e1", fontSize: 13, padding: "3px 0" };
