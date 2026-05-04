import { useState } from "react";

const API = import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-api-ihun.onrender.com";

export default function SystemControlPanel() {
  const [job, setJob] = useState(null);
  const [log, setLog] = useState([]);
  const [offset, setOffset] = useState(0);
  const [limit, setLimit] = useState(100);
  const [months, setMonths] = useState(12);
  const [productType, setProductType] = useState("股票");
  const [market, setMarket] = useState("上市");

  const addLog = (text) => setLog((prev) => [`${new Date().toLocaleTimeString()} ${text}`, ...prev].slice(0, 12));

  async function call(path, label) {
    addLog(`開始：${label}`);
    try {
      const res = await fetch(`${API}${path}`);
      const data = await res.json();
      setJob(data);
      addLog(`完成：${label}`);
    } catch (e) {
      setJob({ error: e.message });
      addLog(`失敗：${label} - ${e.message}`);
    }
  }

  const query = `product_type=${encodeURIComponent(productType)}&market=${encodeURIComponent(market)}`;

  return (
    <section style={{ background: "#020617", color: "white", padding: 18, fontFamily: "Arial, sans-serif" }}>
      <div style={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 18, padding: 18 }}>
        <div style={{ color: "#38bdf8", fontWeight: 800, letterSpacing: 1 }}>SYSTEM MAINTENANCE CONSOLE</div>
        <h2 style={{ margin: "8px 0" }}>資料維護控制台</h2>
        <p style={{ color: "#94a3b8" }}>一鍵檢查、清空、回補上市 / 上櫃 / ETF 資料，並可快速除錯。</p>

        <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center", marginBottom: 12 }}>
          <select value={productType} onChange={(e) => setProductType(e.target.value)} style={fieldStyle}>
            <option value="股票">股票</option>
            <option value="ETF">ETF</option>
            <option value="債券ETF">債券ETF</option>
            <option value="all">全部</option>
          </select>
          <select value={market} onChange={(e) => setMarket(e.target.value)} style={fieldStyle}>
            <option value="上市">上市</option>
            <option value="上櫃">上櫃</option>
            <option value="all">全部</option>
          </select>
          <label>Offset <input type="number" value={offset} onChange={(e) => setOffset(Number(e.target.value))} style={inputStyle} /></label>
          <label>Limit <input type="number" value={limit} onChange={(e) => setLimit(Number(e.target.value))} style={inputStyle} /></label>
          <label>Months <input type="number" value={months} onChange={(e) => setMonths(Number(e.target.value))} style={inputStyle} /></label>
        </div>

        <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
          <button style={btnStyle} onClick={() => call(`/api/products?${query}&limit=5000`, "讀取商品清單")}>📋 讀取清單</button>
          <button style={btnStyle} onClick={() => call(`/api/firebase/audit_all`, "資料品質檢查")}>📊 檢查資料</button>
          <button style={{ ...btnStyle, background: "#dc2626" }} onClick={() => call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${limit}`, "清空本批資料")}>💣 清空本批</button>
          <button style={{ ...btnStyle, background: "#16a34a" }} onClick={() => call(`/api/job/backfill_all?${query}&offset=${offset}&limit=${limit}&months=${months}`, "回補本批資料")}>🔄 回補本批</button>
          <button style={{ ...btnStyle, background: "#7c3aed" }} onClick={() => call(`/api/job/daily`, "更新今日資料")}>📅 更新今日</button>
          <button style={{ ...btnStyle, background: "#f59e0b" }} onClick={() => call(`/api/kline/2330`, "測試2330")}>🧪 測試2330</button>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))", gap: 12, marginTop: 14 }}>
          <div style={panelStyle}>
            <h3>API 回傳</h3>
            <pre style={preStyle}>{JSON.stringify(job || {}, null, 2)}</pre>
          </div>
          <div style={panelStyle}>
            <h3>操作紀錄</h3>
            {log.map((x, i) => <div key={i} style={{ color: "#cbd5e1", fontSize: 13, padding: "3px 0" }}>{x}</div>)}
          </div>
        </div>
      </div>
    </section>
  );
}

const fieldStyle = { padding: 10, borderRadius: 10, background: "#020617", color: "white", border: "1px solid #334155" };
const inputStyle = { width: 80, padding: 8, borderRadius: 10, background: "#020617", color: "white", border: "1px solid #334155" };
const btnStyle = { padding: "10px 12px", borderRadius: 10, border: 0, background: "#2563eb", color: "white", fontWeight: 800, cursor: "pointer" };
const panelStyle = { border: "1px solid #1e293b", borderRadius: 14, padding: 12, background: "rgba(15,23,42,.75)" };
const preStyle = { maxHeight: 320, overflow: "auto", color: "#cbd5e1", background: "#020617", padding: 12, borderRadius: 12, fontSize: 12 };
