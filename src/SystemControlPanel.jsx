import { useState } from "react";

const API = import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-api-ihun.onrender.com";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export default function SystemControlPanel() {
  const [job, setJob] = useState(null);
  const [log, setLog] = useState([]);
  const [progress, setProgress] = useState({ running: false, phase: "idle", done: 0, total: 0 });
  const [batchSize, setBatchSize] = useState(100);
  const [months, setMonths] = useState(12);
  const [productType, setProductType] = useState("all");
  const [market, setMarket] = useState("all");

  const addLog = (text) => setLog((prev) => [`${new Date().toLocaleTimeString()} ${text}`, ...prev].slice(0, 18));
  const query = `product_type=${encodeURIComponent(productType)}&market=${encodeURIComponent(market)}`;
  const pct = progress.total ? Math.floor((progress.done / progress.total) * 100) : 0;

  async function api(path) {
    const res = await fetch(`${API}${path}`);
    const data = await res.json();
    setJob(data);
    return data;
  }

  async function call(path, label) {
    addLog(`開始：${label}`);
    try {
      const data = await api(path);
      addLog(`完成：${label}`);
      return data;
    } catch (e) {
      setJob({ error: e.message });
      addLog(`失敗：${label} - ${e.message}`);
      return null;
    }
  }

  async function rebuildAll() {
    if (progress.running) return;
    setProgress({ running: true, phase: "讀取清單", done: 0, total: 0 });
    addLog("開始：一鍵重建全部資料");

    const productResult = await call(`/api/products?${query}&limit=5000`, "讀取全部商品清單");
    const total = Number(productResult?.count || 0);
    if (!total) {
      setProgress({ running: false, phase: "失敗：無商品清單", done: 0, total: 0 });
      return;
    }

    setProgress({ running: true, phase: "清空資料", done: 0, total });
    for (let offset = 0; offset < total; offset += batchSize) {
      await call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${batchSize}`, `清空 ${offset}-${Math.min(offset + batchSize, total)}`);
      setProgress({ running: true, phase: "清空資料", done: Math.min(offset + batchSize, total), total });
      await sleep(500);
    }

    setProgress({ running: true, phase: "回補資料", done: 0, total });
    for (let offset = 0; offset < total; offset += batchSize) {
      await call(`/api/job/backfill_all?${query}&offset=${offset}&limit=${batchSize}&months=${months}`, `回補 ${offset}-${Math.min(offset + batchSize, total)}`);
      setProgress({ running: true, phase: "回補資料", done: Math.min(offset + batchSize, total), total });
      await sleep(1200);
    }

    setProgress({ running: true, phase: "更新今日資料", done: total, total });
    await call(`/api/job/daily`, "更新今日資料");
    setProgress({ running: false, phase: "完成", done: total, total });
    addLog("完成：一鍵重建全部資料");
  }

  return (
    <section style={{ background: "#020617", color: "white", padding: 18, fontFamily: "Arial, sans-serif" }}>
      <div style={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 18, padding: 18 }}>
        <div style={{ color: "#38bdf8", fontWeight: 800, letterSpacing: 1 }}>SYSTEM MAINTENANCE CONSOLE</div>
        <h2 style={{ margin: "8px 0" }}>資料維護控制台</h2>
        <p style={{ color: "#94a3b8" }}>不用輸入區間，按一次即可依商品清單自動清空、分批回補、更新今日，並顯示進度。</p>

        <div style={{ display: "flex", flexWrap: "wrap", gap: 10, alignItems: "center", marginBottom: 12 }}>
          <select value={productType} onChange={(e) => setProductType(e.target.value)} style={fieldStyle}>
            <option value="all">全部</option>
            <option value="股票">股票</option>
            <option value="ETF">ETF</option>
            <option value="債券ETF">債券ETF</option>
          </select>
          <select value={market} onChange={(e) => setMarket(e.target.value)} style={fieldStyle}>
            <option value="all">全部市場</option>
            <option value="上市">上市</option>
            <option value="上櫃">上櫃</option>
          </select>
          <label>每批 <input type="number" value={batchSize} onChange={(e) => setBatchSize(Number(e.target.value || 100))} style={inputStyle} /></label>
          <label>月數 <input type="number" value={months} onChange={(e) => setMonths(Number(e.target.value || 12))} style={inputStyle} /></label>
        </div>

        <button style={{ ...btnStyle, background: "#16a34a", fontSize: 18 }} disabled={progress.running} onClick={rebuildAll}>🚀 一鍵清空並重建全部</button>
        <button style={{ ...btnStyle, marginLeft: 10 }} onClick={() => call(`/api/kline/2330`, "測試2330")}>🧪 測試2330</button>

        <div style={{ marginTop: 16, background: "#020617", borderRadius: 999, overflow: "hidden", border: "1px solid #334155" }}>
          <div style={{ width: `${pct}%`, height: 18, background: "#22c55e", transition: "width .3s" }} />
        </div>
        <div style={{ marginTop: 8, color: "#cbd5e1" }}>階段：{progress.phase}　進度：{progress.done}/{progress.total}　{pct}%</div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))", gap: 12, marginTop: 14 }}>
          <div style={panelStyle}><h3>API 回傳</h3><pre style={preStyle}>{JSON.stringify(job || {}, null, 2)}</pre></div>
          <div style={panelStyle}><h3>操作紀錄</h3>{log.map((x, i) => <div key={i} style={{ color: "#cbd5e1", fontSize: 13, padding: "3px 0" }}>{x}</div>)}</div>
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
