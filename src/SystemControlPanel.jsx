import { useState } from "react";

const API = import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-api-ihun.onrender.com";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export default function SystemControlPanel() {
  const [job, setJob] = useState(null);
  const [log, setLog] = useState([]);
  const [progress, setProgress] = useState({ running: false, phase: "idle", done: 0, total: 0 });
  const [batchSize, setBatchSize] = useState(100);
  const [loops, setLoops] = useState(50);
  const [delaySeconds, setDelaySeconds] = useState(15);
  const [retrySeconds, setRetrySeconds] = useState(30);
  const [months, setMonths] = useState(12);
  const [productType, setProductType] = useState("all");
  const [market, setMarket] = useState("all");

  const addLog = (text) => setLog((prev) => [`${new Date().toLocaleTimeString()} ${text}`, ...prev].slice(0, 24));
  const query = `product_type=${encodeURIComponent(productType)}&market=${encodeURIComponent(market)}`;
  const pct = progress.total ? Math.floor((progress.done / progress.total) * 100) : 0;

  async function api(path, timeoutMs = 25000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(`${API}${path}`, { signal: controller.signal });
      const data = await res.json();
      if (!res.ok) throw new Error(data?.detail || data?.error || `HTTP ${res.status}`);
      setJob(data);
      return data;
    } finally {
      clearTimeout(timer);
    }
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

  async function safeResetLoop() {
    if (progress.running) return;
    const total = loops * batchSize;
    setProgress({ running: true, phase: "安全清空資料", done: 0, total });
    addLog(`開始：安全清空 ${loops} 批，每批 ${batchSize}，成功間隔 ${delaySeconds} 秒，失敗等待 ${retrySeconds} 秒`);

    for (let i = 0; i < loops; i += 1) {
      const offset = i * batchSize;
      const label = `清空第 ${i + 1}/${loops} 批 offset=${offset}`;
      let ok = false;
      while (!ok) {
        const data = await call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${batchSize}`, label);
        if (data) {
          ok = true;
          const done = Math.min((i + 1) * batchSize, total);
          setProgress({ running: true, phase: "安全清空資料", done, total });
          if (data.next_offset === null) {
            addLog("後端回報 next_offset=null，清空流程完成");
            setProgress({ running: false, phase: "清空完成", done, total });
            return;
          }
          await sleep(delaySeconds * 1000);
        } else {
          addLog(`無回應或失敗，等待 ${retrySeconds} 秒後重試同一批`);
          await sleep(retrySeconds * 1000);
        }
      }
    }

    setProgress({ running: false, phase: "清空完成", done: total, total });
    addLog("完成：安全清空 loop");
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
      let ok = false;
      while (!ok) {
        const data = await call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${batchSize}`, `清空 ${offset}-${Math.min(offset + batchSize, total)}`);
        if (data) {
          ok = true;
          setProgress({ running: true, phase: "清空資料", done: Math.min(offset + batchSize, total), total });
          await sleep(delaySeconds * 1000);
        } else {
          await sleep(retrySeconds * 1000);
        }
      }
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
        <p style={{ color: "#94a3b8" }}>安全模式：按一次後，前端自動分批清空；成功等 15 秒，失敗等 30 秒重試同一批。</p>

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
          <label>次數 <input type="number" value={loops} onChange={(e) => setLoops(Number(e.target.value || 50))} style={inputStyle} /></label>
          <label>成功秒 <input type="number" value={delaySeconds} onChange={(e) => setDelaySeconds(Number(e.target.value || 15))} style={inputStyle} /></label>
          <label>重試秒 <input type="number" value={retrySeconds} onChange={(e) => setRetrySeconds(Number(e.target.value || 30))} style={inputStyle} /></label>
          <label>月數 <input type="number" value={months} onChange={(e) => setMonths(Number(e.target.value || 12))} style={inputStyle} /></label>
        </div>

        <button style={{ ...btnStyle, background: "#dc2626", fontSize: 18 }} disabled={progress.running} onClick={safeResetLoop}>💣 安全清空 50 批</button>
        <button style={{ ...btnStyle, background: "#16a34a", fontSize: 18, marginLeft: 10 }} disabled={progress.running} onClick={rebuildAll}>🚀 清空並回補</button>
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
