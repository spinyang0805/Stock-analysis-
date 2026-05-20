import React, { useState } from "react";

const API = "https://stock-analysis-api-ihun.onrender.com";
const PAGE_VERSION = "db-maintenance-v2-zh-tw";
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export default function DatabaseMaintenancePage() {
  const [universeOffset, setUniverseOffset] = useState(0);
  const [universeLimit, setUniverseLimit] = useState(10);
  const [universeTotal, setUniverseTotal] = useState(0);
  const [universeDone, setUniverseDone] = useState(false);
  const [universeBusy, setUniverseBusy] = useState(false);
  const [universeResult, setUniverseResult] = useState(null);

  const [job, setJob] = useState(null);
  const [log, setLog] = useState([]);
  const [progress, setProgress] = useState({ running: false, phase: "待命", done: 0, total: 0 });
  const [batchSize, setBatchSize] = useState(20);
  const [delaySeconds, setDelaySeconds] = useState(30);
  const [retrySeconds, setRetrySeconds] = useState(30);
  const [months, setMonths] = useState(12);
  const [productType, setProductType] = useState("all");
  const [market, setMarket] = useState("all");

  const addLog = (text) => setLog((prev) => [`${new Date().toLocaleTimeString()} ${text}`, ...prev].slice(0, 30));
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

  async function call(path, label, timeoutMs = 25000) {
    addLog(`開始：${label}`);
    try {
      const data = await api(path, timeoutMs);
      addLog(`完成：${label}`);
      return data;
    } catch (e) {
      const message = e.name === "AbortError" ? "請求逾時" : e.message;
      setJob({ error: message });
      addLog(`失敗：${label} - ${message}`);
      return null;
    }
  }

  async function runUniverse(path) {
    setUniverseBusy(true);
    try {
      const res = await fetch(API + path);
      const json = await res.json();
      setUniverseResult(json);
      if (json.count) setUniverseTotal(Number(json.count));

      if (json.next_offset === null) {
        setUniverseDone(true);
        if (json.count) setUniverseOffset(Number(json.count));
      } else if (json.next_offset !== undefined) {
        setUniverseOffset(Number(json.next_offset));
      }
    } catch (e) {
      setUniverseResult({ error: String(e.message || e) });
    } finally {
      setUniverseBusy(false);
    }
  }

  async function getProductTotal() {
    const productResult = await call(`/api/products?${query}&limit=5000`, "讀取商品清單", 120000);
    return Number(productResult?.count || 0);
  }

  async function safeResetLoop() {
    if (progress.running) return;
    if (!window.confirm("這會清除所選商品的 stock_daily 與 analysis_cache。確定繼續？")) return;

    setProgress({ running: true, phase: "讀取商品清單", done: 0, total: 0 });
    const total = await getProductTotal();
    if (!total) {
      setProgress({ running: false, phase: "失敗：沒有商品資料", done: 0, total: 0 });
      return;
    }

    setProgress({ running: true, phase: "清除資料庫快取", done: 0, total });
    addLog(`開始清除 ${total} 檔商品，批次=${batchSize}，間隔=${delaySeconds}秒，重試=${retrySeconds}秒`);

    for (let offset = 0; offset < total; offset += batchSize) {
      const label = `清除 ${offset} 到 ${Math.min(offset + batchSize, total)}`;
      let ok = false;
      while (!ok) {
        const data = await call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${batchSize}`, label);
        if (data) {
          ok = true;
          const done = Math.min(offset + batchSize, total);
          setProgress({ running: true, phase: "清除資料庫快取", done, total });
          if (data.next_offset === null) {
            setProgress({ running: false, phase: "清除完成", done, total });
            addLog("後端回傳 next_offset=null，清除完成。");
            return;
          }
          await sleep(delaySeconds * 1000);
        } else {
          addLog(`${retrySeconds} 秒後重試`);
          await sleep(retrySeconds * 1000);
        }
      }
    }

    setProgress({ running: false, phase: "清除完成", done: total, total });
    addLog("清除完成。");
  }

  async function rebuildAll() {
    if (progress.running) return;
    if (!window.confirm("這會先清除所選資料，再回補 stock_daily。確定繼續？")) return;

    setProgress({ running: true, phase: "讀取商品清單", done: 0, total: 0 });
    addLog("開始清除並重建流程。");

    const total = await getProductTotal();
    if (!total) {
      setProgress({ running: false, phase: "失敗：沒有商品資料", done: 0, total: 0 });
      return;
    }

    setProgress({ running: true, phase: "清除資料庫快取", done: 0, total });
    for (let offset = 0; offset < total; offset += batchSize) {
      let ok = false;
      while (!ok) {
        const data = await call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${batchSize}`, `清除 ${offset}-${Math.min(offset + batchSize, total)}`);
        if (data) {
          ok = true;
          setProgress({ running: true, phase: "清除資料庫快取", done: Math.min(offset + batchSize, total), total });
          await sleep(delaySeconds * 1000);
        } else {
          addLog(`清除失敗，${retrySeconds} 秒後重試`);
          await sleep(retrySeconds * 1000);
        }
      }
    }

    setProgress({ running: true, phase: "回補股價日線", done: 0, total });
    for (let offset = 0; offset < total; offset += batchSize) {
      let ok = false;
      while (!ok) {
        const data = await call(`/api/job/backfill_all?${query}&offset=${offset}&limit=${batchSize}&months=${months}`, `回補 ${offset}-${Math.min(offset + batchSize, total)}`);
        if (data) {
          ok = true;
          setProgress({ running: true, phase: "回補股價日線", done: Math.min(offset + batchSize, total), total });
          await sleep(delaySeconds * 1000);
        } else {
          addLog(`回補失敗，${retrySeconds} 秒後重試`);
          await sleep(retrySeconds * 1000);
        }
      }
    }

    setProgress({ running: true, phase: "執行每日更新", done: total, total });
    await call("/api/job/daily", "每日資料更新");
    setProgress({ running: false, phase: "重建完成", done: total, total });
    addLog("清除並重建流程完成。");
  }

  async function runDailyUpdate() {
    if (progress.running) return;
    setProgress({ running: true, phase: "啟動每日更新", done: 0, total: 1 });
    await call("/api/job/daily", "每日股價與籌碼更新");
    setProgress({ running: false, phase: "每日更新已排入背景", done: 1, total: 1 });
  }

  async function runChipHistoryBackfill() {
    if (progress.running) return;
    if (!window.confirm("這會啟動約一年的籌碼歷史資料背景回補。確定繼續？")) return;
    setProgress({ running: true, phase: "啟動籌碼歷史回補", done: 0, total: 1 });
    await call("/api/chip/backfill_history_all?months=12", "回補一年籌碼歷史");
    setProgress({ running: false, phase: "籌碼歷史回補已排入背景", done: 1, total: 1 });
  }

  async function runStockYearlyBackfill() {
    if (progress.running) return;
    if (!window.confirm("這會啟動所選商品約一年的股價歷史背景回補。確定繼續？")) return;
    setProgress({ running: true, phase: "啟動一年股價回補", done: 0, total: 1 });
    await call(`/api/job/backfill_all_yearly?${query}&months=12`, "回補一年股價歷史");
    setProgress({ running: false, phase: "一年股價回補已排入背景", done: 1, total: 1 });
  }

  function resetUniverseState() {
    setUniverseOffset(0);
    setUniverseDone(false);
    setUniverseResult(null);
    setUniverseTotal(0);
  }

  return (
    <div style={pageStyle}>
      <div style={headerStyle}>
        <div style={eyebrowStyle}>資料庫維護</div>
        <h2 style={titleStyle}>股票同步與資料庫重建</h2>
        <div style={mutedStyle}>{PAGE_VERSION}</div>
      </div>

      <section style={boxStyle}>
        <h3 style={sectionTitleStyle}>商品清單同步</h3>
        <p style={mutedStyle}>目前進度：{universeOffset} / {universeTotal || "?"} {universeDone ? "已完成" : ""}</p>

        <div style={controlRowStyle}>
          <label style={labelStyle}>
            起始位置
            <input value={universeOffset} onChange={(e) => setUniverseOffset(Number(e.target.value || 0))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            批次筆數
            <input value={universeLimit} onChange={(e) => setUniverseLimit(Number(e.target.value || 10))} style={inputStyle} />
          </label>
        </div>

        <div style={buttonRowStyle}>
          <button disabled={universeBusy} onClick={() => runUniverse("/api/init_universe")} style={primaryButtonStyle}>檢查商品清單</button>
          <button disabled={universeBusy || universeDone} onClick={() => runUniverse(`/api/init_universe_batch?offset=${universeOffset}&limit=${universeLimit}`)} style={primaryButtonStyle}>
            {universeDone ? "已完成" : "執行本批同步"}
          </button>
          <button disabled={universeBusy} onClick={resetUniverseState} style={secondaryButtonStyle}>重設狀態</button>
        </div>

        <pre style={preStyle}>{JSON.stringify(universeResult || {}, null, 2)}</pre>
      </section>

      <section style={boxStyle}>
        <h3 style={sectionTitleStyle}>清除 / 重建股票快取</h3>
        <div style={controlRowStyle}>
          <label style={labelStyle}>
            商品類型
            <select value={productType} onChange={(e) => setProductType(e.target.value)} style={fieldStyle}>
              <option value="all">全部</option>
              <option value="股票">股票</option>
              <option value="ETF">ETF</option>
              <option value="高股息ETF">高股息 ETF</option>
            </select>
          </label>
          <label style={labelStyle}>
            市場
            <select value={market} onChange={(e) => setMarket(e.target.value)} style={fieldStyle}>
              <option value="all">全部</option>
              <option value="上市">上市</option>
              <option value="上櫃">上櫃</option>
            </select>
          </label>
          <label style={labelStyle}>
            批次筆數
            <input type="number" value={batchSize} onChange={(e) => setBatchSize(Number(e.target.value || 20))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            間隔秒數
            <input type="number" value={delaySeconds} onChange={(e) => setDelaySeconds(Number(e.target.value || 30))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            重試秒數
            <input type="number" value={retrySeconds} onChange={(e) => setRetrySeconds(Number(e.target.value || 30))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            回補月數
            <input type="number" value={months} onChange={(e) => setMonths(Number(e.target.value || 12))} style={inputStyle} />
          </label>
        </div>

        <div style={buttonRowStyle}>
          <button style={dangerButtonStyle} disabled={progress.running} onClick={safeResetLoop}>清除所選快取</button>
          <button style={successButtonStyle} disabled={progress.running} onClick={rebuildAll}>清除並重建</button>
          <button style={primaryButtonStyle} disabled={progress.running} onClick={runDailyUpdate}>立即每日更新</button>
          <button style={primaryButtonStyle} disabled={progress.running} onClick={runStockYearlyBackfill}>回補一年股價</button>
          <button style={primaryButtonStyle} disabled={progress.running} onClick={runChipHistoryBackfill}>回補一年籌碼</button>
          <button style={secondaryButtonStyle} onClick={() => call("/api/kline/2330", "2330 K 線測試")}>測試 2330</button>
        </div>

        <div style={progressTrackStyle}>
          <div style={{ ...progressBarStyle, width: `${pct}%` }} />
        </div>
        <div style={mutedStyle}>階段：{progress.phase} | 進度：{progress.done}/{progress.total} | {pct}%</div>

        <div style={gridStyle}>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>API 回應</h3>
            <pre style={preStyle}>{JSON.stringify(job || {}, null, 2)}</pre>
          </div>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>操作紀錄</h3>
            {log.map((x, i) => (
              <div key={i} style={logLineStyle}>{x}</div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}

const pageStyle = { background: "#020617", color: "white", padding: 18, fontFamily: "Arial, sans-serif" };
const headerStyle = { marginBottom: 14 };
const eyebrowStyle = { color: "#38bdf8", fontWeight: 800, letterSpacing: 1 };
const titleStyle = { margin: "8px 0" };
const mutedStyle = { color: "#94a3b8" };
const boxStyle = { border: "1px solid #334155", borderRadius: 12, padding: 14, marginBottom: 14, background: "#0f172a" };
const sectionTitleStyle = { marginTop: 0 };
const controlRowStyle = { display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" };
const buttonRowStyle = { display: "flex", gap: 10, flexWrap: "wrap", marginTop: 12 };
const labelStyle = { display: "grid", gap: 6, color: "#cbd5e1", fontSize: 13 };
const fieldStyle = { padding: 10, borderRadius: 10, background: "#020617", color: "white", border: "1px solid #334155" };
const inputStyle = { width: 90, padding: 10, borderRadius: 10, background: "#020617", color: "white", border: "1px solid #334155" };
const primaryButtonStyle = { padding: "10px 12px", borderRadius: 10, border: 0, background: "#2563eb", color: "white", fontWeight: 800, cursor: "pointer" };
const secondaryButtonStyle = { ...primaryButtonStyle, background: "#475569" };
const dangerButtonStyle = { ...primaryButtonStyle, background: "#dc2626" };
const successButtonStyle = { ...primaryButtonStyle, background: "#16a34a" };
const progressTrackStyle = { marginTop: 16, background: "#020617", borderRadius: 999, overflow: "hidden", border: "1px solid #334155" };
const progressBarStyle = { height: 18, background: "#22c55e", transition: "width .3s" };
const gridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))", gap: 12, marginTop: 14 };
const panelStyle = { border: "1px solid #1e293b", borderRadius: 12, padding: 12, background: "rgba(15,23,42,.75)" };
const panelTitleStyle = { marginTop: 0 };
const preStyle = { maxHeight: 320, overflow: "auto", color: "#cbd5e1", background: "#020617", padding: 12, borderRadius: 10, fontSize: 12 };
const logLineStyle = { color: "#cbd5e1", fontSize: 13, padding: "3px 0" };
