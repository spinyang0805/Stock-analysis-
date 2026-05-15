import { useState } from "react";

const API = "https://stock-analysis-api-ihun.onrender.com";
const PAGE_VERSION = "db-maintenance-v1";
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
  const [progress, setProgress] = useState({ running: false, phase: "idle", done: 0, total: 0 });
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
    addLog(`Start: ${label}`);
    try {
      const data = await api(path, timeoutMs);
      addLog(`Done: ${label}`);
      return data;
    } catch (e) {
      const message = e.name === "AbortError" ? "Request timed out" : e.message;
      setJob({ error: message });
      addLog(`Failed: ${label} - ${message}`);
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
    const productResult = await call(`/api/products?${query}&limit=5000`, "Load product universe", 120000);
    return Number(productResult?.count || 0);
  }

  async function safeResetLoop() {
    if (progress.running) return;
    if (!window.confirm("This will clear stock_daily and analysis_cache for the selected universe. Continue?")) return;

    setProgress({ running: true, phase: "loading universe", done: 0, total: 0 });
    const total = await getProductTotal();
    if (!total) {
      setProgress({ running: false, phase: "failed: no products", done: 0, total: 0 });
      return;
    }

    setProgress({ running: true, phase: "clearing database", done: 0, total });
    addLog(`Clearing ${total} products with batch=${batchSize}, delay=${delaySeconds}s, retry=${retrySeconds}s`);

    for (let offset = 0; offset < total; offset += batchSize) {
      const label = `Clear offset=${offset} to ${Math.min(offset + batchSize, total)}`;
      let ok = false;
      while (!ok) {
        const data = await call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${batchSize}`, label);
        if (data) {
          ok = true;
          const done = Math.min(offset + batchSize, total);
          setProgress({ running: true, phase: "clearing database", done, total });
          if (data.next_offset === null) {
            setProgress({ running: false, phase: "clear complete", done, total });
            addLog("Backend returned next_offset=null. Clear complete.");
            return;
          }
          await sleep(delaySeconds * 1000);
        } else {
          addLog(`Retry in ${retrySeconds}s`);
          await sleep(retrySeconds * 1000);
        }
      }
    }

    setProgress({ running: false, phase: "clear complete", done: total, total });
    addLog("Clear complete.");
  }

  async function rebuildAll() {
    if (progress.running) return;
    if (!window.confirm("This will clear selected data, then backfill stock_daily. Continue?")) return;

    setProgress({ running: true, phase: "loading universe", done: 0, total: 0 });
    addLog("Starting clear-and-rebuild workflow.");

    const total = await getProductTotal();
    if (!total) {
      setProgress({ running: false, phase: "failed: no products", done: 0, total: 0 });
      return;
    }

    setProgress({ running: true, phase: "clearing database", done: 0, total });
    for (let offset = 0; offset < total; offset += batchSize) {
      let ok = false;
      while (!ok) {
        const data = await call(`/api/firebase/reset_all?${query}&offset=${offset}&limit=${batchSize}`, `Clear ${offset}-${Math.min(offset + batchSize, total)}`);
        if (data) {
          ok = true;
          setProgress({ running: true, phase: "clearing database", done: Math.min(offset + batchSize, total), total });
          await sleep(delaySeconds * 1000);
        } else {
          addLog(`Clear failed. Retry in ${retrySeconds}s`);
          await sleep(retrySeconds * 1000);
        }
      }
    }

    setProgress({ running: true, phase: "backfilling stock data", done: 0, total });
    for (let offset = 0; offset < total; offset += batchSize) {
      let ok = false;
      while (!ok) {
        const data = await call(`/api/job/backfill_all?${query}&offset=${offset}&limit=${batchSize}&months=${months}`, `Backfill ${offset}-${Math.min(offset + batchSize, total)}`);
        if (data) {
          ok = true;
          setProgress({ running: true, phase: "backfilling stock data", done: Math.min(offset + batchSize, total), total });
          await sleep(delaySeconds * 1000);
        } else {
          addLog(`Backfill failed. Retry in ${retrySeconds}s`);
          await sleep(retrySeconds * 1000);
        }
      }
    }

    setProgress({ running: true, phase: "running daily update", done: total, total });
    await call("/api/job/daily", "Daily update");
    setProgress({ running: false, phase: "rebuild complete", done: total, total });
    addLog("Clear-and-rebuild workflow complete.");
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
        <div style={eyebrowStyle}>DATABASE MAINTENANCE</div>
        <h2 style={titleStyle}>Stock Sync And Database Reset</h2>
        <div style={mutedStyle}>{PAGE_VERSION}</div>
      </div>

      <section style={boxStyle}>
        <h3 style={sectionTitleStyle}>Product Universe Sync</h3>
        <p style={mutedStyle}>Current: {universeOffset} / {universeTotal || "?"} {universeDone ? "done" : ""}</p>

        <div style={controlRowStyle}>
          <label style={labelStyle}>
            Offset
            <input value={universeOffset} onChange={(e) => setUniverseOffset(Number(e.target.value || 0))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            Limit
            <input value={universeLimit} onChange={(e) => setUniverseLimit(Number(e.target.value || 10))} style={inputStyle} />
          </label>
        </div>

        <div style={buttonRowStyle}>
          <button disabled={universeBusy} onClick={() => runUniverse("/api/init_universe")} style={primaryButtonStyle}>Check Universe</button>
          <button disabled={universeBusy || universeDone} onClick={() => runUniverse(`/api/init_universe_batch?offset=${universeOffset}&limit=${universeLimit}`)} style={primaryButtonStyle}>
            {universeDone ? "Done" : "Run Current Batch"}
          </button>
          <button disabled={universeBusy} onClick={resetUniverseState} style={secondaryButtonStyle}>Reset</button>
        </div>

        <pre style={preStyle}>{JSON.stringify(universeResult || {}, null, 2)}</pre>
      </section>

      <section style={boxStyle}>
        <h3 style={sectionTitleStyle}>Clear / Rebuild Stock Cache</h3>
        <div style={controlRowStyle}>
          <label style={labelStyle}>
            Product Type
            <select value={productType} onChange={(e) => setProductType(e.target.value)} style={fieldStyle}>
              <option value="all">all</option>
              <option value="股票">股票</option>
              <option value="ETF">ETF</option>
              <option value="高股息ETF">高股息ETF</option>
            </select>
          </label>
          <label style={labelStyle}>
            Market
            <select value={market} onChange={(e) => setMarket(e.target.value)} style={fieldStyle}>
              <option value="all">all</option>
              <option value="上市">上市</option>
              <option value="上櫃">上櫃</option>
            </select>
          </label>
          <label style={labelStyle}>
            Batch
            <input type="number" value={batchSize} onChange={(e) => setBatchSize(Number(e.target.value || 20))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            Delay sec
            <input type="number" value={delaySeconds} onChange={(e) => setDelaySeconds(Number(e.target.value || 30))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            Retry sec
            <input type="number" value={retrySeconds} onChange={(e) => setRetrySeconds(Number(e.target.value || 30))} style={inputStyle} />
          </label>
          <label style={labelStyle}>
            Months
            <input type="number" value={months} onChange={(e) => setMonths(Number(e.target.value || 12))} style={inputStyle} />
          </label>
        </div>

        <div style={buttonRowStyle}>
          <button style={dangerButtonStyle} disabled={progress.running} onClick={safeResetLoop}>Clear Selected Cache</button>
          <button style={successButtonStyle} disabled={progress.running} onClick={rebuildAll}>Clear And Rebuild</button>
          <button style={secondaryButtonStyle} onClick={() => call("/api/kline/2330", "Kline smoke test 2330")}>Smoke Test 2330</button>
        </div>

        <div style={progressTrackStyle}>
          <div style={{ ...progressBarStyle, width: `${pct}%` }} />
        </div>
        <div style={mutedStyle}>Phase: {progress.phase} | Progress: {progress.done}/{progress.total} | {pct}%</div>

        <div style={gridStyle}>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>API Response</h3>
            <pre style={preStyle}>{JSON.stringify(job || {}, null, 2)}</pre>
          </div>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>Log</h3>
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
