import { useState } from "react";

const API = "https://stock-analysis-api-ihun.onrender.com";
const PAGE_VERSION = "batch-v3-chip-only";

export default function BatchPage() {
  const [chipOffset, setChipOffset] = useState(0);
  const [chipLimit, setChipLimit] = useState(100);
  const [chipMarket, setChipMarket] = useState("上市");
  const [chipType, setChipType] = useState("all");
  const [chipTotal, setChipTotal] = useState(0);
  const [chipDone, setChipDone] = useState(false);
  const [chipBusy, setChipBusy] = useState(false);
  const [chipResult, setChipResult] = useState(null);
  const [chipLogs, setChipLogs] = useState([]);

  const addChipLog = (text) => {
    setChipLogs((old) => [`${new Date().toLocaleTimeString()} ${text}`, ...old].slice(0, 30));
  };

  async function runChipCurrent() {
    if (chipBusy || chipDone) return;
    setChipBusy(true);
    const path = `/api/chip/backfill_all?product_type=${encodeURIComponent(chipType)}&market=${encodeURIComponent(chipMarket)}&offset=${chipOffset}&limit=${chipLimit}`;

    try {
      addChipLog(`Starting chip backfill offset=${chipOffset}, limit=${chipLimit}`);
      const res = await fetch(API + path);
      const json = await res.json();
      setChipResult(json);

      if (json.universe_count !== undefined) setChipTotal(Number(json.universe_count));

      if (json.next_offset === null) {
        setChipDone(true);
        setChipOffset(Number(json.universe_count || chipOffset));
        addChipLog(`Chip backfill complete. processed=${json.processed}, written=${json.written_stocks}`);
      } else if (json.next_offset !== undefined) {
        setChipOffset(Number(json.next_offset));
        addChipLog(`Batch complete. processed=${json.processed}, written=${json.written_stocks}, next=${json.next_offset}`);
      } else {
        addChipLog("Batch returned without next_offset.");
      }
    } catch (e) {
      const msg = String(e.message || e);
      setChipResult({ error: msg });
      addChipLog(`Failed: ${msg}`);
    } finally {
      setChipBusy(false);
    }
  }

  function resetChip() {
    setChipOffset(0);
    setChipDone(false);
    setChipResult(null);
    setChipLogs([]);
    setChipTotal(0);
  }

  return (
    <div style={pageStyle}>
      <div style={headerStyle}>
        <div style={eyebrowStyle}>BATCH TOOL</div>
        <h2 style={titleStyle}>Chip Data Backfill</h2>
        <div style={mutedStyle}>{PAGE_VERSION}</div>
      </div>

      <section style={boxStyle}>
        <h3 style={sectionTitleStyle}>chip_daily Backfill</h3>
        <p style={mutedStyle}>
          Current: {chipOffset} / {chipTotal || "?"} {chipDone ? "done" : ""}
        </p>

        <div style={controlRowStyle}>
          <label style={labelStyle}>
            Market
            <select value={chipMarket} onChange={(e) => setChipMarket(e.target.value)} style={fieldStyle}>
              <option value="上市">上市</option>
              <option value="上櫃">上櫃</option>
              <option value="all">all</option>
            </select>
          </label>

          <label style={labelStyle}>
            Type
            <select value={chipType} onChange={(e) => setChipType(e.target.value)} style={fieldStyle}>
              <option value="all">all</option>
              <option value="股票">股票</option>
              <option value="ETF">ETF</option>
              <option value="高股息ETF">高股息ETF</option>
            </select>
          </label>

          <label style={labelStyle}>
            Offset
            <input value={chipOffset} onChange={(e) => setChipOffset(Number(e.target.value || 0))} style={inputStyle} />
          </label>

          <label style={labelStyle}>
            Limit
            <input value={chipLimit} onChange={(e) => setChipLimit(Number(e.target.value || 100))} style={inputStyle} />
          </label>
        </div>

        <div style={buttonRowStyle}>
          <button disabled={chipBusy || chipDone} onClick={runChipCurrent} style={primaryButtonStyle}>
            {chipBusy ? "Running..." : chipDone ? "Done" : `Run chip offset=${chipOffset}`}
          </button>
          <button disabled={chipBusy} onClick={resetChip} style={secondaryButtonStyle}>
            Reset
          </button>
        </div>

        <div style={gridStyle}>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>API Response</h3>
            <pre style={preStyle}>{JSON.stringify(chipResult || {}, null, 2)}</pre>
          </div>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>Log</h3>
            {chipLogs.map((x, i) => (
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
const gridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))", gap: 12, marginTop: 14 };
const panelStyle = { border: "1px solid #1e293b", borderRadius: 12, padding: 12, background: "rgba(15,23,42,.75)" };
const panelTitleStyle = { marginTop: 0 };
const preStyle = { maxHeight: 320, overflow: "auto", color: "#cbd5e1", background: "#020617", padding: 12, borderRadius: 10, fontSize: 12 };
const logLineStyle = { color: "#cbd5e1", fontSize: 13, padding: "3px 0" };
