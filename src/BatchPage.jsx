import React, { useState } from "react";

const API = "https://stock-analysis-tw.fly.dev";
const PAGE_VERSION = "batch-v5-safe-chip-backfill";
const SAFE_REQUEST_LIMIT = 5;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

    try {
      const targetEnd = chipOffset + Number(chipLimit || SAFE_REQUEST_LIMIT);
      let cursor = chipOffset;
      let totalWritten = 0;
      let totalProcessed = 0;
      let latestJson = null;
      addChipLog(`開始籌碼批次回補，目標 ${chipOffset} 到 ${targetEnd}，單次最多 ${SAFE_REQUEST_LIMIT} 檔`);

      while (cursor < targetEnd) {
        const requestLimit = Math.min(SAFE_REQUEST_LIMIT, targetEnd - cursor);
        const path = `/api/chip/backfill_all?product_type=${encodeURIComponent(chipType)}&market=${encodeURIComponent(chipMarket)}&offset=${cursor}&limit=${requestLimit}`;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 90000);
        let json;

        try {
          const res = await fetch(API + path, { signal: controller.signal, cache: "no-store" });
          json = await res.json();
          if (!res.ok) throw new Error(json?.detail || json?.error || `HTTP ${res.status}`);
        } finally {
          clearTimeout(timer);
        }

        latestJson = json;
        setChipResult(json);
        if (json.universe_count !== undefined) setChipTotal(Number(json.universe_count));

        totalProcessed += Number(json.processed || 0);
        totalWritten += Number(json.written_stocks || 0);
        if (json.error_count) {
          addChipLog(`本小批有 ${json.error_count} 筆錯誤，可能是 Firestore 暫時限流；已先停止，稍後可從 ${cursor} 重試。`);
          break;
        }

        if (json.next_offset === null) {
          setChipDone(true);
          setChipOffset(Number(json.universe_count || cursor));
          addChipLog(`籌碼回補完成，處理=${totalProcessed}，寫入=${totalWritten}`);
          return;
        }

        cursor = Number(json.next_offset ?? (cursor + requestLimit));
        setChipOffset(cursor);
        addChipLog(`小批完成，累計處理=${totalProcessed}，寫入=${totalWritten}，下一批=${cursor}`);
        await sleep(2500);
      }

      setChipResult(latestJson || {});
      addChipLog(`本輪完成，處理=${totalProcessed}，寫入=${totalWritten}，目前位置=${cursor}`);
    } catch (e) {
      const msg = e.name === "AbortError" ? "單次請求逾時，已停止本輪，請稍後從目前位置重試" : String(e.message || e);
      setChipResult({ error: msg });
      addChipLog(`執行失敗：${msg}`);
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
        <div style={eyebrowStyle}>批次工具</div>
        <h2 style={titleStyle}>籌碼資料回補</h2>
        <div style={mutedStyle}>{PAGE_VERSION}</div>
      </div>

      <section style={boxStyle}>
        <h3 style={sectionTitleStyle}>chip_daily 批次回補</h3>
        <p style={mutedStyle}>
          目前進度：{chipOffset} / {chipTotal || "?"} {chipDone ? "已完成" : ""}
        </p>

        <div style={controlRowStyle}>
          <label style={labelStyle}>
            市場
            <select value={chipMarket} onChange={(e) => setChipMarket(e.target.value)} style={fieldStyle}>
              <option value="上市">上市</option>
              <option value="上櫃">上櫃</option>
              <option value="all">全部</option>
            </select>
          </label>

          <label style={labelStyle}>
            商品類型
            <select value={chipType} onChange={(e) => setChipType(e.target.value)} style={fieldStyle}>
              <option value="all">全部</option>
              <option value="股票">股票</option>
              <option value="ETF">ETF</option>
              <option value="高股息ETF">高股息 ETF</option>
            </select>
          </label>

          <label style={labelStyle}>
            起始位置
            <input value={chipOffset} onChange={(e) => setChipOffset(Number(e.target.value || 0))} style={inputStyle} />
          </label>

          <label style={labelStyle}>
            批次筆數
            <input value={chipLimit} onChange={(e) => setChipLimit(Number(e.target.value || 100))} style={inputStyle} />
          </label>
        </div>

        <div style={buttonRowStyle}>
          <button disabled={chipBusy || chipDone} onClick={runChipCurrent} style={primaryButtonStyle}>
            {chipBusy ? "執行中..." : chipDone ? "已完成" : `執行本批：${chipOffset}`}
          </button>
          <button disabled={chipBusy} onClick={resetChip} style={secondaryButtonStyle}>
            重設狀態
          </button>
        </div>

        <div style={gridStyle}>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>API 回應</h3>
            <pre style={preStyle}>{JSON.stringify(chipResult || {}, null, 2)}</pre>
          </div>
          <div style={panelStyle}>
            <h3 style={panelTitleStyle}>操作紀錄</h3>
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
