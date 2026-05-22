import React, { useState } from "react";

const API = (import.meta.env.VITE_API_BASE_URL || "https://stock-analysis-tw.fly.dev").replace(/\/$/, "");

function cleanCode(value) {
  return String(value || "2330")
    .trim()
    .replace(".TW", "")
    .replace(".TWO", "")
    .split(/\s+/)[0]
    .toUpperCase();
}

function fmt(value) {
  if (value === null || value === undefined || value === "") return "--";
  if (typeof value === "number") return Number.isFinite(value) ? value.toLocaleString() : "--";
  return String(value);
}

async function fetchJson(path, label) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 90000);
  try {
    const res = await fetch(`${API}${path}`, { cache: "no-store", signal: controller.signal });
    const text = await res.text();
    let json = {};
    try {
      json = text ? JSON.parse(text) : {};
    } catch {
      json = { raw: text };
    }
    if (!res.ok) {
      throw new Error(`${label} API ${res.status}: ${json?.detail || json?.error || text || "request failed"}`);
    }
    return json;
  } catch (error) {
    if (error?.name === "AbortError") throw new Error(`${label} API 逾時，後端回應超過 90 秒`);
    throw new Error(`${label} fetch failed: ${error?.message || error}`);
  } finally {
    window.clearTimeout(timer);
  }
}

function pickKlineRows(payload) {
  return Array.isArray(payload?.data) ? payload.data : [];
}

function pickChipRows(payload) {
  return Array.isArray(payload?.rows) ? payload.rows : [];
}

function DataTable({ columns, rows, emptyText }) {
  return (
    <div style={tableWrapStyle}>
      <table style={tableStyle}>
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.key} style={thStyle}>{col.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} style={emptyStyle}>{emptyText}</td>
            </tr>
          ) : rows.map((row, index) => (
            <tr key={`${row.date || row.time || index}-${index}`}>
              {columns.map((col) => (
                <td key={col.key} style={tdStyle}>{fmt(col.render ? col.render(row) : row[col.key])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function DataTablesPage() {
  const [stock, setStock] = useState("2330");
  const [loading, setLoading] = useState(false);
  const [klinePayload, setKlinePayload] = useState(null);
  const [chipPayload, setChipPayload] = useState(null);
  const [error, setError] = useState("");

  async function load() {
    const code = cleanCode(stock);
    setStock(code);
    setLoading(true);
    setError("");
    setKlinePayload(null);
    setChipPayload(null);

    const [klineResult, chipResult] = await Promise.allSettled([
      fetchJson(`/api/kline/${encodeURIComponent(code)}`, "股價歷史"),
      fetchJson(`/api/chip/${encodeURIComponent(code)}?auto_init=false`, "籌碼"),
    ]);

    const errors = [];
    if (klineResult.status === "fulfilled") setKlinePayload(klineResult.value);
    else errors.push(klineResult.reason?.message || String(klineResult.reason));

    if (chipResult.status === "fulfilled") setChipPayload(chipResult.value);
    else errors.push(chipResult.reason?.message || String(chipResult.reason));

    setError(errors.join("\n"));
    setLoading(false);
  }

  const klineRows = pickKlineRows(klinePayload).slice().reverse();
  const chipRows = pickChipRows(chipPayload).slice().reverse();

  return (
    <section style={pageStyle}>
      <div style={headerStyle}>
        <div>
          <div style={eyebrowStyle}>DATA CHECK</div>
          <h1 style={titleStyle}>資料表檢查</h1>
          <div style={mutedStyle}>直接查詢後端 API，確認資料庫是否已有股價歷史與三大法人籌碼資料。</div>
        </div>
        <div style={controlStyle}>
          <input
            value={stock}
            onChange={(event) => setStock(event.target.value)}
            onKeyDown={(event) => { if (event.key === "Enter") load(); }}
            placeholder="輸入股票代號，例如 1402"
            style={inputStyle}
          />
          <button type="button" onClick={load} disabled={loading} style={buttonStyle}>
            {loading ? "查詢中..." : "查詢"}
          </button>
        </div>
      </div>

      {error && <pre style={errorStyle}>{error}</pre>}

      <div style={summaryGridStyle}>
        <div style={summaryStyle}>
          <b>股價資料</b>
          <span>狀態：{klinePayload?.status || "--"}</span>
          <span>顯示筆數：{klineRows.length}</span>
          <span>來源：{klinePayload?.source || "--"}</span>
        </div>
        <div style={summaryStyle}>
          <b>籌碼資料</b>
          <span>狀態：{chipPayload?.status || "--"}</span>
          <span>顯示筆數：{chipRows.length}</span>
          <span>原始筆數：{chipPayload?.raw_row_count ?? chipPayload?.row_count ?? "--"}</span>
          <span>三大法人：{chipPayload?.has_institutional_data === true ? "有" : chipPayload ? "無" : "--"}</span>
        </div>
      </div>

      <section style={sectionStyle}>
        <h2 style={sectionTitleStyle}>股價歷史資料</h2>
        <DataTable
          columns={[
            { key: "date", label: "日期" },
            { key: "open", label: "開盤" },
            { key: "high", label: "最高" },
            { key: "low", label: "最低" },
            { key: "close", label: "收盤" },
            { key: "volume", label: "成交量" },
            { key: "change_pct", label: "漲跌幅" },
            { key: "source", label: "來源", render: () => klinePayload?.source || "--" },
          ]}
          rows={klineRows}
          emptyText="尚未查詢，或 API 沒有回傳股價資料。"
        />
      </section>

      <section style={sectionStyle}>
        <h2 style={sectionTitleStyle}>籌碼資料</h2>
        <DataTable
          columns={[
            { key: "date", label: "日期" },
            { key: "foreign_buy", label: "外資買賣超", render: (row) => row.foreign_buy ?? row.foreign },
            { key: "investment_trust_buy", label: "投信買賣超", render: (row) => row.investment_trust_buy ?? row.investment_trust },
            { key: "dealer_buy", label: "自營商買賣超", render: (row) => row.dealer_buy ?? row.dealer },
            { key: "margin_balance", label: "融資餘額" },
            { key: "short_balance", label: "融券餘額" },
            { key: "source", label: "來源", render: (row) => row.source_t86 || row.source_margin || row.source || "--" },
          ]}
          rows={chipRows}
          emptyText="尚未查詢，或 API 沒有回傳籌碼資料。"
        />
      </section>

      <section style={sectionStyle}>
        <h2 style={sectionTitleStyle}>原始摘要</h2>
        <pre style={preStyle}>{JSON.stringify({
          kline: {
            status: klinePayload?.status,
            source: klinePayload?.source,
            cache_rows: klinePayload?.cache_rows,
            data_rows: klineRows.length,
            meta: klinePayload?.meta,
          },
          chip: {
            status: chipPayload?.status,
            row_count: chipPayload?.row_count,
            raw_row_count: chipPayload?.raw_row_count,
            has_institutional_data: chipPayload?.has_institutional_data,
            latest_chip: chipPayload?.latest_chip,
            live_refresh: chipPayload?.live_refresh,
            live_backfill: chipPayload?.live_backfill,
          },
        }, null, 2)}</pre>
      </section>
    </section>
  );
}

const pageStyle = { minHeight: "100vh", background: "#020617", color: "white", padding: 18, fontFamily: "Arial, sans-serif" };
const headerStyle = { display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", alignItems: "end", border: "1px solid #1e293b", background: "#0f172a", padding: 18, borderRadius: 8 };
const eyebrowStyle = { color: "#38bdf8", fontWeight: 800, letterSpacing: 1 };
const titleStyle = { margin: "6px 0" };
const mutedStyle = { color: "#94a3b8" };
const controlStyle = { display: "flex", gap: 10, flexWrap: "wrap" };
const inputStyle = { padding: "12px 14px", borderRadius: 8, border: "1px solid #334155", background: "#020617", color: "white", minWidth: 240 };
const buttonStyle = { padding: "12px 18px", borderRadius: 8, border: 0, background: "#2563eb", color: "white", fontWeight: 800, cursor: "pointer" };
const errorStyle = { marginTop: 12, padding: 12, borderRadius: 8, background: "#7f1d1d", color: "white", whiteSpace: "pre-wrap" };
const summaryGridStyle = { display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(260px,1fr))", gap: 12, marginTop: 12 };
const summaryStyle = { display: "grid", gap: 6, border: "1px solid #1e293b", background: "#0f172a", padding: 14, borderRadius: 8, color: "#cbd5e1" };
const sectionStyle = { marginTop: 14, border: "1px solid #1e293b", background: "#0f172a", padding: 14, borderRadius: 8 };
const sectionTitleStyle = { marginTop: 0 };
const tableWrapStyle = { overflow: "auto", border: "1px solid #1e293b", borderRadius: 8 };
const tableStyle = { width: "100%", borderCollapse: "collapse", minWidth: 760 };
const thStyle = { position: "sticky", top: 0, textAlign: "right", padding: 10, background: "#111827", color: "#cbd5e1", borderBottom: "1px solid #334155", whiteSpace: "nowrap" };
const tdStyle = { textAlign: "right", padding: 10, borderBottom: "1px solid #1e293b", color: "#e5e7eb", whiteSpace: "nowrap" };
const emptyStyle = { padding: 18, textAlign: "center", color: "#94a3b8" };
const preStyle = { maxHeight: 360, overflow: "auto", background: "#020617", color: "#cbd5e1", padding: 12, borderRadius: 8, fontSize: 12 };
