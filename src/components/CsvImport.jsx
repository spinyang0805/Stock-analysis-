// 券商對帳單 CSV 匯入精靈：上傳 → 欄位對應（可存成國泰／亞東預設）→ 預覽錯誤 → 匯入（重複自動略過）
import React, { useEffect, useMemo, useRef, useState } from "react";
import { FileUp, Save } from "lucide-react";
import { Card } from "./ui.jsx";
import { FIELDS, convertRows, decodeBytes, detectHeader, guessMapping, parseCSV, parseXlsx } from "../lib/csvImport.js";
import { supabase } from "../lib/supabase.js";
import { useApp } from "../lib/appState.jsx";
import { sideLabel } from "../lib/portfolio.js";
import { fmt, fmtInt } from "../lib/format.js";

const BROKERS = ["國泰證券", "亞東證券", "其他"];

export default function CsvImport({ onDone }) {
  const app = useApp();
  const [step, setStep] = useState(1);
  const [fileName, setFileName] = useState("");
  const [table, setTable] = useState(null); // { header, rows, headerLine }
  const [mapping, setMapping] = useState({});
  const [broker, setBroker] = useState("國泰證券");
  const [account, setAccount] = useState("國泰證券");
  const [presets, setPresets] = useState([]);
  const [over, setOver] = useState(false);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState(null);
  const [err, setErr] = useState("");
  const inputRef = useRef(null);

  useEffect(() => {
    supabase.from("import_presets").select("*").then(({ data }) => setPresets(data || []));
  }, []);

  async function readFile(file) {
    if (!file) return;
    setErr(""); setResult(null);
    const buf = await file.arrayBuffer();
    let rows;
    try {
      rows = /\.xlsx$/i.test(file.name) ? await parseXlsx(buf) : parseCSV(decodeBytes(buf));
    } catch (e) {
      setErr(`檔案解析失敗：${e.message}（若是 .xls 舊格式，請在 Excel 另存成 .xlsx 或 CSV）`); return;
    }
    if (rows.length < 2) { setErr("檔案內容太少，請確認是 CSV 或 xlsx 對帳單"); return; }
    const h = detectHeader(rows);
    const header = rows[h];
    const preset = presets.find((p) => p.name === broker);
    const auto = guessMapping(header);
    const fromPreset = preset ? Object.fromEntries(Object.entries(preset.mapping).filter(([, v]) => header.includes(v))) : {};
    setTable({ header, rows: rows.slice(h + 1).filter((r) => r.length >= Math.min(3, header.length)), headerLine: h });
    setMapping({ ...auto, ...fromPreset });
    setFileName(file.name);
    setStep(2);
  }

  const converted = useMemo(() => (table ? convertRows(table.header, table.rows, mapping, { broker: account.trim() || broker, headerLine: table.headerLine }) : []), [table, mapping, broker, account]);
  const okRows = converted.filter((r) => r.ok);
  const badRows = converted.filter((r) => !r.ok && !r.skipped);
  const splitRows = okRows.filter((r) => r.tx.note?.startsWith("股票分割"));
  const missing = FIELDS.filter((f) => f.required && !mapping[f.key]);
  const qtyMissing = !mapping.shares && !mapping.lots;

  async function savePreset() {
    const { error } = await supabase.from("import_presets").upsert({ name: broker, mapping, updated_at: new Date().toISOString() }, { onConflict: "user_id,name" });
    if (error) setErr(`預設儲存失敗：${error.message}`);
    else setPresets((p) => [...p.filter((x) => x.name !== broker), { name: broker, mapping }]);
  }

  async function doImport() {
    setBusy(true); setErr("");
    const rows = okRows.map((r) => r.tx);
    let inserted = 0;
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const { data, error } = await supabase.from("transactions").upsert(chunk, { onConflict: "user_id,import_hash", ignoreDuplicates: true }).select("id");
      if (error) { setErr(`匯入失敗：${error.message}`); setBusy(false); return; }
      inserted += data?.length || 0;
    }
    await savePreset();
    await app.reload();
    setBusy(false);
    setResult({ inserted, skipped: rows.length - inserted, bad: badRows.length });
    setStep(4);
  }

  return (
    <Card title="匯入券商對帳單 CSV" icon={FileUp}>
      <div className="steps" aria-label="匯入步驟">
        {["上傳檔案", "欄位對應", "預覽確認", "完成"].map((s, i) => <span key={s} className={step === i + 1 ? "on" : ""}>{i + 1}. {s}</span>)}
      </div>

      {step === 1 && (
        <div className="grid" style={{ gap: 12 }}>
          <div className="form-grid">
            <div className="field">
              <label htmlFor="csv-broker">券商</label>
              <select id="csv-broker" className="input" value={broker} onChange={(e) => { setBroker(e.target.value); setAccount(e.target.value === "其他" ? "" : e.target.value); }}>
                {BROKERS.map((b) => <option key={b}>{b}</option>)}
                {presets.filter((p) => !BROKERS.includes(p.name)).map((p) => <option key={p.name}>{p.name}</option>)}
              </select>
            </div>
            <div className="field">
              <label htmlFor="csv-account">匯入到帳戶</label>
              <input id="csv-account" className="input" list="csv-account-list" value={account} onChange={(e) => setAccount(e.target.value)} placeholder="例：國泰證券、國泰（太太）" />
              <datalist id="csv-account-list">
                {[...new Set(app.transactions.map((t) => t.broker).filter(Boolean))].map((a) => <option key={a} value={a} />)}
              </datalist>
              <span className="dim">同一家券商有兩個帳戶時，用不同名稱區分</span>
            </div>
          </div>
          <div
            className={`dropzone ${over ? "over" : ""}`} role="button" tabIndex={0}
            onClick={() => inputRef.current?.click()} onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && inputRef.current?.click()}
            onDragOver={(e) => { e.preventDefault(); setOver(true); }} onDragLeave={() => setOver(false)}
            onDrop={(e) => { e.preventDefault(); setOver(false); readFile(e.dataTransfer.files?.[0]); }}
          >
            <FileUp size={28} aria-hidden="true" />
            <div style={{ fontWeight: 700, color: "var(--fg-2)", marginTop: 6 }}>拖曳 CSV／xlsx 到這裡，或點擊選擇檔案</div>
            <div className="dim">支援 .xlsx 與 CSV（UTF-8、Big5）；從券商 App／網站匯出「交易明細」或「對帳單」</div>
            <input ref={inputRef} type="file" accept=".csv,.txt,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" hidden onChange={(e) => readFile(e.target.files?.[0])} />
          </div>
          {presets.some((p) => p.name === broker) && <div className="dim">已有「{broker}」欄位對應預設，上傳後自動套用。</div>}
        </div>
      )}

      {step === 2 && table && (
        <div className="grid" style={{ gap: 12 }}>
          <div className="dim">帳戶：<b>{account.trim() || broker}</b>・檔案：{fileName}，表頭在第 {table.headerLine + 1} 列，共 {table.rows.length} 筆資料。請確認每個欄位對應到正確的表頭（* 為必填，股數與張數擇一）。</div>
          <div className="form-grid">
            {FIELDS.map((f) => (
              <div className="field" key={f.key}>
                <label htmlFor={`map-${f.key}`}>{f.label}{f.required ? " *" : ""}</label>
                <select id={`map-${f.key}`} className="input" value={mapping[f.key] || ""} onChange={(e) => setMapping((m) => ({ ...m, [f.key]: e.target.value || undefined }))}>
                  <option value="">（不使用）</option>
                  {table.header.map((h, i) => <option key={`${h}-${i}`} value={h}>{h || `第 ${i + 1} 欄`}</option>)}
                </select>
              </div>
            ))}
          </div>
          {(missing.length > 0 || qtyMissing) && (
            <div className="notice warn">尚未對應：{[...missing.map((f) => f.label), ...(qtyMissing ? ["成交股數或張數"] : [])].join("、")}</div>
          )}
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button type="button" className="btn" onClick={() => setStep(1)}>重新上傳</button>
            <button type="button" className="btn" onClick={savePreset}><Save size={16} />存成「{broker}」預設</button>
            <button type="button" className="btn primary" disabled={missing.length > 0 || qtyMissing} onClick={() => setStep(3)}>下一步：預覽</button>
          </div>
        </div>
      )}

      {step === 3 && (
        <div className="grid" style={{ gap: 12 }}>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <span className="badge blue">可匯入 {okRows.length} 筆</span>
            {badRows.length > 0 && <span className="badge down" style={{ color: "#fca5a5", background: "rgba(239,68,68,.14)" }}>有問題 {badRows.length} 筆（不會匯入）</span>}
            {splitRows.length > 0 && <span className="badge blue">股票分割 {splitRows.length} 筆（成本不變、不計損益）</span>}
            <span className="dim">已存在的交易（同日期、代號、類別、股數、價格、費用）會自動略過</span>
          </div>
          <div className="table-wrap" style={{ maxHeight: 420 }}>
            <table className="data">
              <thead><tr><th className="left">列</th><th className="left">日期</th><th className="left">股票</th><th className="left">類別</th><th>股數</th><th>價格</th><th>手續費</th><th>稅</th><th className="left">狀態</th></tr></thead>
              <tbody>
                {converted.slice(0, 200).map((r) => (
                  <tr key={r.line} className={r.ok ? "" : "err"}>
                    <td className="left dim">{r.line}</td>
                    {r.ok ? (
                      <>
                        <td className="left num">{r.tx.trade_date}</td>
                        <td className="left">{r.tx.stock_id} {r.tx.stock_name}</td>
                        <td className="left"><span className={r.tx.side === "buy" ? "up" : r.tx.side === "sell" ? "down" : ""}>{sideLabel(r.tx)}</span></td>
                        <td>{fmtInt(r.tx.shares)}</td><td>{fmt(r.tx.price)}</td><td>{fmtInt(r.tx.fee)}</td><td>{fmtInt(r.tx.tax)}</td>
                        <td className="left dim">OK</td>
                      </>
                    ) : (
                      <>
                        <td className="left" colSpan={7} style={{ whiteSpace: "normal" }}><span className="dim">{r.raw.join(" | ")}</span></td>
                        <td className="left" style={{ color: "#fca5a5", whiteSpace: "normal" }}>{r.errors.join("；")}</td>
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {converted.length > 200 && <div className="dim">僅預覽前 200 筆，匯入會包含全部 {okRows.length} 筆可匯入資料。</div>}
          {badRows.length > 0 && <div className="dim">有問題的列多半是小計、空白列或欄位對應錯誤；可回上一步調整對應。</div>}
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button type="button" className="btn" onClick={() => setStep(2)}>上一步</button>
            <button type="button" className="btn primary" disabled={!okRows.length || busy} onClick={doImport}>{busy ? "匯入中…" : `匯入 ${okRows.length} 筆`}</button>
          </div>
        </div>
      )}

      {step === 4 && result && (
        <div className="grid" style={{ gap: 12 }}>
          <div className="notice">完成：新增 {result.inserted} 筆，略過重複 {result.skipped} 筆{result.bad ? `，有問題未匯入 ${result.bad} 筆` : ""}。已把欄位對應存成「{broker}」預設。</div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button type="button" className="btn primary" onClick={() => onDone?.()}>查看持股</button>
            <button type="button" className="btn" onClick={() => { setStep(1); setTable(null); setResult(null); }}>再匯入一份</button>
          </div>
        </div>
      )}
      {err && <div className="notice err" style={{ marginTop: 10 }}>{err}<div style={{ marginTop: 6 }}><button type="button" className="btn sm" onClick={() => setErr("")}>知道了</button></div></div>}
    </Card>
  );
}
