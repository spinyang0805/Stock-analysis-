// 券商對帳單 CSV 匯入精靈：上傳 → 欄位對應（可存成國泰／亞東預設）→ 預覽錯誤 → 匯入（重複自動略過）
import React, { useEffect, useMemo, useRef, useState } from "react";
import { Badge } from "./ui.jsx";
import { Check, CheckCircle2, FileUp, Save, X } from "lucide-react";
import { FIELDS, convertRows, decodeBytes, detectHeader, guessMapping, parseCSV, parseXlsx } from "../lib/csvImport.js";
import { findUsHeader, parseUsRows } from "../lib/usPortfolio.js";
import { supabase } from "../lib/supabase.js";
import { useApp } from "../lib/appState.jsx";
import { sideLabel } from "../lib/portfolio.js";
import { fmt, fmtInt } from "../lib/format.js";

const BROKERS = ["國泰證券", "亞東證券", "其他"];
const STEPS = ["上傳檔案", "欄位對應", "預覽確認", "完成"];
const REQUIRED = FIELDS.filter((f) => f.required);
const OPTIONAL = FIELDS.filter((f) => !f.required);
const QTY = OPTIONAL.filter((f) => f.key === "shares" || f.key === "lots");
const EXTRA = OPTIONAL.filter((f) => f.key !== "shares" && f.key !== "lots");

const okRowsOf = (list) => list.filter((r) => r.ok);

export default function CsvImport({ onDone, onClose }) {
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
  const [us, setUs] = useState(null); // 美股對帳單（國泰複委託）：自動偵測，解析結果存這裡
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
    if (findUsHeader(rows) >= 0) {
      setUs(parseUsRows(rows)); setTable(null); setFileName(file.name); setStep(3);
      return;
    }
    setUs(null);
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

  const converted = useMemo(() => (us ? us : table ? convertRows(table.header, table.rows, mapping, { broker: account.trim() || broker, headerLine: table.headerLine }) : []), [us, table, mapping, broker, account]);
  const usKnown = useMemo(() => new Set((app.usTransactions || []).map((t) => t.import_hash)), [app.usTransactions]);
  const usDup = us ? okRowsOf(converted).filter((r) => usKnown.has(r.tx.import_hash)).length : 0;
  const okRows = okRowsOf(converted);
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
      const { data, error } = await supabase.from(us ? "us_transactions" : "transactions").upsert(chunk, { onConflict: "user_id,import_hash", ignoreDuplicates: true }).select("id");
      if (error) { setErr(`匯入失敗：${error.message}`); setBusy(false); return; }
      inserted += data?.length || 0;
    }
    if (!us) await savePreset();
    await app.reload();
    setBusy(false);
    setResult({ inserted, skipped: rows.length - inserted, bad: badRows.length });
    setStep(4);
  }

  // 對應到該欄位的第一筆資料，讓使用者一眼確認選對表頭
  const sample = (key) => {
    const idx = table && mapping[key] ? table.header.indexOf(mapping[key]) : -1;
    return idx >= 0 ? table.rows[0]?.[idx] : "";
  };
  const fieldSelect = (f) => (
    <div className="field" key={f.key}>
      <label htmlFor={`map-${f.key}`}>{f.label}{f.required && <b className="req"> *</b>}</label>
      <select id={`map-${f.key}`} className="input" value={mapping[f.key] || ""} onChange={(e) => setMapping((m) => ({ ...m, [f.key]: e.target.value || undefined }))}>
        <option value="">（不使用）</option>
        {table.header.map((h, i) => <option key={`${h}-${i}`} value={h}>{h || `第 ${i + 1} 欄`}</option>)}
      </select>
      <span className="dim imp-sample">{sample(f.key) ? `範例：${sample(f.key)}` : " "}</span>
    </div>
  );

  return (
    <section className="imp" aria-label="匯入券商對帳單">
      <header className="imp-head">
        <h2 className="imp-title"><FileUp size={18} aria-hidden="true" />匯入券商對帳單</h2>
        {onClose && <button type="button" className="btn ghost sm" onClick={onClose} aria-label="關閉匯入"><X size={16} />關閉</button>}
      </header>

      <ol className="stepper" aria-label="匯入步驟">
        {STEPS.map((s, i) => {
          const n = i + 1, state = step === n ? "on" : step > n ? "done" : "";
          return (
            <li key={s} className={state} aria-current={step === n ? "step" : undefined}>
              <span className="dot">{step > n ? <Check size={14} aria-hidden="true" /> : n}</span>
              <span className="lbl">{s}</span>
            </li>
          );
        })}
      </ol>

      {step === 1 && (
        <div className="imp-body">
          <div className="imp-sec">
            <h3 className="imp-sec-title"><span className="num">1</span>選擇券商與帳戶</h3>
            <div className="form-grid two">
              <div className="field">
                <label htmlFor="csv-broker">券商（決定欄位對應預設）</label>
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
              </div>
            </div>
            <p className="dim imp-hint">同一家券商有兩個帳戶時，用不同帳戶名稱區分。{presets.some((p) => p.name === broker) && ` 已有「${broker}」欄位對應預設，上傳後自動套用。`}</p>
          </div>

          <div className="imp-sec">
            <h3 className="imp-sec-title"><span className="num">2</span>上傳對帳單檔案</h3>
            <div
              className={`dropzone ${over ? "over" : ""}`} role="button" tabIndex={0}
              onClick={() => inputRef.current?.click()} onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && inputRef.current?.click()}
              onDragOver={(e) => { e.preventDefault(); setOver(true); }} onDragLeave={() => setOver(false)}
              onDrop={(e) => { e.preventDefault(); setOver(false); readFile(e.dataTransfer.files?.[0]); }}
            >
              <FileUp size={32} aria-hidden="true" />
              <div className="dz-main">拖曳檔案到這裡，或點擊選擇檔案</div>
              <div className="dim">支援 .xlsx 與 CSV（UTF-8、Big5）・從券商 App／網站匯出「交易明細」或「對帳單」；國泰複委託美股對帳單會自動辨識</div>
              <input ref={inputRef} type="file" accept=".csv,.txt,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" hidden onChange={(e) => readFile(e.target.files?.[0])} />
            </div>
          </div>
        </div>
      )}

      {step === 2 && table && (
        <div className="imp-body">
          <div className="imp-summary">
            <span>帳戶：<b>{account.trim() || broker}</b></span>
            <span>檔案：{fileName}</span>
            <span>表頭在第 {table.headerLine + 1} 列・共 {table.rows.length} 筆</span>
          </div>
          <div className="imp-sec">
            <h3 className="imp-sec-title">必填欄位<span className="dim">每個都要對應到檔案的表頭</span></h3>
            <div className="form-grid">{REQUIRED.map(fieldSelect)}</div>
          </div>
          <div className="imp-sec">
            <h3 className="imp-sec-title">數量<span className="dim">成交股數與成交張數擇一即可</span></h3>
            <div className="form-grid">{QTY.map(fieldSelect)}</div>
          </div>
          <div className="imp-sec">
            <h3 className="imp-sec-title">選填欄位</h3>
            <div className="form-grid">{EXTRA.map(fieldSelect)}</div>
          </div>
          {(missing.length > 0 || qtyMissing) && (
            <div className="notice warn">尚未對應：{[...missing.map((f) => f.label), ...(qtyMissing ? ["成交股數或張數"] : [])].join("、")}</div>
          )}
          <div className="imp-actions">
            <button type="button" className="btn" onClick={() => setStep(1)}>重新上傳</button>
            <button type="button" className="btn" onClick={savePreset}><Save size={16} />存成「{broker}」預設</button>
            <button type="button" className="btn primary" disabled={missing.length > 0 || qtyMissing} onClick={() => setStep(3)}>下一步：預覽</button>
          </div>
        </div>
      )}

      {step === 3 && (
        <div className="imp-body">
          <div className="imp-stats">
            <div className="imp-stat ok"><b>{okRows.length}</b><span>可匯入</span></div>
            <div className={`imp-stat ${badRows.length ? "bad" : ""}`}><b>{badRows.length}</b><span>有問題（不匯入）</span></div>
            {splitRows.length > 0 && <div className="imp-stat"><b>{splitRows.length}</b><span>股票分割（成本不變）</span></div>}
          </div>
          {us && <p className="dim imp-hint">偵測到國泰複委託「美股」對帳單（金額為美元），將匯入美股持股；已匯入過的 {usDup} 筆會自動略過。</p>}
          <p className="dim imp-hint">已存在的交易（同日期、代號、類別、股數、價格、費用）會自動略過，不會重複。</p>
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
                        <td className="left">{r.tx.stock_id ?? r.tx.ticker} {r.tx.stock_name ?? r.tx.name}{us && <Badge tone="blue">美股</Badge>}</td>
                        <td className="left"><span className={r.tx.side === "buy" ? "up" : r.tx.side === "sell" ? "down" : ""}>{sideLabel(r.tx)}</span></td>
                        <td>{fmt(r.tx.shares, 4)}</td><td>{fmt(r.tx.price)}</td><td>{fmtInt(r.tx.fee)}</td><td>{fmtInt(r.tx.tax ?? r.tx.other_fee)}</td>
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
          <div className="imp-actions">
            <button type="button" className="btn" onClick={() => setStep(us ? 1 : 2)}>上一步</button>
            <button type="button" className="btn primary" disabled={!okRows.length || busy} onClick={doImport}>{busy ? "匯入中…" : `匯入 ${okRows.length} 筆`}</button>
          </div>
        </div>
      )}

      {step === 4 && result && (
        <div className="imp-body imp-done">
          <CheckCircle2 size={40} aria-hidden="true" />
          <h3>匯入完成</h3>
          <p>新增 {result.inserted} 筆，略過重複 {result.skipped} 筆{result.bad ? `，有問題未匯入 ${result.bad} 筆` : ""}。{us ? "（美股）" : <><br />欄位對應已存成「{broker}」預設。</>}</p>
          <div className="imp-actions center">
            <button type="button" className="btn primary" onClick={() => onDone?.()}>查看持股</button>
            <button type="button" className="btn" onClick={() => { setStep(1); setTable(null); setUs(null); setResult(null); }}>再匯入一份</button>
          </div>
        </div>
      )}
      {err && <div className="notice err" style={{ marginTop: 10 }}>{err}<div style={{ marginTop: 6 }}><button type="button" className="btn sm" onClick={() => setErr("")}>知道了</button></div></div>}
    </section>
  );
}
