import React, { useState, useEffect, useRef, useCallback } from "react";

const API = "https://stock-analysis-tw.fly.dev";
const PAGE_VERSION = "batch-v6-real-data";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── styles ───────────────────────────────────────────────────────────────────
const S = {
  page:    { background:"#020617", color:"#e2e8f0", padding:"16px 20px", fontFamily:"system-ui,sans-serif", minHeight:"100vh" },
  header:  { marginBottom:20 },
  badge:   { color:"#38bdf8", fontWeight:800, fontSize:11, letterSpacing:1, textTransform:"uppercase" },
  title:   { margin:"6px 0 2px", fontSize:22, fontWeight:800 },
  muted:   { color:"#64748b", fontSize:12 },
  card:    { border:"1px solid #1e293b", borderRadius:14, padding:16, marginBottom:14, background:"#0f172a" },
  cardTitle:{ marginTop:0, marginBottom:12, fontSize:15, fontWeight:700, color:"#cbd5e1" },
  row:     { display:"flex", gap:10, flexWrap:"wrap", alignItems:"flex-end", marginBottom:12 },
  col:     { display:"flex", flexDirection:"column", gap:5 },
  label:   { fontSize:12, color:"#94a3b8" },
  input:   { padding:"8px 10px", borderRadius:8, border:"1px solid #334155", background:"#020617", color:"#e2e8f0", fontSize:13, width:110 },
  select:  { padding:"8px 10px", borderRadius:8, border:"1px solid #334155", background:"#020617", color:"#e2e8f0", fontSize:13 },
  btn:     (color="#2563eb",disabled=false) => ({
    padding:"9px 16px", borderRadius:9, border:0,
    background: disabled ? "#1e293b" : color,
    color: disabled ? "#475569" : "white",
    fontWeight:700, fontSize:13, cursor: disabled ? "not-allowed" : "pointer",
    transition:"opacity .15s",
  }),
  grid2:   { display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))", gap:12, marginTop:12 },
  grid3:   { display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:10, marginBottom:12 },
  panel:   { border:"1px solid #1e293b", borderRadius:10, padding:12, background:"#020617" },
  pre:     { maxHeight:260, overflow:"auto", color:"#94a3b8", fontSize:11, margin:0, whiteSpace:"pre-wrap", wordBreak:"break-all" },
  logLine: { fontSize:12, color:"#94a3b8", padding:"2px 0", borderBottom:"1px solid #0f172a" },
  prog:    { height:6, borderRadius:3, background:"#1e293b", overflow:"hidden", marginBottom:8 },
  progBar: (pct,color="#2563eb") => ({ height:"100%", width:`${Math.min(100,pct)}%`, background:color, transition:"width .4s" }),
  tag:     (ok) => ({
    display:"inline-block", padding:"2px 8px", borderRadius:20, fontSize:11, fontWeight:700,
    background: ok===true?"rgba(34,197,94,.15)": ok===false?"rgba(239,68,68,.15)":"rgba(148,163,184,.1)",
    color: ok===true?"#22c55e": ok===false?"#ef4444":"#94a3b8",
  }),
};

function addLog(setLogs, msg) {
  const ts = new Date().toLocaleTimeString("zh-TW", { hour12:false });
  setLogs((p) => [`[${ts}] ${msg}`, ...p].slice(0, 50));
}

// ── Job poller ───────────────────────────────────────────────────────────────
function useJobPoller() {
  const [jobs, setJobs] = useState({});
  const timers = useRef({});

  const poll = useCallback((jobId, onDone) => {
    if (timers.current[jobId]) return;
    timers.current[jobId] = setInterval(async () => {
      try {
        const res = await fetch(`${API}/api/batch/job/${encodeURIComponent(jobId)}`, { cache:"no-store" });
        if (!res.ok) return;
        const j = await res.json();
        setJobs((p) => ({ ...p, [jobId]: j }));
        if (j.status !== "running") {
          clearInterval(timers.current[jobId]);
          delete timers.current[jobId];
          if (onDone) onDone(j);
        }
      } catch {}
    }, 2500);
  }, []);

  useEffect(() => () => Object.values(timers.current).forEach(clearInterval), []);
  return { jobs, poll };
}

// ── Section 1 : Connection Tests ─────────────────────────────────────────────
function ConnTestSection() {
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState(null);

  async function run() {
    setBusy(true);
    setResult(null);
    try {
      const res = await fetch(`${API}/api/batch/test`, { cache:"no-store" });
      setResult(await res.json());
    } catch (e) {
      setResult({ error: String(e) });
    } finally {
      setBusy(false);
    }
  }

  const r = result?.results || {};
  const checks = [
    { key:"twse_t86",   label:"TWSE T86 籌碼 API" },
    { key:"tpex_insti", label:"TPEx 法人 API" },
    { key:"postgresql", label:"PostgreSQL (Supabase)" },
  ];

  return (
    <div style={S.card}>
      <h3 style={S.cardTitle}>🔌 連線測試</h3>
      <div style={S.grid3}>
        {checks.map(({ key, label }) => {
          const v = r[key];
          return (
            <div key={key} style={{ ...S.panel, textAlign:"center" }}>
              <div style={{ fontSize:22, marginBottom:4 }}>
                {!result ? "○" : v?.ok ? "✅" : "❌"}
              </div>
              <div style={{ fontSize:12, fontWeight:700, color:"#cbd5e1", marginBottom:4 }}>{label}</div>
              {v && (
                <>
                  <span style={S.tag(v.ok)}>{v.ok ? "OK" : "FAIL"}</span>
                  {v.rows !== undefined && <div style={{ color:"#64748b", fontSize:11, marginTop:3 }}>{v.rows} rows</div>}
                  {v.error && <div style={{ color:"#ef4444", fontSize:10, marginTop:3, wordBreak:"break-all" }}>{v.error}</div>}
                </>
              )}
            </div>
          );
        })}
      </div>
      <div style={{ display:"flex", gap:10, alignItems:"center" }}>
        <button style={S.btn("#0f766e", busy)} disabled={busy} onClick={run}>
          {busy ? "測試中…" : "執行連線測試"}
        </button>
        {result?.tested_at && <span style={S.muted}>測試時間：{result.tested_at}</span>}
      </div>
    </div>
  );
}

// ── Section 2 : Chip Data ────────────────────────────────────────────────────
function ChipSection({ logs, setLogs }) {
  const { jobs, poll } = useJobPoller();

  // Today
  const [todayBusy, setTodayBusy] = useState(false);
  const [todayResult, setTodayResult] = useState(null);
  const [todayDate, setTodayDate] = useState("");

  async function runToday() {
    setTodayBusy(true);
    addLog(setLogs, `籌碼今日寫入：開始 (date=${todayDate || "today"})`);
    try {
      const url = `${API}/api/batch/chip/today${todayDate ? `?date=${todayDate}` : ""}`;
      const res = await fetch(url, { cache:"no-store" });
      const j = await res.json();
      setTodayResult(j);
      addLog(setLogs, `籌碼今日完成：TWSE ${j.twse_chips} 筆，TPEx ${j.tpex_chips} 筆，錯誤 ${(j.errors||[]).length}`);
    } catch (e) {
      addLog(setLogs, `籌碼今日失敗：${e.message}`);
      setTodayResult({ error: String(e) });
    } finally {
      setTodayBusy(false);
    }
  }

  // History backfill
  const [histMonths, setHistMonths] = useState(3);
  const [histJobId, setHistJobId] = useState(null);
  const histJob = histJobId ? jobs[histJobId] : null;

  async function runHistory() {
    addLog(setLogs, `籌碼歷史回補：啟動 ${histMonths} 個月`);
    try {
      const res = await fetch(`${API}/api/batch/chip/history?months=${histMonths}`, { cache:"no-store" });
      const j = await res.json();
      setHistJobId(j.job_id);
      poll(j.job_id, (done) => {
        const r = done.result || {};
        addLog(setLogs, `籌碼歷史完成：T86 ${r.t86_written}，margin ${r.margin_written}，TPEx法人 ${r.tpex_t86_written}，TPEx融資 ${r.tpex_margin_written}，處理${r.processed_dates}天`);
      });
    } catch (e) {
      addLog(setLogs, `籌碼歷史失敗：${e.message}`);
    }
  }

  return (
    <div style={S.card}>
      <h3 style={S.cardTitle}>📊 籌碼資料 (chip_daily)</h3>

      {/* Today */}
      <div style={{ borderBottom:"1px solid #1e293b", paddingBottom:14, marginBottom:14 }}>
        <div style={{ fontWeight:700, fontSize:13, color:"#94a3b8", marginBottom:8 }}>今日籌碼寫入（TWSE T86 + 融資融券 + TPEx 法人 + TPEx 融資）</div>
        <div style={S.row}>
          <div style={S.col}>
            <span style={S.label}>指定日期（留空=今日）</span>
            <input style={S.input} placeholder="YYYYMMDD" value={todayDate} onChange={(e) => setTodayDate(e.target.value)} />
          </div>
          <button style={S.btn("#0369a1", todayBusy)} disabled={todayBusy} onClick={runToday}>
            {todayBusy ? "寫入中…" : "寫入真實籌碼"}
          </button>
        </div>
        {todayResult && (
          <div style={{ display:"flex", gap:16, flexWrap:"wrap", fontSize:12 }}>
            {[
              ["TWSE 三大法人", todayResult.twse_chips],
              ["TWSE 融資券",   todayResult.twse_margin],
              ["TPEx 三大法人", todayResult.tpex_chips],
              ["TPEx 融資券",   todayResult.tpex_margin],
            ].map(([l, v]) => (
              <div key={l} style={{ background:"#1e293b", borderRadius:8, padding:"6px 12px" }}>
                <div style={{ color:"#64748b" }}>{l}</div>
                <div style={{ color:"#22c55e", fontWeight:700, fontSize:16 }}>{v ?? "—"}</div>
              </div>
            ))}
            {(todayResult.errors || []).slice(0, 3).map((e, i) => (
              <div key={i} style={{ color:"#ef4444", fontSize:11, alignSelf:"center" }}>{e}</div>
            ))}
          </div>
        )}
      </div>

      {/* History */}
      <div>
        <div style={{ fontWeight:700, fontSize:13, color:"#94a3b8", marginBottom:8 }}>歷史批次回補（背景執行）</div>
        <div style={S.row}>
          <div style={S.col}>
            <span style={S.label}>回補月數</span>
            <select style={S.select} value={histMonths} onChange={(e) => setHistMonths(Number(e.target.value))}>
              {[1,2,3,6,12].map((m) => <option key={m} value={m}>{m} 個月 (~{m*22} 天)</option>)}
            </select>
          </div>
          <button style={S.btn("#7c3aed", !!(histJob?.status === "running"))} disabled={histJob?.status === "running"} onClick={runHistory}>
            {histJob?.status === "running" ? "回補中…" : "啟動歷史回補"}
          </button>
        </div>
        {histJob && <JobCard job={histJob} />}
      </div>
    </div>
  );
}

// ── Section 3 : Stock Daily ──────────────────────────────────────────────────
function StockSection({ logs, setLogs }) {
  const { jobs, poll } = useJobPoller();

  // Daily update
  const [dailyLookback, setDailyLookback] = useState(5);
  const [dailyJobId, setDailyJobId] = useState(null);
  const dailyJob = dailyJobId ? jobs[dailyJobId] : null;

  async function runDaily() {
    addLog(setLogs, `K線今日更新：啟動 lookback=${dailyLookback}天`);
    try {
      const res = await fetch(`${API}/api/batch/stock/today?lookback_days=${dailyLookback}`, { cache:"no-store" });
      const j = await res.json();
      setDailyJobId(j.job_id);
      poll(j.job_id, (done) => {
        const r = done.result || {};
        addLog(setLogs, `K線今日完成：寫入 ${r.stocks} 筆，日期 ${(r.dates_written||[]).join(",")||r.twse_date}`);
      });
    } catch (e) {
      addLog(setLogs, `K線今日失敗：${e.message}`);
    }
  }

  // Single stock
  const [singleCode, setSingleCode] = useState("");
  const [singleMonths, setSingleMonths] = useState(12);
  const [singleMarket, setSingleMarket] = useState("TWSE");
  const [singleJobId, setSingleJobId] = useState(null);
  const singleJob = singleJobId ? jobs[singleJobId] : null;

  async function runSingle() {
    if (!singleCode.trim()) return;
    addLog(setLogs, `個股回補：${singleCode} ${singleMarket} ${singleMonths}月`);
    try {
      const res = await fetch(`${API}/api/batch/stock/backfill?stock=${encodeURIComponent(singleCode)}&months=${singleMonths}&market=${singleMarket}`, { cache:"no-store" });
      const j = await res.json();
      setSingleJobId(j.job_id);
      poll(j.job_id, (done) => {
        const r = done.result || {};
        addLog(setLogs, `個股完成：${singleCode} 寫入 ${r.written_days} 天`);
      });
    } catch (e) {
      addLog(setLogs, `個股失敗：${e.message}`);
    }
  }

  // Universe batch
  const [uniType, setUniType] = useState("股票");
  const [uniMarket, setUniMarket] = useState("上市");
  const [uniOffset, setUniOffset] = useState(0);
  const [uniLimit, setUniLimit] = useState(50);
  const [uniMonths, setUniMonths] = useState(12);
  const [uniJobId, setUniJobId] = useState(null);
  const [uniNextOffset, setUniNextOffset] = useState(null);
  const uniJob = uniJobId ? jobs[uniJobId] : null;

  async function runUniverse() {
    addLog(setLogs, `全市場批次：${uniMarket} ${uniType} offset=${uniOffset} limit=${uniLimit} ${uniMonths}月`);
    try {
      const url = `${API}/api/batch/stock/universe?product_type=${encodeURIComponent(uniType)}&market=${encodeURIComponent(uniMarket)}&offset=${uniOffset}&limit=${uniLimit}&months=${uniMonths}`;
      const res = await fetch(url, { cache:"no-store" });
      const j = await res.json();
      setUniJobId(j.job_id);
      if (j.next_offset != null) setUniNextOffset(j.next_offset);
      poll(j.job_id, (done) => {
        const r = done.result || {};
        addLog(setLogs, `批次完成：${r.stocks_done} 檔，${r.written_days} 天資料，next=${r.next_offset ?? "結束"}`);
        if (r.next_offset != null) setUniOffset(r.next_offset);
      });
    } catch (e) {
      addLog(setLogs, `批次失敗：${e.message}`);
    }
  }

  return (
    <div style={S.card}>
      <h3 style={S.cardTitle}>📈 K線 / 股價資料 (stock_daily)</h3>

      {/* Today */}
      <div style={{ borderBottom:"1px solid #1e293b", paddingBottom:14, marginBottom:14 }}>
        <div style={{ fontWeight:700, fontSize:13, color:"#94a3b8", marginBottom:8 }}>今日全市場更新（TWSE + TPEx）</div>
        <div style={S.row}>
          <div style={S.col}>
            <span style={S.label}>回溯天數</span>
            <input style={{ ...S.input, width:70 }} type="number" min={1} max={20} value={dailyLookback} onChange={(e) => setDailyLookback(Number(e.target.value))} />
          </div>
          <button style={S.btn("#0f766e", dailyJob?.status === "running")} disabled={dailyJob?.status === "running"} onClick={runDaily}>
            {dailyJob?.status === "running" ? "更新中…" : "今日全市場更新"}
          </button>
        </div>
        {dailyJob && <JobCard job={dailyJob} />}
      </div>

      {/* Single */}
      <div style={{ borderBottom:"1px solid #1e293b", paddingBottom:14, marginBottom:14 }}>
        <div style={{ fontWeight:700, fontSize:13, color:"#94a3b8", marginBottom:8 }}>個股歷史回補</div>
        <div style={S.row}>
          <div style={S.col}>
            <span style={S.label}>股票代號</span>
            <input style={S.input} placeholder="2330" value={singleCode} onChange={(e) => setSingleCode(e.target.value)} />
          </div>
          <div style={S.col}>
            <span style={S.label}>市場</span>
            <select style={S.select} value={singleMarket} onChange={(e) => setSingleMarket(e.target.value)}>
              <option value="TWSE">上市 TWSE</option>
              <option value="TPEx">上櫃 TPEx</option>
            </select>
          </div>
          <div style={S.col}>
            <span style={S.label}>回補月數</span>
            <select style={S.select} value={singleMonths} onChange={(e) => setSingleMonths(Number(e.target.value))}>
              {[1,3,6,12,24].map((m) => <option key={m} value={m}>{m} 個月</option>)}
            </select>
          </div>
          <button style={S.btn("#0369a1", singleJob?.status === "running" || !singleCode.trim())} disabled={singleJob?.status === "running" || !singleCode.trim()} onClick={runSingle}>
            {singleJob?.status === "running" ? "回補中…" : "個股回補"}
          </button>
        </div>
        {singleJob && <JobCard job={singleJob} />}
      </div>

      {/* Universe */}
      <div>
        <div style={{ fontWeight:700, fontSize:13, color:"#94a3b8", marginBottom:8 }}>全市場批次回補</div>
        <div style={S.row}>
          <div style={S.col}>
            <span style={S.label}>市場</span>
            <select style={S.select} value={uniMarket} onChange={(e) => setUniMarket(e.target.value)}>
              <option value="上市">上市</option>
              <option value="上櫃">上櫃</option>
              <option value="all">全部</option>
            </select>
          </div>
          <div style={S.col}>
            <span style={S.label}>商品類型</span>
            <select style={S.select} value={uniType} onChange={(e) => setUniType(e.target.value)}>
              <option value="股票">股票</option>
              <option value="ETF">ETF</option>
              <option value="all">全部</option>
            </select>
          </div>
          <div style={S.col}>
            <span style={S.label}>起始位置</span>
            <input style={S.input} type="number" min={0} value={uniOffset} onChange={(e) => setUniOffset(Number(e.target.value))} />
          </div>
          <div style={S.col}>
            <span style={S.label}>每批筆數 (max 100)</span>
            <input style={S.input} type="number" min={1} max={100} value={uniLimit} onChange={(e) => setUniLimit(Number(e.target.value))} />
          </div>
          <div style={S.col}>
            <span style={S.label}>回補月數</span>
            <select style={S.select} value={uniMonths} onChange={(e) => setUniMonths(Number(e.target.value))}>
              {[1,3,6,12,24].map((m) => <option key={m} value={m}>{m} 月</option>)}
            </select>
          </div>
          <button style={S.btn("#7c3aed", uniJob?.status === "running")} disabled={uniJob?.status === "running"} onClick={runUniverse}>
            {uniJob?.status === "running" ? "批次中…" : "執行這批"}
          </button>
        </div>
        {uniNextOffset != null && (
          <div style={{ fontSize:12, color:"#f59e0b", marginBottom:8 }}>
            ⚡ 下一批起始位置：{uniNextOffset}
            <button style={{ ...S.btn("#92400e"), marginLeft:8, padding:"4px 10px", fontSize:11 }} onClick={() => setUniOffset(uniNextOffset)}>
              套用
            </button>
          </div>
        )}
        {uniJob && <JobCard job={uniJob} />}
      </div>
    </div>
  );
}

// ── JobCard component ────────────────────────────────────────────────────────
function JobCard({ job }) {
  if (!job) return null;
  const isRunning = job.status === "running";
  const isDone    = job.status === "done";
  const isError   = job.status === "error";
  const r         = job.result || {};

  const statusColor = isRunning ? "#f59e0b" : isDone ? "#22c55e" : "#ef4444";
  const statusLabel = isRunning ? "執行中…" : isDone ? "已完成" : "失敗";

  return (
    <div style={{ marginTop:10, padding:"10px 14px", borderRadius:10, background:"#0a1628", border:`1px solid ${statusColor}33` }}>
      <div style={{ display:"flex", gap:12, alignItems:"center", marginBottom:8 }}>
        <span style={{ ...S.tag(isDone ? true : isError ? false : null) }}>{statusLabel}</span>
        <span style={{ fontSize:11, color:"#475569" }}>{job.job_id}</span>
        {job.started_at && <span style={{ fontSize:11, color:"#475569" }}>開始 {job.started_at.slice(11,19)}</span>}
        {job.finished_at && <span style={{ fontSize:11, color:"#475569" }}>結束 {job.finished_at.slice(11,19)}</span>}
      </div>
      {isRunning && (
        <div style={S.prog}>
          <div style={{ height:"100%", background:"#2563eb", animation:"pulse 1.5s infinite", width:"100%" }} />
        </div>
      )}
      {isDone && (
        <div style={{ display:"flex", gap:12, flexWrap:"wrap", fontSize:12 }}>
          {r.stocks !== undefined     && <Stat label="股票寫入" value={r.stocks} />}
          {r.written_days !== undefined && <Stat label="天數" value={r.written_days} />}
          {r.stocks_done !== undefined  && <Stat label="處理檔數" value={r.stocks_done} />}
          {r.t86_written !== undefined  && <Stat label="T86" value={r.t86_written} />}
          {r.margin_written !== undefined && <Stat label="融資券" value={r.margin_written} />}
          {r.tpex_t86_written !== undefined && <Stat label="TPEx法人" value={r.tpex_t86_written} />}
          {r.tpex_margin_written !== undefined && <Stat label="TPEx融資" value={r.tpex_margin_written} />}
          {r.processed_dates !== undefined && <Stat label="處理天數" value={r.processed_dates} />}
          {(r.errors||[]).length > 0   && <Stat label="錯誤" value={(r.errors||[]).length} color="#ef4444" />}
        </div>
      )}
      {isError && (
        <div style={{ color:"#ef4444", fontSize:12 }}>{job.error}</div>
      )}
      {isDone && (r.errors||[]).length > 0 && (
        <details style={{ marginTop:8 }}>
          <summary style={{ fontSize:11, color:"#64748b", cursor:"pointer" }}>錯誤詳情 ({(r.errors||[]).length})</summary>
          <pre style={{ ...S.pre, maxHeight:120 }}>{(r.errors||[]).slice(0,10).join("\n")}</pre>
        </details>
      )}
    </div>
  );
}

function Stat({ label, value, color = "#22c55e" }) {
  return (
    <div style={{ textAlign:"center", background:"#1e293b", borderRadius:8, padding:"5px 12px" }}>
      <div style={{ color:"#64748b", fontSize:10 }}>{label}</div>
      <div style={{ color, fontWeight:800, fontSize:16 }}>{value}</div>
    </div>
  );
}

// ── Section 4 : Activity Log ──────────────────────────────────────────────────
function LogSection({ logs }) {
  return (
    <div style={S.card}>
      <h3 style={{ ...S.cardTitle, marginBottom:8 }}>📋 操作紀錄</h3>
      <div style={{ ...S.panel, maxHeight:220, overflowY:"auto" }}>
        {logs.length === 0
          ? <div style={S.muted}>尚無紀錄</div>
          : logs.map((l, i) => <div key={i} style={S.logLine}>{l}</div>)
        }
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function BatchPage() {
  const [logs, setLogs] = useState([]);

  return (
    <div style={S.page}>
      <style>{`@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}`}</style>
      <div style={S.header}>
        <div style={S.badge}>Admin · 批次工具</div>
        <h2 style={S.title}>資料批次後台</h2>
        <div style={S.muted}>{PAGE_VERSION} · 所有寫入操作皆為真實 TWSE/TPEx API → PostgreSQL</div>
      </div>

      <ConnTestSection />
      <ChipSection logs={logs} setLogs={setLogs} />
      <StockSection logs={logs} setLogs={setLogs} />
      <LogSection logs={logs} />
    </div>
  );
}
