// 我的持股：總覽 KPI、資產變化曲線、持股明細（即時市值、建議）
import React, { useEffect, useMemo, useState } from "react";
import { FileUp, Plus, Wallet, PieChart as PieIcon, Download } from "lucide-react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";
import EquityChart from "../components/EquityChart.jsx";
import TxForm from "../components/TxForm.jsx";
import { Badge, Card, Empty, Kpi, Seg } from "../components/ui.jsx";
import { useApp } from "../lib/appState.jsx";
import { isStale, useQuotes } from "../lib/useQuotes.js";
import { accountOf, computePositions, equityCurve, listAccounts, periodChange, valuePositions } from "../lib/portfolio.js";
import AccountPicker, { ALL, SPLIT, useAccountView } from "../components/AccountPicker.jsx";
import { fetchLongHistory } from "../lib/market.js";
import { fetchStockBundle, loadLatestPrices, normalizeRows } from "../lib/data.js";
import { computeIndicators } from "../lib/indicators.js";
import { buildStrategy } from "../lib/strategy.js";
import { navigate } from "../lib/router.js";
import { supabase } from "../lib/supabase.js";
import { fmt, fmtInt, fmtMoney, signed, todayISO, trendColor } from "../lib/format.js";

const PIE = ["#3b82f6", "#f59e0b", "#a78bfa", "#38bdf8", "#f97316", "#14b8a6", "#e879f9", "#facc15", "#64748b", "#fb7185"];

export default function PortfolioPage() {
  const app = useApp();
  const [txModal, setTxModal] = useState(false);
  const [history, setHistory] = useState({});
  const [histLoading, setHistLoading] = useState(false);
  const [closes, setCloses] = useState({});
  const [advice, setAdvice] = useState({});
  const [sort, setSort] = useState("mv");
  const accounts = useMemo(() => listAccounts(app.transactions), [app.transactions]);
  const [view, setView] = useAccountView(accounts);
  const txs = useMemo(() => (view === ALL || view === SPLIT ? app.transactions : app.transactions.filter((t) => accountOf(t) === view)), [app.transactions, view]);
  const positions = useMemo(() => computePositions(txs, { byAccount: view === SPLIT }), [txs, view]);

  const codes = useMemo(() => [...new Set(app.transactions.map((t) => t.stock_id))], [app.transactions]);
  const heldCodes = useMemo(() => [...new Set(app.positions.filter((p) => p.shares > 0).map((p) => p.code))], [app.positions]);
  const q = useQuotes(heldCodes);

  useEffect(() => { loadLatestPrices().then(setCloses); }, []);

  // 每檔交易過的股票抓歷史收盤（資產曲線用）
  useEffect(() => {
    if (!codes.length) return;
    let alive = true;
    const start = app.transactions.reduce((m, t) => (t.trade_date < m ? t.trade_date : m), "9999");
    setHistLoading(true);
    Promise.all(codes.map(async (c) => {
      try {
        const bars = await fetchLongHistory(c, start);
        return [c, bars.map((b) => ({ time: b.time, close: b.close }))];
      } catch {
        const b = await fetchStockBundle(c);
        return [c, normalizeRows(b?.kline).map((r) => ({ time: r.time, close: r.close }))];
      }
    })).then((pairs) => { if (alive) { setHistory(Object.fromEntries(pairs)); setHistLoading(false); } });
    return () => { alive = false; };
  }, [codes.join(","), app.transactions]);

  // 每檔持股的進出場建議（依標記風格）
  useEffect(() => {
    let alive = true;
    (async () => {
      const out = {};
      for (const p of app.positions.filter((x) => x.shares > 0)) {
        const b = await fetchStockBundle(p.code);
        const rows = computeIndicators(normalizeRows(b?.kline).map(({ time, open, high, low, close, volume }) => ({ time, open, high, low, close, volume })));
        const s = buildStrategy({ rows, chipData: b?.chip, fundamentals: b?.fundamentals, financials: b?.financials, dividends: [], position: { shares: p.shares, avgCost: p.avgCost }, style: app.styleOf(p.code) });
        if (s) out[p.code] = s.primary;
      }
      if (alive) setAdvice(out);
    })();
    return () => { alive = false; };
  }, [app.positions, app.styleOf]);

  const prices = useMemo(() => {
    const p = {};
    // 即時報價 > 歷史行情最新收盤（FinMind 收盤後當天就有）> prices.json（前一次排程）
    for (const c of codes) p[c] = Number.isFinite(q.quotes[c]?.price) ? q.quotes[c].price : history[c]?.at(-1)?.close ?? closes[c];
    return p;
  }, [codes, q.quotes, closes, history]);
  const live = useMemo(() => Object.fromEntries(Object.entries(q.quotes).map(([c, v]) => [c, v.price]).filter(([, v]) => Number.isFinite(v))), [q.quotes]);

  const { held, totals } = useMemo(() => valuePositions(positions, prices), [positions, prices]);
  const curve = useMemo(() => equityCurve(txs, history, { livePrices: live, today: todayISO() }), [txs, history, live]);
  // 今日損益：昨日已持有的股數 × 漲跌；今天買進的以（現價 − 成交價）計
  const dayPnl = useMemo(() => {
    const today = todayISO();
    let sum = 0;
    for (const r of held) {
      const qq = q.quotes[r.code];
      if (!Number.isFinite(qq?.price) || !Number.isFinite(qq?.change)) continue;
      const todays = txs.filter((t) => t.stock_id === r.code && t.trade_date === today && t.side === "buy" && (!r.account || accountOf(t) === r.account));
      const boughtToday = todays.reduce((s, t) => s + Number(t.shares), 0);
      sum += qq.change * Math.max(0, r.shares - boughtToday);
      sum += todays.reduce((s, t) => s + (qq.price - Number(t.price)) * Number(t.shares), 0);
    }
    return sum;
  }, [held, q.quotes, txs]);
  const week = periodChange(curve, 5), month = periodChange(curve, 21);

  const sorted = useMemo(() => {
    const key = { mv: "marketValue", pnl: "unrealized", pct: "unrealizedPct", code: "code" }[sort];
    return [...held].sort((a, b) => (key === "code" ? a.code.localeCompare(b.code) || String(a.account).localeCompare(String(b.account)) : (b[key] || 0) - (a[key] || 0)));
  }, [held, sort]);

  if (!app.configured) return <div className="page"><div className="notice warn">網站尚未設定 Supabase 金鑰，無法使用持股功能。</div></div>;
  if (!app.user) {
    return (
      <div className="page">
        <Card><Empty icon={Wallet} title="登入後管理你的持股、看資產變化">
          <button type="button" className="btn primary" onClick={app.signIn}>Google 登入</button>
        </Empty></Card>
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">MY PORTFOLIO</div>
          <h1 className="page-title">我的持股</h1>
          <div className="dim" style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
            {q.live ? <><i className="live-dot" aria-hidden="true" />盤中即時 {q.updatedAt?.toLocaleTimeString("zh-TW")}{isStale(q.updatedAt, q.live) && <Badge tone="warn">延遲</Badge>}</> : "收盤後以最新收盤價計算"}
            {histLoading && <span>・歷史行情載入中…</span>}
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          <AccountPicker accounts={accounts} value={view} onChange={setView} />
          <button type="button" className="btn" onClick={() => navigate("/transactions", { import: 1 })}><FileUp size={16} />匯入 CSV</button>
          <button type="button" className="btn primary" onClick={() => setTxModal(true)}><Plus size={16} />記一筆</button>
        </div>
      </div>

      {app.error && <div className="notice err" style={{ marginBottom: 12 }}>{app.error}</div>}

      {!app.transactions.length ? (
        <Card>
          <Empty icon={Wallet} title="還沒有任何交易紀錄">
            <div className="muted">匯入國泰、亞東等券商的對帳單 CSV，或手動記一筆。</div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "center" }}>
              <button type="button" className="btn primary" onClick={() => navigate("/transactions", { import: 1 })}><FileUp size={16} />匯入對帳單</button>
              <button type="button" className="btn" onClick={() => setTxModal(true)}><Plus size={16} />手動新增</button>
              <MigrateWatchlist />
            </div>
          </Empty>
        </Card>
      ) : (
        <>
          <div className="kpis">
            <Kpi label="總市值" value={fmtMoney(totals.marketValue)} sub={`投入成本 ${fmtMoney(totals.cost)}`} />
            <Kpi label="未實現損益" value={signed(totals.unrealized, 0)} sub={signed(totals.unrealizedPct, 2, "%")} tone={totals.unrealized >= 0 ? "up" : "down"} />
            <Kpi label="今日損益" value={signed(dayPnl, 0)} sub={q.live ? "盤中" : "最近交易日"} tone={dayPnl >= 0 ? "up" : "down"} />
            <Kpi label="已實現＋股利" value={signed(totals.realized + totals.dividends, 0)} sub={`股利 ${fmtInt(totals.dividends)}`} tone={totals.realized + totals.dividends >= 0 ? "up" : "down"} />
            <Kpi label="總損益" value={signed(totals.totalPnl, 0)} sub={`近一週 ${signed(week?.diff, 0)}・近一月 ${signed(month?.diff, 0)}`} tone={totals.totalPnl >= 0 ? "up" : "down"} />
          </div>

          <div className="grid" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 340px), 1fr))", marginTop: 12 }}>
            <Card title="資產變化" icon={Wallet} className="span-2">
              <EquityChart curve={curve} />
            </Card>
            <Card title="持股配置" icon={PieIcon}>
              <div style={{ height: 220 }}>
                <ResponsiveContainer>
                  <PieChart>
                    <Pie data={sorted.map((r) => ({ name: `${r.code} ${r.name || ""}${r.account ? `（${r.account}）` : ""}`, value: r.marketValue || 0 }))} dataKey="value" nameKey="name" innerRadius={55} outerRadius={90} isAnimationActive={false}>
                      {sorted.map((r, i) => <Cell key={r.code} fill={PIE[i % PIE.length]} />)}
                    </Pie>
                    <Tooltip contentStyle={{ background: "#0e1223", border: "1px solid #334155", borderRadius: 8 }} formatter={(v) => fmtInt(v)} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div style={{ display: "grid", gap: 4 }}>
                {sorted.map((r, i) => (
                  <div key={`${r.code}|${r.account}`} style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13 }}>
                    <i style={{ width: 10, height: 10, borderRadius: 3, background: PIE[i % PIE.length] }} aria-hidden="true" />
                    <span>{r.code} {r.name}{r.account && <span className="dim">（{r.account}）</span>}</span>
                    <b className="num" style={{ marginLeft: "auto" }}>{fmt(r.weight, 1)}%</b>
                  </div>
                ))}
              </div>
            </Card>
          </div>

          <Card title={`持股明細（${held.length} ${view === SPLIT ? "筆" : "檔"}）`} icon={Wallet} style={{ marginTop: 12 }} right={
            <Seg label="排序" options={[{ value: "mv", label: "市值" }, { value: "pnl", label: "損益" }, { value: "pct", label: "報酬率" }, { value: "code", label: "代號" }]} value={sort} onChange={setSort} />
          }>
            <div className="table-wrap desktop-table">
              <table className="data">
                <thead>
                  <tr>
                    <th className="left">股票</th><th className="left">{view === SPLIT ? "帳戶" : "風格"}</th><th>股數</th><th>均價</th><th>現價</th><th>今日</th>
                    <th>市值</th><th>未實現</th><th>報酬率</th><th>權重</th><th className="left">建議</th>
                  </tr>
                </thead>
                <tbody>
                  {sorted.map((r) => {
                    const qq = q.quotes[r.code];
                    const adv = advice[r.code];
                    return (
                      <tr key={`${r.code}|${r.account}`} className="clickable" onClick={() => navigate(`/stock/${r.code}`, { tab: "position" })}>
                        <td className="left"><b className="num" style={{ color: "var(--gold)" }}>{r.code}</b> {r.name}
                          {view === ALL && r.accounts?.length > 1 && <span className="dim">（{r.accounts.length} 個帳戶）</span>}</td>
                        <td className="left">{view === SPLIT ? <Badge>{r.account}</Badge> : <Badge tone={app.styleOf(r.code) === "波段" ? "warn" : "blue"}>{app.styleOf(r.code)}</Badge>}</td>
                        <td>{fmtInt(r.shares)}</td>
                        <td>{fmt(r.avgCost)}</td>
                        <td style={{ color: trendColor(qq?.change), fontWeight: 700 }}>{fmt(r.price)}</td>
                        <td style={{ color: trendColor(qq?.change) }}>{signed(qq?.changePct, 2, "%")}</td>
                        <td>{fmtInt(r.marketValue)}</td>
                        <td style={{ color: trendColor(r.unrealized) }}>{signed(r.unrealized, 0)}</td>
                        <td style={{ color: trendColor(r.unrealizedPct) }}>{signed(r.unrealizedPct, 2, "%")}</td>
                        <td>{fmt(r.weight, 1)}%</td>
                        <td className="left">{adv ? <span className={adv.tone === "up" ? "up" : adv.tone === "down" ? "down" : ""} style={{ fontWeight: 700 }} title={adv.why}>{adv.label}</span> : <span className="dim">…</span>}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <div className="mobile-cards">
              {sorted.map((r) => {
                const qq = q.quotes[r.code];
                const adv = advice[r.code];
                return (
                  <button type="button" key={`${r.code}|${r.account}`} className="card" style={{ textAlign: "left", cursor: "pointer", padding: 12 }} onClick={() => navigate(`/stock/${r.code}`, { tab: "position" })}>
                    <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
                      <b className="num" style={{ color: "var(--gold)" }}>{r.code}</b><span>{r.name}</span>
                      {r.account && <span className="dim">{r.account}</span>}
                      <span className="num" style={{ marginLeft: "auto", color: trendColor(qq?.change), fontWeight: 800 }}>{fmt(r.price)}</span>
                    </div>
                    <div className="num" style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginTop: 6 }}>
                      <span className="muted">{fmtInt(r.shares)} 股 @ {fmt(r.avgCost)}</span>
                      <span style={{ color: trendColor(r.unrealized) }}>{signed(r.unrealized, 0)}（{signed(r.unrealizedPct, 1, "%")}）</span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginTop: 4 }}>
                      <span className="muted">市值 {fmtInt(r.marketValue)}・{fmt(r.weight, 1)}%</span>
                      {adv && <span className={adv.tone === "up" ? "up" : adv.tone === "down" ? "down" : ""} style={{ fontWeight: 700 }}>{adv.label}</span>}
                    </div>
                  </button>
                );
              })}
            </div>
            <div className="dim" style={{ marginTop: 8 }}>均價採移動平均成本（含手續費），各帳戶分開計算後再合併。點任一檔看個股部位與進出場建議；「風格」可在個股頁的進出場頁籤切換。</div>
          </Card>
        </>
      )}
      {txModal && <TxForm onClose={() => setTxModal(false)} />}
    </div>
  );
}

// 舊版「存股清單」（watchlist：均價＋張數）→ 轉成一筆買進交易
function MigrateWatchlist() {
  const app = useApp();
  const [msg, setMsg] = useState("");
  const [busy, setBusy] = useState(false);
  async function run() {
    setBusy(true); setMsg("");
    const { data, error } = await supabase.from("watchlist").select("*");
    if (error) { setMsg(`讀取舊清單失敗：${error.message}`); setBusy(false); return; }
    const rows = (data || []).filter((w) => w.avg_cost > 0 && w.shares > 0).map((w) => ({
      trade_date: String(w.created_at || new Date().toISOString()).slice(0, 10),
      stock_id: w.stock_id, stock_name: w.stock_name, side: "buy", shares: w.shares * 1000, price: w.avg_cost,
      fee: 0, tax: 0, source: "watchlist", import_hash: `watchlist|${w.id}`, note: w.note || "由舊存股清單轉入",
    }));
    if (!rows.length) { setMsg("舊清單沒有可轉入的資料"); setBusy(false); return; }
    const { error: e2 } = await supabase.from("transactions").upsert(rows, { onConflict: "user_id,import_hash", ignoreDuplicates: true });
    setBusy(false);
    if (e2) setMsg(`轉入失敗：${e2.message}`);
    else { setMsg(`已轉入 ${rows.length} 檔`); app.reload(); }
  }
  return (
    <>
      <button type="button" className="btn" onClick={run} disabled={busy}><Download size={16} />{busy ? "轉入中…" : "從舊存股清單轉入"}</button>
      {msg && <div className="dim" style={{ width: "100%" }}>{msg}</div>}
    </>
  );
}
