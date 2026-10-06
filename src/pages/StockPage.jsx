// 個股頁：看盤圖 + 多角度頁籤（總覽 / 技術 / 籌碼 / 基本 / 我的部位 / 進出場）
import React, { useEffect, useMemo, useState } from "react";
import {
  Building2, CandlestickChart, Compass, Gauge, LayoutDashboard, Pencil, Plus, Trash2, Wallet, PiggyBank,
} from "lucide-react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import StockChart from "../components/StockChart.jsx";
import SearchBox from "../components/SearchBox.jsx";
import StrategyPanel from "../components/StrategyPanel.jsx";
import EquityChart from "../components/EquityChart.jsx";
import TxForm from "../components/TxForm.jsx";
import { Badge, Card, Empty, Kpi, Tabs } from "../components/ui.jsx";
import {
  BlackCandleCard, ChipXrayCard, FinancialsCard, FundamentalsCard, InstitutionalFlowCard, MaStatusCard,
  MomentumCard, PatternCard, RiskMirrorCard, ScenarioCard, TechRadarCard, VolPriceCard,
} from "../components/AnalysisCards.jsx";
import { fetchStockBundle, findStock, normalizeRows } from "../lib/data.js";
import {
  detectPatterns, getChipAnalysis, getKdAnalysis, getMaStatus, getOverallScore, getRiskMetrics, getRsiAnalysis, getScenarios, getVolPriceMatrix,
} from "../lib/analysis.js";
import { fetchDividends, fetchLongHistory, fetchQuarterlyEps } from "../lib/market.js";
import { computeIndicators } from "../lib/indicators.js";
import { annualCashDividends, buildStrategy } from "../lib/strategy.js";
import { accountOf, computePositions, equityCurve, listAccounts, valuePositions, SIDE_LABEL } from "../lib/portfolio.js";
import AccountPicker, { ALL, SPLIT, useAccountView } from "../components/AccountPicker.jsx";
import { isStale, useQuotes } from "../lib/useQuotes.js";
import { useApp } from "../lib/appState.jsx";
import { navigate, useRoute } from "../lib/router.js";
import { supabase } from "../lib/supabase.js";
import { DOWN, UP, fmt, fmtInt, signed, todayISO, trendColor } from "../lib/format.js";

const TABS = [
  { id: "analysis", label: "綜合分析", icon: LayoutDashboard },
  { id: "position", label: "我的部位", icon: Wallet, star: true },
  { id: "entry", label: "進出場", icon: Compass, star: true },
];

export default function StockPage() {
  const route = useRoute();
  const app = useApp();
  const code = (route.parts[1] || "2330").toUpperCase();
  // 舊網址的 overview / tech / chip / fund 都併入「綜合分析」
  const tab = TABS.some((t) => t.id === route.query.tab) ? route.query.tab : "analysis";

  const [bundle, setBundle] = useState(null);
  const [meta, setMeta] = useState({ code, name: code, market: "" });
  const [status, setStatus] = useState("載入中…");
  const [longBars, setLongBars] = useState([]);
  const [longLoading, setLongLoading] = useState(true);
  const [dividends, setDividends] = useState([]);
  const [epsQ, setEpsQ] = useState([]);
  const [txModal, setTxModal] = useState(null);

  useEffect(() => { try { localStorage.setItem("lastStock", code); } catch { /* ignore */ } }, [code]);

  useEffect(() => {
    let alive = true;
    setBundle(null); setStatus("載入中…"); setLongBars([]); setLongLoading(true); setDividends([]); setEpsQ([]);
    findStock(code).then((s) => alive && setMeta(s || { code, name: code, market: "" }));
    fetchStockBundle(code).then((b) => {
      if (!alive) return;
      setBundle(b);
      setStatus(b ? `資料日 ${String(b.kline?.data?.at(-1)?.date || "").replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3")}` : "非追蹤清單股票：改用 FinMind 歷史資料");
    });
    fetchLongHistory(code, "2015-01-01").then((bars) => alive && setLongBars(bars)).catch(() => {}).finally(() => alive && setLongLoading(false));
    fetchDividends(code).then((d) => alive && setDividends(d));
    fetchQuarterlyEps(code).then((d) => alive && setEpsQ(d));
    return () => { alive = false; };
  }, [code]);

  const quoteState = useQuotes([code]);
  const quote = quoteState.quotes[code];

  const dailyRows = useMemo(() => normalizeRows(bundle?.kline), [bundle]);
  // 分析卡沿用靜態資料的指標；沒有靜態檔時用長期歷史自行計算
  const longRows = useMemo(() => {
    const map = new Map(longBars.map((b) => [b.time, b]));
    for (const r of dailyRows) map.set(r.time, { time: r.time, open: r.open, high: r.high, low: r.low, close: r.close, volume: r.volume });
    return computeIndicators([...map.values()].sort((a, b) => a.time.localeCompare(b.time)));
  }, [longBars, dailyRows]);
  const cardRows = dailyRows.length ? dailyRows : longRows.slice(-250);
  const chip = bundle?.chip || null;

  const position = app.positionOf(code);
  const style = app.styleOf(code);
  const livePrice = Number.isFinite(quote?.price) ? quote.price : longRows.at(-1)?.close;
  const strategy = useMemo(() => buildStrategy({
    rows: longRows.length >= 30 ? longRows : computeIndicators(dailyRows), chipData: chip,
    fundamentals: bundle?.fundamentals, financials: bundle?.financials, dividends, longBars: longRows, epsQuarters: epsQ,
    position: position ? { shares: position.shares, avgCost: position.avgCost } : null, style, livePrice,
  }), [longRows, dailyRows, chip, bundle, dividends, epsQ, position, style, livePrice]);

  const last = longRows.at(-1) || dailyRows.at(-1) || {};
  const prev = longRows.at(-2) || dailyRows.at(-2) || {};
  const price = Number.isFinite(quote?.price) ? quote.price : last.close;
  const prevClose = Number.isFinite(quote?.prev) ? quote.prev : (quote ? prev.close : prev.close);
  const change = Number.isFinite(quote?.change) ? quote.change : last.close - prev.close;
  const changePct = Number.isFinite(quote?.changePct) ? quote.changePct : prev.close ? (change / prev.close) * 100 : NaN;
  const name = meta?.name || bundle?.name || quote?.name || code;
  const stale = isStale(quoteState.updatedAt, quoteState.live);

  const levels = useMemo(() => (tab === "entry" || tab === "position" || tab === "analysis" ? strategy?.levels || [] : position ? [{ price: position.avgCost, label: "成本", color: "#e2e8f0", style: 0 }] : []), [tab, strategy, position]);

  return (
    <div className="page">
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginBottom: 12 }}>
        <SearchBox onSelect={(s) => navigate(`/stock/${s.code}`, { tab })} />
        {app.user && (
          <button type="button" className="btn" onClick={() => setTxModal({ stock: { code, name, price } })}><Plus size={16} />記一筆</button>
        )}
      </div>

      <header className="stock-head">
        <div>
          <div className="stock-id">
            <span className="code num">{code}</span>
            <span className="name">{name}</span>
            {meta?.market && <Badge>{meta.market}</Badge>}
            {position && <Badge tone="blue">持有 {fmt(position.shares / 1000, 3)} 張</Badge>}
            {app.user && <Badge tone={style === "波段" ? "warn" : "blue"}>{style}</Badge>}
          </div>
          <div className="dim" style={{ marginTop: 4, display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
            {quoteState.live ? (
              <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                <i className="live-dot" aria-hidden="true" />盤中即時（每 5 秒）{quoteState.updatedAt && `・${quoteState.updatedAt.toLocaleTimeString("zh-TW")}`}
                {stale && <Badge tone="warn">延遲</Badge>}
              </span>
            ) : <span>{quote?.time ? `最新成交 ${quote.date?.replace(/(\d{4})(\d{2})(\d{2})/, "$1-$2-$3")} ${quote.time}` : status}</span>}
            {quoteState.error && <Badge tone="warn">即時報價暫停：{quoteState.error}</Badge>}
          </div>
        </div>
        <div className="quote-big">
          <div className="price num" style={{ color: trendColor(change) }}>{fmt(price)}</div>
          <div className="chg num" style={{ color: trendColor(change) }}>{signed(change)}（{signed(changePct, 2, "%")}）</div>
          <div className="ohlc num">
            <span><span>開</span>{fmt(quote?.open ?? last.open)}</span>
            <span><span>高</span><b className="up">{fmt(quote?.high ?? last.high)}</b></span>
            <span><span>低</span><b className="down">{fmt(quote?.low ?? last.low)}</b></span>
            <span><span>量</span>{fmtInt(quote?.volumeLots ?? (last.volume || 0) / 1000)} 張</span>
          </div>
        </div>
      </header>

      <div style={{ marginTop: 12 }}>
        <StockChart code={code} market={meta?.market} dailyRows={dailyRows} longBars={longBars} longLoading={longLoading} quote={quote} levels={levels} prevClose={prevClose} />
      </div>

      <Tabs tabs={TABS} value={tab} onChange={(t) => route.setQuery({ tab: t })} />

      {tab === "analysis" && (
        <AnalysisBoard rows={cardRows} chip={chip} bundle={bundle} strategy={strategy} price={price}
          dividends={dividends} position={position} onEntry={() => route.setQuery({ tab: "entry" })} />
      )}

      {tab === "position" && <PositionTab code={code} name={name} price={price} longBars={longRows} quote={quote} onEdit={(t) => setTxModal({ initial: t })} onAdd={() => setTxModal({ stock: { code, name, price } })} />}

      {tab === "entry" && <StrategyPanel strategy={strategy} longRows={longRows} name={name} code={code} loadingExtra={longLoading} />}

      {txModal && <TxForm initial={txModal.initial} stock={txModal.stock} onClose={() => setTxModal(null)} />}
    </div>
  );
}

const toneCls = (t) => (t === "up" ? "up" : t === "down" ? "down" : "");

const jump = (id) => () => document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });

function GlanceTile({ title, icon: Icon, color, score, verdict, verdictTone, items, onClick }) {
  const body = (
    <>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <Icon size={16} aria-hidden="true" style={{ color }} />
        <b style={{ fontSize: 14 }}>{title}</b>
        {score != null && <span className="num" style={{ marginLeft: "auto", fontSize: 26, fontWeight: 900, color }}>{score}</span>}
      </div>
      <div className={toneCls(verdictTone)} style={{ fontSize: 18, fontWeight: 900, margin: "2px 0 6px", color: verdictTone ? undefined : color }}>{verdict}</div>
      <div className="glance-items">
        {items.filter(Boolean).map(([k, v, c]) => (
          <div key={k}><span>{k}</span><b className="num" style={c ? { color: c } : undefined}>{v}</b></div>
        ))}
      </div>
    </>
  );
  return <button type="button" className="card glance-tile" style={{ borderTopColor: color }} onClick={onClick}>{body}</button>;
}

// 綜合分析：上方一排摘要 + 技術｜籌碼｜基本 三欄並排（手機改成上下三段 + 跳段按鈕）
function AnalysisBoard({ rows, chip, bundle, strategy, price, dividends, position, onEntry }) {
  const g = useMemo(() => {
    const score = getOverallScore(rows, chip);
    const ma = getMaStatus(rows), vol = getVolPriceMatrix(rows), rsi = getRsiAnalysis(rows), kd = getKdAnalysis(rows);
    const pat = detectPatterns(rows), ch = getChipAnalysis(chip), risk = getRiskMetrics(rows, chip), sc = getScenarios(rows, chip);
    return { score, ma, vol, rsi, kd, pat, ch, risk, sc };
  }, [rows, chip]);
  const f = bundle?.fundamentals || {};
  const v = strategy?.value;
  const techColor = g.score >= 60 ? UP : g.score >= 45 ? "#f59e0b" : DOWN;
  const chipColor = g.ch.score > 60 ? UP : g.ch.score < 40 ? DOWN : "#f59e0b";
  const valLabel = !v ? "估值資料不足" : price <= v.cheap ? "便宜" : price <= v.fair ? "合理偏低" : price < v.expensive ? "合理偏高" : "昂貴";
  const valColor = !v ? "#94a3b8" : price <= v.cheap ? "#38bdf8" : price <= v.fair ? "#22d3ee" : price < v.expensive ? "#facc15" : "#f97316";
  const trend = g.score >= 75 ? "強勢多頭" : g.score >= 60 ? "偏多" : g.score >= 45 ? "盤整" : g.score >= 30 ? "偏空" : "強勢空頭";
  const streak = (n, word) => (n > 0 ? `連買 ${n} 天` : n < 0 ? `連賣 ${-n} 天` : word);

  return (
    <div>
      <div className="glance">
        <GlanceTile title="技術面" icon={CandlestickChart} color={techColor} score={g.score} verdict={trend} onClick={jump("sec-tech")} items={[
          ["均線", g.ma.label, g.ma.color],
          ["量價", g.vol.type, g.vol.color],
          ["RSI／KD", `${g.rsi.rsi ? g.rsi.rsi.toFixed(0) : "--"}／${g.kd.status}`, g.kd.color],
          ["型態", g.pat.patterns[0]?.label || "--"],
        ]} />
        <GlanceTile title="籌碼面" icon={Building2} color={chipColor} score={chip ? g.ch.score : null} verdict={chip ? g.ch.status : "無籌碼資料"} onClick={jump("sec-chip")} items={chip && [
          ["外資", `${streak(g.ch.foreignStreak, "中性")}・5日 ${signed(g.ch.foreign5d / 1000, 0)}`, trendColor(g.ch.foreign5d)],
          ["投信", `${streak(g.ch.trustStreak, "中性")}・5日 ${signed(g.ch.trust5d / 1000, 0)}`, trendColor(g.ch.trust5d)],
          ["風險", g.risk.isLongRisk ? "融資斷頭警戒" : g.risk.isShortSqueeze ? "軋空預兆" : "無異常", g.risk.isLongRisk ? UP : g.risk.isShortSqueeze ? "#f59e0b" : undefined],
        ].concat([["多空機率", `多 ${g.sc.bull}%／空 ${g.sc.bear}%`]]) || [["多空機率", `多 ${g.sc.bull}%／空 ${g.sc.bear}%`]]} />
        <GlanceTile title="基本面" icon={Gauge} color={valColor} verdict={valLabel} onClick={jump("sec-fund")} items={[
          v && ["便宜／合理／昂貴", `${fmt(v.cheap, 0)}／${fmt(v.fair, 0)}／${fmt(v.expensive, 0)}`],
          ["本益比／殖利率", `${f.pe_ratio != null ? fmt(f.pe_ratio, 1) : "--"}／${f.dividend_yield != null ? `${fmt(f.dividend_yield, 2)}%` : "--"}`],
          ["EPS（近四季）／ROE", `${f.eps != null ? fmt(f.eps, 2) : "--"}／${f.roe != null ? `${fmt(f.roe, 1)}%` : "--"}`],
          ["月營收 YoY", f.revenue_yoy != null ? signed(f.revenue_yoy, 1, "%") : "--", trendColor(f.revenue_yoy)],
        ]} />
        {strategy && (
          <GlanceTile title="綜合建議" icon={Compass} color="var(--accent-2)" verdict={strategy.primary.label} verdictTone={strategy.primary.tone} onClick={onEntry} items={[
            ["依據", strategy.primary.why],
            ["波段條件", `${strategy.swing.score}/6`],
            ["停損／目標", `${fmt(strategy.swing.stop)}／${fmt(strategy.swing.target1)}`],
            position && ["我的未實現", signed(position.shares * price - position.cost, 0), trendColor(position.shares * price - position.cost)],
          ]} />
        )}
      </div>

      <nav className="board-jump" aria-label="跳到分析區段">
        <button type="button" className="btn sm" onClick={jump("sec-tech")}>技術面</button>
        <button type="button" className="btn sm" onClick={jump("sec-chip")}>籌碼面</button>
        <button type="button" className="btn sm" onClick={jump("sec-fund")}>基本面</button>
      </nav>

      <div className="board">
        <section id="sec-tech" aria-labelledby="h-tech">
          <h2 id="h-tech" className="board-h" style={{ borderColor: techColor }}><CandlestickChart size={16} aria-hidden="true" />技術面</h2>
          <TechRadarCard rows={rows} chipData={chip} />
          <MaStatusCard rows={rows} />
          <MomentumCard rows={rows} />
          <VolPriceCard rows={rows} />
          <PatternCard rows={rows} />
          <BlackCandleCard rows={rows} chipData={chip} />
        </section>
        <section id="sec-chip" aria-labelledby="h-chip">
          <h2 id="h-chip" className="board-h" style={{ borderColor: chipColor }}><Building2 size={16} aria-hidden="true" />籌碼面</h2>
          {chip ? (
            <>
              <InstitutionalFlowCard chipData={chip} />
              <ChipXrayCard chipData={chip} />
            </>
          ) : <Card><div className="dim">此股票沒有籌碼資料（不在每日追蹤清單）</div></Card>}
          <RiskMirrorCard rows={rows} chipData={chip} />
          <ScenarioCard rows={rows} chipData={chip} />
        </section>
        <section id="sec-fund" aria-labelledby="h-fund">
          <h2 id="h-fund" className="board-h" style={{ borderColor: valColor }}><Gauge size={16} aria-hidden="true" />基本面</h2>
          <ValuationCard strategy={strategy} price={price} />
          <FundamentalsCard data={bundle?.fundamentals} />
          <DividendCard dividends={dividends} />
          <FinancialsCard data={bundle?.financials} />
        </section>
      </div>
    </div>
  );
}

function ValuationCard({ strategy, price }) {
  const v = strategy?.value;
  if (!v) return <Card title="估值位置" icon={PiggyBank}><div className="dim">股利／EPS 資料不足</div></Card>;
  const lo = Math.min(v.cheap, price) * 0.95, hi = Math.max(v.expensive, price) * 1.05;
  const pos = (x) => `${((x - lo) / (hi - lo)) * 100}%`;
  return (
    <Card title="估值位置" icon={PiggyBank}>
      <div style={{ position: "relative", height: 70, margin: "6px 4px 6px" }}>
        <div className="num" style={{ position: "absolute", left: pos(price), top: 0, transform: "translateX(-50%)", fontSize: 12, fontWeight: 800, whiteSpace: "nowrap" }}>現價 {fmt(price)}</div>
        <div style={{ position: "absolute", left: pos(price), top: 18, width: 2, height: 26, background: "#fff", transform: "translateX(-1px)", zIndex: 1 }} />
        <div style={{ position: "absolute", top: 26, left: 0, right: 0, height: 10, borderRadius: 5, background: `linear-gradient(90deg, #38bdf8 ${pos(v.cheap)}, #facc15 ${pos(v.fair)}, #f97316 ${pos(v.expensive)})` }} />
        {[["便宜", v.cheap], ["合理", v.fair], ["昂貴", v.expensive]].map(([l, x]) => (
          <div key={l} className="dim num" style={{ position: "absolute", left: pos(x), top: 48, transform: "translateX(-50%)", whiteSpace: "nowrap", textAlign: "center", lineHeight: 1.2 }}>{l}<br />{fmt(x, 1)}</div>
        ))}
      </div>
      <div style={{ marginTop: 22 }} className="dim">
        {v.methods.map((m) => <div key={m.name} style={m.excluded ? { opacity: 0.7 } : undefined}>{m.name}：{m.basis}</div>)}
      </div>
    </Card>
  );
}

function DividendCard({ dividends }) {
  const annual = annualCashDividends(dividends).slice(-8);
  return (
    <Card title="歷年現金股利（依除息年）" icon={PiggyBank}>
      {!annual.length ? <div className="dim">查無股利紀錄</div> : (
        <div style={{ height: 180 }}>
          <ResponsiveContainer>
            <BarChart data={annual} margin={{ top: 4, right: 4, left: -16, bottom: 0 }}>
              <XAxis dataKey="year" tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#0e1223", border: "1px solid #334155", borderRadius: 8 }} formatter={(v) => [`${Number(v).toFixed(2)} 元`, "現金股利"]} />
              <Bar dataKey="cash" fill="#f59e0b" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
      {dividends.slice(-1).map((d) => d.exDate && (
        <div key={d.exDate} className="dim">最近一次：{d.year} 除息 {d.exDate}，現金 {fmt(d.cash, 2)} 元{d.payDate ? `，發放 ${d.payDate}` : ""}</div>
      ))}
    </Card>
  );
}

function PositionTab({ code, name, price, longBars, quote, onEdit, onAdd }) {
  const app = useApp();
  const allTxs = useMemo(() => app.transactions.filter((t) => t.stock_id === code), [app.transactions, code]);
  const accounts = useMemo(() => listAccounts(allTxs), [allTxs]);
  const [view, setView] = useAccountView(accounts, "account.view.stock");
  const txs = useMemo(() => (view === ALL || view === SPLIT ? allTxs : allTxs.filter((t) => accountOf(t) === view)), [allTxs, view]);
  const pos = useMemo(() => computePositions(txs)[0], [txs]);
  const byAcc = useMemo(() => valuePositions(computePositions(allTxs, { byAccount: true }), { [code]: price }).rows, [allTxs, code, price]);
  const curve = useMemo(() => equityCurve(txs, { [code]: longBars.map((b) => ({ time: b.time, close: b.close })) }, {
    onlyCode: code, livePrices: Number.isFinite(quote?.price) ? { [code]: quote.price } : null, today: todayISO(),
  }), [txs, code, longBars, quote]);

  if (!app.user) return <Card><Empty icon={Wallet} title="登入後查看你的部位"><button type="button" className="btn primary" onClick={app.signIn}>Google 登入</button></Empty></Card>;
  if (!allTxs.length) {
    return (
      <Card>
        <Empty icon={Wallet} title={`尚未記錄 ${name} 的交易`}>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "center" }}>
            <button type="button" className="btn primary" onClick={onAdd}><Plus size={16} />記一筆</button>
            <button type="button" className="btn" onClick={() => navigate("/transactions", { import: 1 })}>匯入對帳單 CSV</button>
          </div>
        </Empty>
      </Card>
    );
  }
  const { rows } = valuePositions([pos], { [code]: price });
  const r = rows[0];
  const holdDays = r.firstDate ? Math.round((Date.now() - Date.parse(r.firstDate)) / 86400000) : null;

  async function remove(t) {
    if (!window.confirm(`刪除 ${t.trade_date} ${SIDE_LABEL[t.side]} ${t.shares} 股？`)) return;
    const { error } = await supabase.from("transactions").delete().eq("id", t.id);
    if (error) { window.alert(`刪除失敗：${error.message}`); return; }
    app.reload();
  }

  return (
    <div className="grid" style={{ gap: 12 }}>
      {accounts.length > 1 && (
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <AccountPicker accounts={accounts} value={view} onChange={setView} allowSplit={false} />
          <span className="dim">{view === ALL ? `合併 ${accounts.length} 個帳戶：均價 = 各帳戶成本加總 ÷ 總股數` : `只看 ${view}`}</span>
        </div>
      )}
      <div className="kpis">
        <Kpi label="持股" value={`${fmtInt(r.shares)} 股`} sub={`${fmt(r.shares / 1000, 3)} 張`} />
        <Kpi label="均價（含手續費）" value={fmt(r.avgCost)} sub={`成本 ${fmtInt(r.cost)}`} />
        <Kpi label="市值" value={fmtInt(r.marketValue)} sub={`現價 ${fmt(price)}`} />
        <Kpi label="未實現損益" value={signed(r.unrealized, 0)} sub={signed(r.unrealizedPct, 2, "%")} tone={r.unrealized >= 0 ? "up" : "down"} />
        <Kpi label="已實現＋股利" value={signed(r.realized + r.dividends, 0)} sub={`股利 ${fmtInt(r.dividends)}`} tone={r.realized + r.dividends >= 0 ? "up" : "down"} />
        <Kpi label="總損益" value={signed(r.totalPnl, 0)} sub={holdDays != null ? `持有 ${holdDays} 天` : ""} tone={r.totalPnl >= 0 ? "up" : "down"} />
      </div>
      {accounts.length > 1 && (
        <Card title="各帳戶部位" icon={Wallet}>
          <div className="table-wrap">
            <table className="data">
              <thead><tr><th className="left">帳戶</th><th>股數</th><th>均價</th><th>成本</th><th>市值</th><th>未實現</th><th>報酬率</th><th>已實現＋股利</th></tr></thead>
              <tbody>
                {byAcc.map((a) => (
                  <tr key={a.account} className="clickable" onClick={() => setView(a.account)} style={view === a.account ? { outline: "1px solid var(--accent)" } : undefined}>
                    <td className="left"><b>{a.account}</b></td>
                    <td>{fmtInt(a.shares)}</td><td>{a.shares ? fmt(a.avgCost) : "--"}</td><td>{fmtInt(a.cost)}</td>
                    <td>{fmtInt(a.marketValue)}</td>
                    <td style={{ color: trendColor(a.unrealized) }}>{signed(a.unrealized, 0)}</td>
                    <td style={{ color: trendColor(a.unrealizedPct) }}>{signed(a.unrealizedPct, 2, "%")}</td>
                    <td style={{ color: trendColor(a.realized + a.dividends) }}>{signed(a.realized + a.dividends, 0)}</td>
                  </tr>
                ))}
                <tr>
                  <td className="left"><b>合併</b></td>
                  <td><b>{fmtInt(byAcc.reduce((s, a) => s + a.shares, 0))}</b></td>
                  <td><b>{fmt(byAcc.reduce((s, a) => s + a.cost, 0) / Math.max(1, byAcc.reduce((s, a) => s + a.shares, 0)))}</b></td>
                  <td><b>{fmtInt(byAcc.reduce((s, a) => s + a.cost, 0))}</b></td>
                  <td><b>{fmtInt(byAcc.reduce((s, a) => s + (a.marketValue || 0), 0))}</b></td>
                  <td colSpan={3} />
                </tr>
              </tbody>
            </table>
          </div>
          <div className="dim" style={{ marginTop: 6 }}>點帳戶列可只看該帳戶的損益與曲線。</div>
        </Card>
      )}
      <Card title={`${name} 資產變化`} icon={Wallet}>
        <EquityChart curve={curve} height={240} />
      </Card>
      <Card title="交易明細" icon={Pencil} right={<button type="button" className="btn sm" onClick={onAdd}><Plus size={14} />記一筆</button>}>
        <div className="table-wrap">
          <table className="data">
            <thead><tr><th className="left">日期</th><th className="left">類別</th><th>股數</th><th>價格</th><th>手續費</th><th>稅</th><th className="left">帳戶</th><th aria-label="操作" /></tr></thead>
            <tbody>
              {[...txs].reverse().map((t) => (
                <tr key={t.id}>
                  <td className="left num">{t.trade_date}</td>
                  <td className="left"><span className={t.side === "buy" ? "up" : t.side === "sell" ? "down" : ""}>{SIDE_LABEL[t.side]}</span></td>
                  <td>{fmtInt(t.shares)}</td><td>{fmt(t.price)}</td><td>{fmtInt(t.fee)}</td><td>{fmtInt(t.tax)}</td>
                  <td className="left dim">{accountOf(t)}</td>
                  <td>
                    <button type="button" className="btn ghost sm" onClick={() => onEdit(t)} aria-label="編輯"><Pencil size={14} /></button>
                    <button type="button" className="btn ghost sm" onClick={() => remove(t)} aria-label="刪除"><Trash2 size={14} /></button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
