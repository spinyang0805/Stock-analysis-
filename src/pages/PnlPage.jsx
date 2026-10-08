// 損益分析：依對帳單交易（含自動配股配息）算出歷年／各月賺賠，含息與不含息並列
import React, { useEffect, useMemo, useState } from "react";
import { BarChart3, ChevronDown, ChevronRight, Coins, FileUp, Layers } from "lucide-react";
import { Bar, BarChart, CartesianGrid, Legend, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import AccountPicker, { ALL, useAccountView } from "../components/AccountPicker.jsx";
import { Card, Empty, Kpi, Seg } from "../components/ui.jsx";
import { useApp } from "../lib/appState.jsx";
import { accountOf, isDividendTx, listAccounts } from "../lib/portfolio.js";
import { autoDividendTx, periodBreakdown, stockDividendValue } from "../lib/pnl.js";
import { fetchDividends, fetchLongHistory } from "../lib/market.js";
import { fetchStockBundle, normalizeRows } from "../lib/data.js";
import { useQuotes } from "../lib/useQuotes.js";
import { navigate } from "../lib/router.js";
import { fmt, fmtInt, fmtMoney, signed, todayISO, trendColor } from "../lib/format.js";

const NO_DIV = "#94a3b8", WITH_DIV = "#f59e0b";

function Pct({ v }) {
  return <span style={{ color: trendColor(v) }}>{signed(v, 2, "%")}</span>;
}

export default function PnlPage() {
  const app = useApp();
  const accounts = useMemo(() => listAccounts(app.transactions), [app.transactions]);
  const [view, setView] = useAccountView(accounts, "account.view.pnl");
  const [history, setHistory] = useState({});
  const [divs, setDivs] = useState({});
  const [loading, setLoading] = useState(false);
  const [openYear, setOpenYear] = useState(null);
  const [period, setPeriod] = useState(null); // { key, label }
  const [autoDiv, setAutoDiv] = useState(true);

  const baseTx = useMemo(() => (view === ALL ? app.transactions : app.transactions.filter((t) => accountOf(t) === view)), [app.transactions, view]);
  const codes = useMemo(() => [...new Set(app.transactions.map((t) => t.stock_id))], [app.transactions]);
  const held = useMemo(() => app.positions.filter((p) => p.shares > 0).map((p) => p.code), [app.positions]);
  const q = useQuotes(held);

  useEffect(() => {
    if (!codes.length) return undefined;
    let alive = true;
    const start = app.transactions.reduce((m, t) => (t.trade_date < m ? t.trade_date : m), "9999");
    setLoading(true);
    Promise.all(codes.map(async (c) => {
      let bars;
      try {
        bars = (await fetchLongHistory(c, start)).map((b) => ({ time: b.time, close: b.close }));
      } catch {
        bars = normalizeRows((await fetchStockBundle(c))?.kline).map((r) => ({ time: r.time, close: r.close }));
      }
      return [c, bars, await fetchDividends(c)];
    })).then((rows) => {
      if (!alive) return;
      setHistory(Object.fromEntries(rows.map(([c, b]) => [c, b])));
      setDivs(Object.fromEntries(rows.map(([c, , d]) => [c, d])));
      setLoading(false);
    });
    return () => { alive = false; };
  }, [codes.join(","), app.transactions]);

  const autoTx = useMemo(() => (autoDiv ? autoDividendTx(baseTx, divs) : []), [baseTx, divs, autoDiv]);
  const allTx = useMemo(() => [...baseTx, ...autoTx], [baseTx, autoTx]);
  const live = useMemo(() => Object.fromEntries(Object.entries(q.quotes).map(([c, v]) => [c, v.price]).filter(([, v]) => Number.isFinite(v))), [q.quotes]);
  const result = useMemo(() => (Object.keys(history).length ? periodBreakdown(allTx, history, { livePrices: live, today: todayISO() }) : null), [allTx, history, live]);

  const dividendRows = useMemo(() => allTx.filter(isDividendTx)
    .sort((a, b) => b.trade_date.localeCompare(a.trade_date)), [allTx]);

  if (!app.user) {
    return <div className="page"><Card><Empty icon={BarChart3} title="登入後查看你的歷年損益"><button type="button" className="btn primary" onClick={app.signIn}>Google 登入</button></Empty></Card></div>;
  }
  if (!app.transactions.length) {
    return (
      <div className="page"><Card><Empty icon={BarChart3} title="還沒有交易紀錄">
        <button type="button" className="btn primary" onClick={() => navigate("/transactions", { import: 1 })}><FileUp size={16} />匯入對帳單</button>
      </Empty></Card></div>
    );
  }

  const years = result ? [...result.years].reverse() : [];
  const monthsOf = (y) => (result ? result.months.filter((m) => m.key.startsWith(y)) : []);
  const sel = period ? (period.key.length === 4 ? result?.years.find((y) => y.key === period.key) : result?.months.find((m) => m.key === period.key)) : result?.total;
  const selLabel = period ? period.label : "全部期間";
  const t = result?.total;
  const cashDivTotal = dividendRows.filter((d) => d.side === "cash_dividend").reduce((s, d) => s + (Number(d.amount) || d.shares * d.price), 0);
  const stockDivShares = dividendRows.filter((d) => d.side === "stock_dividend").reduce((s, d) => s + Number(d.shares), 0);
  const stockDivValue = dividendRows.filter((d) => d.side === "stock_dividend").reduce((s, d) => s + stockDividendValue(d, history).value, 0);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">PROFIT & LOSS</div>
          <h1 className="page-title">損益分析</h1>
          <div className="dim">{t ? `${t.from} ～ ${t.to}` : ""}{loading ? "・行情與股利載入中…" : ""}</div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <AccountPicker accounts={accounts} value={view} onChange={setView} allowSplit={false} />
          <label className="dim" style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <input type="checkbox" checked={autoDiv} onChange={(e) => setAutoDiv(e.target.checked)} />自動計算配股配息
          </label>
        </div>
      </div>

      {!result ? <Card><div className="dim">計算中…</div></Card> : (
        <>
          <div className="kpis">
            <Kpi label="累計損益（含息）" value={signed(t.pnlWithDiv, 0)} sub={<>報酬率 <Pct v={t.retWithDiv} /></>} tone={t.pnlWithDiv >= 0 ? "up" : "down"} />
            <Kpi label="累計損益（不含息）" value={signed(t.pnlNoDiv, 0)} sub={<>報酬率 <Pct v={t.retNoDiv} /></>} tone={t.pnlNoDiv >= 0 ? "up" : "down"} />
            <Kpi label="股利貢獻" value={signed(t.pnlWithDiv - t.pnlNoDiv, 0)} sub={stockDivShares
              ? `現金 ${fmtInt(cashDivTotal)}・配股 ${fmtInt(stockDivShares)} 股（除權日價值 ${fmtInt(stockDivValue)}，其餘為配股之後的漲跌）`
              : `現金股利 ${fmtInt(cashDivTotal)}（含息 − 不含息）`} />
            <Kpi label="已實現損益" value={signed(t.realized, 0)} tone={t.realized >= 0 ? "up" : "down"} sub="賣出價差（扣費稅）" />
            <Kpi label="未實現損益" value={signed(t.unrealized, 0)} tone={t.unrealized >= 0 ? "up" : "down"} sub={`平均投入 ${fmtMoney(t.avgCost)}`} />
          </div>

          <Card title="各年度損益" icon={Layers} style={{ marginTop: 12 }}>
            <div className="table-wrap">
              <table className="data">
                <thead>
                  <tr>
                    <th className="left">期間</th><th>已實現</th><th>未實現變動</th><th>現金股利</th><th>配股價值</th>
                    <th>損益（不含息）</th><th>損益（含息）</th><th>報酬率（不含息）</th><th>報酬率（含息）</th>
                  </tr>
                </thead>
                <tbody>
                  {years.map((y) => (
                    <React.Fragment key={y.key}>
                      <tr className="clickable" onClick={() => { setOpenYear(openYear === y.key ? null : y.key); setPeriod({ key: y.key, label: `${y.key} 年` }); }}
                        style={period?.key === y.key ? { outline: "1px solid var(--accent)" } : undefined}>
                        <td className="left"><b>{openYear === y.key ? <ChevronDown size={14} /> : <ChevronRight size={14} />} {y.key}</b></td>
                        <td style={{ color: trendColor(y.realized) }}>{signed(y.realized, 0)}</td>
                        <td style={{ color: trendColor(y.unrealized) }}>{signed(y.unrealized, 0)}</td>
                        <td style={{ color: WITH_DIV }}>{fmtInt(y.cashDiv)}</td>
                        <td style={{ color: WITH_DIV }}>{y.stockDiv ? fmtInt(y.stockDiv) : "--"}</td>
                        <td style={{ color: trendColor(y.pnlNoDiv), fontWeight: 700 }}>{signed(y.pnlNoDiv, 0)}</td>
                        <td style={{ color: trendColor(y.pnlWithDiv), fontWeight: 700 }}>{signed(y.pnlWithDiv, 0)}</td>
                        <td><Pct v={y.retNoDiv} /></td><td><Pct v={y.retWithDiv} /></td>
                      </tr>
                      {openYear === y.key && monthsOf(y.key).map((m) => (
                        <tr key={m.key} className="clickable" onClick={() => setPeriod({ key: m.key, label: `${m.key.replace("-", " 年 ")} 月` })}
                          style={{ background: "rgba(59,130,246,.04)", ...(period?.key === m.key ? { outline: "1px solid var(--accent)" } : {}) }}>
                          <td className="left" style={{ paddingLeft: 34 }}>{Number(m.key.slice(5))} 月</td>
                          <td style={{ color: trendColor(m.realized) }}>{signed(m.realized, 0)}</td>
                          <td style={{ color: trendColor(m.unrealized) }}>{signed(m.unrealized, 0)}</td>
                          <td style={{ color: WITH_DIV }}>{m.cashDiv ? fmtInt(m.cashDiv) : "--"}</td>
                          <td style={{ color: WITH_DIV }}>{m.stockDiv ? fmtInt(m.stockDiv) : "--"}</td>
                          <td style={{ color: trendColor(m.pnlNoDiv) }}>{signed(m.pnlNoDiv, 0)}</td>
                          <td style={{ color: trendColor(m.pnlWithDiv) }}>{signed(m.pnlWithDiv, 0)}</td>
                          <td><Pct v={m.retNoDiv} /></td><td><Pct v={m.retWithDiv} /></td>
                        </tr>
                      ))}
                    </React.Fragment>
                  ))}
                  <tr className="clickable" onClick={() => { setPeriod(null); setOpenYear(null); }} style={!period ? { outline: "1px solid var(--accent)" } : undefined}>
                    <td className="left"><b>全部</b></td>
                    <td><b style={{ color: trendColor(t.realized) }}>{signed(t.realized, 0)}</b></td>
                    <td><b style={{ color: trendColor(t.unrealized) }}>{signed(t.unrealized, 0)}</b></td>
                    <td><b style={{ color: WITH_DIV }}>{fmtInt(t.cashDiv)}</b></td>
                    <td><b style={{ color: WITH_DIV }}>{t.stockDiv ? fmtInt(t.stockDiv) : "--"}</b></td>
                    <td><b style={{ color: trendColor(t.pnlNoDiv) }}>{signed(t.pnlNoDiv, 0)}</b></td>
                    <td><b style={{ color: trendColor(t.pnlWithDiv) }}>{signed(t.pnlWithDiv, 0)}</b></td>
                    <td><Pct v={t.retNoDiv} /></td><td><Pct v={t.retWithDiv} /></td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div className="dim" style={{ marginTop: 6 }}>點年份展開各月；點任一列，下方「各股貢獻」就切到那個期間。未實現變動＝期間內持股市值相對成本的增減；報酬率＝期間損益 ÷ 期間平均投入成本。已實現＋未實現變動＋現金股利＋配股價值＝含息損益。</div>
          </Card>

          <div className="grid two" style={{ marginTop: 12 }}>
            <Card title={`每月損益（${openYear || "近 24 個月"}）`} icon={BarChart3}>
              <MonthlyChart months={openYear ? monthsOf(openYear) : result.months.slice(-24)} />
            </Card>
            <Card title={`各股貢獻：${selLabel}`} icon={Layers}>
              <StockContribution period={sel} />
            </Card>
          </div>

          <Card title={`配股配息明細（${dividendRows.length} 筆）`} icon={Coins} style={{ marginTop: 12 }}>
            {!dividendRows.length ? <div className="dim">期間內沒有配股配息{loading ? "（股利資料載入中）" : ""}</div> : (
              <div className="table-wrap" style={{ maxHeight: 420 }}>
                <table className="data">
                  <thead><tr><th className="left">除權息日</th><th className="left">股票</th><th className="left">帳戶</th><th className="left">類別</th><th>持股</th><th>每股</th><th>金額／配股</th><th>配股價值</th><th className="left">發放日</th><th className="left">來源</th></tr></thead>
                  <tbody>
                    {dividendRows.map((d) => (
                      <tr key={d.id}>
                        <td className="left num">{d.trade_date}</td>
                        <td className="left"><b className="num">{d.stock_id}</b> {d.stock_name}</td>
                        <td className="left dim">{accountOf(d)}</td>
                        <td className="left">{d.side === "cash_dividend" ? "現金股利" : "股票股利"}</td>
                        <td>{d.side === "cash_dividend" ? fmtInt(d.shares) : "--"}</td>
                        <td>{d.side === "cash_dividend" ? fmt(d.price, 4) : d.perShare ? `${fmt(d.perShare, 4)} 元` : "--"}</td>
                        <td style={{ color: WITH_DIV, fontWeight: 700 }}>{d.side === "cash_dividend" ? fmtInt(Number(d.amount) || d.shares * d.price) : `+${fmtInt(d.shares)} 股`}</td>
                        <td>{d.side === "stock_dividend" ? (() => { const v = stockDividendValue(d, history); return Number.isFinite(v.price) ? `${fmtInt(v.value)}（@${fmt(v.price)}）` : "--"; })() : ""}</td>
                        <td className="left dim">{d.payDate || "--"}</td>
                        <td className="left dim">{d.source === "auto" ? "自動計算" : "對帳單"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <div className="dim" style={{ marginTop: 6 }}>
              自動計算：依除權息日前一天各帳戶持股 × 每股股利（配股數＝持股 × 股票股利 ÷ 10）。配股價值＝配股股數 × 除權當天收盤價，計入含息報酬；配到的股數成本為 0，持股均價因此攤低。依除權息日認列，未扣二代健保補充保費與匯費；對帳單已有的股利不重複計算。
            </div>
          </Card>
        </>
      )}
    </div>
  );
}

function MonthlyChart({ months }) {
  const data = months.map((m) => ({ label: m.key.slice(2).replace("-", "/"), 不含息: Math.round(m.pnlNoDiv), 含息: Math.round(m.pnlWithDiv) }));
  if (!data.length) return <div className="dim">尚無資料</div>;
  return (
    <div style={{ height: 260 }}>
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 6, right: 6, left: 0, bottom: 0 }}>
          <CartesianGrid stroke="#18213a" vertical={false} />
          <XAxis dataKey="label" tick={{ fill: "#94a3b8", fontSize: 11 }} />
          <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} width={60} tickFormatter={fmtMoney} />
          <Tooltip contentStyle={{ background: "#0e1223", border: "1px solid #334155", borderRadius: 8 }} formatter={(v) => signed(v, 0)} />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <ReferenceLine y={0} stroke="#475569" />
          <Bar dataKey="不含息" fill={NO_DIV} radius={[3, 3, 0, 0]} />
          <Bar dataKey="含息" fill={WITH_DIV} radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function StockContribution({ period }) {
  const [sort, setSort] = useState("with");
  if (!period) return <div className="dim">尚無資料</div>;
  const rows = Object.entries(period.byCode || {}).map(([code, r]) => ({ code, ...r }))
    .sort((a, b) => (sort === "with" ? b.pnlWithDiv - a.pnlWithDiv : sort === "div" ? (b.cashDiv + (b.stockDiv || 0)) - (a.cashDiv + (a.stockDiv || 0)) : b.realized - a.realized));
  if (!rows.length) return <div className="dim">此期間沒有損益變動</div>;
  return (
    <>
      <Seg label="排序" value={sort} onChange={setSort} options={[{ value: "with", label: "含息損益" }, { value: "realized", label: "已實現" }, { value: "div", label: "股利" }]} />
      <div className="table-wrap" style={{ maxHeight: 300, marginTop: 8 }}>
        <table className="data">
          <thead><tr><th className="left">股票</th><th>已實現</th><th>未實現變動</th><th>股利</th><th>不含息</th><th>含息</th></tr></thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.code} className="clickable" onClick={() => navigate(`/stock/${r.code}`, { tab: "position" })}>
                <td className="left"><b className="num" style={{ color: "var(--gold)" }}>{r.code}</b> {r.name}</td>
                <td style={{ color: trendColor(r.realized) }}>{signed(r.realized, 0)}</td>
                <td style={{ color: trendColor(r.unrealized) }}>{signed(r.unrealized, 0)}</td>
                <td style={{ color: WITH_DIV }}>{r.cashDiv || r.stockDiv ? fmtInt((r.cashDiv || 0) + (r.stockDiv || 0)) : "--"}</td>
                <td style={{ color: trendColor(r.pnlNoDiv) }}>{signed(r.pnlNoDiv, 0)}</td>
                <td style={{ color: trendColor(r.pnlWithDiv), fontWeight: 700 }}>{signed(r.pnlWithDiv, 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
