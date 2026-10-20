// 借貸試算「目前狀況」分頁：已還多少、還剩多少、整體貸款部位現在是賺還是賠
// 所有數字都由同一個 tracker 狀態 + 同一份行情，經 loanOverview 一次算出
import React, { useEffect, useMemo, useRef, useState } from "react";
import { useApp } from "../lib/appState.jsx";
import { supabase } from "../lib/supabase.js";
import { useQuotes } from "../lib/useQuotes.js";
import { loadLatestPrices } from "../lib/data.js";
import { accountOf, computePositions, listAccounts, valuePositions } from "../lib/portfolio.js";
import { parseXlsx } from "../lib/csvImport.js";
import { fxSourceLabel } from "../lib/usPortfolio.js";
import { DEFAULT_TRACKER, applyLoanImport, loanOverview, parseLoanXlsxRows, sanitizeTracker } from "../lib/loanTracker.js";
import { todayISO } from "../lib/format.js";

// 本機暫存依使用者分開（登出、換帳號不會看到上一個人的數字）
const keyFor = (user) => `loanTracker:${user?.id || "guest"}`;
const money = (v) => (Number.isFinite(v) ? Math.round(v).toLocaleString("zh-TW") : "--");
const signedMoney = (v) => (Number.isFinite(v) ? (v < 0 ? "−" : v > 0 ? "+" : "") + Math.round(Math.abs(v)).toLocaleString("zh-TW") : "--");

function loadLocal(key) {
  try { return sanitizeTracker(JSON.parse(localStorage.getItem(key) || "null")); } catch { return DEFAULT_TRACKER; }
}

export default function LoanStatus() {
  const app = useApp();
  const user = app?.user;
  const key = keyFor(user);
  const [t, setT] = useState(() => loadLocal(key));
  const [tKey, setTKey] = useState(key);
  if (tKey !== key) { setTKey(key); setT(loadLocal(key)); }   // 換人 → 換成那個人的本機資料
  const [msg, setMsg] = useState("");
  const [dbState, setDbState] = useState("idle"); // idle | loading | ok | error
  const [closes, setCloses] = useState({});
  const [acc, setAcc] = useState("cathay");
  const fileRef = useRef(null);

  useEffect(() => { if (tKey === key) { try { localStorage.setItem(key, JSON.stringify(t)); } catch { /* ignore */ } } }, [t, key, tKey]);
  useEffect(() => {                                 // 舊的共用鍵：交給第一個登入的人（本站只有一位使用者），然後刪掉
    if (!user) return;
    try {
      const old = localStorage.getItem("loanTracker");
      if (old && !localStorage.getItem(key)) { localStorage.setItem(key, old); setT(loadLocal(key)); }
      localStorage.removeItem("loanTracker");
    } catch { /* ignore */ }
  }, [user, key]);
  useEffect(() => { loadLatestPrices().then(setCloses).catch(() => {}); }, []);

  // 登入後載入雲端資料（有就覆蓋本機）
  useEffect(() => {
    if (!user || !app.configured) return undefined;
    let alive = true;
    setDbState("loading");
    supabase.from("loan_tracker").select("data").eq("user_id", user.id).maybeSingle().then(({ data, error }) => {
      if (!alive) return;
      if (error) { setDbState("error"); setMsg(`讀取雲端資料失敗：${error.message}`); return; }
      if (data?.data) setT(sanitizeTracker(data.data));
      setDbState("ok");
    });
    return () => { alive = false; };
  }, [user, app.configured]);

  const txsAll = app?.transactions || [];
  const accounts = useMemo(() => listAccounts(txsAll), [txsAll]);
  const cathay = accounts.filter((a) => a.includes("國泰"));
  const chosen = acc === "all" ? accounts : acc === "cathay" ? (cathay.length ? cathay : accounts) : [acc];
  const txs = useMemo(() => txsAll.filter((x) => chosen.includes(accountOf(x))), [txsAll, chosen.join("|")]); // eslint-disable-line react-hooks/exhaustive-deps
  const positions = useMemo(() => computePositions(txs), [txs]);
  const heldCodes = useMemo(() => positions.filter((p) => p.shares > 0).map((p) => p.code), [positions]);
  const q = useQuotes(heldCodes);
  const tw = useMemo(() => {
    const prices = {};
    for (const c of heldCodes) prices[c] = Number.isFinite(q.quotes[c]?.price) ? q.quotes[c].price : closes[c];
    return valuePositions(positions, prices);
  }, [positions, heldCodes, q.quotes, closes]);

  const us = app?.usValued;
  const fx = us?.fx || null;
  const o = loanOverview(t, todayISO(), { fx: fx?.close, twStockTwd: tw.totals.marketValue, usStockTwd: us?.totals.mvTwd || 0 });
  const stockPnl = tw.totals.unrealized + tw.totals.realized + (us ? us.totals.unrealizedTwd + us.totals.realizedTwd : 0);
  const divs = tw.totals.dividends + (us ? us.totals.dividendsTwd : 0);

  const edit = (patch) => setT((x) => sanitizeTracker({ ...x, ...patch }));
  const setAcct = (key, v) => setT((x) => sanitizeTracker({ ...x, accounts: x.accounts.map((a) => (a.key === key ? { ...a, amount: v } : a)) }));

  async function save() {
    setMsg("");
    if (!user || !app.configured) { setMsg("尚未登入，已存在這台瀏覽器；登入後再按一次「儲存」才會同步到雲端。"); return; }
    const { error } = await supabase.from("loan_tracker").upsert({ user_id: user.id, data: t, updated_at: new Date().toISOString() }, { onConflict: "user_id" });
    setMsg(error ? `儲存失敗：${error.message}` : "已儲存到雲端。");
  }

  async function importFile(file) {
    if (!file) return;
    try {
      const rows = await parseXlsx(await file.arrayBuffer());
      const parsed = parseLoanXlsxRows(rows);
      if (!parsed.loan && !parsed.accounts.length) { setMsg("讀不到借貸資料，請確認是「台新借貸內容.xlsx」。"); return; }
      setT((x) => applyLoanImport(x, parsed));
      setMsg("已讀入借貸內容（貸款條件、第一期還款日、帳戶餘額）；確認後按「儲存」。");
    } catch (e) { setMsg(`檔案解析失敗：${e.message}`); }
    if (fileRef.current) fileRef.current.value = "";
  }

  const hasLoan = t.loan > 0 && t.firstPayment;
  const tone = o.net > 0 ? "good" : o.net < 0 ? "bad" : "";
  const word = o.net > 0 ? "正數，目前是賺的" : o.net < 0 ? "負數，目前是賠的" : "剛好打平";

  return (
    <div>
      {!user && <div className="notice warn loan-gap">{app?.configured ? "尚未登入：資料只存在這台瀏覽器，台股／美股市值需登入後才會帶入。" : "網站尚未設定雲端金鑰：資料只存在這台瀏覽器，台股／美股市值無法帶入。"}</div>}
      {dbState === "error" && <div className="notice warn loan-gap">雲端資料暫時讀不到，先用本機資料。</div>}

      <section className={`loan-hero ${tone}`} aria-labelledby="loan-now-h">
        <h3 id="loan-now-h" className="loan-hero-q">到目前為止（整體貸款部位）</h3>
        <div className={`loan-net num ${tone}`}>淨值 {signedMoney(o.net)} 元</div>
        <p className="loan-hero-a">{hasLoan ? <>這個數字是{word}。</> : "先填入貸款條件（或匯入台新借貸內容.xlsx），才算得出淨值。"}</p>
        <p className="loan-fine">淨值 ＝ 總資產 {money(o.assets)} − 剩餘本金 {money(o.remaining)}{t.startEquity ? ` − 起始自有資金 ${money(t.startEquity)}` : ""}。
          組成：股票損益 {signedMoney(stockPnl)}、股利 {signedMoney(divs)}、已付利息 −{money(o.interestPaid)}（元，其餘為手續費、存款利息等差異）。</p>
      </section>

      <section className="loan-hero" aria-labelledby="loan-pay-h">
        <h3 id="loan-pay-h" className="loan-hero-q">還款進度</h3>
        <p className="loan-hero-a">已還 <b className="num">{o.paid}</b> / {o.months} 期{o.paid < o.due || t.paidOverride !== null ? "（手動調整）" : ""}；每月 <b className="num">{money(o.payment)}</b> 元。</p>
        <div className="loan-bar" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(o.progress * 100)}><i style={{ width: `${Math.min(100, o.progress * 100)}%` }} /></div>
        <div className="loan-minis four">
          <div className="loan-mini"><div className="loan-mini-label">已還總額</div><div className="loan-mini-value num">{money(o.totalPaid)}</div><div className="loan-mini-sentence">本金 {money(o.principalPaid)}・利息 {money(o.interestPaid)}</div></div>
          <div className="loan-mini"><div className="loan-mini-label">剩餘本金</div><div className="loan-mini-value num">{money(o.remaining)}</div><div className="loan-mini-sentence">還沒還的貸款</div></div>
          <div className="loan-mini"><div className="loan-mini-label">下次還款日</div><div className="loan-mini-value num">{o.nextDate || "--"}</div><div className="loan-mini-sentence">{o.nextDate ? `依第一期還款日每月推算` : o.paid >= o.months ? "已還清" : "尚未填第一期還款日"}</div></div>
          <div className="loan-mini"><div className="loan-mini-label">依日期應已還</div><div className="loan-mini-value num">{o.due} 期</div><div className="loan-mini-sentence">與實際不同時，在下方改</div></div>
        </div>
        <div className="loan-fields">
          <div className="loan-field"><label className="loan-field-label" htmlFor="ls-paid">實際已還期數<span className="loan-field-unit">（留空＝依日期自動算）</span></label>
            <input id="ls-paid" className="input num" type="number" min={0} max={t.months} value={t.paidOverride ?? ""} placeholder={String(o.due)} onChange={(e) => edit({ paidOverride: e.target.value === "" ? null : Number(e.target.value) })} /></div>
          <div className="loan-field"><label className="loan-field-label" htmlFor="ls-loan">貸款金額<span className="loan-field-unit">（元）</span></label>
            <input id="ls-loan" className="input num" type="number" min={0} value={t.loan} onChange={(e) => edit({ loan: e.target.value })} /></div>
          <div className="loan-field"><label className="loan-field-label" htmlFor="ls-rate">年利率<span className="loan-field-unit">（%）</span></label>
            <input id="ls-rate" className="input num" type="number" min={0} step={0.01} value={t.rate} onChange={(e) => edit({ rate: e.target.value })} /></div>
          <div className="loan-field"><label className="loan-field-label" htmlFor="ls-months">期數<span className="loan-field-unit">（月）</span></label>
            <input id="ls-months" className="input num" type="number" min={1} value={t.months} onChange={(e) => edit({ months: e.target.value })} /></div>
          <div className="loan-field"><label className="loan-field-label" htmlFor="ls-first">第一期還款日</label>
            <input id="ls-first" className="input" type="date" value={t.firstPayment} onChange={(e) => edit({ firstPayment: e.target.value })} /></div>
        </div>
      </section>

      <section className="loan-hero" aria-labelledby="loan-asset-h">
        <h3 id="loan-asset-h" className="loan-hero-q">現在有哪些資產</h3>
        <table className="loan-asset">
          <tbody>
            {t.accounts.map((a) => (
              <tr key={a.key}>
                <th scope="row"><label htmlFor={`ls-${a.key}`}>{a.label}</label>{a.currency === "USD" && <div className="loan-hint">美元，{fx ? `以 ${fx.close} 換算（${fx.date}，${fxSourceLabel(fx)}）= ${money(o.usdTwd)} 元` : "尚無匯率報價，暫不計入"}</div>}</th>
                <td><input id={`ls-${a.key}`} className="input num" type="number" min={0} step="any" value={a.amount} onChange={(e) => setAcct(a.key, e.target.value)} /> {a.currency === "USD" ? "美元" : "元"}</td>
              </tr>
            ))}
            <tr>
              <th scope="row">台股市值
                <div className="loan-hint">
                  <select className="input" aria-label="台股帳戶" value={acc} onChange={(e) => setAcc(e.target.value)} style={{ width: "auto", minHeight: 32 }}>
                    <option value="cathay">國泰帳戶{cathay.length ? "" : "（沒有名稱含國泰的，改用全部）"}</option>
                    <option value="all">全部帳戶</option>
                    {accounts.map((a) => <option key={a} value={a}>只看 {a}</option>)}
                  </select>
                </div>
              </th>
              <td className="num">{user ? money(o.twStock) : "--"} 元</td>
            </tr>
            <tr>
              <th scope="row">美股市值<div className="loan-hint">{us?.totals.unpriced ? `有 ${us.totals.unpriced} 檔尚無報價（不計入）` : us?.held.length ? "國泰複委託，台幣換算" : app?.usError ? "美股資料讀取失敗" : "尚無美股資料"}</div></th>
              <td className="num">{user ? money(o.usStock) : "--"} 元</td>
            </tr>
          </tbody>
          <tfoot><tr><th scope="row">總資產</th><td className="num">{money(o.assets)} 元</td></tr></tfoot>
        </table>
        <div className="loan-fields">
          <div className="loan-field"><label className="loan-field-label" htmlFor="ls-eq">起始自有資金<span className="loan-field-unit">（元，預設 0）</span></label>
            <input id="ls-eq" className="input num" type="number" step="any" value={t.startEquity} onChange={(e) => edit({ startEquity: e.target.value })} />
            <span className="loan-hint">如果全部資產都是貸款來的，淨值一開始就是 0，之後付出的利息會讓它慢慢往下。若有自己出的本金，填在這裡，淨值才不會把它算成賺的。</span></div>
        </div>
      </section>

      <div className="loan-actions">
        <button type="button" className="btn primary" onClick={save}>儲存</button>
        <button type="button" className="btn" onClick={() => fileRef.current?.click()}>匯入台新借貸內容.xlsx</button>
        <input ref={fileRef} type="file" accept=".xlsx" hidden onChange={(e) => importFile(e.target.files?.[0])} />
        {app?.reload && user && <button type="button" className="btn" onClick={() => app.reload()}>重新整理行情</button>}
      </div>
      {msg && <p className="loan-hint" role="status">{msg}</p>}
    </div>
  );
}
