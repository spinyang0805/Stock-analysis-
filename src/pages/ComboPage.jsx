// 組合拳訊號：買進候選 → 持有 → 賣出確認 → 移除（紙上追蹤，每個交易日 19:00 更新）
// 資料：public/data/combo_signals.json（backend/tuning/combo_daily.py）
import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { History, LogOut, Sparkles, Wallet } from "lucide-react";
import { Empty } from "../components/ui.jsx";
import { useSort } from "../components/SortableTh.jsx";
import { fetchStaticJson } from "../lib/data.js";
import { fetchQuotes } from "../lib/market.js";
import { signed, trendColor } from "../lib/format.js";
import { navigate } from "../lib/router.js";
import { useApp } from "../lib/appState.jsx";
import { MY_STOP, compareMine, lateBuyLight, loadMarks, markKey, myReturn, removeMark, saveMark } from "../lib/comboMarks.js";
import "./ComboPage.css";

// 狀態標籤：圓點＋文字（綠 = 可買／已突破、黃 = 接近／等待、紅 = 賣出、灰 = 尚遠）
function Chip({ color = "gray", label, pulse = false }) {
  return <span className={`combo-chip is-${color}${pulse ? " is-pulse" : ""}`}>{label}</span>;
}

// 台股盤中（台北時間週一～五 09:00–13:35）
function marketOpen(now = new Date()) {
  const tw = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Taipei" }));
  const m = tw.getHours() * 60 + tw.getMinutes();
  return tw.getDay() >= 1 && tw.getDay() <= 5 && m >= 540 && m <= 815;
}

// 盤中每 60 秒抓一次即時報價；非盤中只抓一次（顯示最近報價）。網頁關掉就停。
function useLiveQuotes(codes) {
  const key = [...new Set(codes)].sort().join(",");
  const [state, setState] = useState({ quotes: {}, at: null, open: marketOpen(), error: null });
  useEffect(() => {
    if (!key) return undefined;
    let alive = true;
    const load = async () => {
      const open = marketOpen();
      try {
        const r = await fetchQuotes(key.split(","));
        if (alive) setState({ quotes: r.quotes, at: new Date(), open, error: null });
      } catch (e) {
        if (alive) setState((st) => ({ ...st, open, error: e.message || "報價取得失敗" }));
      }
    };
    load();
    const id = setInterval(() => { if (marketOpen()) load(); }, 60000);
    return () => { alive = false; clearInterval(id); };
  }, [key]);
  return state;
}

function LiveBanner({ live }) {
  const t = live.at ? live.at.toLocaleTimeString("zh-TW", { hour12: false, timeZone: "Asia/Taipei" }) : "–";
  return (
    <div className={`combo-live${live.open ? " is-open" : ""}`} role="status">
      <Chip color={live.open ? "green" : "gray"} label={live.open ? "盤中：每 60 秒自動更新報價" : "非交易時間：顯示最近一筆報價"} pulse={live.open} />
      <span className="dim">最後更新 {t}</span>
      {live.error && <span className="combo-err">（{live.error}）</span>}
    </div>
  );
}

const fmtDate = (d) => (d ? `${d.slice(0, 4)}/${d.slice(4, 6)}/${d.slice(6)}` : "–");
// 「今天／明天」看網頁的時間不同會搞混，一律寫實際日期＋星期，例如 10/22（四）
const toDate = (d) => new Date(+d.slice(0, 4), +d.slice(4, 6) - 1, +d.slice(6));
const fmtDay = (d) => (d ? `${+d.slice(4, 6)}/${+d.slice(6)}（${"日一二三四五六"[toDate(d).getDay()]}）` : "–");
// 後端沒給下個交易日時退回下一個平日（不知道假日）
function nextWeekday(d) {
  const t = toDate(d);
  do t.setDate(t.getDate() + 1); while (t.getDay() === 0 || t.getDay() === 6);
  return `${t.getFullYear()}${String(t.getMonth() + 1).padStart(2, "0")}${String(t.getDate()).padStart(2, "0")}`;
}

function Pct({ v }) {
  if (v == null) return <span className="dim">–</span>;
  return <span className="num" style={{ color: trendColor(v) }}>{signed(v * 100, 1, "%")}</span>;
}

function LivePrice({ q }) {
  if (!q || !Number.isFinite(q.price)) return <span className="dim">–</span>;
  return (
    <span className="num">{q.price}{" "}
      <span style={{ color: trendColor(q.changePct), fontSize: 12 }}>{signed(q.changePct, 2, "%")}</span>
    </span>
  );
}

// 理由收進「看理由」，預設收合
function Why({ r, title }) {
  if (!r || !Object.keys(r).length) return null;
  return (
    <details className="combo-why">
      <summary>{title}</summary>
      <ul className="combo-why-list">
        {Object.entries(r).map(([k, v]) => (
          <li key={k}><b>{k}</b>{v.join("、")}</li>
        ))}
      </ul>
    </details>
  );
}

function Name({ p }) {
  return (
    <button type="button" className="combo-name" onClick={() => navigate(`/stock/${p.stock_id}`)} aria-label={`查看 ${p.name} ${p.stock_id} 個股頁`}>
      <span className="num">{p.stock_id}</span>{p.name}
      {p.in_rank && <span className="combo-tag" title="也在當月排名前 20">排名前 20</span>}
      {p.replay && <span className="combo-tag" title="上線前回放的模擬部位，晨報沒有推薦過">模擬</span>}
    </button>
  );
}

function Stat({ label, children, big, wide }) {
  return (
    <div className={wide ? "combo-wide" : undefined}>
      <dt>{label}</dt>
      <dd className={big ? "is-big" : undefined}>{children}</dd>
    </div>
  );
}

// 即時價只在是正數時採用（報價 API 可能回 NaN），否則退回最近收盤
const goodPx = (x) => (Number.isFinite(x) && x > 0 ? x : null);
// 我的報酬用的價格：即時價 → 最近收盤 → 紙上賣價（卡片和上方對照共用，數字才一致）
const markPx = (p, q) => goodPx(q?.[p.stock_id]?.price) ?? p.last_close ?? p.exit_price ?? null;
const liveRet = (p, q) => (q?.[p.stock_id]?.price && p.entry_price ? q[p.stock_id].price / p.entry_price - 1 : p.ret);

function BuyCard({ p, day }) {
  return (
    <article className="combo-item is-green">
      <div className="combo-item-top">
        <Name p={p} />
        <Chip color="green" label="可買進" />
      </div>
      <dl className="combo-stats">
        <Stat label={`${fmtDay(p.signal_date)}收盤`} big><span className="num">{p.signal_close}</span></Stat>
        <Stat label="怎麼做">{fmtDay(day)}開盤買進</Stat>
      </dl>
      <Why r={p.buy_reasons} title="為什麼可以買（看理由）" />
      <MarkBox p={p} />
    </article>
  );
}

function WatchCard({ w, q, open }) {
  const px = q[w.stock_id]?.price;
  const gap = Number.isFinite(px) ? w.level / px - 1 : null;
  const hit = gap != null && gap <= 0;
  const chip = hit ? <Chip color="green" label="盤中已突破" pulse={open} />
    : gap == null ? <Chip color="gray" label="沒有報價" />
      : gap <= 0.02 ? <Chip color="yellow" label="接近突破" />
        : <Chip color="gray" label="還沒到" />;
  return (
    <article className={`combo-item is-${hit ? "green" : gap != null && gap <= 0.02 ? "yellow" : "gray"}`}>
      <div className="combo-item-top">
        <Name p={w} />
        {chip}
      </div>
      <dl className="combo-stats">
        <Stat label="現價（即時）" big><LivePrice q={q[w.stock_id]} /></Stat>
        <Stat label="要站上的價位" big><span className="num">{w.level}</span></Stat>
        <Stat label="離突破還差">
          {gap == null ? <span className="dim">–</span> : hit ? <b style={{ color: "var(--combo-green)" }}>已站上</b> : <span className="num">+{(gap * 100).toFixed(1)}%</span>}
        </Stat>
        <Stat label="昨天收盤"><span className="num">{w.close}</span></Stat>
      </dl>
      {hit && <div className="dim">盤中站上不算數，收盤仍站穩才確認買進。</div>}
      <Why r={w.buy_reasons} title="已通過的條件（看理由）" />
    </article>
  );
}

function SellCard({ p, q, day }) {
  return (
    <article className="combo-item is-red">
      <div className="combo-item-top">
        <Name p={p} />
        <Chip color="red" label="該賣出" />
      </div>
      <dl className="combo-stats">
        <Stat label="怎麼做">{fmtDay(day)}開盤賣出</Stat>
        <Stat label="目前報酬" big><Pct v={liveRet(p, q)} /></Stat>
        <Stat label="買進日"><span className="num">{fmtDate(p.entry_date)}</span></Stat>
        <Stat label="買價 / 現價"><span className="num">{p.entry_price ?? "–"}</span> / <LivePrice q={q[p.stock_id]} /></Stat>
      </dl>
      <Why r={p.sell_reasons} title="為什麼要賣（看理由）" />
      <MarkBox p={p} price={q[p.stock_id]?.price} />
    </article>
  );
}

function HoldingList({ rows, quotes }) {
  const cols = useMemo(() => ({
    id: (p) => p.stock_id, entry: (p) => p.entry_date, days: (p) => p.days, ret: (p) => (quotes ? liveRet(p, quotes) : p.ret),
  }), [quotes]);
  const { sorted, sort, toggle } = useSort(rows, cols, { key: "ret", dir: "desc" });
  const opts = [["ret", "報酬"], ["entry", "買進日"], ["days", "持有天數"], ["id", "股票代號"]];
  return (
    <>
      <div className="combo-sortbar" role="group" aria-label="排序方式">
        排序：
        {opts.map(([k, label]) => (
          <button key={k} type="button" aria-pressed={sort.key === k} onClick={() => toggle(k, k === "id" ? "asc" : "desc")}>
            {label}{sort.key === k ? (sort.dir === "asc" ? " ↑" : " ↓") : ""}
          </button>
        ))}
      </div>
      <div className="combo-list">
        {sorted.map((p) => {
          const waiting = p.status === "waiting";
          return (
            <article key={p.stock_id + p.signal_date} className={`combo-item is-${waiting ? "yellow" : "green"}`}>
              <div className="combo-item-top">
                <Name p={p} />
                <Chip color={waiting ? "yellow" : "green"} label={waiting ? "等待進場" : "繼續持有"} />
              </div>
              <dl className="combo-stats">
                <Stat label={quotes ? "目前報酬（即時）" : "報酬"} big><Pct v={quotes ? liveRet(p, quotes) : p.ret} /></Stat>
                <Stat label="持有天數"><span className="num">{p.days ?? "–"}</span> 天</Stat>
                <Stat label="買進日"><span className="num">{fmtDate(p.entry_date)}</span></Stat>
                <Stat label="買價 / 現價"><span className="num">{p.entry_price ?? "–"}</span>{quotes && <> / <LivePrice q={quotes[p.stock_id]} /></>}</Stat>
              </dl>
              <Why r={p.buy_reasons} title="當初為什麼買（看理由）" />
              <MarkBox p={p} price={quotes?.[p.stock_id]?.price} late={!waiting} />
            </article>
          );
        })}
      </div>
    </>
  );
}

// ── 我有買：使用者自己的實際買賣價（只有自己看得到），跟紙上追蹤並列 ──
const MarkCtx = createContext(null);
const todayTw = () => new Date().toLocaleDateString("sv-SE", { timeZone: "Asia/Taipei" });
const slashDate = (d) => (d ? d.replaceAll("-", "/") : "–");

function MarkForm({ p, mark, price, onDone }) {
  const ctx = useContext(MarkCtx);
  const [f, setF] = useState(() => ({
    buy_date: mark?.buy_date || todayTw(),
    buy_price: String(mark?.buy_price ?? (Number.isFinite(price) ? price : p.entry_price ?? "")),
    sell_date: mark?.sell_date || "",
    sell_price: mark?.sell_price == null ? "" : String(mark.sell_price),
  }));
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const set = (k) => (e) => setF((x) => ({ ...x, [k]: e.target.value }));
  const submit = async (e) => {
    e.preventDefault();
    const buy = Number(f.buy_price), sell = f.sell_price === "" ? null : Number(f.sell_price);
    if (!(buy > 0)) return setErr("買入價要大於 0");
    if ((sell == null) !== (f.sell_date === "")) return setErr("賣出日和賣出價要一起填（或都不填）");
    if (sell != null && !(sell > 0)) return setErr("賣出價要大於 0");
    if (f.sell_date && f.sell_date < f.buy_date) return setErr("賣出日不能早於買進日");
    setBusy(true); setErr("");
    try {
      await ctx.save({ stock_id: p.stock_id, signal_date: p.signal_date, buy_date: f.buy_date, buy_price: buy, sell_date: f.sell_date || null, sell_price: sell });
      onDone();
    } catch (x) { setErr(x.message || "儲存失敗"); setBusy(false); }
  };
  return (
    <form className="combo-markform" onSubmit={submit}>
      <label>買進日<input type="date" value={f.buy_date} onChange={set("buy_date")} required /></label>
      <label>買入價<input type="number" inputMode="decimal" step="0.01" min="0" value={f.buy_price} onChange={set("buy_price")} required /></label>
      {mark && <>
        <label>賣出日<input type="date" value={f.sell_date} onChange={set("sell_date")} /></label>
        <label>賣出價<input type="number" inputMode="decimal" step="0.01" min="0" value={f.sell_price} onChange={set("sell_price")} /></label>
      </>}
      <div className="combo-markform-btns">
        <button type="submit" className="is-primary" disabled={busy}>{busy ? "儲存中…" : "儲存"}</button>
        <button type="button" onClick={onDone} disabled={busy}>取消</button>
      </div>
      {err && <div className="combo-err" role="alert">{err}</div>}
    </form>
  );
}

function MarkBox({ p, price, late = false }) {
  const ctx = useContext(MarkCtx);
  const [editing, setEditing] = useState(false);
  if (!ctx) return null;
  const mark = ctx.marks[markKey(p)];
  const light = !mark && late ? lateBuyLight(price, p.entry_price) : null;
  const lateRow = late && !mark && (
    <div className="combo-late">
      {light ? <><Chip color={light.color} label={`盤中：${light.label}`} />
        <span className="dim">現價比紙上買價 <span className="num">{signed(light.r * 100, 1, "%")}</span>
          {light.key === "red_lo" && p.support != null && (p.support ? "；季線和結構還在，但回測證據不足" : "；季線或結構已轉壞")}</span></>
        : <span className="dim">沒有即時報價，無法判斷現在適不適合買。</span>}
    </div>
  );
  if (!ctx.user) return <div className="combo-mark">{lateRow}<div className="dim">登入後可以標記「我有買」，用自己的買價追蹤。</div></div>;
  if (editing) return <div className="combo-mark"><MarkForm p={p} mark={mark} price={price} onDone={() => setEditing(false)} /></div>;
  if (!mark) {
    return (
      <div className="combo-mark">
        {lateRow}
        <button type="button" className="combo-markbtn" onClick={() => setEditing(true)}>我有買</button>
      </div>
    );
  }
  const sold = mark.sell_price != null;
  const r = myReturn(mark, goodPx(price) ?? p.last_close ?? p.exit_price ?? null);
  const remove = async () => { try { await ctx.remove(mark); } catch (x) { ctx.setError(x.message || "刪除失敗"); } };
  return (
    <div className="combo-mark is-mine">
      <div className="combo-mark-top">
        <Chip color="green" label={sold ? "我已賣出" : "我有買"} />
        <span className="num">{slashDate(mark.buy_date)} 買 {mark.buy_price}{sold ? ` → ${slashDate(mark.sell_date)} 賣 ${mark.sell_price}` : ""}</span>
      </div>
      <dl className="combo-stats">
        <Stat label={sold ? "我的報酬" : "我的報酬（即時）"}><Pct v={r} /></Stat>
        {!sold && <Stat label="我的停損價"><span className="num">{(mark.buy_price * (1 - MY_STOP)).toFixed(2)}</span></Stat>}
      </dl>
      <div className="combo-markform-btns">
        <button type="button" onClick={() => setEditing(true)}>{sold ? "修改" : "修改／記錄賣出"}</button>
        <button type="button" onClick={remove}>取消標記</button>
      </div>
    </div>
  );
}

function MineSummary({ s }) {
  if (!s) return null;
  return (
    <section className="card combo-mine" aria-labelledby="combo-mine-h">
      <h2 id="combo-mine-h">我的實際操作 vs 紙上追蹤（{s.n} 檔）</h2>
      <dl className="combo-stats">
        <Stat label="我的平均報酬" big><Pct v={s.mine} /></Stat>
        <Stat label="同一批紙上平均報酬" big><Pct v={s.paper} /></Stat>
        <Stat label="差距（我 − 紙上）"><Pct v={s.mine - s.paper} /></Stat>
        <Stat label="我的買價比紙上買價">{s.gap == null ? <span className="dim">–</span> : <Pct v={s.gap} />}</Stat>
      </dl>
      <p className="dim">紙上報酬代表「推薦本身準不準」；差距多半來自進場時機和價格。我的報酬未扣手續費與稅，紙上已結案的有扣。</p>
    </section>
  );
}

function Section({ icon: Icon, title, count, hint, children }) {
  return (
    <section className="combo-section">
      <div className="combo-section-head">
        <h2><Icon size={18} aria-hidden="true" />{title}</h2>
        {count != null && <span className="combo-count">{count} 檔</span>}
      </div>
      {hint && <p className="combo-section-hint">{hint}</p>}
      {children}
    </section>
  );
}

const Nothing = ({ children }) => <div className="combo-empty">{children}</div>;

function ClosedItem({ p, q, markable = false }) {
  return (
    <div className="combo-closed">
      <div className="combo-closed-top"><Name p={p} /><Pct v={p.ret} /></div>
      <div className="num">{fmtDate(p.entry_date)} 買 {p.entry_price} → {fmtDate(p.exit_date)} 賣 {p.exit_price}（持有 {p.days} 天，已扣成本）</div>
      <Why r={p.sell_reasons} title="賣出理由" />
      {markable && <MarkBox p={p} price={q?.[p.stock_id]?.price} />}
    </div>
  );
}

export default function ComboPage() {
  const [data, setData] = useState(undefined);
  useEffect(() => { fetchStaticJson("/combo_signals.json").then(setData); }, []);
  const { user } = useApp();
  const [marks, setMarks] = useState({});
  const [markErr, setMarkErr] = useState("");
  // 換帳號／登出時先清空，並丟掉舊帳號還在路上的回應，避免顯示到別人的標記
  const uid = user?.id || null;
  const reqRef = useRef(0);
  const reloadMarks = useCallback(async () => {
    const req = ++reqRef.current;
    if (!uid) { setMarks({}); return; }
    try {
      const m = await loadMarks();
      if (req === reqRef.current) setMarks(m);
    } catch (e) { if (req === reqRef.current) setMarkErr(e.message || "讀取標記失敗"); }
  }, [uid]);
  useEffect(() => { setMarks({}); setMarkErr(""); }, [uid]);
  useEffect(() => { reloadMarks(); }, [reloadMarks]);
  const markCtx = useMemo(() => ({
    user: uid, marks, setError: setMarkErr,
    save: async (m) => { await saveMark(m); setMarkErr(""); await reloadMarks(); },
    remove: async (m) => { await removeMark(m); setMarkErr(""); await reloadMarks(); },
  }), [uid, marks, reloadMarks]);

  const since = data?.live_since || "20261019";
  const mark = (p) => ({ ...p, replay: p.signal_date < since });
  const pos = (data?.positions || []).map(mark);
  const newIds = new Set(data?.new_today || []);
  // 晨報真正推薦過的（上線後）才是主畫面；上線前回放的模擬部位收在最下面的參考區
  const live = pos.filter((p) => !p.replay);
  const replay = pos.filter((p) => p.replay);
  const fresh = live.filter((p) => newIds.has(p.stock_id) && p.status === "waiting");
  const sells = live.filter((p) => p.status === "sell_signal");
  const holding = live.filter((p) => p.status === "holding" || (p.status === "waiting" && !newIds.has(p.stock_id)));
  const allClosed = (data?.closed || []).filter((c) => c.status === "closed").map(mark);
  const liveClosed = allClosed.filter((c) => !c.replay);
  // 列最近 40 筆；有我標記的較舊結案也一直列出，才能記錄賣出或修改
  const closed = liveClosed.filter((c, i) => i < 40 || marks[markKey(c)]);
  const replayClosed = allClosed.filter((c) => c.replay);
  const watch = data?.intraday_watch || [];
  // 已結案但我還沒賣的標記也要即時價，我的報酬才不會停在紙上的賣價
  const openMarkedClosed = liveClosed.filter((c) => marks[markKey(c)] && marks[markKey(c)].sell_price == null);
  const rt = useLiveQuotes([...watch.map((w) => w.stock_id), ...sells.map((p) => p.stock_id), ...holding.map((p) => p.stock_id),
    ...openMarkedClosed.map((c) => c.stock_id)]);
  const q = rt.quotes;
  const names = (arr) => arr.map((p) => `${p.name}`).join("、");
  const wins = closed.filter((c) => c.ret > 0).length;
  const mine = compareMine(marks, [
    ...live.map((p) => ({ p, price: markPx(p, q), paper: p.entry_price ? liveRet(p, q) : null })),
    ...liveClosed.map((c) => ({ p: c, price: markPx(c, q), paper: c.ret })),
  ]);
  const closeDay = fmtDay(data?.updated);
  const actDate = data?.next_trade_date || (data?.updated ? nextWeekday(data.updated) : null);
  const actDay = fmtDay(actDate);

  return (
    <MarkCtx.Provider value={markCtx}>
    <div className="page combo-page">
      <div className="page-head">
        <div>
          <div className="eyebrow">COMBINATION SIGNALS</div>
          <h1 className="page-title">組合拳訊號</h1>
          <div className="dim">{data?.updated ? `資料至 ${fmtDate(data.updated)} 收盤` : ""}　這是紙上追蹤，不是你的持股，也不是下單建議。</div>
        </div>
      </div>

      {data === undefined ? <div className="combo-empty">載入中…</div>
        : !data ? (
          <Empty icon={Sparkles} title="還沒有組合拳資料">每個交易日晚上 19:00 後產生。</Empty>
        ) : (
          <>
            <section className="card combo-today" aria-labelledby="combo-today-h">
              <h2 id="combo-today-h">{actDay}要做什麼</h2>
              <p className="dim">依 {closeDay}收盤的資料，下一個交易日 {actDay}開盤照下面做（已避開國定假日；遇颱風等臨時休市，就順延到下一個開盤日）。</p>
              <ul>
                <li>
                  <Chip color={sells.length ? "red" : "gray"} label="賣出" />
                  <span>{sells.length ? <><b>{actDay}開盤賣出 {sells.length} 檔</b>：{names(sells)}</> : "目前沒有要賣的股票。"}</span>
                </li>
                <li>
                  <Chip color={fresh.length ? "green" : "gray"} label="買進" />
                  <span>{fresh.length ? <><b>{actDay}開盤可買進 {fresh.length} 檔</b>：{names(fresh)}</> : `${closeDay}收盤沒有新的買進候選，不用動作。`}</span>
                </li>
                <li>
                  <Chip color={watch.length ? "yellow" : "gray"} label="觀察" />
                  <span>{watch.length ? <>另有 <b>{watch.length} 檔</b>只差一步突破，盤中看即時報價。</> : "沒有只差突破的股票。"}</span>
                </li>
                <li>
                  <Chip color={holding.length ? "green" : "gray"} label="持有" />
                  <span>追蹤中 {holding.length} 檔，沒被標為「該賣出」就續抱。</span>
                </li>
              </ul>
            </section>

            {markErr && <div className="notice err" role="alert">{markErr}</div>}
            <MineSummary s={mine} />

            <Section icon={Sparkles} title="買進候選" count={fresh.length + watch.length}>
              <h3 className="combo-sub">{closeDay}收盤已確認 → {actDay}開盤買（{fresh.length} 檔）</h3>
              {!fresh.length ? <Nothing>{closeDay}收盤沒有股票通過全部條件。</Nothing>
                : <div className="combo-list">{fresh.map((p) => <BuyCard key={p.stock_id} p={p} day={actDate} />)}</div>}

              <h3 className="combo-sub">還差一點：{actDay}盤中觀察（{watch.length} 檔）</h3>
              {!watch.length ? <Nothing>沒有只差突破的股票。</Nothing> : (
                <>
                  <p className="combo-section-hint">
                    現價站上「要站上的價位」就會亮綠燈，離價位 2% 以內亮黃燈。網頁先亮燈、手機提醒晚一點到是正常的（提醒還要求連續 2 分鐘站上、量有放大）。最後要收盤仍站穩才算買點。
                  </p>
                  <LiveBanner live={rt} />
                  <div className="combo-list">{watch.map((w) => <WatchCard key={w.stock_id} w={w} q={q} open={rt.open} />)}</div>
                </>
              )}
            </Section>

            <Section icon={LogOut} title="賣出確認" count={sells.length} hint={`這些已出現轉壞訊號，紙上追蹤 ${actDay}開盤賣出，賣出後會自動從清單移除。`}>
              {!sells.length ? <Nothing>目前沒有要賣的股票。</Nothing>
                : <div className="combo-list">{sells.map((p) => <SellCard key={p.stock_id} p={p} q={q} day={actDate} />)}</div>}
            </Section>

            <Section icon={Wallet} title="持有中" count={holding.length} hint="晨報推薦過、目前還在追蹤的股票，會一直列在這裡直到出現賣出確認。">
              {!holding.length ? <Nothing>目前沒有。晨報推薦的候選進場後會出現在這裡。</Nothing> : (
                <>
                  <LiveBanner live={rt} />
                  <HoldingList rows={holding} quotes={q} />
                </>
              )}
            </Section>

            <details className="combo-fold">
              <summary>
                <History size={16} aria-hidden="true" style={{ marginRight: 8 }} />
                歷史結案紀錄（{closed.length} 筆{closed.length ? `，賺錢 ${Math.round(wins / closed.length * 100)}%` : ""}）
              </summary>
              <div className="combo-fold-body">
                {!closed.length ? <Nothing>還沒有結案紀錄（上線後推薦過的股票賣出後會列在這裡）。</Nothing>
                  : <div className="combo-closed-list">{closed.map((p) => <ClosedItem key={p.stock_id + p.signal_date} p={p} q={q} markable />)}</div>}
              </div>
            </details>

            {(replay.length > 0 || replayClosed.length > 0) && (
              <details className="combo-fold">
                <summary>參考：上線前的模擬部位（{replay.length} 檔持有、{replayClosed.length} 筆結案）</summary>
                <div className="combo-fold-body">
                  <p>用最近 40 個交易日的歷史資料，模擬「如果當時照規則買」的結果，只用來觀察規則表現。這不是推薦，也不是你的持股，晨報不會針對這些股票提醒。</p>
                  {replay.length > 0 && <HoldingList rows={replay} />}
                </div>
              </details>
            )}

            <details className="combo-fold">
              <summary>規則說明（買進、賣出怎麼判斷）</summary>
              <div className="combo-fold-body">
                <p><b>買進</b>要四個條件都過：</p>
                <p>1. 股價站上「往上走的季線」，而且大盤不是空頭。<br />
                  2. 最近 3 天真的突破（創新高、W 底頸線、趨勢線等）。<br />
                  3. 最近 5 天有資金流入（量增、OBV 上升）。<br />
                  4. 籌碼、相對強勁、營收，至少一項也不錯。</p>
                <p><b>賣出</b>要「真的轉壞」：最近 10 天至少 2 種結構破壞（高點越來越低、跌破趨勢線等），再加上至少 2 類確認（資金流出、籌碼賣出、大盤轉壞）；或跌破往下彎的季線；或比買價跌 10%。只是短線過熱（RSI、KD 在高檔）不會賣。</p>
                <p>標「排名前 20」的是也在當月排名前 20 名。標「模擬」的是上線（{fmtDate(since)}）前用歷史資料模擬的部位。</p>
                <p>回測 2020–2026，每筆平均贏過股票池約 1.8%；但最近半年只打平，輸給 0050。</p>
              </div>
            </details>
          </>
        )}
    </div>
    </MarkCtx.Provider>
  );
}
