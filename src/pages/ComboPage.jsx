// 組合拳訊號：買進候選 → 持有 → 賣出確認 → 移除（紙上追蹤，每個交易日 19:00 更新）
// 資料：public/data/combo_signals.json（backend/tuning/combo_daily.py）
import React, { useEffect, useMemo, useState } from "react";
import { History, LogOut, Sparkles, Wallet } from "lucide-react";
import { Empty } from "../components/ui.jsx";
import { useSort } from "../components/SortableTh.jsx";
import { fetchStaticJson } from "../lib/data.js";
import { fetchQuotes } from "../lib/market.js";
import { signed, trendColor } from "../lib/format.js";
import { navigate } from "../lib/router.js";
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

const liveRet = (p, q) => (q?.[p.stock_id]?.price && p.entry_price ? q[p.stock_id].price / p.entry_price - 1 : p.ret);

function BuyCard({ p }) {
  return (
    <article className="combo-item is-green">
      <div className="combo-item-top">
        <Name p={p} />
        <Chip color="green" label="可買進" />
      </div>
      <dl className="combo-stats">
        <Stat label="今日收盤" big><span className="num">{p.signal_close}</span></Stat>
        <Stat label="怎麼做">明天開盤買進</Stat>
      </dl>
      <Why r={p.buy_reasons} title="為什麼可以買（看理由）" />
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

function SellCard({ p, q }) {
  return (
    <article className="combo-item is-red">
      <div className="combo-item-top">
        <Name p={p} />
        <Chip color="red" label="該賣出" />
      </div>
      <dl className="combo-stats">
        <Stat label="怎麼做">明天開盤賣出</Stat>
        <Stat label="目前報酬" big><Pct v={liveRet(p, q)} /></Stat>
        <Stat label="買進日"><span className="num">{fmtDate(p.entry_date)}</span></Stat>
        <Stat label="買價 / 現價"><span className="num">{p.entry_price ?? "–"}</span> / <LivePrice q={q[p.stock_id]} /></Stat>
      </dl>
      <Why r={p.sell_reasons} title="為什麼要賣（看理由）" />
    </article>
  );
}

function HoldingList({ rows, quotes }) {
  const cols = useMemo(() => ({
    id: (p) => p.stock_id, entry: (p) => p.entry_date, days: (p) => p.days, ret: (p) => p.ret,
  }), []);
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
            </article>
          );
        })}
      </div>
    </>
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

function ClosedItem({ p }) {
  return (
    <div className="combo-closed">
      <div className="combo-closed-top"><Name p={p} /><Pct v={p.ret} /></div>
      <div className="num">{fmtDate(p.entry_date)} 買 {p.entry_price} → {fmtDate(p.exit_date)} 賣 {p.exit_price}（持有 {p.days} 天，已扣成本）</div>
      <Why r={p.sell_reasons} title="賣出理由" />
    </div>
  );
}

export default function ComboPage() {
  const [data, setData] = useState(undefined);
  useEffect(() => { fetchStaticJson("/combo_signals.json").then(setData); }, []);

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
  const closed = allClosed.filter((c) => !c.replay).slice(0, 40);
  const replayClosed = allClosed.filter((c) => c.replay);
  const watch = data?.intraday_watch || [];
  const rt = useLiveQuotes([...watch.map((w) => w.stock_id), ...sells.map((p) => p.stock_id), ...holding.map((p) => p.stock_id)]);
  const q = rt.quotes;
  const names = (arr) => arr.map((p) => `${p.name}`).join("、");
  const wins = closed.filter((c) => c.ret > 0).length;

  return (
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
              <h2 id="combo-today-h">今天要做什麼</h2>
              <ul>
                <li>
                  <Chip color={sells.length ? "red" : "gray"} label="賣出" />
                  <span>{sells.length ? <><b>明天開盤賣出 {sells.length} 檔</b>：{names(sells)}</> : "目前沒有要賣的股票。"}</span>
                </li>
                <li>
                  <Chip color={fresh.length ? "green" : "gray"} label="買進" />
                  <span>{fresh.length ? <><b>明天開盤可買進 {fresh.length} 檔</b>：{names(fresh)}</> : "今天沒有新的買進候選，不用動作。"}</span>
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

            <Section icon={Sparkles} title="買進候選" count={fresh.length + watch.length}>
              <h3 className="combo-sub">今天收盤確認，明天開盤買（{fresh.length} 檔）</h3>
              {!fresh.length ? <Nothing>今天沒有股票通過全部條件。</Nothing>
                : <div className="combo-list">{fresh.map((p) => <BuyCard key={p.stock_id} p={p} />)}</div>}

              <h3 className="combo-sub">還差一點：盤中觀察（{watch.length} 檔）</h3>
              {!watch.length ? <Nothing>今天沒有只差突破的股票。</Nothing> : (
                <>
                  <p className="combo-section-hint">
                    現價站上「要站上的價位」就會亮綠燈，離價位 2% 以內亮黃燈。網頁先亮燈、手機提醒晚一點到是正常的（提醒還要求連續 2 分鐘站上、量有放大）。最後要收盤仍站穩才算買點。
                  </p>
                  <LiveBanner live={rt} />
                  <div className="combo-list">{watch.map((w) => <WatchCard key={w.stock_id} w={w} q={q} open={rt.open} />)}</div>
                </>
              )}
            </Section>

            <Section icon={LogOut} title="賣出確認" count={sells.length} hint="這些已出現轉壞訊號，紙上追蹤明天開盤賣出，賣出後會自動從清單移除。">
              {!sells.length ? <Nothing>目前沒有要賣的股票。</Nothing>
                : <div className="combo-list">{sells.map((p) => <SellCard key={p.stock_id} p={p} q={q} />)}</div>}
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
                  : <div className="combo-closed-list">{closed.map((p) => <ClosedItem key={p.stock_id + p.signal_date} p={p} />)}</div>}
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
  );
}
