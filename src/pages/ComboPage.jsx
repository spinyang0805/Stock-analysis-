// 組合拳訊號：買進候選 → 持有 → 賣出確認 → 移除（紙上追蹤，每個交易日 19:00 更新）
// 資料：public/data/combo_signals.json（backend/tuning/combo_daily.py）
import React, { useEffect, useMemo, useState } from "react";
import { History, LogOut, Sparkles, Wallet } from "lucide-react";
import { Card, Empty, Kpi } from "../components/ui.jsx";
import { SortTh, useSort } from "../components/SortableTh.jsx";
import { fetchStaticJson } from "../lib/data.js";
import { fetchQuotes } from "../lib/market.js";
import { signed, trendColor } from "../lib/format.js";
import { navigate } from "../lib/router.js";

// 燈號：圓燈＋文字（綠 = 買點／突破、黃 = 接近／等待、紅 = 賣出、灰 = 尚遠）
const LIGHTS = {
  green: { c: "#22c55e", bg: "rgba(34,197,94,.14)" },
  yellow: { c: "#eab308", bg: "rgba(234,179,8,.16)" },
  red: { c: "#ef4444", bg: "rgba(239,68,68,.14)" },
  gray: { c: "#94a3b8", bg: "rgba(148,163,184,.14)" },
};

function Light({ color = "gray", label, sub, pulse = false, size = 14 }) {
  const l = LIGHTS[color];
  return (
    <span className="light" style={{ background: l.bg, color: l.c }}>
      <i className={pulse ? "light-dot pulse" : "light-dot"}
        style={{ width: size, height: size, background: l.c, boxShadow: `0 0 8px ${l.c}` }} aria-hidden="true" />
      <span>
        <b>{label}</b>
        {sub && <span className="light-sub">{sub}</span>}
      </span>
    </span>
  );
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
  const style = { padding: "6px 10px", borderRadius: 8, fontSize: 13, marginBottom: 8,
    background: live.open ? "rgba(34,197,94,.12)" : "rgba(148,163,184,.15)" };
  return (
    <div style={style}>
      {live.open ? <b>🟢 盤中即時：每 60 秒自動更新</b> : <b>⚪ 非交易時間：顯示最近一筆報價</b>}
      <span className="dim">　最後更新 {t}</span>
      {live.error && <span style={{ color: "#ef4444" }}>　（{live.error}）</span>}
    </div>
  );
}

function LivePrice({ q }) {
  if (!q || !Number.isFinite(q.price)) return <span className="dim">–</span>;
  return (
    <span className="num">{q.price}{" "}
      <span style={{ color: trendColor(q.changePct), fontSize: 12 }}>{signed(q.changePct, 2, "%")}</span>
    </span>
  );
}

const fmtDate = (d) => (d ? `${d.slice(0, 4)}/${d.slice(4, 6)}/${d.slice(6)}` : "–");

function Pct({ v }) {
  if (v == null) return <span className="dim">–</span>;
  return <span className="num" style={{ color: trendColor(v) }}>{signed(v * 100, 1, "%")}</span>;
}

function Reasons({ r }) {
  if (!r || !Object.keys(r).length) return <span className="dim">–</span>;
  return (
    <div style={{ display: "grid", gap: 2 }}>
      {Object.entries(r).map(([k, v]) => (
        <div key={k}><b>{k}</b>：<span className="dim">{v.join("、")}</span></div>
      ))}
    </div>
  );
}

function StockCell({ p }) {
  return (
    <><b className="num">{p.stock_id}</b> {p.name}{p.in_rank ? " ⭐" : ""}
      {p.replay && <span className="dim" style={{ fontSize: 11, marginLeft: 4 }} title="上線前回放的模擬部位，晨報沒有推薦過">（回放）</span>}</>
  );
}

function HoldingTable({ rows, quotes }) {
  const cols = useMemo(() => ({
    id: (p) => p.stock_id, entry: (p) => p.entry_date, days: (p) => p.days, ret: (p) => p.ret,
  }), []);
  const { sorted, sort, toggle } = useSort(rows, cols, { key: "ret", dir: "desc" });
  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            <th className="left">燈號</th>
            {[["id", "股票", true], ["entry", "買進日", true], ["days", "持有日"], ["ret", "報酬"]]
              .map(([k, label, left]) => <SortTh key={k} k={k} label={label} left={left} sort={sort} onSort={toggle} />)}
            <th>買價</th><th>現價（即時）</th><th>即時報酬</th><th className="left">買進理由</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((p) => (
            <tr key={p.stock_id + p.signal_date} className="clickable" onClick={() => navigate(`/stock/${p.stock_id}`)}>
              <td className="left">{p.status === "waiting" ? <Light color="yellow" label="待進場" /> : <Light color="green" label="續抱" />}</td>
              <td className="left"><StockCell p={p} /></td>
              <td className="left num">{fmtDate(p.entry_date)}</td>
              <td className="num">{p.days ?? "–"}</td>
              <td><Pct v={p.ret} /></td>
              <td className="num">{p.entry_price ?? "–"}</td>
              <td><LivePrice q={quotes?.[p.stock_id]} /></td>
              <td>{quotes?.[p.stock_id]?.price && p.entry_price ? <Pct v={quotes[p.stock_id].price / p.entry_price - 1} /> : <Pct v={p.ret} />}</td>
              <td className="left" style={{ fontSize: 12 }}><Reasons r={p.buy_reasons} /></td>
            </tr>
          ))}
        </tbody>
      </table>
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

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">COMBINATION SIGNALS</div>
          <h1 className="page-title">組合拳訊號</h1>
          <div className="dim">{data?.updated ? `資料至 ${fmtDate(data.updated)} 收盤` : ""}</div>
        </div>
      </div>

      <Card>
        <div className="dim" style={{ lineHeight: 1.7 }}>
          <b>買進</b>要四關都過：站上往上走的季線且大盤不是空頭 → 近 3 天真突破（新高、W 底頸線、趨勢線…）→ 近 5 天資金流入（OBV、量增）→
          籌碼、相對強度、營收至少一項。<b>賣出</b>要「真的轉壞」：近 10 天 ≥2 種結構破壞（頭頭低、跌破趨勢線…）加上 ≥2 類確認（資金流出、籌碼賣出、環境轉壞），
          或跌破往下彎的季線，或比買價跌 10%。過熱（RSI、KD 高檔）不賣。⭐ = 也在當月排名前 20。
          這裡是紙上追蹤，不是你的持股，也不是下單建議；標「回放」的是上線（{fmtDate(since)}）前用歷史資料模擬的部位，晨報不會提醒。回測 2020–2026 每筆平均贏股票池約 1.8%，但最近半年只打平、輸 0050。
        </div>
      </Card>

      {data === undefined ? <Card style={{ marginTop: 12 }}><div className="dim">載入中…</div></Card>
        : !data ? (
          <Card style={{ marginTop: 12 }}><Empty icon={Sparkles} title="還沒有組合拳資料">每個交易日晚上 19:00 後產生。</Empty></Card>
        ) : (
          <>
            <div className="kpis" style={{ marginTop: 12 }}>
              <Kpi label={<Light color={fresh.length ? "green" : "gray"} label="今日買進候選" size={10} />} value={fresh.length} sub="明日開盤進場" />
              <Kpi label={<Light color={sells.length ? "red" : "gray"} label="賣出確認" size={10} />} value={sells.length} sub="明日開盤賣出" tone={sells.length ? "down" : undefined} />
              <Kpi label={<Light color={watch.some((w) => (rt.quotes[w.stock_id]?.price ?? 0) >= w.level) ? "green" : watch.length ? "yellow" : "gray"} label="盤中觀察" size={10} />} value={watch.length} sub="只差突破" />
              <Kpi label={<Light color={holding.length ? "green" : "gray"} label="追蹤持有中" size={10} />} value={holding.length} sub="直到賣出確認" />
              <Kpi label="已結案" value={closed.length} sub={closed.length ? `勝率 ${Math.round(closed.filter((c) => c.ret > 0).length / closed.length * 100)}%` : "上線後累積"} />
            </div>

            <Card title="今日買進候選（明日開盤進場）" icon={Sparkles} style={{ marginTop: 12 }}>
              {!fresh.length ? <div className="dim">今天沒有股票通過四關。</div> : (
                <div className="table-wrap">
                  <table className="data">
                    <thead><tr><th className="left">燈號</th><th className="left">股票</th><th>收盤</th><th className="left">為什麼買（各關通過的訊號）</th></tr></thead>
                    <tbody>
                      {fresh.map((p) => (
                        <tr key={p.stock_id} className="clickable" onClick={() => navigate(`/stock/${p.stock_id}`)}>
                          <td className="left"><Light color="green" label="買點確認" sub="明日開盤進場" /></td>
                          <td className="left"><StockCell p={p} /></td>
                          <td className="num">{p.signal_close}</td>
                          <td className="left" style={{ fontSize: 12 }}><Reasons r={p.buy_reasons} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>

            <Card title="盤中觀察：只差突破（帶量站上 20 日高會即時提醒）" icon={Sparkles} style={{ marginTop: 12 }}>
              {!watch.length ? <div className="dim">今天沒有只差突破的股票。</div> : (
                <>
                  <LiveBanner live={rt} />
                  <div className="table-wrap">
                    <table className="data">
                      <thead><tr><th className="left">狀態</th><th className="left">股票</th><th>現價（即時）</th><th>突破價</th><th>離突破還差</th><th>昨收</th><th className="left">已通過的關卡</th></tr></thead>
                      <tbody>
                        {watch.map((w) => {
                          const px = q[w.stock_id]?.price;
                          const gap = Number.isFinite(px) ? w.level / px - 1 : null;
                          const hit = gap != null && gap <= 0;
                          return (
                            <tr key={w.stock_id} className="clickable" onClick={() => navigate(`/stock/${w.stock_id}`)}
                              style={hit ? { background: "rgba(34,197,94,.10)", fontWeight: 600 } : undefined}>
                              <td className="left" style={{ whiteSpace: "nowrap" }}>
                                {hit ? <Light color="green" label="盤中已突破" sub="收盤站穩才確認" pulse={rt.open} />
                                  : gap == null ? <Light color="gray" label="無報價" />
                                    : gap <= 0.02 ? <Light color="yellow" label="接近突破" sub={`還差 ${(gap * 100).toFixed(1)}%`} />
                                      : <Light color="gray" label="未突破" sub={`還差 ${(gap * 100).toFixed(1)}%`} />}
                              </td>
                              <td className="left"><StockCell p={w} /></td>
                              <td><LivePrice q={q[w.stock_id]} /></td>
                              <td className="num"><b>{w.level}</b></td>
                              <td>{gap == null ? <span className="dim">–</span> : hit ? <b style={{ color: "#22c55e" }}>已站上</b> : <span className="num">+{(gap * 100).toFixed(1)}%</span>}</td>
                              <td className="num">{w.close}</td>
                              <td className="left" style={{ fontSize: 12 }}><Reasons r={w.buy_reasons} /></td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                  <div className="dim" style={{ fontSize: 12, marginTop: 6 }}>
                    燈號：🟢 盤中已突破（閃爍）、🟡 離突破 2% 以內、⚪ 還遠。🟢 只代表現價已高於突破價（網頁即時判斷）。正式的盤中提醒另外要求「連 2 分鐘站上＋帶量＋在均價之上」，所以網頁先亮燈、提醒稍後才到是正常的；收盤仍站在突破價之上，才是組合拳買點。
                  </div>
                </>
              )}
            </Card>

            <Card title="賣出確認（紙上追蹤：明日開盤賣出，賣出後自動移除）" icon={LogOut} style={{ marginTop: 12 }}>
              {!sells.length ? <div className="dim">目前沒有賣出確認。</div> : (
                <div className="table-wrap">
                  <table className="data">
                    <thead><tr><th className="left">燈號</th><th className="left">股票</th><th className="left">買進日</th><th>買價</th><th>現價（即時）</th><th>報酬</th><th className="left">為什麼賣</th></tr></thead>
                    <tbody>
                      {sells.map((p) => (
                        <tr key={p.stock_id} className="err clickable" onClick={() => navigate(`/stock/${p.stock_id}`)}>
                          <td className="left"><Light color="red" label="賣出確認" sub="明日開盤賣" /></td>
                          <td className="left"><StockCell p={p} /></td>
                          <td className="left num">{fmtDate(p.entry_date)}</td>
                          <td className="num">{p.entry_price}</td>
                          <td><LivePrice q={q[p.stock_id]} /></td>
                          <td>{q[p.stock_id]?.price && p.entry_price ? <Pct v={q[p.stock_id].price / p.entry_price - 1} /> : <Pct v={p.ret} />}</td>
                          <td className="left" style={{ fontSize: 12 }}><Reasons r={p.sell_reasons} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>

            <Card title={`持有中（晨報推薦過的，${holding.length} 檔）`} icon={Wallet} style={{ marginTop: 12 }}>
              {!holding.length ? <div className="dim">目前沒有。晨報推薦的組合拳候選，進場後會出現在這裡，直到賣出確認。</div> : <><LiveBanner live={rt} /><HoldingTable rows={holding} quotes={q} /></>}
            </Card>

            <Card title="最近結案（已賣出）" icon={History} style={{ marginTop: 12 }}>
              {!closed.length ? <div className="dim">還沒有結案紀錄（上線後推薦過的股票賣出後會列在這裡）。</div> : (
                <div className="table-wrap" style={{ maxHeight: 420 }}>
                  <table className="data">
                    <thead><tr><th className="left">股票</th><th className="left">買進</th><th className="left">賣出</th><th>持有日</th><th>報酬(扣成本)</th><th className="left">賣出理由</th></tr></thead>
                    <tbody>
                      {closed.map((p) => (
                        <tr key={p.stock_id + p.signal_date}>
                          <td className="left"><StockCell p={p} /></td>
                          <td className="left num">{fmtDate(p.entry_date)} @ {p.entry_price}</td>
                          <td className="left num">{fmtDate(p.exit_date)} @ {p.exit_price}</td>
                          <td className="num">{p.days}</td>
                          <td><Pct v={p.ret} /></td>
                          <td className="left" style={{ fontSize: 12 }}><Reasons r={p.sell_reasons} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>

            {(replay.length > 0 || replayClosed.length > 0) && (
              <details className="card" style={{ marginTop: 12 }}>
                <summary style={{ cursor: "pointer", fontWeight: 700 }}>
                  參考：上線前回放的模擬部位（{replay.length} 檔持有、{replayClosed.length} 筆結案；不是推薦、也不是你的持股）
                </summary>
                <div className="dim" style={{ margin: "8px 0", fontSize: 12 }}>
                  用最近 40 個交易日的歷史資料模擬「如果當時照規則買」的結果，只用來觀察規則的表現；晨報不會針對這些股票提醒。
                </div>
                {replay.length > 0 && <HoldingTable rows={replay} />}
              </details>
            )}
          </>
        )}
    </div>
  );
}
