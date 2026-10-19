// 組合拳訊號：買進候選 → 持有 → 賣出確認 → 移除（紙上追蹤，每個交易日 19:00 更新）
// 資料：public/data/combo_signals.json（backend/tuning/combo_daily.py）
import React, { useEffect, useMemo, useState } from "react";
import { History, LogOut, Sparkles, Wallet } from "lucide-react";
import { Card, Empty, Kpi } from "../components/ui.jsx";
import { SortTh, useSort } from "../components/SortableTh.jsx";
import { fetchStaticJson } from "../lib/data.js";
import { signed, trendColor } from "../lib/format.js";
import { navigate } from "../lib/router.js";

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

function HoldingTable({ rows }) {
  const cols = useMemo(() => ({
    id: (p) => p.stock_id, entry: (p) => p.entry_date, days: (p) => p.days, ret: (p) => p.ret,
  }), []);
  const { sorted, sort, toggle } = useSort(rows, cols, { key: "ret", dir: "desc" });
  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            {[["id", "股票", true], ["entry", "買進日", true], ["days", "持有日"], ["ret", "報酬"]]
              .map(([k, label, left]) => <SortTh key={k} k={k} label={label} left={left} sort={sort} onSort={toggle} />)}
            <th>買價</th><th>收盤</th><th className="left">買進理由</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((p) => (
            <tr key={p.stock_id + p.signal_date} className="clickable" onClick={() => navigate(`/stock/${p.stock_id}`)}>
              <td className="left"><StockCell p={p} /></td>
              <td className="left num">{fmtDate(p.entry_date)}</td>
              <td className="num">{p.days ?? "–"}</td>
              <td><Pct v={p.ret} /></td>
              <td className="num">{p.entry_price ?? "–"}</td>
              <td className="num">{p.last_close ?? "–"}</td>
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
              <Kpi label="今日買進候選" value={fresh.length} sub="明日開盤進場" />
              <Kpi label="賣出確認" value={sells.length} sub="明日開盤賣出" tone={sells.length ? "down" : undefined} />
              <Kpi label="追蹤持有中" value={holding.length} sub="直到賣出確認" />
              <Kpi label="已結案" value={closed.length} sub={closed.length ? `勝率 ${Math.round(closed.filter((c) => c.ret > 0).length / closed.length * 100)}%` : "上線後累積"} />
            </div>

            <Card title="今日買進候選（明日開盤進場）" icon={Sparkles} style={{ marginTop: 12 }}>
              {!fresh.length ? <div className="dim">今天沒有股票通過四關。</div> : (
                <div className="table-wrap">
                  <table className="data">
                    <thead><tr><th className="left">股票</th><th>收盤</th><th className="left">為什麼買（各關通過的訊號）</th></tr></thead>
                    <tbody>
                      {fresh.map((p) => (
                        <tr key={p.stock_id} className="clickable" onClick={() => navigate(`/stock/${p.stock_id}`)}>
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
                <div className="table-wrap">
                  <table className="data">
                    <thead><tr><th className="left">股票</th><th>昨收</th><th>突破價</th><th>距離</th><th className="left">已通過的關卡</th></tr></thead>
                    <tbody>
                      {watch.map((w) => (
                        <tr key={w.stock_id} className="clickable" onClick={() => navigate(`/stock/${w.stock_id}`)}>
                          <td className="left"><StockCell p={w} /></td>
                          <td className="num">{w.close}</td>
                          <td className="num">{w.level}</td>
                          <td><Pct v={w.dist} /></td>
                          <td className="left" style={{ fontSize: 12 }}><Reasons r={w.buy_reasons} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>

            <Card title="賣出確認（紙上追蹤：明日開盤賣出，賣出後自動移除）" icon={LogOut} style={{ marginTop: 12 }}>
              {!sells.length ? <div className="dim">目前沒有賣出確認。</div> : (
                <div className="table-wrap">
                  <table className="data">
                    <thead><tr><th className="left">股票</th><th className="left">買進日</th><th>買價</th><th>收盤</th><th>報酬</th><th className="left">為什麼賣</th></tr></thead>
                    <tbody>
                      {sells.map((p) => (
                        <tr key={p.stock_id} className="err clickable" onClick={() => navigate(`/stock/${p.stock_id}`)}>
                          <td className="left"><StockCell p={p} /></td>
                          <td className="left num">{fmtDate(p.entry_date)}</td>
                          <td className="num">{p.entry_price}</td>
                          <td className="num">{p.last_close}</td>
                          <td><Pct v={p.ret} /></td>
                          <td className="left" style={{ fontSize: 12 }}><Reasons r={p.sell_reasons} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>

            <Card title={`持有中（晨報推薦過的，${holding.length} 檔）`} icon={Wallet} style={{ marginTop: 12 }}>
              {!holding.length ? <div className="dim">目前沒有。晨報推薦的組合拳候選，進場後會出現在這裡，直到賣出確認。</div> : <HoldingTable rows={holding} />}
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
