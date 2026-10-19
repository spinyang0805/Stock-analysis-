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
  return <><b className="num">{p.stock_id}</b> {p.name}{p.in_rank ? " ⭐" : ""}</>;
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

  const pos = data?.positions || [];
  const newIds = new Set(data?.new_today || []);
  const fresh = pos.filter((p) => newIds.has(p.stock_id) && p.status === "waiting");
  const sells = pos.filter((p) => p.status === "sell_signal");
  const holding = pos.filter((p) => p.status === "holding");
  const closed = (data?.closed || []).filter((c) => c.status === "closed").slice(0, 40);
  const sm = data?.summary || {};

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
          這裡是紙上追蹤，不是下單建議；回測 2020–2026 每筆平均贏股票池約 1.8%，但最近半年只打平、輸 0050。
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
              <Kpi label="已結案" value={sm.n ?? 0} sub={sm.n ? `勝率 ${Math.round(sm.win * 100)}%・平均 ${signed(sm.mean * 100, 1, "%")}` : "–"} />
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

            <Card title="賣出確認（明日開盤賣出，賣出後自動移除）" icon={LogOut} style={{ marginTop: 12 }}>
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

            <Card title={`持有中（${holding.length} 檔）`} icon={Wallet} style={{ marginTop: 12 }}>
              {!holding.length ? <div className="dim">目前沒有持有中的股票。</div> : <HoldingTable rows={holding} />}
            </Card>

            <Card title="最近結案（已賣出）" icon={History} style={{ marginTop: 12 }}>
              {!closed.length ? <div className="dim">還沒有結案紀錄。</div> : (
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
              <div className="dim" style={{ marginTop: 8, fontSize: 12 }}>
                第一次上線時回放了最近 40 個交易日：還在漲的會繼續持有，所以早期結案多半是停損的，平均報酬會偏低，要看長期累積才準。
              </div>
            </Card>
          </>
        )}
    </div>
  );
}
