// 每月排名（紙上模擬）：強勢股＋營收成長前 20 名，每月換榜，只記錄成績、不下單
// 資料：public/data/rank_paper.json（rank-paper.yml 每個交易日 19:00 更新）
import React, { useEffect, useMemo, useState } from "react";
import { History, Trophy } from "lucide-react";
import { Card, Empty, Kpi } from "../components/ui.jsx";
import { SortTh, useSort } from "../components/SortableTh.jsx";
import { fetchStaticJson, loadStockList } from "../lib/data.js";
import { signed, trendColor } from "../lib/format.js";
import { navigate } from "../lib/router.js";

const fmtDate = (d) => (d ? `${d.slice(0, 4)}/${d.slice(4, 6)}/${d.slice(6)}` : "–");

function Pct({ v }) {
  if (v == null) return <span className="dim">–</span>;
  return <span className="num" style={{ color: trendColor(v) }}>{signed(v * 100, 1, "%")}</span>;
}

// 各期依序連乘；未結束的期間以最新收盤計
function compound(periods, key) {
  const done = periods.filter((p) => p[key] != null);
  return done.length ? done.reduce((s, p) => s * (1 + p[key]), 1) - 1 : null;
}

export default function RankPage() {
  const [data, setData] = useState(undefined);
  const [names, setNames] = useState({});
  useEffect(() => {
    fetchStaticJson("/rank_paper.json").then(setData);
    loadStockList().then((l) => setNames(Object.fromEntries(l.map((s) => [s.code, String(s.name).replace("股份有限公司", "")]))));
  }, []);

  const periods = data?.periods || [];
  const cur = periods[periods.length - 1];
  const priced = periods.filter((p) => p.net != null);
  const tot = { list: compound(priced, "net"), b0050: compound(priced, "b0050"), b006208: compound(priced, "b006208") };

  const rows = useMemo(() => (cur?.picks || []).map((q, i) => ({ ...q, rank: i + 1 })), [cur]);
  const cols = useMemo(() => ({
    rank: (r) => r.rank, id: (r) => r.id, mom: (r) => r.mom, rev: (r) => r.rev, ret: (r) => r.ret,
  }), []);
  const { sorted, sort, toggle } = useSort(rows, cols, { key: "rank", dir: "asc" });

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">MONTHLY RANKING · PAPER</div>
          <h1 className="page-title">每月排名（紙上模擬）</h1>
          <div className="dim">{data?.updated ? `資料至 ${fmtDate(data.updated)}` : ""}</div>
        </div>
      </div>

      <Card>
        <div className="dim" style={{ lineHeight: 1.7 }}>
          每月第一個交易日收盤後，把成交量前 500 大的股票依「過去一年漲幅（不算最近一個月）」和「最新月營收年增率」合併排名，
          取前 20 名、每檔一樣多錢，隔天開盤起算，持有到下個月換榜（換掉的股票扣手續費與證交稅）。
          <b>這裡只記錄成績、不是買進建議。</b>回測（2020–2026）一年約 +30%，0050 約 +29.5%，差距還不算數，
          所以先觀察半年，看實際表現是否跟回測一樣。
        </div>
      </Card>

      {data === undefined ? <Card style={{ marginTop: 12 }}><div className="dim">載入中…</div></Card>
        : !cur ? (
          <Card style={{ marginTop: 12 }}>
            <Empty icon={Trophy} title="還沒有排名紀錄">第一份清單會在下一個交易日晚上 19:00 後產生。</Empty>
          </Card>
        ) : (
          <>
            <div className="kpis" style={{ marginTop: 12 }}>
              <Kpi label="清單累計（扣成本）" value={<Pct v={tot.list} />} sub={`${priced.length} 期`} />
              <Kpi label="0050 同期間" value={<Pct v={tot.b0050} />} sub="含配息" />
              <Kpi label="006208 同期間" value={<Pct v={tot.b006208} />} sub="含配息" />
              <Kpi label="贏 0050" value={tot.list == null || tot.b0050 == null ? "–" : <Pct v={tot.list - tot.b0050} />}
                sub={`${priced.filter((p) => p.b0050 != null && p.net > p.b0050).length} / ${priced.filter((p) => p.b0050 != null).length} 期勝出`} />
            </div>

            <Card title={`本期清單（${fmtDate(cur.as_of)} 排名）`} icon={Trophy} style={{ marginTop: 12 }}
              right={cur.status === "waiting" ? "明日開盤起算" : `${fmtDate(cur.entry)} 起・本期 ${cur.net == null ? "–" : signed(cur.net * 100, 1, "%")}`}>
              <div className="table-wrap">
                <table className="data">
                  <thead>
                    <tr>
                      {[["rank", "名次"], ["id", "股票", true], ["mom", "過去一年漲幅"], ["rev", "月營收年增"], ["ret", "本期報酬"]]
                        .map(([k, label, left]) => <SortTh key={k} k={k} label={label} left={left} sort={sort} onSort={toggle} firstDir={k === "rank" ? "asc" : undefined} />)}
                    </tr>
                  </thead>
                  <tbody>
                    {sorted.map((q) => (
                      <tr key={q.id} className="clickable" onClick={() => navigate(`/stock/${q.id}`)}>
                        <td className="num">{q.rank}</td>
                        <td className="left"><b className="num">{q.id}</b> {names[q.id] || ""}</td>
                        <td><Pct v={q.mom} /></td>
                        <td><Pct v={q.rev == null ? null : q.rev / 100} /></td>
                        <td><Pct v={q.ret} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>

            <Card title="歷期成績" icon={History} style={{ marginTop: 12 }}>
              <div className="table-wrap">
                <table className="data">
                  <thead><tr><th className="left">排名日</th><th className="left">持有期間</th><th>清單（扣成本）</th><th>0050</th><th>006208</th><th>贏 0050</th><th className="left">狀態</th></tr></thead>
                  <tbody>
                    {[...periods].reverse().map((p) => (
                      <tr key={p.as_of}>
                        <td className="left num">{fmtDate(p.as_of)}</td>
                        <td className="left num">{p.entry ? `${fmtDate(p.entry)} ～ ${fmtDate(p.exit || p.marked)}` : "–"}</td>
                        <td><Pct v={p.net} /></td>
                        <td><Pct v={p.b0050} /></td>
                        <td><Pct v={p.b006208} /></td>
                        <td><Pct v={p.net == null || p.b0050 == null ? null : p.net - p.b0050} /></td>
                        <td className="left">{p.status === "closed" ? "已結束" : p.status === "open" ? "進行中" : "待進場"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          </>
        )}
    </div>
  );
}
