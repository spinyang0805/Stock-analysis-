// 資產配置：實盤政策 C10（50% 0050 ／ 50% 現金，每季調整）+ 前向紙上測試
// 資料：public/data/policy_paper.json（policy-paper.yml 每個交易日 19:10 更新）
// 計算機的輸入只存在本機瀏覽器（localStorage），不會送出到任何地方
import React, { useEffect, useMemo, useState } from "react";
import { Calculator, FlaskConical, Scale } from "lucide-react";
import { Card, Empty } from "../components/ui.jsx";
import { fetchStaticJson } from "../lib/data.js";
import { signed, trendColor } from "../lib/format.js";

const LINE_INFO = {
  C10: ["C10", "實盤政策：50/50 每季調整"],
  C1: ["C1", "50/50 每月調整"],
  C8: ["C8", "跌 10% 減碼至 4 成、回到 95% 高點才加回"],
  VT15: ["VT15", "每週依波動度調整，目標年化波動 15%"],
  BH: ["0050", "0050 買進持有（對照）"],
};
const COLORS = { C10: "#2563eb", C1: "#7c3aed", C8: "#dc2626", VT15: "#059669", BH: "#6b7280" };
const KEY = "allocationCalc";

const fmtIso = (d) => (d ? d.replace(/-/g, "/") : "–");
const money = (v) => Math.round(v).toLocaleString("zh-TW");

function Pct({ v, digits = 1 }) {
  if (v == null) return <span className="dim">–</span>;
  return <span className="num" style={{ color: trendColor(v) }}>{signed(v * 100, digits, "%")}</span>;
}

// 沒有資料庫的備援：每年 3、6、9、12 月最後一個週一～週五
function fallbackNextDecision() {
  const today = new Date();
  for (let k = 0; k < 15; k += 1) {
    const m = today.getMonth() + k;
    if ((m % 12) % 3 !== 2) continue;
    const d = new Date(today.getFullYear(), m + 1, 0);
    while (d.getDay() === 0 || d.getDay() === 6) d.setDate(d.getDate() - 1);
    if (d >= new Date(today.getFullYear(), today.getMonth(), today.getDate())) {
      return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
    }
  }
  return null;
}

function LinesChart({ lines }) {
  const keys = Object.keys(lines);
  const all = keys.flatMap((k) => lines[k].nav.map((p) => p[1]));
  const n = Math.max(...keys.map((k) => lines[k].nav.length));
  const lo = Math.min(...all, 1), hi = Math.max(...all, 1);
  const pad = (hi - lo) * 0.1 || 0.01;
  const W = 640, H = 220, L = 44, B = 20;
  const x = (i) => L + (n <= 1 ? 0 : (i / (n - 1)) * (W - L - 8));
  const y = (v) => 8 + (1 - (v - (lo - pad)) / (hi - lo + 2 * pad)) * (H - B - 8);
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto" }} role="img" aria-label="各組合淨值走勢">
      <line x1={L} x2={W - 8} y1={y(1)} y2={y(1)} stroke="currentColor" strokeOpacity="0.25" strokeDasharray="4 3" />
      <text x={4} y={y(1) + 4} fontSize="11" fill="currentColor" opacity="0.6">1.00</text>
      <text x={4} y={y(hi) + 4} fontSize="11" fill="currentColor" opacity="0.6">{hi.toFixed(3)}</text>
      <text x={4} y={y(lo) + 4} fontSize="11" fill="currentColor" opacity="0.6">{lo.toFixed(3)}</text>
      {keys.map((k) => (
        <polyline key={k} fill="none" stroke={COLORS[k]} strokeWidth="1.8"
          points={lines[k].nav.map((p, i) => `${x(i).toFixed(1)},${y(p[1]).toFixed(1)}`).join(" ")} />
      ))}
    </svg>
  );
}

// 股票部位可分放在追蹤台灣前 50 大的 ETF；三檔加總視為「股票」那一半
const ETFS = [["e0050", "0050 元大台灣50"], ["e006208", "006208 富邦台50"], ["e009816", "009816 凱基台灣TOP50"]];

function Calculator50() {
  const [vals, setVals] = useState(() => {
    try {
      const o = JSON.parse(localStorage.getItem(KEY) || "{}");
      if (o.e0050 == null && o.stock != null) o.e0050 = o.stock;   // 舊版只有一個 0050 欄位
      return Object.fromEntries([...ETFS.map(([k]) => k), "cash", "target"].map((k) => [k, String(o[k] ?? (k === "target" ? "50" : ""))]));
    } catch { return { e0050: "", e006208: "", e009816: "", cash: "", target: "50" }; }
  });
  const change = (which, v) => {
    const next = { ...vals, [which]: v.replace(/[^\d.]/g, "") };
    setVals(next);
    try { localStorage.setItem(KEY, JSON.stringify(next)); } catch { /* ignore */ }
  };

  const parts = ETFS.map(([k, label]) => [label, Number(vals[k]) || 0]);
  const s = parts.reduce((a, [, v]) => a + v, 0), c = Number(vals.cash) || 0, total = s + c;
  const t = Math.min(100, Math.max(0, Number(vals.target) || 0)) / 100;   // 使用者自訂目標，預設 50%
  const w = total > 0 ? s / total : null;
  const diff = total * t - s;                 // > 0 買進股票 ETF，< 0 賣出股票 ETF
  return (
    <Card title="再平衡計算機（只在本機）" icon={Calculator} style={{ marginTop: 12 }}>
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
        {ETFS.map(([k, label]) => (
          <label key={k}>{label} 市值（元）<br />
            <input className="input" inputMode="numeric" value={vals[k]} onChange={(e) => change(k, e.target.value)} />
          </label>
        ))}
        <label>目標股票比例（%）<br />
          <input className="input" inputMode="numeric" value={vals.target} onChange={(e) => change("target", e.target.value)} />
        </label>
        <label>現金（元）<br />
          <input className="input" inputMode="numeric" value={vals.cash} onChange={(e) => change("cash", e.target.value)} />
        </label>
      </div>
      {w == null ? <div className="dim" style={{ marginTop: 8 }}>輸入金額後顯示結果（沒持有的 ETF 留空即可）。</div> : (
        <div style={{ marginTop: 8, lineHeight: 1.8 }}>
          股票 ETF 合計 <b className="num">{money(s)}</b> 元，實際股票比例：<b className="num">{(w * 100).toFixed(1)}%</b>，與目標 {(t * 100).toFixed(0)}% 相差 <b className="num">{signed((w - t) * 100, 1, " 個百分點")}</b>。<br />
          {Math.abs(diff) < 1 ? "已經在目標比例，不用動。"
            : diff > 0 ? <>要回到目標：<b>用現金買進約 {money(diff)} 元的股票 ETF</b>（三檔任一或分買皆可）。</>
              : <>要回到目標：<b>賣出約 {money(-diff)} 元的股票 ETF</b>（轉成現金）。</>}
          <div className="dim">未含手續費與證交稅；只在每季決定日之後的下一個開盤調整。009816 上市未滿一年，尚無崩盤時的歷史可驗證。<br />目標比例請事先決定、不要因漲跌改動；股票每多 10%，年化約多 1.3%，但 2008 型最大跌幅約深 6 個百分點（40%：約 −28%／5.8%；50%：約 −34%／7.1%；55%：約 −37%／7.8%；100%：約 −58%／13.8%）。</div>
        </div>
      )}
      <div className="dim" style={{ marginTop: 8 }}>這些數字只存在你這台瀏覽器（localStorage），不會傳到任何伺服器或寫進網站資料。</div>
    </Card>
  );
}

export default function AllocationPage() {
  const [data, setData] = useState(undefined);
  useEffect(() => { fetchStaticJson("/policy_paper.json").then(setData); }, []);

  const lines = data?.lines && Object.keys(data.lines).length ? data.lines : null;
  const next = data?.next_c10_decision || fallbackNextDecision();
  const order = useMemo(() => ["C10", "C1", "C8", "VT15", "BH"].filter((k) => lines?.[k]), [lines]);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">ALLOCATION POLICY</div>
          <h1 className="page-title">資產配置</h1>
          <div className="dim">政策目標：50% 0050 ／ 50% 現金，每季調整（使用者 2026-10-20 同意例外啟用）</div>
        </div>
      </div>

      <Card title="下次調整決定日" icon={Scale}>
        <div style={{ fontSize: 20 }}><b className="num">{fmtIso(next)}</b>（該日收盤後決定，下一個交易日開盤執行）</div>
        <div className="dim">每年 3、6、9、12 月最後一個交易日；日期以證交所行事曆為準。</div>
        <ol style={{ lineHeight: 1.8, marginTop: 8 }}>
          <li>決定日收盤後，用下方計算機輸入 0050／006208／009816 市值與現金，算出與 50/50 的差額。</li>
          <li>下一個交易日開盤後下單：0050 多了就賣出差額，少了就用現金買進差額。</li>
          <li>剩下的現金放在受存款保險保障的台幣存款，不另外投資。</li>
          <li>兩次決定日之間不動，不看盤、不追高殺低。</li>
        </ol>
        <div style={{ lineHeight: 1.8 }}>
          <b>預期：</b>歷史年化約 7.1%（2004–2026，稅前、現金 0%）。最大跌幅：2008 型約 −34%、2000–01 型約 −37%；
          若出現比 2008 更重的崩盤，可能達 −36～−40%（超過 35% 上限）。<br />
          <b>檢討／放棄條件：</b>若 0050 比例偏離超過季調時才處理，不追高殺低。
        </div>
      </Card>

      <Calculator50 />

      <Card title="前向紙上測試（不影響實盤；至少 60 個月，C8 須勝過 C10 才考慮）" icon={FlaskConical} style={{ marginTop: 12 }}
        right={data?.updated ? `資料至 ${data.updated.slice(0, 4)}/${data.updated.slice(4, 6)}/${data.updated.slice(6)}` : ""}>
        {data === undefined ? <div className="dim">載入中…</div>
          : !lines ? <Empty icon={FlaskConical} title="前測自 2026-10-21 開始，尚無資料">規則已凍結，不重啟、不調參數。</Empty>
            : (
              <>
                <LinesChart lines={Object.fromEntries(order.map((k) => [k, lines[k]]))} />
                <div className="table-wrap">
                  <table className="data">
                    <thead>
                      <tr><th className="left">組合</th><th>累計報酬</th><th>目前 0050 比例</th><th>最大跌幅</th><th>年化（滿一年才算）</th></tr>
                    </thead>
                    <tbody>
                      {order.map((k) => (
                        <tr key={k}>
                          <td className="left"><span style={{ color: COLORS[k] }}>●</span> <b>{LINE_INFO[k][0]}</b> <span className="dim">{LINE_INFO[k][1]}</span></td>
                          <td><Pct v={lines[k].total_return} /></td>
                          <td className="num">{(lines[k].weight * 100).toFixed(0)}%</td>
                          <td><Pct v={lines[k].max_drawdown} /></td>
                          <td><Pct v={lines[k].cagr} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="dim" style={{ marginTop: 8 }}>
                  起算 2026-10-20 收盤，首次買進 2026-10-21 開盤；含手續費、證交稅、滑價，現金利息 0%，0050 配息計入現金。
                </div>
              </>
            )}
      </Card>

      <div className="dim" style={{ marginTop: 12 }}>非投資建議、僅供本人研究參考。</div>
    </div>
  );
}
