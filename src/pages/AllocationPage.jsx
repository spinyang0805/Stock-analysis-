// 資產配置：實盤政策 C10（50% 0050 ／ 50% 現金，每季調整）+ 前向紙上測試
// 資料：public/data/policy_paper.json（policy-paper.yml 每個交易日 19:10 更新）
// 計算機的輸入只存在本機瀏覽器（localStorage），不會送出到任何地方
import React, { useEffect, useMemo, useState } from "react";
import { Calculator, FlaskConical, Scale, Lock } from "lucide-react";
import { Card, Empty } from "../components/ui.jsx";
import { fetchStaticJson } from "../lib/data.js";
import { signed, trendColor } from "../lib/format.js";
import "./AllocationPage.css";

// [白話名稱, 一句話說明]（內部代號 C10/C1/C8/VT15 不顯示給使用者）
const LINE_INFO = {
  C10: ["基準做法：半股半現金", "一半放 0050、一半放現金，每季調整一次（實際採用）"],
  C1: ["每月調整版", "同樣半股半現金，但每月調整一次"],
  C8: ["下跌保護版", "0050 從高點跌 10% 就減到 4 成，回到高點 95% 才加回"],
  VT15: ["依波動調整版", "市場震盪大就少放股票、平靜就多放，每週調整"],
  BH: ["全買 0050 不動", "對照組：全部買 0050、一直持有"],
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
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto" }} role="img" aria-label="各做法淨值走勢">
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
  const act = Math.abs(diff) < 1 ? null : diff > 0 ? "buy" : "sell";
  return (
    <Card title="試算：我該買或賣多少？" icon={Calculator}>
      <p className="alloc-sub" style={{ marginBottom: 12 }}>填入你現在的持有金額，下方會直接告訴你要買進或賣出多少。</p>
      <div className="alloc-form">
        <div>
          <div className="alloc-group-title">1. 你現在持有的股票 ETF</div>
          <div className="alloc-group">
            {ETFS.map(([k, label]) => (
              <label key={k} className="alloc-field"><span>{label} 目前市值</span>
                <input className="input" inputMode="numeric" placeholder="0" value={vals[k]} onChange={(e) => change(k, e.target.value)} />
                <small>單位：元；沒買的留空</small>
              </label>
            ))}
          </div>
        </div>
        <div>
          <div className="alloc-group-title">2. 現金與目標</div>
          <div className="alloc-group">
            <label className="alloc-field"><span>現金</span>
              <input className="input" inputMode="numeric" placeholder="0" value={vals.cash} onChange={(e) => change("cash", e.target.value)} />
              <small>單位：元；活存、定存都算</small>
            </label>
            <label className="alloc-field"><span>股票想佔總資產幾 %</span>
              <input className="input" inputMode="numeric" value={vals.target} onChange={(e) => change("target", e.target.value)} />
              <small>預設 50。請事先決定，不要因漲跌改動</small>
            </label>
          </div>
        </div>
        {w == null ? <div className="alloc-result is-empty">輸入金額後，這裡會顯示建議。</div> : (
          <div className="alloc-result" aria-live="polite">
            <div className={`alloc-result-action ${act || ""}`}>
              {!act ? "目前剛好在目標，不用動。"
                : act === "buy" ? <>用現金買進約 <span className="num">{money(diff)}</span> 元股票 ETF</>
                  : <>賣出約 <span className="num">{money(-diff)}</span> 元股票 ETF，換成現金</>}
            </div>
            <div className="alloc-result-hint">
              {act === "buy" ? "三檔任選一檔或分開買都可以。" : act === "sell" ? "從持有的三檔中挑一檔或分開賣都可以。" : "維持現況即可。"}
            </div>
            <div className="alloc-stats">
              <div><small>股票 ETF 合計</small><b className="num">{money(s)} 元</b></div>
              <div><small>目前股票比例</small><b className="num">{(w * 100).toFixed(1)}%</b></div>
              <div><small>目標比例</small><b className="num">{(t * 100).toFixed(0)}%</b></div>
              <div><small>與目標差</small><b className="num">{signed((w - t) * 100, 1, " 個百分點")}</b></div>
            </div>
            <div className="dim" style={{ marginTop: 10 }}>未含手續費與證交稅；請在決定日之後的下一個開盤日調整。</div>
          </div>
        )}
        <details className="alloc-fold">
          <summary>股票比例調高或調低，風險差多少？</summary>
          <div className="alloc-fold-body">
            <p>股票每多 10%，長期年化報酬約多 1.3%，但遇到 2008 年那種大崩盤時，最大跌幅會深約 6 個百分點。</p>
            <table className="alloc-risk">
              <thead><tr><th>股票比例</th><th>2008 型最大跌幅</th><th>歷史年化</th></tr></thead>
              <tbody>
                <tr><td>40%</td><td>約 −28%</td><td>5.8%</td></tr>
                <tr><td>50%（預設）</td><td>約 −34%</td><td>7.1%</td></tr>
                <tr><td>55%</td><td>約 −37%</td><td>7.8%</td></tr>
                <tr><td>100%</td><td>約 −58%</td><td>13.8%</td></tr>
              </tbody>
            </table>
            <p style={{ marginTop: 8 }}>009816 上市未滿一年，尚無崩盤時期的歷史可驗證。</p>
          </div>
        </details>
        <div className="alloc-privacy">
          <Lock size={13} aria-hidden="true" style={{ marginTop: 3, flex: "none" }} />
          <span>輸入的數字只存在你這台瀏覽器（localStorage），不會傳到任何伺服器，也不會寫進網站資料。</span>
        </div>
      </div>
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
          <div className="dim">一半放 0050、一半放現金，每季調整一次</div>
        </div>
      </div>

      <div className="alloc-stack">
        <section className="card alloc-hero">
          <div className="alloc-hero-label">本季建議</div>
          <div className="alloc-hero-main">資產的 <em>一半買 0050</em>（或 006208、009816），<em>另一半留現金</em></div>
          <p className="alloc-sub">不看盤、不預測，只在固定日期檢查比例，偏掉了就買或賣回來。</p>
          <div className="alloc-hero-meta">
            <div><small>下次檢查日（收盤後決定）</small><b className="num">{fmtIso(next)}</b></div>
            <div><small>執行時間</small><b>檢查日隔天開盤</b></div>
          </div>
        </section>

        <Card title="怎麼做（4 步驟）" icon={Scale}>
          <ol className="alloc-steps">
            <li>每年 3、6、9、12 月最後一個交易日收盤後，用下方試算填入持有金額。</li>
            <li>下一個交易日開盤，依試算結果買進或賣出股票 ETF。</li>
            <li>剩下的現金放在有存款保險的台幣存款，不另外投資。</li>
            <li>兩次檢查之間不動，不追高殺低。</li>
          </ol>
          <details className="alloc-fold" style={{ marginTop: 12 }}>
            <summary>預期報酬與最壞情況</summary>
            <div className="alloc-fold-body">
              <p>歷史年化約 7.1%（2004–2026，稅前、現金利息以 0% 計）。</p>
              <p>最大跌幅：2008 年型約 −34%，2000–01 年型約 −37%；若出現比 2008 年更嚴重的崩盤，可能到 −36～−40%（超過原訂 35% 上限）。</p>
              <p>檢討條件：只在每季檢查日處理比例偏離，其餘時間不動。日期以證交所行事曆為準。</p>
            </div>
          </details>
        </Card>

        <Calculator50 />

        <Card title="紙上測試：同時比較幾種做法" icon={FlaskConical}
          right={data?.updated ? `資料至 ${data.updated.slice(0, 4)}/${data.updated.slice(4, 6)}/${data.updated.slice(6)}` : ""}>
          <p className="alloc-sub">不動用真錢，每天記錄「如果照這幾種做法」會怎樣，至少看 60 個月。「下跌保護版」要明顯勝過「基準做法」才會考慮換。</p>
          {data === undefined ? <div className="dim" style={{ marginTop: 8 }}>載入中…</div>
            : !lines ? <Empty icon={FlaskConical} title="紙上測試自 2026-10-21 開始，尚無資料">規則已固定，不重來、不調參數。</Empty>
              : (
                <>
                  <div className="alloc-cards">
                    {order.map((k) => (
                      <div key={k} className="alloc-pcard" style={{ borderLeftColor: COLORS[k] }}>
                        <h4>{LINE_INFO[k][0]}</h4>
                        <p>{LINE_INFO[k][1]}</p>
                        <div className="alloc-pcard-ret"><Pct v={lines[k].total_return} /></div>
                        <dl>
                          <dt>目前 0050 比例</dt><dd className="num">{(lines[k].weight * 100).toFixed(0)}%</dd>
                          <dt>最大跌幅</dt><dd><Pct v={lines[k].max_drawdown} /></dd>
                          <dt>年化（滿一年才算）</dt><dd><Pct v={lines[k].cagr} /></dd>
                        </dl>
                      </div>
                    ))}
                  </div>
                  <details className="alloc-fold" style={{ marginTop: 12 }}>
                    <summary>看走勢圖</summary>
                    <div className="alloc-fold-body">
                      <div className="alloc-legend">{order.map((k) => <span key={k}><span style={{ color: COLORS[k] }}>●</span> {LINE_INFO[k][0]}</span>)}</div>
                      <LinesChart lines={Object.fromEntries(order.map((k) => [k, lines[k]]))} />
                    </div>
                  </details>
                  <div className="dim" style={{ marginTop: 8 }}>
                    起算 2026-10-20 收盤，首次買進 2026-10-21 開盤；已含手續費、證交稅、滑價，現金利息 0%，0050 配息併入現金。
                  </div>
                </>
              )}
        </Card>

        <div className="alloc-disclaimer">僅供本人研究參考，非投資建議。</div>
      </div>
    </div>
  );
}
