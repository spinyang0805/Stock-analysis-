// 進出場頁籤：主建議、存股估值區間、波段條件與價位、規則回測、AI 白話解說
import React, { useMemo, useState } from "react";
import { Bot, CheckCircle2, Circle, Compass, FlaskConical, PiggyBank, TrendingUp } from "lucide-react";
import { Badge, Card, Seg } from "./ui.jsx";
import { backtestSwing, strategyNarrative } from "../lib/strategy.js";
import { explainWithAI } from "../lib/market.js";
import { fmt, signed, trendColor } from "../lib/format.js";
import { useApp } from "../lib/appState.jsx";

const toneClass = { up: "up", down: "down", flat: "" };

function Level({ color, label, price, note, current }) {
  const diff = Number.isFinite(current) && Number.isFinite(price) && price ? ((current - price) / price) * 100 : NaN;
  return (
    <div className="level">
      <i className="tag" style={{ background: color }} aria-hidden="true" />
      <div>
        <div style={{ fontWeight: 700 }}>{label}</div>
        {note && <div className="dim">{note}</div>}
      </div>
      <div style={{ textAlign: "right" }}>
        <div className="num" style={{ fontWeight: 800, fontSize: 16 }}>{fmt(price)}</div>
        {Number.isFinite(diff) && <div className="dim num">現價{diff >= 0 ? "高" : "低"} {Math.abs(diff).toFixed(1)}%</div>}
      </div>
    </div>
  );
}

export default function StrategyPanel({ strategy, longRows, name, code, loadingExtra }) {
  const app = useApp();
  const [ai, setAi] = useState({ text: "", loading: false, error: "" });
  const bt = useMemo(() => (longRows?.length > 200 ? backtestSwing(longRows.slice(-1250)) : null), [longRows]);

  if (!strategy) return <Card><div className="dim">資料不足（需 30 日以上 K 線）</div></Card>;
  const s = strategy;
  const narrative = strategyNarrative(s, name);

  async function runAI() {
    setAi({ text: "", loading: true, error: "" });
    const prompt = `股票：${code} ${name}\n使用者標記：${s.style}${s.holding ? "（已持有）" : "（未持有）"}\n\n規則引擎結果：\n${narrative}\n\n波段條件：\n${s.swing.conditions.map((c) => `${c.ok ? "✔" : "✘"} ${c.label}（${c.detail}）`).join("\n")}${s.value ? `\n\n估值方法：\n${s.value.methods.map((m) => `${m.name}：${m.basis}`).join("\n")}` : ""}${bt ? `\n\n近五年波段規則回測：${bt.count} 筆，勝率 ${bt.winRate.toFixed(0)}%，平均每筆 ${bt.avgRet.toFixed(1)}%` : ""}\n\n請用 5～8 句白話說明：現在該做什麼、為什麼、什麼價位要調整，以及主要風險。`;
    const r = await explainWithAI(prompt);
    setAi({ text: r.text || "", loading: false, error: r.error || "" });
  }

  return (
    <div className="grid" style={{ gap: 12 }}>
      <Card title="主建議" icon={Compass} right={app.user && (
        <Seg label="投資風格" options={["存股", "波段"]} value={s.style} onChange={(v) => app.setStyle(code, v)} />
      )}>
        <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
          <span className={`num ${toneClass[s.primary.tone]}`} style={{ fontSize: 28, fontWeight: 900 }}>{s.primary.label}</span>
          <Badge tone="blue">{s.style}</Badge>
          {s.holding ? <Badge>已持有</Badge> : <Badge>未持有</Badge>}
        </div>
        <div style={{ marginTop: 6 }} className="muted">{s.primary.why}</div>
        {!app.user && <div className="notice" style={{ marginTop: 10 }}>登入後可把這檔標記為「存股」或「波段」，並依你的成本計算停損與殖利率。</div>}
        <div className="conclusion pre" style={{ borderLeftColor: "var(--accent)" }}>{narrative}</div>
        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
          <button type="button" className="btn primary" onClick={runAI} disabled={ai.loading || !app.user}>
            <Bot size={16} />{ai.loading ? "AI 解說中…" : "AI 白話解說"}
          </button>
          {!app.user && <span className="dim">AI 解說需登入</span>}
        </div>
        {ai.error && <div className="notice err" style={{ marginTop: 10 }}>{ai.error}</div>}
        {ai.text && <div className="notice pre" style={{ marginTop: 10 }}>{ai.text}</div>}
      </Card>

      <div className="grid two">
        <Card title="存股：估值區間" icon={PiggyBank} right={s.value?.action && <span className={toneClass[s.value.action.tone]} style={{ fontWeight: 800 }}>{s.value.action.label}</span>}>
          {!s.value ? <div className="dim">{loadingExtra ? "股利與 EPS 資料載入中…" : "缺少股利／EPS 資料，無法估值（ETF 或新上市股常見）"}</div> : (
            <>
              <div className="level-list">
                <Level color="#38bdf8" label="便宜價" price={s.value.cheap} current={s.price} note="分批加碼區" />
                <Level color="#facc15" label="合理價" price={s.value.fair} current={s.price} note="可買進、續抱" />
                <Level color="#f97316" label="昂貴價" price={s.value.expensive} current={s.price} note="分批減碼區" />
              </div>
              <div style={{ marginTop: 10 }}>
                <div className="dim" style={{ marginBottom: 4 }}>分批加碼計畫</div>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {s.value.batches.map((b) => <Badge key={b.label} tone="blue">{b.label} {fmt(b.price)}</Badge>)}
                  <Badge tone="warn">減碼 {fmt(s.value.reduce[0])}／{fmt(s.value.reduce[1])}</Badge>
                </div>
              </div>
              {(s.value.yieldNow || s.value.yieldOnCost) && (
                <div style={{ marginTop: 10, display: "flex", gap: 16, flexWrap: "wrap", fontSize: 14 }}>
                  {s.value.yieldNow && <span>現價殖利率 <b className="num">{s.value.yieldNow.toFixed(2)}%</b></span>}
                  {s.value.yieldOnCost && <span>成本殖利率 <b className="num up">{s.value.yieldOnCost.toFixed(2)}%</b></span>}
                </div>
              )}
              <div className="dim" style={{ marginTop: 10, display: "grid", gap: 2 }}>
                {s.value.methods.map((m) => <span key={m.name}>{m.name}：{m.basis}</span>)}
              </div>
            </>
          )}
        </Card>

        <Card title="波段：進出場價位" icon={TrendingUp} right={<span className={toneClass[s.swing.action.tone]} style={{ fontWeight: 800 }}>{s.swing.action.label}</span>}>
          <div className="level-list">
            <Level color="#a78bfa" label="突破買點" price={s.swing.breakout} current={s.price} note={`近 20 日高 ${fmt(s.swing.high20)} + 0.1 ATR`} />
            <Level color="#38bdf8" label="拉回買點" price={s.swing.pullback[1]} current={s.price} note={`月線 ${fmt(s.swing.pullback[0])}～+0.5 ATR`} />
            <Level color={trendColor(-1)} label="停損" price={s.swing.stop} current={s.price} note={s.holding ? `成本−2ATR ${fmt(s.swing.initialStop)}、移動停利 ${fmt(s.swing.trailStop)} 取高` : "進場價 − 2 ATR"} />
            <Level color="#f97316" label="目標 1／目標 2" price={s.swing.target1} current={s.price} note={`3 ATR／6 ATR（${fmt(s.swing.target2)}）`} />
          </div>
          <div className="dim" style={{ marginTop: 8 }}>ATR(14) = {fmt(s.swing.atr)}：近 14 日平均真實波動，停損抓 2 倍避免被正常波動洗出場</div>
        </Card>
      </div>

      <div className="grid two">
        <Card title={`波段條件（${s.swing.score}/6）`} icon={CheckCircle2}>
          {s.swing.conditions.map((c) => (
            <div className="check" key={c.key}>
              {c.ok ? <CheckCircle2 size={18} color="var(--up)" aria-label="成立" /> : <Circle size={18} color="var(--fg-4)" aria-label="不成立" />}
              <div>
                <div style={{ fontWeight: c.ok ? 700 : 500, color: c.ok ? "var(--fg)" : "var(--fg-3)" }}>{c.label}</div>
                <div className="dim num">{c.detail}</div>
              </div>
            </div>
          ))}
        </Card>

        <Card title="波段規則回測（近 5 年日K）" icon={FlaskConical}>
          {!bt ? <div className="dim">{loadingExtra ? "長期歷史載入中…" : "歷史資料不足"}</div> : (
            <>
              <div className="kpis" style={{ gridTemplateColumns: "repeat(2, 1fr)" }}>
                <div className="kpi"><div className="label">交易次數</div><div className="value num">{bt.count}</div></div>
                <div className="kpi"><div className="label">勝率</div><div className="value num">{Number.isFinite(bt.winRate) ? `${bt.winRate.toFixed(0)}%` : "--"}</div></div>
                <div className="kpi"><div className="label">平均每筆</div><div className="value num" style={{ color: trendColor(bt.avgRet) }}>{signed(bt.avgRet, 1, "%")}</div></div>
                <div className="kpi"><div className="label">策略累積 vs 買進持有</div><div className="value num" style={{ fontSize: 16 }}><span style={{ color: trendColor(bt.totalRet) }}>{signed(bt.totalRet, 0, "%")}</span> / <span style={{ color: trendColor(bt.buyHold) }}>{signed(bt.buyHold, 0, "%")}</span></div></div>
              </div>
              <div className="table-wrap" style={{ maxHeight: 200, marginTop: 10 }}>
                <table className="data">
                  <thead><tr><th className="left">進場</th><th>出場</th><th>進價</th><th>出價</th><th>報酬</th></tr></thead>
                  <tbody>
                    {[...bt.trades].reverse().slice(0, 12).map((t) => (
                      <tr key={t.entryDate}>
                        <td className="left num">{t.entryDate}</td><td className="num">{t.open ? "持有中" : t.exitDate}</td>
                        <td>{fmt(t.entry)}</td><td>{fmt(t.exit)}</td>
                        <td style={{ color: trendColor(t.ret) }}>{signed(t.ret, 1, "%")}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="dim" style={{ marginTop: 6 }}>{bt.from}～{bt.to}；收盤價進出、未計手續費與稅，僅供參考。</div>
            </>
          )}
        </Card>
      </div>
      <div className="dim">本頁為規則引擎量化結果與 AI 解說，不構成投資建議；請依自身風險承受度判斷。</div>
    </div>
  );
}
