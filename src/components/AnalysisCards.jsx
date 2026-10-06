// 既有指標卡（算法、文字結論與舊版相同；版面改用設計 tokens、圖示改 lucide）
import React, { useMemo } from "react";
import {
  Activity, AlertTriangle, BarChart3, Building2, CandlestickChart, Gauge, Layers, Radar, ScanSearch, Shuffle, BookOpen, Landmark,
} from "lucide-react";
import { Bar, BarChart, Cell, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Card, Conclusion, Meter, Row } from "./ui.jsx";
import {
  detectBlackCandleAccum, detectPatterns, getChipAnalysis, getKdAnalysis, getMaStatus, getOverallScore,
  getRiskMetrics, getRsiAnalysis, getScenarios, getTechRadar, getVolPriceMatrix,
} from "../lib/analysis.js";
import { DOWN, UP, fmt, signed, trendColor } from "../lib/format.js";

export function ScoreBadge({ score }) {
  const color = score >= 65 ? UP : score >= 45 ? "#f59e0b" : DOWN;
  const label = score >= 75 ? "強勢多頭" : score >= 60 ? "偏多觀察" : score >= 45 ? "中性盤整" : score >= 30 ? "偏空觀望" : "強勢空頭";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 12 }}>
      <div className="num" style={{ fontSize: 46, fontWeight: 900, color, lineHeight: 1 }}>{score}</div>
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 17, fontWeight: 800, color }}>{label}</div>
        <div className="dim">技術 + 籌碼綜合評分</div>
        <div className="meter" style={{ marginTop: 6, maxWidth: 160 }}>
          <i style={{ width: `${score}%`, background: "linear-gradient(90deg,#22c55e,#f59e0b,#ef4444)" }} />
        </div>
      </div>
    </div>
  );
}

export function TechRadarCard({ rows, chipData }) {
  const radar = useMemo(() => getTechRadar(rows, chipData), [rows, chipData]);
  const score = useMemo(() => getOverallScore(rows, chipData), [rows, chipData]);
  if (!radar.dims.length) return null;
  const trend = score >= 75 ? "強勢多頭" : score >= 60 ? "偏多觀察" : score >= 45 ? "中性盤整" : score >= 30 ? "偏空觀望" : "強勢空頭";
  const tc = score >= 60 ? UP : score >= 45 ? "#f59e0b" : DOWN;
  return (
    <Card title="技術強度雷達" icon={Radar}>
      <ScoreBadge score={score} />
      <div className="grid" style={{ gap: 8 }}>
        {radar.dims.map((d) => (
          <div key={d.label}>
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginBottom: 3 }}>
              <span className="muted">{d.label}</span>
              <b className="num" style={{ color: d.color }}>{Math.round(d.value)}</b>
            </div>
            <Meter value={d.value} color={d.color} />
          </div>
        ))}
      </div>
      <Conclusion color={tc} text={`綜合評分 ${score} 分，當前研判：${trend}，${score >= 60 ? "技術面偏多，可積極持股。" : score >= 45 ? "多空均衡，等待突破。" : "技術面偏空，建議降低持倉。"}`} />
    </Card>
  );
}

export function MaStatusCard({ rows }) {
  const ma = useMemo(() => getMaStatus(rows), [rows]);
  const latest = rows?.at(-1) || {};
  return (
    <Card title="均線排列" icon={Layers}>
      <div style={{ textAlign: "center", marginBottom: 10 }}>
        <span style={{ fontSize: 20, fontWeight: 900, color: ma.color, padding: "4px 14px", borderRadius: 6, background: `${ma.color}22` }}>{ma.label}</span>
      </div>
      {[["MA5", "ma5", "#facc15"], ["MA10", "ma10", "#fb923c"], ["MA20", "ma20", "#38bdf8"], ["MA60", "ma60", "#a78bfa"]].map(([label, key, c]) => (
        <Row key={label} label={label} value={fmt(ma[key])} color={c} />
      ))}
      {ma.ma20 && <Row label="收盤 vs MA20" value={signed((latest.close / ma.ma20 - 1) * 100, 1, "%")} color={trendColor(latest.close - ma.ma20)} />}
      {ma.goldenCross && <Row label="訊號" value="黃金交叉" color="#f59e0b" />}
      {ma.deathCross && <Row label="訊號" value="死亡交叉" color={DOWN} />}
      <Conclusion text={ma.conclusion} color={ma.color} />
    </Card>
  );
}

export function VolPriceCard({ rows }) {
  const vol = useMemo(() => getVolPriceMatrix(rows), [rows]);
  const latest = rows?.at(-1) || {};
  const prev = rows?.at(-2) || {};
  const chg = prev.close ? ((latest.close - prev.close) / prev.close) * 100 : 0;
  const matrix = [
    { label: "量增價漲", color: UP, desc: "強勢買盤" },
    { label: "量增價跌", color: DOWN, desc: "賣壓沉重" },
    { label: "量縮價漲", color: "#f59e0b", desc: "謹慎無量" },
    { label: "量縮價跌", color: "#64748b", desc: "無力下跌" },
  ];
  return (
    <Card title="量價矩陣" icon={BarChart3}>
      <div style={{ textAlign: "center", marginBottom: 8 }}>
        <span style={{ fontSize: 16, fontWeight: 900, color: vol.color, padding: "3px 16px", borderRadius: 6, background: `${vol.color}22`, border: `1px solid ${vol.color}55` }}>{vol.type}</span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6, marginBottom: 10 }}>
        {matrix.map((m) => {
          const active = vol.type === m.label;
          return (
            <div key={m.label} style={{ padding: "7px 8px", borderRadius: 6, textAlign: "center", background: active ? `${m.color}2e` : "var(--card-2)", border: active ? `2px solid ${m.color}` : "1px solid var(--border)", opacity: active ? 1 : 0.55 }}>
              <div style={{ fontSize: 13, fontWeight: active ? 800 : 500, color: active ? m.color : "var(--fg-3)" }}>{m.label}</div>
              <div className="dim">{m.desc}</div>
            </div>
          );
        })}
      </div>
      <Row label="量比（今/5日均）" value={`${vol.volRatio.toFixed(2)}x`} color={vol.volRatio > 1.3 ? UP : vol.volRatio < 0.7 ? DOWN : undefined} />
      <Row label="今日漲跌" value={signed(chg, 2, "%")} color={trendColor(chg)} />
      <Conclusion text={vol.conclusion} color={vol.color} />
    </Card>
  );
}

export function PatternCard({ rows }) {
  const pat = useMemo(() => detectPatterns(rows), [rows]);
  const typeColor = { bull: UP, bear: DOWN, neutral: "#94a3b8" };
  const cc = pat.bullCount > 0 && pat.bearCount === 0 ? UP : pat.bearCount > 0 ? DOWN : "#94a3b8";
  return (
    <Card title="型態偵測" icon={ScanSearch}>
      <div className="grid" style={{ gap: 6 }}>
        {pat.patterns.map((p, i) => (
          <div key={i} style={{ padding: "8px 10px", borderRadius: 6, background: `${typeColor[p.type]}14`, borderLeft: `3px solid ${typeColor[p.type]}` }}>
            <div style={{ fontWeight: 700, color: typeColor[p.type], fontSize: 14 }}>{p.type === "bull" ? "▲ " : p.type === "bear" ? "▼ " : ""}{p.label}</div>
            <div className="dim">{p.desc}</div>
          </div>
        ))}
      </div>
      <Conclusion text={pat.conclusion} color={cc} />
    </Card>
  );
}

export function ChipXrayCard({ chipData }) {
  const chip = useMemo(() => getChipAnalysis(chipData), [chipData]);
  const cc = chip.foreign5d > 0 && chip.trust5d > 0 ? UP : chip.foreign5d < 0 && chip.trust5d < 0 ? DOWN : "#94a3b8";
  const sc = chip.score > 60 ? UP : chip.score < 40 ? DOWN : "#f59e0b";
  return (
    <Card title="籌碼透視" icon={Building2}>
      <div style={{ display: "flex", gap: 10, marginBottom: 10 }}>
        <div style={{ flex: 1, textAlign: "center", padding: "8px 4px", borderRadius: 8, background: `${sc}1a` }}>
          <div className="num" style={{ fontSize: 28, fontWeight: 900, color: sc }}>{chip.score}</div>
          <div className="dim">籌碼評分</div>
        </div>
        <div style={{ flex: 2 }}>
          <Row label="外資連續" value={chip.foreignStreak > 0 ? `連買 ${chip.foreignStreak} 天` : chip.foreignStreak < 0 ? `連賣 ${-chip.foreignStreak} 天` : "中性"} color={trendColor(chip.foreignStreak)} />
          <Row label="投信連續" value={chip.trustStreak > 0 ? `連買 ${chip.trustStreak} 天` : "中性"} color={trendColor(chip.trustStreak)} />
          <Row label="狀態" value={chip.status} />
        </div>
      </div>
      {[["外資近5日", chip.foreign5d], ["投信近5日", chip.trust5d]].map(([label, val]) => (
        <div key={label} style={{ marginBottom: 8 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginBottom: 3 }}>
            <span className="muted">{label}</span>
            <b className="num" style={{ color: trendColor(val) }}>{signed(val / 1000, 0, " 張")}</b>
          </div>
          <Meter value={(Math.abs(val) / 500000) * 100} color={trendColor(val)} />
        </div>
      ))}
      <Row label="融資餘額" value={fmt(chip.latestChip.margin_balance, 0)} />
      <Row label="融券餘額" value={fmt(chip.latestChip.short_balance, 0)} />
      <Conclusion text={chip.conclusion} color={cc} />
    </Card>
  );
}

// 新增：法人每日買賣超（近 20 日）
export function InstitutionalFlowCard({ chipData }) {
  const data = useMemo(() => (chipData?.rows || []).map((r) => ({
    date: `${String(r.date).slice(4, 6)}/${String(r.date).slice(6, 8)}`,
    外資: Math.round((r.foreign_buy || 0) / 1000),
    投信: Math.round((r.investment_trust_buy || 0) / 1000),
    自營: Math.round((r.dealer_buy || 0) / 1000),
  })), [chipData]);
  if (!data.length) return null;
  const sum = (k) => data.reduce((s, d) => s + d[k], 0);
  return (
    <Card title="法人每日買賣超（張）" icon={Landmark}>
      <div style={{ height: 220 }}>
        <ResponsiveContainer>
          <BarChart data={data} margin={{ top: 4, right: 4, left: -8, bottom: 0 }}>
            <XAxis dataKey="date" tick={{ fill: "#94a3b8", fontSize: 11 }} interval="preserveStartEnd" />
            <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} width={56} />
            <Tooltip contentStyle={{ background: "#0e1223", border: "1px solid #334155", borderRadius: 8 }} formatter={(v) => `${v.toLocaleString()} 張`} />
            <ReferenceLine y={0} stroke="#475569" />
            <Bar dataKey="外資">{data.map((d, i) => <Cell key={i} fill={d.外資 >= 0 ? UP : DOWN} />)}</Bar>
            <Bar dataKey="投信">{data.map((d, i) => <Cell key={i} fill={d.投信 >= 0 ? "#f97316" : "#14b8a6"} />)}</Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="dim" style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
        <span>外資（紅買／綠賣）{signed(sum("外資"), 0)}</span>
        <span>投信（橘買／青賣）{signed(sum("投信"), 0)}</span>
        <span>自營 {signed(sum("自營"), 0)}</span>
        <span>期間 {data[0].date}～{data.at(-1).date}</span>
      </div>
    </Card>
  );
}

export function BlackCandleCard({ rows, chipData }) {
  const bc = useMemo(() => detectBlackCandleAccum(rows, chipData), [rows, chipData]);
  return (
    <Card title="法人黑K偵測" icon={CandlestickChart}>
      <div style={{ textAlign: "center", marginBottom: 10 }}>
        <span style={{ fontSize: 17, fontWeight: 800, color: bc.color, padding: "4px 12px", borderRadius: 6, background: `${bc.color}22` }}>{bc.signal}</span>
      </div>
      <Row label="K棒形態" value={bc.isBlack ? "黑K（收跌）" : "紅K（收漲）"} color={bc.isBlack ? DOWN : UP} />
      <Row label="收盤 vs 昨收" value={bc.closeAbovePrev ? "▲ 高於昨收" : "▼ 低於昨收"} color={bc.closeAbovePrev ? UP : DOWN} />
      <Row label="法人買超" value={bc.instBuy ? "是" : "否"} color={bc.instBuy ? UP : undefined} />
      <Row label="當日漲跌幅" value={signed(bc.changeRate, 2, "%")} color={trendColor(bc.changeRate)} />
      {bc.isAccum && <div className="notice warn" style={{ marginTop: 8 }}>發現「法人黑K吸籌」訊號 — 主力洗盤吸籌的典型形態</div>}
      <Conclusion text={bc.conclusion} color={bc.color} />
    </Card>
  );
}

export function MomentumCard({ rows }) {
  const rsi = useMemo(() => getRsiAnalysis(rows), [rows]);
  const kd = useMemo(() => getKdAnalysis(rows), [rows]);
  const latest = rows?.at(-1) || {};
  return (
    <Card title="動能指標" icon={Activity}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 10 }}>
        <div style={{ padding: "8px 6px", borderRadius: 8, background: "var(--card-2)", textAlign: "center" }}>
          <div className="dim">RSI14</div>
          <div className="num" style={{ fontSize: 26, fontWeight: 900, color: rsi.color }}>{rsi.rsi ? rsi.rsi.toFixed(1) : "--"}</div>
          <div style={{ fontSize: 12, color: rsi.color }}>{rsi.status}</div>
        </div>
        <div style={{ padding: "8px 6px", borderRadius: 8, background: "var(--card-2)", textAlign: "center" }}>
          <div className="dim">KD 值</div>
          <div className="num" style={{ fontSize: 16, fontWeight: 900, color: kd.color, marginTop: 6 }}>{kd.k != null ? `K ${kd.k.toFixed(1)} / D ${kd.d?.toFixed(1)}` : "--"}</div>
          <div style={{ fontSize: 12, color: kd.color }}>{kd.status}</div>
        </div>
      </div>
      <Row label="布林寬度" value={fmt(latest.bb_width, 4)} color={latest.bb_width < 0.02 ? "#f59e0b" : undefined} />
      {Number.isFinite(latest.bb_width) && latest.bb_width < 0.02 && <Row label="布林狀態" value="極度收縮（大波動蓄勢）" color="#f59e0b" />}
      <Conclusion text={`RSI：${rsi.conclusion}`} color={rsi.color} />
      <Conclusion text={`KD：${kd.conclusion}`} color={kd.color} />
    </Card>
  );
}

export function RiskMirrorCard({ rows, chipData }) {
  const risk = useMemo(() => getRiskMetrics(rows, chipData), [rows, chipData]);
  const cc = risk.isLongRisk ? UP : risk.isShortSqueeze ? "#f59e0b" : DOWN;
  return (
    <Card title="風險雙鏡" icon={AlertTriangle}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 10 }}>
        {[["融資斷頭", risk.isLongRisk, "警戒", "安全", UP], ["軋空預兆", risk.isShortSqueeze, "偵測到", "無", "#f59e0b"]].map(([label, on, yes, no, c]) => (
          <div key={label} style={{ padding: "10px 6px", borderRadius: 8, textAlign: "center", background: on ? `${c}1f` : "rgba(34,197,94,.07)", border: `1px solid ${on ? `${c}55` : "#22c55e44"}` }}>
            <div className="dim">{label}</div>
            <div style={{ fontSize: 14, fontWeight: 800, color: on ? c : DOWN }}>{on ? yes : no}</div>
          </div>
        ))}
      </div>
      <Row label="融資餘額" value={fmt(risk.marginBalance, 0)} />
      <Row label="融券餘額" value={fmt(risk.shortBalance, 0)} />
      {risk.shortRatio != null && <Row label="券資比" value={`${risk.shortRatio.toFixed(1)}%`} color={risk.shortRatio > 30 ? "#f59e0b" : undefined} />}
      <Row label="跌破MA60" value={risk.belowMa60 ? "是" : "否"} color={risk.belowMa60 ? UP : DOWN} />
      <Conclusion text={risk.conclusion} color={cc} />
    </Card>
  );
}

export function ScenarioCard({ rows, chipData }) {
  const sc = useMemo(() => getScenarios(rows, chipData), [rows, chipData]);
  const cc = sc.bull > sc.bear ? UP : sc.bear > sc.bull + 10 ? DOWN : "#f59e0b";
  return (
    <Card title="情境機率" icon={Shuffle}>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12 }} className="muted">
        <span>空頭 {sc.bear}%</span><span>中性 {sc.neutral}%</span><span>多頭 {sc.bull}%</span>
      </div>
      <div style={{ height: 10, borderRadius: 6, overflow: "hidden", display: "flex", margin: "4px 0 10px" }}>
        <div style={{ width: `${sc.bear}%`, background: DOWN }} />
        <div style={{ width: `${sc.neutral}%`, background: "#475569" }} />
        <div style={{ width: `${sc.bull}%`, background: UP }} />
      </div>
      {[
        { label: "多頭走強", prob: sc.bull, color: UP, desc: "有效突破，法人加碼，放量上攻" },
        { label: "中性整理", prob: sc.neutral, color: "#f59e0b", desc: "均線纏繞，量能萎縮，等待方向" },
        { label: "空頭走弱", prob: sc.bear, color: DOWN, desc: "跌破支撐，法人出場，量增下跌" },
      ].map((s) => (
        <div key={s.label} style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6, padding: "6px 8px", borderRadius: 6, background: `${s.color}12` }}>
          <div className="num" style={{ width: 46, textAlign: "center", fontWeight: 900, fontSize: 17, color: s.color }}>{s.prob}%</div>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: s.color }}>{s.label}</div>
            <div className="dim">{s.desc}</div>
          </div>
        </div>
      ))}
      <Conclusion text={sc.conclusion} color={cc} />
    </Card>
  );
}

export function FundamentalsCard({ data }) {
  const pct = (v, d = 1) => (v != null ? signed(v, d, "%") : "--");
  if (!data || data.error) {
    return <Card title="個股基本面" icon={Gauge}><div className="dim">{data?.error || "基本面資料暫無"}</div></Card>;
  }
  const items = [
    { label: "本益比 PE", value: data.pe_ratio != null ? fmt(data.pe_ratio, 1) : "--" },
    { label: "殖利率", value: data.dividend_yield != null ? `${fmt(data.dividend_yield, 2)}%` : "--", color: "#f59e0b" },
    { label: "股價淨值比 PB", value: data.pb_ratio != null ? fmt(data.pb_ratio, 2) : "--" },
    { label: "EPS（近四季）", value: data.eps != null ? fmt(data.eps, 2) : "--" },
    { label: "ROE", value: data.roe != null ? `${fmt(data.roe, 1)}%` : "--" },
    { label: "毛利率", value: data.gross_margin != null ? `${fmt(data.gross_margin, 1)}%` : "--" },
    { label: "營益率", value: data.operating_margin != null ? `${fmt(data.operating_margin, 1)}%` : "--" },
    { label: "負債比", value: data.debt_ratio != null ? `${fmt(data.debt_ratio, 1)}%` : "--" },
    { label: "月營收 YOY", value: pct(data.revenue_yoy), color: trendColor(data.revenue_yoy) },
    { label: "月營收 MOM", value: pct(data.revenue_mom), color: trendColor(data.revenue_mom) },
  ];
  return (
    <Card title="個股基本面" icon={Gauge}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
        {items.map((it) => (
          <div key={it.label} style={{ padding: "8px 10px", borderRadius: 6, background: "var(--card-2)" }}>
            <div className="dim">{it.label}</div>
            <div className="num" style={{ color: it.color || "var(--fg)", fontWeight: 700, fontSize: 15 }}>{it.value}</div>
          </div>
        ))}
      </div>
      <div className="dim" style={{ marginTop: 8 }}>資料：TWSE／TPEx・yfinance・MOPS {data.data_date ? `（${data.data_date}）` : ""}</div>
    </Card>
  );
}

export function FinancialsCard({ data }) {
  const years = (data?.years || []).slice(-6);
  const toE = (v) => (v == null ? null : v / 1e8);
  return (
    <Card title="歷年財務" icon={BookOpen}>
      {!years.length ? <div className="dim">歷年財務資料暫無（每週日自動更新）</div> : (
        <div className="table-wrap">
          <table className="data">
            <thead><tr><th className="left">年度</th><th>營收(億)</th><th>淨利(億)</th><th>EPS</th><th>股利</th><th>殖利率</th></tr></thead>
            <tbody>
              {years.map((y) => (
                <tr key={y.year}>
                  <td className="left num" style={{ color: "var(--gold)", fontWeight: 700 }}>{y.year}{y.year === new Date().getFullYear() ? "*" : ""}</td>
                  <td>{y.revenue != null ? fmt(toE(y.revenue), 0) : "--"}</td>
                  <td style={{ color: (y.net_income ?? 0) < 0 ? DOWN : undefined }}>{y.net_income != null ? fmt(toE(y.net_income), 0) : "--"}</td>
                  <td>{y.eps != null ? fmt(y.eps, 2) : "--"}</td>
                  <td style={{ color: "#f59e0b" }}>{y.dividend != null ? fmt(y.dividend, 2) : "--"}</td>
                  <td style={{ color: "#38bdf8" }}>{y.dividend_yield != null ? `${fmt(y.dividend_yield, 2)}%` : "--"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="dim" style={{ marginTop: 6 }}>殖利率＝當年股利 ÷ 年終收盤；* 今年為部分資料</div>
    </Card>
  );
}
