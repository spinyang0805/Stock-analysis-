// 資產變化曲線（市值 vs 投入成本，或總損益），附區間切換與資料表
import React, { useMemo, useState } from "react";
import { Area, AreaChart, CartesianGrid, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Table2 } from "lucide-react";
import { Seg } from "./ui.jsx";
import { DOWN, UP, fmtInt, fmtMoney, signed, trendColor } from "../lib/format.js";

const RANGES = [
  { value: "1M", label: "1月", days: 31 },
  { value: "3M", label: "3月", days: 92 },
  { value: "6M", label: "6月", days: 183 },
  { value: "1Y", label: "1年", days: 366 },
  { value: "ALL", label: "全部", days: Infinity },
];

function cutoff(days) {
  if (days === Infinity) return "";
  return new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
}

export default function EquityChart({ curve, height = 280 }) {
  const [range, setRange] = useState("6M");
  const [mode, setMode] = useState("value");
  const [table, setTable] = useState(false);
  const data = useMemo(() => {
    const from = cutoff(RANGES.find((r) => r.value === range).days);
    return curve.filter((c) => c.date >= from).map((c) => ({ ...c, label: c.date.slice(5) }));
  }, [curve, range]);
  if (!curve.length) return <div className="dim">尚無資料</div>;
  const first = data[0], last = data[data.length - 1];
  const change = last && first ? last.pnl - first.pnl : NaN;
  const lastPositive = (last?.pnl ?? 0) >= 0;

  return (
    <div>
      <div className="chart-toolbar">
        <Seg label="顯示" options={[{ value: "value", label: "市值 vs 成本" }, { value: "pnl", label: "總損益" }]} value={mode} onChange={setMode} />
        <Seg label="區間" options={RANGES.map(({ value, label }) => ({ value, label }))} value={range} onChange={setRange} />
        <span className="spacer" />
        <span className="num" style={{ fontSize: 13 }}>區間損益變化 <b style={{ color: trendColor(change) }}>{signed(change, 0)}</b></span>
        <button type="button" className="btn sm" aria-pressed={table} onClick={() => setTable((v) => !v)}><Table2 size={14} />資料表</button>
      </div>
      <div style={{ height }}>
        <ResponsiveContainer>
          {mode === "value" ? (
            <AreaChart data={data} margin={{ top: 6, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="mvFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#18213a" vertical={false} />
              <XAxis dataKey="label" tick={{ fill: "#94a3b8", fontSize: 11 }} minTickGap={24} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} width={64} tickFormatter={fmtMoney} tickCount={5} domain={[(min) => Math.floor(min * 0.95), (max) => Math.ceil(max * 1.03)]} />
              <Tooltip contentStyle={{ background: "#0e1223", border: "1px solid #334155", borderRadius: 8 }} labelFormatter={(_, p) => p?.[0]?.payload?.date}
                formatter={(v, n) => [fmtInt(v), n]} />
              <Area type="monotone" dataKey="marketValue" name="市值" stroke="#3b82f6" strokeWidth={2} fill="url(#mvFill)" isAnimationActive={false} />
              <Line type="stepAfter" dataKey="cost" name="投入成本" stroke="#94a3b8" strokeDasharray="5 4" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </AreaChart>
          ) : (
            <AreaChart data={data} margin={{ top: 6, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="pnlFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={lastPositive ? UP : DOWN} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={lastPositive ? UP : DOWN} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#18213a" vertical={false} />
              <XAxis dataKey="label" tick={{ fill: "#94a3b8", fontSize: 11 }} minTickGap={24} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} width={64} tickFormatter={fmtMoney} tickCount={5} />
              <ReferenceLine y={0} stroke="#475569" />
              <Tooltip contentStyle={{ background: "#0e1223", border: "1px solid #334155", borderRadius: 8 }} labelFormatter={(_, p) => p?.[0]?.payload?.date}
                formatter={(v, n) => [signed(v, 0), n]} />
              <Area type="monotone" dataKey="pnl" name="總損益（含已實現與股利）" stroke={lastPositive ? UP : DOWN} strokeWidth={2} fill="url(#pnlFill)" isAnimationActive={false} />
            </AreaChart>
          )}
        </ResponsiveContainer>
      </div>
      <div className="dim">實線：每日收盤市值；虛線：持有部位的投入成本（含手續費）。總損益 = 未實現 + 已實現 + 現金股利。</div>
      {table && (
        <div className="table-wrap" style={{ maxHeight: 320, marginTop: 8 }}>
          <table className="data">
            <thead><tr><th className="left">日期</th><th>市值</th><th>投入成本</th><th>未實現</th><th>已實現</th><th>股利</th><th>總損益</th></tr></thead>
            <tbody>
              {[...data].reverse().map((c) => (
                <tr key={c.date}>
                  <td className="left num">{c.date}</td>
                  <td>{fmtInt(c.marketValue)}</td><td>{fmtInt(c.cost)}</td>
                  <td style={{ color: trendColor(c.marketValue - c.cost) }}>{signed(c.marketValue - c.cost, 0)}</td>
                  <td style={{ color: trendColor(c.realized) }}>{signed(c.realized, 0)}</td>
                  <td>{fmtInt(c.dividends)}</td>
                  <td style={{ color: trendColor(c.pnl), fontWeight: 700 }}>{signed(c.pnl, 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
