import { useEffect, useRef, useState } from "react";
import { createChart } from "lightweight-charts";

export default function App() {
  const chartRef = useRef();
  const [symbol, setSymbol] = useState("2330");
  const [analysis, setAnalysis] = useState(null);

  useEffect(() => {
    const chart = createChart(chartRef.current, {
      width: 900,
      height: 400,
      layout: { background: { color: "#0f172a" }, textColor: "#fff" },
    });

    const candle = chart.addCandlestickSeries();

    fetch(`https://你的-backend.onrender.com/api/kline/${symbol}`)
      .then(res => res.json())
      .then(data => candle.setData(data));

    fetch(`https://你的-backend.onrender.com/api/analysis/${symbol}`)
      .then(res => res.json())
      .then(data => setAnalysis(data));

  }, [symbol]);

  return (
    <div style={{ background: "#0f172a", color: "white", padding: 20 }}>
      <h1>交易決策系統</h1>

      <input value={symbol} onChange={e => setSymbol(e.target.value)} />
      <button onClick={() => setSymbol(symbol)}>查詢</button>

      <div ref={chartRef}></div>

      {analysis && (
        <div style={{ marginTop: 20 }}>
          <h2>{analysis.trend} ({analysis.score})</h2>
          <p>{analysis.summary}</p>

          <h3>決策訊號</h3>
          {analysis.signals.map((s, i) => (
            <div key={i} style={{
              background: s.score > 0 ? "#064e3b" : "#7f1d1d",
              margin: 5,
              padding: 10
            }}>
              <b>{s.title}</b>：{s.message}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
