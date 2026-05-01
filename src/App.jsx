import { useEffect, useRef, useState } from "react";
import { createChart } from "lightweight-charts";

export default function App() {
  const chartRef = useRef();
  const [symbol, setSymbol] = useState("2330");
  const [analysis, setAnalysis] = useState("");

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
      .then(data => setAnalysis(`${data.trend} - ${data.action}`));

  }, [symbol]);

  return (
    <div style={{ background: "#0f172a", color: "white", padding: 20 }}>
      <h1>專業台股看盤系統</h1>

      <input value={symbol} onChange={e => setSymbol(e.target.value)} />
      <button onClick={() => setSymbol(symbol)}>查詢</button>

      <div ref={chartRef}></div>

      <h2>{analysis}</h2>
    </div>
  );
}
