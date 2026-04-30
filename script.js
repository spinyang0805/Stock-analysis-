const BACKEND_URL = "http://localhost:8000"; // 之後改成你的IP

const input = document.getElementById('stockInput');
const suggestions = document.getElementById('suggestions');

let chart;

async function fetchHistory(stock) {
  const res = await fetch(`${BACKEND_URL}/api/history/${stock}`);
  const json = await res.json();
  return json.data || [];
}

async function fetchRealtime(stock) {
  try {
    const res = await fetch(`${BACKEND_URL}/api/realtime/${stock}`);
    return await res.json();
  } catch {
    return null;
  }
}

function SMA(data, p) {
  return data.map((_, i) =>
    i < p ? null : data.slice(i - p, i).reduce((a, b) => a + b, 0) / p
  );
}

function EMA(data, p) {
  let k = 2 / (p + 1);
  let ema = [data[0]];
  for (let i = 1; i < data.length; i++) {
    ema.push(data[i] * k + ema[i - 1] * (1 - k));
  }
  return ema;
}

function MACD(data) {
  const ema12 = EMA(data, 12);
  const ema26 = EMA(data, 26);
  return ema12.map((v, i) => v - ema26[i]);
}

document.getElementById('searchBtn').addEventListener('click', async () => {
  const stock = input.value;

  const history = await fetchHistory(stock);

  if (!history.length) {
    alert("查無資料，請確認股票代號");
    return;
  }

  const prices = history.map(d => d.close);

  const realtime = await fetchRealtime(stock);
  if (realtime && realtime.price) {
    prices[prices.length - 1] = Number(realtime.price);
  }

  const ma5 = SMA(prices, 5);
  const ma20 = SMA(prices, 20);
  const macd = MACD(prices);

  if (chart) chart.destroy();
  chart = new Chart(document.getElementById('chart'), {
    type: 'line',
    data: {
      labels: history.map(d => d.date),
      datasets: [
        { label: 'Price', data: prices },
        { label: 'MA5', data: ma5 },
        { label: 'MA20', data: ma20 }
      ]
    }
  });

  const last = prices[prices.length - 1];
  const lastMA20 = ma20[ma20.length - 1];
  const lastMACD = macd[macd.length - 1];

  let trend = "盤整";
  if (last > lastMA20 && lastMACD > 0) trend = "偏多";
  if (last < lastMA20 && lastMACD < 0) trend = "偏空";

  document.getElementById('analysisText').innerText = `價格:${last} 趨勢:${trend}`;
}
