const apiUrl="https://api.finmindtrade.com/api/v4/data";

async function fetchStock(stockId){
  const res=await fetch(`${apiUrl}?dataset=TaiwanStockPrice&data_id=${stockId}&start_date=2024-01-01`);
  const json=await res.json();
  return json.data;
}

function SMA(data,period){
  return data.map((_,i)=>{
    if(i<period) return null;
    const slice=data.slice(i-period,i);
    return slice.reduce((a,b)=>a+b,0)/period;
  });
}

function RSI(prices,period=14){
  let gains=0,losses=0;
  for(let i=1;i<=period;i++){
    let diff=prices[i]-prices[i-1];
    if(diff>0) gains+=diff;
    else losses-=diff;
  }
  let rs=gains/losses;
  return 100-(100/(1+rs));
}

function analyze(price,ma5,ma20,rsi){
  let score=0;
  if(price>ma5) score++;
  if(price>ma20) score++;
  if(rsi<30) score++;
  if(rsi>70) score--;

  let trend="盤整";
  if(score>=2) trend="偏多";
  if(score<=-1) trend="偏空";

  return {score,trend};
}

document.getElementById('searchBtn').addEventListener('click',async()=>{
  const stock=document.getElementById('stockInput').value;

  const data=await fetchStock(stock);
  const prices=data.map(d=>d.close);

  const ma5=SMA(prices,5).pop();
  const ma20=SMA(prices,20).pop();
  const rsi=RSI(prices.slice(-20));
  const price=prices[prices.length-1];

  const result=analyze(price,ma5,ma20,rsi);

  document.getElementById('stockTitle').innerText=stock+" 技術分析";

  document.getElementById('analysisText').innerText=
    `股價 ${price}\nMA5 ${ma5?.toFixed(2)} / MA20 ${ma20?.toFixed(2)}\nRSI ${rsi.toFixed(2)}\n趨勢：${result.trend}`;

  document.getElementById('technicalList').innerHTML=`
    <li>MA5：${ma5.toFixed(2)}</li>
    <li>MA20：${ma20.toFixed(2)}</li>
    <li>RSI：${rsi.toFixed(2)}</li>`;

  document.getElementById('riskList').innerHTML=`
    <li>RSI過高=可能過熱</li>
    <li>跌破MA20=轉弱訊號</li>`;
});