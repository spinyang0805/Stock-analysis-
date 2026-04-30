let chart;

async function fetchStock(stock){
 const res=await fetch(`https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPrice&data_id=${stock}&start_date=2024-01-01`);
 const json=await res.json();
 return json.data;
}

function SMA(data,p){return data.map((_,i)=>i<p?null:data.slice(i-p,i).reduce((a,b)=>a+b,0)/p)}
function EMA(data,p){let k=2/(p+1);let ema=[data[0]];for(let i=1;i<data.length;i++){ema.push(data[i]*k+ema[i-1]*(1-k));}return ema}
function MACD(data){const ema12=EMA(data,12);const ema26=EMA(data,26);return ema12.map((v,i)=>v-ema26[i])}

function Bollinger(data,p=20){return data.map((_,i)=>{
 if(i<p) return null;
 let slice=data.slice(i-p,i);
 let avg=slice.reduce((a,b)=>a+b,0)/p;
 let std=Math.sqrt(slice.map(x=>Math.pow(x-avg,2)).reduce((a,b)=>a+b)/p);
 return {up:avg+2*std,down:avg-2*std,mid:avg};
 })}

document.getElementById('searchBtn').addEventListener('click',async()=>{
 const stock=document.getElementById('stockInput').value;
 const data=await fetchStock(stock);
 const prices=data.map(d=>d.close);

 const ma5=SMA(prices,5);
 const ma20=SMA(prices,20);
 const macd=MACD(prices);

 if(chart) chart.destroy();
 chart=new Chart(document.getElementById('chart'),{
 type:'line',
 data:{labels:data.map(d=>d.date),datasets:[
 {label:'Price',data:prices,borderColor:'white'},
 {label:'MA5',data:ma5,borderColor:'green'},
 {label:'MA20',data:ma20,borderColor:'yellow'}
 ]}
 });

 const last=prices[prices.length-1];
 const lastMA20=ma20[ma20.length-1];
 const lastMACD=macd[macd.length-1];

 let trend="盤整";
 if(last>lastMA20 && lastMACD>0) trend="偏多";
 if(last<lastMA20 && lastMACD<0) trend="偏空";

 document.getElementById('analysisText').innerText=`價格:${last} 趨勢:${trend}`;
 document.getElementById('technicalList').innerHTML=`
 <li>MA5:${ma5[ma5.length-1]}</li>
 <li>MA20:${lastMA20}</li>
 <li>MACD:${lastMACD}</li>`;
 document.getElementById('riskList').innerHTML=`
 <li>跌破MA20需注意</li>
 <li>MACD翻負轉弱</li>`;
});