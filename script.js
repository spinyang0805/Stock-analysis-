document.getElementById('searchBtn').addEventListener('click',()=>{
  const stock=document.getElementById('stockInput').value;

  // 模擬資料（之後可改API）
  const price=Math.floor(Math.random()*200)+100;
  const change=(Math.random()*4-2).toFixed(2);

  document.getElementById('stockTitle').innerText=stock+" 即時分析";

  document.getElementById('analysisText').innerText=
    `目前股價約 ${price} 元，漲跌 ${change}%。\n趨勢判斷：${change>0?"偏多":"偏空"}。`;

  document.getElementById('technicalList').innerHTML=`
    <li>MA5：${price-2}</li>
    <li>MA20：${price-5}</li>
    <li>RSI：${Math.floor(Math.random()*100)}</li>`;

  document.getElementById('riskList').innerHTML=`
    <li>短線波動較大</li>
    <li>注意量能變化</li>`;
});
