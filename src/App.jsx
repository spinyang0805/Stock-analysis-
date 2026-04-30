import { useState } from 'react'

export default function App(){
  const [stock,setStock]=useState('2330')
  const [price,setPrice]=useState(null)

  const handleSearch=()=>{
    const p=Math.floor(Math.random()*200)+100
    setPrice(p)
  }

  return (
    <div style={{padding:20,color:'#fff',background:'#0f172a',height:'100vh'}}>
      <h1>台股 React 即時分析</h1>
      <input value={stock} onChange={e=>setStock(e.target.value)} />
      <button onClick={handleSearch}>查詢</button>
      {price && <h2>價格: {price}</h2>}
    </div>
  )
}
