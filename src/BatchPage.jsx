import { useState } from 'react'

const API = 'https://stock-analysis-api-ihun.onrender.com'
const PAGE_VERSION = 'batch-v2-chip'

export default function BatchPage() {
  const [offset, setOffset] = useState(0)
  const [limit, setLimit] = useState(10)
  const [total, setTotal] = useState(0)
  const [done, setDone] = useState(false)
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState(null)

  const [chipOffset, setChipOffset] = useState(0)
  const [chipLimit, setChipLimit] = useState(100)
  const [chipMarket, setChipMarket] = useState('上市')
  const [chipType, setChipType] = useState('all')
  const [chipTotal, setChipTotal] = useState(0)
  const [chipDone, setChipDone] = useState(false)
  const [chipBusy, setChipBusy] = useState(false)
  const [chipResult, setChipResult] = useState(null)
  const [chipLogs, setChipLogs] = useState([])

  const addChipLog = (text) => setChipLogs(old => [`${new Date().toLocaleTimeString()} ${text}`, ...old].slice(0, 30))

  async function run(path) {
    setBusy(true)
    try {
      const res = await fetch(API + path)
      const json = await res.json()
      setResult(json)
      if (json.count) setTotal(Number(json.count))
      if (json.next_offset === null) {
        setDone(true)
        if (json.count) setOffset(Number(json.count))
      } else if (json.next_offset !== undefined) {
        setOffset(Number(json.next_offset))
      }
    } catch (e) {
      setResult({ error: String(e.message || e) })
    } finally {
      setBusy(false)
    }
  }

  async function runChipCurrent() {
    if (chipBusy || chipDone) return
    setChipBusy(true)
    const path = `/api/chip/backfill_all?product_type=${encodeURIComponent(chipType)}&market=${encodeURIComponent(chipMarket)}&offset=${chipOffset}&limit=${chipLimit}`
    try {
      addChipLog(`開始籌碼 offset=${chipOffset}, limit=${chipLimit}`)
      const res = await fetch(API + path)
      const json = await res.json()
      setChipResult(json)
      if (json.universe_count !== undefined) setChipTotal(Number(json.universe_count))
      if (json.next_offset === null) {
        setChipDone(true)
        setChipOffset(Number(json.universe_count || chipOffset))
        addChipLog(`完成全部籌碼：processed=${json.processed}, written=${json.written_stocks}`)
      } else if (json.next_offset !== undefined) {
        setChipOffset(Number(json.next_offset))
        addChipLog(`完成本批：processed=${json.processed}, written=${json.written_stocks}, next=${json.next_offset}`)
      } else {
        addChipLog('完成本批，但沒有 next_offset')
      }
    } catch (e) {
      const msg = String(e.message || e)
      setChipResult({ error: msg })
      addChipLog(`失敗：${msg}`)
    } finally {
      setChipBusy(false)
    }
  }

  function resetChip() {
    setChipOffset(0)
    setChipDone(false)
    setChipResult(null)
    setChipLogs([])
    setChipTotal(0)
  }

  return (
    <div style={{ background: '#020617', color: 'white', padding: 16, fontFamily: 'Arial' }}>
      <h2>Batch Tool</h2>
      <div style={{ color: '#38bdf8', marginBottom: 12 }}>{PAGE_VERSION}</div>

      <section style={boxStyle}>
        <h3>Universe 初始化</h3>
        <p>Current: {offset} / {total || '?'}</p>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <label>Offset <input value={offset} onChange={e => setOffset(Number(e.target.value || 0))} /></label>
          <label>Limit <input value={limit} onChange={e => setLimit(Number(e.target.value || 10))} /></label>
          <button disabled={busy} onClick={() => run('/api/init_universe')}>Check</button>
          <button disabled={busy || done} onClick={() => run(`/api/init_universe_batch?offset=${offset}&limit=${limit}`)}>{done ? 'Done' : 'Run current'}</button>
          <button disabled={busy} onClick={() => { setOffset(0); setDone(false); setResult(null); }}>Reset</button>
        </div>
        <pre style={preStyle}>{JSON.stringify(result || {}, null, 2)}</pre>
      </section>

      <section style={boxStyle}>
        <h3>籌碼資料批次更新 chip_daily</h3>
        <p>Current: {chipOffset} / {chipTotal || '?'} {chipDone ? '已完成' : ''}</p>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
          <label>Market
            <select value={chipMarket} onChange={e => setChipMarket(e.target.value)}>
              <option value="上市">上市</option>
              <option value="上櫃">上櫃</option>
              <option value="all">全部市場</option>
            </select>
          </label>
          <label>Type
            <select value={chipType} onChange={e => setChipType(e.target.value)}>
              <option value="all">全部</option>
              <option value="股票">股票</option>
              <option value="ETF">ETF</option>
              <option value="債券ETF">債券ETF</option>
            </select>
          </label>
          <label>Offset <input value={chipOffset} onChange={e => setChipOffset(Number(e.target.value || 0))} /></label>
          <label>Limit <input value={chipLimit} onChange={e => setChipLimit(Number(e.target.value || 100))} /></label>
          <button disabled={chipBusy || chipDone} onClick={runChipCurrent}>{chipBusy ? '更新中...' : chipDone ? 'Done' : `Run chip offset=${chipOffset}`}</button>
          <button disabled={chipBusy} onClick={resetChip}>Reset</button>
        </div>
        <pre style={preStyle}>{JSON.stringify(chipResult || {}, null, 2)}</pre>
        <div>{chipLogs.map((x, i) => <div key={i} style={{ color: '#cbd5e1', fontSize: 13 }}>{x}</div>)}</div>
      </section>
    </div>
  )
}

const boxStyle = { border: '1px solid #334155', borderRadius: 12, padding: 12, marginBottom: 14, background: '#0f172a' }
const preStyle = { marginTop: 12, background: '#020617', padding: 12, overflow: 'auto', maxHeight: 320, borderRadius: 10 }
