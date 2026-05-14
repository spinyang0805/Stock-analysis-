import { useState } from 'react'

const API = 'https://stock-analysis-api-ihun.onrender.com'

export default function BatchPage() {
  const [offset, setOffset] = useState(0)
  const [limit, setLimit] = useState(10)
  const [total, setTotal] = useState(0)
  const [done, setDone] = useState(false)
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState(null)

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

  return (
    <div style={{ background: '#020617', color: 'white', padding: 16, fontFamily: 'Arial' }}>
      <h2>Batch Tool</h2>
      <p>Current: {offset} / {total || '?'}</p>
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        <label>Offset <input value={offset} onChange={e => setOffset(Number(e.target.value || 0))} /></label>
        <label>Limit <input value={limit} onChange={e => setLimit(Number(e.target.value || 10))} /></label>
        <button disabled={busy} onClick={() => run('/api/init_universe')}>Check</button>
        <button disabled={busy || done} onClick={() => run(`/api/init_universe_batch?offset=${offset}&limit=${limit}`)}>{done ? 'Done' : 'Run current'}</button>
        <button disabled={busy} onClick={() => { setOffset(0); setDone(false); setResult(null); }}>Reset</button>
      </div>
      <pre style={{ marginTop: 12, background: '#0f172a', padding: 12, overflow: 'auto' }}>{JSON.stringify(result || {}, null, 2)}</pre>
    </div>
  )
}
