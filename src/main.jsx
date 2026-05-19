import React from 'react'
import ReactDOM from 'react-dom/client'
import { useState } from 'react'
import App from './App.jsx'
import SystemControlPanel from './SystemControlPanel.jsx'
import BatchPage from './BatchPage.jsx'
import DatabaseMaintenancePage from './DatabaseMaintenancePage.jsx'
import DataTablesPage from './DataTablesPage.jsx'

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('Tab render error', error, info)
  }

  render() {
    if (!this.state.error) return this.props.children
    return (
      <section style={errorPanelStyle}>
        <h2 style={{ marginTop: 0 }}>頁面載入失敗</h2>
        <p style={{ color: '#cbd5e1' }}>這個頁籤發生前端錯誤，請先切換到其他頁籤確認資料。</p>
        <pre style={errorPreStyle}>{String(this.state.error?.message || this.state.error)}</pre>
        <button type="button" style={tabStyle} onClick={() => this.setState({ error: null })}>重新載入此頁籤</button>
      </section>
    )
  }
}

const tabs = [
  { id: 'dashboard', label: '個股儀表板', component: App },
  { id: 'data-tables', label: '資料表檢查', component: DataTablesPage },
  { id: 'chip-batch', label: '籌碼批次', component: BatchPage },
  { id: 'database', label: '資料庫維護', component: DatabaseMaintenancePage },
  { id: 'system', label: '系統控制', component: SystemControlPanel },
]

function RootShell() {
  const [activeTab, setActiveTab] = useState('data-tables')
  const ActiveComponent = tabs.find((tab) => tab.id === activeTab)?.component || App

  return (
    <div style={shellStyle}>
      <nav style={navStyle} aria-label="系統頁籤">
        {tabs.map((tab) => {
          const active = tab.id === activeTab
          return (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveTab(tab.id)}
              style={active ? activeTabStyle : tabStyle}
            >
              {tab.label}
            </button>
          )
        })}
      </nav>
      <ErrorBoundary key={activeTab}>
        <ActiveComponent />
      </ErrorBoundary>
    </div>
  )
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <RootShell />
  </React.StrictMode>
)

const shellStyle = { minHeight: '100vh', background: '#020617' }
const navStyle = {
  position: 'sticky',
  top: 0,
  zIndex: 10,
  display: 'flex',
  gap: 8,
  flexWrap: 'wrap',
  padding: 12,
  background: '#020617',
  borderBottom: '1px solid #1e293b',
}
const tabStyle = {
  padding: '10px 12px',
  borderRadius: 8,
  border: '1px solid #334155',
  background: '#0f172a',
  color: '#cbd5e1',
  fontWeight: 700,
  cursor: 'pointer',
}
const activeTabStyle = {
  ...tabStyle,
  background: '#2563eb',
  borderColor: '#60a5fa',
  color: 'white',
}
const errorPanelStyle = {
  margin: 18,
  padding: 18,
  borderRadius: 8,
  border: '1px solid #7f1d1d',
  background: '#0f172a',
  color: 'white',
  fontFamily: 'Arial, sans-serif',
}
const errorPreStyle = {
  whiteSpace: 'pre-wrap',
  background: '#020617',
  color: '#fecaca',
  padding: 12,
  borderRadius: 8,
  overflow: 'auto',
}
