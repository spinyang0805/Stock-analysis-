import React from 'react'
import ReactDOM from 'react-dom/client'
import { useState } from 'react'
import App from './App.jsx'
import SystemControlPanel from './SystemControlPanel.jsx'
import BatchPage from './BatchPage.jsx'
import DatabaseMaintenancePage from './DatabaseMaintenancePage.jsx'

const tabs = [
  { id: 'dashboard', label: 'Dashboard', component: App },
  { id: 'chip-batch', label: 'Chip Batch', component: BatchPage },
  { id: 'database', label: 'Database Maintenance', component: DatabaseMaintenancePage },
  { id: 'system', label: 'System Control', component: SystemControlPanel },
]

function RootShell() {
  const [activeTab, setActiveTab] = useState('dashboard')
  const ActiveComponent = tabs.find((tab) => tab.id === activeTab)?.component || App

  return (
    <div style={shellStyle}>
      <nav style={navStyle} aria-label="Application sections">
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
      <ActiveComponent />
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
  borderRadius: 10,
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
