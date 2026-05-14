import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import SystemControlPanel from './SystemControlPanel.jsx'
import BatchPage from './BatchPage.jsx'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
    <BatchPage />
    <SystemControlPanel />
  </React.StrictMode>
)
