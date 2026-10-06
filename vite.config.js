import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// GitHub Pages 部署在 /Stock-analysis-/ 子路徑（由 workflow 設 VITE_BASE）；本機與其他主機用 /
export default defineConfig({
  base: process.env.VITE_BASE || '/',
  plugins: [react()],
})
