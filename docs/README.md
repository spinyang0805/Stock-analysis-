# 文件索引

專案：台股決策看盤網站（Vite/React 靜態前端 + Supabase + GitHub Actions 資料管線）。Fly.io 後端已於 2026-10-18 前移除，部分文件因此過時。

## 文件一覽

| 文件 | 用途 | 狀態 | 最後更新 |
|---|---|---|---|
| [AGENT_HANDOVER.md](AGENT_HANDOVER.md) | 交接入口：現行架構、規則、程式碼地圖 | 現行 | 2026-10-18 |
| [DEPLOY_GUIDE.md](DEPLOY_GUIDE.md) | 部署與排程（開頭為現況，其後為舊版） | 部分過時 | 2026-10-18 |
| [BACKEND_ARCHITECTURE.md](BACKEND_ARCHITECTURE.md) | 後端模組（開頭為現況，其後 FastAPI 內容已過時） | 部分過時 | 2026-10-18 |
| [FRONTEND_ARCHITECTURE.md](FRONTEND_ARCHITECTURE.md) | 前端架構（開頭為現況，其後舊 tab／BatchPage 已過時） | 部分過時 | 2026-10-18 |
| [BUSINESS_RULE_ENGINE.md](BUSINESS_RULE_ENGINE.md) | 評分、觀點卡、訊號、回測規則 | 部分過時 | 2026-10-18 |
| [FIREBASE_SCHEMA.md](FIREBASE_SCHEMA.md) | 舊 Firestore 結構（現為 Supabase Postgres） | 部分過時 | 2026-10-18 |
| [API_REFERENCE.md](API_REFERENCE.md) | 舊 `/api/*` 路由說明 | 已過時 | 2026-10-18 |
| [AI_PROMPT.md](AI_PROMPT.md) | 舊 `/api/ai/context` 與 prompt 範本 | 已過時 | 2026-10-18 |
| [STOCK_WEBSITE_DESIGN_SPEC.md](STOCK_WEBSITE_DESIGN_SPEC.md) | Render + FastAPI 時期的完整設計規格 | 已過時 | 2026-10-18 |
| [../FIREBASE_INDICATORS_SCHEMA.md](../FIREBASE_INDICATORS_SCHEMA.md) | `analysis_cache` 觀點卡 JSON 範例（Firestore 時期） | 已過時（根目錄，未加註） | 2026-05-04 |

## 目前架構一覽

- 前端：Vite/React SPA（`src/`），hash 路由；頁面為個股、我的持股、損益分析、交易紀錄、AI 選股、帳號管理（admin）。
- 部署：Render 靜態站（`render.yaml` 僅此一服務）+ GitHub Pages（`deploy-pages.yml`）。
- 無後端 HTTP 服務；`/api/*` 已不存在。
- 前端資料：靜態 `public/data/*`、Supabase（RPC `market_quote`、`market_intraday`、`ai_explain`、`ai_pick`；資料表 `transactions`、`import_presets`、`watchlist`、`profiles`、`holding_settings`、`track_requests`）、瀏覽器直連 FinMind。
- 資料管線：`update-data.yml`（平日 15:40 台北時間）-> `daily_job.py` -> `ai_screen_refresh()` -> `track_new.py` -> `export_static_json.py --source db` -> commit `public/data`。
- 其他排程：`track-stocks.yml`（每 20 分鐘）、`update-financials.yml`（週日）、`probe.yml`、`db-migrate.yml`。
- 資料庫：Supabase Postgres（`DATABASE_URL`）；`backend/firebase.py`、`firebase_cache.py` 為歷史命名，實際是 psycopg2。
- 盤中資料：pg_cron 擷取（`supabase/migrations/004`）；AI 為 Groq，金鑰在 Supabase Vault（`005`）。
- 注意：`export_static_json.py --source api` 指向已下線的 fly.dev，已失效。

## 常用入口

- 前端殼層：`src/App.jsx`；頁面：`src/pages/`
- 每日管線：`backend/daily_job.py`、`.github/workflows/update-data.yml`
- 靜態輸出：`backend/export_static_json.py`、`backend/payload_builder.py`
- 分析引擎：`backend/analysis_engine.py`、`signal_engine.py`、`perspective_engine.py`
- 資料表結構：`backend/schema.sql`、`supabase/migrations/`
- 部署：`render.yaml`、`.github/workflows/`
