# Deploy Guide

This project is configured for Render.

## Services

`render.yaml` defines two services:

| Service | Runtime | Root | Start/Build |
|---|---|---|---|
| `stock-analysis-api` | Python web service | `backend` | `uvicorn main:app --host 0.0.0.0 --port $PORT` |
| `stock-analysis-frontend` | Static site | repo root | `npm install && npm run build`, publish `dist` |

## Backend

Backend dependencies:

```text
fastapi
uvicorn[standard]
pandas
numpy
requests
yfinance
firebase-admin
pytz
```

Render backend settings:

- `rootDir`: `backend`
- `buildCommand`: `pip install -r requirements.txt`
- `startCommand`: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- `healthCheckPath`: `/`

Health check:

```http
GET /
```

Expected:

```json
{"status":"ok","service":"TW Stock Decision API"}
```

## Frontend

Frontend scripts:

```bash
npm run dev
npm run build
npm run preview
```

Render static settings:

- `buildCommand`: `npm install && npm run build`
- `staticPublishPath`: `dist`

Frontend API selection in `src/App.jsx`:

```js
const DEFAULT_API = "https://stock-analysis-api-ihun.onrender.com";
const RAW_API = import.meta.env.VITE_API_BASE_URL || DEFAULT_API;
const API = String(RAW_API).includes("stock-analysis-api-ihun")
  ? String(RAW_API).replace(/\/$/, "")
  : DEFAULT_API;
```

Agent note: this code ignores any API base URL that does not contain `stock-analysis-api-ihun`. Update this intentionally if changing API service names.

## Environment Variables

Known frontend env var:

| Name | Purpose |
|---|---|
| `VITE_API_BASE_URL` | API base URL injected at Vite build time |

Backend Firebase credentials are expected by `firebase-admin`. Check `backend/firebase.py` before changing credential handling.

## Deploy Validation

After deploy, validate in this order:

1. Backend root:

```http
GET https://stock-analysis-api-ihun.onrender.com/
```

2. Firebase write/read:

```http
GET https://stock-analysis-api-ihun.onrender.com/api/firebase/test
```

3. Product universe:

```http
GET https://stock-analysis-api-ihun.onrender.com/api/products?product_type=all&market=all&limit=20
```

4. Kline:

```http
GET https://stock-analysis-api-ihun.onrender.com/api/kline/2330
```

5. Analysis:

```http
GET https://stock-analysis-api-ihun.onrender.com/api/analysis/2330
```

6. Chip:

```http
GET https://stock-analysis-api-ihun.onrender.com/api/chip/2330
```

7. Frontend:

```text
https://stock-analysis-ya45.onrender.com?v=latest
```

## Automatic Data Updates

GitHub Actions workflow:

```text
.github/workflows/daily-data-update.yml
```

Schedule:

- Monday-Friday 18:30 Asia/Taipei: calls `/api/job/daily` and warms recent chip history.
- Sunday 19:00 Asia/Taipei: also calls `/api/job/backfill_all_yearly?product_type=all&market=all&months=12` and `/api/chip/backfill_history_all?months=12`.

The workflow is also manually runnable through GitHub Actions `workflow_dispatch`.

Current chip-history coverage is TWSE T86, TWSE margin/short, TPEx institutional dailyTrade, and TPEx margin/balance data.

## Batch Initialization

Initialize product universe:

```http
GET /api/init_universe
GET /api/init_universe_batch?offset=0&limit=10
```

Backfill stock daily data:

```http
GET /api/job/backfill_all?product_type=股票&market=上市&offset=0&limit=100&months=12
GET /api/job/backfill_all_yearly?product_type=all&market=all&months=12
```

Backfill chip data:

```http
GET /api/chip/backfill_all?product_type=all&market=上市&offset=0&limit=100&days=20
GET /api/chip/backfill_history_all?months=12
```

Continue batch calls until `next_offset` is `null`.

## Common Deployment Risks

- Frontend built with the wrong API base URL.
- API route collisions if `/api/chip/{stock}` is registered before `/api/chip/backfill_all`.
- Firebase credentials missing or invalid in Render.
- Free Render services sleeping and causing first request latency.
- Batch operations timing out if `limit` is too large.
