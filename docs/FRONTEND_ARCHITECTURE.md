# Frontend Architecture

The frontend is a React + Vite app for stock dashboard analysis, charting, and batch maintenance controls.

## Entry Points

| File | Purpose |
|---|---|
| `src/main.jsx` | React app mount and basic routing between dashboard/batch/control views |
| `src/App.jsx` | Main stock dashboard |
| `src/BatchPage.jsx` | Product universe and chip batch tool |
| `src/SystemControlPanel.jsx` | Maintenance/control panel |
| `index.html` | Vite HTML shell |
| `style.css` | Global CSS |

## Dependencies

From `package.json`:

- React
- Vite
- `lightweight-charts`
- `lucide-react`
- `recharts`

## App Shell

`src/main.jsx` imports the main views and mounts React into the page.

Expected views:

- Dashboard: `App.jsx`
- Batch tool: `BatchPage.jsx`
- Database maintenance: `DatabaseMaintenancePage.jsx`
- System control panel: `SystemControlPanel.jsx`

The app should use a top-level tab shell instead of rendering all pages one after another. This keeps destructive database actions away from the daily dashboard workflow.

## Tab Layout

Planned top-level tabs:

| Tab | Component | Responsibility |
|---|---|---|
| Dashboard | `App.jsx` | Stock chart, score, analysis, signals |
| Chip Batch | `BatchPage.jsx` | Chip data backfill only |
| Database Maintenance | `DatabaseMaintenancePage.jsx` | Product universe sync, stock cache reset, clear-and-rebuild workflow, kline smoke test |
| System Control | `SystemControlPanel.jsx` | Non-destructive system checks and future operational controls |

Database-related operations must live in `DatabaseMaintenancePage.jsx`:

- `GET /api/init_universe`
- `GET /api/init_universe_batch?offset={offset}&limit={limit}`
- `GET /api/firebase/reset_all?product_type={type}&market={market}&offset={offset}&limit={limit}`
- `GET /api/job/backfill_all?product_type={type}&market={market}&offset={offset}&limit={limit}&months={months}`
- `GET /api/job/daily`
- `GET /api/kline/2330` smoke test

`BatchPage.jsx` should not own product universe sync after this split. It should focus on chip-specific backfill through `/api/chip/backfill_all`.

`SystemControlPanel.jsx` should not expose destructive reset/rebuild buttons after this split. If future database actions are added, place them in the Database Maintenance tab.

## API Base URL

`src/App.jsx` defines:

```js
const DEFAULT_API = "https://stock-analysis-api-ihun.onrender.com";
const RAW_API = import.meta.env.VITE_API_BASE_URL || DEFAULT_API;
const API = String(RAW_API).includes("stock-analysis-api-ihun")
  ? String(RAW_API).replace(/\/$/, "")
  : DEFAULT_API;
```

Agent note: this intentionally forces the known production API unless the env var contains `stock-analysis-api-ihun`.

## Version Labels

`src/App.jsx` exposes visible build labels:

```js
const APP_VERSION = "v16";
const BUILD_LABEL = "2026-05-14 21:45";
const COMMIT_LABEL = "multi-kline-schema";
```

When changing user-visible dashboard behavior, update these labels so production screenshots and bug reports can be tied to a build.

## Dashboard Data Flow

```mermaid
flowchart TD
  Input[User stock input] --> Kline[GET /api/kline/{stock}]
  Input --> Analysis[GET /api/analysis/{stock}]
  Kline --> Normalize[pickRows + normalize OHLCV]
  Normalize --> Chart[lightweight-charts]
  Analysis --> Cards[Score / perspective cards / signals]
```

## Kline Parser Contract

The dashboard accepts multiple backend shapes. Existing parser logic checks these row containers:

- `payload.data`
- `payload.rows`
- `payload.items`
- `payload.kline`
- `payload.kline_data`
- `payload.daily`
- `payload.result.data`
- `payload.result.rows`
- `payload.basic.data`

Field aliases:

| Normalized | Accepted Aliases |
|---|---|
| `open` | `open`, `Open`, `o` |
| `high` | `high`, `High`, `h` |
| `low` | `low`, `Low`, `l` |
| `close` | `close`, `Close`, `c` |
| `volume` | `volume`, `Volume`, `vol` |
| `date` | `date`, `Date`, `data_date` |
| `time` | `time`, `timestamp` |

Chart data shape:

```json
{"time":"2026-05-05","open":2250,"high":2270,"low":2240,"close":2250}
```

Volume shape:

```json
{"time":"2026-05-05","value":24233983,"color":"rgba(34,197,94,.55)"}
```

Line series shape:

```json
{"time":"2026-05-05","value":2214}
```

## Dashboard Responsibilities

`App.jsx` is responsible for:

- Stock code input.
- Fetching `/api/kline/{stock}`.
- Fetching `/api/analysis/{stock}`.
- Normalizing backend rows into chart rows.
- Rendering candlestick, volume, MA, RSI, MACD, and Bollinger-related views where available.
- Showing data status, API status, raw row count, normalized row count, and latest data date.
- Rendering decision score, perspective cards, signals, and trade plan.

## Batch Tool

`src/BatchPage.jsx` controls batch maintenance APIs.

Important endpoints:

```http
GET /api/chip/backfill_all?product_type=all&market=上市&offset=0&limit=100&days=20
```

Expected batch response fields:

- `status`
- `offset`
- `limit`
- `processed`
- `error_count`
- `errors`
- `next_offset`

UI should continue with `next_offset` until it is `null`.

## System Control Panel

`src/SystemControlPanel.jsx` calls backend maintenance routes.

Use it for:

- Firebase health checks.
- Job starts.
- Non-destructive operational checks.

Destructive cache reset and full rebuild workflows belong in `DatabaseMaintenancePage.jsx`.

## Frontend Change Checklist

- Preserve defensive API parsing unless backend response shape is fully migrated.
- Keep chart data sorted ascending by time.
- Deduplicate chart rows by `time`.
- Do not assume every indicator is present; `null` is valid for early rows.
- Keep batch UI resumable by offset.
- Update version labels for visible dashboard changes.
- Run `npm run build` before handing off frontend changes.
