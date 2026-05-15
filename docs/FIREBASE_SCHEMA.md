# Firebase Schema

This document describes Firestore collections used by the stock analysis system.

## Principles

- Firestore is the source for cached market data and analysis output.
- Daily rows are stored as subcollection documents under each stock.
- Batch and maintenance operations should be resumable with `offset`, `limit`, and `next_offset`.
- Writes should include timestamps such as `updated_at` where possible.

## Collections

| Collection | Purpose | Main Writers | Main Readers |
|---|---|---|---|
| `stock_daily` | OHLCV and indicator source rows | `jobs.py`, `firebase_cache.py` | `main.py`, `firebase_cache.py` |
| `analysis_cache` | Cached rule analysis payloads | `main.py` | `main.py`, frontend APIs |
| `product_universe` | Stock/ETF universe | `maintenance_routes.py` | `main.py`, `stock_list.py`, `chip_routes.py` |
| `chip_daily` | Chip/institutional daily rows | `chip_routes.py` | `chip_routes.py` |
| `chip_analysis` | Cached chip analysis result | `chip_routes.py` | `chip_routes.py`, dashboard payloads |
| `job_logs` | Batch status snapshots | `main.py`, `firebase_cache.py` | status/debug APIs |
| `job_queue` | Long-running job control and progress | `queue_api.py`, `auto_routes.py`, `maintenance_routes.py` | job status APIs |
| `system_health` | Firebase health writes | `main.py` | `/api/firebase/test` |
| `indicators` | Maintenance reset target | `maintenance_routes.py` | reserved/legacy |

Note: `backend/firebase_cache.py` still has helper functions using `chip_data`, while `backend/chip_routes.py` writes the active chip API data to `chip_daily` and `chip_analysis`. Treat `chip_daily` as the current API-facing collection unless intentionally migrating legacy data.

## `stock_daily`

Path:

```text
stock_daily/{stock_id}
stock_daily/{stock_id}/data/{yyyymmdd}
```

Parent document fields:

```json
{
  "stock_id": "2330",
  "latest_date": "20260505",
  "updated_at": "2026-05-14T12:00:00"
}
```

Daily data document:

```json
{
  "date": "20260505",
  "data_date": "20260505",
  "open": 2250.0,
  "high": 2270.0,
  "low": 2240.0,
  "close": 2250.0,
  "volume": 24233983.0,
  "market": "TWSE",
  "name": "TSMC",
  "source": "TWSE STOCK_DAY"
}
```

Validation expectations:

- `date` must be present.
- `open`, `high`, `low`, `close`, and `volume` should be numeric or convertible to numeric.
- Invalid rows should be skipped or cleaned by Firebase maintenance endpoints.

## `analysis_cache`

Path:

```text
analysis_cache/{stock_id}
```

Document outline:

```json
{
  "stock_id": "2330",
  "latest_date": "20260505",
  "updated_at": "2026-05-14T12:00:00",
  "score": 72,
  "trend": {},
  "rating": {},
  "summary": "",
  "meta": {},
  "perspective_cards": [],
  "signals": {},
  "trade_plan": {},
  "data_rows": 225,
  "data_requirement": {
    "minimum_rows": 90,
    "has_enough_rows": true
  }
}
```

## `product_universe`

Path:

```text
product_universe/{code}
```

Document outline:

```json
{
  "code": "2330",
  "name": "TSMC",
  "market": "上市",
  "type": "股票",
  "industry": "Semiconductor",
  "updated_at": "2026-05-14T12:00:00"
}
```

Market/type values used by the code include:

- `market`: `上市`, `上櫃`, `all`
- `type`: `股票`, `ETF`, `高股息ETF`, `all`

## `chip_daily`

Path:

```text
chip_daily/{stock_id}
chip_daily/{stock_id}/data/{yyyymmdd}
```

Parent document:

```json
{
  "stock_id": "2330",
  "latest": {},
  "analysis": {},
  "updated_at": "2026-05-14T12:00:00"
}
```

Daily document:

```json
{
  "date": "20260514",
  "foreign_buy": 12000,
  "investment_trust_buy": 2300,
  "dealer_buy": -500,
  "margin_balance": 10000,
  "short_balance": 2500,
  "source": "generated_seed_v1"
}
```

Important: the current chip initialization route can generate mock/seed rows. Do not label those rows as real exchange data.

## `chip_analysis`

Path:

```text
chip_analysis/{stock_id}
```

Document outline:

```json
{
  "stock_id": "2330",
  "analysis": {
    "score": 72,
    "status": "chip bullish",
    "level": "bullish",
    "reasons": [],
    "metrics": {}
  },
  "latest": {},
  "updated_at": "2026-05-14T12:00:00"
}
```

## `job_logs`

Path:

```text
job_logs/{job_id}
```

Typical fields:

```json
{
  "job_id": "backfill_all_latest",
  "status": "running",
  "processed_count": 100,
  "error_count": 0,
  "errors": [],
  "next_offset": 100,
  "updated_at": "2026-05-14T12:00:00"
}
```

## `job_queue`

Path:

```text
job_queue/{job_id}
```

Used for queued, paused, resumed, stopped, and rebuild jobs.

Typical fields:

```json
{
  "job_id": "job_20260514_120000",
  "status": "running",
  "control": "resume",
  "phase": "backfill",
  "progress": 50,
  "total": 887,
  "created_at": "2026-05-14T12:00:00",
  "updated_at": "2026-05-14T12:05:00"
}
```

## Maintenance APIs

See [API Reference](API_REFERENCE.md) for request/response details.

Relevant endpoints:

- `GET /api/firebase/test`
- `GET /api/firebase/audit_all`
- `GET /api/firebase/cleanup_all`
- `GET /api/firebase/reset_all`
- `GET /api/firebase/cleanup/{stock}`
- `GET /api/init_universe`
- `GET /api/init_universe_batch`
- `GET /api/chip/backfill_all`
