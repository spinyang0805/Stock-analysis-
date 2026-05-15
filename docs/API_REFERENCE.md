# API Reference

Base URL: `https://stock-analysis-api-ihun.onrender.com`

## General Rules

- All JSON APIs should return `application/json; charset=utf-8`.
- Every API should include `status` where possible.
- Batch APIs must return `next_offset`.
- Batch APIs should return `error_count` and `errors`.
- Large operations must support `offset` and `limit`.

## Health

### GET `/`

Returns backend health.

```json
{"status":"ok","service":"TW Stock Decision API"}
```

### GET `/api/firebase/test`

Tests Firestore write access.

Expected response:

```json
{
  "status":"ok",
  "firebase_enabled":true,
  "write":"system_health/test",
  "data":{}
}
```

## Products

### GET `/api/search?q={keyword}`

Search stock/ETF by code or name.

### GET `/api/products?product_type=股票&market=all&limit=5000`

Parameters:

| Name | Default | Description |
|---|---|---|
| product_type | 股票 | 股票 / ETF / 債券ETF / all |
| market | all | 上市 / 上櫃 / all |
| limit | 5000 | max universe rows |

## Firebase Maintenance

### GET `/api/firebase/audit_all?limit_stocks=5000&limit_per_stock=30`

Audits `stock_daily` documents.

### GET `/api/firebase/cleanup_all?limit_stocks=5000&limit_per_stock=260`

Deletes invalid `stock_daily` documents.

### GET `/api/firebase/reset_all?product_type=all&market=all&offset=0&limit=500`

Clears `stock_daily/{code}` and `analysis_cache/{code}` by product universe batch.

Expected response:

```json
{
  "status":"ok",
  "mode":"stock_universe_batch_reset",
  "product_type":"all",
  "market":"all",
  "offset":0,
  "limit":500,
  "universe_count":887,
  "processed_count":500,
  "deleted_stock_docs":500,
  "deleted_stock_data_docs":100000,
  "deleted_analysis_cache":500,
  "next_offset":500
}
```

### GET `/api/firebase/cleanup/{stock}?limit=500`

Cleans invalid data for one stock.

## Jobs

### GET `/api/job/daily`

Starts daily update in background.

### GET `/api/job/preload`

Starts hot stock preload in background.

### GET `/api/job/backfill/{stock}?months=12`

Starts one-stock historical backfill.

### GET `/api/job/backfill_all?product_type=股票&market=上市&offset=0&limit=100&months=12`

Starts batch backfill.

Parameters:

| Name | Default | Description |
|---|---:|---|
| product_type | 股票 | 股票 / ETF / 債券ETF / all |
| market | 上市 | 上市 / 上櫃 / all |
| offset | 0 | batch start |
| limit | 100 | batch size |
| months | 12 | months to backfill |

## Cache

### GET `/api/cache/status/{stock}`

Checks Firebase cache status for a stock.

## Kline

### GET `/api/kline/{stock}`

Reads `stock_daily` and returns OHLCV + indicators.

Success response:

```json
{
  "status":"ok",
  "message":"ok",
  "stock":"2330",
  "normalized_stock":"2330",
  "meta":{
    "code":"2330",
    "name":"台積電",
    "market":"上市",
    "industry":"半導體",
    "source":"Firebase stock_daily",
    "price":2250,
    "open":2250,
    "high":2270,
    "low":2240,
    "close":2250,
    "previous_close":2275,
    "change":-25,
    "change_pct":-1.1,
    "volume":24233983,
    "data_date":"20260505"
  },
  "source":"Firebase stock_daily",
  "last_close":2250,
  "last_date":1777939200,
  "data":[
    {
      "time":1748822400,
      "date":"20250602",
      "open":958,
      "high":961,
      "low":946,
      "close":946,
      "volume":40608468,
      "volume_ma5":null,
      "change_pct":null,
      "bb_width":null,
      "ma5":null,
      "ma10":null,
      "ma20":null,
      "ma60":null,
      "bb_upper":null,
      "bb_mid":null,
      "bb_lower":null,
      "rsi14":null,
      "macd":0,
      "macd_signal":0,
      "macd_hist":0
    }
  ],
  "cache_rows":225,
  "data_requirement":{"minimum_rows":90,"has_enough_rows":true}
}
```

Loading response:

```json
{"status":"loading","data":[],"backfill_started":true}
```

## Analysis

### GET `/api/analysis/{stock}`

Returns rule-based analysis.

Main fields:

- `score`
- `trend`
- `rating`
- `summary`
- `meta`
- `perspective_cards`
- `signals`
- `trade_plan`
- `data_rows`
- `data_requirement`

## Dashboard

### GET `/api/dashboard/{stock}`

Returns combined basic/kline/analysis/dashboard/chip payload.

```json
{"basic":{},"kline":[],"analysis":{},"dashboard":{},"chip":{},"source":"Firebase stock_daily"}
```

## Backtest

### GET `/api/backtest/{stock}`

Returns strategy backtest result.

## Chip APIs

### GET `/api/chip/init/{stock}?days=20`

Initializes chip data for one stock.

Response:

```json
{
  "status":"ok",
  "stock":"2330",
  "normalized_stock":"2330",
  "written_days":20,
  "analysis":{},
  "collection":"chip_daily"
}
```

### GET `/api/chip/{stock}?auto_init=true`

Reads chip rows and chip analysis. If empty and `auto_init=true`, generates seed rows.

Response:

```json
{
  "status":"ok",
  "route":"/api/chip/{stock}",
  "stock":"2330",
  "normalized_stock":"2330",
  "source":"Firebase chip_daily",
  "latest_chip":{},
  "rows":[],
  "row_count":20,
  "analysis":{"score":72,"status":"籌碼偏多","level":"bullish","reasons":[],"metrics":{}},
  "updated_at":"2026-05-14T12:00:00"
}
```

### GET `/api/chip/backfill_all?product_type=all&market=上市&offset=0&limit=100&days=20`

Batch creates/updates chip data for product universe.

Parameters:

| Name | Default | Description |
|---|---:|---|
| product_type | all | all / 股票 / ETF / 債券ETF |
| market | all | 上市 / 上櫃 / all |
| offset | 0 | batch start |
| limit | 20 | batch size; frontend default 100 |
| days | 20 | chip rows per stock |

Response must include `next_offset`:

```json
{
  "status":"ok",
  "route":"/api/chip/backfill_all",
  "collection":"chip_daily",
  "analysis_collection":"chip_analysis",
  "universe_count":887,
  "offset":0,
  "limit":100,
  "processed":100,
  "written_stocks":100,
  "error_count":0,
  "errors":[],
  "next_offset":100
}
```

Important: `/api/chip/backfill_all` must be declared before `/api/chip/{stock}` in FastAPI.
