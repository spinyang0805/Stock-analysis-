# Business Rule Engine

This document describes how the backend converts stock rows and chip rows into scores, perspective cards, signals, trade plans, and backtests.

## Code Map

| Concern | File | Main Function |
|---|---|---|
| Technical indicators | `backend/analysis_engine.py` | `enrich_indicators` |
| Rule-based analysis | `backend/analysis_engine.py` | `build_rule_based_analysis` |
| Perspective cards | `backend/perspective_engine.py` | `generate_perspective_cards` |
| Signals | `backend/signal_engine.py` | `generate_signals` |
| Trade plan | `backend/signal_engine.py` | `generate_trade_plan` |
| Backtest | `backend/signal_engine.py` | `backtest_strategy` |
| Chip score | `backend/chip_routes.py` | `_analyze_rows` |
| AI rule context | `backend/rule_engine.py` | `build_ai_rule_context` |

## Data Inputs

Technical analysis expects a DataFrame or kline rows with:

- `date`
- `open`
- `high`
- `low`
- `close`
- `volume`

Chip analysis expects rows with:

- `foreign_buy`
- `investment_trust_buy`
- `dealer_buy`
- `margin_balance`
- `short_balance`

## Indicator Layer

`enrich_indicators` calculates fields used by later rules:

- Moving averages: `ma5`, `ma10`, `ma20`, `ma60`
- Volume average: `volume_ma5`
- Bollinger bands: `bb_upper`, `bb_mid`, `bb_lower`, `bb_width`
- RSI: `rsi14`
- MACD: `macd`, `macd_signal`, `macd_hist`
- Percentage change: `change_pct`

Agent note: if adding a new rule, prefer adding the derived field once in this layer instead of recomputing it in multiple APIs.

## Rule-Based Analysis

`build_rule_based_analysis(df, stock)` returns the main analysis object for `/api/analysis/{stock}`.

Main output areas:

- `score`
- `trend`
- `rating`
- `summary`
- `signals`
- `trade_plan`
- `data_rows`
- `data_requirement`

The analysis depends on having enough rows. Existing docs use `minimum_rows: 90` as the baseline for reliable analysis.

## Perspective Cards

`generate_perspective_cards(kline, chip)` creates UI-facing cards.

Expected card shape:

```json
{
  "category": "trend",
  "title": "Trend",
  "status": "Bullish",
  "level": "bullish",
  "meaning": "Short moving averages are above longer moving averages.",
  "logic": "MA5 > MA20 > MA60"
}
```

Current categories:

| Category | Meaning |
|---|---|
| `trend` | Moving average trend state |
| `volume_price` | Price and volume confirmation |
| `chip` | Institutional/chip accumulation or distribution |
| `credit` | Margin/short pressure and squeeze conditions |
| `risk` | Downside or overheated warnings |

## Trend Rules

Outline from existing documentation:

| Condition | Interpretation |
|---|---|
| `MA5 > MA10 > MA20 > MA60` | Strong bullish alignment |
| `MA5 > MA20 > MA60` | Bullish trend |
| `MA5 < MA20 < MA60` | Bearish trend |
| otherwise | Neutral or mixed trend |

## Volume-Price Rules

Outline from existing documentation:

| Condition | Interpretation |
|---|---|
| `Close > BB_UPPER` and `BB_WIDTH` expanding | Breakout / high momentum |
| `Volume > Volume_MA5` and `Change > 3%` | Volume-backed upside |
| `Volume > Volume_MA5` and `Change < -3%` | Volume-backed downside |
| `Volume < Volume_MA5` and `Change > 0` | Low-volume rebound |
| otherwise | Neutral volume-price state |

## Chip Rules

Metrics tracked by chip analysis:

| Metric | Meaning |
|---|---|
| `foreign_5d_sum` | Foreign investor 5-day net buy sum |
| `foreign_10d_sum` | Foreign investor 10-day net buy sum |
| `foreign_buy_days_5` | Foreign investor buy days in last 5 rows |
| `foreign_sell_days_5` | Foreign investor sell days in last 5 rows |
| `foreign_buy_streak` | Foreign investor consecutive buy streak |
| `investment_trust_5d_sum` | Investment trust 5-day net buy sum |
| `investment_trust_buy_days_5` | Investment trust buy days in last 5 rows |
| `investment_trust_buy_streak` | Investment trust consecutive buy streak |
| `dealer_5d_sum` | Dealer 5-day net buy sum |
| `short_margin_ratio` | Short balance to margin balance ratio |

Score outline:

| Condition | Score Delta |
|---|---:|
| Foreign 5-day sum positive and buy days >= 3 | +18 |
| Foreign 5-day sum negative and sell days >= 3 | -18 |
| Foreign 10-day sum positive | +8 |
| Foreign 10-day sum negative | -8 |
| Investment trust 5-day sum positive and buy days >= 3 | +20 |
| Investment trust 5-day sum negative and sell days >= 3 | -16 |
| Investment trust buy streak >= 2 | +10 |
| Dealer 5-day sum positive | +5 |
| Dealer 5-day sum negative | -5 |
| Short/margin ratio > 30% | +6 |

Status outline:

| Score | Status |
|---:|---|
| `>= 65` | Bullish chip state |
| `<= 40` | Weak chip state |
| otherwise | Neutral chip state |

## Credit Rules

Outline from existing documentation:

| Condition | Interpretation |
|---|---|
| `Short_Margin_Ratio > 30%` and `Close >= Price_20D_Max` | Potential short squeeze |
| `Margin_Ratio > 60%` | Overheated margin risk |
| otherwise | Normal credit state |

## Risk Rules

Outline from existing documentation:

| Condition | Interpretation |
|---|---|
| `Close < MA60` | Trend breakdown risk |
| `Margin_Ratio > 60%` | Margin overheating |
| `Bias20 > 15%` | Overextended price |
| `Low[0] > High[1]` | Gap-up exhaustion risk |
| otherwise | No major risk flag |

## Signals And Trade Plan

`generate_signals(kline, chip)` returns structured signal groups. `generate_trade_plan(kline)` returns an actionable plan for UI display.

Keep these outputs stable for the frontend:

- Prefer additive fields over renaming fields.
- Keep signal levels simple, such as `bullish`, `bearish`, `warning`, `neutral`.
- Include enough context for the UI to show human-readable reasons.

## AI Rule Context

`GET /api/ai/context/{stock}` builds a deterministic JSON package for AI analysis.

It combines:

- `stock_daily` kline rows and indicators.
- `chip_daily` recent rows and chip metrics.
- Existing perspective cards, signals, and trade plan.
- Data availability checks.
- Deterministic rule cards.
- Missing data warnings.
- `ai_prompt_template` for the AI model.

The endpoint is intentionally read-only. It does not call Groq or any AI provider directly; the caller should fetch this JSON and send it to the AI model with the prompt in `docs/AI_PROMPT.md`.

Data that can currently be judged:

| Area | Available |
|---|---|
| MA trend, RSI, MACD, Bollinger, volume-price | yes, from `stock_daily` |
| Prior 20-day high/low breakout | yes, from `stock_daily` |
| Institutional 1D/5D/10D buy-sell | yes, from `chip_daily` |
| Margin balance, short balance, short/margin ratio | yes, from `chip_daily` when the daily job has written margin data |
| Short squeeze watch | yes, if short/margin ratio and price history exist |

Data that is not currently in Firestore and must not be inferred:

- 400-lot large holder ratio.
- Shareholder concentration.
- Retail shareholder count.
- Margin maintenance ratio.

## Backtest

`backtest_strategy(kline)` powers `/api/backtest/{stock}`.

When changing strategy logic:

- Keep the endpoint response shape backward compatible.
- Document assumptions such as entry condition, exit condition, holding period, fees, and slippage if they are added.
- Add or update tests before using backtest output for decisions.
