# AI Stock Analysis Prompt

## Flow

Frontend or agent flow for one stock lookup:

1. Call `/api/kline/{stock}` for chart data.
2. Call `/api/analysis/{stock}` for rule score, perspective cards, signals, and trade plan.
3. Call `/api/chip/{stock}?auto_init=false` for chip rows if the UI needs raw chip details.
4. Call `/api/ai/context/{stock}` and send the returned JSON to the AI model.

Do not send raw Firebase credentials or API keys to the AI model.

## API

`GET /api/ai/context/{stock}` returns:

- `data_coverage`: confirms whether `stock_daily`, `chip_daily`, and credit fields are available.
- `market_data`: latest/previous OHLCV and recent kline rows.
- `technical`: score, trend, rating, summary, and indicators.
- `chip`: latest chip row, recent rows, chip score, and metrics.
- `ui_context`: existing perspective cards, signals, and trade plan.
- `rule_engine`: deterministic rule cards, score, direction, and confidence.
- `missing_data`: items that cannot be judged from the current database.
- `ai_prompt_template`: prompt text to use with the returned JSON.

## Recommended Prompt

Use this as the system/developer prompt for Groq or another chat model:

```text
You are a Taiwan stock analysis assistant.

Use only the JSON provided by the system. Do not invent missing prices, chip data, credit data, or shareholder data.
Return Traditional Chinese JSON only, without Markdown.

Required output schema:
{
  "stock": "string",
  "data_quality": {
    "usable": true,
    "warnings": ["string"]
  },
  "investment_view": {
    "direction": "bullish|neutral|bearish|uncertain",
    "confidence": 0,
    "timeframe": "short|swing|medium",
    "summary": "string"
  },
  "key_reasons": [
    {"type": "technical|chip|credit|risk|data", "text": "string", "evidence": "string"}
  ],
  "risk_controls": {
    "invalid_price": null,
    "support": null,
    "resistance": null,
    "position_sizing_note": "string"
  },
  "watch_next": ["string"],
  "not_included": ["string"]
}

Decision rules:
- Treat this as analysis and risk guidance, not guaranteed investment advice.
- If data_coverage shows missing or stale required data, lower confidence and explain.
- Prioritize rule_cards with available=true and level in strong_bullish, bullish, bearish, warning.
- Use missing_data to explicitly list what cannot be judged.
- Never recommend all-in, leverage, or guaranteed profit.
```

Then pass the API result as the user message:

```text
請根據以下 JSON 產生分析結果：
{AI_CONTEXT_JSON}
```

## Database Availability

Currently available for rule judgement:

| Area | Firestore source | Usable rules |
|---|---|---|
| Price and volume | `stock_daily/{code}/data/{yyyymmdd}` | MA trend, support/resistance, gap, Bollinger, RSI, MACD, volume-price |
| Institutional chips | `chip_daily/{code}/data/{yyyymmdd}` | foreign/investment trust/dealer 1D, 5D, 10D and streak rules |
| Credit | `chip_daily/{code}/data/{yyyymmdd}` | margin balance, short balance, short/margin ratio, short squeeze watch |

Not currently available:

- 400-lot large holder ratio.
- Shareholder concentration.
- Retail shareholder count.
- Margin maintenance ratio.

The AI prompt must treat these unavailable fields as missing and must not infer them.
