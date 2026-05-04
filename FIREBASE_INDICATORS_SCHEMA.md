# Firebase Indicators Schema (Perspective Analysis)

## analysis_cache/{stock_id}

```json
{
  "perspective_cards": [
    {
      "category": "trend",
      "title": "趨勢面",
      "status": "多頭排列",
      "level": "bullish",
      "meaning": "短中長期均線依序向上，股價趨勢偏強。",
      "logic": "MA5 > MA20 > MA60"
    },
    {
      "category": "volume_price",
      "title": "量價面",
      "status": "量增價漲",
      "level": "bullish",
      "meaning": "成交量放大且股價上漲，代表買盤積極。",
      "logic": "Volume > Volume_MA5 AND Price_Change > 3%"
    },
    {
      "category": "chip",
      "title": "籌碼面",
      "status": "投信連續買超",
      "level": "bullish",
      "meaning": "投信鎖碼股，容易形成波段走勢。",
      "logic": "trust_buy_streak >= 3"
    },
    {
      "category": "credit",
      "title": "信用交易",
      "status": "軋空條件成立",
      "level": "strong_bullish",
      "meaning": "券資比偏高且股價突破壓力，可能軋空。",
      "logic": "Short_Margin_Ratio > 30% AND Close > Price_20D_Max"
    },
    {
      "category": "risk",
      "title": "風險面",
      "status": "融資過高",
      "level": "warning",
      "meaning": "融資過高，跌破MA60可能引發斷頭。",
      "logic": "Margin_Ratio > 60% OR Close < MA60"
    }
  ]
}
```

## 設計原則
- 所有分析卡片由 backend 計算
- Firebase 僅存結果（避免前端計算）
- frontend 只負責顯示
- 支援 fallback（避免資料未到）

## 對應 API
- `/api/analysis/{stock}`
- `/api/dashboard/{stock}`

## 說明
此 schema 直接對應 UI 的「多面向分析卡」，
每個卡片 = 一個分析維度 + 狀態 + 解釋 + 邏輯。
