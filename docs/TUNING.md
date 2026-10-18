# 個股指標選擇與參數調校（tuning engine）

程式在 `backend/tuning/`。用途：替每檔股票從固定的指標庫中挑出歷史上「樣本外」有效的買進訊號、持有天數與出場規則，再由每日評分程式找出今天觸發的股票。純 Python（pandas/numpy），不寫入資料庫，所有 DB 查詢都是 READ ONLY。

> 目前 DB／Parquet 只有約 1.4 年（2025-06 起）的完整日K，試跑結果**不可靠**，只能驗證流程。等 5 年歷史回補完成後再跑一次，才有參考價值。

## 檔案

| 檔案 | 內容 |
|---|---|
| `data.py` | 讀價量／籌碼（`--source parquet` 讀 `history/*.parquet`；`--source db` 讀 Supabase，唯讀）、清理、選股池、持股 |
| `adjust.py` | 除權息／分割／減資／面額變更還原（FinMind 公開資料 + 交易所參考價），覆蓋率統計 |
| `indicators.py` | 向量化指標與固定訊號庫（22 個買進、7 個出場），盤中觸發價計算 |
| `evaluate.py` | 事件研究、成本、BH-FDR、分組、shrinkage、walk-forward 選擇 |
| `run_tuning.py` | 調校 CLI → `history/out/stock_params.{parquet,json}`、`tuning_report.md` |
| `score_today.py` | 每日評分 → `history/out/recommendations_YYYYMMDD.{json,md}` |
| `proposed_schema.sql` | 建議的 Supabase 精簡表（未套用） |
| `tests/test_tuning.py` | 無前視、成本、還原、BH 的 pytest |

## 使用方式

```bash
cd backend
python tuning/run_tuning.py --source parquet --universe 500 --workers 4     # 每週或每月
python tuning/score_today.py --source parquet                               # 每日收盤後
python -m pytest tuning/tests -q
```

常用參數：`--universe`（預設 500）、`--years`（預設 5）、`--horizons 3,5,10,20`、`--train-days 504`、`--min-train-days 189`、`--test-days 63`、`--n-min 30`、`--fdr-q 0.10`、`--shrink-k 20`、`--fee-discount 0.6`、`--holdings 2330,0050` 或 `--no-holdings`、`--finmind-budget`／`--finmind-interval`／`--finmind-offline`、`--out-dir`。`score_today.py` 另有 `--as-of YYYYMMDD`、`--include-unreliable`（診斷用，結果會標示「未通過驗證」）。

## 資料處理

1. **來源**：`--source` 必須明確指定，不會自動切換。持股一律從 Supabase `transactions` 唯讀計算（買進 + 配股 − 賣出 > 0）。
2. **清理**：
   - 某些日期的 `TWSE STOCK_DAY_ALL` 列其實是別日資料的複製（週末、端午節都有），同一檔股票 OHLC 與成交量完全相同的列，只保留逐日官方來源那一列。
   - 交易日曆＝平日、非 manifest `nontrading_dates`、筆數 ≥ 常態的 30%。
   - 每檔只保留最後一段連續資料（中間斷超過 20 個營業日就切開），避免 2017、2023 的零星探測列造成假報酬。
   - 同一天同時有官方列與 yfinance 列時用官方列。只有 yfinance 的日期（約 9%）：Yahoo 價格已經還原，所以每段用下一筆官方列的參考價（收盤 − 漲跌）接回原始價位；找不到錨點的段落維持 Yahoo 還原值，並計入 `yf_unanchored_rows`。
3. **還原權值**（`adjust.py`）：
   - 事件來源：FinMind `TaiwanStockDividendResult`（每檔 1 次）、`TaiwanStockParValueChange`（全市場 1 次），以及只在仍有缺口時才查的 `TaiwanStockSplitPrice`、`TaiwanStockCapitalReductionReferencePrice`。快取在 `history/cache/finmind/`（7 天），請求間隔預設 2 秒，遇到額度錯誤就停止，改用快取與參考價推算。設定 `FINMIND_TOKEN` 可提高額度。
   - 另外用官方列的參考價（收盤 − 漲跌 ≠ 前收）推算事件，並用「漲跌為空值」的除權息日標記交叉驗證。
   - 往回還原：`adj_t = raw_t × Π(after/before)`，只乘 t 之後的事件，所以最新價＝原始價。指標與報酬使用還原價，給使用者看的觸發價、停損、停利都是原始價。
   - 單日變動超過 10.5% 卻找不到對應事件的缺口只會標記（`adjust_coverage.csv`、報告的「需人工確認」），不會自動修補。
4. **籌碼**：外資 + 投信買賣超。|買超| 大於成交量，或與前一日完全相同的列視為無效。有效資料不到 120 天的股票不計算籌碼訊號。

## 指標庫（固定、有限）

- 趨勢／突破：MA 5/20、10/60、20/60 黃金交叉；收盤突破前 20／55 日最高；MACD(12,26,9) 黃金交叉（含零軸下版本）。
- 拉回：RSI14 低於自身歷史 P10／P20；KD(9,3,3) 在 K<20／<30 黃金交叉；收盤跌破布林(20,2)下軌；MA20 乖離低於自身 P10／P20。
- 量能：量比（對前 20 日均量）>1.5／2.0 且收紅；OBV 創 60 日新高且收紅；量縮（前 5 日最低量比 <0.6）後放量收紅。
- 籌碼：外資+投信連 3／5 日買超；5 日買超比高於自身 P80／P90。
- 出場：跌破 MA20、MACD 死亡交叉、RSI 高於自身 P80／P90、跌破 22 日最高收盤 − 2／2.5／3×ATR14。停利＝該股所選訊號歷史 MFE 的 P60。

「自身歷史百分位」一律只用 t−1 以前的 250 日（至少 60 日）計算，沒有前視。水準型條件只在第一次成立的那天算一次事件。

## 評估方法

- **事件**：t 日收盤得知訊號，t+1 開盤進場，t+h 收盤出場，h ∈ {3, 5, 10, 20}。t+1 開盤漲幅 ≥ 9.5%（買不到）的事件略過。同一股票、同一訊號、同一 h 的事件至少間隔 h 日，避免持有期重疊灌水 t 值。
- **成本**：手續費 0.1425% × 折扣（預設 0.6）買賣各一次，證交稅股票 0.3%、ETF 0.1%。來回約 0.47%。
- **超額**：同一進出場窗口下，全體選股池等權平均報酬作為基準。選擇與可靠度判定都用「扣成本淨報酬 − 基準」（xnet），避免把大盤上漲誤認為訊號有效。排序用每持有日 xnet，讓 3 日與 20 日公平比較。
- **Walk-forward**：train 2 年（資料不足時最少 9 個月，報告會標記）→ 選擇 → 下一個 3 個月測試 → 往前滾動。只計樣本外（OOS）。train 事件的出場日必須早於 test 起點（purge）。
- **Shrinkage**：分組＝波動度三分位 × 上市／上櫃（ETF 獨立一組，以 train 期間資料分組）。個股分數 = w·個股 + (1−w)·同組，w = n/(n+k)，k 預設 20。
- **三種策略同時做 OOS**：個股專屬（每檔前 3 名）、同組統一、全市場統一（baseline）。報告會直接列出個股專屬是否勝過 baseline。

## 防過度擬合規則（可靠度判定）

一檔股票要標為可靠，所採用層級的 OOS 結果必須同時滿足：

1. OOS 交易數 ≥ `n_min`（預設 30；調低時報告會標記）；
2. OOS 平均淨報酬 > 0 且淨超額 > 0；
3. 有交易的 fold 中至少 2/3 的淨超額為正；
4. 所選訊號至少一個在最後 train window 通過 Benjamini–Hochberg FDR（q=0.10，個股層級跨該股全部候選；組或全市場層級跨該層候選），或 |t| > 3；
5. 個股專屬層級另外要勝過同組與 baseline 在同一檔股票上的 OOS 淨超額。

依序嘗試：個股專屬 → 同組 → 全市場統一；都不通過就標「no reliable signal」，不會推薦。信心值 = 層級權重(1.0／0.8／0.6) × n/(n+k) × 正向 fold 比例 × min(1, 超額 t/3)。

## 輸出

- `stock_params.json`／`.parquet`：每檔的 level、buy_signals（訊號、h、train 統計）、best_h、OOS 指標（n、平均、中位數、勝率、t、超額、MAE/MFE 中位數、fold 數）、confidence、group_fallback、tp_pct、sell_rule、資料品質、as_of。
- `tuning_report.md`：設定、警示、三策略 OOS 對照、層級分佈、最常選中的訊號、還原覆蓋率、前 20 名。
- `oos_trades.parquet`、`adjust_coverage.csv`、`data_quality.csv`：稽核用。
- `recommendations_YYYYMMDD.json`／`.md`：今日觸發（觸發價、停損、停利、持有日、OOS 勝率與 n、資料品質旗標）、持股出場警示、明日盤中觸發價（突破 20/55 日高、MA5/MA20 與 MA20/MA60 交叉價、觸及布林下軌、跌破 MA20、追蹤停損）。排序 = OOS 平均淨報酬 ÷ |MAE 中位數| × 信心。Markdown 只有數字，AI 說明之後由其他 CLI 補上。

## 限制

- **歷史太短**：目前約 340 個交易日，只能切出 2 個 OOS fold，穩定度檢定幾乎沒有統計力。
- **存活者偏差**：選股池用今天的成交值前 N 名，回測期間下市或變冷門的股票不在樣本內。
- **還原缺口**：FinMind 免費額度每小時約 300 次，第一次跑 500 檔需要分兩次（快取會累積）。未取得資料的股票只能靠參考價推算，並標記 `finmind=unavailable`。
- **yfinance 補值**：無官方錨點的段落可能有些微誤差；暫停交易期間 Yahoo 也會補平盤列。
- **籌碼來源混雜**：`local_backfill` 與 T86／TPEx 的口徑可能不同，百分位型籌碼訊號要特別小心。
- **選股池與市場狀態**：試跑期間台股大多上漲，若只看淨報酬，幾乎所有訊號都「有效」；因此判定改用超額報酬。

## 5 年資料到位後

1. `python backend/history_store.py pull`（或 backfill）確認 `history/prices_*.parquet` 涵蓋 5 年。
2. 先跑 `run_tuning.py --source parquet --universe 500 --finmind-budget 280`，必要時隔一小時再跑一次，補齊 FinMind 快取。
3. 檢查報告：fold 數應 ≥ 10，`SHORT HISTORY` 警示應消失；看「需人工確認」清單。
4. 確認個股專屬是否真的勝過 baseline，再決定前端與每日排程採用哪一層。
5. 排程：`run_tuning` 每月一次、`score_today` 每日收盤後；確認輸出後再考慮套用 `proposed_schema.sql`。
