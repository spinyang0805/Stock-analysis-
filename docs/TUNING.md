# 個股指標選擇與參數調校（tuning engine）

程式在 `backend/tuning/`，用途是替每檔股票從固定的指標庫中挑出樣本外（OOS）有效的買進訊號、持有天數與出場規則，再由每日評分程式找出當天觸發的股票。只用 Python（pandas/numpy），不寫入資料庫，所有 DB 查詢都在 READ ONLY 交易中執行。

> 目前 DB 與 Parquet 只有約 1.4 年（2025-06 起）的完整日K，試跑結果**不可靠**，只能拿來驗證流程。5 年歷史回補完成後要重跑一次，結果才有參考價值。

## 檔案

| 檔案 | 內容 |
|---|---|
| `data.py` | 讀價量與籌碼（`--source parquet` 讀 `history/*.parquet`，`--source db` 唯讀讀 Supabase）、清理、point-in-time 選股池、持股 |
| `adjust.py` | 除權息、分割、減資、面額變更還原（FinMind 或交易所公告，加上交易所參考價），並統計覆蓋率 |
| `indicators.py` | 向量化指標與固定訊號庫（22 個買進、7 個出場），以及盤中觸發價計算 |
| `evaluate.py` | 事件研究、成本、BH-FDR、cluster t、分組、shrinkage、非重疊交易 |
| `run_tuning.py` | 調校 CLI，輸出 `history/out/stock_params.{parquet,json}` 與 `tuning_report.md` |
| `score_today.py` | 每日評分，輸出 `history/out/recommendations_YYYYMMDD.{json,md}` |
| `proposed_schema.sql` | 建議新增的 Supabase 精簡資料表（尚未套用） |
| `tests/test_tuning.py` | pytest：無前視（截斷與擾動未來資料）、成本、還原、BH、PIT 選股池、nested 選擇、fold、JSON、快取 |

## 使用方式

```bash
cd backend                      # 或在 repo 根目錄設 PYTHONPATH=backend
python tuning/run_tuning.py --source parquet --universe 500 --workers 4      # 每月
python tuning/score_today.py --source parquet                                # 每日收盤後
python -m pytest tuning/tests -q
```

`run_tuning.py` 常用參數：

- 選股池與資料：`--universe`（預設 500）、`--rebalance-days 21`、`--years 5`、`--end-date YYYYMMDD`（用截至某日的資料調校）
- 走勢檢驗：`--horizons 3,5,10,20`（單位為該股的交易列數）、`--train-days 504`、`--min-train-days 189`、`--test-days 63`（同時也是內層驗證長度）
- 可靠度門檻：`--n-min 30`、`--min-train-events 10`、`--fdr-q 0.10`、`--shrink-k 20`
- 成本：`--fee-discount 0.6`
- 共用參數：`--holdings` 或 `--no-holdings`、`--adjust-source finmind|exchange`、`--finmind-budget`、`--finmind-interval`、`--finmind-offline`、`--exchange-budget`、`--out-dir`

`score_today.py` 另有：

- `--as-of YYYYMMDD`：日期若早於參數檔的 as_of 會直接拒絕執行，因為那樣不是有效的回測。要回測請先用 `run_tuning --end-date` 產生當時的參數。
- `--include-unreliable`：診斷用，結果會標示「未通過驗證」。

## 資料處理

1. **來源**：`--source` 必須明確指定，不會自動切換。持股一律從 Supabase `transactions` 唯讀計算。`side` 只認白名單：`buy`、`stock_dividend` 計為加，`sell` 計為減，`cash_dividend` 等其他值忽略；淨股數 > 0 視為持有。
2. **清理**：
   - 部分日期的 `TWSE STOCK_DAY_ALL` 列其實是別日資料的複製（週末、端午節都有）。同一檔股票 OHLC 與成交量完全相同的列，只保留逐日官方來源。
   - 交易日曆：平日，排除 manifest `nontrading_dates`，且當日筆數至少是常態的 30%。
   - 每檔只保留最後一段連續資料，中間斷超過 20 個營業日就切開。
   - 同一天同時有官方列與 yfinance 列時採用官方列。只有 yfinance 的日期，用下一筆官方列的參考價（收盤 − 漲跌）接回原始價位；接不回的段落維持 Yahoo 還原值，並記入 `yf_unanchored_rows`。
3. **還原權值**（`adjust.py`），用 `--adjust-source` 選擇除權息事件來源：
   - `finmind`（預設）：FinMind `TaiwanStockDividendResult`，每檔查 1 次。免費額度每小時約 300 次，用完（402）就停止查詢；設定 `FINMIND_TOKEN` 可提高額度。
   - `exchange`：證交所 `TWT49U`（除權除息計算結果表）與櫃買 `exDailyQ`，每個市場每月查 1 次，涵蓋全部股票，額度不是問題。以 272 檔已有 FinMind 資料的股票比對 2025-06～2026-10：事件 548/548 完全一致、因子差異 0；交易所多 1 筆（00992A）。
   - 兩種模式都會另外用 FinMind 補兩類事件：`TaiwanStockParValueChange`（全市場只查 1 次）；`SplitPrice` 與 `CapitalReductionReferencePrice`，只在仍有缺口時才查。交易所公告不含分割、減資與面額變更。
   - 快取：`history/cache/finmind/`（7 天）與 `history/cache/exchange/`（已結束的月份永久保存，當月 1 天）。寫入方式是先寫暫存檔再 `os.replace`，所以不會留下寫一半的檔；快取檔損壞時自動重抓。連續 3 次網路錯誤就停止查詢。
   - 另外用官方列的參考價推算事件（收盤 − 漲跌 ≠ 前收），並用「漲跌為空值」的除權息日標記交叉驗證。
   - 往回還原：`adj_t = raw_t × Π(after/before)`，只乘 t 之後的事件，所以最新價格等於原始價格。指標與報酬都用還原價；顯示給使用者的價格用原始價。
   - 單日變動超過 10.5% 卻找不到對應事件的缺口，只標記在 `adjust_coverage.csv` 與報告的「需人工確認」，不自動修補。
4. **籌碼**：外資 + 投信買賣超。|買超| 大於成交量，或與前一日完全相同的列視為無效；有效資料不到 120 天的股票不計算籌碼訊號。

## 選股池（point-in-time）

每 21 個交易日重排一次，取當時前 60 個交易日平均成交值前 N 名的 4 碼非 ETF 股票。排名只用重排日前一天以前的資料；未上市期間的成交值以 0 計。

- 事件只有在訊號日該股屬於當時選股池才納入選擇與評估。
- 等權基準也只用同一批股票。
- 持股不進入選擇池、基準與 OOS 檢定，只用來產生出場警示與盤中價位；不在選股池的持股永遠不會被標為可靠。

## 指標庫（固定、有限）

- 趨勢／突破：MA 5/20、10/60、20/60 黃金交叉；收盤突破前 20／55 日最高；MACD(12,26,9) 黃金交叉（含零軸下版本）。
- 拉回：RSI14 低於自身歷史 P10／P20；KD(9,3,3) 在 K<20／<30 時黃金交叉；收盤跌破布林(20,2)下軌；MA20 乖離低於自身 P10／P20。
- 量能：量比（對前 20 日均量）>1.5／2.0 且收紅；OBV 創 60 日新高且收紅；量縮（前 5 日最低量比 <0.6）後放量收紅。
- 籌碼：外資＋投信連 3／5 日買超；5 日買超比高於自身 P80／P90。
- 出場：跌破 MA20、MACD 死亡交叉、RSI 高於自身 P80／P90、跌破 22 日最高收盤 − 2／2.5／3×ATR14。停利＝該股所選訊號歷史 MFE 的 P60（樣本內資料，僅供參考）。

「自身歷史百分位」只用 t−1 以前的 250 日（至少 60 日）計算。水準型條件只在第一次成立的那天算一次事件。測試用兩種方式確認沒有前視：截斷未來資料，以及大幅擾動未來資料，t 日以前的訊號都必須完全不變。

## 評估方法

- **事件**：t 日收盤得知訊號，t+1 開盤進場，t+h 收盤出場，h 以該股的交易列數計。t+1 開盤漲幅 ≥ 9.5%（買不到）的事件略過。同一股票、同一訊號、同一 h 的事件至少間隔 h 列。
- **成本**：手續費 0.1425% × 折扣（預設 0.6），買賣各收一次；證交稅股票 0.3%、ETF 0.1%。一買一賣合計約 0.47%。
- **超額**：同一進出場窗口下，以 point-in-time 選股池的等權平均報酬為基準。選擇與判定都用「扣成本淨報酬 − 基準」（xnet）；排序用每持有日 xnet，讓不同持有天數可以公平比較。
- **Walk-forward**：
  - train 2 年（資料不足時最少 9 個月，報告會標記）→ 測試接下來 3 個月 → 往前滾動。
  - train 事件的出場日必須早於 test 起點（purge）。
  - 候選訊號在 train 期間至少要有 10 個事件（`--min-train-events`）才會進入選擇與樣本內 BH。
- **Shrinkage**：分組＝波動度三分位 × 上市／上櫃（ETF 另成一組），分組只用 train 期間的資料。個股分數 = w·個股 + (1−w)·同組，w = n/(n+k)。
- **層級選擇（nested，只用 train）**：
  - train window 的最後 `test_days` 當內層驗證：前段選訊號，後段驗證，比較個股專屬、同組、全市場統一三種層級的 xnet，每檔取最好的層級。
  - 內層驗證期間沒有交易時，預設用同組。
  - 層級決定後，以完整 train window 重新選訊號，再交易 test。test 資料完全不參與層級或訊號選擇。
- **交易模擬**：同一檔股票同一時間只持有一筆部位，前一筆出場後才能再進場。因此每檔的交易數就是有效樣本數。跨股票合併的統計用 cluster-robust t：訊號日每 20 個交易日為一個 cluster，報告中的「有效 n」就是 cluster 數。
- **報告中同時做 OOS 的策略**：adaptive（上述 nested 層級）、個股專屬、同組、全市場統一（baseline）。報告直接寫出個股專屬是否勝過 baseline。

## 防過度擬合規則（可靠度判定只看 OOS）

一檔股票要標為可靠，它在 adaptive 策略下的 OOS 紀錄必須同時符合以下條件：

1. 屬於目前的選股池（`in_universe`）；
2. 非重疊 OOS 交易數 ≥ `n_min`（預設 30；調低時報告會標記）；
3. 平均淨報酬 > 0，且淨超額 > 0；
4. 有交易的 fold 中至少 2/3 的淨超額為正；
5. 單邊 OOS p 值（淨超額 > 0）在**跨全部股票**的 Benjamini–Hochberg 檢定中通過（q=0.10）。交易數不足 n_min 的股票以 p=1 計入，屬於保守做法。

各檔每個閘門是否通過記在 `gates` 欄位。樣本內 BH 結果只當資訊（`train_fdr_pass`），不作為判定。未通過的股票標為「no reliable signal」，不會被推薦。

信心值 = 層級權重（stock 1.0／group 0.8／uniform 0.6）× n/(n+k) × 正向 fold 比例 × min(1, 超額 t/3)。

## 輸出

- `stock_params.json`／`.parquet`：每檔的 level、gates、reliable、buy_signals（訊號、h、train 統計與 `train_fdr_pass`）、best_h，以及 OOS 指標（n、n_eff、平均、中位數、勝率、t、xnet、xt、單邊 p、MAE/MFE 中位數、fold 數）。另含 confidence、tp_pct、sell_rule、資料品質、as_of。JSON 中 NaN 一律寫成 null（`allow_nan=False`）。
- `tuning_report.md`：設定、警示、四種策略的 OOS 對照、閘門通過數、最常選中的訊號、還原覆蓋率、前 20 名。
- `oos_trades.parquet`、`adjust_coverage.csv`、`data_quality.csv`：稽核用。
- `recommendations_YYYYMMDD.json`／`.md`：
  - 今日觸發的股票：`ref_close`（今日收盤，原始價）、進場方式（次日開盤，開盤漲幅 ≥ 9.5% 不追）、`stop_pct`／`take_profit_pct`（相對實際成交價的百分比），以及「若以參考收盤成交」時對應的停損與停利價、持有日（交易列數）、OOS 勝率與 n、資料品質旗標。
  - 持股出場警示。
  - 明日盤中觸發價：突破 20／55 日高、MA5/MA20 與 MA20/MA60 交叉價、觸及布林下軌、跌破 MA20、追蹤停損。
  - 排序 = OOS 平均淨報酬 ÷ |MAE 中位數| × 信心；NaN 一律視為 0。

## 限制

- **歷史太短**：目前約 340 個交易日，扣掉選股池排名需要的 60 日後，只能切出 2 個 OOS fold，穩定度與 BH 檢定幾乎沒有統計力。
- **存活者偏差（殘留）**：選股池雖然是 point-in-time，但資料只包含目前仍存在於 stock_daily 的代號。回測期間已下市、資料又被刪除的股票不在樣本內。
- **還原缺口**：交易所來源不含分割、減資與面額變更，這幾類仍靠 FinMind 補查；FinMind 額度不足時會留下缺口，報告會標記。新上市股票前 5 日沒有漲跌幅限制，也會出現在「未解釋跳空」清單中。
- **yfinance 補值**：接不回原始價位的段落可能有些微誤差；暫停交易期間 Yahoo 也會補平盤列。
- **籌碼來源混雜**：`local_backfill` 與 T86／TPEx 的口徑可能不同，百分位型籌碼訊號要特別小心。
- **停利、MAE 參考值**：用樣本內事件計算，只是參考，不是 OOS 數字。

## 5 年資料到位後

1. `python backend/history_store.py pull`（或 backfill），確認 `history/prices_*.parquet` 涵蓋 5 年。
2. 執行 `run_tuning.py --source parquet --universe 500 --adjust-source exchange`。改用 finmind 也可以，但額度不夠時要分次跑，讓快取慢慢補齊。
3. 檢查報告：fold 數應 ≥ 10，`SHORT HISTORY` 警示應該消失；看「需人工確認」清單與閘門通過數。
4. 確認個股專屬或 adaptive 是否真的勝過 baseline，再決定前端與每日排程要採用哪一層。
5. 排程：`run_tuning` 每月一次、`score_today` 每日收盤後。輸出確認沒問題後，再考慮套用 `proposed_schema.sql`。
