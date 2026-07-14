# 資料管線修復規格書（2026-07-14）

## 背景

GitHub Actions workflow `update-data.yml`（每日 15:40 TW）負責：更新 DB → 匯出靜態 JSON → commit push。
目前 3 次 run 全部因 120 分鐘 timeout 被 cancel，**export + commit 步驟從未執行過**。

### 已確認的根因（勿重新診斷，直接依此實作）

| # | 問題 | 證據 |
|---|------|------|
| P1 | TPEx 批次日K端點 `https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyCloseQuotes` 已死，回 HTML 404 頁 | probe run 29237356931：所有日期格式（roc-slash/yyyymmdd/iso/no-date/csv）皆回 `404 - 證券櫃檯買賣中心` HTML；同路徑風格的 `dailyTrade` 正常，代表是端點改名/移除，非 IP 封鎖 |
| P2 | TPEx 個股月K `https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock` 活著但參數錯 | probe 回 `{"stat":"參數輸入錯誤"}`（HTTP 200, JSON）。jobs.py 現有 5 組 params_candidates 全部無效 |
| P3 | heal 必超時：實測 ~26 秒/檔 × 743 檔 ≈ 5.4 小時 | run 29243559572：`[heal] 25/743` 每 25 檔約 11 分鐘 |
| P4 | heal 排在 export 之前、無時間預算 → timeout 砍掉整個 job，export 永遠跑不到 | workflow 步驟順序 + 3 次 cancelled run |
| P5 | `healed` 計數造假：`run_on_demand_backfill` 寫 0 筆也不 raise → 下市股每天重掃重試 | log `healed=100 failed=0` 但清單開頭 9962/9951/8942… 全是 yfinance「possibly delisted」；cnt=0 的股隔天仍在清單 |
| P6 | `run_daily_update` 54 分鐘 + valuation 21 分鐘，本身就吃掉大半預算 | run 29243559572 時間戳：10:40→11:35→11:55 |

因 P1，上櫃 ~800 檔每天拿不到日K → 永遠 gappy → heal 永遠扛全量。**修好 P1 是最高優先，修好後 gappy 數會自然掉到個位數。**

## 修改範圍

只動這些檔案：
- `backend/jobs.py`
- `backend/daily_job.py`
- `backend/probe_tpex.py`（探測候選端點用，可自由改）
- `.github/workflows/update-data.yml`
- `.github/workflows/probe.yml`

**禁止**：動前端、動 `export_static_json.py` 邏輯、fly deploy、改 DB schema（新增欄位/表除外，見 T4）。

## 任務

### T1. 修 TPEx 批次日K（P1）— 最高優先

用 probe workflow 實測找出可用端點，候選依序：

1. **TPEx OpenAPI（首選，官方文件化）**：`https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes`
   - 回 JSON array，只有最新交易日（與 TWSE STOCK_DAY_ALL 同限制，`_write_tpex_day` 已有以資料自身日期為準的邏輯，沿用）
   - 注意欄位名是英文（`SecuritiesCompanyCode`, `Close`, `Open`, `High`, `Low`, `TradingShares`, `TransactionAmount`, `Date`…以實測為準）
   - ETF/ETN 若不在 mainboard 端點，查 `tpex_` 開頭其他 openapi 端點（如 etf 相關），沒有就先接受只有股票
2. www 站新路徑候選（若 OpenAPI 欄位不足才需要）：用 probe 測 `afterTrading/` 下的其他名稱變體

實作：
- `jobs.py` `TPEX_ALL` 改指到實測可用端點，`_write_tpex_day` 適配新回應格式（欄位對映、日期格式 `Date` 可能是 `1150713` ROC 或 ISO，以實測為準）
- 舊格式解析留著無妨，但新端點為主路徑

### T2. 修 TPEx 個股月K 參數（P2）

用 probe 實測 `tradingStock` 正確參數，候選：
- `{"code": stock_id, "date": "2026/07/01", "response": "json"}`
- `{"code": stock_id, "date": "115/07/01", "response": "json"}`
- `{"code": stock_id, "date": "115/07", "response": "json"}`
- 帶/不帶 `id=""`、`l=zh-tw` 的組合
- 也測 openapi 有無個股歷史端點

找到後把 `fetch_tpex_stock_month` 的 params_candidates 換成正確組合（保留 yfinance fallback）。

### T3. heal 加時間預算 + 真實計數（P3, P4, P5）

`daily_job.py`：
- 新增 `--heal-budget-min N`（預設 45）：`heal_stale_stocks` 內每檔開始前檢查累計耗時，超過預算即停止並印 `[heal] budget exhausted at i/N`
- `healed` 只在 `result["written_days"] > 0` 時 +1；寫 0 筆算 `noop`，分開計數印出
- 排序維持 cnt ASC（最缺的先補）

`update-data.yml`：
- heal 預算由 workflow input 傳入（default 45）
- **順序不變**（DB 更新 → heal → export → commit），因為有預算保證 export 一定跑得到
- `timeout-minutes` 維持 120

### T4. 下市股跳過（P5）

- DB 新增表（不動既有表）：
  ```sql
  CREATE TABLE IF NOT EXISTS heal_blacklist (
    stock_id TEXT PRIMARY KEY,
    noop_count INT NOT NULL DEFAULT 0,
    last_attempt DATE NOT NULL
  );
  ```
- heal 每檔結束：written=0 → upsert noop_count+1；written>0 → 刪除該列
- heal 掃描 query 排除 `noop_count >= 3` 的股票
- 這樣下市股 3 天後自動不再浪費時間，若哪天復活（真的有資料）會被移出黑名單？——不會，因為被排除就不再嘗試。可接受：黑名單股若要救回，手動 `DELETE FROM heal_blacklist WHERE stock_id=...`。在 daily_job.py docstring 註明此事

### T5. probe.yml 增強（給 T1/T2 用）

- `probe_tpex.py` 改成測 T1/T2 的候選端點清單，印 status + content-type + 前 300 字
- 跑法：push 後 `gh workflow run probe.yml`，`gh run watch` 看結果，依結果定案端點/參數，再改 jobs.py
- **這是本規格的實測迴圈：先 probe 定案，再寫解析code，不准憑猜測寫解析**

## 驗收標準

1. probe run 證明：新 TPEx 批次端點回 JSON 且含當日上櫃股票列（附 run id）
2. probe run 證明：`tradingStock` 正確參數回個股月K JSON（附 run id）
3. `gh workflow run update-data.yml` 手動 dispatch 一次完整 run：
   - status = success（非 cancelled）
   - log 有 `[heal] done` 或 `[heal] budget exhausted`
   - log 有 `tpex` 日K寫入筆數 > 500（`[daily] stocks=` 含上櫃）
   - **Export static JSON 步驟執行完成**
   - **Commit & push data 步驟執行完成**（repo 出現 `data: daily update 2026-07-14` commit，`public/data/` 有檔案）
4. 隔天（或再 dispatch 一次）heal 掃描的 gappy 數字明顯下降（743 → 預期 <100，因上櫃日K恢復 + 黑名單生效）

## 執行順序

1. T5 probe 增強 → push → dispatch probe → 讀結果
2. T1 + T2 依 probe 結果實作 → T3 + T4 同批實作
3. push → dispatch update-data.yml → watch 至完成 → 對驗收標準逐項打勾
4. 若 P6（run_daily_update 54 分鐘）導致總時間仍超 120 分：不要優化 P6，改把 `--lookback` 降到 3 並記錄在回報中（P6 優化留待下次）

## 回報格式

逐項驗收標準附證據（run id、log 行、commit hash）。失敗項說明卡在哪、已嘗試什麼。

## 注意

- commit 訊息中文、格式照 repo 慣例（`fix:` / `feat:` / `chore:`），結尾加 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- 直接 push main（repo 慣例）
- `.omc/` 與 `__pycache__` 的髒檔不要一起 commit
- DATABASE_URL 在 `backend/.env`（本機測試用）與 GH secret（已設好）
