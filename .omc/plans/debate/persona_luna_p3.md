## P3 合併預註冊（唯讀；未跑回測、未計算報酬）

### 1. 複合情境

以下事件只由 [research_combo.py 的 `masks()`](D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_combo.py:112) 圖層與 [obvs.py 的 `DEFAULT`／mode state](D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/obvs.py:18) 組成。表中的每股年事件數是**規劃推估，尚未計數**。

| ID | 固定定義 | 推估事件／股／年 |
|---|---|---:|
| `CP_main` | `buy["main"]`：`edge(env & trig & money & other)` | 1–3 |
| `CP_money_no_other` | `edge(env & trig & money & ~other)`，即環境、突破、資金俱全，但沒有籌碼／RS／營收確認 | 2–8 |
| `OBVS_m5_stage2` | `mode=5` 的 `turn_on`，且 `layers["stage2"]` 為真 | 1–5 |
| `OBVS_m2_money` | `mode=2` 的 `turn_on`，且 `layers["env"] & layers["money"]`；OBV 通道突破後維持至通道低點出場 | 0–3 |
| `OBVS_m3_CPcore` | `mode=3` 的 `turn_on`（OBV-RSI 上穿 55），且 `env & trig & money` | 0–2 |

`stage2 = aclose > MA60 且 MA60 > 20 日前 MA60`；`env` 另要求 `mkt_regime` 有效且不等於 −1。`trig` 是近 3 日的 `TRIGGER`；`money` 是近 5 日 `MONEY` 或 `obv_on`；`other` 是近 10 日 `CHIPS`／`RS` 或近 40 日 `REV`。清單沿用程式常數：`TRIGGER={brk_20,brk_55,pat_w_bottom,pat_n_up,tl_down_break,ma_squeeze_brk,bb_squeeze_brk,gap_up}`；`MONEY={obv_high,vol_20_up,vol_15_up,dry_expand}`；`CHIPS={foreign_streak_3/5,trust_streak_3/5,trust_first,chip_streak_3/5,chip_p80/90,whale_conc,whale_400_jump,margin_drop_up}`；`RS={rs_high,rs_lead,rs_resilient}`；`REV={rev_high12,rev_yoy_accel,rev_yoy_3up}`。營收依可用日延後，最早採次月 11 日。

OBVS 全部固定用 `DEFAULT`，只替換 `mode`，不挑 13 組 `CUSTOM`。不另測 mode 1（金叉已包含於 combo `money`）或 mode 4（價格轉折與 mode 5 重疊）。事件取訊號日，次日 `fill_ok` 才可成交；每股每情境至少間隔 20 個交易日，且前一事件出場後才計下一件。

### 2. 「股性」主定義

選 **(a) 每股對固定複合情境的反應交互作用 θ**。用 `X20`（次日開盤至第 21 個交易日開盤的扣成本報酬，減 `0050 TR`）估計 stock×situation 交互作用，扣除股票、情境與日期效應；固定收縮 `θ̃ = n/(n+30) × θ`。這最直接檢驗股性，也保留與 P2 的可比性。

組合拳同時要求環境、突破、資金與額外確認，理論上能少收雜訊事件、提高事件平均效果的訊噪比；但若反應對所有股票相同，`θ` 仍是零。條件越多，事件越少，交互作用標準誤越大、收縮越強。既有單訊號的半段交互作用 CI 全跨 0；OBVS 個股客製組也未勝 `0050`。因此 (a) 先驗偏弱；(b) 個股參數選擇最不被既有證據支持。 (c) 出場風格相對較少被直接否定，但容易混入入場與出場效果，本輪不選。

### 3. 門檻、期間與比較

採 P2 圓桌提案中**最嚴格門檻**，不因複合事件較少而放寬：

- 每個 `stock×situation`：36 個月窗 `N≥30`，前後半各 `≥15`；`θ̃≥+1pp`、兩半皆正、淨超額為正、60 日區塊校正 CI 下界 `>0`，每月 BY `q≤.05`；連續 3 個月通過才進入，連續 3 個月未通過才退出。
- 全域異質性 gate：每情境以 1,000 次標籤洗牌校準，跨 5 情境 Holm `α=.05`。主要事件比較至少 **500 個獨立事件、20 檔股票**；相對同情境全股票控制 `U` 的增益 `≥1pp`、校正 CI 下界 `>0`，`ρ≥.10` 且 CI `>0`，去除最大貢獻股後仍為正。2023、2024、2025–2026-10-20 三段皆須為正。
- 歷史分段：2019 暖機；2020–22 開發／累積；2023 首個完整 profile／開發檢查；2024、2025–2026-10-20 為 walk-forward audit，**不是乾淨 holdout**。同一股性假說已看過同一段資料，最終確認只看封存的 **2026-10-21 至 2027-10-20**；沿用相同門檻，不延長期間。
- 比較臂：`P` 股性、`U` 同複合情境套用全股票、`0050 TR`、標籤洗牌 `L`、共享 combo。`P` 固定 20 日持有；帳戶規格 NT$1M、最多 10 檔、單檔≤10%、整張。共享 combo 凍結 P2 的 `main/main + 10% stop`；另報目前 `main/strict3 + 10% stop`，`P` 必須勝過兩者及 `U`、`0050`，且 MDD 不高於 `0050`。
- 全臂成本明定 `Costs(slippage=0.001)`：每邊滑價 10bp、手續費 0.1425%×60% 折扣、個股賣出稅 0.3%、ETF 賣出稅 0.1%。洗牌分層為日期×情境×市場狀態×交易所；TWSE 用產業碼，TPEx 用 2019–22 報酬相關 cluster 凍結分群。若無足夠可交換標籤，該檢定列 `insufficient`。

**可行性先行：**只計訊號事件、去重數、每股分布與分半數；不讀取或計算任何報酬。門檻現在固定，事件數只判斷是否可行；不足即報 `insufficient`，不降低 `N`、`q`、效果門檻或延長封存期。研究範圍限定於目前上市股票 cohort，不外推下市股或全市場。

### 4. 變體與放棄條件

最多五項：`V0` 主規格；`V1` expanding-window profile；`V2` 更嚴格 `θ̃≥1.5pp`；`V3` 滑價壓力測試 15bp／邊；`V4` `strict3` combo 帳戶比較。變體不能救回主檢驗，也不新增訊號或搜尋 OBVS 參數。

全域 gate、事件數、校正效果、持續性、稽核期符號、去最大貢獻股或帳戶比較任一主條件失敗，即放棄股性層；封存期若事件數不足，結論就是 `insufficient`。

### 5. 剩餘 blocker

**事件可行性尚未計數**，目前不能確認任何情境能達到 P2 門檻。執行前只做上述 count-only 審核；這是唯一未解的執行 blocker。