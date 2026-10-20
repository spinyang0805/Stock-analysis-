## 1. Per-stock (a)

**同意：**36 個月門檻下，C1–C8 每個月末的合格 `stock×situation` cell 都是 0；(a) 判 `insufficient`，不放寬門檻。

## 2. 凍結設計：T，只測每個 type 的 exit choice

六個 type 為 `volatility tercile × trend/revert`，依 `research_persona.traits` 使用訊號日前可得資料分類。固定共同進場為 **C1 `buy["main"]`**，比較每個 type 的 `X1 strict3`、`X2 peak −8% & OBV EMA20<60`、`X3 fixed 20d`。

選 exit choice 的理由：C1 有 **11,803** 個去重事件、涉及 **1,200** 檔；粗估 36 個月約 4.4k 個事件，均分約 740/type，作為可行性線索足夠，且三種 exit 可在同一批 entry 上配對比較。這只是總量推估，實際 type 分布尚未計數。C2 有 11,386、C5 有 6,612、C6 有 2,724 個事件；本輪不加 entry 選擇，以免增加搜尋並混入進場差異。

不選 O：8–11 檔 ex-post 選股集只支持對那些股票的 paired DEFAULT 比較，不能支持更廣的 type 結論。不選 P：本輪凍結的是 type-level 股性假說。

## 3. 相對 P3 的凍結 delta

| 項目 | 凍結規格 |
|---|---|
| 估計單位 | 從 per-stock `θ` 改為六個 type 的 exit choice；每月用 trailing 36m 比較同一 C1 entry 的配對 exit 差異。這是另一個假說，不改寫 (a) 的判定。 |
| 可行性門檻 | 每個 type 至少 200 個可成交 paired events、20 檔股票；前後各 18m 至少 100 個事件；全體至少 500。六個 type 都須達標，按 `fill_ok`、20-session 去重及前一筆已出場規則計數；任一不足即 `insufficient`，不合併 type、不延長窗口。 |
| 選擇與檢定 | 每 type 只在 `X2` 或 `X3` 相對 `X1` 的扣成本差異 ≥1pp、60-session block-bootstrap CI 下界 >0、兩半皆正時切換；連續 3 個月達標才切換，首個不達標月退回 `X1`。12 個 type×exit 比較採 Holm α=.05、BY q≤.05，另做 1,000 次分層 label shuffle。 |
| 產業與群聚 | 用入場日可得的兩位數 industry code（TWSE、TPEx）及凍結的 return-correlation clusters；shuffle 限於 date×market×industry×cluster。若分層無足夠可交換樣本，判 `insufficient`。 |
| 測試前檢查 | 先做 type×industry、type×trailing-12m-return-quintile 交叉表，檢查 Cramér’s V、占比和共同支持；若任一表 V>0.25、單一類別占某 type >50%，或缺乏跨 type 比較支持，停止並判 `confounded/insufficient`。 |
| 封存與變體 | 2019–2021 暖機／建 cluster，2022–2026-10-20 用於 rolling profile；2026-10-20 凍結規則與 type→exit mapping，封存評估期為 **2026-10-21→2027-10-20**，期間不重選 exit。僅留 V1 expanding-window、V2 volatility-only 兩個敏感度版本；不得挽救主規格。 |

主規格還須保持 P3 的策略層門檻：至少一個 type 穩定採用非 `X1`，兩半同向、分層檢定通過；帳戶須勝過 pooled `X1` 與 `0050 TR`，且 MDD 不高於 `0050`。成本及帳戶限制沿用 P3。

## 4. BLOCKING

設計可凍結；**測試前仍有兩項阻擋**：報告尚無六 type 的實際 36m／分半／可成交計數；type×industry 與 type×trailing-12m-return 的預檢也尚未提供。兩者只能按上述固定門檻通過或停止。