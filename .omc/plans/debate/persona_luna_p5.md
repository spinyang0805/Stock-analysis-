## 1. 套用 P4 停止規則

- **Sol：STOP(insufficient)。** P4 規則是 “per type per window ≥200 events, ≥20 stocks, each 18m half ≥100”。0T 只有 48/58 個月末通過全部門檻，1T 為 57/58；照「每個窗口」要求，不能算通過。Sol 另寫 “type churn > 1/3”：12 個月換型比例 50.8% 超過門檻，但月換型平均是 25.7%；P4 沒指定 churn 的計算期間，所以「不穩定」標籤有歧義，樣本門檻已足以停止。[Sol P4](D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/persona_sol_p4.md)

- **Luna：STOP(confounded)，且也 STOP(insufficient)。** P4 寫「若任一表 V>0.25……停止並判 confounded/insufficient」。產業表未校正的 V=0.278，已超過門檻；不改用 bias-corrected V。其六類每月樣本門檻也未全數通過。[Luna P4](D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/persona_luna_p4.md)

- **Haiku：本報告下 STOP(insufficient)，沒有可確認通過的預檢。** 規則是 “ANY pre-check failure (type counts, type×industry, type×trailing-12m return, survivorship, C1 recount)”。其凍結 C1 要移除兩個 TDCC OR 條件，但目前報告沒有該版 C1 的重算；不能把這份原 C1 預檢當成它已通過。[Haiku P4](D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/persona_haiku_p4.md)

**結論：T 不通過 P5。沒有任何 P4 persona 設計在這份預檢中取得通行；persona 線關閉，維持 shared combo rules。** 既有 8 年 group 結果（+0.64% 對 pooled +1.17%）方向一致，但不是本次預檢的停損依據。

## 2. O 的用途

O（13 組 CUSTOM OBVS 對 DEFAULT、相同股票與日期）**可作為清楚標示的 ex-post 描述檢查**：呈現這些指定組合在該批日期的差異。它不能驗證 persona／type 效果、代表其他股票或期間，也不能把已停止的 T 重新變成預註冊測試。

## 3. 尚存阻擋項目

停止結論本身沒有阻擋疑點；若繼續其他策略研究，仍須處理：2026 年停止交易的 60 檔尚未核實、`prices_2018` 缺檔，以及 C1/X1 資料版本和停牌／跌停成交規則。Haiku 的凍結 C1 重算也尚未完成。[預檢報告](D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/persona_work/persona_pretest.md)