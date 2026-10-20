1. **裁判結果：組合拳目前沒有通過 0050 這道門檻。** N10 組合拳年化 **20.3%、MDD −52%**，0050 是 **28.5%、−34%**；已測改良也沒有同時勝過 0050。逐筆平均 +7.5% 不能取代帳戶複利結果。[portfolio_r2.md:7](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:7) [portfolio_r2.md:8](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:8)

2. **Claude Opus：核心判斷確認，舊初跑結果以修正後數字為準。** 把逐筆報酬改成受限資金的組合淨值來裁決是對的；而且即使零滑價，H_slip0 也只有 21.5% CAGR、−51% MDD，仍輸 0050。Round 1 的 NaN 初稿不應再作依據。[portfolio_r2.md:18](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:18)

3. **Codex Luna：大方向多數確認，部分主張尚未被測到。**
   - 閒置資金放 0050 有改善，但 B 仍是 **25.7%、−47%**，不足以過關。
   - 快速熊市出場 D 是 **26.9%、−48%**：比基礎組合 CAGR 高，卻仍輸 0050，且 2022 年 −32%。這支持「別把兩段熊市當成已證實優勢」。
   - 排名優先 G 只有 **14.3%、−52%**，反駁目前實作的 rank-priority；但 top-100 硬過濾不是同一測試，不能說它已被這張表單獨否證。
   - 0.5% 風險配置加 ATR 停損的近似版 E 是 **11.1%、−35%**；Luna 的 2.5 ATR、產業上限與熊市新倉半額並未原樣測試。F 的移動停利版本只有 **8.2%、−52%**，足以把停利線暫時踢出候選。[portfolio_r2.md:11](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:11) [portfolio_r2.md:14](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:14) [portfolio_r2.md:15](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:15) [portfolio_r2.md:16](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:16) [portfolio_r2.md:17](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:17)

4. **Claude Haiku：裁決規則正確；240 日均線版本被數據反駁。** CAGR 必須勝過 0050、MDD 不得更深，正是我採用的門檻。C_expo_ma240 只有 **17.7%、−48%**；N=5、10、20 也都沒過關，N20 的 −44% 回撤仍比 0050 深。滑價有影響，但零滑價組合仍遠落後，解釋不了主要差距。[portfolio_r2.md:10](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:10) [portfolio_r2.md:12](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:12) [portfolio_r2.md:18](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:18)

5. **我修正 Round 1 的優先順序。** 我原先把市場曝險分層、風險配置、ATR 停損、獲利移動停利和排名優先都列為改良方向。現在撤回 ATR sizing、移動停利與排名優先作為「提高報酬」的候選；市場分層是相對最有希望的一項，但 C_expo_regime 仍是 **26.8%、−37%**，兩個門檻都沒過。快速出場也不再列入最終候選。

6. **落後原因：** 組合拳在 2020–23 有優勢，卻錯過 2024–26 的指數行情：基礎組合 2024 **+15%**、2025 **−19%**、2026（截至 10 月）**+20%**；0050 分別 **+49%、+37%、+70%**。這與小中型股訊號組合追不上大型權值股主導的指數走勢相符；元大官方資料顯示 0050 在 2026/10/19 的台積電持股權重為 **56.85%**。但回測報告沒有持股市值／產業歸因，這是合理解釋，尚非已完成的歸因分析。[portfolio_r2.md:25](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/.omc/plans/debate/portfolio_r2.md:25) [元大投信 0050 持股比重](https://www.yuantaetfs.com/product/detail/0050/ratio)

7. **低勝率與名額競爭也會拖累複利。** N10 只有 27% 勝率、270 筆交易、46% 虧損月份、−52% MDD。低勝率本身不證明策略無效——少數大贏家可能拉高平均單筆——但真正的帳戶 CAGR 已顯示贏家不足以補回回撤與錯過指數上漲。名額有限時，程式只按 20 日相對強度排序候選；這可能排掉後來的大趨勢股。[research_portfolio.py:178](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:178)

8. **程式限制與偏誤：**
   - regime cap 只限制新開倉空位，不會按新上限賣掉舊持股；所以 +26.8%/−37% 測到的是「限制進場名額」，不是嚴格曝險上限。[research_portfolio.py:171](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:171) [research_portfolio.py:175](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:175)
   - 快速出場以五日內兩個 sell signal 計數，不是兩個證據類別；型態別名可能重複計票。[research_portfolio.py:76](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:76)
   - 缺少市場資料會被填成 regime 0，買進條件仍放行；而持倉依 `close_ff` 用最後已知收盤價估值，停牌時也不產生新出場判斷。兩者都可能讓風險看起來較低。[research_combo.py:123](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_combo.py:123) [research_portfolio.py:160](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:160)
   - 0050 基準未扣買進費用或滑價，偏袒基準；但這點不足以解釋 8.2 個百分點的 CAGR 差距。反過來，閒置 0050 的買賣也沒套用 slip 參數，會高估該版本。[research_portfolio.py:150](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:150) [research_portfolio.py:275](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:275)
   - 組合 cap 用 TAIEX MA60；個股買進環境則從個股資料的市場指數計算，OTC 可用 TPEx，定義並不完全一致。成分股只在訊號日套用 universe 篩選；存活者偏誤也未由這段程式排除。[research_portfolio.py:243](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:243) [research_portfolio.py:230](/D:/個人資料/投資理財/股票網站分析/Stock-analysis-/backend/tuning/research_portfolio.py:230)

9. **下一個只登錄一個候選，做 24 個月紙上前瞻測試：**組合拳買進四層條件維持原設定（股價在 MA60 上且 MA60 高於 20 日前；3 日內突破、5 日內量能、10 日內籌碼／RS 或 40 日內營收確認）；最多 10 檔、每檔新倉為帳戶淨值 10%，候選按 20 日 RS 排序。TAIEX regime 為多／中／空時，新開倉上限固定 **10／5／2 檔**，不因 cap 收緊強制賣舊倉；閒置現金一律停泊 0050，需要資金才賣出所需份額。保留 strict-B／stage 4／收盤跌破進場價 10% 出場，隔日有效開盤成交、跳空漲幅達 9.5% 不追；缺市場資料停買。所有股票與 0050 交易都計入 10 bp 單邊滑價、手續費與證交稅，並限制單筆成交額不超過 20 日均成交額 5%。不調參、不加快速出場、ATR sizing、移動停利或排名過濾。**通過條件：同期間、同成本的 0050 基準下，CAGR 嚴格較高且 MDD 不深於 0050；任一失敗就淘汰。**

10. **實務裁決：現在不值得用真金白銀跑組合拳，連小額衛星部位也沒有被這份證據支持。**目前角色應是 **signal-only**；實際配置選 0050 買進持有。只有上述凍結規則的前瞻測試同時通過 CAGR 與回撤門檻，才重新考慮實盤。全程只讀，未執行回測、未修改檔案。