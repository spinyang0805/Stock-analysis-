# Codex Sol — persona P1 (faithful summary)
Persona = a stock's persistent DIFFERENT reaction vs other stocks in the same situation.
Outcome Y: decide at t close, enter t+1 open, exit t+21 open; X20 = after-cost return − 0050; also MAE20, +6% before −4%
(tie → −4% first; descriptive only). Count only edge (false→true) days.
A. Base: S01 touch_ma20_up, S02 touch_ma60_up, S03 touch_ma240_up, S04 dry_touch_ma20 (VR<0.7), S05 lshadow_ma20, S06 brk_55,
S07 ma_squeeze_brk (VR>1.2), S08 chip_streak_3, S09 trust_first, S10 rev_high12 (published ≥ 10th next month, else 11th),
S11 rs_lead, S12 bb_lower_back. Combos (same day): C01 S02 + 5-day net buy > 0; C02 S04 + S09; C03 S06 + VR>1.5 + S08;
C04 S11 + S10 within 20 days. Missing chips ≠ 0; ≥120 valid rows. TDCC/SBL deferred.
B. Families: MA-support (S01–05), breakout (S06–07), chips (S08–09), revenue/strength (S10–11), oversold (S12), none.
Monthly rebuild, 3-yr window, matured events only; within a family merge same-day, keep events ≥20 sessions apart; N ≥ 24,
each half ≥10. μ_shrunk = μ_pool + N/(N+30)(μ−μ_pool), pool excludes the stock. δ = vs other stocks same family/year/
market state. R: μ_shrunk>0, shrunk δ ≥ 0.5pp, δ>0 in both halves, 60-day block resample, BH over stock×5 families q ≤0.05.
Multi-membership allowed; uniform 20-day hold; no per-stock exits.
C. Main test: labelled stocks vs unlabelled stocks in the same situation, future X20. 2019–22 warm-up; 2023–cutoff
walk-forward; 2 windows (3-yr main, all-history sensitivity); plus 12 months sealed forward. Portfolio: NT$1M, ≤10 names,
≤10% each, round lots, t+1 open, no fills when halted/limit-locked, exit after 20 sessions. Four accounts: persona, shared
situations, 0050 B&H, combo main/main + 10% stop. 10bp slip + real fees/min fee/tax; 0050 dividends reinvested.
Pass (all): ≥100 independent events over ≥10 stocks, labelled−unlabelled ≥ 0.5pp with corrected 95% CI lower > 0;
positive in 2023–24 and 2025–cutoff and after dropping the top contributor; persona account CAGR > 0050, shared, combo with
MDD no worse than 0050; 12-month forward positive (insufficient → undecided).
D. Risks/abandon: (1) overlapping events → few independent samples → abandon per-stock, study pooled reactions;
(2) persona = market/industry exposure → abandon if gone after year/market-state control or not better than permuted labels;
(3) drift faster than estimable → abandon if halves differ in sign or CI upper < 0.5pp; if events work but account loses to
0050 and combo → not a standalone strategy.
