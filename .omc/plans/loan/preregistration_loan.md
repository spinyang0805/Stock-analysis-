# Pre-registration — loan-funded ETF plan (written BEFORE running, 2026-10-20)
User facts: personal loan 2,000,000 @ 2.9%, 120 months (payment 19,220/mo), salary contributes 0 (all payments from the pot).
User's own plan U: 200k bank (payments), 300k reserve, 1,500k in stock ETF (0050/006208/009816 → modelled by 0050 total return).
Data: 0050 TR month-end closes (codex_repro_vt.load_prices), 10-year windows starting every month 2004-01 … 2016-10 (154, overlapping).
Costs: buy 0.1425%×0.6 + 10bp, sell same + 0.1% tax. Cash 1.5%/yr (sensitivity 0%). Monthly granularity.
Payments: withdrawn from cash; if cash is 0, sell stock. Ruin = stock+cash ≤ 0 before month 120 (remaining payments out of pocket).
Set 1 — weight (lump sum, rebalanced quarterly to w of current pot, payments from cash leg): W40, W50, W60, W75; plus U (no rebalancing).
Set 2 — entry for U's 1.5M stock leg: A lump (=U); B 6 equal monthly tranches; C 12 tranches; D staged: 1/3 now, 1/3 when 0050
  month-end close ≤ 0.90 × running high since start, 1/3 at ≤ 0.80 × high; anything not invested by month 12 goes in at month 12.
Metrics per variant: terminal pot after loan repaid (min, p10, median), share < 0, share < 500k, ruin count, worst mid-way pot.
Decision rule: admissible = no ruin in any window AND p10 > 0; pick highest median; if within 5% of a simpler one, pick simpler
(simplicity order: U/A < W* < B < C < D). Report every variant. Window overlap means windows are not independent.
