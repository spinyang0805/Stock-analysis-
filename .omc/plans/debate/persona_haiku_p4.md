# Haiku 5.5 (max) — persona P4 (faithful summary)
(a) insufficient — agree (structural: 20-day spacing caps ~12.5/yr; prior per-stock −0.44%/trade t −10 (5y), −0.56% t −4.5 (8y)).
Choose T-exit with POOLED main test; forward period is the only decision evidence. Entry C1 (frozen: drop whale_conc /
whale_400_jump); spacing by signal date only (so arms are paired). Types vol tercile × trend (5-day-sampled autocorr > 0),
expanding window (rolling 36m = V1). Exits X1 strict3 minus s_whale_disp/s_sbl_surge (renamed, ≠ live), X3 fixed 20d, both
−10% close stop; X2 dropped (new rule, unchecked). Switch from X1 to X3 per type only if X3 net ≥ +1pp with ≥100 closed trades.
Main test (T − pooled control) paired ≥ +1pp, 60d block bootstrap lower > 0, both 2023–24 and 2025–26 > 0, ≥500 events/20
stocks, shuffle within industry × return-cluster 1,000×, p ≤ .05; per-type BY descriptive only. Forward 2026-10-21→2027-10-20:
pass = point ≥ +1pp & lower > 0; fail = upper < +1pp; else undecided (no extension). Variants V0–V4 (rolling, 1.5pp, 15bp,
vol-only). Prior pass probability ~10–20%. Power: per-type tests near zero unless σ_d ≈ 5pp.
Abandon: ANY pre-check failure (type counts, type×industry, type×trailing-12m return, survivorship, C1 recount) → none.
BLOCKING B1 survivorship, B2 frozen-C1 recount + revenue vintage, B3 limit-down fills, B4 round lots (NT$100k per slot only
buys ≤ NT$100 stocks), B5 TPEx code check, B6 explain theory to user + get OK. Notes: research_persona 'group' (same traits)
result should lower T prior; memory plan A still uses per-stock engine for exits — user decision; CUSTOM = 13 stocks in obvs.py.
