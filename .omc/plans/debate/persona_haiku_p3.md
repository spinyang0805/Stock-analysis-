# Haiku 5.5 (max) — persona P3 (composite re-scope, faithful summary)
Corrections: OBVS per-stock fitted results were MIXED, not "always lost": 5 validations per-trade excess vs 0050
+1.97, −0.36, −1.46, +1.26, −0.80% (2:3); 2022–23 sign flips between universes. The 8–11 CUSTOM-param stocks were positive
in all 5 (+6.4…+9.3%/trade) but chosen ex post with no same-stock default-param control. combo brief figures exclude slippage.
SBL from 2022 (+120 rows warm-up), TDCC only 2025-10-23→ — live S1 & strict3 use them → frozen versions drop whale/sbl fields.
Situations: S1 buy["main"] (frozen, no whale) ~2–3/stock/yr; S2 env+trig+money & no CHIPS 10d; S3 OBVS mode-5 raw turn_on & env;
S4 OBVS mode-2 channel & trig 3d. Exits: X1 strict3 (−10% stop, no sbl/whale); X2 new: peak −8% & OBV EMA20<60; X3 fixed 20d.
Form: choose (c) at TYPE level (volatility tercile × trend/revert, research_persona.traits), not per stock. (a) per-stock:
S1 only 6–9 events per stock per 36m → shrink ≈0.2, zero stocks meet N≥30 → no power. (b) least likely. (c) per type
500–750 events / 36m, paired exit differences cancel entry noise; prior still leans null.
Composites raise the average edge but give no reason for more persistent per-stock differences; fewer events → wider CIs.
Prereg: control = pooled exit chosen by same rolling 36m; fixed X1, X3 arms; shuffle type labels same-day within return-
correlation clusters; warm-up 2019–21, dev 2022, test 2023-01-02→2026-10-16, sealed forward 2026-10-21→2027-10-20.
Each type ≥200 entries & ≥20 stocks, total ≥500, each half ≥100; paired diff ≥ +1pp net; block bootstrap lower >0;
BY q ≤.05 (12 cells); account beats 0050 and frozen strict3, MDD ≤ 0050; NT$20 min fee. Variants V1 main (choose X1/X2/X3
per type, must beat X1 by ≥1pp to switch), V2 expanding, V3 volatility only, V4 X2→break_only; V2–V4 same sign.
Abandon: insufficient; any V1 failure; either half negative; sign flips; vanishes within clusters; account fails; forward ≤0;
type churn > 1/3 monthly.
BLOCKING: type may proxy industry/recent momentum (TPEx no industry) → before freezing: TWSE type×industry crosstab
(pre-test), return-correlation clusters, correlation of type with trailing 12m return. Also: event counts; X2 vs obvs.trades check.
Advice: history = screening only; evidence = sealed forward period.
