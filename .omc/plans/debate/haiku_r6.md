# Claude Haiku (max) — Round 6 (summary, faithful)
Verified hashes; ran codex_repro_vt.py (13.93/−55.75, 10.74/−33.56, 7.09/−32.25); C7 7.77/−34.96; C8 10.4/−27.3
(2004–14 3.7%, 2015+ 17.1%, avg exposure 71%). VT15's matched control should be fixed 82.8% (not 90%).
1. Withdraws own C7 (0.04-pt margin = boundary pick on one path = overfit). C9 drifts — no.
   Stress tests (disclosed as defined after seeing 2008): (a) 2008 ×1.2; (b) 0050 −60% over 19 months, monthly
   rebalance. 40%: 30.0/30.3; 45%: 33.2/33.4; 50%: 36.2/36.4; 55%: 39.2/39.3. Breaking crash depth: 50% ≈ −58%,
   45% ≈ −63%, 40% ≈ −66%. Proposal: fixed 45% monthly (highest weight passing both stresses, chosen by stress not
   return); 40% if the 2000–01 crash exceeds ~63%. Cost: 45% ≈ 0.7 pt/yr less than 50%, ≈7 pts less than 100%.
2. User message drafted (45%, 6.4% historical; 40% if 2000–01 deeper; 100% only if −50%+ acceptable); planning range
   3–6%; C8 only paper.
3. VT15 not tracked (rejected; matched control: 2004–14 6.7 vs 5.7, 2015+ 14.7 vs 17.1). C8 frozen paper challenger vs
   fixed 45%: monthly reproducibility check; −35% pause; at ~2031-10 (60 months) paired 12-month block bootstrap 5th
   pct > 0 to consider switching. C8's period-dependent edge is an overfitting warning.
4. Cash: liquid short-term (savings or ≤1-month MMF); not 3-year deposits; interest taxed; plan without the 1%.
5. Combo/per-stock: 0% real; if ever: equal weight, max 5, ≤20% each, no per-stock tuning. Conflict: weekly-tuning.yml
   re-tunes per-stock params every Sunday → disable or freeze if combo paper-tracking must avoid per-stock tuning.
6. BLOCKING: (1) 2000–01 crash unverified (decides 45% vs 40%) [moderator: fetched — TAIEX 10,202 (2000-02-17) →
   3,446 (2001-10-03) = −66.2%; monthly-rebalanced mixes: 40% −33.5%, 45% −36.9%, 50% −40.3% (price index, no
   dividends)]; (2) rule-2 metric ambiguity (blocks C8 only); (3) code not frozen [moderator: committed ee120235f];
   (4) weekly per-stock re-tuning vs "no per-stock tuning" (blocks combo paper only).
