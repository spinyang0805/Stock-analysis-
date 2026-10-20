Read-only — do NOT modify any file. Repo root = current directory.

ROUND 7 — FINAL CONFIRMATION of the round table (Claude Opus moderator, Codex GPT-6.1-Sol, Codex GPT-6-Luna, Claude
Haiku). Read .omc/plans/debate/sol_r6.md, .omc/plans/debate/luna_r6.md, .omc/plans/debate/haiku_r6.md and the end of
.omc/plans/debate/preregistration_r4.md.

New since round 6:
- USER DECISION: the 35% drawdown limit is judged against a 2008-type crash. Disclosed to the user: a 2000–01-type crash
  (TAIEX 10,202 on 2000-02-17 → 3,446 on 2001-10-03, −66.2%, fetched from TWSE) would take a monthly-rebalanced 50% mix
  to about −40% (price index, no dividends); 2008 in the same simulation: 50% → −34.1%, 40% → −28.1%.
- Code frozen in git: commit ee120235f (research_etf_long.py, research_portfolio.py, research_combo.py,
  research_repro_vt.py = Codex's independent re-implementation, which reproduces the results).
- weekly-tuning.yml re-tunes per-stock params only for the holdings advice / old pick engine; the combo (組合拳) rules
  use none of them (fixed shared rules in research_combo.masks) — please verify in code.

Moderator's proposed FINAL (attack it):
 A. Real money now: 50% 0050 / 50% cash (cash in insured TWD demand/short deposit), rebalanced back to 50/50. 0% 00631L,
    0% combo, 0% per-stock tuning. Expectation: historical 2004+ ≈7%/yr, −32% max drawdown; planning range 3–7%;
    a 2000–01-type crash could reach ≈ −40%; 100% 0050 earned ≈13.8% with −56% (user's choice to stay at 35%).
 B. Rule 6 ("forward paper test before increasing real money") applies to rules that need validation (C8, VT15,
    combo); a static 50/50 allocation has no fitted parameter except the weight, which the user's own risk decision
    sets — so it is not blocked by rule 6. Challenge this if you disagree.
 C. Costs: monthly rebalancing of a small account may cost more than modelled (TW broker minimum fee ≈ NT$20/order,
    odd-lot trading). Should the rule be monthly (C1), quarterly (C10, registered, same historical result), or
    "rebalance only when the stock weight drifts outside 45–55%" (new, not registered)? Choose without adding new
    research.
 D. Forward paper tracking from the first trading day after 2026-10-19 of C1 (benchmark), C8 and VT15, frozen, per
    Sol/Luna/Haiku protocols (merge them into ONE protocol: 60 months minimum, ≥20% 0050 drawdown required, paired
    12-month block bootstrap 2,000 draws seed 7 vs C1, 2.5th pct > 0, forward MDD ≤ 30%; no tuning, no restarts).
 E. Website: show this allocation guide (current target, drift, next rebalance date) and the paper-tracking panel.

Answer (Traditional Chinese, Taiwan, numbered, max ~45 lines):
1. Accept / amend A–E (state the exact amendment).
2. The single merged forward-test protocol (final wording, ≤10 lines).
3. Last line exactly either "no remaining blocking doubt" or "BLOCKING: <reason>".
