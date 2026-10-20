# Haiku 5.5 (max) — persona P2 (faithful summary)
Common flaws: no β/stock-drift removal; 0050 benchmark for small-cap events; sample vs effect thresholds mismatched; no
industry control in shuffle. Verified: evaluate.py:30 slippage 0 & research_combo.py:163 Costs() → combo +0.86/1.80% excl.
10bp (~0.2pp overstated). Revenue availability is the 11th in code (fetch_extra.py:5), not 10th. run_tuning.py:803 survivorship.
Decisions: a) 10 base: touch_ma20_up, touch_ma60_up, dry_touch_ma20, chip_streak_3, chip_p90, trust_first, brk_20,
ma_squeeze_brk, rs_lead, bb_lower_back; combos C1 touch_ma20_up+chip_streak_3, C2 touch_ma60_up+chip_streak_3,
C3 brk_20+chip_streak_3 (drop ma240, revenue, W-bottom). b) A and B both within last 5 sessions (recent(…,5)), first-true
day, no recount within 20d. c) 20d, t+1 open→t+21 open; event benchmark = PIT equal-weight pool; 0050 only for accounts.
d) β-adjust (120d), residualize year×regime, then stock×family double-centred θ; shrink with method-of-moments τ̂ (floor 0).
e) no separate global gate. f) 36m window, monthly, N≥30, halves ≥10, θ̃ ≥ +1pp, raw net >0, BY q ≤ .10; 3 months in/out.
g) warm-up 2019–21, dev 2022 (pipeline only), main test 2023-01-02→2026-10-16, sealed forward 2026-10-21→2027-10-20.
h) event: ≥500 labelled events/≥20 stocks; Δ (selected − non-selected, same year×regime×family) ≥ +1pp raw, β-adj >0,
monthly-block bootstrap 95% lower >0; 1,000 date×industry shuffles p ≤ .05; both 2023–24 and 2025+ >0; robust to top
stock and limit-up entries; selected net >0. Account (seeds 0–199 median): CAGR > 0050, > all-stock control, > combo;
MDD ≤ 0050. Variants V1 main, V2 expanding window, V3 θ̃ .5, V4 1.5, V5 combo exits (report only); V2–V4 same sign.
i) fixed 20d. Account: NT$1M, ≤10, ≤10%, round lots, NT$20 min fee, ≤5% of 20d value, no limit-up fills, T+2.
j) abandon on any main failure / shuffle p>.05 / sign flip / zero members / forward 12m Δ ≤0 with ≥200 events.
BLOCKING: B1 TPEx chips 2019–21 may be the old bug (latest-day data) — check unique net_buy per stock; B2 survivorship
(pass = upper bound only); B3 industry field needed for shuffle (data.py:219, TPEx mostly "股票"); B4 feasibility count of
events (no returns) before freezing N; must explain theory to user and get OK before running.
