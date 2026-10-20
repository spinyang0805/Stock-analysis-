# Stock-personality (股性) study — Round P1: propose situations + stock classification (PROPOSAL ONLY)

You are one of four round-table models (Claude Opus moderator, Codex Sol, Codex Luna, Claude Haiku). This round is
PRE-REGISTRATION: propose, do NOT run any backtest and do NOT look at forward returns. Read-only; do not modify files.
Answer in Traditional Chinese (zh-TW); keep code identifiers in English.

## The user's hypothesis (take it seriously)
Each stock has a 股性 that shows in how it reacts to recurring situations — e.g. "touches a rising 月線/季線 and bounces,
and when big players (法人/大戶) come in at the same time it surges". These reactions are in the history, are sampled far
more often than buy/sell trades (a 月線 touch can occur 15–30×/yr), and drift over time, so the profile must be rebuilt
periodically from past data only. Earlier evidence (for context, not to dismiss the idea): per-stock picking of the
BEST trading-rule parameters showed ~0 persistence; single signals averaged −0.37%/trade; the shared 組合拳 rule
+0.86–1.80%/trade but a capital-limited account lost to 0050 (2020–26).

## Data available (point-in-time, daily unless noted)
- history/prices_YYYY.parquet: OHLCV, ~2019→2026-10 for the study universe (2017 partial, 33 ids).
- history/chips_YYYY.parquet 2019+: 外資/投信/自營 net buy, 融資/融券.
- history/sbl_YYYY.parquet 2022+ (借券). history/revenue.parquet (monthly revenue, use publish lag ≥ 10th of next month).
- history/market_index.parquet: TAIEX/TPEx 1999+.
- history/tdcc.parquet: 集保大戶 weekly — ONLY 2025-10 → 2026-10 (too short for history; may be listed as "later").
- Existing signal library: backend/tuning/indicators.py (BUY_DESC / SELL_DESC ~75 buy + ~30 sell definitions,
  e.g. touch_ma20_up, touch_ma60_up, touch_ma240_up, pat_w_bottom, chip_streak_3, trust_first, rev_high12, rs_lead,
  dry_touch_ma20, lshadow_ma20, ma_squeeze_brk, gap_support ...). Prefer reusing these definitions.
- Shared combo rule: backend/tuning/research_combo.py (`masks`).

## Deliver exactly these sections
### A. Situation list (≤ 12 base situations + ≤ 6 combinations)
For each: id, plain zh-TW name, exact computable definition (reuse indicators.py names where possible, state
thresholds), why it should matter (market theory: support/resistance, institutional flow, earnings drift, etc.),
expected yearly frequency per stock (rough), and the outcome to measure after it (e.g. 20-day return vs 0050,
max adverse excursion, hit = +X% before −Y%). Combinations must be things like "touch rising 季線 AND 法人 5-day net buy".

### B. Stock-personality classification (股性分類)
Propose 4–7 types (e.g. 均線反彈型, 突破動能型, 籌碼驅動型, 營收驅動型, 區間震盪型, 弱勢/無股性). For each: a measurable
rule computed only from past data (which situations' stats decide membership; minimum samples), what a stock of that
type should be traded on, and how often to re-classify. A stock may be "no reliable personality" — say how that is
decided. State how you avoid a stock being labelled by luck (multiple-testing correction, half-split stability, minimum
N, shrinkage toward the all-stock average, etc.).

### C. Pre-registered test (how to check the classification is real)
Key control: the SAME situations applied to ALL stocks without per-stock selection. Name pass/fail criteria, sub-periods
(2019–22 and 2023–26), re-profile cadence (monthly, all-past vs 3-yr window), costs (10 bp slip + real fees/tax),
and the portfolio-level comparison vs 0050 and the shared 組合拳. Keep the variant count small and disclosed.

### D. Your top 3 risks / objections to this whole idea, and what result would make you abandon it.

Be concrete and concise (≤ 900 words of prose plus tables).
