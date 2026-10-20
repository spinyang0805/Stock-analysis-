# Round 1 — positions of the four models (faithful summaries; Luna's full text in luna_r1.md)

## Codex GPT-6.1-Sol (medium)
- Brief/code mismatch: STRUCT has 7 signals (no s_bb_lower_break); OTC stocks use TPEx index for the market filter;
  market MA computed on stock rows (suspensions distort 60 sessions); backtest universe refreshed every 21 days,
  live daily.
- Layers are not independent evidence (pattern aliases; breakout/OBV/RS all price-driven). Windows keep stale evidence
  (breakout that already failed 2 days later can still buy; 40-day-old revenue). Event vs state mixing
  (s_mkt_turn_bear fires only on the turn day).
- strict B may give back most gains; 10% stop is from cost only; 10% is a close trigger, not a loss cap (gaps,
  limit-down). Live evaluate() never exits on CAP if no sell signal; 1.6y window drops old entries.
- Per-trade averages ≠ portfolio. "Validation 2026-04..10" is not truly out-of-sample unless a frozen earlier
  version exists.
- Bear 240 fast-exit: logic OK, evidence too thin (2 episodes, +0.13%/trade); must keep strict-B/stage-4 exits and
  ADD the fast rule; exclude overheat signals from "2 sell signals"; test 4 arms + "only reduce exposure".
- Top improvements: (1) max 10 positions, risk 0.5% of equity per trade, weight min(10%, 0.5%/stop distance),
  sector ≤30%; (2) exposure caps by market regime 100/50/25% (no new buys in bear); (3) ATR stop
  clip(3·ATR14/close, 6%, 12%); (4) profit protection after +20%: best close − 4·ATR14 (only ratchets up);
  (5) monthly rank only as priority when cash is short (not a hard top-100 filter).
- Final test: NT$1M, max 10, 10% each, daily marked, T+1 open fills, 10 bp slippage (0/20 sensitivity), T+2 cash,
  0050 B&H same costs, CAGR/MDD/worst year/% losing months/Calmar/exposure; block-bootstrap by months.

## Codex GPT-6-Luna (max)
- Layers mostly "any-of" → correlated; missing market data → regime 0 → buys allowed (should block).
- strict-B alias counting (s_pat_bear_struct vs lh/ll); 10-day windows can stitch unrelated old warnings.
- Same execution caveats (close-trigger stop, zero slippage), live tracker CAP/1.6y issues, per-trade ≠ compounding,
  revenue 11th-of-month assumption, survivorship.
- Bear 240 fast-exit = likely overfit; if tested, count ≥2 different EVIDENCE CATEGORIES not aliases; report 2022
  and 2025 separately; prefer "halve NEW positions in bear" over fast exit.
- Top improvements: (1) monthly-rank top-100 filter + rank priority; (2) max 10, sector ≤30%, risk 0.5%,
  stop 2.5·ATR, position ≤10%; (3) bear: half-size new positions (compare with no change / fast exit);
  (4) profit trail after +2R: best close − 3·ATR; (5) conditional re-entry after 5-day cooldown with a NEW breakout.
- Final test: 2020-01..2026-10, NT$10M, max 10, idle cash variant in 0050 only while 0050 > rising 240-MA,
  10 bp slippage (5/20 sensitivity), volume cap 5% of 20-day traded value, monthly block CI.

## Claude Haiku 5.5
- Same per-trade vs portfolio point; zero slippage matters: +0.15%/trade vs 0050 is wiped out by ~0.6% round-trip
  slippage; honest current conclusion "about equal to the market, lags in narrow bull runs".
- +7.85% includes open trades; multiple comparisons (≈40 variants) → discount t.
- Bear fast-exit: not enough evidence; test EXPOSURE not exit.
- Improvements: (1) exposure tiers: TAIEX < 240-MA → max positions halved; below a falling 240-MA → no new
  entries; (2) N ∈ {5,10,20} chosen on 2019–23, validated 2024–26; (3) volatility sizing (1% risk or 1/ATR);
  (4) realistic stop fills + limit-down delay; (5) idle cash in 0050.
- Rejects the ranking filter (validation −1.9%).
- Verdict rule: portfolio CAGR must beat 0050 AND max drawdown must not be deeper — else don't ship.

## Claude Opus (moderator) — round-1 evidence: first capital-constrained run (one account, equal weight, no slippage)
2020-01-02 .. 2026-10-16. (An earlier draft had a NaN bug from suspended stocks; corrected numbers in round 2.)
