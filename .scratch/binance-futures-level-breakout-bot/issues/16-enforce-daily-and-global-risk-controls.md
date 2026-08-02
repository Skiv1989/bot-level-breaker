# 16 — Enforce daily and global risk controls

**What to build:** A global risk-control slice that maintains the 03:00 UTC equity budget, flattens and locks on a 5% trading drawdown, preserves cross-boundary obligations, and applies the consecutive-loss entry cooldown.

**Blocked by:** 08 — Admit only risk-compliant attempts; 14 — Exit invalidated and expired positions safely; 15 — Survive data failures and recover from SAFE_MODE.

**Status:** completed

**Source:** `PRD.md` v1.0 §§18.1–18.7, 18.10, 20.4, 30.1–30.3, 31.5.

## Acceptance criteria

- [x] Daily drawdown uses total account equity and adjusts deposits and withdrawals out of trading performance.
- [x] Startup establishes and visibly labels a temporary anchor until the next 03:00 UTC boundary; restart intentionally resets the intraday budget as documented.
- [x] At a drawdown of at least 5%, the global queue atomically blocks new risk, cancels pending entries and necessary TPs, market-closes every account position reduce-only, retains hard stops until flat, and enters `DAILY_LOCKED`.
- [x] The dedicated-account “close all” behavior includes every reconciled account position, not only positions associated with a surviving memory level.
- [x] At the next 03:00 UTC boundary, a new anchor is established, the daily counter resets, open-position worst-case reservations remain, original holding deadlines remain, and a valid account automatically leaves `DAILY_LOCKED`.
- [x] Three consecutive attempts with negative net PnL after fees and funding start a 15-minute global entry cooldown; a profitable attempt resets the streak.
- [x] The loss cooldown blocks new entries without closing existing positions and resumes automatically after its deadline.
- [x] Tests cover transfer adjustment, exact 5% boundary, daily rollover with exposure, restart anchor labeling, concurrent reservations, loss-streak reset, and emergency flattening.

## Verification

- `gradlew.bat test --tests 'com.scalpsecta.breakoutbot.risk.*' --tests 'com.scalpsecta.breakoutbot.failure.*' --no-daemon --offline`: focused global-risk, virtual-clock, and flattening tests passed.
- `gradlew.bat build --no-daemon --offline`: full regression suite and packaging checks passed.
