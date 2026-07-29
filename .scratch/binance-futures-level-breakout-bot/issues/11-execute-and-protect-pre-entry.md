# 11 — Execute and protect the pre-entry tranche

**What to build:** The first live trading slice from a qualified approach through atomic risk reservation, a 30% marketable IOC fill, and confirmed exchange-side hard-stop protection before any later tranche is allowed.

**Blocked by:** 08 — Admit only risk-compliant attempts; 10 — Resolve orders without blind retries.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§6.7, 12.1–12.5, 15, 19.4, 31.2.

## Acceptance criteria

- [ ] Pre-entry dispatch requires price on the pre-break side within two frozen NPU, healthy mandatory gates, no lock, and an atomically granted risk reservation.
- [ ] The first order requests the executable 30% tranche as a marketable IOC limit capped one NPU through the current best price with exchange-valid rounding.
- [ ] The level's single attempt becomes consumed exactly when the first entry order is dispatched, not while gates are merely blocked.
- [ ] A tranche filling at least 80% is reconciled by actual quantity; below 80% is not retried, closes actual exposure, and terminates as `INSUFFICIENT_LIQUIDITY`.
- [ ] Immediately after the first actual fill, one close-all `STOP_MARKET` using `CONTRACT_PRICE` and disabled price protection is placed at the frozen structural stop and confirmed within two seconds.
- [ ] No second or final tranche can be emitted before the hard stop is confirmed with the expected trigger.
- [ ] Stop confirmation failure reconciles and closes exposure, enters `SAFE_MODE`, records `STOP_SETUP_FAILED`, and releases risk only after confirmed reduction.
- [ ] If crossing occurs before the first fill is resolved and protected, no late crossing tranche is sent; actual exposure is closed and the level terminates as `CROSS_BEFORE_PROTECTED`.
