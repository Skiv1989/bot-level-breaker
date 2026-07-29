# 08 — Admit only risk-compliant attempts

**What to build:** A serialized global risk slice that evaluates a qualified approach, reserves its level risk atomically, and reports every economic, margin, liquidation, position-cap, and daily-cap reason that can block the first entry.

**Blocked by:** 04 — Establish authenticated Binance readiness; 05 — Create and delete exchange-valid levels; 06 — Calculate NPU, market metrics, and mandatory gates; 07 — Drive levels through warmup and approach deterministically.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§15.1–15.2, 17, 18.1–18.5, 18.8–18.9, 23.6.

## Acceptance criteria

- [ ] Level risk is exactly 1% of configured position notional and is not reserved merely by creating a level.
- [ ] Immediately before a first-entry intent, one global ordered queue atomically accounts for trading drawdown, open-position reservations, pending-attempt reservations, and the new budget.
- [ ] The global attempt/risk service directly owns in-memory attempt and reservation state and exposes its current state for snapshot assembly.
- [ ] Simultaneous eligible levels cannot overbook daily capacity, exceed five exposed symbols, or create two active attempts for one symbol.
- [ ] Structural stop uses the frozen NPU and preceding one-second micro-swing formula, is exchange-valid, and is never tightened merely to pass risk validation.
- [ ] Planned worst loss includes worst capped entry, taker fees, and exit-slippage reserve; weighted TP reward and conservative fees produce `PlannedNetR`, which must be at least 1.5.
- [ ] Projected isolated margin leaves at least 20% free margin, selected leverage respects the 20x/bracket ceiling, and projected liquidation remains beyond the stop on the loss side.
- [ ] Failures surface stable codes including `BLOCKED_DAILY_RISK`, `BLOCKED_MARGIN_BUFFER`, `BLOCKED_POSITION_CAP`, `STOP_RISK_TOO_HIGH`, `PLANNED_NET_R_TOO_LOW`, and `LIQUIDATION_TOO_CLOSE`.
- [ ] Reservation shrink/release APIs require confirmed reducing fills or confirmed flat state; request intent alone never releases risk.
