# 06 — Calculate NPU, market metrics, and mandatory gates

**What to build:** A direction-aware signal slice that converts live symbol events into NPU, FAST/MID/SLOW metrics, mandatory v1 gate results, and diagnostic scores visible for each level.

**Blocked by:** 03 — Observe healthy shared public market data; 05 — Create and delete exchange-valid levels.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§8–10, 30.1, 31.3.

## Acceptance criteria

- [ ] Mid-price is sampled every 100 ms, ten seconds of samples are retained, and NPU is the tick-ceiled maximum of tick size and the 75th percentile of absolute consecutive moves.
- [ ] NPU can be recomputed once per second while armed and frozen on approach; both absolute and percentage values are exposed.
- [ ] FAST, MID, and SLOW rates, sizes, directional shares, delta, acceleration ratios, signed progress, adverse pullback, and FlowEfficiency match the PRD formulas.
- [ ] LONG and SHORT calculations are exact directional mirrors and preserve decimal precision until exchange/order boundaries.
- [ ] Acceleration, directional-flow, ramp, one-shot burst, price-response, latency, spread, and data-integrity gates report both pass/fail state and blocker reasons.
- [ ] Both spread limits are applied, with the stricter of one NPU and 0.10% controlling entry eligibility.
- [ ] PressureScore and every component are observable diagnostics but cannot authorize trading when any mandatory gate fails.
- [ ] Unit tests cover empty/sparse windows, bin boundaries, epsilon behavior, NPU freezing, burst persistence, and the PRD threshold edges.
