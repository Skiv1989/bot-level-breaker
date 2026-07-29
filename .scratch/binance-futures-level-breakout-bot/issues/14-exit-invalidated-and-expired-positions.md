# 14 — Exit invalidated and expired positions safely

**What to build:** A position-management slice that closes confirmed exposure for current ExitScore, hard snapback, maximum holding time, take-profit completion, or hard-stop fill and then enforces the symbol cooldown.

**Blocked by:** 13 — Install and reconcile the complete take-profit set.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §14, §§18.5, 19.4, 30.2–30.3, 31.3.

## Acceptance criteria

- [ ] ExitScore is recomputed from current observations using the PRD point rules, mirrors LONG/SHORT correctly, and closes when the current score is at least three without cumulative point carryover.
- [ ] A move more than two NPU behind the level within 500 ms triggers an immediate `SNAPBACK` close regardless of ExitScore.
- [ ] A confirmed position has one ten-minute deadline starting at breakout confirmation; daily boundaries do not reset it.
- [ ] Normal strategy and timeout exits send one reduce-only marketable IOC close capped one NPU through best price, wait up to 500 ms, reconcile, then market-close one confirmed residual.
- [ ] Hard-stop fills, take-profit completion, strategy exits, and timeout exits produce the correct terminal reason and net result using fees, funding, and slippage where known.
- [ ] Reservations shrink only on confirmed reducing fills and release fully only when flat.
- [ ] Confirmed flat state starts a 30-second per-symbol cooldown during which no level on that symbol can begin an attempt.
- [ ] Virtual-clock and fake-exchange tests cover ExitScore threshold changes, snapback, residual close, hard-stop fill, TP completion, timeout, and cooldown.
