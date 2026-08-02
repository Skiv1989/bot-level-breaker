# 14 — Exit invalidated and expired positions safely

**What to build:** A position-management slice that closes confirmed exposure for current ExitScore, hard snapback, maximum holding time, take-profit completion, or hard-stop fill and then enforces the symbol cooldown.

**Blocked by:** 13 — Install and reconcile the complete take-profit set.

**Status:** completed

**Source:** `PRD.md` v1.0 §14, §§18.5, 19.4, 30.2–30.3, 31.3.

## Acceptance criteria

- [x] ExitScore is recomputed from current observations using the PRD point rules, mirrors LONG/SHORT correctly, and closes when the current score is at least three without cumulative point carryover.
- [x] A move more than two NPU behind the level within 500 ms triggers an immediate `SNAPBACK` close regardless of ExitScore.
- [x] A confirmed position has one ten-minute deadline starting at breakout confirmation; daily boundaries do not reset it.
- [x] Normal strategy and timeout exits send one reduce-only marketable IOC close capped one NPU through best price, wait up to 500 ms, reconcile, then market-close one confirmed residual.
- [x] Hard-stop fills, take-profit completion, strategy exits, and timeout exits produce the correct terminal reason and net result using fees, funding, and slippage where known.
- [x] Reservations shrink only on confirmed reducing fills and release fully only when flat.
- [x] Confirmed flat state starts a 30-second per-symbol cooldown during which no level on that symbol can begin an attempt.
- [x] Virtual-clock and fake-exchange tests cover ExitScore threshold changes, snapback, residual close, hard-stop fill, TP completion, timeout, and cooldown.

## Verification

- `gradlew.bat test --tests com.scalpsecta.breakoutbot.execution.ExecutionServiceTest --tests com.scalpsecta.breakoutbot.execution.BreakoutExecutionServiceTest --tests com.scalpsecta.breakoutbot.level.BreakoutStateMachineTest --tests com.scalpsecta.breakoutbot.level.LevelServiceTest --tests com.scalpsecta.breakoutbot.level.SymbolCooldownsTest --no-daemon --offline`: focused tests passed.
- `gradlew.bat cleanTest test --no-daemon --offline`: 119 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
