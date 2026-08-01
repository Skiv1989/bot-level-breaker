# 12 — Cross and confirm the breakout

**What to build:** A protected continuation slice that adds 30% on the first qualifying crossing trade, applies pre-break invalidation, runs the one-second confirmation, and adds the final 40% only when the breakout remains valid.

**Blocked by:** 11 — Execute and protect the pre-entry tranche.

**Status:** completed

**Source:** `PRD.md` v1.0 §§11.4, 12.6–12.8, 13, 30.2, 31.2–31.3.

## Acceptance criteria

- [x] The second 30% IOC is triggered only by the first qualifying aggregate trade at/through the level; no resting level order exists.
- [x] The crossing tranche requires resolved first fill, confirmed hard stop, healthy public/private data, valid gates, and a still-active attempt.
- [x] Each added tranche uses the same capped IOC, 80% minimum-fill, reconciliation, no-blind-retry, and exchange-protection rules as pre-entry.
- [x] Before confirmed breakout, the bot exits for the PRD adverse-retreat, sustained opposite flow, collapsed acceleration, active burst, three-second data failure, or five-second no-cross timeout conditions with stable terminal reasons.
- [x] The confirmation window lasts one second, tolerates price up to one NPU behind the level, and requires directional pressure, burst, absorption/ExitScore, and data-health checks to remain valid.
- [x] Price moving more than one NPU behind during confirmation closes existing exposure and terminates as `BREAK_CONFIRM_FAILED`.
- [x] Successful confirmation dispatches the final executable 40% IOC; a failed final tranche closes actual exposure rather than continuing with an undersized unmanaged position.
- [x] Virtual-clock tests cover both directions, confirmation boundary noise, every pre-break invalidation, and prove that no addition precedes hard-stop confirmation.

## Verification

- `gradlew.bat test --tests com.scalpsecta.breakoutbot.level.BreakoutStateMachineTest --tests com.scalpsecta.breakoutbot.execution.BreakoutExecutionServiceTest --tests com.scalpsecta.breakoutbot.level.LevelServiceTest --tests com.scalpsecta.breakoutbot.execution.PreEntryExecutionServiceTest --tests com.scalpsecta.breakoutbot.execution.ExecutionServiceTest --no-daemon --offline`: 45 focused tests passed.
- `gradlew.bat cleanTest test --no-daemon --offline`: 103 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
