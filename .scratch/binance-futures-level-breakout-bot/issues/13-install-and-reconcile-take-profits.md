# 13 — Install and reconcile the complete take-profit set

**What to build:** A post-confirmation slice that calculates all targets from the configured level, places three reduce-only take profits for actual exposure, and either verifies the full set or closes safely.

**Blocked by:** 12 — Cross and confirm the breakout.

**Status:** completed

**Source:** `PRD.md` v1.0 §16, §§17.3, 30.1, 31.4.

## Acceptance criteria

- [x] TP prices derive from level price at 35%, 70%, and 100% of configured maximum impulse for both LONG and SHORT, never from weighted entry.
- [x] Prices use side-safe tick rounding, and actual reconciled position quantity is split into executable 33%, 33%, and 34% allocations without exceeding exposure.
- [x] All three targets are reduce-only `LIMIT GTC` orders with deterministic identities.
- [x] The complete target set is verified within three seconds before the level enters `POSITION_MANAGEMENT`.
- [x] If verification is incomplete, TP fragments are canceled, actual exposure is closed, the attempt records `TP_SETUP_FAILED`, and the runtime enters `SAFE_MODE`.
- [x] TP fills shrink exposure and reserved risk only after confirmed fills.
- [x] The close-all hard stop remains at its original trigger after every TP fill and is never moved, tightened, widened, or trailed.
- [x] Fake-exchange tests cover complete setup, rounding residue, partial setup, partial TP fills, and all-TP completion.

## Verification

- `gradlew.bat test --tests com.scalpsecta.breakoutbot.execution.BreakoutExecutionServiceTest --tests com.scalpsecta.breakoutbot.execution.ExecutionServiceTest --tests com.scalpsecta.breakoutbot.binance.LiveAuthenticatedBinanceClientTest --no-daemon --offline`: 31 focused tests passed.
- `gradlew.bat cleanTest test --no-daemon --offline`: 111 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
