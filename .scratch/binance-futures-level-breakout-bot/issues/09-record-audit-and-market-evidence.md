# 09 — Record audit and market evidence

**What to build:** An append-only evidence pipeline that lets the operator reconstruct state transitions and decisions from JSONL audit records, compressed attempt event files, and a bounded recent-event snapshot.

**Blocked by:** 03 — Observe healthy shared public market data; 07 — Drive levels through warmup and approach deterministically.

**Status:** completed

**Source:** `PRD.md` v1.0 §24.

## Acceptance criteria

- [x] Every material audit record has a unique event ID, timestamp, application start time, level/symbol identity, before/after state, event type, decision, and applicable blocker or recovery details.
- [x] Decision records can carry the complete market metric, gate, NPU, age, spread, price, quantity, order, risk, and PnL fields required by the PRD without exposing secrets.
- [x] JSON Lines audit data appends to the configured mounted directory and survives application restart.
- [x] Every armed symbol retains a rolling ten-second raw-event buffer; entering `APPROACH` flushes it into a compressed event file.
- [x] Attempt recording continues through the complete attempt and for ten seconds afterward, including public events, private events, state changes, order intents, and reconciliations.
- [x] The consolidated snapshot exposes a bounded recent audit/trade summary while the persistent files remain authoritative.
- [x] The audit and compressed-event writers perform a best-effort flush during graceful shutdown without blocking shutdown indefinitely.
- [x] Deterministic tests verify record ordering, application-run separation, retention boundaries, compressed-event readability, and graceful-shutdown flushing.

## Verification

- `gradlew.bat test --tests com.scalpsecta.breakoutbot.evidence.EvidenceServiceTest --no-daemon --offline`: 4 focused evidence tests passed.
- `gradlew.bat test --tests com.scalpsecta.breakoutbot.level.LevelServiceTest --tests com.scalpsecta.breakoutbot.risk.AttemptRiskServiceTest --no-daemon --offline`: 26 affected-owner tests passed.
- `gradlew.bat cleanTest test --no-daemon --offline`: 73 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
