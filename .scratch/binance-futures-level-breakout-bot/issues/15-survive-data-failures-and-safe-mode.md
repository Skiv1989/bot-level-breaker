# 15 — Survive data failures and recover from SAFE_MODE

**What to build:** A failure-handling slice that blocks new risk immediately when required data is unhealthy, closes exposed positions after bounded outages, and recovers or escalates SAFE_MODE deterministically.

**Blocked by:** 10 — Resolve orders without blind retries; 14 — Exit invalidated and expired positions safely.

**Status:** completed

**Source:** `PRD.md` v1.0 §§7.4, 20, 29, 30.3, 31.6.

## Acceptance criteria

- [x] Public data older than 250 ms, disconnected, missing/stale bid/ask, unresolved trade-ID gap, excessive spread, unhealthy private stream, or unknown order outcome immediately blocks entries and additions.
- [x] A continuously unhealthy public stream for more than three seconds with exposure triggers reconciliation and one idempotent reduce-only market close while retaining the hard stop until flat.
- [x] Private-stream loss immediately blocks additions and starts signed REST reconciliation; failure to restore it within five seconds with exposure closes reconciled exposure and retains protection until flat.
- [x] Public and private failure exits record `MARKET_DATA_FAILURE` or `PRIVATE_STREAM_FAILURE` and enter `SAFE_MODE`.
- [x] Automatic recovery requires 30 continuous healthy seconds, three matching signed reconciliations, no unexplained exposure or orphaned bot order, and passing account, symbol, clock, and stream checks.
- [x] Three SAFE_MODE events in a rolling 15-minute interval flatten all account exposure, cancel bot orders as required, and enter `MANUAL_LOCK`.
- [x] No-trade periods alone do not start a public-data outage timer.
- [x] Virtual-clock and fake-exchange tests cover threshold boundaries, restoration before timeout, auto-recovery mismatch, order uncertainty, and third-event escalation.

## Verification

- `gradlew.bat test --tests com.scalpsecta.breakoutbot.failure.SafeModeServiceTest --tests com.scalpsecta.breakoutbot.execution.ExecutionServiceTest --no-daemon --offline`: focused virtual-clock and fake-exchange tests passed.
- `gradlew.bat cleanTest test --no-daemon --offline`: full regression suite passed.
- `gradlew.bat build --no-daemon --offline`: successful.
