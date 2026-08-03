# 19 — Replay recorded attempts deterministically

**What to build:** An offline test facility that feeds recorded market, private, timer, reconciliation, and command events through the production decision boundaries in their original order and timestamps.

**Blocked by:** 09 — Record audit and market evidence; 17 — Add manual close, kill, and unlock controls.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§24.4, 30.2–30.5, 32.1.

## Acceptance criteria

- [x] Replay accepts the compressed event artifacts produced by the recorder and preserves original event ordering, exchange timestamps, receive timestamps, and timer relationships.
- [x] A virtual clock drives all warmup, confirmation, invalidation, stop/TP setup, outage, cooldown, holding, daily-boundary, and SAFE_MODE deadlines.
- [x] Replaying the same input and initial configuration produces identical state transitions, decisions, order intents, reason codes, and audit results.
- [x] The fake exchange can script fills, partial fills, timeouts, rejection, unknown outcomes, stop/TP behavior, stream outages, and reconciliation results without network access.
- [x] Fixture coverage includes every deterministic state-machine and exchange-adapter scenario listed in PRD §30.
- [x] Parser fixtures use representative Binance messages for aggregate trades, book ticker, order updates, account updates, duplicate IDs, and ID gaps.
- [x] Replay is available only as a test/offline facility and cannot be selected as a production runtime trading mode.
- [x] Running the full replay suite cannot instantiate the live order transport or send any Binance trading request.
