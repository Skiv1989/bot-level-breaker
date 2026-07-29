# 09 — Record audit and market evidence

**What to build:** An append-only evidence pipeline that lets the operator reconstruct state transitions and decisions from JSONL audit records, compressed attempt event files, and a bounded recent-event snapshot.

**Blocked by:** 03 — Observe healthy shared public market data; 07 — Drive levels through warmup and approach deterministically.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §24.

## Acceptance criteria

- [ ] Every material audit record has a unique event ID, timestamp, application start time, level/symbol identity, before/after state, event type, decision, and applicable blocker or recovery details.
- [ ] Decision records can carry the complete market metric, gate, NPU, age, spread, price, quantity, order, risk, and PnL fields required by the PRD without exposing secrets.
- [ ] JSON Lines audit data appends to the configured mounted directory and survives application restart.
- [ ] Every armed symbol retains a rolling ten-second raw-event buffer; entering `APPROACH` flushes it into a compressed event file.
- [ ] Attempt recording continues through the complete attempt and for ten seconds afterward, including public events, private events, state changes, order intents, and reconciliations.
- [ ] The consolidated snapshot exposes a bounded recent audit/trade summary while the persistent files remain authoritative.
- [ ] The audit and compressed-event writers perform a best-effort flush during graceful shutdown without blocking shutdown indefinitely.
- [ ] Deterministic tests verify record ordering, application-run separation, retention boundaries, compressed-event readability, and graceful-shutdown flushing.
