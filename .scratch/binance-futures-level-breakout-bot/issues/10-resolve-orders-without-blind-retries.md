# 10 — Resolve orders without blind retries

**What to build:** An execution slice that gives every order intent deterministic identity, resolves normal outcomes from the private stream, and uses bounded REST reconciliation for uncertain outcomes without ever duplicating an order blindly.

**Blocked by:** 04 — Establish authenticated Binance readiness; 07 — Drive levels through warmup and approach deterministically; 09 — Record audit and market evidence.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §19, §§20.2, 23.6, 30.3.

## Acceptance criteria

- [ ] Each intent receives a Binance-safe deterministic `clientOrderId` containing restart-safe identity derived from the application start time, level, attempt, order role, tranche/target, and monotonic sequence identity.
- [ ] Normal order lifecycle, fills, account changes, and position changes are driven by authenticated user-data events on the owning symbol queue.
- [ ] A timeout or lost HTTP response never triggers a resend; resolution waits for the private event and queries by `clientOrderId`.
- [ ] Reconciliation performs no more than three bounded status checks across three seconds and classifies filled, partially filled, rejected, canceled, or still unknown.
- [ ] An unresolved private outcome blocks entries/additions, records `ORDER_OUTCOME_UNKNOWN`, enters `SAFE_MODE`, and closes only exposure confirmed by reconciliation.
- [ ] Fill, stop, TP, risk, and close calculations consume confirmed actual quantities rather than requested quantities.
- [ ] Closing intents are reduce-only or close-position operations and cannot increase or reverse exposure.
- [ ] Fake-exchange tests cover immediate success, timeout-then-fill, timeout-then-rejection, partial fill, cancellation, and permanently unknown outcomes.
