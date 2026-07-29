# 07 — Drive levels through warmup and approach deterministically

**What to build:** A serialized per-symbol event flow that advances operator levels from `WARMING_UP` to `ARMED` and `APPROACH`, freezes NPU at the right moment, and exposes deterministic states and blockers without yet placing orders.

**Blocked by:** 05 — Create and delete exchange-valid levels; 06 — Calculate NPU, market metrics, and mandatory gates.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§11, 23.6, 30.2.

## Acceptance criteria

- [ ] Market events, timers, order-event placeholders, and UI commands for a symbol are processed by one ordered queue; callbacks and controllers cannot mutate trading state directly.
- [ ] Ten continuous seconds of healthy history are required before a level becomes `ARMED`.
- [ ] A qualifying aggregate trade that reaches or crosses the level during warmup makes it terminal with `MISSED_DURING_WARMUP`; bid/ask alone never defines crossing.
- [ ] Entering the eight-NPU activation band moves the level to `APPROACH` and freezes its NPU for the remainder of the attempt.
- [ ] Multiple stored levels may share a symbol, while the model can designate at most one active attempt or position owner for that symbol.
- [ ] Global `RUNNING`, `ENTRY_COOLDOWN`, `SAFE_MODE`, `DAILY_LOCKED`, and `MANUAL_LOCK` blockers are represented in every affected level snapshot.
- [ ] Virtual-clock tests reproduce warmup success, missed warmup, activation-band entry, mirrored crossing rules, event ordering, and simultaneous same-symbol levels.
