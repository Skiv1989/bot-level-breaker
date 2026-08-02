# 17 — Add manual close, kill, and unlock controls

**What to build:** Secured operator commands to close one active symbol, kill all trading and exposure, and unlock a healthy flat account after manual intervention.

**Blocked by:** 14 — Exit invalidated and expired positions safely; 15 — Survive data failures and recover from SAFE_MODE; 16 — Enforce daily and global risk controls.

**Status:** ready-for-agent

**Source:** `PRD.md` v1.0 §§21, 25, 26.2, 29.

## Acceptance criteria

- [x] Per-position close blocks additions for the symbol, uses the normal reduce-only close flow, confirms flat state, records `MANUAL_CLOSE`, and leaves the owning level terminal.
- [x] The global kill switch atomically blocks entries, cancels pending entries and required TPs, market-closes every account position reduce-only, retains each hard stop until flat, records `KILL_SWITCH`, and enters `MANUAL_LOCK`.
- [x] Unlock succeeds only when the account is flat and runtime health/reconciliation requirements pass; it does not recreate levels lost on restart.
- [x] Every command is serialized through its owning symbol or global queue and is idempotent under duplicate browser submission.
- [x] Mutations require Basic Auth, same-origin access, and a valid CSRF token and return stable machine codes plus human-readable explanations.
- [x] Level deletion becomes available only after the close command has confirmed there is no exposure or unresolved order.
- [x] Audit and snapshot output make command progress, blockers, residual exposure, global state, and final outcome observable.
- [x] Fake-exchange tests cover partial close, residual reconciliation, duplicate kill, unlock rejection, and successful unlock.
