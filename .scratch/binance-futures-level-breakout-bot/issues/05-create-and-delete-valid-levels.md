# 05 — Create and delete exchange-valid levels

**What to build:** An end-to-end level-management slice that lets the operator create a normalized, executable pre-break level and delete it only while it owns no exposure or unresolved order.

**Blocked by:** 03 — Observe healthy shared public market data; 04 — Establish authenticated Binance readiness.

**Status:** completed

**Source:** `PRD.md` v1.0 §§5.2, 6, 25, 29, 31.1.

## Acceptance criteria

- [x] The create operation accepts symbol, `LONG`/`SHORT`, level price, position notional USDT, and maximum impulse percent, uppercases the symbol, and rejects non-positive or non-finite values.
- [x] Only tradable Binance USD-M perpetual symbols are accepted, and price/quantity planning obeys tick size, step size, minimum quantity, and minimum notional filters.
- [x] Current price must be strictly on the pre-break side; equal or crossed input is rejected with `LEVEL_ALREADY_CROSSED`.
- [x] Exact duplicates use normalized symbol, direction, and tick-rounded level price; duplicates, the 101st stored level, and invalid symbols return their stable reason codes.
- [x] Level creation selects leverage as the lesser of 20x and the applicable bracket maximum, sets and verifies isolated margin, disables and verifies Auto-Add Margin, verifies leverage, and rejects an unsafe liquidation relationship.
- [x] The stored level exposes normalized price, planned full quantity, executable 30/30/40 allocation, derived blockers, and initial `WARMING_UP` state in the consolidated snapshot.
- [x] The level service directly owns the in-memory level collection and exposes its current state for snapshot assembly.
- [x] Delete succeeds only with no exposure or unresolved order, there is no update operation, and restart leaves the level service empty.
- [x] Same-origin mutation tests prove that valid Basic Auth plus CSRF succeeds and a missing CSRF token is rejected.

## Verification

- `gradlew.bat test --no-daemon --offline`: 43 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
- `liner-starter` and `liner-dto`: no tracked source changes.
