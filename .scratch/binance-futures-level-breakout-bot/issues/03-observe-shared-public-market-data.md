# 03 — Observe healthy shared public market data

**What to build:** Shared per-symbol Binance USD-M Futures aggregate-trade and best-bid/ask streams that expose parsed events and an observable public-data health assessment to the operator snapshot.

**Blocked by:** 02 — Serve a secured operator snapshot over HTTPS.

**Status:** completed

**Source:** `PRD.md` v1.0 §§7, 23.5, 23.6, 30.4.

## Acceptance criteria

- [x] Bot-local WebSocket adapters reuse the starter's abstract reactive infrastructure without modifying `liner-starter`.
- [x] Aggregate-trade events preserve `a`, `E`, `T`, `p`, `q`, `m`, derived aggressor side, and local receive time; book-ticker events preserve bid/ask price and quantity plus exchange and receive times.
- [x] Recorded Binance fixture tests prove that `buyerIsMaker=false` maps to aggressive buy and `buyerIsMaker=true` maps to aggressive sell.
- [x] Duplicate aggregate-trade IDs are ignored, and unresolved ID gaps are detected and surfaced as unhealthy.
- [x] Health reflects connection state, bid/ask heartbeat, event age, and ID continuity; a quiet market without trades is not by itself classified as an outage.
- [x] Multiple levels for one symbol share the same public streams, and subscriptions are released only when the symbol no longer needs them.
- [x] The snapshot exposes latest bid/ask, spread, event ages, connection state, and gap status without quantizing analytical values prematurely.

## Verification

- `gradlew.bat cleanTest test --no-daemon --offline`: 22 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
- `liner-starter`: clean worktree.
