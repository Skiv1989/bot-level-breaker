# 04 — Establish authenticated Binance readiness

**What to build:** A bot-local signed Binance client and user-data stream that establish clock, account, commission, equity, and private-stream readiness without adopting or cleaning up pre-existing exchange exposure.

**Blocked by:** 02 — Serve a secured operator snapshot over HTTPS.

**Status:** completed

**Source:** `PRD.md` v1.0 §§5, 17.1, 18.1, 19.3, 20.2, 22.1.

## Acceptance criteria

- [x] API key and HMAC secret are read only from environment configuration, used for signed live USD-M Futures requests, and never logged or returned.
- [x] Exchange clock synchronization is measured and exposed with an explicit healthy/unhealthy status.
- [x] Startup obtains the equity information needed for the temporary daily anchor and starts the authenticated user-data stream without querying positions or open orders for adoption or cleanup.
- [x] The runtime verifies One-way Mode and Single-Asset Mode but never changes either account-wide setting automatically.
- [x] The client can obtain symbol metadata, leverage brackets, actual commission rates, account updates, order updates, and position updates through typed bot-local contracts.
- [x] Recorded private-stream fixtures validate order/account event parsing, timestamps, quantities, prices, and identifiers.
- [x] Public readiness, private readiness, clock readiness, and overall trading readiness remain distinguishable in the snapshot.

## Verification

- `gradlew.bat cleanTest test --no-daemon --offline`: 31 tests passed.
- `gradlew.bat build --no-daemon --offline`: successful.
- `liner-starter` and `liner-dto`: no tracked source changes.
