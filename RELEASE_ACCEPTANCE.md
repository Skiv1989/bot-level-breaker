# Release acceptance evidence

The release is accepted only when the documented command in `README.md`
finishes with exit code zero and its `release-evidence.txt` report marks every
PRD §31 area `PASS`. A failed command is evidence of non-acceptance, not a
partial release approval.

## Safety boundary

Automated verification never receives production credentials. Unit,
state-machine, adapter, and replay tests use injected fakes. Every Gradle test
worker installs a filter on the production Binance REST transport. If a test
reaches a non-read Binance production request other than listen-key
maintenance, the filter stops it before exchange and writes a persistent
marker; the Gradle task fails even if reactive application code were to absorb
the request error. Adapter tests that intentionally validate request encoding
inject an in-memory `ExchangeFunction` instead of constructing the production
transport.

The container proof uses fixed dummy values and Docker `--network none`, so the
live-only runtime endpoints have no route to Binance. It verifies authenticated
HTTPS liveness from inside the container and separately requires readiness to
report `tradingReadiness=BLOCKED`. The certificate is generated for that run,
mounted read-only, and removed with the temporary context; the audit directory
is mounted read-write.

The command fingerprints each reference repository before staging and after
all checks. A fingerprint covers HEAD, staged and unstaged diffs, status, and
the content of non-ignored untracked files. Any change fails acceptance. The
staged context is rejected if it contains VCS/IDE/build/private-data
directories, environment files, credential/certificate extensions, a populated
secret in `runtime.env.example`, or PEM private-key material.

## Automated evidence layers

The command runs `cleanTest test` once and parses the resulting JUnit XML. It
rejects failures, errors, skipped tests, an empty suite, or a missing required
suite. The complete suite contains:

| Layer | Primary evidence |
|---|---|
| Decimal, signal, risk, cooldown, and stable-code units | `SignalEngineTest`, `AttemptRiskServiceTest`, `DailyRiskControlServiceTest`, `SymbolCooldownsTest`, `ReleaseAcceptanceContractTest` |
| Deterministic state machines and virtual time | `BreakoutStateMachineTest`, `LevelServiceTest`, `ReplayVirtualClockTest` |
| Order and Binance adapters | `PreEntryExecutionServiceTest`, `BreakoutExecutionServiceTest`, `ExecutionServiceTest`, `LiveAuthenticatedBinanceClientTest` |
| Offline replay and scripted exchange | `RecordedAttemptReplayTest`, `ScriptedFakeExchangeTest` |
| Recorded public/private parser fixtures | `BinancePublicMarketDataParserTest`, `BinanceUserDataEventParserTest`, `AggregateTradeContinuityTest` |
| SAFE_MODE, data failure, and controls | `SafeModeServiceTest`, `DailyRiskControlServiceTest`, `OperatorControlServiceTest` |
| HTTPS security and secret redaction | `OperatorSecurityIntegrationTest`, `LiveAuthenticatedBinanceClientTest` |
| Local dependencies, staging, restart, and shutdown | `LocalDependencyResolutionTest`, `PackagingStagingTest`, `RuntimeLifecycleIntegrationTest`, `ProductionBootstrapIntegrationTest` |
| Live-transport prevention | `AutomatedVerificationBinanceTransportGuardTest` plus the Gradle task marker |

These tests cover exact `BigDecimal` price/quantity/tick rounding, FAST/MID/SLOW
rates, NPU sampling/freezing, mandatory gates and PressureScore diagnostics,
LONG/SHORT mirroring, structural stops, take-profit calculations, fees,
slippage, PlannedNetR, daily transfer adjustments, 03:00 and temporary anchors,
atomic reservations, loss streaks, and symbol cooldowns. Deterministic scenarios
cover the 30/30/40 flow, protection before later additions, every pre-break and
position invalidation, take-profit setup, hard-stop fills, uncertain outcomes,
public/private failures, SAFE_MODE recovery/escalation, daily/manual flattening,
and restart/shutdown boundaries.

## PRD §31 result map

| Area | Required passing evidence |
|---|---|
| 31.1 Level management | `LevelServiceTest` proves creation, crossed/equal rejection, duplicates, capacity, create/delete semantics, and exposure deletion rules; `RuntimeLifecycleIntegrationTest` proves restart clears memory-only levels. |
| 31.2 Entry and protection | `PreEntryExecutionServiceTest`, `BreakoutExecutionServiceTest`, and `ExecutionServiceTest` prove 30/30/40 allocation, trade-triggered additions, one-second confirmation, stop-before-addition, two-second stop confirmation, 80% fills, and no blind retry. |
| 31.3 Strategy | `SignalEngineTest` and `BreakoutStateMachineTest` prove observable gates, diagnostic-only PressureScore, one-NPU confirmation tolerance, five-second timeout, current-state ExitScore, 500 ms snapback, and ten-minute maximum hold. |
| 31.4 Stops and take profits | `AttemptRiskServiceTest`, `ExecutionServiceTest`, and `BreakoutExecutionServiceTest` prove mirrored structural stops, wide-stop rejection, immutable CONTRACT_PRICE protection without price protection, 35/70/100 prices, 33/33/34 quantities, and the three-second TP failure close plus SAFE_MODE. |
| 31.5 Risk | `AttemptRiskServiceTest`, `DailyRiskControlServiceTest`, and `SymbolCooldownsTest` prove the 1% budget, leverage cap, margin buffer, five-symbol cap, transfer-adjusted equity drawdown, daily flatten/reset, and loss cooldown. |
| 31.6 Failure handling | `ExecutionServiceTest`, `SafeModeServiceTest`, and `PublicMarketDataServiceTest` prove 250 ms freshness, bounded public/private outages, no duplicate after uncertainty, 30-second/three-match recovery, and third-event manual locking. |
| 31.7 Operations | Lifecycle and packaging tests prove no startup/shutdown Binance command, no runtime persistence, and local artifact wiring. The release script additionally proves sanitized staging, an actual single-image build, mounted TLS/audit storage, HTTPS liveness, separate blocked readiness, no network egress, and unchanged reference repositories. |

## Stable reason catalog

`ReleaseAcceptanceContractTest` prevents removal of any PRD §29 stable code,
while the behavior suites above exercise their owning paths. The accepted
catalog is:

`INVALID_SYMBOL`, `INVALID_LEVEL`, `DUPLICATE_LEVEL`,
`LEVEL_CAPACITY_REACHED`, `LEVEL_ALREADY_CROSSED`,
`SYMBOL_CONFIGURATION_FAILED`, `LIQUIDATION_TOO_CLOSE`,
`MISSED_DURING_WARMUP`, `BLOCKED_DAILY_RISK`, `BLOCKED_MARGIN_BUFFER`,
`BLOCKED_POSITION_CAP`, `STOP_RISK_TOO_HIGH`, `PLANNED_NET_R_TOO_LOW`,
`PRE_ENTRY_INVALIDATED`, `PRE_ENTRY_TIMEOUT`, `CROSS_BEFORE_PROTECTED`,
`BREAK_CONFIRM_FAILED`, `INSUFFICIENT_LIQUIDITY`, `STOP_SETUP_FAILED`,
`TP_SETUP_FAILED`, `EXIT_SCORE`, `SNAPBACK`, `MAX_HOLD_TIME`,
`MARKET_DATA_FAILURE`, `PRIVATE_STREAM_FAILURE`, `ORDER_OUTCOME_UNKNOWN`,
`DAILY_LOSS_LIMIT`, `MANUAL_CLOSE`, `KILL_SWITCH`, `HARD_STOP_FILLED`, and
`TAKE_PROFITS_COMPLETE`.

## Accepted operational risks

Passing acceptance does not remove or weaken any PRD §32 risk:

1. The rollout is live-only; fake exchange and replay evidence substitute for a Binance testnet.
2. Restart establishes a new temporary daily anchor before the next 03:00 UTC boundary.
3. Startup does not discover, reconcile, or adopt old positions and orders.
4. A general pre-entry account-exposure check is absent, so unmanaged One-way Mode exposure can merge.
5. Levels exist only in memory and disappear after restart or crash.
6. Shutdown sends no Binance action; stops, take profits, orders, and positions can outlive the process.
7. Gaps and slippage can exceed planned risk; STOP_MARKET cannot guarantee a 1% realized-loss ceiling.
8. Self-signed TLS depends on operators trusting and protecting the private CA.
9. Single-IP access requires firewall reconfiguration after an operator IP change.
10. Engineering conformance does not establish positive expected value or guarantee profitability.

Acceptance also does not claim automatic recovery or adoption beyond the exact
SAFE_MODE, startup, restart, and shutdown behavior approved by the PRD.
