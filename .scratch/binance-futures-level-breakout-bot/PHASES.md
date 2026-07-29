# Binance Futures Level-Breakout Bot — Implementation Phases

This plan converts the 21 dependency-ordered tickets into small implementation iterations.

## Phasing decision

Each phase contains exactly one ticket.

The tickets were written as narrow, independently verifiable vertical slices. Combining several into a phase would create long iterations across safety-critical areas such as order reconciliation, hard-stop protection, global risk, and SAFE_MODE recovery. A phase is complete only when its ticket is fully implemented, tested, and demoable.

The critical path is:

```text
01 → 02 → {03, 04} → 05 → 06 → 07 → {08, 09} → 10 → 11 → 12
   → 13 → 14 → 15 → 16 → 17 → {18, 19} → 20 → 21
```

Optional parallel branches:

- Phases 03 and 04 can start independently after Phase 02.
- Phases 08 and 09 can start independently after Phase 07.
- Phases 18 and 19 can start independently after Phase 17.
- Sequential delivery remains the default when one implementation agent owns the work.

## Definition of done for every phase

- Every acceptance criterion in the linked ticket is satisfied.
- Focused tests and the broader affected Gradle checks pass.
- The phase has one observable demo using fixtures, a fake exchange, offline replay, or the secured application surface.
- Automated verification cannot send a live Binance trading order.
- No unfinished compatibility path or temporary bypass is carried into the next phase.
- `liner-starter` and `liner-dto` remain unchanged.
- New behavior is visible through the snapshot, audit output, or deterministic test evidence where applicable.

## Phase plan

| Phase | Ticket | Small iteration outcome | Phase exit demonstration |
|---:|---|---|---|
| 01 | [Bootstrap a safe live-only application](issues/01-bootstrap-safe-live-only-application.md) | A buildable Kotlin/JVM 17 WebFlux shell with fake exchange boundaries and safe startup/shutdown behavior. | Start and stop the application under test; show empty memory state, blocked trading readiness, zero trading calls, and unchanged dependency repositories. |
| 02 | [Serve a secured operator snapshot over HTTPS](issues/02-secure-operator-snapshot.md) | The first secured operator-facing vertical slice. | Read the snapshot with valid Basic credentials over HTTPS; prove invalid credentials fail, CORS is disabled, and secrets are absent. |
| 03 | [Observe healthy shared public market data](issues/03-observe-shared-public-market-data.md) | Shared aggregate-trade and book-ticker ingestion with observable health. | Feed recorded Binance fixtures and show aggressor mapping, deduplication, gap detection, bid/ask state, event age, and shared-symbol subscription behavior. |
| 04 | [Establish authenticated Binance readiness](issues/04-establish-authenticated-binance-readiness.md) | Signed REST and private-stream readiness without startup exposure adoption. | Use the fake adapter and recorded private fixtures to show clock, account mode, equity, commission, and private readiness while proving startup performs no position/open-order discovery. |
| 05 | [Create and delete exchange-valid levels](issues/05-create-and-delete-valid-levels.md) | A complete secured create/read/delete level workflow. | Create a valid pre-break level, observe normalized sizing and symbol configuration, reject crossed/duplicate/capacity cases, and delete only an exposure-free level. |
| 06 | [Calculate NPU, market metrics, and mandatory gates](issues/06-calculate-npu-metrics-and-gates.md) | Transparent strategy calculations without order placement. | Feed deterministic LONG and SHORT events and show NPU, FAST/MID/SLOW metrics, every mandatory gate, blocker reasons, and diagnostic PressureScore. |
| 07 | [Drive levels through warmup and approach deterministically](issues/07-drive-warmup-and-approach-states.md) | Serialized symbol state through `WARMING_UP`, `ARMED`, and `APPROACH`. | With a virtual clock, demonstrate healthy warmup, missed-during-warmup termination, eight-NPU activation, frozen NPU, and same-symbol ownership. |
| 08 | [Admit only risk-compliant attempts](issues/08-admit-only-risk-compliant-attempts.md) | Atomic global risk admission before the first order intent. | Submit competing eligible levels and show deterministic reservation, structural stop, PlannedNetR, margin/liquidation checks, and stable rejection codes without placing an order. |
| 09 | [Record audit and market evidence](issues/09-record-audit-and-market-evidence.md) | Persistent decision evidence and bounded recent history. | Drive a level into `APPROACH`, then inspect ordered JSONL records, the compressed pre-attempt buffer, continued attempt recording, and recent snapshot entries. |
| 10 | [Resolve orders without blind retries](issues/10-resolve-orders-without-blind-retries.md) | Deterministic order identity and bounded uncertainty resolution. | Script timeout-then-fill, timeout-then-rejection, partial-fill, and unknown outcomes; prove no duplicate order is sent and confirmed quantities control downstream state. |
| 11 | [Execute and protect the pre-entry tranche](issues/11-execute-and-protect-pre-entry.md) | The first protected exposure: 30% IOC followed by a confirmed exchange-side hard stop. | Run a qualified fake attempt through fill and stop confirmation; prove the attempt is consumed at dispatch and no later tranche can occur before protection. |
| 12 | [Cross and confirm the breakout](issues/12-cross-and-confirm-the-breakout.md) | The protected 30/30/40 entry sequence through breakout confirmation. | Trigger the crossing with an aggregate trade, demonstrate the one-second confirmation and final tranche, then replay each pre-break invalidation and confirmation failure. |
| 13 | [Install and reconcile the complete take-profit set](issues/13-install-and-reconcile-take-profits.md) | Confirmed positions receive a verified 33/33/34 reduce-only TP set. | Show level-derived targets, actual-quantity allocation, unchanged hard stop after fills, and safe close plus SAFE_MODE when the full set cannot be verified. |
| 14 | [Exit invalidated and expired positions safely](issues/14-exit-invalidated-and-expired-positions.md) | Complete normal position management through confirmed flat state and cooldown. | Demonstrate ExitScore, snapback, ten-minute timeout, IOC-to-market residual close, hard-stop/TP completion, reservation release, and symbol cooldown. |
| 15 | [Survive data failures and recover from SAFE_MODE](issues/15-survive-data-failures-and-safe-mode.md) | Exposure is protected during public/private outages and uncertain order state. | Cross each outage threshold with a virtual clock, show immediate entry blocking and bounded emergency close, then prove valid auto-recovery and third-event manual escalation. |
| 16 | [Enforce daily and global risk controls](issues/16-enforce-daily-and-global-risk-controls.md) | Daily equity limits, emergency flattening, boundary reset, and loss-streak cooldown. | Reach the exact 5% drawdown, show atomic account flatten and `DAILY_LOCKED`, cross 03:00 UTC with preserved obligations, and exercise the three-loss cooldown. |
| 17 | [Add manual close, kill, and unlock controls](issues/17-add-manual-close-kill-and-unlock-controls.md) | Authenticated, idempotent operator control over one position or the whole account. | Use CSRF-protected commands to close one symbol, activate kill twice safely, reject an unsafe unlock, and unlock a reconciled flat healthy account. |
| 18 | [Operate the bot from one web page](issues/18-operate-the-bot-from-one-web-page.md) | The full single-page operator workflow over the consolidated REST snapshot. | In a browser test, create/delete a level, inspect health/risk/signals/orders/audit, close a position, kill trading, and unlock without WebSocket, SSE, or SPA code. |
| 19 | [Replay recorded attempts deterministically](issues/19-replay-recorded-attempts-deterministically.md) | Offline deterministic reproduction of production decision paths. | Replay the same event artifact twice and compare identical transitions, intents, reasons, and audit output while proving the live order transport is unavailable. |
| 20 | [Package and document single-image operation](issues/20-package-and-document-single-image-operation.md) | One reproducible live-only Docker image and complete operator runbook. | Stage clean read-only dependency snapshots, build and start the image with test mounts, reach HTTPS liveness, and show readiness remains distinct from liveness. |
| 21 | [Prove release acceptance without live orders](issues/21-prove-release-acceptance-without-live-orders.md) | A complete evidence-backed release candidate. | Run one documented verification command covering PRD acceptance, security, replay, packaging, repository cleanliness, and the transport-level live-order guard. |

## Delivery checkpoints

- **Checkpoint A — Safe observable service:** Phases 01–04 complete.
- **Checkpoint B — Operator-defined strategy readiness:** Phases 05–09 complete.
- **Checkpoint C — Fully protected trade lifecycle:** Phases 10–14 complete.
- **Checkpoint D — Failure and operator control:** Phases 15–17 complete.
- **Checkpoint E — Operable release candidate:** Phases 18–21 complete.

Checkpoints are reporting milestones only. They do not combine phases or weaken the one-ticket-per-iteration rule.
