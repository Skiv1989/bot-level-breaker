package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.binance.BinanceBalanceUpdate
import com.scalpsecta.breakoutbot.binance.BinanceExecutionClient
import com.scalpsecta.breakoutbot.binance.BinanceOrderAcknowledgement
import com.scalpsecta.breakoutbot.binance.BinanceOrderReconciliation
import com.scalpsecta.breakoutbot.binance.BinanceOrderRequest
import com.scalpsecta.breakoutbot.binance.BinanceOrderStatus
import com.scalpsecta.breakoutbot.binance.BinancePositionRisk
import com.scalpsecta.breakoutbot.binance.BinancePositionUpdate
import com.scalpsecta.breakoutbot.binance.BinanceUserDataEvent
import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.level.PositionNetResult
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException

class ExecutionServiceTest {
    private val resources = mutableListOf<AutoCloseable>()

    @AfterEach
    fun closeResources() {
        resources.reversed().forEach(AutoCloseable::close)
    }

    @Test
    fun `deterministic client order IDs contain compact restart-safe identity`() {
        val firstFactory = ClientOrderIdFactory(STARTED_AT)
        val restartedFactory = ClientOrderIdFactory(STARTED_AT)
        val request = entryRequest()

        val first = firstFactory.create(request)
        val second = firstFactory.create(request)
        val reproducedFirst = restartedFactory.create(request)

        assertThat(first.clientOrderId).isEqualTo(reproducedFirst.clientOrderId)
        assertThat(second.clientOrderId).isNotEqualTo(first.clientOrderId)
        assertThat(first.clientOrderId)
            .matches("^[.A-Za-z0-9_:/-]{1,36}$")
            .startsWith("b${STARTED_AT.toEpochMilli().toString(36)}-")
            .contains("-1e1-")
        assertThat(first.clientOrderId.length).isLessThanOrEqualTo(36)
        assertThat(first.intentSequence).isEqualTo(1)
        assertThat(second.intentSequence).isEqualTo(2)
    }

    @Test
    fun `immediate fill and account changes resolve from private stream`() {
        val harness = harness()
        harness.client.onPlace = { request ->
            harness.events.tryEmitNext(accountUpdate(positionAmount = "0.30"))
            harness.events.tryEmitNext(
                orderUpdate(request, status = "FILLED", filledQuantity = "0.30"),
            )
            Mono.just(request.acknowledgement())
        }

        val resolution = harness.service.execute(entryRequest()).block(TIMEOUT)!!

        assertThat(resolution.outcome).isEqualTo(OrderOutcome.FILLED)
        assertThat(resolution.source)
            .isEqualTo(OrderResolutionSource.PRIVATE_STREAM)
        assertThat(resolution.actualFilledQuantity)
            .isEqualByComparingTo("0.30")
        assertThat(resolution.confirmedPositionAmount)
            .isEqualByComparingTo("0.30")
        assertThat(resolution.reconciliationChecks).isZero()
        assertThat(harness.client.placements).hasSize(1)
        assertThat(harness.client.reconciliationCounts).isEmpty()
        assertThat(harness.service.currentState().positions.single().positionAmount)
            .isEqualByComparingTo("0.30")
        assertThat(harness.service.currentState().balances.single().walletBalance)
            .isEqualByComparingTo("1000")
        assertThat(harness.coordinator.events)
            .anyMatch { event -> event.startsWith("private-account:") }
            .anyMatch { event -> event.startsWith("private-order:") }
    }

    @Test
    fun `timeout followed by fill never resends the order`() {
        val harness = harness()
        harness.client.onPlace = { request ->
            harness.events.tryEmitNext(
                orderUpdate(request, status = "FILLED", filledQuantity = "0.30"),
            )
            Mono.error(TimeoutException("lost response"))
        }

        val resolution = harness.service.execute(entryRequest()).block(TIMEOUT)!!

        assertThat(resolution.outcome).isEqualTo(OrderOutcome.FILLED)
        assertThat(resolution.source)
            .isEqualTo(OrderResolutionSource.PRIVATE_STREAM)
        assertThat(harness.client.placements).hasSize(1)
        assertThat(harness.client.reconciliationCounts).isEmpty()
    }

    @Test
    fun `timeout followed by rejection never resends the order`() {
        val harness = harness()
        harness.client.onPlace = { request ->
            harness.events.tryEmitNext(
                orderUpdate(request, status = "REJECTED", filledQuantity = "0"),
            )
            Mono.error(TimeoutException("lost response"))
        }

        val resolution = harness.service.execute(entryRequest()).block(TIMEOUT)!!

        assertThat(resolution.outcome).isEqualTo(OrderOutcome.REJECTED)
        assertThat(resolution.actualFilledQuantity).isEqualByComparingTo("0")
        assertThat(harness.client.placements).hasSize(1)
        assertThat(harness.client.reconciliationCounts).isEmpty()
    }

    @Test
    fun `partial fill consumes actual quantity rather than requested quantity`() {
        val harness = harness()
        harness.client.onPlace = { request ->
            harness.events.tryEmitNext(
                orderUpdate(
                    request = request,
                    status = "PARTIALLY_FILLED",
                    filledQuantity = "0.18",
                ),
            )
            Mono.just(request.acknowledgement())
        }

        val resolution = harness.service.execute(entryRequest()).block(TIMEOUT)!!

        assertThat(resolution.outcome)
            .isEqualTo(OrderOutcome.PARTIALLY_FILLED)
        assertThat(resolution.intent.confirmedQuantity)
            .isEqualByComparingTo("0.30")
        assertThat(resolution.actualFilledQuantity)
            .isEqualByComparingTo("0.18")
        assertThat(resolution.confirmedPositionAmount)
            .isEqualByComparingTo("0.18")
    }

    @Test
    fun `private cancellation resolves without REST retry`() {
        val harness = harness()
        harness.client.onPlace = { request ->
            harness.events.tryEmitNext(
                orderUpdate(request, status = "CANCELED", filledQuantity = "0"),
            )
            Mono.just(request.acknowledgement())
        }

        val resolution = harness.service.execute(entryRequest()).block(TIMEOUT)!!

        assertThat(resolution.outcome).isEqualTo(OrderOutcome.CANCELED)
        assertThat(harness.client.placements).hasSize(1)
        assertThat(harness.client.reconciliationCounts).isEmpty()
    }

    @Test
    fun `hard stop confirms only as one exact active exchange-side trigger`() {
        val harness = harness()
        harness.client.onReconcile = { _, clientOrderId ->
            Mono.just(
                activeHardStopReconciliation(
                    clientOrderId = clientOrderId,
                    stopPrice = BigDecimal("99.7"),
                    workingType = "CONTRACT_PRICE",
                    priceProtect = false,
                ),
            )
        }

        val confirmation = harness.service
            .confirmHardStop(hardStopRequest())
            .block(TIMEOUT)!!

        assertThat(confirmation.confirmed).isTrue()
        assertThat(confirmation.observedStopPrice)
            .isEqualByComparingTo("99.7")
        assertThat(confirmation.observedWorkingType)
            .isEqualTo(TriggerWorkingType.CONTRACT_PRICE)
        assertThat(confirmation.observedPriceProtect).isFalse()
        assertThat(confirmation.reconciliationChecks).isEqualTo(1)
        assertThat(harness.client.placements).hasSize(1)
        val placement = harness.client.placements.single()
        assertThat(placement.type).isEqualTo("STOP_MARKET")
        assertThat(placement.quantity).isNull()
        assertThat(placement.closePosition).isTrue()
        assertThat(placement.workingType).isEqualTo("CONTRACT_PRICE")
        assertThat(placement.priceProtect).isFalse()
        assertThat(harness.service.currentState().orders.single().outcome)
            .isEqualTo(OrderOutcome.ACTIVE)
        assertThat(harness.service.currentState().entriesAndAdditionsBlocked)
            .isFalse()
    }

    @Test
    fun `hard stop rejects a mismatched exchange trigger within the deadline`() {
        val harness = harness()
        harness.client.onReconcile = { _, clientOrderId ->
            Mono.just(
                activeHardStopReconciliation(
                    clientOrderId = clientOrderId,
                    stopPrice = BigDecimal("99.6"),
                    workingType = "MARK_PRICE",
                    priceProtect = true,
                ),
            )
        }

        val confirmation = harness.service
            .confirmHardStop(hardStopRequest())
            .block(TIMEOUT)!!

        assertThat(confirmation.confirmed).isFalse()
        assertThat(confirmation.reconciliationChecks).isGreaterThan(0)
        assertThat(harness.client.placements).hasSize(1)
        assertThat(harness.service.currentState().orders.single().outcome)
            .isEqualTo(OrderOutcome.UNKNOWN)
        assertThat(harness.service.currentState().orders.single().reason)
            .isEqualTo(ExecutionReasonCode.STOP_SETUP_FAILED)
        assertThat(harness.service.currentState().entriesAndAdditionsBlocked)
            .isTrue()
    }

    @Test
    fun `complete reduce-only GTC target set is verified by exact identities`() {
        val harness = harness()
        harness.client.onReconcile = { _, clientOrderId ->
            val placement = harness.client.placements.single { request ->
                request.clientOrderId == clientOrderId
            }
            Mono.just(activeTakeProfitReconciliation(placement))
        }

        val confirmation = harness.service
            .confirmTakeProfits(
                takeProfitRequests(),
                Duration.ofMillis(100),
            )
            .block(TIMEOUT)!!

        assertThat(confirmation.confirmed).isTrue()
        assertThat(confirmation.intents.map(OrderIntent::slot))
            .containsExactly(1, 2, 3)
        assertThat(confirmation.intents.map(OrderIntent::clientOrderId))
            .allSatisfy { clientOrderId ->
                assertThat(clientOrderId).matches("^[.A-Za-z0-9_:/-]{1,36}$")
            }
        assertThat(harness.client.placements).hasSize(3)
        assertThat(harness.client.placements).allSatisfy { placement ->
            assertThat(placement.type).isEqualTo("LIMIT")
            assertThat(placement.timeInForce).isEqualTo("GTC")
            assertThat(placement.reduceOnly).isTrue()
            assertThat(placement.closePosition).isFalse()
        }
        assertThat(harness.service.currentState().orders)
            .allSatisfy { order ->
                assertThat(order.role).isEqualTo(OrderRole.TAKE_PROFIT)
                assertThat(order.outcome).isEqualTo(OrderOutcome.ACTIVE)
            }
    }

    @Test
    fun `partial exchange target set remains unconfirmed and every fragment is canceled`() {
        val harness = harness()
        harness.client.onReconcile = { _, clientOrderId ->
            val placement = harness.client.placements.single { request ->
                request.clientOrderId == clientOrderId
            }
            if (placement.price == BigDecimal("102.0")) {
                Mono.just(
                    BinanceOrderReconciliation(
                        order = null,
                        position = BinancePositionRisk(
                            symbol = SYMBOL,
                            positionAmount = BigDecimal("0.30"),
                            entryPrice = BigDecimal("100.0"),
                        ),
                        openClientOrderIds = emptySet(),
                    ),
                )
            } else {
                Mono.just(activeTakeProfitReconciliation(placement))
            }
        }

        val confirmation = harness.service
            .confirmTakeProfits(
                takeProfitRequests(),
                Duration.ofMillis(100),
            )
            .block(TIMEOUT)!!
        val cancellationComplete = harness.service
            .cancelTakeProfits(confirmation.intents)
            .block(TIMEOUT)!!

        assertThat(confirmation.confirmed).isFalse()
        assertThat(cancellationComplete).isTrue()
        assertThat(harness.client.cancellations).hasSize(3)
        assertThat(harness.service.currentState().orders)
            .allSatisfy { order ->
                assertThat(order.outcome).isEqualTo(OrderOutcome.CANCELED)
            }
    }

    @Test
    fun `confirmed target fills shrink exposure while the original hard stop stays unchanged`() {
        val harness = harness()
        harness.client.onReconcile = { _, clientOrderId ->
            val placement = harness.client.placements.single { request ->
                request.clientOrderId == clientOrderId
            }
            if (placement.type == "STOP_MARKET") {
                Mono.just(
                    activeHardStopReconciliation(
                        clientOrderId = clientOrderId,
                        stopPrice = BigDecimal("99.7"),
                        workingType = "CONTRACT_PRICE",
                        priceProtect = false,
                    ),
                )
            } else {
                Mono.just(activeTakeProfitReconciliation(placement))
            }
        }
        harness.service.confirmHardStop(hardStopRequest()).block(TIMEOUT)
        val confirmation = harness.service
            .confirmTakeProfits(
                takeProfitRequests(),
                Duration.ofMillis(100),
            )
            .block(TIMEOUT)!!
        harness.service.activateTakeProfits(confirmation).block(TIMEOUT)
        val fills = mutableListOf<PositionReduction>()
        val fillSubscription = harness.service.positionReductions().subscribe(fills::add)

        val placements = confirmation.intents.map { intent ->
            harness.client.placements.single { request ->
                request.clientOrderId == intent.clientOrderId
            }
        }
        harness.events.tryEmitNext(
            orderUpdate(
                request = placements[0],
                status = "PARTIALLY_FILLED",
                filledQuantity = "0.050",
                tradeId = 2101L,
            ),
        )
        harness.events.tryEmitNext(
            orderUpdate(
                request = placements[0],
                status = "FILLED",
                filledQuantity = "0.099",
                lastFilledQuantity = "0.049",
                tradeId = 2102L,
            ),
        )
        harness.events.tryEmitNext(
            orderUpdate(
                request = placements[1],
                status = "FILLED",
                filledQuantity = "0.099",
                tradeId = 2103L,
            ),
        )
        harness.events.tryEmitNext(
            orderUpdate(
                request = placements[2],
                status = "FILLED",
                filledQuantity = "0.102",
                tradeId = 2104L,
            ),
        )

        assertThat(fills.map(PositionReduction::confirmedRemainingQuantity))
            .usingElementComparator(BigDecimal::compareTo)
            .containsExactly(
                BigDecimal("0.250"),
                BigDecimal("0.201"),
                BigDecimal("0.102"),
                BigDecimal.ZERO,
            )
        assertThat(fills.last().terminalReason)
            .isEqualTo(LevelReasonCode.TAKE_PROFITS_COMPLETE)
        assertThat(fills.last().netResult).isEqualTo(
            PositionNetResult(
                grossPnl = BigDecimal.ZERO,
                fees = BigDecimal("0.04"),
                funding = BigDecimal.ZERO,
                slippage = BigDecimal("0.26190"),
                netPnl = BigDecimal("-0.04"),
            ),
        )
        val hardStop = harness.service.currentState().orders.single { order ->
            order.role == OrderRole.HARD_STOP
        }
        assertThat(hardStop.stopPrice).isEqualByComparingTo("99.7")
        assertThat(hardStop.outcome).isEqualTo(OrderOutcome.ACTIVE)
        fillSubscription.dispose()
    }

    @Test
    fun `hard-stop fill emits a terminal reduction with known net result`() {
        val harness = harness()
        harness.client.onReconcile = { _, clientOrderId ->
            Mono.just(
                activeHardStopReconciliation(
                    clientOrderId = clientOrderId,
                    stopPrice = BigDecimal("99.7"),
                    workingType = "CONTRACT_PRICE",
                    priceProtect = false,
                ),
            )
        }
        harness.service.confirmHardStop(hardStopRequest()).block(TIMEOUT)
        val reductions = mutableListOf<PositionReduction>()
        val subscription = harness.service.positionReductions()
            .subscribe(reductions::add)
        harness.events.tryEmitNext(
            accountUpdate("0.30").copy(
                reason = "FUNDING_FEE",
                balances = listOf(
                    BinanceBalanceUpdate(
                        asset = "USDT",
                        walletBalance = BigDecimal("999.95"),
                        crossWalletBalance = BigDecimal("999.95"),
                        balanceChange = BigDecimal("-0.05"),
                    ),
                ),
            ),
        )
        val hardStop = harness.client.placements.single { request ->
            request.type == "STOP_MARKET"
        }
        harness.events.tryEmitNext(
            orderUpdate(
                request = hardStop,
                status = "FILLED",
                filledQuantity = "0.30",
                fillPrice = "99.5",
                commission = "0.02",
                realizedProfit = "-1.50",
                tradeId = 3001L,
            ),
        )

        assertThat(reductions).hasSize(1)
        val reduction = reductions.single()
        assertThat(reduction.role).isEqualTo(OrderRole.HARD_STOP)
        assertThat(reduction.confirmedRemainingQuantity)
            .isEqualByComparingTo("0")
        assertThat(reduction.terminalReason)
            .isEqualTo(LevelReasonCode.HARD_STOP_FILLED)
        assertThat(reduction.netResult).isEqualTo(
            PositionNetResult(
                grossPnl = BigDecimal("-1.50"),
                fees = BigDecimal("0.02"),
                funding = BigDecimal("-0.05"),
                slippage = BigDecimal("0.060"),
                netPnl = BigDecimal("-1.57"),
            ),
        )
        subscription.dispose()
    }

    @Test
    fun `normal exit places once waits then performs one authoritative reconciliation`() {
        val harness = harness()
        var reconciliationCount = 0
        harness.client.onReconcile = { _, clientOrderId ->
            reconciliationCount += 1
            Mono.just(
                BinanceOrderReconciliation(
                    order = reconciledOrder(
                        clientOrderId = clientOrderId,
                        status = "CANCELED",
                        filledQuantity = "0.18",
                    ).copy(
                        reduceOnly = true,
                        type = "LIMIT",
                        side = "SELL",
                        timeInForce = "IOC",
                        price = BigDecimal("99.8"),
                    ),
                    position = BinancePositionRisk(
                        symbol = SYMBOL,
                        positionAmount = BigDecimal("0.12"),
                        entryPrice = BigDecimal("100"),
                    ),
                    openClientOrderIds = emptySet(),
                ),
            )
        }

        val result = harness.service.executeNormalExit(
            request = closeRequest().copy(
                slot = 0,
                type = OrderType.LIMIT,
                timeInForce = OrderTimeInForce.IOC,
                price = BigDecimal("99.8"),
            ),
            wait = Duration.ofMillis(20),
        ).block(TIMEOUT)!!

        assertThat(harness.client.placements).hasSize(1)
        val placement = harness.client.placements.single()
        assertThat(placement.type).isEqualTo("LIMIT")
        assertThat(placement.timeInForce).isEqualTo("IOC")
        assertThat(placement.reduceOnly).isTrue()
        assertThat(reconciliationCount).isEqualTo(1)
        assertThat(result.outcome).isEqualTo(OrderOutcome.PARTIALLY_FILLED)
        assertThat(result.confirmedPositionAmount).isEqualByComparingTo("0.12")
        assertThat(result.hasUnresolvedOrder).isFalse()
    }

    @Test
    fun `REST reconciliation classifies every resolved terminal outcome`() {
        val harness = harness()
        val cases = listOf(
            Triple("FILLED", "0.30", OrderOutcome.FILLED),
            Triple("PARTIALLY_FILLED", "0.18", OrderOutcome.PARTIALLY_FILLED),
            Triple("REJECTED", "0", OrderOutcome.REJECTED),
            Triple("CANCELED", "0", OrderOutcome.CANCELED),
        )

        cases.forEachIndexed { index, (status, filledQuantity, expected) ->
            harness.client.onReconcile = { _, clientOrderId ->
                Mono.just(
                    BinanceOrderReconciliation(
                        order = reconciledOrder(
                            clientOrderId = clientOrderId,
                            status = status,
                            filledQuantity = filledQuantity,
                        ),
                        position = BinancePositionRisk(
                            symbol = SYMBOL,
                            positionAmount = BigDecimal(filledQuantity),
                            entryPrice = BigDecimal("100.50"),
                        ),
                        openClientOrderIds = emptySet(),
                    ),
                )
            }

            val resolution = harness.service
                .execute(entryRequest(slot = index + 1))
                .block(TIMEOUT)!!

            assertThat(resolution.outcome).isEqualTo(expected)
            assertThat(resolution.source)
                .isEqualTo(OrderResolutionSource.REST_RECONCILIATION)
            assertThat(resolution.reconciliationChecks).isEqualTo(1)
        }
        assertThat(harness.client.placements).hasSize(cases.size)
    }

    @Test
    fun `permanently unknown outcome checks three times enters safe mode and closes only confirmed exposure`() {
        val harness = harness()
        harness.client.onPlace = { request ->
            if (request.reduceOnly) {
                harness.events.tryEmitNext(
                    orderUpdate(
                        request = request,
                        status = "FILLED",
                        filledQuantity = checkNotNull(request.quantity).toPlainString(),
                    ),
                )
            }
            Mono.just(request.acknowledgement())
        }
        harness.client.onReconcile = { _, clientOrderId ->
            Mono.just(
                BinanceOrderReconciliation(
                    order = null,
                    position = BinancePositionRisk(
                        symbol = SYMBOL,
                        positionAmount = BigDecimal("0.25"),
                        entryPrice = BigDecimal("100"),
                    ),
                    openClientOrderIds = emptySet(),
                ),
            ).doOnNext {
                harness.client.reconciliationCounts.merge(
                    clientOrderId,
                    1,
                    { current, increment -> current + increment },
                )
            }
        }

        val resolution = harness.service.execute(entryRequest()).block(TIMEOUT)!!
        val close = harness.client.placements.single { request -> request.reduceOnly }

        assertThat(resolution.outcome).isEqualTo(OrderOutcome.UNKNOWN)
        assertThat(resolution.reason)
            .isEqualTo(ExecutionReasonCode.ORDER_OUTCOME_UNKNOWN)
        assertThat(resolution.reconciliationChecks).isEqualTo(3)
        assertThat(harness.client.reconciliationCounts[resolution.intent.clientOrderId])
            .isEqualTo(3)
        assertThat(harness.client.placements).hasSize(2)
        assertThat(close.side).isEqualTo(OrderSide.SELL.name)
        assertThat(close.type).isEqualTo(OrderType.MARKET.name)
        assertThat(close.quantity).isEqualByComparingTo("0.25")
        assertThat(close.closePosition).isFalse()
        assertThat(
            harness.riskService.currentState().globalTradingState,
        ).isEqualTo(GlobalTradingState.SAFE_MODE)
        assertThat(harness.service.currentState().entriesAndAdditionsBlocked)
            .isTrue()
        assertThatThrownBy {
            harness.service.execute(entryRequest(slot = 2)).block(TIMEOUT)
        }.isInstanceOf(OrderExecutionException::class.java)
    }

    @Test
    fun `unknown outcome never closes exposure that REST did not confirm`() {
        val harness = harness()
        harness.client.onReconcile = { _, clientOrderId ->
            harness.client.reconciliationCounts.merge(
                clientOrderId,
                1,
                { current, increment -> current + increment },
            )
            Mono.just(
                BinanceOrderReconciliation(
                    order = null,
                    position = null,
                    openClientOrderIds = emptySet(),
                    safeDetail = "position reconciliation unavailable",
                ),
            )
        }

        val resolution = harness.service.execute(entryRequest()).block(TIMEOUT)!!

        assertThat(resolution.outcome).isEqualTo(OrderOutcome.UNKNOWN)
        assertThat(resolution.confirmedPositionAmount)
            .isEqualByComparingTo(BigDecimal.ZERO)
        assertThat(harness.client.reconciliationCounts[resolution.intent.clientOrderId])
            .isEqualTo(3)
        assertThat(harness.client.placements).hasSize(1)
    }

    @Test
    fun `closing intents cannot increase reverse or exceed confirmed exposure`() {
        val factory = ClientOrderIdFactory(STARTED_AT)

        assertThatThrownBy {
            factory.create(closeRequest(side = OrderSide.BUY))
        }.isInstanceOf(OrderExecutionException::class.java)
            .hasMessageContaining("oppose")
        assertThatThrownBy {
            factory.create(
                closeRequest(confirmedQuantity = BigDecimal("0.31")),
            )
        }.isInstanceOf(OrderExecutionException::class.java)
            .hasMessageContaining("cannot exceed")
        assertThatThrownBy {
            factory.create(
                closeRequest(reduceOnly = false),
            )
        }.isInstanceOf(OrderExecutionException::class.java)
            .hasMessageContaining("reduce-only or close-position")
    }

    private fun harness(): ExecutionHarness {
        val events = Sinks.many().multicast().onBackpressureBuffer<BinanceUserDataEvent>()
        val client = FakeBinanceExecutionClient()
        val coordinator = ImmediateSymbolExecutionCoordinator()
        val riskScheduler = Schedulers.newSingle("execution-test-risk")
        val executionScheduler = Schedulers.newSingle("execution-test-reconcile")
        val riskService = AttemptRiskService(
            clock = CLOCK,
            scheduler = riskScheduler,
            evidenceRecorder = NoOpEvidenceRecorder,
        )
        val service = ExecutionService(
            client = client,
            privateEvents = events.asFlux(),
            symbolCoordinator = coordinator,
            riskService = riskService,
            clientOrderIdFactory = ClientOrderIdFactory(STARTED_AT),
            evidenceRecorder = NoOpEvidenceRecorder,
            clock = CLOCK,
            scheduler = executionScheduler,
            requestTimeout = Duration.ofMillis(10),
            reconciliationInterval = Duration.ofMillis(10),
            stopConfirmationTimeout = Duration.ofMillis(100),
        )
        resources += AutoCloseable {
            service.close()
            riskService.close()
            executionScheduler.dispose()
        }
        return ExecutionHarness(
            service = service,
            client = client,
            events = events,
            coordinator = coordinator,
            riskService = riskService,
        )
    }

    private fun entryRequest(slot: Int = 1): OrderIntentRequest =
        OrderIntentRequest(
            levelId = LEVEL_ID,
            attemptNumber = 1,
            symbol = SYMBOL,
            role = OrderRole.ENTRY,
            slot = slot,
            side = OrderSide.BUY,
            type = OrderType.LIMIT,
            timeInForce = OrderTimeInForce.IOC,
            confirmedQuantity = BigDecimal("0.30"),
            price = BigDecimal("101"),
        )

    private fun closeRequest(
        side: OrderSide = OrderSide.SELL,
        confirmedQuantity: BigDecimal = BigDecimal("0.30"),
        reduceOnly: Boolean = true,
    ): OrderIntentRequest =
        OrderIntentRequest(
            levelId = LEVEL_ID,
            attemptNumber = 1,
            symbol = SYMBOL,
            role = OrderRole.CLOSE,
            slot = 1,
            side = side,
            type = OrderType.MARKET,
            confirmedQuantity = confirmedQuantity,
            reduceOnly = reduceOnly,
            confirmedPositionAmount = BigDecimal("0.30"),
        )

    private fun hardStopRequest(): OrderIntentRequest =
        OrderIntentRequest(
            levelId = LEVEL_ID,
            attemptNumber = 1,
            symbol = SYMBOL,
            role = OrderRole.HARD_STOP,
            slot = 0,
            side = OrderSide.SELL,
            type = OrderType.STOP_MARKET,
            stopPrice = BigDecimal("99.7"),
            workingType = TriggerWorkingType.CONTRACT_PRICE,
            priceProtect = false,
            closePosition = true,
            confirmedPositionAmount = BigDecimal("0.30"),
        )

    private fun takeProfitRequests(): List<OrderIntentRequest> = listOf(
        BigDecimal("0.099") to BigDecimal("100.7"),
        BigDecimal("0.099") to BigDecimal("101.4"),
        BigDecimal("0.102") to BigDecimal("102.0"),
    ).mapIndexed { index, (quantity, price) ->
        OrderIntentRequest(
            levelId = LEVEL_ID,
            attemptNumber = 1,
            symbol = SYMBOL,
            role = OrderRole.TAKE_PROFIT,
            slot = index + 1,
            side = OrderSide.SELL,
            type = OrderType.LIMIT,
            timeInForce = OrderTimeInForce.GTC,
            confirmedQuantity = quantity,
            price = price,
            reduceOnly = true,
            confirmedPositionAmount = BigDecimal("0.30"),
        )
    }

    private fun activeTakeProfitReconciliation(
        request: BinanceOrderRequest,
    ): BinanceOrderReconciliation = BinanceOrderReconciliation(
        order = BinanceOrderStatus(
            symbol = SYMBOL,
            clientOrderId = request.clientOrderId,
            orderId = 1100L,
            status = "NEW",
            originalQuantity = checkNotNull(request.quantity),
            executedQuantity = BigDecimal.ZERO,
            averagePrice = BigDecimal.ZERO,
            reduceOnly = true,
            closePosition = false,
            updatedAt = EVENT_AT,
            type = request.type,
            side = request.side,
            timeInForce = request.timeInForce,
            price = request.price,
        ),
        position = BinancePositionRisk(
            symbol = SYMBOL,
            positionAmount = BigDecimal("0.30"),
            entryPrice = BigDecimal("100.0"),
        ),
        openClientOrderIds = setOf(request.clientOrderId),
    )

    private fun activeHardStopReconciliation(
        clientOrderId: String,
        stopPrice: BigDecimal,
        workingType: String,
        priceProtect: Boolean,
    ): BinanceOrderReconciliation =
        BinanceOrderReconciliation(
            order = BinanceOrderStatus(
                symbol = SYMBOL,
                clientOrderId = clientOrderId,
                orderId = 1002L,
                status = "NEW",
                originalQuantity = BigDecimal.ZERO,
                executedQuantity = BigDecimal.ZERO,
                averagePrice = BigDecimal.ZERO,
                reduceOnly = false,
                closePosition = true,
                updatedAt = EVENT_AT,
                type = "STOP_MARKET",
                stopPrice = stopPrice,
                workingType = workingType,
                priceProtect = priceProtect,
            ),
            position = BinancePositionRisk(
                symbol = SYMBOL,
                positionAmount = BigDecimal("0.30"),
                entryPrice = BigDecimal("100.0"),
            ),
            openClientOrderIds = setOf(clientOrderId),
        )

    private fun orderUpdate(
        request: BinanceOrderRequest,
        status: String,
        filledQuantity: String,
        lastFilledQuantity: String = filledQuantity,
        fillPrice: String = "100.50",
        commission: String = "0.01",
        realizedProfit: String = "0",
        tradeId: Long = 2001L,
    ): BinanceUserDataEvent.OrderUpdate =
        BinanceUserDataEvent.OrderUpdate(
            eventTime = EVENT_AT,
            transactionTime = EVENT_AT,
            receivedAt = EVENT_AT,
            symbol = request.symbol,
            clientOrderId = request.clientOrderId,
            side = request.side,
            orderType = request.type,
            timeInForce = request.timeInForce.orEmpty(),
            originalQuantity = request.quantity ?: BigDecimal("0.30"),
            originalPrice = request.price ?: BigDecimal.ZERO,
            averagePrice = BigDecimal(fillPrice),
            stopPrice = request.stopPrice ?: BigDecimal.ZERO,
            executionType = if (status == "REJECTED") "NEW" else "TRADE",
            orderStatus = status,
            orderId = 1001L,
            lastFilledQuantity = BigDecimal(lastFilledQuantity),
            accumulatedFilledQuantity = BigDecimal(filledQuantity),
            lastFilledPrice = BigDecimal(fillPrice),
            commissionAsset = "USDT",
            commission = BigDecimal(commission),
            tradeId = tradeId,
            realizedProfit = BigDecimal(realizedProfit),
            positionSide = "BOTH",
            reduceOnly = request.reduceOnly,
        )

    private fun accountUpdate(
        positionAmount: String,
    ): BinanceUserDataEvent.AccountUpdate =
        BinanceUserDataEvent.AccountUpdate(
            eventTime = EVENT_AT,
            transactionTime = EVENT_AT,
            receivedAt = EVENT_AT,
            reason = "ORDER",
            balances = listOf(
                BinanceBalanceUpdate(
                    asset = "USDT",
                    walletBalance = BigDecimal("1000"),
                    crossWalletBalance = BigDecimal("1000"),
                    balanceChange = BigDecimal.ZERO,
                ),
            ),
            positions = listOf(
                BinancePositionUpdate(
                    symbol = SYMBOL,
                    positionAmount = BigDecimal(positionAmount),
                    entryPrice = BigDecimal("100.50"),
                    breakEvenPrice = BigDecimal("100.51"),
                    accumulatedRealizedProfit = BigDecimal.ZERO,
                    unrealizedProfit = BigDecimal.ZERO,
                    marginType = "isolated",
                    isolatedWallet = BigDecimal("10"),
                    positionSide = "BOTH",
                ),
            ),
        )

    private fun reconciledOrder(
        clientOrderId: String,
        status: String,
        filledQuantity: String,
    ): BinanceOrderStatus =
        BinanceOrderStatus(
            symbol = SYMBOL,
            clientOrderId = clientOrderId,
            orderId = 1001L,
            status = status,
            originalQuantity = BigDecimal("0.30"),
            executedQuantity = BigDecimal(filledQuantity),
            averagePrice = BigDecimal("100.50"),
            reduceOnly = false,
            closePosition = false,
            updatedAt = EVENT_AT,
        )
}

private data class ExecutionHarness(
    val service: ExecutionService,
    val client: FakeBinanceExecutionClient,
    val events: Sinks.Many<BinanceUserDataEvent>,
    val coordinator: ImmediateSymbolExecutionCoordinator,
    val riskService: AttemptRiskService,
)

private class FakeBinanceExecutionClient : BinanceExecutionClient {
    val placements = mutableListOf<BinanceOrderRequest>()
    val cancellations = mutableListOf<Pair<String, String>>()
    val reconciliationCounts = ConcurrentHashMap<String, Int>()
    var onPlace: (BinanceOrderRequest) -> Mono<BinanceOrderAcknowledgement> =
        { request -> Mono.just(request.acknowledgement()) }
    var onReconcile: (String, String) -> Mono<BinanceOrderReconciliation> =
        { symbol, _ ->
            Mono.just(
                BinanceOrderReconciliation(
                    order = null,
                    position = BinancePositionRisk(
                        symbol = symbol,
                        positionAmount = BigDecimal.ZERO,
                        entryPrice = BigDecimal.ZERO,
                    ),
                    openClientOrderIds = emptySet(),
                ),
            )
        }

    override fun placeOrder(
        request: BinanceOrderRequest,
    ): Mono<BinanceOrderAcknowledgement> =
        Mono.defer {
            placements += request
            onPlace(request)
        }

    override fun cancelOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<Void> = Mono.fromRunnable<Void> {
        cancellations += symbol to clientOrderId
    }.then()

    override fun reconcileOrder(
        symbol: String,
        clientOrderId: String,
    ): Mono<BinanceOrderReconciliation> =
        onReconcile(symbol, clientOrderId)
}

private class ImmediateSymbolExecutionCoordinator : SymbolExecutionCoordinator {
    val events = mutableListOf<String>()
    val ownership = mutableListOf<RecordedOwnership>()

    override fun submit(
        symbol: String,
        eventId: String,
        action: () -> Any,
    ): Mono<Any> =
        Mono.fromCallable {
            events += eventId
            action()
        }

    override fun recordOwnership(
        levelId: UUID,
        ownsActiveAttempt: Boolean,
        ownsExposure: Boolean,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void> =
        Mono.fromRunnable<Void> {
            ownership += RecordedOwnership(
                levelId = levelId,
                ownsActiveAttempt = ownsActiveAttempt,
                ownsExposure = ownsExposure,
                hasUnresolvedOrder = hasUnresolvedOrder,
            )
        }.then()
}

private data class RecordedOwnership(
    val levelId: UUID,
    val ownsActiveAttempt: Boolean,
    val ownsExposure: Boolean,
    val hasUnresolvedOrder: Boolean,
)

private fun BinanceOrderRequest.acknowledgement(): BinanceOrderAcknowledgement =
    BinanceOrderAcknowledgement(
        symbol = symbol,
        clientOrderId = clientOrderId,
        orderId = 1001L,
        status = "NEW",
    )

private val STARTED_AT: Instant = Instant.parse("2026-08-01T07:00:00Z")
private val EVENT_AT: Instant = STARTED_AT.plusSeconds(1)
private val CLOCK: Clock = Clock.fixed(EVENT_AT, ZoneOffset.UTC)
private val LEVEL_ID: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555")
private const val SYMBOL = "BTCUSDT"
private val TIMEOUT: Duration = Duration.ofSeconds(2)
