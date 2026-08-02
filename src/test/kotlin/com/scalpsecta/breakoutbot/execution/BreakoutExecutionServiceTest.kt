package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.level.PositionNetResult
import com.scalpsecta.breakoutbot.risk.AttemptAdmissionRequest
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import com.scalpsecta.breakoutbot.risk.RiskAccountState
import com.scalpsecta.breakoutbot.risk.RiskLeverageBracket
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

class BreakoutExecutionServiceTest {
    private val resources = mutableListOf<AutoCloseable>()

    @AfterEach
    fun closeResources() {
        resources.reversed().forEach(AutoCloseable::close)
    }

    @Test
    fun `crossing and final tranches are mirrored capped IOC additions`() {
        listOf(LevelDirection.LONG, LevelDirection.SHORT).forEach { direction ->
            val harness = harness(direction)

            val crossing = harness.service
                .execute(addition(direction, BreakoutTranche.CROSSING, "3", "3"))
                .block(TIMEOUT)!!
            val final = harness.service
                .execute(addition(direction, BreakoutTranche.FINAL, "4", "6"))
                .block(TIMEOUT)!!

            assertThat(crossing.status)
                .isEqualTo(BreakoutResultStatus.CONFIRMING)
            assertThat(final.status).isEqualTo(BreakoutResultStatus.CONFIRMED)
            val additions = harness.executor.requests.filter { request ->
                request.role == OrderRole.ADDITION
            }
            assertThat(additions.map(OrderIntentRequest::slot))
                .containsExactly(1, 2)
            assertThat(additions.map(OrderIntentRequest::type))
                .containsOnly(OrderType.LIMIT)
            assertThat(additions.map(OrderIntentRequest::timeInForce))
                .containsOnly(OrderTimeInForce.IOC)
            assertThat(additions.map { request -> checkNotNull(request.confirmedQuantity) })
                .usingElementComparator(BigDecimal::compareTo)
                .containsExactly(BigDecimal("3"), BigDecimal("4"))
            when (direction) {
                LevelDirection.LONG -> {
                    assertThat(additions.map(OrderIntentRequest::side))
                        .containsOnly(OrderSide.BUY)
                    assertThat(additions.map { request -> checkNotNull(request.price) })
                        .usingElementComparator(BigDecimal::compareTo)
                        .containsOnly(BigDecimal("100.2"))
                }

                LevelDirection.SHORT -> {
                    assertThat(additions.map(OrderIntentRequest::side))
                        .containsOnly(OrderSide.SELL)
                    assertThat(additions.map { request -> checkNotNull(request.price) })
                        .usingElementComparator(BigDecimal::compareTo)
                        .containsOnly(BigDecimal("99.8"))
                }
            }
            assertThat(harness.coordinator.crossingFills).containsExactly("cross")
            assertThat(harness.coordinator.finalFills).containsExactly("final")
            val takeProfits = harness.executor.requests.filter { request ->
                request.role == OrderRole.TAKE_PROFIT
            }
            assertThat(takeProfits.map(OrderIntentRequest::slot))
                .containsExactly(1, 2, 3)
            assertThat(takeProfits.map(OrderIntentRequest::type))
                .containsOnly(OrderType.LIMIT)
            assertThat(takeProfits.map(OrderIntentRequest::timeInForce))
                .containsOnly(OrderTimeInForce.GTC)
            assertThat(takeProfits).allSatisfy { takeProfit ->
                assertThat(takeProfit.reduceOnly).isTrue()
                assertThat(takeProfit.confirmedPositionAmount)
                    .isEqualByComparingTo(
                        if (direction == LevelDirection.LONG) "10" else "-10",
                    )
            }
            assertThat(takeProfits.map { request -> checkNotNull(request.price) })
                .usingElementComparator(BigDecimal::compareTo)
                .containsExactlyElementsOf(
                    if (direction == LevelDirection.LONG) {
                        listOf(
                            BigDecimal("100.7"),
                            BigDecimal("101.4"),
                            BigDecimal("102.0"),
                        )
                    } else {
                        listOf(
                            BigDecimal("99.3"),
                            BigDecimal("98.6"),
                            BigDecimal("98.0"),
                        )
                    },
                )
            assertThat(takeProfits.map { request ->
                checkNotNull(request.confirmedQuantity)
            })
                .usingElementComparator(BigDecimal::compareTo)
                .containsExactly(
                    BigDecimal("3.300"),
                    BigDecimal("3.300"),
                    BigDecimal("3.400"),
                )
            assertThat(harness.riskService.currentState().attempts.single()
                .confirmedPositionQuantity).isEqualByComparingTo("10")
        }
    }

    @Test
    fun `rounding residue is assigned to the third executable target`() {
        val harness = harness(LevelDirection.LONG)
        harness.riskService
            .recordConfirmedExposure(LEVEL_ID, BigDecimal("6"))
            .block(TIMEOUT)

        val result = harness.service.execute(
            addition(
                direction = LevelDirection.LONG,
                tranche = BreakoutTranche.FINAL,
                requestedQuantity = "3.999",
                confirmedQuantity = "6",
            ),
        ).block(TIMEOUT)!!

        assertThat(result.status).isEqualTo(BreakoutResultStatus.CONFIRMED)
        val quantities = harness.executor.requests
            .filter { request -> request.role == OrderRole.TAKE_PROFIT }
            .map { request -> checkNotNull(request.confirmedQuantity) }
        assertThat(quantities)
            .usingElementComparator(BigDecimal::compareTo)
            .containsExactly(
                BigDecimal("3.299"),
                BigDecimal("3.299"),
                BigDecimal("3.401"),
            )
        assertThat(quantities.fold(BigDecimal.ZERO, BigDecimal::add))
            .isEqualByComparingTo("9.999")
    }

    @Test
    fun `target prices use side-safe tick rounding for both directions`() {
        listOf(LevelDirection.LONG, LevelDirection.SHORT).forEach { direction ->
            val harness = harness(direction)
            harness.riskService
                .recordConfirmedExposure(LEVEL_ID, BigDecimal("6"))
                .block(TIMEOUT)
            val request = addition(
                direction = direction,
                tranche = BreakoutTranche.FINAL,
                requestedQuantity = "4",
                confirmedQuantity = "6",
            ).copy(
                levelPrice = BigDecimal("99.9"),
                maxImpulsePct = BigDecimal.ONE,
                tickSize = BigDecimal("0.3"),
            )

            harness.service.execute(request).block(TIMEOUT)

            val prices = harness.executor.requests
                .filter { candidate ->
                    candidate.role == OrderRole.TAKE_PROFIT
                }
                .map { candidate -> checkNotNull(candidate.price) }
            assertThat(prices)
                .usingElementComparator(BigDecimal::compareTo)
                .containsExactlyElementsOf(
                    if (direction == LevelDirection.LONG) {
                        listOf(
                            BigDecimal("100.2"),
                            BigDecimal("100.5"),
                            BigDecimal("100.8"),
                        )
                    } else {
                        listOf(
                            BigDecimal("99.6"),
                            BigDecimal("99.3"),
                            BigDecimal("99.0"),
                        )
                    },
                )
        }
    }

    @Test
    fun `incomplete target set is canceled closed and enters safe mode`() {
        val harness = harness(LevelDirection.LONG)
        harness.riskService
            .recordConfirmedExposure(LEVEL_ID, BigDecimal("6"))
            .block(TIMEOUT)
        harness.executor.takeProfitConfirmed = false

        val result = harness.service.execute(
            addition(LevelDirection.LONG, BreakoutTranche.FINAL, "4", "6"),
        ).block(TIMEOUT)!!

        assertThat(result.terminalReason)
            .isEqualTo(LevelReasonCode.TP_SETUP_FAILED)
        assertThat(harness.coordinator.finalFills).isEmpty()
        assertThat(harness.executor.cancelledTakeProfits).hasSize(3)
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(
                OrderRole.ADDITION,
                OrderRole.TAKE_PROFIT,
                OrderRole.TAKE_PROFIT,
                OrderRole.TAKE_PROFIT,
                OrderRole.CLOSE,
            )
        assertThat(harness.riskService.currentState().globalTradingState)
            .isEqualTo(com.scalpsecta.breakoutbot.level.GlobalTradingState.SAFE_MODE)
        assertThat(harness.riskService.currentState().stateReason)
            .isEqualTo(LevelReasonCode.TP_SETUP_FAILED.name)
        assertThat(harness.riskService.currentState().reservations).isEmpty()
        assertThat(harness.coordinator.terminations.last().unresolved).isFalse()
    }

    @Test
    fun `confirmed take-profit fills reduce risk and all fills complete the attempt`() {
        val harness = harness(LevelDirection.LONG)
        harness.riskService
            .recordConfirmedExposure(LEVEL_ID, BigDecimal("6"))
            .block(TIMEOUT)
        harness.service.execute(
            addition(LevelDirection.LONG, BreakoutTranche.FINAL, "4", "6"),
        ).block(TIMEOUT)

        harness.executor.emitTakeProfitFill("6.7", complete = false)
        harness.coordinator.takeProfitFillSignal.asFlux().next().block(TIMEOUT)

        val partialState = harness.riskService.currentState()
        assertThat(partialState.attempts.single().confirmedPositionQuantity)
            .isEqualByComparingTo("6.7")
        assertThat(partialState.reservedRiskForOpenPositions)
            .isLessThan(BigDecimal("10"))

        harness.executor.emitTakeProfitFill("0", complete = true)
        harness.coordinator.takeProfitFillSignal.asFlux().skip(1).next().block(TIMEOUT)

        assertThat(harness.coordinator.takeProfitFills)
            .usingElementComparator(BigDecimal::compareTo)
            .containsExactly(BigDecimal("6.7"), BigDecimal.ZERO)
        assertThat(harness.riskService.currentState().reservations).isEmpty()
    }

    @Test
    fun `no addition is emitted before hard-stop and gate validation`() {
        val harness = harness(LevelDirection.LONG)
        harness.coordinator.dispatchAllowed = false

        val result = harness.service
            .execute(addition(LevelDirection.LONG, BreakoutTranche.CROSSING, "3", "3"))
            .block(TIMEOUT)!!

        assertThat(result.status).isEqualTo(BreakoutResultStatus.TERMINATED)
        assertThat(result.terminalReason)
            .isEqualTo(LevelReasonCode.PRE_ENTRY_INVALIDATED)
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(OrderRole.CLOSE)
        assertThat(harness.riskService.currentState().reservations).isEmpty()
    }

    @Test
    fun `every added tranche applies the eighty-percent rule and closes total exposure`() {
        listOf(
            BreakoutTranche.CROSSING to BigDecimal("2.399"),
            BreakoutTranche.FINAL to BigDecimal("3.199"),
        ).forEach { (tranche, fill) ->
            val harness = harness(LevelDirection.LONG)
            if (tranche == BreakoutTranche.FINAL) {
                harness.riskService
                    .recordConfirmedExposure(LEVEL_ID, BigDecimal("6"))
                    .block(TIMEOUT)
            }
            harness.executor.additionFilledQuantity = fill
            val confirmedBefore = if (tranche == BreakoutTranche.CROSSING) {
                "3"
            } else {
                "6"
            }
            val requested = if (tranche == BreakoutTranche.CROSSING) {
                "3"
            } else {
                "4"
            }

            val result = harness.service.execute(
                addition(
                    direction = LevelDirection.LONG,
                    tranche = tranche,
                    requestedQuantity = requested,
                    confirmedQuantity = confirmedBefore,
                ),
            ).block(TIMEOUT)!!

            assertThat(result.terminalReason)
                .isEqualTo(LevelReasonCode.INSUFFICIENT_LIQUIDITY)
            assertThat(harness.executor.requests.map(OrderIntentRequest::role))
                .containsExactly(OrderRole.ADDITION, OrderRole.CLOSE)
            assertThat(harness.executor.requests.last().confirmedQuantity)
                .isEqualByComparingTo(
                    BigDecimal(confirmedBefore).add(fill),
                )
            assertThat(harness.riskService.currentState().reservations).isEmpty()
        }
    }

    @Test
    fun `unknown final outcome is not retried or double-closed`() {
        val harness = harness(LevelDirection.LONG)
        harness.riskService
            .recordConfirmedExposure(LEVEL_ID, BigDecimal("6"))
            .block(TIMEOUT)
        harness.executor.additionOutcome = OrderOutcome.UNKNOWN
        harness.executor.additionFilledQuantity = BigDecimal("2")

        val result = harness.service.execute(
            addition(LevelDirection.LONG, BreakoutTranche.FINAL, "4", "6"),
        ).block(TIMEOUT)!!

        assertThat(result.terminalReason)
            .isEqualTo(LevelReasonCode.ORDER_OUTCOME_UNKNOWN)
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(OrderRole.ADDITION)
        assertThat(harness.coordinator.terminations.single().unresolved).isTrue()
    }

    @Test
    fun `strategy exit uses one capped IOC then market closes one reconciled residual`() {
        LevelDirection.entries.forEach { direction ->
            val harness = harness(direction)
            harness.executor.softExitResidual = BigDecimal("1.2")
            harness.executor.netResult = PositionNetResult(
                grossPnl = BigDecimal("-0.40"),
                fees = BigDecimal("0.10"),
                funding = BigDecimal("0.02"),
                slippage = BigDecimal("0.03"),
                netPnl = BigDecimal("-0.48"),
            )

            val result = harness.service.execute(
                BreakoutExitRequest(
                    requestId = "exit-score",
                    levelId = LEVEL_ID,
                    attemptNumber = 1,
                    symbol = "BTCUSDT",
                    direction = direction,
                    confirmedPositionQuantity = BigDecimal("3"),
                    reason = LevelReasonCode.EXIT_SCORE,
                    bestBidPrice = BigDecimal("99.9"),
                    bestAskPrice = BigDecimal("100.1"),
                    frozenNpu = BigDecimal("0.1"),
                    tickSize = BigDecimal("0.1"),
                ),
            ).block(TIMEOUT)!!

            val closes = harness.executor.requests.filter { request ->
                request.role == OrderRole.CLOSE
            }
            assertThat(closes.map(OrderIntentRequest::type))
                .containsExactly(OrderType.LIMIT, OrderType.MARKET)
            assertThat(closes.first().timeInForce).isEqualTo(OrderTimeInForce.IOC)
            assertThat(closes.first().reduceOnly).isTrue()
            assertThat(closes.first().price).isEqualByComparingTo(
                if (direction == LevelDirection.LONG) "99.8" else "100.2",
            )
            assertThat(closes.last().confirmedQuantity).isEqualByComparingTo("1.2")
            assertThat(harness.executor.reconciliationWait)
                .isEqualTo(Duration.ofMillis(500))
            assertThat(harness.executor.activeTakeProfitCancellations).isEqualTo(1)
            assertThat(harness.riskService.currentState().reservations).isEmpty()
            assertThat(harness.coordinator.terminations.single().reason)
                .isEqualTo(LevelReasonCode.EXIT_SCORE)
            assertThat(harness.coordinator.terminations.single().netResult)
                .isEqualTo(harness.executor.netResult)
            assertThat(result.confirmedPositionQuantity).isEqualByComparingTo("0")
        }
    }

    private fun harness(direction: LevelDirection): BreakoutHarness {
        val scheduler = Schedulers.newSingle("breakout-risk-test")
        val riskService = AttemptRiskService(
            clock = CLOCK,
            scheduler = scheduler,
            evidenceRecorder = NoOpEvidenceRecorder,
        )
        val admitted = riskService.admit(
            request = admissionRequest(direction),
            accountState = RiskAccountState(
                dailyAnchorEquity = BigDecimal("1000"),
                currentTotalAccountEquity = BigDecimal("1000"),
                availableMargin = BigDecimal("1000"),
            ),
        ).block(TIMEOUT)!!
        check(admitted.admitted) { "test risk request was not admitted" }
        riskService.recordConfirmedExposure(LEVEL_ID, BigDecimal("3"))
            .block(TIMEOUT)
        val coordinator = FakeBreakoutLevelCoordinator()
        val executor = FakeBreakoutOrderExecutor()
        val service = BreakoutExecutionService(
            levelCoordinator = coordinator,
            riskService = riskService,
            orderExecutor = executor,
            automaticDispatch = false,
        )
        resources += AutoCloseable {
            service.close()
            riskService.close()
        }
        return BreakoutHarness(
            service = service,
            coordinator = coordinator,
            executor = executor,
            riskService = riskService,
        )
    }

    private fun admissionRequest(
        direction: LevelDirection,
    ): AttemptAdmissionRequest {
        val short = direction == LevelDirection.SHORT
        return AttemptAdmissionRequest(
            levelId = LEVEL_ID,
            symbol = "BTCUSDT",
            direction = direction,
            levelPrice = BigDecimal("100"),
            positionNotionalUsdt = BigDecimal("1000"),
            plannedQuantity = BigDecimal("10"),
            maxImpulsePct = BigDecimal("2"),
            frozenNpu = BigDecimal("0.1"),
            precedingOneSecondTradePrices = if (short) {
                listOf(BigDecimal("100.15"), BigDecimal("100.10"))
            } else {
                listOf(BigDecimal("99.85"), BigDecimal("99.90"))
            },
            bestBidPrice = BigDecimal(if (short) "100.1" else "99.8"),
            bestAskPrice = BigDecimal(if (short) "100.2" else "99.9"),
            tickSize = BigDecimal("0.1"),
            takerFeeRate = BigDecimal("0.0004"),
            leverageBracket = RiskLeverageBracket(
                maximumLeverage = 50,
                maintenanceMarginRatio = BigDecimal("0.004"),
                cumulativeMaintenanceAmount = BigDecimal.ZERO,
            ),
        )
    }

    private fun addition(
        direction: LevelDirection,
        tranche: BreakoutTranche,
        requestedQuantity: String,
        confirmedQuantity: String,
    ): BreakoutAdditionRequest =
        BreakoutAdditionRequest(
            requestId = if (tranche == BreakoutTranche.CROSSING) {
                "cross"
            } else {
                "final"
            },
            levelId = LEVEL_ID,
            attemptNumber = 1,
            symbol = "BTCUSDT",
            direction = direction,
            confirmedPositionQuantity = BigDecimal(confirmedQuantity),
            tranche = tranche,
            requestedQuantity = BigDecimal(requestedQuantity),
            bestBidPrice = BigDecimal("99.9"),
            bestAskPrice = BigDecimal("100.1"),
            frozenNpu = BigDecimal("0.1"),
            hardStopClientOrderId = "hard-stop-1",
            hardStopPrice = BigDecimal(if (direction == LevelDirection.LONG) "99" else "101"),
            levelPrice = BigDecimal("100"),
            maxImpulsePct = BigDecimal("2"),
            tickSize = BigDecimal("0.1"),
            quantityStepSize = BigDecimal("0.001"),
            minimumQuantity = BigDecimal("0.001"),
            maximumQuantity = BigDecimal("1000"),
        )
}

private data class BreakoutHarness(
    val service: BreakoutExecutionService,
    val coordinator: FakeBreakoutLevelCoordinator,
    val executor: FakeBreakoutOrderExecutor,
    val riskService: AttemptRiskService,
)

private class FakeBreakoutLevelCoordinator : BreakoutLevelCoordinator {
    val crossingFills = mutableListOf<String>()
    val finalFills = mutableListOf<String>()
    val takeProfitFills = mutableListOf<BigDecimal>()
    val takeProfitFillSignal = Sinks.many().replay().limit<BigDecimal>(10)
    val terminations = mutableListOf<BreakoutTermination>()
    var dispatchAllowed = true

    override fun breakoutRequests(): Flux<BreakoutExecutionRequest> = Flux.never()

    override fun validateAddition(
        request: BreakoutAdditionRequest,
    ): Mono<BreakoutDispatchDecision> =
        Mono.just(
            BreakoutDispatchDecision(
                request = request,
                dispatchAllowed = dispatchAllowed,
                terminalReason = if (dispatchAllowed) {
                    null
                } else {
                    LevelReasonCode.PRE_ENTRY_INVALIDATED
                },
            ),
        )

    override fun recordCrossingFill(
        requestId: String,
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
    ): Mono<BreakoutContinuationDecision> =
        Mono.fromCallable {
            crossingFills += requestId
            BreakoutContinuationDecision(continueAttempt = true)
        }

    override fun recordFinalFill(
        requestId: String,
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
    ): Mono<Void> =
        Mono.fromRunnable<Void> { finalFills += requestId }.then()

    override fun recordPositionReduction(
        levelId: UUID,
        confirmedRemainingQuantity: BigDecimal,
        terminalReason: LevelReasonCode?,
        netResult: PositionNetResult?,
    ): Mono<Void> =
        Mono.fromRunnable<Void> {
            takeProfitFills += confirmedRemainingQuantity
            takeProfitFillSignal.tryEmitNext(confirmedRemainingQuantity)
        }.then()

    override fun terminatePosition(
        levelId: UUID,
        reason: LevelReasonCode,
        confirmedRemainingQuantity: BigDecimal,
        hasUnresolvedOrder: Boolean,
        netResult: PositionNetResult?,
    ): Mono<Void> =
        Mono.fromRunnable<Void> {
            terminations += BreakoutTermination(
                reason = reason,
                remainingQuantity = confirmedRemainingQuantity,
                unresolved = hasUnresolvedOrder,
                netResult = netResult,
            )
        }.then()
}

private data class BreakoutTermination(
    val reason: LevelReasonCode,
    val remainingQuantity: BigDecimal,
    val unresolved: Boolean,
    val netResult: PositionNetResult?,
)

private class FakeBreakoutOrderExecutor : BreakoutOrderExecutor {
    val requests = mutableListOf<OrderIntentRequest>()
    val cancelledTakeProfits = mutableListOf<OrderIntent>()
    private val factory = ClientOrderIdFactory(STARTED_AT)
    private val takeProfitFillSink = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer<PositionReduction>()
    var additionFilledQuantity: BigDecimal? = null
    var additionOutcome: OrderOutcome? = null
    var takeProfitConfirmed: Boolean = true
    var softExitResidual: BigDecimal = BigDecimal.ZERO
    var reconciliationWait: Duration? = null
    var activeTakeProfitCancellations: Int = 0
    var netResult: PositionNetResult? = null
    private var reconciledPositionAmount: BigDecimal? = null

    override fun execute(request: OrderIntentRequest): Mono<OrderResolution> =
        Mono.fromCallable {
            requests += request
            val intent = factory.create(request)
            when (request.role) {
                OrderRole.ADDITION -> {
                    val requested = checkNotNull(request.confirmedQuantity)
                    val filled = additionFilledQuantity ?: requested
                    val signedFill = if (request.side == OrderSide.BUY) {
                        filled
                    } else {
                        filled.negate()
                    }
                    OrderResolution(
                        intent = intent,
                        outcome = additionOutcome ?: if (filled >= requested) {
                            OrderOutcome.FILLED
                        } else {
                            OrderOutcome.PARTIALLY_FILLED
                        },
                        source = OrderResolutionSource.PRIVATE_STREAM,
                        exchangeOrderId = 101L,
                        actualFilledQuantity = filled,
                        averageFilledPrice = request.price,
                        confirmedPositionAmount =
                            checkNotNull(request.confirmedPositionAmount)
                                .add(signedFill),
                        reconciliationChecks = 0,
                    ).also { resolution ->
                        reconciledPositionAmount =
                            resolution.confirmedPositionAmount
                    }
                }

                OrderRole.CLOSE -> {
                    val originalPosition =
                        checkNotNull(request.confirmedPositionAmount)
                    val remainingPosition = if (request.type == OrderType.LIMIT) {
                        if (originalPosition.signum() > 0) {
                            softExitResidual
                        } else {
                            softExitResidual.negate()
                        }
                    } else {
                        BigDecimal.ZERO
                    }
                    reconciledPositionAmount = remainingPosition
                    OrderResolution(
                        intent = intent,
                        outcome = if (remainingPosition.signum() == 0) {
                            OrderOutcome.FILLED
                        } else {
                            OrderOutcome.PARTIALLY_FILLED
                        },
                        source = OrderResolutionSource.PRIVATE_STREAM,
                        exchangeOrderId = 102L,
                        actualFilledQuantity = originalPosition.abs()
                            .subtract(remainingPosition.abs()),
                        averageFilledPrice = request.price ?: BigDecimal("99.8"),
                        confirmedPositionAmount = remainingPosition,
                        reconciliationChecks = 0,
                    )
                }

                else -> error("Unexpected order role ${request.role}")
            }
        }

    override fun reconcilePosition(
        symbol: String,
        clientOrderId: String,
    ): Mono<BigDecimal> = Mono.just(checkNotNull(reconciledPositionAmount))

    override fun reconcilePositionAfter(
        symbol: String,
        clientOrderId: String,
        wait: Duration,
    ): Mono<BigDecimal> {
        reconciliationWait = wait
        return reconcilePosition(symbol, clientOrderId)
    }

    override fun cancelActiveTakeProfits(levelId: UUID): Mono<Boolean> =
        Mono.fromCallable {
            activeTakeProfitCancellations += 1
            true
        }

    override fun positionResult(levelId: UUID): PositionNetResult? = netResult

    override fun confirmTakeProfits(
        requests: List<OrderIntentRequest>,
        timeout: Duration,
    ): Mono<TakeProfitSetConfirmation> = Mono.fromCallable {
        this.requests += requests
        val intents = requests.map(factory::create)
        TakeProfitSetConfirmation(
            intents = intents,
            confirmed = takeProfitConfirmed,
            confirmedPositionAmount = checkNotNull(
                requests.first().confirmedPositionAmount,
            ),
            reconciliationChecks = 1,
        )
    }

    override fun cancelTakeProfits(
        intents: List<OrderIntent>,
    ): Mono<Boolean> = Mono.fromCallable {
        cancelledTakeProfits += intents
        true
    }

    override fun activateTakeProfits(
        confirmation: TakeProfitSetConfirmation,
    ): Mono<Void> = Mono.empty()

    override fun positionReductions(): Flux<PositionReduction> =
        takeProfitFillSink.asFlux()

    fun emitTakeProfitFill(
        remainingQuantity: String,
        complete: Boolean,
    ) {
        takeProfitFillSink.tryEmitNext(
            PositionReduction(
                levelId = LEVEL_ID,
                symbol = "BTCUSDT",
                clientOrderId = "take-profit-fill",
                role = OrderRole.TAKE_PROFIT,
                confirmedRemainingQuantity = BigDecimal(remainingQuantity),
                terminalReason = if (complete) {
                    LevelReasonCode.TAKE_PROFITS_COMPLETE
                } else {
                    null
                },
            ),
        )
    }
}

private val STARTED_AT: Instant = Instant.parse("2026-08-01T07:00:00Z")
private val CLOCK: Clock = Clock.fixed(STARTED_AT.plusSeconds(30), ZoneOffset.UTC)
private val LEVEL_ID: UUID =
    UUID.fromString("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
private val TIMEOUT: Duration = Duration.ofSeconds(2)
