package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import com.scalpsecta.breakoutbot.risk.AttemptAdmissionRequest
import com.scalpsecta.breakoutbot.risk.AttemptRiskService
import com.scalpsecta.breakoutbot.risk.RiskAccountState
import com.scalpsecta.breakoutbot.risk.RiskLeverageBracket
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
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
            assertThat(harness.riskService.currentState().attempts.single()
                .confirmedPositionQuantity).isEqualByComparingTo("10")
        }
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

    override fun terminate(
        levelId: UUID,
        reason: LevelReasonCode,
        confirmedRemainingQuantity: BigDecimal,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void> =
        Mono.fromRunnable<Void> {
            terminations += BreakoutTermination(
                reason = reason,
                remainingQuantity = confirmedRemainingQuantity,
                unresolved = hasUnresolvedOrder,
            )
        }.then()
}

private data class BreakoutTermination(
    val reason: LevelReasonCode,
    val remainingQuantity: BigDecimal,
    val unresolved: Boolean,
)

private class FakeBreakoutOrderExecutor : PreEntryOrderExecutor {
    val requests = mutableListOf<OrderIntentRequest>()
    private val factory = ClientOrderIdFactory(STARTED_AT)
    var additionFilledQuantity: BigDecimal? = null
    var additionOutcome: OrderOutcome? = null

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
                    )
                }

                OrderRole.CLOSE -> OrderResolution(
                    intent = intent,
                    outcome = OrderOutcome.FILLED,
                    source = OrderResolutionSource.PRIVATE_STREAM,
                    exchangeOrderId = 102L,
                    actualFilledQuantity =
                        checkNotNull(request.confirmedQuantity),
                    averageFilledPrice = BigDecimal("99.8"),
                    confirmedPositionAmount = BigDecimal.ZERO,
                    reconciliationChecks = 0,
                )

                else -> error("Unexpected order role ${request.role}")
            }
        }

    override fun confirmHardStop(
        request: OrderIntentRequest,
    ): Mono<HardStopConfirmation> =
        Mono.error(UnsupportedOperationException("Not used by breakout tests"))
}

private val STARTED_AT: Instant = Instant.parse("2026-08-01T07:00:00Z")
private val CLOCK: Clock = Clock.fixed(STARTED_AT.plusSeconds(30), ZoneOffset.UTC)
private val LEVEL_ID: UUID =
    UUID.fromString("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
private val TIMEOUT: Duration = Duration.ofSeconds(2)
