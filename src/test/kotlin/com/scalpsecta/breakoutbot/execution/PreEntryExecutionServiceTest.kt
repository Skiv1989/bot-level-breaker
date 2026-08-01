package com.scalpsecta.breakoutbot.execution

import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.level.GlobalTradingState
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.level.LevelReasonCode
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

class PreEntryExecutionServiceTest {
    private val resources = mutableListOf<AutoCloseable>()

    @AfterEach
    fun closeResources() {
        resources.reversed().forEach(AutoCloseable::close)
    }

    @Test
    fun `dispatch consumes the attempt only after atomic risk admission and confirms protection`() {
        val harness = harness()
        harness.executor.entryFilledQuantity = BigDecimal("2.40")
        harness.executor.entryPositionAmount = BigDecimal("2.40")

        val result = harness.service.execute(LEVEL_ID).block(TIMEOUT)!!

        assertThat(result.status).isEqualTo(PreEntryResultStatus.PROTECTED)
        assertThat(result.actualFilledQuantity).isEqualByComparingTo("2.40")
        assertThat(harness.coordinator.events).containsExactly(
            "PREPARED",
            "DISPATCHED",
            "PROTECTED",
        )
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(OrderRole.ENTRY, OrderRole.HARD_STOP)
        val entry = harness.executor.requests.first()
        assertThat(entry.side).isEqualTo(OrderSide.BUY)
        assertThat(entry.type).isEqualTo(OrderType.LIMIT)
        assertThat(entry.timeInForce).isEqualTo(OrderTimeInForce.IOC)
        assertThat(entry.confirmedQuantity).isEqualByComparingTo("3.0")
        assertThat(entry.price).isEqualByComparingTo("100.0")
        val stop = harness.executor.requests.last()
        assertThat(stop.type).isEqualTo(OrderType.STOP_MARKET)
        assertThat(stop.closePosition).isTrue()
        assertThat(stop.confirmedQuantity).isNull()
        assertThat(stop.stopPrice).isEqualByComparingTo("99.7")
        assertThat(stop.workingType)
            .isEqualTo(TriggerWorkingType.CONTRACT_PRICE)
        assertThat(stop.priceProtect).isFalse()
        assertThat(harness.riskService.currentState().reservations).hasSize(1)
        assertThat(harness.riskService.currentState().reservations.single().status.name)
            .isEqualTo("OPEN_POSITION")
    }

    @Test
    fun `blocked risk leaves approach unconsumed and emits no order`() {
        val harness = harness(
            context = riskContext(availableMargin = "10"),
        )

        val result = harness.service.execute(LEVEL_ID).block(TIMEOUT)!!

        assertThat(result.status).isEqualTo(PreEntryResultStatus.RISK_BLOCKED)
        assertThat(harness.coordinator.events)
            .containsExactly("PREPARED", "PREPARATION_CANCELED")
        assertThat(harness.executor.requests).isEmpty()
        assertThat(harness.riskService.currentState().reservations).isEmpty()
    }

    @Test
    fun `fill below eighty percent is not retried and closes actual exposure`() {
        val harness = harness()
        harness.executor.entryFilledQuantity = BigDecimal("2.39")
        harness.executor.entryPositionAmount = BigDecimal("2.39")
        harness.executor.closeRemainingPositionAmount = BigDecimal.ZERO

        val result = harness.service.execute(LEVEL_ID).block(TIMEOUT)!!

        assertThat(result.status).isEqualTo(PreEntryResultStatus.TERMINATED)
        assertThat(result.terminalReason)
            .isEqualTo(LevelReasonCode.INSUFFICIENT_LIQUIDITY)
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(OrderRole.ENTRY, OrderRole.CLOSE)
        val close = harness.executor.requests.last()
        assertThat(close.confirmedQuantity).isEqualByComparingTo("2.39")
        assertThat(close.reduceOnly).isTrue()
        assertThat(harness.coordinator.terminalUpdates.last().remainingQuantity)
            .isEqualByComparingTo(BigDecimal.ZERO)
        assertThat(harness.riskService.currentState().reservations).isEmpty()
    }

    @Test
    fun `stop confirmation failure enters safe mode closes exposure and releases after flat`() {
        val harness = harness()
        harness.executor.hardStopConfirmed = false
        harness.executor.closeRemainingPositionAmount = BigDecimal.ZERO

        val result = harness.service.execute(LEVEL_ID).block(TIMEOUT)!!

        assertThat(result.terminalReason)
            .isEqualTo(LevelReasonCode.STOP_SETUP_FAILED)
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(
                OrderRole.ENTRY,
                OrderRole.HARD_STOP,
                OrderRole.CLOSE,
            )
        assertThat(harness.riskService.currentState().globalTradingState)
            .isEqualTo(GlobalTradingState.SAFE_MODE)
        assertThat(harness.riskService.currentState().stateReason)
            .isEqualTo(LevelReasonCode.STOP_SETUP_FAILED.name)
        assertThat(harness.riskService.currentState().reservations).isEmpty()
        assertThat(harness.coordinator.terminalUpdates.last().unresolved)
            .isTrue()
    }

    @Test
    fun `crossing before protection suppresses later tranches and closes actual exposure`() {
        val harness = harness()
        harness.coordinator.crossed = true

        val result = harness.service.execute(LEVEL_ID).block(TIMEOUT)!!

        assertThat(result.terminalReason)
            .isEqualTo(LevelReasonCode.CROSS_BEFORE_PROTECTED)
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(OrderRole.ENTRY, OrderRole.CLOSE)
        assertThat(harness.executor.requests)
            .noneMatch { request -> request.role == OrderRole.ADDITION }
        assertThat(harness.riskService.currentState().reservations).isEmpty()
        assertThat(harness.riskService.currentState().globalTradingState)
            .isEqualTo(GlobalTradingState.RUNNING)
    }

    @Test
    fun `crossing serialized between stop confirmation and protected state still closes`() {
        val harness = harness()
        harness.coordinator.failProtectionAsCross = true

        val result = harness.service.execute(LEVEL_ID).block(TIMEOUT)!!

        assertThat(result.terminalReason)
            .isEqualTo(LevelReasonCode.CROSS_BEFORE_PROTECTED)
        assertThat(harness.executor.requests.map(OrderIntentRequest::role))
            .containsExactly(
                OrderRole.ENTRY,
                OrderRole.HARD_STOP,
                OrderRole.CLOSE,
            )
        assertThat(harness.coordinator.terminalUpdates.last().unresolved)
            .isTrue()
    }

    private fun harness(
        context: PreEntryRiskContext = riskContext(),
    ): PreEntryHarness {
        val riskScheduler = Schedulers.newSingle("pre-entry-test-risk")
        val riskService = AttemptRiskService(
            clock = CLOCK,
            scheduler = riskScheduler,
            evidenceRecorder = NoOpEvidenceRecorder,
        )
        val coordinator = FakePreEntryLevelCoordinator(opportunity())
        val executor = FakePreEntryOrderExecutor()
        val service = PreEntryExecutionService(
            levelCoordinator = coordinator,
            riskContextProvider = PreEntryRiskContextProvider {
                Mono.just(context)
            },
            riskService = riskService,
            orderExecutor = executor,
            automaticDispatch = false,
        )
        resources += AutoCloseable {
            service.close()
            riskService.close()
        }
        return PreEntryHarness(
            service = service,
            coordinator = coordinator,
            executor = executor,
            riskService = riskService,
        )
    }

    private fun opportunity(): PreEntryOpportunity =
        PreEntryOpportunity(
            levelId = LEVEL_ID,
            attemptNumber = 1,
            symbol = "BTCUSDT",
            direction = LevelDirection.LONG,
            levelPrice = BigDecimal("100"),
            positionNotionalUsdt = BigDecimal("1000"),
            plannedQuantity = BigDecimal("10"),
            preEntryQuantity = BigDecimal("3.0"),
            maxImpulsePct = BigDecimal("2"),
            frozenNpu = BigDecimal("0.1"),
            precedingOneSecondTradePrices = listOf(
                BigDecimal("99.85"),
                BigDecimal("99.90"),
            ),
            bestBidPrice = BigDecimal("99.8"),
            bestAskPrice = BigDecimal("99.9"),
            tickSize = BigDecimal("0.1"),
            preparedAt = NOW,
        )

    private fun riskContext(
        availableMargin: String = "1000",
    ): PreEntryRiskContext =
        PreEntryRiskContext(
            accountState = RiskAccountState(
                dailyAnchorEquity = BigDecimal("1000"),
                currentTotalAccountEquity = BigDecimal("1000"),
                availableMargin = BigDecimal(availableMargin),
            ),
            takerFeeRate = BigDecimal("0.0004"),
            leverageBracket = RiskLeverageBracket(
                maximumLeverage = 50,
                maintenanceMarginRatio = BigDecimal("0.004"),
                cumulativeMaintenanceAmount = BigDecimal.ZERO,
            ),
        )
}

private data class PreEntryHarness(
    val service: PreEntryExecutionService,
    val coordinator: FakePreEntryLevelCoordinator,
    val executor: FakePreEntryOrderExecutor,
    val riskService: AttemptRiskService,
)

private class FakePreEntryLevelCoordinator(
    private val opportunity: PreEntryOpportunity,
) : PreEntryLevelCoordinator {
    val events = mutableListOf<String>()
    val terminalUpdates = mutableListOf<TerminalUpdate>()
    var crossed: Boolean = false
    var failProtectionAsCross: Boolean = false

    override fun opportunities(): Flux<UUID> = Flux.never()

    override fun prepare(levelId: UUID): Mono<PreEntryOpportunity> =
        Mono.fromCallable {
            events += "PREPARED"
            opportunity
        }

    override fun markDispatched(levelId: UUID): Mono<Void> =
        Mono.fromRunnable<Void> { events += "DISPATCHED" }.then()

    override fun cancelPreparation(levelId: UUID): Mono<Void> =
        Mono.fromRunnable<Void> {
            events += "PREPARATION_CANCELED"
        }.then()

    override fun crossedBeforeProtection(levelId: UUID): Boolean = crossed

    override fun markProtected(
        levelId: UUID,
        confirmedPositionQuantity: BigDecimal,
        hardStopClientOrderId: String,
        hardStopPrice: BigDecimal,
        preEntryFilledAt: Instant?,
    ): Mono<Void> =
        Mono.defer {
            if (failProtectionAsCross) {
                crossed = true
                Mono.error(IllegalStateException("crossed before protection"))
            } else {
                Mono.fromRunnable<Void> { events += "PROTECTED" }.then()
            }
        }

    override fun terminate(
        levelId: UUID,
        reason: LevelReasonCode,
        confirmedRemainingQuantity: BigDecimal,
        hasUnresolvedOrder: Boolean,
    ): Mono<Void> =
        Mono.fromRunnable<Void> {
            events += "TERMINATED:$reason"
            terminalUpdates += TerminalUpdate(
                reason = reason,
                remainingQuantity = confirmedRemainingQuantity,
                unresolved = hasUnresolvedOrder,
            )
        }.then()
}

private data class TerminalUpdate(
    val reason: LevelReasonCode,
    val remainingQuantity: BigDecimal,
    val unresolved: Boolean,
)

private class FakePreEntryOrderExecutor : PreEntryOrderExecutor {
    val requests = mutableListOf<OrderIntentRequest>()
    private val factory = ClientOrderIdFactory(STARTED_AT)
    var entryFilledQuantity: BigDecimal = BigDecimal("3.0")
    var entryPositionAmount: BigDecimal = BigDecimal("3.0")
    var closeRemainingPositionAmount: BigDecimal = BigDecimal.ZERO
    var hardStopConfirmed: Boolean = true

    override fun execute(request: OrderIntentRequest): Mono<OrderResolution> =
        Mono.fromCallable {
            requests += request
            val intent = factory.create(request)
            when (request.role) {
                OrderRole.ENTRY -> OrderResolution(
                    intent = intent,
                    outcome = if (
                        entryFilledQuantity >= checkNotNull(request.confirmedQuantity)
                    ) {
                        OrderOutcome.FILLED
                    } else {
                        OrderOutcome.PARTIALLY_FILLED
                    },
                    source = OrderResolutionSource.PRIVATE_STREAM,
                    exchangeOrderId = 101L,
                    actualFilledQuantity = entryFilledQuantity,
                    averageFilledPrice = request.price,
                    confirmedPositionAmount = entryPositionAmount,
                    reconciliationChecks = 0,
                )

                OrderRole.CLOSE -> OrderResolution(
                    intent = intent,
                    outcome = OrderOutcome.FILLED,
                    source = OrderResolutionSource.PRIVATE_STREAM,
                    exchangeOrderId = 103L,
                    actualFilledQuantity =
                        checkNotNull(request.confirmedQuantity)
                            .subtract(closeRemainingPositionAmount.abs()),
                    averageFilledPrice = BigDecimal("99.8"),
                    confirmedPositionAmount = closeRemainingPositionAmount,
                    reconciliationChecks = 0,
                )

                else -> error("Unexpected executable role ${request.role}")
            }
        }

    override fun confirmHardStop(
        request: OrderIntentRequest,
    ): Mono<HardStopConfirmation> =
        Mono.fromCallable {
            requests += request
            val intent = factory.create(request)
            HardStopConfirmation(
                intent = intent,
                confirmed = hardStopConfirmed,
                exchangeOrderId = 102L,
                observedStopPrice = request.stopPrice,
                observedWorkingType = request.workingType,
                observedPriceProtect = request.priceProtect,
                reconciliationChecks = 1,
                confirmedPositionAmount =
                    checkNotNull(request.confirmedPositionAmount),
            )
        }
}

private val STARTED_AT: Instant = Instant.parse("2026-08-01T07:00:00Z")
private val NOW: Instant = STARTED_AT.plusSeconds(30)
private val CLOCK: Clock = Clock.fixed(NOW, ZoneOffset.UTC)
private val LEVEL_ID: UUID =
    UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
private val TIMEOUT: Duration = Duration.ofSeconds(2)
