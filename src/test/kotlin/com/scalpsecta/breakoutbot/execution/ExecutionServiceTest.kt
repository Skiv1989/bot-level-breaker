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

    private fun orderUpdate(
        request: BinanceOrderRequest,
        status: String,
        filledQuantity: String,
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
            averagePrice = BigDecimal("100.50"),
            stopPrice = request.stopPrice ?: BigDecimal.ZERO,
            executionType = if (status == "REJECTED") "NEW" else "TRADE",
            orderStatus = status,
            orderId = 1001L,
            lastFilledQuantity = BigDecimal(filledQuantity),
            accumulatedFilledQuantity = BigDecimal(filledQuantity),
            lastFilledPrice = BigDecimal("100.50"),
            commissionAsset = "USDT",
            commission = BigDecimal("0.01"),
            tradeId = 2001L,
            realizedProfit = BigDecimal.ZERO,
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
