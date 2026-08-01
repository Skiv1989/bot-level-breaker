package com.scalpsecta.breakoutbot.level

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceAccountSummary
import com.scalpsecta.breakoutbot.binance.BinanceAssetMode
import com.scalpsecta.breakoutbot.binance.BinanceClockMeasurement
import com.scalpsecta.breakoutbot.binance.BinanceCommissionRate
import com.scalpsecta.breakoutbot.binance.BinanceExchangeInfo
import com.scalpsecta.breakoutbot.binance.BinanceLeverageBracket
import com.scalpsecta.breakoutbot.binance.BinanceLotSizeFilter
import com.scalpsecta.breakoutbot.binance.BinanceMarginType
import com.scalpsecta.breakoutbot.binance.BinancePositionMode
import com.scalpsecta.breakoutbot.binance.BinancePriceFilter
import com.scalpsecta.breakoutbot.binance.BinanceSymbolConfiguration
import com.scalpsecta.breakoutbot.binance.BinanceSymbolLeverageBrackets
import com.scalpsecta.breakoutbot.binance.BinanceSymbolMetadata
import com.scalpsecta.breakoutbot.evidence.EvidenceRecorder
import com.scalpsecta.breakoutbot.evidence.NoOpEvidenceRecorder
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.AggressorSide
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataStreamProvider
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicReference

class LevelServiceTest {
    private val now = Instant.parse("2026-07-31T12:00:00Z")
    private val clock = LevelMutableClock(now)
    private val client = LevelTestBinanceClient(now)
    private val marketDataService = PublicMarketDataService(
        streamProvider = EmptyMarketDataStreamProvider,
        clock = clock,
    )
    private val evidenceRecorder = RecordingEvidenceRecorder()
    private val service = LevelService(
        client = client,
        publicMarketDataService = marketDataService,
        clock = clock,
        automaticTimers = false,
        evidenceRecorder = evidenceRecorder,
    )

    @AfterEach
    fun closeServices() {
        service.close()
        marketDataService.close()
    }

    @Test
    fun `creates an uppercase normalized executable warming-up level`() {
        val created = service.create(
            command(
                symbol = " btcusdt ",
                levelPrice = "101.26",
            ),
        ).block()!!

        assertThat(created.symbol).isEqualTo("BTCUSDT")
        assertThat(created.requestedLevelPrice)
            .isEqualByComparingTo(BigDecimal("101.26"))
        assertThat(created.normalizedLevelPrice)
            .isEqualByComparingTo(BigDecimal("101.3"))
        assertThat(created.sizingReferencePrice)
            .isEqualByComparingTo(BigDecimal("100"))
        assertThat(created.plannedQuantity)
            .isEqualByComparingTo(BigDecimal("10"))
        assertThat(created.entryAllocation.map(LevelEntryTranche::allocationPercent))
            .containsExactly(30, 30, 40)
        assertThat(created.entryAllocation.map(LevelEntryTranche::quantity))
            .usingElementComparator(BigDecimal::compareTo)
            .containsExactly(
                BigDecimal("3"),
                BigDecimal("3"),
                BigDecimal("4"),
            )
        assertThat(created.leverage).isEqualTo(20)
        assertThat(created.projectedIsolatedMargin)
            .isEqualByComparingTo(BigDecimal("50"))
        assertThat(created.estimatedLiquidationPrice)
            .isLessThan(created.riskBoundaryStopPrice)
        assertThat(created.state).isEqualTo(LevelState.WARMING_UP)
        assertThat(created.blockers).containsExactly(LevelBlocker.WARMING_UP)
        assertThat(created.signal.windows.fast.tradeCount).isZero()
        assertThat(created.signal.pressureScore.diagnosticOnly).isTrue()
        assertThat(created.signal.mandatoryGates.entryEligible).isFalse()
        assertThat(created.deleteAllowed).isTrue()
        assertThat(service.currentState()).containsExactly(created)
        assertThat(marketDataService.activeSymbolCount()).isOne()
        assertThat(client.configurationOperations).containsExactly(
            "READ:CROSSED:true:5",
            "MARGIN:ISOLATED",
            "LEVERAGE:20",
            "READ:ISOLATED:false:20",
        )
    }

    @Test
    fun `rejects crossed and equal-side levels without storing them`() {
        val equal = levelFailure {
            service.create(command(levelPrice = "100.04")).block()
        }
        val crossedShort = levelFailure {
            service.create(
                command(
                    direction = LevelDirection.SHORT,
                    levelPrice = "101",
                ),
            ).block()
        }

        assertThat(equal.code).isEqualTo(LevelReasonCode.LEVEL_ALREADY_CROSSED)
        assertThat(crossedShort.code)
            .isEqualTo(LevelReasonCode.LEVEL_ALREADY_CROSSED)
        assertThat(service.currentState()).isEmpty()
        assertThat(client.configurationOperations).isEmpty()
    }

    @Test
    fun `rejects exact normalized duplicates`() {
        service.create(command(levelPrice = "101.26")).block()

        val duplicate = levelFailure {
            service.create(command(levelPrice = "101.25")).block()
        }

        assertThat(duplicate.code).isEqualTo(LevelReasonCode.DUPLICATE_LEVEL)
        assertThat(service.currentState()).hasSize(1)
    }

    @Test
    fun `serializes symbol configuration while allowing multiple same-symbol levels`() {
        val created = Mono.zip(
            service.create(command(levelPrice = "101.2")),
            service.create(command(levelPrice = "102.2")),
        ).block()!!

        assertThat(created.t1.normalizedLevelPrice)
            .isEqualByComparingTo(BigDecimal("101.2"))
        assertThat(created.t2.normalizedLevelPrice)
            .isEqualByComparingTo(BigDecimal("102.2"))
        assertThat(service.currentState()).hasSize(2)
        assertThat(marketDataService.activeSymbolCount()).isOne()
    }

    @Test
    fun `rejects unsupported symbols and non-positive values with stable codes`() {
        val unsupported = levelFailure {
            service.create(command(symbol = "ETHUSDT")).block()
        }
        val nonPositive = levelFailure {
            service.create(command(positionNotionalUsdt = "0")).block()
        }

        assertThat(unsupported.code).isEqualTo(LevelReasonCode.INVALID_SYMBOL)
        assertThat(nonPositive.code).isEqualTo(LevelReasonCode.INVALID_LEVEL)
        assertThat(service.currentState()).isEmpty()
    }

    @Test
    fun `rejects a plan whose entry tranches violate minimum notional`() {
        client.minimumNotional = BigDecimal("400")

        val failure = levelFailure {
            service.create(command()).block()
        }

        assertThat(failure.code).isEqualTo(LevelReasonCode.INVALID_LEVEL)
        assertThat(service.currentState()).isEmpty()
    }

    @Test
    fun `rejects the one hundred and first stored level`() {
        repeat(100) { index ->
            service.create(
                command(levelPrice = (101 + index).toString()),
            ).block()
        }

        val failure = levelFailure {
            service.create(command(levelPrice = "201")).block()
        }

        assertThat(failure.code)
            .isEqualTo(LevelReasonCode.LEVEL_CAPACITY_REACHED)
        assertThat(service.currentState()).hasSize(100)
        assertThat(marketDataService.activeSymbolCount()).isOne()
    }

    @Test
    fun `rejects a liquidation estimate on the stop side`() {
        client.maintenanceMarginRatio = BigDecimal("0.049")

        val failure = levelFailure {
            service.create(command()).block()
        }

        assertThat(failure.code)
            .isEqualTo(LevelReasonCode.LIQUIDATION_TOO_CLOSE)
        assertThat(client.configurationOperations).isEmpty()
        assertThat(service.currentState()).isEmpty()
    }

    @Test
    fun `uses a bracket maximum below the twenty-times ceiling`() {
        client.bracketLeverage = 10

        val created = service.create(command()).block()!!

        assertThat(created.leverage).isEqualTo(10)
        assertThat(created.projectedIsolatedMargin)
            .isEqualByComparingTo(BigDecimal("100"))
        assertThat(client.configurationOperations).contains("LEVERAGE:10")
    }

    @Test
    fun `rejects isolated configuration while auto-add margin is enabled`() {
        client.presetConfiguration(
            marginType = BinanceMarginType.ISOLATED,
            autoAddMargin = true,
            leverage = 20,
        )

        val failure = levelFailure {
            service.create(command()).block()
        }

        assertThat(failure.code)
            .isEqualTo(LevelReasonCode.SYMBOL_CONFIGURATION_FAILED)
        assertThat(service.currentState()).isEmpty()
    }

    @Test
    fun `deletes only levels without exposure or unresolved orders`() {
        val created = service.create(command()).block()!!

        service.recordOwnership(
            levelId = created.id,
            ownsExposure = true,
            hasUnresolvedOrder = false,
        ).block()
        val exposureFailure = levelFailure { service.delete(created.id).block() }
        assertThat(exposureFailure.code)
            .isEqualTo(LevelReasonCode.LEVEL_HAS_EXPOSURE)

        service.recordOwnership(
            levelId = created.id,
            ownsExposure = false,
            hasUnresolvedOrder = true,
        ).block()
        val orderFailure = levelFailure { service.delete(created.id).block() }
        assertThat(orderFailure.code)
            .isEqualTo(LevelReasonCode.LEVEL_HAS_UNRESOLVED_ORDER)

        service.recordOwnership(
            levelId = created.id,
            ownsExposure = false,
            hasUnresolvedOrder = false,
        ).block()
        assertThat(service.delete(created.id).block()!!.id).isEqualTo(created.id)
        assertThat(service.currentState()).isEmpty()
        assertThat(marketDataService.activeSymbolCount()).isZero()
    }

    @Test
    fun `signal NPU is computed when armed and freezes on approach`() {
        val created = service.create(command()).block()!!

        service.process(bookTicker("90"), marketHealthy = true).block()
        clock.advance(Duration.ofSeconds(10))
        service.process(bookTicker("90"), marketHealthy = true).block()

        val armed = service.currentState().single()
        assertThat(armed.state).isEqualTo(LevelState.ARMED)
        assertThat(armed.signal.npu.absolute)
            .isEqualByComparingTo(BigDecimal("0.1"))
        assertThat(armed.signal.npu.frozen).isFalse()

        service.process(bookTicker("100.5"), marketHealthy = true).block()

        val approach = service.currentState().single()
        assertThat(approach.id).isEqualTo(created.id)
        assertThat(approach.state).isEqualTo(LevelState.APPROACH)
        assertThat(approach.signal.npu.absolute)
            .isEqualByComparingTo(BigDecimal("0.1"))
        assertThat(approach.signal.npu.frozen).isTrue()
        assertThat(evidenceRecorder.transitions)
            .containsExactly(
                RecordedTransition(
                    before = LevelState.WARMING_UP,
                    after = LevelState.ARMED,
                    decision = "WARMUP_COMPLETE",
                ),
                RecordedTransition(
                    before = LevelState.ARMED,
                    after = LevelState.APPROACH,
                    decision = "ACTIVATION_BAND_ENTERED",
                ),
            )
    }

    @Test
    fun `warmup needs ten continuous healthy seconds`() {
        service.create(command()).block()
        service.process(bookTicker("90"), marketHealthy = true).block()

        clock.advance(Duration.ofSeconds(9))
        service.processTimer("BTCUSDT", marketHealthy = true).block()
        assertThat(service.currentState().single().state)
            .isEqualTo(LevelState.WARMING_UP)

        service.processTimer("BTCUSDT", marketHealthy = false).block()
        service.processTimer("BTCUSDT", marketHealthy = true).block()
        clock.advance(Duration.ofSeconds(9).plusMillis(999))
        service.processTimer("BTCUSDT", marketHealthy = true).block()
        assertThat(service.currentState().single().state)
            .isEqualTo(LevelState.WARMING_UP)

        clock.advance(Duration.ofMillis(1))
        service.processTimer("BTCUSDT", marketHealthy = true).block()

        val armed = service.currentState().single()
        assertThat(armed.state).isEqualTo(LevelState.ARMED)
        assertThat(armed.warmupHealthySince)
            .isEqualTo(clock.instant().minusSeconds(10))
        assertThat(armed.blockers).doesNotContain(LevelBlocker.WARMING_UP)
    }

    @Test
    fun `bid ask cannot cross but aggregate trades mirror warmup crossing`() {
        val long = service.create(command(levelPrice = "101.2")).block()!!
        val short = service.create(
            command(
                direction = LevelDirection.SHORT,
                levelPrice = "98.8",
            ),
        ).block()!!

        service.process(bookTicker("102"), marketHealthy = true).block()
        assertThat(service.currentState().map(LevelSnapshot::state))
            .containsOnly(LevelState.WARMING_UP)

        service.process(trade("101.2"), marketHealthy = true).block()
        service.process(trade("98.8"), marketHealthy = true).block()

        val levels = service.currentState().associateBy(LevelSnapshot::id)
        assertThat(levels.getValue(long.id).state).isEqualTo(LevelState.TERMINAL)
        assertThat(levels.getValue(short.id).state).isEqualTo(LevelState.TERMINAL)
        assertThat(levels.getValue(long.id).terminalReason)
            .isEqualTo(LevelReasonCode.MISSED_DURING_WARMUP)
        assertThat(levels.getValue(short.id).terminalReason)
            .isEqualTo(LevelReasonCode.MISSED_DURING_WARMUP)
        assertThat(levels.values.flatMap(LevelSnapshot::blockers))
            .containsOnly(LevelBlocker.TERMINAL)
    }

    @Test
    fun `activation band is mirrored and freezes NPU for both directions`() {
        val long = service.create(command(levelPrice = "101.2")).block()!!
        val short = service.create(
            command(
                direction = LevelDirection.SHORT,
                levelPrice = "98.8",
            ),
        ).block()!!
        service.process(bookTicker("100"), marketHealthy = true).block()
        clock.advance(Duration.ofSeconds(10))
        service.processTimer("BTCUSDT", marketHealthy = true).block()
        assertThat(service.currentState().map(LevelSnapshot::state))
            .containsOnly(LevelState.ARMED)

        service.process(bookTicker("100.5"), marketHealthy = true).block()
        service.process(bookTicker("99.5"), marketHealthy = true).block()

        val levels = service.currentState().associateBy(LevelSnapshot::id)
        assertThat(levels.getValue(long.id).state).isEqualTo(LevelState.APPROACH)
        assertThat(levels.getValue(short.id).state).isEqualTo(LevelState.APPROACH)
        assertThat(levels.values.map { level -> level.signal.npu.frozen })
            .containsOnly(true)
        assertThat(levels.values.mapNotNull { level -> level.signal.npu.absolute })
            .usingElementComparator(BigDecimal::compareTo)
            .containsOnly(BigDecimal("0.1"))
    }

    @Test
    fun `same-symbol event order advances one level while missing another`() {
        val nearer = service.create(command(levelPrice = "101.2")).block()!!
        val farther = service.create(command(levelPrice = "102.2")).block()!!
        service.process(bookTicker("90"), marketHealthy = true).block()
        clock.advance(Duration.ofSeconds(10))

        service.process(trade("101.2"), marketHealthy = true).block()
        service.processTimer("BTCUSDT", marketHealthy = true).block()

        val levels = service.currentState().associateBy(LevelSnapshot::id)
        assertThat(levels.getValue(nearer.id).state)
            .isEqualTo(LevelState.TERMINAL)
        assertThat(levels.getValue(farther.id).state)
            .isEqualTo(LevelState.ARMED)
    }

    @Test
    fun `one same-symbol level can own the active attempt or position`() {
        val first = service.create(command(levelPrice = "101.2")).block()!!
        val second = service.create(command(levelPrice = "102.2")).block()!!

        service.recordOwnership(
            levelId = first.id,
            ownsActiveAttempt = true,
            ownsExposure = false,
            hasUnresolvedOrder = false,
        ).block()
        service.processOrderEventPlaceholder("BTCUSDT", "order-1").block()

        val conflict = levelFailure {
            service.recordOwnership(
                levelId = second.id,
                ownsActiveAttempt = true,
                ownsExposure = false,
                hasUnresolvedOrder = false,
            ).block()
        }
        assertThat(conflict.code)
            .isEqualTo(LevelReasonCode.SYMBOL_OWNERSHIP_CONFLICT)
        assertThat(
            service.currentState().single { level -> level.id == second.id }.blockers,
        ).contains(LevelBlocker.SYMBOL_HAS_ACTIVE_OWNER)

        service.recordOwnership(
            levelId = first.id,
            ownsActiveAttempt = false,
            ownsExposure = false,
            hasUnresolvedOrder = false,
        ).block()
        val owner = service.recordOwnership(
            levelId = second.id,
            ownsExposure = true,
            hasUnresolvedOrder = false,
        ).block()!!
        assertThat(owner.ownsExposure).isTrue()
        assertThat(owner.deleteAllowed).isFalse()
    }

    @Test
    fun `every global state is represented by affected level snapshots`() {
        service.create(command()).block()

        assertThat(
            service.currentState(globalState = GlobalTradingState.RUNNING)
                .single().globalState,
        ).isEqualTo(GlobalTradingState.RUNNING)

        val blockingStates = mapOf(
            GlobalTradingState.ENTRY_COOLDOWN to LevelBlocker.ENTRY_COOLDOWN,
            GlobalTradingState.SAFE_MODE to LevelBlocker.SAFE_MODE,
            GlobalTradingState.DAILY_LOCKED to LevelBlocker.DAILY_LOCKED,
            GlobalTradingState.MANUAL_LOCK to LevelBlocker.MANUAL_LOCK,
        )
        blockingStates.forEach { (globalState, blocker) ->
            val snapshot = service.currentState(globalState = globalState).single()
            assertThat(snapshot.globalState).isEqualTo(globalState)
            assertThat(snapshot.blockers).contains(blocker)
        }
    }

    private fun bookTicker(midPrice: String): BookTickerEvent {
        val mid = BigDecimal(midPrice)
        return BookTickerEvent(
            symbol = "BTCUSDT",
            updateId = clock.instant().toEpochMilli(),
            eventTime = clock.instant(),
            transactionTime = clock.instant(),
            bidPrice = mid.subtract(BigDecimal("0.01")),
            bidQuantity = BigDecimal.ONE,
            askPrice = mid.add(BigDecimal("0.01")),
            askQuantity = BigDecimal.ONE,
            receivedAt = clock.instant(),
        )
    }

    private fun trade(price: String): AggregateTradeEvent =
        AggregateTradeEvent(
            symbol = "BTCUSDT",
            aggregateTradeId = clock.instant().toEpochMilli(),
            eventTime = clock.instant(),
            tradeTime = clock.instant(),
            price = BigDecimal(price),
            quantity = BigDecimal.ONE,
            buyerIsMaker = false,
            aggressorSide = AggressorSide.BUY,
            receivedAt = clock.instant(),
        )

    private fun command(
        symbol: String = "BTCUSDT",
        direction: LevelDirection = LevelDirection.LONG,
        levelPrice: String = "101.2",
        positionNotionalUsdt: String = "1000",
        maxImpulsePct: String = "2.5",
    ): CreateLevelCommand =
        CreateLevelCommand(
            symbol = symbol,
            direction = direction,
            levelPrice = BigDecimal(levelPrice),
            positionNotionalUsdt = BigDecimal(positionNotionalUsdt),
            maxImpulsePct = BigDecimal(maxImpulsePct),
        )

    private fun levelFailure(block: () -> Unit): LevelException =
        catchThrowableOfType(block, LevelException::class.java)
}

private class RecordingEvidenceRecorder : EvidenceRecorder by NoOpEvidenceRecorder {
    val transitions = mutableListOf<RecordedTransition>()

    override fun recordStateTransition(
        before: LevelSnapshot,
        after: LevelSnapshot,
        marketData: PublicMarketDataSnapshot?,
        decision: String,
    ) {
        transitions += RecordedTransition(before.state, after.state, decision)
    }
}

private data class RecordedTransition(
    val before: LevelState,
    val after: LevelState,
    val decision: String,
)

private object EmptyMarketDataStreamProvider : PublicMarketDataStreamProvider {
    override fun aggregateTrades(symbol: String): Flux<AggregateTradeEvent> =
        Flux.never()

    override fun bookTickers(symbol: String): Flux<BookTickerEvent> = Flux.never()
}

private class LevelMutableClock(
    initialInstant: Instant,
) : Clock() {
    private val currentInstant = AtomicReference(initialInstant)

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant.get()

    fun advance(duration: Duration) {
        currentInstant.updateAndGet { instant -> instant.plus(duration) }
    }
}

private class LevelTestBinanceClient(
    private val now: Instant,
) : AuthenticatedBinanceClient {
    val configurationOperations = mutableListOf<String>()
    var markPrice = BigDecimal("100")
    var minimumNotional = BigDecimal("5")
    var maintenanceMarginRatio = BigDecimal("0.004")
    var bracketLeverage = 50
    private var marginType = BinanceMarginType.CROSSED
    private var autoAddMargin = true
    private var leverage = 5

    override fun synchronizeClock(): Mono<BinanceClockMeasurement> =
        unsupported()

    override fun accountSummary(): Mono<BinanceAccountSummary> = unsupported()

    override fun positionMode(): Mono<BinancePositionMode> = unsupported()

    override fun assetMode(): Mono<BinanceAssetMode> = unsupported()

    override fun exchangeInfo(): Mono<BinanceExchangeInfo> =
        Mono.just(
            BinanceExchangeInfo(
                serverTime = now,
                symbols = listOf(
                    BinanceSymbolMetadata(
                        symbol = "BTCUSDT",
                        status = "TRADING",
                        contractType = "PERPETUAL",
                        baseAsset = "BTC",
                        quoteAsset = "USDT",
                        marginAsset = "USDT",
                        pricePrecision = 1,
                        quantityPrecision = 3,
                        priceFilter = BinancePriceFilter(
                            minimumPrice = BigDecimal("0.1"),
                            maximumPrice = BigDecimal("1000000"),
                            tickSize = BigDecimal("0.1"),
                        ),
                        lotSizeFilter = BinanceLotSizeFilter(
                            minimumQuantity = BigDecimal("0.001"),
                            maximumQuantity = BigDecimal("1000"),
                            stepSize = BigDecimal("0.001"),
                        ),
                        marketLotSizeFilter = null,
                        minimumNotional = minimumNotional,
                    ),
                ),
            ),
        )

    override fun leverageBrackets(
        symbol: String,
    ): Mono<BinanceSymbolLeverageBrackets> =
        Mono.just(
            BinanceSymbolLeverageBrackets(
                symbol = symbol,
                notionalCoefficient = BigDecimal.ONE,
                brackets = listOf(
                    BinanceLeverageBracket(
                        bracket = 1,
                        initialLeverage = bracketLeverage,
                        notionalFloor = BigDecimal.ZERO,
                        notionalCap = BigDecimal("50000"),
                        maintenanceMarginRatio = maintenanceMarginRatio,
                        cumulativeMaintenanceAmount = BigDecimal.ZERO,
                    ),
                ),
            ),
        )

    override fun commissionRate(symbol: String): Mono<BinanceCommissionRate> =
        unsupported()

    override fun markPrice(symbol: String): Mono<BigDecimal> = Mono.just(markPrice)

    override fun symbolConfiguration(
        symbol: String,
    ): Mono<BinanceSymbolConfiguration> {
        configurationOperations +=
            "READ:${marginType.name}:$autoAddMargin:$leverage"
        return Mono.just(
            BinanceSymbolConfiguration(
                symbol = symbol,
                marginType = marginType,
                autoAddMargin = autoAddMargin,
                leverage = leverage,
                maximumNotional = BigDecimal("50000"),
            ),
        )
    }

    override fun changeMarginType(
        symbol: String,
        marginType: BinanceMarginType,
    ): Mono<Void> {
        configurationOperations += "MARGIN:${marginType.name}"
        this.marginType = marginType
        if (marginType == BinanceMarginType.ISOLATED) {
            autoAddMargin = false
        }
        return Mono.empty()
    }

    override fun changeInitialLeverage(
        symbol: String,
        leverage: Int,
    ): Mono<Void> {
        configurationOperations += "LEVERAGE:$leverage"
        this.leverage = leverage
        return Mono.empty()
    }

    override fun startUserDataStream(): Mono<String> = unsupported()

    override fun keepAliveUserDataStream(listenKey: String): Mono<Void> =
        unsupported()

    fun presetConfiguration(
        marginType: BinanceMarginType,
        autoAddMargin: Boolean,
        leverage: Int,
    ) {
        this.marginType = marginType
        this.autoAddMargin = autoAddMargin
        this.leverage = leverage
    }

    private fun <T> unsupported(): Mono<T> =
        Mono.error(UnsupportedOperationException("Not used by level tests"))
}
