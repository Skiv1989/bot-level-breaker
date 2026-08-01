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
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataService
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataStreamProvider
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class LevelServiceTest {
    private val now = Instant.parse("2026-07-31T12:00:00Z")
    private val client = LevelTestBinanceClient(now)
    private val marketDataService = PublicMarketDataService(
        streamProvider = EmptyMarketDataStreamProvider,
        clock = Clock.fixed(now, ZoneOffset.UTC),
    )
    private val service = LevelService(
        client = client,
        publicMarketDataService = marketDataService,
        clock = Clock.fixed(now, ZoneOffset.UTC),
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
        )
        val exposureFailure = levelFailure { service.delete(created.id) }
        assertThat(exposureFailure.code)
            .isEqualTo(LevelReasonCode.LEVEL_HAS_EXPOSURE)

        service.recordOwnership(
            levelId = created.id,
            ownsExposure = false,
            hasUnresolvedOrder = true,
        )
        val orderFailure = levelFailure { service.delete(created.id) }
        assertThat(orderFailure.code)
            .isEqualTo(LevelReasonCode.LEVEL_HAS_UNRESOLVED_ORDER)

        service.recordOwnership(
            levelId = created.id,
            ownsExposure = false,
            hasUnresolvedOrder = false,
        )
        assertThat(service.delete(created.id).id).isEqualTo(created.id)
        assertThat(service.currentState()).isEmpty()
        assertThat(marketDataService.activeSymbolCount()).isZero()
    }

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

private object EmptyMarketDataStreamProvider : PublicMarketDataStreamProvider {
    override fun aggregateTrades(symbol: String): Flux<AggregateTradeEvent> =
        Flux.never()

    override fun bookTickers(symbol: String): Flux<BookTickerEvent> = Flux.never()
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
