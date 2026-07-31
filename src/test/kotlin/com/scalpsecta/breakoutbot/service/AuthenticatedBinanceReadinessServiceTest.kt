package com.scalpsecta.breakoutbot.service

import com.scalpsecta.breakoutbot.binance.AuthenticatedBinanceClient
import com.scalpsecta.breakoutbot.binance.BinanceAccountSummary
import com.scalpsecta.breakoutbot.binance.BinanceAssetMode
import com.scalpsecta.breakoutbot.binance.BinanceClockMeasurement
import com.scalpsecta.breakoutbot.binance.BinanceCommissionRate
import com.scalpsecta.breakoutbot.binance.BinanceExchangeInfo
import com.scalpsecta.breakoutbot.binance.BinanceLeverageBracket
import com.scalpsecta.breakoutbot.binance.BinancePositionMode
import com.scalpsecta.breakoutbot.binance.BinancePrivateStreamConnectionState
import com.scalpsecta.breakoutbot.binance.BinancePrivateStreamMessage
import com.scalpsecta.breakoutbot.binance.BinanceSymbolLeverageBrackets
import com.scalpsecta.breakoutbot.binance.BinanceSymbolMetadata
import com.scalpsecta.breakoutbot.binance.BinanceUserDataStreamProvider
import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CopyOnWriteArrayList

class AuthenticatedBinanceReadinessServiceTest {
    private val now = Instant.parse("2026-07-31T11:12:13.456Z")
    private val clock = Clock.fixed(now, ZoneOffset.UTC)
    private val client = RecordingReadinessClient(now)
    private val streamProvider = BinanceUserDataStreamProvider {
        Flux.concat(
            Flux.just(
                BinancePrivateStreamMessage.Connected(now.plusMillis(20)),
            ),
            Flux.never(),
        )
    }
    private val service = AuthenticatedBinanceReadinessService(
        client = client,
        streamProvider = streamProvider,
        clock = clock,
        startupEnabled = true,
    )

    @AfterEach
    fun closeService() {
        service.close()
    }

    @Test
    fun `startup establishes clock account equity metadata and private readiness`() {
        service.start()

        val snapshot = service.snapshot()
        assertThat(snapshot.clock.readiness).isEqualTo(BinanceReadiness.READY)
        assertThat(snapshot.clock.serverOffsetMillis).isEqualTo(15L)
        assertThat(snapshot.account.readiness).isEqualTo(BinanceReadiness.READY)
        assertThat(snapshot.account.canTrade).isTrue()
        assertThat(snapshot.account.positionMode)
            .isEqualTo(BinancePositionMode.ONE_WAY)
        assertThat(snapshot.account.assetMode)
            .isEqualTo(BinanceAssetMode.SINGLE_ASSET)
        assertThat(snapshot.account.loadedSymbolCount).isOne()
        assertThat(snapshot.currentEquity)
            .isEqualByComparingTo(BigDecimal("1003.50000000"))
        assertThat(snapshot.temporaryDailyAnchorEquity)
            .isEqualByComparingTo(BigDecimal("1003.50000000"))
        assertThat(snapshot.privateStream.readiness)
            .isEqualTo(BinanceReadiness.READY)
        assertThat(snapshot.privateStream.connectionState)
            .isEqualTo(BinancePrivateStreamConnectionState.CONNECTED)

        assertThat(client.operations).containsExactly(
            ReadinessOperation.SYNCHRONIZE_CLOCK,
            ReadinessOperation.ACCOUNT_SUMMARY,
            ReadinessOperation.POSITION_MODE,
            ReadinessOperation.ASSET_MODE,
            ReadinessOperation.EXCHANGE_INFO,
            ReadinessOperation.START_USER_DATA_STREAM,
        )
    }

    @Test
    fun `hedge or multi-asset configuration remains visible and not ready`() {
        client.positionMode = BinancePositionMode.HEDGE
        client.assetMode = BinanceAssetMode.MULTI_ASSET

        service.start()

        val account = service.snapshot().account
        assertThat(account.readiness).isEqualTo(BinanceReadiness.NOT_READY)
        assertThat(account.positionMode).isEqualTo(BinancePositionMode.HEDGE)
        assertThat(account.assetMode).isEqualTo(BinanceAssetMode.MULTI_ASSET)
        assertThat(client.operations).doesNotContain(
            ReadinessOperation.CHANGE_POSITION_MODE,
            ReadinessOperation.CHANGE_ASSET_MODE,
            ReadinessOperation.DISCOVER_POSITIONS,
            ReadinessOperation.DISCOVER_OPEN_ORDERS,
        )
    }

    @Test
    fun `clock skew is explicit and independently unhealthy`() {
        client.clockOffsetMillis = 1_001L

        service.start()

        val snapshot = service.snapshot()
        assertThat(snapshot.clock.readiness).isEqualTo(BinanceReadiness.NOT_READY)
        assertThat(snapshot.clock.serverOffsetMillis).isEqualTo(1_001L)
        assertThat(snapshot.account.readiness).isEqualTo(BinanceReadiness.READY)
        assertThat(snapshot.privateStream.readiness)
            .isEqualTo(BinanceReadiness.READY)
    }
}

private class RecordingReadinessClient(
    private val now: Instant,
) : AuthenticatedBinanceClient {
    val operations = CopyOnWriteArrayList<ReadinessOperation>()
    var clockOffsetMillis: Long = 15L
    var positionMode: BinancePositionMode = BinancePositionMode.ONE_WAY
    var assetMode: BinanceAssetMode = BinanceAssetMode.SINGLE_ASSET

    override fun synchronizeClock(): Mono<BinanceClockMeasurement> =
        record(ReadinessOperation.SYNCHRONIZE_CLOCK) {
            BinanceClockMeasurement(
                serverTime = now.plusMillis(clockOffsetMillis),
                checkedAt = now,
                serverOffsetMillis = clockOffsetMillis,
                roundTripMillis = 20L,
            )
        }

    override fun accountSummary(): Mono<BinanceAccountSummary> =
        record(ReadinessOperation.ACCOUNT_SUMMARY) {
            BinanceAccountSummary(
                canTrade = true,
                feeTier = 1,
                totalWalletBalance = BigDecimal("1000.00000000"),
                totalUnrealizedProfit = BigDecimal("3.50000000"),
                totalMarginBalance = BigDecimal("1003.50000000"),
                availableBalance = BigDecimal("800.00000000"),
                updatedAt = now,
            )
        }

    override fun positionMode(): Mono<BinancePositionMode> =
        record(ReadinessOperation.POSITION_MODE) { positionMode }

    override fun assetMode(): Mono<BinanceAssetMode> =
        record(ReadinessOperation.ASSET_MODE) { assetMode }

    override fun exchangeInfo(): Mono<BinanceExchangeInfo> =
        record(ReadinessOperation.EXCHANGE_INFO) {
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
                        pricePrecision = 2,
                        quantityPrecision = 3,
                        priceFilter = null,
                        lotSizeFilter = null,
                        marketLotSizeFilter = null,
                        minimumNotional = null,
                    ),
                ),
            )
        }

    override fun leverageBrackets(
        symbol: String,
    ): Mono<BinanceSymbolLeverageBrackets> =
        record(ReadinessOperation.LEVERAGE_BRACKETS) {
            BinanceSymbolLeverageBrackets(
                symbol = symbol,
                notionalCoefficient = BigDecimal.ONE,
                brackets = listOf(
                    BinanceLeverageBracket(
                        bracket = 1,
                        initialLeverage = 20,
                        notionalFloor = BigDecimal.ZERO,
                        notionalCap = BigDecimal("50000"),
                        maintenanceMarginRatio = BigDecimal("0.004"),
                        cumulativeMaintenanceAmount = BigDecimal.ZERO,
                    ),
                ),
            )
        }

    override fun commissionRate(symbol: String): Mono<BinanceCommissionRate> =
        record(ReadinessOperation.COMMISSION_RATE) {
            BinanceCommissionRate(
                symbol = symbol,
                makerRate = BigDecimal("0.0002"),
                takerRate = BigDecimal("0.0005"),
            )
        }

    override fun startUserDataStream(): Mono<String> =
        record(ReadinessOperation.START_USER_DATA_STREAM) {
            "recorded-listen-key"
        }

    override fun keepAliveUserDataStream(listenKey: String): Mono<Void> {
        operations += ReadinessOperation.KEEP_ALIVE_USER_DATA_STREAM
        return Mono.empty()
    }

    private fun <T : Any> record(
        operation: ReadinessOperation,
        result: () -> T,
    ): Mono<T> {
        operations += operation
        return Mono.just(result())
    }
}

private enum class ReadinessOperation {
    SYNCHRONIZE_CLOCK,
    ACCOUNT_SUMMARY,
    POSITION_MODE,
    ASSET_MODE,
    EXCHANGE_INFO,
    LEVERAGE_BRACKETS,
    COMMISSION_RATE,
    START_USER_DATA_STREAM,
    KEEP_ALIVE_USER_DATA_STREAM,
    DISCOVER_POSITIONS,
    DISCOVER_OPEN_ORDERS,
    CHANGE_POSITION_MODE,
    CHANGE_ASSET_MODE,
}
