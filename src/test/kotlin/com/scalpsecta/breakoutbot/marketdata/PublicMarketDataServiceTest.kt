package com.scalpsecta.breakoutbot.marketdata

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.test.StepVerifier
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class PublicMarketDataServiceTest {
    private val now = Instant.parse("2026-07-29T10:15:30Z")
    private val clock = MutableClock(now)
    private val provider = RecordingPublicMarketDataStreamProvider()
    private val service = PublicMarketDataService(provider, clock)

    @AfterEach
    fun closeService() {
        service.close()
    }

    @Test
    fun `multiple levels share streams until the last observation closes`() {
        val firstLevel = service.observe("btcusdt")
        val secondLevel = service.observe("BTCUSDT")

        assertThat(provider.aggregateTradeOpenCount("BTCUSDT")).isOne()
        assertThat(provider.bookTickerOpenCount("BTCUSDT")).isOne()
        assertThat(service.activeSymbolCount()).isOne()

        firstLevel.close()

        assertThat(provider.aggregateTradeCancelCount("BTCUSDT")).isZero()
        assertThat(provider.bookTickerCancelCount("BTCUSDT")).isZero()
        assertThat(service.activeSymbolCount()).isOne()

        secondLevel.close()

        assertThat(provider.aggregateTradeCancelCount("BTCUSDT")).isOne()
        assertThat(provider.bookTickerCancelCount("BTCUSDT")).isOne()
        assertThat(service.activeSymbolCount()).isZero()
    }

    @Test
    fun `duplicate aggregate trade IDs are ignored and a later fill resolves a gap`() {
        val observation = service.observe("BTCUSDT")

        StepVerifier
            .create(observation.bookTickers)
            .then {
                provider.emit(bookTicker(receivedAt = now))
            }
            .expectNextCount(1)
            .thenCancel()
            .verify()

        StepVerifier
            .create(observation.aggregateTrades)
            .then {
                provider.emit(aggregateTrade(100L))
                provider.emit(aggregateTrade(100L))
                provider.emit(aggregateTrade(102L))
            }
            .assertNext { trade ->
                assertThat(trade.aggregateTradeId).isEqualTo(100L)
            }
            .assertNext { trade ->
                assertThat(trade.aggregateTradeId).isEqualTo(102L)
            }
            .thenCancel()
            .verify()

        assertThat(service.snapshots().single().gapStatus)
            .isEqualTo(AggregateTradeGapStatus.GAP_DETECTED)
        assertThat(service.snapshots().single().healthy).isFalse()

        StepVerifier
            .create(observation.aggregateTrades)
            .then {
                provider.emit(aggregateTrade(101L))
            }
            .assertNext { trade ->
                assertThat(trade.aggregateTradeId).isEqualTo(101L)
            }
            .thenCancel()
            .verify()

        assertThat(service.snapshots().single().gapStatus)
            .isEqualTo(AggregateTradeGapStatus.CONTINUOUS)
        assertThat(service.snapshots().single().healthy).isTrue()
    }

    @Test
    fun `quiet trade stream stays healthy while bid ask heartbeat is fresh`() {
        val observation = service.observe("BTCUSDT")

        StepVerifier
            .create(observation.bookTickers)
            .then {
                provider.emit(bookTicker(receivedAt = now.minusMillis(50)))
            }
            .expectNextCount(1)
            .thenCancel()
            .verify()

        val snapshot = service.snapshots().single()
        assertThat(snapshot.latestAggregateTradeId).isNull()
        assertThat(snapshot.connectionState)
            .isEqualTo(PublicStreamConnectionState.CONNECTED)
        assertThat(snapshot.bidAskHeartbeatHealthy).isTrue()
        assertThat(snapshot.healthy).isTrue()
        assertThat(service.readiness()).isEqualTo(BinanceReadiness.READY)
    }

    @Test
    fun `stale bid ask heartbeat is disconnected and unhealthy`() {
        val observation = service.observe("BTCUSDT")

        StepVerifier
            .create(observation.bookTickers)
            .then {
                provider.emit(bookTicker(receivedAt = now))
            }
            .expectNextCount(1)
            .thenCancel()
            .verify()

        clock.advance(Duration.ofMillis(251))

        val snapshot = service.snapshots().single()
        assertThat(snapshot.connectionState)
            .isEqualTo(PublicStreamConnectionState.DISCONNECTED)
        assertThat(snapshot.bidAskHeartbeatHealthy).isFalse()
        assertThat(snapshot.healthy).isFalse()
        assertThat(service.readiness()).isEqualTo(BinanceReadiness.NOT_READY)
    }

    @Test
    fun `delayed exchange event is unhealthy even when just received`() {
        val observation = service.observe("BTCUSDT")

        StepVerifier
            .create(observation.bookTickers)
            .then {
                provider.emit(
                    bookTicker(
                        receivedAt = now,
                        eventTime = now.minusMillis(251),
                    ),
                )
            }
            .expectNextCount(1)
            .thenCancel()
            .verify()

        val snapshot = service.snapshots().single()
        assertThat(snapshot.connectionState)
            .isEqualTo(PublicStreamConnectionState.CONNECTED)
        assertThat(snapshot.bidAskHeartbeatHealthy).isTrue()
        assertThat(snapshot.bookTickerAge?.exchangeAgeMillis).isEqualTo(251L)
        assertThat(snapshot.healthy).isFalse()
    }

    @Test
    fun `snapshot preserves decimal precision and exposes receive and exchange ages`() {
        val observation = service.observe("BTCUSDT")
        val ticker = bookTicker(
            receivedAt = now.minusMillis(50),
            eventTime = now.minusMillis(70),
        )

        StepVerifier
            .create(observation.bookTickers)
            .then {
                provider.emit(ticker)
            }
            .expectNext(ticker)
            .thenCancel()
            .verify()

        val snapshot = service.snapshots().single()
        assertThat(snapshot.latestBidPrice)
            .isEqualByComparingTo(BigDecimal("68123.12345678"))
        assertThat(snapshot.latestBidQuantity)
            .isEqualByComparingTo(BigDecimal("1.00000003"))
        assertThat(snapshot.latestAskPrice)
            .isEqualByComparingTo(BigDecimal("68123.12345689"))
        assertThat(snapshot.latestAskQuantity)
            .isEqualByComparingTo(BigDecimal("2.00000007"))
        assertThat(snapshot.spread)
            .isEqualByComparingTo(BigDecimal("0.00000011"))
        assertThat(snapshot.bookTickerAge?.receiveAgeMillis).isEqualTo(50L)
        assertThat(snapshot.bookTickerAge?.exchangeAgeMillis).isEqualTo(70L)
    }

    private fun aggregateTrade(id: Long): AggregateTradeEvent =
        AggregateTradeEvent(
            symbol = "BTCUSDT",
            aggregateTradeId = id,
            eventTime = now.minusMillis(10),
            tradeTime = now.minusMillis(11),
            price = BigDecimal("68123.12345678"),
            quantity = BigDecimal("0.00100000"),
            buyerIsMaker = false,
            aggressorSide = AggressorSide.BUY,
            receivedAt = now.minusMillis(5),
        )

    private fun bookTicker(
        receivedAt: Instant,
        eventTime: Instant = receivedAt.minusMillis(20),
    ): BookTickerEvent =
        BookTickerEvent(
            symbol = "BTCUSDT",
            updateId = 400_900_217L,
            eventTime = eventTime,
            transactionTime = eventTime.minusMillis(1),
            bidPrice = BigDecimal("68123.12345678"),
            bidQuantity = BigDecimal("1.00000003"),
            askPrice = BigDecimal("68123.12345689"),
            askQuantity = BigDecimal("2.00000007"),
            receivedAt = receivedAt,
        )
}

private class MutableClock(
    initialInstant: Instant,
) : Clock() {
    private val currentInstant = AtomicReference(initialInstant)

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = currentInstant.get()

    fun advance(duration: Duration) {
        currentInstant.updateAndGet { instant ->
            instant.plus(duration)
        }
    }
}

private class RecordingPublicMarketDataStreamProvider :
    PublicMarketDataStreamProvider {
    private val aggregateTradeStreams =
        ConcurrentHashMap<String, RecordingStream<AggregateTradeEvent>>()
    private val bookTickerStreams =
        ConcurrentHashMap<String, RecordingStream<BookTickerEvent>>()

    override fun aggregateTrades(symbol: String): Flux<AggregateTradeEvent> =
        aggregateTradeStreams
            .computeIfAbsent(symbol) { RecordingStream() }
            .flux()

    override fun bookTickers(symbol: String): Flux<BookTickerEvent> =
        bookTickerStreams
            .computeIfAbsent(symbol) { RecordingStream() }
            .flux()

    fun emit(event: AggregateTradeEvent) {
        aggregateTradeStreams.getValue(event.symbol).emit(event)
    }

    fun emit(event: BookTickerEvent) {
        bookTickerStreams.getValue(event.symbol).emit(event)
    }

    fun aggregateTradeOpenCount(symbol: String): Int =
        aggregateTradeStreams.getValue(symbol).openCount.get()

    fun aggregateTradeCancelCount(symbol: String): Int =
        aggregateTradeStreams.getValue(symbol).cancelCount.get()

    fun bookTickerOpenCount(symbol: String): Int =
        bookTickerStreams.getValue(symbol).openCount.get()

    fun bookTickerCancelCount(symbol: String): Int =
        bookTickerStreams.getValue(symbol).cancelCount.get()
}

private class RecordingStream<T : Any> {
    private val sink = Sinks.many().multicast().onBackpressureBuffer<T>()
    val openCount = AtomicInteger()
    val cancelCount = AtomicInteger()

    fun flux(): Flux<T> =
        sink
            .asFlux()
            .doOnSubscribe {
                openCount.incrementAndGet()
            }
            .doOnCancel {
                cancelCount.incrementAndGet()
            }

    fun emit(event: T) {
        assertThat(sink.tryEmitNext(event).isSuccess).isTrue()
    }
}
