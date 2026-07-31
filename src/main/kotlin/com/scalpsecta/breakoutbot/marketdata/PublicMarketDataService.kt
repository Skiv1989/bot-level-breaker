package com.scalpsecta.breakoutbot.marketdata

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Service
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Service
class PublicMarketDataService(
    private val streamProvider: PublicMarketDataStreamProvider,
    private val clock: Clock,
) {
    private val lock = ReentrantLock()
    private val channels = mutableMapOf<String, SymbolChannel>()

    fun observe(symbol: String): PublicMarketDataSubscription {
        val normalizedSymbol = symbol.trim().uppercase()
        require(normalizedSymbol.isNotEmpty()) {
            "symbol must not be blank"
        }

        val channel = lock.withLock {
            channels.getOrPut(normalizedSymbol) {
                SymbolChannel(
                    symbol = normalizedSymbol,
                    streamProvider = streamProvider,
                    clock = clock,
                ).also(SymbolChannel::start)
            }.also(SymbolChannel::retain)
        }

        return PublicMarketDataSubscription(
            symbol = normalizedSymbol,
            aggregateTrades = channel.aggregateTrades(),
            bookTickers = channel.bookTickers(),
            release = { release(channel) },
        )
    }

    fun snapshots(): List<PublicMarketDataSnapshot> {
        val currentChannels = lock.withLock {
            channels.values.toList()
        }
        val now = clock.instant()
        return currentChannels
            .map { channel -> channel.snapshot(now) }
            .sortedBy(PublicMarketDataSnapshot::symbol)
    }

    fun readiness(
        snapshots: List<PublicMarketDataSnapshot> = snapshots(),
    ): BinanceReadiness =
        if (
            snapshots.isNotEmpty() &&
            snapshots.all(PublicMarketDataSnapshot::healthy)
        ) {
            BinanceReadiness.READY
        } else {
            BinanceReadiness.NOT_READY
        }

    fun activeSymbolCount(): Int =
        lock.withLock {
            channels.size
        }

    @PreDestroy
    fun close() {
        val channelsToClose = lock.withLock {
            channels.values.toList().also {
                channels.clear()
            }
        }
        channelsToClose.forEach(SymbolChannel::close)
    }

    private fun release(channel: SymbolChannel) {
        val closeChannel = lock.withLock {
            if (channel.release() == 0) {
                channels.remove(channel.symbol, channel)
                true
            } else {
                false
            }
        }
        if (closeChannel) {
            channel.close()
        }
    }
}

class PublicMarketDataSubscription internal constructor(
    val symbol: String,
    val aggregateTrades: Flux<AggregateTradeEvent>,
    val bookTickers: Flux<BookTickerEvent>,
    private val release: () -> Unit,
) : AutoCloseable {
    private val closed = AtomicBoolean()

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            release()
        }
    }
}

private class SymbolChannel(
    val symbol: String,
    private val streamProvider: PublicMarketDataStreamProvider,
    private val clock: Clock,
) {
    private val continuity = AggregateTradeContinuity()
    private val state = AtomicReference(SymbolState())
    private val aggregateTradeSink = Sinks
        .many()
        .multicast()
        .onBackpressureBuffer<AggregateTradeEvent>(
            EVENT_BUFFER_SIZE,
            false,
        )
    private val bookTickerSink = Sinks.many().replay().latest<BookTickerEvent>()
    private val scheduler: Scheduler = Schedulers.newSingle(
        "public-market-data-${symbol.lowercase()}",
    )
    private val closed = AtomicBoolean()
    private lateinit var eventSubscription: Disposable
    private var referenceCount = 0

    fun start() {
        try {
            eventSubscription = Flux
                .merge(
                    streamProvider
                        .aggregateTrades(symbol)
                        .map(SymbolEvent::AggregateTrade),
                    streamProvider
                        .bookTickers(symbol)
                        .map(SymbolEvent::BookTicker),
                )
                .publishOn(scheduler)
                .subscribe(
                    ::handle,
                    ::handleDisconnected,
                )
        } catch (error: RuntimeException) {
            handleDisconnected(error)
        }
    }

    fun retain() {
        referenceCount += 1
    }

    fun release(): Int {
        check(referenceCount > 0) {
            "Cannot release unreferenced public market data for $symbol"
        }
        referenceCount -= 1
        return referenceCount
    }

    fun aggregateTrades(): Flux<AggregateTradeEvent> =
        aggregateTradeSink.asFlux()

    fun bookTickers(): Flux<BookTickerEvent> =
        bookTickerSink.asFlux()

    fun snapshot(now: Instant): PublicMarketDataSnapshot {
        val currentState = state.get()
        val tradeAge = currentState.latestAggregateTrade?.let { trade ->
            marketEventAge(
                now = now,
                receivedAt = trade.receivedAt,
                exchangeTime = trade.eventTime,
            )
        }
        val bookAge = currentState.latestBookTicker?.let { book ->
            marketEventAge(
                now = now,
                receivedAt = book.receivedAt,
                exchangeTime = book.eventTime ?: book.transactionTime,
            )
        }
        val heartbeatHealthy =
            bookAge != null &&
                bookAge.receiveAgeMillis <= MAX_HEALTHY_EVENT_AGE_MILLIS
        val eventAgeHealthy =
            bookAge != null &&
                (
                    bookAge.exchangeAgeMillis == null ||
                        bookAge.exchangeAgeMillis <=
                        MAX_HEALTHY_EVENT_AGE_MILLIS
                    )
        val connectionState = when {
            currentState.disconnected -> PublicStreamConnectionState.DISCONNECTED
            currentState.latestBookTicker == null -> PublicStreamConnectionState.CONNECTING
            heartbeatHealthy -> PublicStreamConnectionState.CONNECTED
            else -> PublicStreamConnectionState.DISCONNECTED
        }
        val bookTicker = currentState.latestBookTicker

        return PublicMarketDataSnapshot(
            symbol = symbol,
            connectionState = connectionState,
            healthy =
                connectionState == PublicStreamConnectionState.CONNECTED &&
                    heartbeatHealthy &&
                    eventAgeHealthy &&
                    currentState.gapStatus == AggregateTradeGapStatus.CONTINUOUS,
            bidAskHeartbeatHealthy = heartbeatHealthy,
            gapStatus = currentState.gapStatus,
            latestAggregateTradeId =
                currentState.latestAggregateTrade?.aggregateTradeId,
            latestBidPrice = bookTicker?.bidPrice,
            latestBidQuantity = bookTicker?.bidQuantity,
            latestAskPrice = bookTicker?.askPrice,
            latestAskQuantity = bookTicker?.askQuantity,
            spread = bookTicker?.let { book ->
                book.askPrice.subtract(book.bidPrice)
            },
            aggregateTradeAge = tradeAge,
            bookTickerAge = bookAge,
        )
    }

    fun close() {
        if (!closed.compareAndSet(false, true)) {
            return
        }
        if (::eventSubscription.isInitialized) {
            eventSubscription.dispose()
        }
        aggregateTradeSink.tryEmitComplete()
        bookTickerSink.tryEmitComplete()
        scheduler.dispose()
    }

    private fun handle(event: SymbolEvent) {
        when (event) {
            is SymbolEvent.AggregateTrade -> handle(event.event)
            is SymbolEvent.BookTicker -> handle(event.event)
        }
    }

    private fun handle(event: AggregateTradeEvent) {
        if (event.symbol != symbol) {
            return
        }

        if (
            continuity.observe(event.aggregateTradeId) ==
            AggregateTradeObservation.DUPLICATE
        ) {
            return
        }

        val currentState = state.get()
        val latestAggregateTrade =
            if (
                currentState.latestAggregateTrade == null ||
                event.aggregateTradeId >
                currentState.latestAggregateTrade.aggregateTradeId
            ) {
                event
            } else {
                currentState.latestAggregateTrade
            }
        state.set(
            currentState.copy(
                latestAggregateTrade = latestAggregateTrade,
                gapStatus = continuity.gapStatus(),
                disconnected = false,
            ),
        )
        aggregateTradeSink.tryEmitNext(event)
    }

    private fun handle(event: BookTickerEvent) {
        if (event.symbol != symbol) {
            return
        }

        state.updateAndGet { currentState ->
            currentState.copy(
                latestBookTicker = event,
                disconnected = false,
            )
        }
        bookTickerSink.tryEmitNext(event)
    }

    private fun handleDisconnected(@Suppress("UNUSED_PARAMETER") error: Throwable) {
        state.updateAndGet { currentState ->
            currentState.copy(disconnected = true)
        }
    }

    private fun marketEventAge(
        now: Instant,
        receivedAt: Instant,
        exchangeTime: Instant?,
    ): MarketEventAgeSnapshot =
        MarketEventAgeSnapshot(
            receiveAgeMillis = ageMillis(receivedAt, now),
            exchangeAgeMillis = exchangeTime?.let { time ->
                ageMillis(time, now)
            },
        )

    private fun ageMillis(
        eventTime: Instant,
        now: Instant,
    ): Long =
        Duration
            .between(eventTime, now)
            .toMillis()
            .coerceAtLeast(0)
}

private data class SymbolState(
    val latestAggregateTrade: AggregateTradeEvent? = null,
    val latestBookTicker: BookTickerEvent? = null,
    val gapStatus: AggregateTradeGapStatus =
        AggregateTradeGapStatus.CONTINUOUS,
    val disconnected: Boolean = false,
)

private sealed interface SymbolEvent {
    data class AggregateTrade(
        val event: AggregateTradeEvent,
    ) : SymbolEvent

    data class BookTicker(
        val event: BookTickerEvent,
    ) : SymbolEvent
}

private const val EVENT_BUFFER_SIZE = 1_024
private const val MAX_HEALTHY_EVENT_AGE_MILLIS = 250L
