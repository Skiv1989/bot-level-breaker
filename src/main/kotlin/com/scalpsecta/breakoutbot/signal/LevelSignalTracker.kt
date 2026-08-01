package com.scalpsecta.breakoutbot.signal

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSubscription
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

internal class LevelSignalTracker(
    symbol: String,
    direction: LevelDirection,
    levelPrice: BigDecimal,
    tickSize: BigDecimal,
    private val marketDataSubscription: PublicMarketDataSubscription,
    private val clock: Clock,
    scheduler: Scheduler,
) : AutoCloseable {
    private val engine = SignalEngine(
        symbol = symbol,
        direction = direction,
        levelPrice = levelPrice,
        tickSize = tickSize,
    )
    private val npuMode = AtomicReference(NpuMode.WARMING_UP)
    private val closed = AtomicBoolean()
    private val eventSubscription: Disposable = Flux
        .merge(
            marketDataSubscription.aggregateTrades.map(TrackerEvent::Trade),
            marketDataSubscription.bookTickers.map(TrackerEvent::BookTicker),
            Flux
                .interval(SAMPLE_INTERVAL, SAMPLE_INTERVAL, scheduler)
                .map { TrackerEvent.Tick },
        )
        .publishOn(scheduler)
        .subscribe(::handle)

    fun snapshot(
        publicMarketData: PublicMarketDataSnapshot?,
        privateStreamReadiness: BinanceReadiness,
        hasUnresolvedOrder: Boolean,
    ): LevelSignalSnapshot =
        engine.snapshot(
            now = clock.instant(),
            publicMarketData = publicMarketData,
            privateStreamReadiness = privateStreamReadiness,
            hasUnresolvedOrder = hasUnresolvedOrder,
        )

    fun updateNpuMode(mode: NpuMode) {
        npuMode.set(mode)
        engine.tick(clock.instant(), mode)
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) {
            return
        }
        eventSubscription.dispose()
        marketDataSubscription.close()
    }

    private fun handle(event: TrackerEvent) {
        when (event) {
            is TrackerEvent.Trade -> engine.record(event.event)
            is TrackerEvent.BookTicker -> engine.record(event.event)
            TrackerEvent.Tick -> engine.tick(clock.instant(), npuMode.get())
        }
    }
}

private sealed interface TrackerEvent {
    data class Trade(
        val event: AggregateTradeEvent,
    ) : TrackerEvent

    data class BookTicker(
        val event: BookTickerEvent,
    ) : TrackerEvent

    data object Tick : TrackerEvent
}

private val SAMPLE_INTERVAL: Duration = Duration.ofMillis(100)
