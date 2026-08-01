package com.scalpsecta.breakoutbot.signal

import com.scalpsecta.breakoutbot.domain.BinanceReadiness
import com.scalpsecta.breakoutbot.level.LevelDirection
import com.scalpsecta.breakoutbot.marketdata.AggregateTradeEvent
import com.scalpsecta.breakoutbot.marketdata.BookTickerEvent
import com.scalpsecta.breakoutbot.marketdata.PublicMarketDataSnapshot
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant

internal class LevelSignalTracker(
    symbol: String,
    direction: LevelDirection,
    levelPrice: BigDecimal,
    tickSize: BigDecimal,
    private val clock: Clock,
) {
    private val engine = SignalEngine(
        symbol = symbol,
        direction = direction,
        levelPrice = levelPrice,
        tickSize = tickSize,
    )

    fun snapshot(
        publicMarketData: PublicMarketDataSnapshot?,
        privateStreamReadiness: BinanceReadiness,
        hasUnresolvedOrder: Boolean,
        now: Instant = clock.instant(),
    ): LevelSignalSnapshot =
        engine.snapshot(
            now = now,
            publicMarketData = publicMarketData,
            privateStreamReadiness = privateStreamReadiness,
            hasUnresolvedOrder = hasUnresolvedOrder,
        )

    fun record(event: AggregateTradeEvent) = engine.record(event)

    fun record(event: BookTickerEvent) = engine.record(event)

    fun tick(now: Instant, mode: NpuMode) = engine.tick(now, mode)

    fun tradePrices(now: Instant, duration: Duration): List<BigDecimal> =
        engine.tradePrices(now, duration)
}
