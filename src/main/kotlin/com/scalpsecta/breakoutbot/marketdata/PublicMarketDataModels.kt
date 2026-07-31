package com.scalpsecta.breakoutbot.marketdata

import java.math.BigDecimal
import java.time.Instant

data class AggregateTradeEvent(
    val symbol: String,
    val aggregateTradeId: Long,
    val eventTime: Instant,
    val tradeTime: Instant,
    val price: BigDecimal,
    val quantity: BigDecimal,
    val buyerIsMaker: Boolean,
    val aggressorSide: AggressorSide,
    val receivedAt: Instant,
)

enum class AggressorSide {
    BUY,
    SELL,
}

data class BookTickerEvent(
    val symbol: String,
    val updateId: Long,
    val eventTime: Instant?,
    val transactionTime: Instant?,
    val bidPrice: BigDecimal,
    val bidQuantity: BigDecimal,
    val askPrice: BigDecimal,
    val askQuantity: BigDecimal,
    val receivedAt: Instant,
)

data class PublicMarketDataSnapshot(
    val symbol: String,
    val connectionState: PublicStreamConnectionState,
    val healthy: Boolean,
    val bidAskHeartbeatHealthy: Boolean,
    val gapStatus: AggregateTradeGapStatus,
    val latestAggregateTradeId: Long?,
    val latestBidPrice: BigDecimal?,
    val latestBidQuantity: BigDecimal?,
    val latestAskPrice: BigDecimal?,
    val latestAskQuantity: BigDecimal?,
    val spread: BigDecimal?,
    val aggregateTradeAge: MarketEventAgeSnapshot?,
    val bookTickerAge: MarketEventAgeSnapshot?,
)

data class MarketEventAgeSnapshot(
    val receiveAgeMillis: Long,
    val exchangeAgeMillis: Long?,
)

enum class PublicStreamConnectionState {
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
}

enum class AggregateTradeGapStatus {
    CONTINUOUS,
    GAP_DETECTED,
}
